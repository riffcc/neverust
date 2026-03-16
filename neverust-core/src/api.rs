//! REST API for block operations and node management

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::Engine;
use cid::{multibase::Base, Cid};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

use crate::archivist_tree::ArchivistTree;
use crate::botg::BoTgProtocol;
use crate::citadel::{
    run_defederation_simulation, CitadelSyncPullRequest, CitadelSyncPullResponse,
    CitadelSyncPushRequest, CitadelSyncPushResponse, DefederationNode,
    DefederationSimulationConfig,
};
use crate::manifest::{Manifest, SHA256_CODEC};
use crate::marketplace::{
    ActiveSlotResponse, MarketplaceRuntimeInfo, MarketplaceStore, PurchaseResponse,
    SaleAvailabilityInput, SalesSlotResponse, StorageRequestInput,
};
use crate::metrics::Metrics;
use crate::storage::{Block, BlockStore, StorageError};
use libp2p::{identity::Keypair, Multiaddr};
use std::sync::RwLock;
use tokio::sync::RwLock as AsyncRwLock;

fn upload_block_size() -> usize {
    std::env::var("NEVERUST_UPLOAD_BLOCK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(1024 * 1024)
}

fn upload_commit_batch_blocks(block_size: usize) -> usize {
    let target_commit_bytes = std::env::var("NEVERUST_UPLOAD_COMMIT_BATCH_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(256 * 1024 * 1024);

    std::env::var("NEVERUST_UPLOAD_COMMIT_BATCH_BLOCKS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or_else(|| (target_commit_bytes / block_size).max(1))
}

fn upload_dedupe_blocks() -> bool {
    std::env::var("NEVERUST_UPLOAD_DEDUPE_BLOCKS")
        .ok()
        .map(|v| {
            let s = v.trim().to_ascii_lowercase();
            !(s == "0" || s == "false" || s == "no" || s == "off")
        })
        .unwrap_or(true)
}

fn upload_hash_workers() -> usize {
    std::env::var("NEVERUST_UPLOAD_HASH_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .clamp(1, 32)
        })
}

fn upload_inflight_batches() -> usize {
    std::env::var("NEVERUST_UPLOAD_INFLIGHT_BATCHES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(4)
        .clamp(1, 16)
}

fn configured_http_fallback_peers() -> Vec<String> {
    std::env::var("NEVERUST_HTTP_FALLBACK_PEERS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn default_http_fallback_peers() -> Vec<String> {
    let mut peer_urls = vec!["http://bootstrap:8080".to_string()];

    // Docker network peers
    for i in 1..50 {
        peer_urls.push(format!("http://node{}:8080", i));
    }

    // Known external peers and common Archivist API ports
    let external_peers = vec![
        "91.98.135.54",
        "10.7.1.200", // blackberry
    ];
    for peer in external_peers {
        for port in [8080, 8070, 8000, 3000] {
            peer_urls.push(format!("http://{}:{}", peer, port));
        }
    }

    peer_urls
}

fn fallback_http_peer_urls() -> Vec<String> {
    let configured = configured_http_fallback_peers();
    if !configured.is_empty() {
        return configured;
    }
    default_http_fallback_peers()
}

fn hash_raw_blocks_parallel(
    raw_blocks: Vec<Vec<u8>>,
    workers: usize,
) -> Result<Vec<Block>, String> {
    let total = raw_blocks.len();
    if total == 0 {
        return Ok(Vec::new());
    }

    let worker_count = workers.max(1).min(total);
    if worker_count == 1 {
        return raw_blocks
            .into_iter()
            .map(|data| Block::new_sha256(data).map_err(|e| e.to_string()))
            .collect();
    }

    let mut lanes: Vec<Vec<(usize, Vec<u8>)>> = (0..worker_count).map(|_| Vec::new()).collect();
    for (idx, data) in raw_blocks.into_iter().enumerate() {
        lanes[idx % worker_count].push((idx, data));
    }

    let mut handles = Vec::with_capacity(worker_count);
    for lane in lanes {
        handles.push(std::thread::spawn(
            move || -> Result<Vec<(usize, Block)>, String> {
                let mut out = Vec::with_capacity(lane.len());
                for (idx, data) in lane {
                    let block = Block::new_sha256(data).map_err(|e| e.to_string())?;
                    out.push((idx, block));
                }
                Ok(out)
            },
        ));
    }

    let mut ordered: Vec<Option<Block>> = vec![None; total];
    for handle in handles {
        let lane = handle
            .join()
            .map_err(|_| "Upload hash worker panicked".to_string())??;
        for (idx, block) in lane {
            ordered[idx] = Some(block);
        }
    }

    ordered
        .into_iter()
        .map(|maybe| maybe.ok_or_else(|| "Missing hashed block result".to_string()))
        .collect()
}

async fn flush_upload_raw_batch(
    state: &ApiState,
    raw_batch: &mut Vec<Vec<u8>>,
    seen_cids: &mut Option<std::collections::HashSet<Cid>>,
    block_cids: &mut Vec<Cid>,
) -> Result<(), ApiError> {
    if raw_batch.is_empty() {
        return Ok(());
    }

    let workers = upload_hash_workers();
    let batch = std::mem::take(raw_batch);
    let blocks = tokio::task::spawn_blocking(move || hash_raw_blocks_parallel(batch, workers))
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to hash upload batch: {}", e)))?
        .map_err(|e| ApiError::Internal(format!("Failed to hash upload batch: {}", e)))?;

    let mut to_store = Vec::with_capacity(blocks.len());
    for block in blocks {
        let cid = block.cid;
        block_cids.push(cid);
        let should_store = if let Some(seen) = seen_cids.as_mut() {
            seen.insert(cid)
        } else {
            true
        };
        if should_store {
            to_store.push(block);
        }
    }

    if !to_store.is_empty() {
        state
            .block_store
            .put_many(to_store)
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to store block batch: {}", e)))?;
    }

    Ok(())
}

async fn process_upload_raw_batch_no_dedupe(
    block_store: Arc<BlockStore>,
    raw_batch: Vec<Vec<u8>>,
) -> Result<Vec<Cid>, ApiError> {
    if raw_batch.is_empty() {
        return Ok(Vec::new());
    }

    let workers = upload_hash_workers();
    let blocks = tokio::task::spawn_blocking(move || hash_raw_blocks_parallel(raw_batch, workers))
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to hash upload batch: {}", e)))?
        .map_err(|e| ApiError::Internal(format!("Failed to hash upload batch: {}", e)))?;

    let cids: Vec<Cid> = blocks.iter().map(|b| b.cid).collect();
    block_store
        .put_many(blocks)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to store block batch: {}", e)))?;
    Ok(cids)
}

/// Convert CID to base58btc string (Archivist format with 'z' prefix)
fn cid_to_string(cid: &Cid) -> String {
    cid.to_string_of_base(Base::Base58Btc)
        .unwrap_or_else(|_| cid.to_string())
}

/// API state shared across handlers
#[derive(Clone)]
pub struct ApiState {
    pub block_store: Arc<BlockStore>,
    pub metrics: Metrics,
    pub peer_id: String,
    pub botg: Arc<BoTgProtocol>,
    pub keypair: Arc<Keypair>,
    pub listen_addrs: Arc<RwLock<Vec<Multiaddr>>>,
    pub announce_addrs: Vec<String>,
    pub fallback_http_peers: Arc<Vec<String>>,
    pub fallback_http_client: reqwest::Client,
    pub ipfs_cluster_pins: Arc<AsyncRwLock<HashMap<String, IpfsClusterPinRecord>>>,
    pub citadel_node: Option<Arc<AsyncRwLock<DefederationNode>>>,
    pub marketplace: Option<MarketplaceStore>,
    pub marketplace_runtime: MarketplaceRuntimeInfo,
}

/// Response for storing a block
#[derive(Serialize, Deserialize)]
pub struct StoreBlockResponse {
    pub cid: String,
    pub size: usize,
}

/// Response for retrieving a block
#[derive(Serialize, Deserialize)]
pub struct GetBlockResponse {
    pub cid: String,
    pub data: String, // base64-encoded
    pub size: usize,
}

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub block_count: usize,
    pub total_bytes: usize,
}

/// Manifest view compatible with Archivist DataItem schema
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestItemResponse {
    pub tree_cid: String,
    pub dataset_size: u64,
    pub block_size: u64,
    #[serde(rename = "protected")]
    pub is_protected: bool,
    pub filename: Option<String>,
    pub mimetype: Option<String>,
}

/// Archivist DataItem
#[derive(Serialize, Deserialize)]
pub struct DataItemResponse {
    pub cid: String,
    pub manifest: ManifestItemResponse,
}

/// Archivist DataList
#[derive(Serialize, Deserialize)]
pub struct DataListResponse {
    pub content: Vec<DataItemResponse>,
}

/// Archivist Space response
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpaceResponse {
    pub total_blocks: usize,
    pub quota_max_bytes: usize,
    pub quota_used_bytes: usize,
    pub quota_reserved_bytes: usize,
}

/// Error response
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IpfsClusterPinRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub replication_factor_min: Option<i32>,
    #[serde(default)]
    pub replication_factor_max: Option<i32>,
    #[serde(default)]
    pub user_allocations: Vec<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    /// Neverust extension tags (optional, backwards compatible).
    #[serde(default)]
    pub neverust_tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpfsClusterPinRecord {
    pub cid: String,
    pub name: Option<String>,
    pub mode: String,
    pub replication_factor_min: i32,
    pub replication_factor_max: i32,
    pub user_allocations: Vec<String>,
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub neverust_tags: HashMap<String, String>,
    pub status: String,
    pub error: Option<String>,
}

impl IpfsClusterPinRecord {
    fn from_request(cid: String, req: IpfsClusterPinRequest) -> Self {
        Self {
            cid,
            name: req.name,
            mode: req.mode.unwrap_or_else(|| "recursive".to_string()),
            replication_factor_min: req.replication_factor_min.unwrap_or(-1),
            replication_factor_max: req.replication_factor_max.unwrap_or(-1),
            user_allocations: req.user_allocations,
            metadata: req.metadata,
            neverust_tags: req.neverust_tags,
            status: "queued".to_string(),
            error: None,
        }
    }
}

/// Create the REST API router
pub fn create_router(
    block_store: Arc<BlockStore>,
    metrics: Metrics,
    peer_id: String,
    botg: Arc<BoTgProtocol>,
    keypair: Arc<Keypair>,
    listen_addrs: Arc<RwLock<Vec<Multiaddr>>>,
) -> Router {
    create_router_with_runtime(
        block_store,
        metrics,
        peer_id,
        botg,
        keypair,
        listen_addrs,
        None,
        None,
        MarketplaceRuntimeInfo::default(),
        Vec::new(),
    )
}

/// Create the REST API router with optional Citadel mode state.
pub fn create_router_with_citadel(
    block_store: Arc<BlockStore>,
    metrics: Metrics,
    peer_id: String,
    botg: Arc<BoTgProtocol>,
    keypair: Arc<Keypair>,
    listen_addrs: Arc<RwLock<Vec<Multiaddr>>>,
    citadel_node: Option<Arc<AsyncRwLock<DefederationNode>>>,
) -> Router {
    create_router_with_runtime(
        block_store,
        metrics,
        peer_id,
        botg,
        keypair,
        listen_addrs,
        citadel_node,
        None,
        MarketplaceRuntimeInfo::default(),
        Vec::new(),
    )
}

/// Create the REST API router with optional Citadel and marketplace state.
pub fn create_router_with_runtime(
    block_store: Arc<BlockStore>,
    metrics: Metrics,
    peer_id: String,
    botg: Arc<BoTgProtocol>,
    keypair: Arc<Keypair>,
    listen_addrs: Arc<RwLock<Vec<Multiaddr>>>,
    citadel_node: Option<Arc<AsyncRwLock<DefederationNode>>>,
    marketplace: Option<MarketplaceStore>,
    marketplace_runtime: MarketplaceRuntimeInfo,
    announce_addrs: Vec<String>,
) -> Router {
    let fallback_http_peers = Arc::new(fallback_http_peer_urls());
    let fallback_http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let state = ApiState {
        block_store,
        metrics,
        peer_id,
        botg,
        keypair,
        listen_addrs,
        announce_addrs,
        fallback_http_peers,
        fallback_http_client,
        ipfs_cluster_pins: Arc::new(AsyncRwLock::new(HashMap::new())),
        citadel_node,
        marketplace,
        marketplace_runtime,
    };

    Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_endpoint))
        .route("/api/v1/blocks", post(store_block))
        .route("/api/v1/blocks/{cid}", get(get_block))
        // Archivist-compatible endpoints
        .route(
            "/api/archivist/v1/data",
            get(archivist_list_data).post(archivist_upload),
        )
        .route(
            "/api/archivist/v1/data/raw",
            post(archivist_upload_raw_block),
        )
        .route(
            "/api/archivist/v1/data/{cid}",
            get(archivist_download_local).delete(archivist_delete),
        )
        .route("/api/archivist/v1/data/{cid}/exists", get(archivist_exists))
        .route(
            "/api/archivist/v1/data/{cid}/network",
            post(archivist_download_network_manifest),
        )
        .route(
            "/api/archivist/v1/data/{cid}/network/stream",
            get(archivist_download),
        )
        .route(
            "/api/archivist/v1/data/{cid}/network/manifest",
            get(archivist_download_network_manifest),
        )
        .route("/api/archivist/v1/space", get(archivist_space))
        .route("/api/archivist/v1/peer-id", get(peer_id_endpoint))
        .route("/api/archivist/v1/peerid", get(peer_id_endpoint))
        .route("/api/archivist/v1/stats", get(archivist_stats))
        .route("/api/archivist/v1/spr", get(spr_endpoint))
        // IPFS Cluster-style compatibility endpoints
        .route("/api/ipfs-cluster/v1/pins", get(ipfs_cluster_list_pins))
        .route(
            "/api/ipfs-cluster/v1/pins/{cid}",
            get(ipfs_cluster_get_pin)
                .post(ipfs_cluster_pin_cid)
                .delete(ipfs_cluster_unpin_cid),
        )
        .route(
            "/api/ipfs-cluster/v1/pins/{cid}/recover",
            post(ipfs_cluster_recover_pin),
        )
        .route(
            "/api/ipfs-cluster/v1/allocations",
            get(ipfs_cluster_list_pins),
        )
        .route(
            "/api/ipfs-cluster/v1/allocations/{cid}",
            get(ipfs_cluster_get_pin),
        )
        .route("/api/citadel/v1/status", get(citadel_status))
        .route("/api/citadel/v1/view/{site_id}", get(citadel_view))
        .route("/api/citadel/v1/follow/{site_id}", post(citadel_follow))
        .route("/api/citadel/v1/unfollow/{site_id}", post(citadel_unfollow))
        .route(
            "/api/citadel/v1/content/{content_slot}/{present}",
            post(citadel_content),
        )
        .route("/api/citadel/v1/simulate", post(citadel_simulate))
        .route("/api/citadel/v1/sync/pull", post(citadel_sync_pull))
        .route("/api/citadel/v1/sync/push", post(citadel_sync_push))
        .route(
            "/api/archivist/v1/connect/{peer_id}",
            get(connect_not_supported),
        )
        .route("/api/archivist/v1/sales/slots", get(list_sales_slots))
        .route(
            "/api/archivist/v1/sales/slots/{slot_id}",
            get(get_sales_slot),
        )
        .route(
            "/api/archivist/v1/sales/availability",
            get(get_sales_availability).post(set_sales_availability),
        )
        .route(
            "/api/archivist/v1/storage/request/{cid}",
            post(create_storage_request),
        )
        .route(
            "/api/archivist/v1/storage/purchases",
            get(list_storage_purchases),
        )
        .route(
            "/api/archivist/v1/storage/purchases/{id}",
            get(get_storage_purchase),
        )
        .route("/api/archivist/v1/debug/info", get(debug_info_endpoint))
        .route(
            "/api/archivist/v1/debug/chronicles/loglevel",
            post(loglevel_not_supported),
        )
        .route(
            "/api/archivist/v1/debug/peer/{peer_id}",
            get(debug_peer_not_supported),
        )
        .route(
            "/api/archivist/v1/debug/testing/option/{key}/{value}",
            post(debug_testing_not_supported),
        )
        .with_state(state)
        // Axum applies a 2 MiB default body limit for `Bytes` extractors.
        // Disable it so upload size is constrained only by host resources.
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http())
}

/// Health check endpoint
async fn health_check(State(state): State<ApiState>) -> impl IntoResponse {
    let stats = state.block_store.stats().await;

    Json(HealthResponse {
        status: "ok".to_string(),
        block_count: stats.block_count,
        total_bytes: stats.total_size,
    })
}

/// Prometheus metrics endpoint
async fn metrics_endpoint(State(state): State<ApiState>) -> impl IntoResponse {
    let stats = state.block_store.stats().await;

    // Generate Prometheus-compatible metrics using the Metrics module
    let metrics = state
        .metrics
        .to_prometheus(stats.block_count, stats.total_size);

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        metrics,
    )
}

async fn citadel_status(State(state): State<ApiState>) -> impl IntoResponse {
    let Some(node) = state.citadel_node else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "enabled": false,
                "error": "citadel mode not enabled"
            })),
        );
    };

    let node = node.read().await;
    (
        StatusCode::OK,
        Json(json!({
            "enabled": true,
            "status": node.status(),
        })),
    )
}

async fn citadel_view(
    State(state): State<ApiState>,
    Path(site_id): Path<u64>,
) -> impl IntoResponse {
    let Some(node) = state.citadel_node else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "citadel mode not enabled"
            })),
        );
    };

    let node = node.read().await;
    let reachable = node.graph.reachable_sites(site_id);
    let visible = node.graph.visible_content(site_id);
    let digest = node.view_digest_hex(site_id);
    (
        StatusCode::OK,
        Json(json!({
            "site_id": site_id,
            "reachable_sites": reachable.len(),
            "visible_items": visible.len(),
            "view_digest": digest,
        })),
    )
}

async fn citadel_follow(
    State(state): State<ApiState>,
    Path(site_id): Path<u64>,
) -> impl IntoResponse {
    let Some(node) = state.citadel_node else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "citadel mode not enabled"
            })),
        );
    };
    let mut node = node.write().await;
    let op = node.emit_local_follow(site_id, true);
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "origin": op.origin,
            "counter": op.counter,
            "target_site_id": site_id,
            "enabled": true,
        })),
    )
}

async fn citadel_unfollow(
    State(state): State<ApiState>,
    Path(site_id): Path<u64>,
) -> impl IntoResponse {
    let Some(node) = state.citadel_node else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "citadel mode not enabled"
            })),
        );
    };
    let mut node = node.write().await;
    let op = node.emit_local_follow(site_id, false);
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "origin": op.origin,
            "counter": op.counter,
            "target_site_id": site_id,
            "enabled": false,
        })),
    )
}

fn parse_boolish(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

async fn citadel_content(
    State(state): State<ApiState>,
    Path((content_slot, present_raw)): Path<(u64, String)>,
) -> impl IntoResponse {
    let Some(node) = state.citadel_node else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "citadel mode not enabled"
            })),
        );
    };
    let Some(present) = parse_boolish(&present_raw) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "present must be one of true/false/1/0/yes/no/on/off"
            })),
        );
    };

    let mut node = node.write().await;
    let op = node.emit_local_content(content_slot, present);
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "origin": op.origin,
            "counter": op.counter,
            "content_slot": content_slot,
            "present": present,
        })),
    )
}

async fn citadel_simulate(
    State(state): State<ApiState>,
    Json(mut cfg): Json<DefederationSimulationConfig>,
) -> impl IntoResponse {
    if let Some(node) = state.citadel_node {
        let node = node.read().await;
        cfg.guard.base_pow_bits = node.guard_cfg.base_pow_bits;
        cfg.guard.trusted_pow_bits = node.guard_cfg.trusted_pow_bits;
        cfg.guard.max_ops_per_origin_per_round = node.guard_cfg.max_ops_per_origin_per_round;
        cfg.guard.max_new_origins_per_host_per_round =
            node.guard_cfg.max_new_origins_per_host_per_round;
        cfg.guard.max_pending_per_origin = node.guard_cfg.max_pending_per_origin;
        cfg.idle_gate.max_idle_bytes_per_sec = node.idle_bandwidth_bytes_per_sec;
    }

    let result = tokio::task::spawn_blocking(move || run_defederation_simulation(&cfg)).await;
    match result {
        Ok(out) => (StatusCode::OK, Json(json!({ "ok": true, "result": out }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "ok": false,
                "error": format!("simulation worker failed: {}", e)
            })),
        ),
    }
}

async fn citadel_sync_pull(
    State(state): State<ApiState>,
    Json(req): Json<CitadelSyncPullRequest>,
) -> Response {
    let Some(node) = state.citadel_node else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "citadel mode not enabled"
            })),
        )
            .into_response();
    };

    let node = node.read().await;
    let accepted = 0usize;
    let max_ops = req.max_ops.unwrap_or(256).clamp(1, 4096);
    let ops = node.missing_ops_for_frontier(&req.frontier, max_ops);
    let provided_ops = ops.len();
    let frontier = node.frontier_snapshot();
    let status = node.status();
    let response = CitadelSyncPullResponse {
        node_id: status.node_id,
        round: req.round,
        accepted_local_ops: accepted,
        provided_ops,
        frontier,
        ops,
        status,
    };
    (StatusCode::OK, Json(response)).into_response()
}

async fn citadel_sync_push(
    State(state): State<ApiState>,
    Json(req): Json<CitadelSyncPushRequest>,
) -> Response {
    let Some(node) = state.citadel_node else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "citadel mode not enabled"
            })),
        )
            .into_response();
    };

    let mut node = node.write().await;
    let accepted_ops = node.ingest_batch(req.round, req.ops);
    let max_ops = req.max_ops.unwrap_or(256).clamp(1, 4096);
    let ops = node.missing_ops_for_frontier(&req.frontier, max_ops);
    let provided_ops = ops.len();
    let frontier = node.frontier_snapshot();
    let status = node.status();
    let response = CitadelSyncPushResponse {
        node_id: status.node_id,
        round: req.round,
        accepted_ops,
        provided_ops,
        frontier,
        ops,
        status,
    };
    (StatusCode::OK, Json(response)).into_response()
}

/// Store a block (POST /api/v1/blocks)
async fn store_block(
    State(state): State<ApiState>,
    body: bytes::Bytes,
) -> Result<Json<StoreBlockResponse>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::BadRequest("Empty block data".to_string()));
    }

    info!("API: Storing block ({} bytes)", body.len());

    // Create block from data
    let block = Block::new(body.to_vec())
        .map_err(|e| ApiError::Internal(format!("Failed to create block: {}", e)))?;

    let cid = block.cid;
    let size = block.size();

    // Store in BlockStore
    state
        .block_store
        .put(block)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to store block: {}", e)))?;

    info!("API: Stored block {} ({} bytes)", cid, size);

    Ok(Json(StoreBlockResponse {
        cid: cid_to_string(&cid),
        size,
    }))
}

/// Retrieve a block (GET /api/v1/blocks/:cid)
/// Supports HTTP Range headers for partial content retrieval
async fn get_block(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    info!("API: Retrieving block {}", cid_str);

    // Parse CID
    let cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;

    // Get block from store
    let block = state.block_store.get(&cid).await.map_err(|e| match e {
        StorageError::BlockNotFound(_) => ApiError::NotFound(cid_str.clone()),
        _ => ApiError::Internal(format!("Failed to retrieve block: {}", e)),
    })?;

    let total_size = block.size();

    // Check for Range header (HTTP partial content request)
    if let Some(range_header) = headers.get("range") {
        if let Ok(range_str) = range_header.to_str() {
            if let Some(range) = parse_range_header(range_str, total_size) {
                let (start, end) = range;
                let range_data = &block.data[start..end];

                info!(
                    "API: Serving range [{}, {}) of block {} ({} bytes of {})",
                    start,
                    end,
                    cid_str,
                    range_data.len(),
                    total_size
                );

                // Return 206 Partial Content with Content-Range header
                let response = Json(GetBlockResponse {
                    cid: cid_str,
                    data: base64::prelude::BASE64_STANDARD.encode(range_data),
                    size: range_data.len(),
                });

                let mut resp = response.into_response();
                *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                resp.headers_mut().insert(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end - 1, total_size)
                        .parse()
                        .unwrap(),
                );
                resp.headers_mut()
                    .insert("accept-ranges", "bytes".parse().unwrap());

                return Ok(resp);
            }
        }
    }

    // No range request - return full block
    info!(
        "API: Retrieved full block {} ({} bytes)",
        cid_str, total_size
    );

    let response = Json(GetBlockResponse {
        cid: cid_str,
        data: base64::prelude::BASE64_STANDARD.encode(&block.data),
        size: total_size,
    });

    let mut resp = response.into_response();
    resp.headers_mut()
        .insert("accept-ranges", "bytes".parse().unwrap());

    Ok(resp)
}

/// Parse HTTP Range header (e.g., "bytes=1024-2047")
/// Returns (start, end) where end is exclusive
fn parse_range_header(range_str: &str, total_size: usize) -> Option<(usize, usize)> {
    // Range header format: "bytes=start-end"
    let range_str = range_str.trim().strip_prefix("bytes=")?;

    // Split on '-'
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start: usize = parts[0].parse().ok()?;
    let end: usize = if parts[1].is_empty() {
        total_size
    } else {
        // HTTP Range header end is inclusive, convert to exclusive
        parts[1].parse::<usize>().ok()? + 1
    };

    // Validate range
    if start >= total_size || start >= end {
        return None;
    }

    let end = std::cmp::min(end, total_size);

    Some((start, end))
}

fn manifest_to_response(cid: &Cid, manifest: &Manifest) -> DataItemResponse {
    DataItemResponse {
        cid: cid_to_string(cid),
        manifest: ManifestItemResponse {
            tree_cid: cid_to_string(&manifest.tree_cid),
            dataset_size: manifest.dataset_size,
            block_size: manifest.block_size,
            is_protected: manifest.is_protected(),
            filename: manifest.filename.clone(),
            mimetype: manifest.mimetype.clone(),
        },
    }
}

fn metadata_cid_from_manifest(manifest: &Manifest) -> Option<Cid> {
    manifest
        .filename
        .as_ref()
        .and_then(|s| s.strip_prefix("metadata:"))
        .and_then(|s| s.parse().ok())
}

async fn retrieve_local_cid_data(
    state: &ApiState,
    cid: &Cid,
    cid_str: &str,
) -> Result<Vec<u8>, ApiError> {
    let block = state.block_store.get(cid).await.map_err(|e| match e {
        StorageError::BlockNotFound(_) => ApiError::NotFound(cid_str.to_string()),
        _ => ApiError::Internal(format!("Failed to retrieve block: {}", e)),
    })?;

    // Manifests need local block reconstruction.
    if cid.codec() == 0xcd01 {
        let manifest = Manifest::from_block(&block)
            .map_err(|e| ApiError::Internal(format!("Failed to decode manifest: {}", e)))?;

        let metadata_cid = metadata_cid_from_manifest(&manifest).ok_or_else(|| {
            ApiError::Internal("Manifest missing metadata CID in filename field".to_string())
        })?;

        let tree_metadata_block =
            state
                .block_store
                .get(&metadata_cid)
                .await
                .map_err(|e| match e {
                    StorageError::BlockNotFound(_) => {
                        ApiError::NotFound(format!("metadata for manifest {} not found", cid_str))
                    }
                    _ => ApiError::Internal(format!(
                        "Failed to fetch tree metadata {}: {}",
                        metadata_cid, e
                    )),
                })?;

        let block_cids =
            ArchivistTree::deserialize_block_list(&tree_metadata_block.data).map_err(|e| {
                ApiError::Internal(format!("Failed to deserialize tree metadata: {}", e))
            })?;

        if block_cids.len() != manifest.blocks_count() {
            return Err(ApiError::Internal(format!(
                "Block count mismatch: tree has {} blocks but manifest expects {}",
                block_cids.len(),
                manifest.blocks_count()
            )));
        }

        let mut data: Vec<u8> = Vec::with_capacity(manifest.dataset_size as usize);
        for block_cid in &block_cids {
            let b = state
                .block_store
                .get(block_cid)
                .await
                .map_err(|e| match e {
                    StorageError::BlockNotFound(_) => {
                        ApiError::NotFound(format!("manifest block {} not found", block_cid))
                    }
                    _ => ApiError::Internal(format!("Failed to fetch block {}: {}", block_cid, e)),
                })?;
            data.extend_from_slice(&b.data);
        }

        if data.len() != manifest.dataset_size as usize {
            return Err(ApiError::Internal(format!(
                "Data size mismatch: assembled {} bytes but manifest expects {} bytes",
                data.len(),
                manifest.dataset_size
            )));
        }

        Ok(data)
    } else {
        Ok(block.data)
    }
}

/// Archivist list data endpoint (GET /api/archivist/v1/data)
async fn archivist_list_data(State(state): State<ApiState>) -> impl IntoResponse {
    let mut content = Vec::new();

    for cid in state.block_store.list_cids().await {
        if cid.codec() != 0xcd01 {
            continue;
        }

        if let Ok(block) = state.block_store.get(&cid).await {
            if let Ok(manifest) = Manifest::from_block(&block) {
                content.push(manifest_to_response(&cid, &manifest));
            }
        }
    }

    Json(DataListResponse { content })
}

/// Archivist local download endpoint (GET /api/archivist/v1/data/:cid)
async fn archivist_download_local(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<Vec<u8>, ApiError> {
    let cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;

    retrieve_local_cid_data(&state, &cid, &cid_str).await
}

/// Archivist delete endpoint (DELETE /api/archivist/v1/data/:cid)
async fn archivist_delete(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<StatusCode, ApiError> {
    let cid: Cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;

    if cid.codec() == 0xcd01 {
        if let Ok(manifest_block) = state.block_store.get(&cid).await {
            if let Ok(manifest) = Manifest::from_block(&manifest_block) {
                if let Some(metadata_cid) = metadata_cid_from_manifest(&manifest) {
                    if let Ok(metadata_block) = state.block_store.get(&metadata_cid).await {
                        if let Ok(block_cids) =
                            ArchivistTree::deserialize_block_list(&metadata_block.data)
                        {
                            for block_cid in block_cids {
                                let _ = state.block_store.delete(&block_cid).await;
                            }
                        }
                    }
                    let _ = state.block_store.delete(&metadata_cid).await;
                }
            }
        }
    }

    // Deleting non-existing data is idempotent and still returns 204.
    let _ = state.block_store.delete(&cid).await;
    Ok(StatusCode::NO_CONTENT)
}

/// Archivist exists endpoint (GET /api/archivist/v1/data/:cid/exists)
async fn archivist_exists(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let cid: Cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;
    let exists = state.block_store.has(&cid).await;
    Ok(Json(serde_json::json!({
        "cid": cid_str,
        "exists": exists
    })))
}

async fn set_ipfs_cluster_pin_status(
    state: &ApiState,
    cid_str: &str,
    status: &str,
    error: Option<String>,
) {
    let mut pins = state.ipfs_cluster_pins.write().await;
    if let Some(record) = pins.get_mut(cid_str) {
        record.status = status.to_string();
        record.error = error;
    }
}

async fn ensure_pin_locally(state: &ApiState, cid_str: &str) -> Result<(), String> {
    let cid: Cid = cid_str
        .parse()
        .map_err(|e| format!("invalid cid {}: {}", cid_str, e))?;
    if state.block_store.has(&cid).await {
        return Ok(());
    }
    let _ = fetch_cid_from_peers(state, &cid, cid_str)
        .await
        .map_err(|e| format!("{:?}", e))?;
    if state.block_store.has(&cid).await {
        Ok(())
    } else {
        Err("content fetched but not persisted under requested CID".to_string())
    }
}

/// IPFS Cluster compatible: pin a CID (POST /api/ipfs-cluster/v1/pins/:cid)
async fn ipfs_cluster_pin_cid(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
    body: Option<Json<IpfsClusterPinRequest>>,
) -> Result<Json<IpfsClusterPinRecord>, ApiError> {
    let _: Cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;
    let request = body.map_or_else(IpfsClusterPinRequest::default, |b| b.0);
    let record = IpfsClusterPinRecord::from_request(cid_str.clone(), request);

    {
        let mut pins = state.ipfs_cluster_pins.write().await;
        pins.insert(cid_str.clone(), record.clone());
    }

    let state_bg = state.clone();
    let cid_bg = cid_str.clone();
    tokio::spawn(async move {
        set_ipfs_cluster_pin_status(&state_bg, &cid_bg, "pinning", None).await;
        match ensure_pin_locally(&state_bg, &cid_bg).await {
            Ok(()) => set_ipfs_cluster_pin_status(&state_bg, &cid_bg, "pinned", None).await,
            Err(e) => set_ipfs_cluster_pin_status(&state_bg, &cid_bg, "pin_error", Some(e)).await,
        }
    });

    Ok(Json(record))
}

/// IPFS Cluster compatible: get pin info (GET /api/ipfs-cluster/v1/pins/:cid)
async fn ipfs_cluster_get_pin(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<Json<IpfsClusterPinRecord>, ApiError> {
    let pins = state.ipfs_cluster_pins.read().await;
    let record = pins
        .get(&cid_str)
        .cloned()
        .ok_or_else(|| ApiError::NotFound(cid_str.clone()))?;
    Ok(Json(record))
}

/// IPFS Cluster compatible: list pins (GET /api/ipfs-cluster/v1/pins)
async fn ipfs_cluster_list_pins(
    State(state): State<ApiState>,
) -> Result<Json<Vec<IpfsClusterPinRecord>>, ApiError> {
    let pins = state.ipfs_cluster_pins.read().await;
    let list = pins.values().cloned().collect::<Vec<_>>();
    Ok(Json(list))
}

/// IPFS Cluster compatible: unpin (DELETE /api/ipfs-cluster/v1/pins/:cid)
async fn ipfs_cluster_unpin_cid(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<StatusCode, ApiError> {
    let cid: Cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;
    {
        let mut pins = state.ipfs_cluster_pins.write().await;
        pins.remove(&cid_str);
    }
    let _ = state.block_store.delete(&cid).await;
    Ok(StatusCode::ACCEPTED)
}

/// IPFS Cluster compatible: recover pin (POST /api/ipfs-cluster/v1/pins/:cid/recover)
async fn ipfs_cluster_recover_pin(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<Json<IpfsClusterPinRecord>, ApiError> {
    {
        let mut pins = state.ipfs_cluster_pins.write().await;
        let entry = pins.entry(cid_str.clone()).or_insert_with(|| {
            IpfsClusterPinRecord::from_request(cid_str.clone(), Default::default())
        });
        entry.status = "queued".to_string();
        entry.error = None;
    }
    let state_bg = state.clone();
    let cid_bg = cid_str.clone();
    tokio::spawn(async move {
        set_ipfs_cluster_pin_status(&state_bg, &cid_bg, "pinning", None).await;
        match ensure_pin_locally(&state_bg, &cid_bg).await {
            Ok(()) => set_ipfs_cluster_pin_status(&state_bg, &cid_bg, "pinned", None).await,
            Err(e) => set_ipfs_cluster_pin_status(&state_bg, &cid_bg, "pin_error", Some(e)).await,
        }
    });

    let pins = state.ipfs_cluster_pins.read().await;
    let record = pins
        .get(&cid_str)
        .cloned()
        .ok_or_else(|| ApiError::NotFound(cid_str))?;
    Ok(Json(record))
}

/// Archivist network manifest endpoint
/// (GET/POST /api/archivist/v1/data/:cid/network/manifest, /network)
async fn archivist_download_network_manifest(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<Json<DataItemResponse>, ApiError> {
    let cid: Cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;

    let block = state.block_store.get(&cid).await.map_err(|e| match e {
        StorageError::BlockNotFound(_) => ApiError::NotFound(cid_str.clone()),
        _ => ApiError::Internal(format!("Failed to retrieve manifest: {}", e)),
    })?;

    let manifest = Manifest::from_block(&block)
        .map_err(|e| ApiError::Internal(format!("Failed to decode manifest: {}", e)))?;

    Ok(Json(manifest_to_response(&cid, &manifest)))
}

/// Archivist space endpoint (GET /api/archivist/v1/space)
async fn archivist_space(State(state): State<ApiState>) -> impl IntoResponse {
    let stats = state.block_store.stats().await;
    let quota_reserved_bytes = if let Some(store) = &state.marketplace {
        store
            .list_active_slots()
            .await
            .into_iter()
            .map(|slot| slot.request.ask.slot_size as usize)
            .sum()
    } else {
        0
    };

    Json(SpaceResponse {
        total_blocks: stats.block_count,
        quota_max_bytes: state.marketplace_runtime.quota_max_bytes,
        quota_used_bytes: stats.total_size,
        quota_reserved_bytes,
    })
}

/// Placeholder until libp2p connect orchestration is exposed in API state.
async fn connect_not_supported() -> Result<StatusCode, ApiError> {
    Err(ApiError::NotImplemented(
        "Peer connect API is not wired yet in neverust runtime".to_string(),
    ))
}

fn marketplace_store(state: &ApiState) -> Result<MarketplaceStore, ApiError> {
    state
        .marketplace
        .clone()
        .ok_or_else(|| ApiError::ServiceUnavailable("Persistence is not enabled".to_string()))
}

async fn get_sales_availability(
    State(state): State<ApiState>,
) -> Result<Json<SaleAvailabilityInput>, ApiError> {
    let store = marketplace_store(&state)?;
    let availability = store
        .availability()
        .await
        .ok_or_else(|| ApiError::NotFound("sales availability not configured".to_string()))?;
    Ok(Json(availability.to_input()))
}

async fn set_sales_availability(
    State(state): State<ApiState>,
    Json(input): Json<SaleAvailabilityInput>,
) -> Result<StatusCode, ApiError> {
    let store = marketplace_store(&state)?;
    store
        .set_availability(input, state.marketplace_runtime.quota_max_bytes as u64)
        .await
        .map_err(ApiError::Unprocessable)?;
    Ok(StatusCode::CREATED)
}

async fn list_sales_slots(
    State(state): State<ApiState>,
) -> Result<Json<Vec<ActiveSlotResponse>>, ApiError> {
    let store = marketplace_store(&state)?;
    Ok(Json(store.list_active_slots().await))
}

async fn get_sales_slot(
    State(state): State<ApiState>,
    Path(slot_id): Path<String>,
) -> Result<Json<SalesSlotResponse>, ApiError> {
    let store = marketplace_store(&state)?;
    let slot = store
        .get_slot(&slot_id)
        .await
        .ok_or_else(|| ApiError::NotFound(slot_id.clone()))?;
    Ok(Json(slot))
}

async fn list_storage_purchases(
    State(state): State<ApiState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let store = marketplace_store(&state)?;
    Ok(Json(store.list_purchase_ids().await))
}

async fn get_storage_purchase(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<PurchaseResponse>, ApiError> {
    let store = marketplace_store(&state)?;
    let purchase = store
        .get_purchase(&id)
        .await
        .ok_or_else(|| ApiError::NotFound(id.clone()))?;
    Ok(Json(PurchaseResponse {
        request_id: purchase.request_id,
        request: Some(purchase.request),
        state: purchase.state.as_api_str().to_string(),
        error: purchase.error,
    }))
}

async fn create_storage_request(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
    Json(request): Json<StorageRequestInput>,
) -> Result<String, ApiError> {
    let store = marketplace_store(&state)?;
    let cid: Cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;
    let slot_size = marketplace_slot_size(&state, &cid).await?;
    store
        .reserve_request(
            cid_to_string(&cid),
            request,
            slot_size,
            state.marketplace_runtime.eth_account.clone(),
        )
        .await
        .map_err(ApiError::Unprocessable)
}

async fn marketplace_slot_size(state: &ApiState, cid: &Cid) -> Result<u64, ApiError> {
    match state.block_store.get(cid).await {
        Ok(block) => {
            if let Ok(manifest) = Manifest::from_block(&block) {
                Ok(manifest.dataset_size)
            } else {
                Ok(block.data.len() as u64)
            }
        }
        Err(StorageError::BlockNotFound(_)) => Err(ApiError::NotFound(cid.to_string())),
        Err(err) => Err(ApiError::Internal(format!(
            "Failed to inspect marketplace request content: {}",
            err
        ))),
    }
}

/// Lightweight debug info endpoint for compatibility.
async fn debug_info_endpoint(State(state): State<ApiState>) -> impl IntoResponse {
    let addrs = state
        .listen_addrs
        .read()
        .map(|v| v.iter().map(ToString::to_string).collect::<Vec<_>>())
        .unwrap_or_default();
    let marketplace_runtime = state.marketplace_runtime.clone();

    Json(serde_json::json!({
        "id": state.peer_id,
        "addrs": addrs,
        "repo": "unknown",
        "spr": "",
        "announceAddresses": state.announce_addrs,
        "ethAddress": marketplace_runtime.eth_account,
        "table": {"localNode": serde_json::Value::Null, "nodes": []},
        "archivist": {
            "version": env!("CARGO_PKG_VERSION"),
            "revision": "unknown",
            "contracts": marketplace_runtime.contracts_addresses,
            "marketplaceAddress": marketplace_runtime.marketplace_address,
            "ethProvider": marketplace_runtime.eth_provider,
            "persistence": marketplace_runtime.persistence_enabled,
            "validator": marketplace_runtime.validator,
            "prover": marketplace_runtime.prover
        }
    }))
}

async fn loglevel_not_supported() -> Result<StatusCode, ApiError> {
    Err(ApiError::NotImplemented(
        "Runtime log level updates are not implemented".to_string(),
    ))
}

async fn debug_peer_not_supported() -> Result<StatusCode, ApiError> {
    Err(ApiError::NotImplemented(
        "Debug peer lookup is not implemented".to_string(),
    ))
}

async fn debug_testing_not_supported() -> Result<StatusCode, ApiError> {
    Err(ApiError::NotImplemented(
        "System testing options are not implemented".to_string(),
    ))
}

/// Archivist-compatible upload endpoint (POST /api/archivist/v1/data)
/// Returns manifest CID as plain text
async fn archivist_upload(State(state): State<ApiState>, body: Body) -> Result<String, ApiError> {
    use futures::StreamExt;
    use std::collections::HashSet;

    let block_size = upload_block_size();
    let commit_batch_blocks = upload_commit_batch_blocks(block_size);
    let dedupe_blocks = upload_dedupe_blocks();
    info!(
        "Archivist API: Streaming upload started (block_size={}, commit_batch_blocks={}, dedupe_blocks={})",
        block_size, commit_batch_blocks, dedupe_blocks
    );

    // Stream chunks from the request body and store fixed-size blocks immediately.
    // This keeps memory bounded instead of buffering the full upload in RAM.
    let mut stream = body.into_data_stream();
    let mut current_block = Vec::with_capacity(block_size);
    let mut dataset_size: u64 = 0;
    let mut block_cids = Vec::new();
    let mut block_cid_slots: Vec<Option<Cid>> = Vec::new();
    let mut raw_batch: Vec<Vec<u8>> = Vec::with_capacity(commit_batch_blocks);
    let max_inflight_batches = upload_inflight_batches();
    let mut inflight_no_dedupe = futures::stream::FuturesUnordered::new();
    let mut next_batch_start_idx: usize = 0;
    let mut seen_cids = if dedupe_blocks {
        Some(HashSet::<Cid>::new())
    } else {
        None
    };

    while let Some(next) = stream.next().await {
        let chunk =
            next.map_err(|e| ApiError::Internal(format!("Failed to read body stream: {}", e)))?;
        if chunk.is_empty() {
            continue;
        }

        dataset_size = dataset_size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| ApiError::BadRequest("Upload too large".to_string()))?;
        let mut rem = chunk.as_ref();
        while !rem.is_empty() {
            let need = block_size.saturating_sub(current_block.len());
            let take = need.min(rem.len());
            current_block.extend_from_slice(&rem[..take]);
            rem = &rem[take..];

            if current_block.len() == block_size {
                raw_batch.push(std::mem::replace(
                    &mut current_block,
                    Vec::with_capacity(block_size),
                ));
                if !dedupe_blocks {
                    block_cid_slots.push(None);
                }
            }
            if raw_batch.len() >= commit_batch_blocks {
                if dedupe_blocks {
                    flush_upload_raw_batch(&state, &mut raw_batch, &mut seen_cids, &mut block_cids)
                        .await?;
                } else {
                    let start_idx = next_batch_start_idx;
                    let expected_len = raw_batch.len();
                    next_batch_start_idx = next_batch_start_idx.saturating_add(expected_len);
                    let batch = std::mem::take(&mut raw_batch);
                    let block_store = Arc::clone(&state.block_store);
                    inflight_no_dedupe.push(tokio::spawn(async move {
                        let cids = process_upload_raw_batch_no_dedupe(block_store, batch).await?;
                        Ok::<(usize, usize, Vec<Cid>), ApiError>((start_idx, expected_len, cids))
                    }));

                    while inflight_no_dedupe.len() > max_inflight_batches {
                        let completed = inflight_no_dedupe.next().await.ok_or_else(|| {
                            ApiError::Internal("Upload pipeline ended unexpectedly".to_string())
                        })?;
                        let (start_idx, expected_len, cids) = completed.map_err(|e| {
                            ApiError::Internal(format!("Upload batch task failed: {}", e))
                        })??;
                        if cids.len() != expected_len {
                            return Err(ApiError::Internal(format!(
                                "Upload batch CID count mismatch: expected {}, got {}",
                                expected_len,
                                cids.len()
                            )));
                        }
                        for (offset, cid) in cids.into_iter().enumerate() {
                            let pos = start_idx + offset;
                            if pos >= block_cid_slots.len() {
                                return Err(ApiError::Internal(format!(
                                    "Upload CID slot out of bounds: {} >= {}",
                                    pos,
                                    block_cid_slots.len()
                                )));
                            }
                            block_cid_slots[pos] = Some(cid);
                        }
                    }
                }
            }
        }
    }

    if !current_block.is_empty() {
        raw_batch.push(current_block);
        if !dedupe_blocks {
            block_cid_slots.push(None);
        }
    }

    if dedupe_blocks {
        flush_upload_raw_batch(&state, &mut raw_batch, &mut seen_cids, &mut block_cids).await?;
    } else {
        if !raw_batch.is_empty() {
            let start_idx = next_batch_start_idx;
            let expected_len = raw_batch.len();
            let block_store = Arc::clone(&state.block_store);
            let batch = std::mem::take(&mut raw_batch);
            inflight_no_dedupe.push(tokio::spawn(async move {
                let cids = process_upload_raw_batch_no_dedupe(block_store, batch).await?;
                Ok::<(usize, usize, Vec<Cid>), ApiError>((start_idx, expected_len, cids))
            }));
        }
        while let Some(completed) = inflight_no_dedupe.next().await {
            let (start_idx, expected_len, cids) = completed
                .map_err(|e| ApiError::Internal(format!("Upload batch task failed: {}", e)))??;
            if cids.len() != expected_len {
                return Err(ApiError::Internal(format!(
                    "Upload batch CID count mismatch: expected {}, got {}",
                    expected_len,
                    cids.len()
                )));
            }
            for (offset, cid) in cids.into_iter().enumerate() {
                let pos = start_idx + offset;
                if pos >= block_cid_slots.len() {
                    return Err(ApiError::Internal(format!(
                        "Upload CID slot out of bounds: {} >= {}",
                        pos,
                        block_cid_slots.len()
                    )));
                }
                block_cid_slots[pos] = Some(cid);
            }
        }
        block_cids = block_cid_slots
            .into_iter()
            .map(|maybe| {
                maybe.ok_or_else(|| {
                    ApiError::Internal("Upload pipeline missing CID slot".to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
    }

    if dataset_size == 0 {
        return Err(ApiError::BadRequest("Empty data".to_string()));
    }

    info!(
        "Archivist API: Stored {} blocks for dataset ({} bytes)",
        block_cids.len(),
        dataset_size
    );

    // Step 2: Build Archivist tree from block CIDs
    let tree = ArchivistTree::new(block_cids)
        .map_err(|e| ApiError::Internal(format!("Failed to create tree: {}", e)))?;

    let tree_cid = tree
        .root_cid()
        .map_err(|e| ApiError::Internal(format!("Failed to compute tree CID: {}", e)))?;

    info!("Archivist API: Built tree with root CID {}", tree_cid);

    // Step 2.5: Store the tree's block list as a metadata block
    // We serialize the block CIDs and store them with a predictable key derived from tree_cid
    // This allows us to reconstruct the block CIDs during download
    let tree_block_list = tree.serialize_block_list();

    // Create a block from the serialized data
    let tree_metadata_block = Block::new_sha256(tree_block_list)
        .map_err(|e| ApiError::Internal(format!("Failed to create tree metadata block: {}", e)))?;

    let tree_metadata_cid = tree_metadata_block.cid;

    state
        .block_store
        .put(tree_metadata_block)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to store tree metadata: {}", e)))?;

    info!(
        "Archivist API: Stored tree metadata {} ({} block CIDs) for tree {}",
        tree_metadata_cid,
        tree.block_cids().len(),
        tree_cid
    );

    // Step 3: Create manifest
    // Store metadata CID in filename field for retrieval during download
    // Format: "metadata:<cid>"
    let manifest = Manifest::new(
        tree_cid,
        block_size as u64,
        dataset_size,
        None,                                            // codec (uses default 0xcd02)
        Some(SHA256_CODEC),                              // hcodec (SHA2-256)
        None,                                            // version (uses default 1)
        Some(format!("metadata:{}", tree_metadata_cid)), // filename (stores metadata CID)
        None,                                            // mimetype
    );

    info!(
        "Archivist API: Created manifest for tree {} ({} blocks, {} bytes)",
        tree_cid,
        manifest.blocks_count(),
        dataset_size
    );

    // Step 4: Encode manifest as block (uses codec 0xcd01)
    let manifest_block = manifest
        .to_block()
        .map_err(|e| ApiError::Internal(format!("Failed to encode manifest: {}", e)))?;

    let manifest_cid = manifest_block.cid;

    // Step 5: Store manifest block
    state
        .block_store
        .put(manifest_block)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to store manifest: {}", e)))?;

    info!(
        "Archivist API: Uploaded manifest {} (tree: {}, blocks: {}, size: {})",
        manifest_cid,
        tree_cid,
        manifest.blocks_count(),
        dataset_size
    );

    // Return manifest CID as plain text (Archivist format with base58btc encoding)
    Ok(cid_to_string(&manifest_cid))
}

/// Fast path upload endpoint (POST /api/archivist/v1/data/raw)
/// Stores request body as a single block and returns block CID as plain text.
async fn archivist_upload_raw_block(
    State(state): State<ApiState>,
    body: Body,
) -> Result<String, ApiError> {
    use futures::StreamExt;

    let mut stream = body.into_data_stream();
    let mut data = Vec::new();
    while let Some(next) = stream.next().await {
        let chunk =
            next.map_err(|e| ApiError::Internal(format!("Failed to read body stream: {}", e)))?;
        if !chunk.is_empty() {
            data.extend_from_slice(&chunk);
        }
    }

    if data.is_empty() {
        return Err(ApiError::BadRequest("Empty data".to_string()));
    }

    let block = Block::new_sha256(data)
        .map_err(|e| ApiError::Internal(format!("Failed to create block: {}", e)))?;
    let cid = block.cid;
    state
        .block_store
        .put(block)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to store block: {}", e)))?;
    Ok(cid_to_string(&cid))
}

/// Archivist-compatible download endpoint (GET /api/archivist/v1/data/:cid/network/stream)
/// Returns raw binary data
async fn fetch_cid_from_peers(
    state: &ApiState,
    cid: &Cid,
    cid_str: &str,
) -> Result<Vec<u8>, ApiError> {
    info!(
        "Archivist API: Block {} not found locally, fetching from peers",
        cid_str
    );

    let client = state.fallback_http_client.clone();
    let fallback_peers = state.fallback_http_peers.as_ref();

    // In the common pair-benchmark case we have exactly one peer;
    // skip allocations and shuffling on every miss.
    let mut peers = fallback_peers.clone();
    if peers.len() > 1 {
        peers.shuffle(&mut rand::thread_rng());
    }

    for base_url in peers.iter().take(25) {
        let url = format!(
            "{}/api/archivist/v1/data/{}/network/stream",
            base_url, cid_str
        );

        info!(
            "Archivist API: Trying to fetch {} from {}",
            cid_str, base_url
        );

        match client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.bytes().await {
                        Ok(data) => {
                            info!(
                                "Archivist API: Fetched {} from {} ({} bytes)",
                                cid_str,
                                base_url,
                                data.len()
                            );

                            // Avoid storing fetched bytes under manifest CID.
                            if cid.codec() != 0xcd01 {
                                let block = Block::from_cid_and_data(cid.clone(), data.to_vec())
                                    .map_err(|e| {
                                        ApiError::Internal(format!("Failed to create block: {}", e))
                                    })?;
                                state.block_store.put(block).await.map_err(|e| {
                                    ApiError::Internal(format!("Failed to store block: {}", e))
                                })?;
                            }

                            return Ok(data.to_vec());
                        }
                        Err(e) => {
                            info!(
                                "Archivist API: Failed to read response from {}: {}",
                                base_url, e
                            );
                        }
                    }
                } else {
                    info!(
                        "Archivist API: Got HTTP {} from {}",
                        resp.status(),
                        base_url
                    );
                }
            }
            Err(e) => {
                info!("Archivist API: Failed to fetch from {}: {}", base_url, e);
            }
        }
    }

    info!(
        "Archivist API: Block {} not available from any peer",
        cid_str
    );
    Err(ApiError::NotFound(cid_str.to_string()))
}

async fn archivist_download(
    State(state): State<ApiState>,
    Path(cid_str): Path<String>,
) -> Result<Vec<u8>, ApiError> {
    info!("Archivist API: Downloading {}", cid_str);

    // Parse CID
    let cid = cid_str
        .parse()
        .map_err(|e| ApiError::BadRequest(format!("Invalid CID: {}", e)))?;

    // Try to get content from local store first.
    match retrieve_local_cid_data(&state, &cid, &cid_str).await {
        Ok(data) => Ok(data),
        Err(ApiError::NotFound(_)) => fetch_cid_from_peers(&state, &cid, &cid_str).await,
        Err(e) => Err(e),
    }
}

/// Peer ID endpoint (GET /api/archivist/v1/peer-id)
async fn peer_id_endpoint(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.peer_id)
}

/// Stats endpoint (GET /api/archivist/v1/stats)
async fn archivist_stats(State(state): State<ApiState>) -> impl IntoResponse {
    let stats = state.block_store.stats().await;

    Json(serde_json::json!({
        "block_count": stats.block_count,
        "total_size": stats.total_size,
    }))
}

/// SPR endpoint (GET /api/archivist/v1/spr)
/// Returns the Signed Peer Record for this node
async fn spr_endpoint(State(state): State<ApiState>) -> Result<String, ApiError> {
    use crate::spr::generate_spr;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Use current timestamp as sequence number
    let seq = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Read listen addresses from shared state
    let addrs_snapshot = state.listen_addrs.read().unwrap().clone();

    // Filter listen addresses to only include UDP addresses (Archivist format)
    // Archivist SPRs contain UDP addresses for discovery
    let udp_addrs: Vec<Multiaddr> = addrs_snapshot
        .iter()
        .filter_map(|addr| {
            let addr_str = addr.to_string();
            if addr_str.contains("/tcp/") {
                // Convert TCP to UDP for SPR (Archivist convention)
                let udp_str = addr_str.replace("/tcp/", "/udp/");
                udp_str.parse().ok()
            } else {
                None
            }
        })
        .collect();

    if udp_addrs.is_empty() {
        return Err(ApiError::Internal(
            "No listen addresses available".to_string(),
        ));
    }

    // Generate SPR
    let spr = generate_spr(&state.keypair, &udp_addrs, seq)
        .map_err(|e| ApiError::Internal(format!("Failed to generate SPR: {}", e)))?;

    info!("Generated SPR for peer {}", state.peer_id);

    Ok(spr)
}

/// API error type
#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Unprocessable(String),
    ServiceUnavailable(String),
    NotImplemented(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Unprocessable(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            ApiError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg),
            ApiError::NotImplemented(msg) => (StatusCode::NOT_IMPLEMENTED, msg),
            ApiError::Internal(msg) => {
                error!("API error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, msg)
            }
        };

        let body = Json(ErrorResponse { error: message });
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::citadel::{
        CitadelSyncPullResponse, CitadelSyncPushResponse, DefederationGuardConfig, DefederationNode,
    };
    use crate::marketplace::MarketplaceRuntimeInfo;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    async fn marketplace_test_app(block_store: Arc<BlockStore>) -> (Router, tempfile::TempDir) {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let tmp = tempfile::tempdir().unwrap();
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));
        let marketplace = MarketplaceStore::open(tmp.path().join("marketplace.json"))
            .await
            .unwrap();

        let app = create_router_with_runtime(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
            None,
            Some(marketplace),
            MarketplaceRuntimeInfo {
                persistence_enabled: true,
                quota_max_bytes: 4096,
                eth_account: Some("0xabc".to_string()),
                ..MarketplaceRuntimeInfo::default()
            },
            Vec::new(),
        );

        (app, tmp)
    }

    #[tokio::test]
    async fn test_health_check() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let peer_id = "12D3KooWTest123".to_string();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));
        let app = create_router(block_store, metrics, peer_id, botg, keypair, listen_addrs);

        let request = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_store_and_get_block() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));
        let app = create_router(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
        );

        // Store a block
        let test_data = b"Hello, REST API!";
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/blocks")
            .header("content-type", "application/octet-stream")
            .body(Body::from(test_data.to_vec()))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let store_response: StoreBlockResponse = serde_json::from_slice(&body).unwrap();

        // Get the block back
        let request = Request::builder()
            .uri(format!("/api/v1/blocks/{}", store_response.cid))
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let get_block_response: GetBlockResponse = serde_json::from_slice(&body).unwrap();

        // Verify data matches
        let decoded_data = base64::prelude::BASE64_STANDARD
            .decode(&get_block_response.data)
            .unwrap();
        assert_eq!(decoded_data, test_data);
    }

    #[tokio::test]
    async fn test_get_nonexistent_block() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));
        let app = create_router(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
        );

        let request = Request::builder()
            .uri("/api/v1/blocks/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_large_archivist_upload_over_2mb() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));
        let app = create_router(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
        );

        // 3 MiB payload exceeds Axum's default 2 MiB limit.
        let payload = vec![0xAB; 3 * 1024 * 1024];
        let request = Request::builder()
            .method("POST")
            .uri("/api/archivist/v1/data")
            .header("content-type", "application/octet-stream")
            .body(Body::from(payload))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_marketplace_endpoints_require_persistence() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));
        let app = create_router(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
        );

        let request = Request::builder()
            .uri("/api/archivist/v1/sales/availability")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_marketplace_availability_and_purchase_flow() {
        let block_store = Arc::new(BlockStore::new());
        let block = Block::new_sha256(b"marketplace payload".to_vec()).unwrap();
        let cid = block.cid.to_string();
        block_store.put(block).await.unwrap();

        let (app, _tmp) = marketplace_test_app(block_store).await;

        let request = Request::builder()
            .method("POST")
            .uri("/api/archivist/v1/sales/availability")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"minimumPricePerBytePerSecond":"2","maximumCollateralPerByte":"9","maximumDuration":3600,"availableUntil":0}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let request = Request::builder()
            .method("POST")
            .uri(format!("/api/archivist/v1/storage/request/{}", cid))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"duration":3600,"proofProbability":"4","pricePerBytePerSecond":"42","collateralPerByte":"7","expiry":300,"nodes":3,"tolerance":1}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let purchase_id = String::from_utf8(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(!purchase_id.trim().is_empty());

        let request = Request::builder()
            .uri("/api/archivist/v1/storage/purchases")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let purchases: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert_eq!(purchases, vec![purchase_id.clone()]);

        let request = Request::builder()
            .uri(format!(
                "/api/archivist/v1/storage/purchases/{}",
                purchase_id
            ))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let purchase: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(purchase["state"], "submitted");

        let request = Request::builder()
            .uri("/api/archivist/v1/sales/slots")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let slots: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(slots.len(), 1);
    }

    #[tokio::test]
    async fn test_citadel_status_disabled_when_mode_off() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));
        let app = create_router(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
        );

        let request = Request::builder()
            .uri("/api/citadel/v1/status")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_citadel_follow_and_view_endpoints() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));

        let guard = DefederationGuardConfig::default();
        let citadel = Arc::new(AsyncRwLock::new(DefederationNode::new(
            42,
            0,
            1,
            std::collections::HashSet::from([42]),
            guard,
        )));

        let app = create_router_with_citadel(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
            Some(citadel),
        );

        let follow_request = Request::builder()
            .method("POST")
            .uri("/api/citadel/v1/follow/2")
            .body(Body::empty())
            .unwrap();
        let follow_response = app.clone().oneshot(follow_request).await.unwrap();
        assert_eq!(follow_response.status(), StatusCode::OK);

        let content_request = Request::builder()
            .method("POST")
            .uri("/api/citadel/v1/content/0/true")
            .body(Body::empty())
            .unwrap();
        let content_response = app.clone().oneshot(content_request).await.unwrap();
        assert_eq!(content_response.status(), StatusCode::OK);

        let view_request = Request::builder()
            .uri("/api/citadel/v1/view/1")
            .body(Body::empty())
            .unwrap();
        let view_response = app.clone().oneshot(view_request).await.unwrap();
        assert_eq!(view_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_citadel_sync_pull_returns_missing_ops() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));

        let guard = DefederationGuardConfig::default();
        let citadel = Arc::new(AsyncRwLock::new(DefederationNode::new(
            77,
            0,
            1,
            std::collections::HashSet::from([77]),
            guard,
        )));
        {
            let mut node = citadel.write().await;
            let _ = node.emit_local_follow(2, true);
            let _ = node.emit_local_content(5, true);
        }

        let app = create_router_with_citadel(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
            Some(citadel),
        );

        let body = serde_json::json!({
            "round": 1,
            "frontier": {},
            "max_ops": 8
        });
        let request = Request::builder()
            .method("POST")
            .uri("/api/citadel/v1/sync/pull")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let out: CitadelSyncPullResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(out.node_id, 77);
        assert!(out.provided_ops >= 2);
        assert!(out.ops.len() >= 2);
        assert_eq!(out.frontier.get(&77).copied().unwrap_or(0), 2);
    }

    #[tokio::test]
    async fn test_citadel_sync_push_merges_ops_and_returns_delta() {
        use crate::botg::BoTgConfig;
        use libp2p::identity::Keypair;

        let block_store = Arc::new(BlockStore::new());
        let metrics = Metrics::new();
        let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
        let keypair = Arc::new(Keypair::generate_ed25519());
        let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
            .parse()
            .unwrap()]));

        let guard = DefederationGuardConfig::default();
        let citadel = Arc::new(AsyncRwLock::new(DefederationNode::new(
            88,
            0,
            1,
            std::collections::HashSet::from([88]),
            guard,
        )));

        let outbound_ops = {
            let mut peer_node = DefederationNode::new(
                99,
                1,
                2,
                std::collections::HashSet::from([99]),
                DefederationGuardConfig::default(),
            );
            vec![
                peer_node.emit_local_follow(1, true),
                peer_node.emit_local_content(7, true),
            ]
        };

        let app = create_router_with_citadel(
            block_store,
            metrics,
            "12D3KooWTest123".to_string(),
            botg,
            keypair,
            listen_addrs,
            Some(citadel),
        );

        let body = serde_json::json!({
            "round": 1,
            "frontier": {},
            "ops": outbound_ops,
            "max_ops": 16
        });
        let request = Request::builder()
            .method("POST")
            .uri("/api/citadel/v1/sync/push")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let out: CitadelSyncPushResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(out.node_id, 88);
        assert!(out.accepted_ops >= 2);
        assert!(out.frontier.get(&99).copied().unwrap_or(0) >= 2);
    }
}
