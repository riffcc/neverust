//! REST API for block operations and node management

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::Engine;
use cid::{multibase::Base, Cid};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

use crate::archivist_tree::ArchivistTree;
use crate::botg::BoTgProtocol;
use crate::chunker::Chunker;
use crate::manifest::Manifest;
use crate::metrics::Metrics;
use crate::storage::{Block, BlockStore, StorageError};
use libp2p::{identity::Keypair, Multiaddr};
use std::io::Cursor;
use std::sync::RwLock;

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

/// Create the REST API router
pub fn create_router(
    block_store: Arc<BlockStore>,
    metrics: Metrics,
    peer_id: String,
    botg: Arc<BoTgProtocol>,
    keypair: Arc<Keypair>,
    listen_addrs: Arc<RwLock<Vec<Multiaddr>>>,
) -> Router {
    let state = ApiState {
        block_store,
        metrics,
        peer_id,
        botg,
        keypair,
        listen_addrs,
    };

    Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_endpoint))
        .route("/api/v1/blocks", post(store_block))
        .route("/api/v1/blocks/:cid", get(get_block))
        // Archivist-compatible endpoints
        .route(
            "/api/archivist/v1/data",
            get(archivist_list_data).post(archivist_upload),
        )
        .route(
            "/api/archivist/v1/data/:cid",
            get(archivist_download_local).delete(archivist_delete),
        )
        .route(
            "/api/archivist/v1/data/:cid/network",
            post(archivist_download_network_manifest),
        )
        .route(
            "/api/archivist/v1/data/:cid/network/stream",
            get(archivist_download),
        )
        .route(
            "/api/archivist/v1/data/:cid/network/manifest",
            get(archivist_download_network_manifest),
        )
        .route("/api/archivist/v1/space", get(archivist_space))
        .route("/api/archivist/v1/peer-id", get(peer_id_endpoint))
        .route("/api/archivist/v1/peerid", get(peer_id_endpoint))
        .route("/api/archivist/v1/stats", get(archivist_stats))
        .route("/api/archivist/v1/spr", get(spr_endpoint))
        .route("/api/archivist/v1/connect/:peer_id", get(connect_not_supported))
        .route(
            "/api/archivist/v1/sales/slots",
            get(marketplace_persistence_disabled),
        )
        .route(
            "/api/archivist/v1/sales/slots/:slot_id",
            get(marketplace_persistence_disabled),
        )
        .route(
            "/api/archivist/v1/sales/availability",
            get(marketplace_persistence_disabled).post(marketplace_persistence_disabled),
        )
        .route(
            "/api/archivist/v1/storage/request/:cid",
            post(marketplace_persistence_disabled),
        )
        .route(
            "/api/archivist/v1/storage/purchases",
            get(marketplace_persistence_disabled),
        )
        .route(
            "/api/archivist/v1/storage/purchases/:id",
            get(marketplace_persistence_disabled),
        )
        .route(
            "/api/archivist/v1/debug/info",
            get(debug_info_endpoint),
        )
        .route(
            "/api/archivist/v1/debug/chronicles/loglevel",
            post(loglevel_not_supported),
        )
        .route(
            "/api/archivist/v1/debug/peer/:peer_id",
            get(debug_peer_not_supported),
        )
        .route(
            "/api/archivist/v1/debug/testing/option/:key/:value",
            post(debug_testing_not_supported),
        )
        .with_state(state)
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

async fn retrieve_local_cid_data(state: &ApiState, cid: &Cid, cid_str: &str) -> Result<Vec<u8>, ApiError> {
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

        let tree_metadata_block = state.block_store.get(&metadata_cid).await.map_err(|e| match e {
            StorageError::BlockNotFound(_) => {
                ApiError::NotFound(format!("metadata for manifest {} not found", cid_str))
            }
            _ => ApiError::Internal(format!("Failed to fetch tree metadata {}: {}", metadata_cid, e)),
        })?;

        let block_cids = ArchivistTree::deserialize_block_list(&tree_metadata_block.data)
            .map_err(|e| ApiError::Internal(format!("Failed to deserialize tree metadata: {}", e)))?;

        if block_cids.len() != manifest.blocks_count() {
            return Err(ApiError::Internal(format!(
                "Block count mismatch: tree has {} blocks but manifest expects {}",
                block_cids.len(),
                manifest.blocks_count()
            )));
        }

        let mut data: Vec<u8> = Vec::with_capacity(manifest.dataset_size as usize);
        for block_cid in &block_cids {
            let b = state.block_store.get(block_cid).await.map_err(|e| match e {
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

    Json(SpaceResponse {
        total_blocks: stats.block_count,
        quota_max_bytes: stats.total_size,
        quota_used_bytes: stats.total_size,
        quota_reserved_bytes: 0,
    })
}

/// Placeholder until libp2p connect orchestration is exposed in API state.
async fn connect_not_supported() -> Result<StatusCode, ApiError> {
    Err(ApiError::NotImplemented(
        "Peer connect API is not wired yet in neverust runtime".to_string(),
    ))
}

/// Placeholder for marketplace-dependent APIs.
async fn marketplace_persistence_disabled() -> Result<StatusCode, ApiError> {
    Err(ApiError::ServiceUnavailable(
        "Persistence is not enabled".to_string(),
    ))
}

/// Lightweight debug info endpoint for compatibility.
async fn debug_info_endpoint(State(state): State<ApiState>) -> impl IntoResponse {
    let addrs = state
        .listen_addrs
        .read()
        .map(|v| v.iter().map(ToString::to_string).collect::<Vec<_>>())
        .unwrap_or_default();

    Json(serde_json::json!({
        "id": state.peer_id,
        "addrs": addrs,
        "repo": "unknown",
        "spr": "",
        "announceAddresses": [],
        "ethAddress": serde_json::Value::Null,
        "table": {"localNode": serde_json::Value::Null, "nodes": []},
        "archivist": {"version": env!("CARGO_PKG_VERSION"), "revision": "unknown", "contracts": "unknown"}
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
async fn archivist_upload(
    State(state): State<ApiState>,
    body: bytes::Bytes,
) -> Result<String, ApiError> {
    if body.is_empty() {
        return Err(ApiError::BadRequest("Empty data".to_string()));
    }

    let dataset_size = body.len();
    info!(
        "Archivist API: Uploading data ({} bytes) - will chunk and create manifest",
        dataset_size
    );

    // Step 1: Chunk the data and store blocks
    let cursor = Cursor::new(body.to_vec());
    let mut chunker = Chunker::new(cursor); // Uses default 64KB chunks
    let mut block_cids = Vec::new();

    while let Some(chunk) = chunker
        .next_chunk()
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to read chunk: {}", e)))?
    {
        // Create block from chunk (uses codec 0xcd02)
        let block = Block::new(chunk)
            .map_err(|e| ApiError::Internal(format!("Failed to create block: {}", e)))?;

        info!(
            "Archivist API: Created block {} ({} bytes)",
            block.cid,
            block.size()
        );

        block_cids.push(block.cid);

        // Store block
        state
            .block_store
            .put(block)
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to store block: {}", e)))?;
    }

    info!(
        "Archivist API: Stored {} blocks for dataset",
        block_cids.len()
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
    let tree_metadata_block = Block::new(tree_block_list)
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
        chunker.chunk_size() as u64,
        dataset_size as u64,
        None, // codec (uses default 0xcd02)
        None, // hcodec (uses default SHA-256)
        None, // version (uses default 1)
        Some(format!("metadata:{}", tree_metadata_cid)), // filename (stores metadata CID)
        None, // mimetype
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

/// Archivist-compatible download endpoint (GET /api/archivist/v1/data/:cid/network/stream)
/// Returns raw binary data
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
        Err(ApiError::NotFound(_)) => {
            // Block not found locally - try fetching from known peers via HTTP
            // This is a temporary solution - in production would use BlockExc/BoTG
            info!(
                "Archivist API: Block {} not found locally, fetching from peers",
                cid_str
            );

            // Try all known peers in Docker network (Archivist-style peer discovery)
            // Generate peer list: bootstrap + node1..node49 (for 50 node cluster)
            let mut peer_urls = vec![];

            // Add Docker network peers
            peer_urls.push("http://bootstrap:8080".to_string());
            for i in 1..50 {
                peer_urls.push(format!("http://node{}:8080", i));
            }

            // Add known external Archivist testnet peers (try multiple ports)
            let external_peers = vec![
                "91.98.135.54",
                "10.7.1.200", // blackberry
            ];
            for peer in external_peers {
                // Try common Archivist API ports
                for port in [8080, 8070, 8000, 3000] {
                    peer_urls.push(format!("http://{}:{}", peer, port));
                }
            }

            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .map_err(|e| ApiError::Internal(format!("Failed to create HTTP client: {}", e)))?;

            // Shuffle peers for load distribution (Archivist-style)
            {
                let mut rng = rand::thread_rng();
                peer_urls.shuffle(&mut rng);
            }

            for base_url in peer_urls.iter().take(25) {
                // Try up to 25 random peers
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
                                        let block = Block::new(data.to_vec()).map_err(|e| {
                                            ApiError::Internal(format!("Failed to create block: {}", e))
                                        })?;

                                        state.block_store.put(block.clone()).await.map_err(|e| {
                                            ApiError::Internal(format!("Failed to store block: {}", e))
                                        })?;

                                        return Ok(block.data);
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
            Err(ApiError::NotFound(cid_str.clone()))
        }
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
    ServiceUnavailable(String),
    NotImplemented(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(cid) => (StatusCode::NOT_FOUND, format!("Block not found: {}", cid)),
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
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

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
}
