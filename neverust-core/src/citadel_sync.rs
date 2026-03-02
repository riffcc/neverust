//! Lagoon-style mesh synchronization loop for Citadel state.
//!
//! This module cleanly separates sync transport/orchestration from
//! `citadel.rs` state and policy logic.

use crate::citadel::{
    CitadelSyncPullRequest, CitadelSyncPullResponse, CitadelSyncPushRequest,
    CitadelSyncPushResponse, DefederationNode, NodeId,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock as AsyncRwLock;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct CitadelMeshSyncConfig {
    pub peers: Vec<String>,
    pub interval_ms: u64,
    pub request_timeout_ms: u64,
    pub startup_delay_ms: u64,
    pub max_ops: usize,
    pub max_peers: usize,
}

fn parse_csv_urls(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_end_matches('/').to_string())
        .collect()
}

pub fn configured_citadel_mesh_sync(api_port: u16) -> Option<CitadelMeshSyncConfig> {
    let mut peers = std::env::var("NEVERUST_CITADEL_SYNC_PEERS")
        .ok()
        .map(|raw| parse_csv_urls(&raw))
        .unwrap_or_default();
    if peers.is_empty() {
        peers = std::env::var("NEVERUST_HTTP_FALLBACK_PEERS")
            .ok()
            .map(|raw| parse_csv_urls(&raw))
            .unwrap_or_default();
    }

    let local_markers = [
        format!("127.0.0.1:{}", api_port),
        format!("localhost:{}", api_port),
        format!("0.0.0.0:{}", api_port),
    ];

    let max_peers = std::env::var("NEVERUST_CITADEL_SYNC_MAX_PEERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(32)
        .clamp(1, 4096);

    let interval_ms = std::env::var("NEVERUST_CITADEL_SYNC_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(500);

    let request_timeout_ms = std::env::var("NEVERUST_CITADEL_SYNC_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(2000);

    let startup_delay_ms = std::env::var("NEVERUST_CITADEL_SYNC_STARTUP_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1200);

    let max_ops = std::env::var("NEVERUST_CITADEL_SYNC_MAX_OPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(1024)
        .clamp(1, 16_384);

    let mut dedup = HashSet::new();
    let peers = peers
        .into_iter()
        .filter(|p| !local_markers.iter().any(|m| p.contains(m)))
        .filter(|p| dedup.insert(p.clone()))
        .take(max_peers)
        .collect::<Vec<_>>();

    if peers.is_empty() {
        return None;
    }

    Some(CitadelMeshSyncConfig {
        peers,
        interval_ms,
        request_timeout_ms,
        startup_delay_ms,
        max_ops,
        max_peers,
    })
}

pub fn spawn_citadel_mesh_sync(
    citadel: Arc<AsyncRwLock<DefederationNode>>,
    cfg: CitadelMeshSyncConfig,
) {
    info!(
        "Citadel mesh sync enabled: peers={}, interval={}ms, timeout={}ms, max_ops={}",
        cfg.peers.len(),
        cfg.interval_ms,
        cfg.request_timeout_ms,
        cfg.max_ops
    );
    let peers = cfg.peers.clone();
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(cfg.request_timeout_ms))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                warn!("Citadel mesh sync disabled (client build failed): {}", e);
                return;
            }
        };

        tokio::time::sleep(std::time::Duration::from_millis(cfg.startup_delay_ms)).await;
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(cfg.interval_ms));
        let mut round: u64 = 1;
        // Tracks the latest known peer frontier to compute incremental deltas.
        let mut peer_frontiers: HashMap<String, HashMap<NodeId, u64>> = HashMap::new();

        loop {
            tick.tick().await;
            for base in &peers {
                let local_frontier = {
                    let node = citadel.read().await;
                    node.frontier_snapshot()
                };
                let peer_frontier = peer_frontiers.get(base).cloned().unwrap_or_default();
                let outbound_ops = {
                    let node = citadel.read().await;
                    node.missing_ops_for_frontier(&peer_frontier, cfg.max_ops)
                };

                let push_req = CitadelSyncPushRequest {
                    round,
                    frontier: local_frontier.clone(),
                    ops: outbound_ops,
                    max_ops: Some(cfg.max_ops),
                };
                let push_url = format!("{}/api/citadel/v1/sync/push", base);
                let mut merged = false;
                match client.post(&push_url).json(&push_req).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.json::<CitadelSyncPushResponse>().await {
                            Ok(sync) => {
                                peer_frontiers.insert(base.clone(), sync.frontier.clone());
                                if !sync.ops.is_empty() {
                                    let mut node = citadel.write().await;
                                    let _accepted = node.ingest_batch(round, sync.ops);
                                }
                                merged = true;
                            }
                            Err(e) => {
                                debug!("Citadel mesh sync push parse failed from {}: {}", base, e);
                            }
                        }
                    }
                    Ok(resp) => {
                        debug!(
                            "Citadel mesh sync push non-success from {}: {}",
                            base,
                            resp.status()
                        );
                    }
                    Err(e) => {
                        debug!("Citadel mesh sync push failed to {}: {}", base, e);
                    }
                }

                // Backward-compatible fallback for peers that only expose pull.
                if merged {
                    continue;
                }
                let pull_req = CitadelSyncPullRequest {
                    round,
                    frontier: local_frontier,
                    max_ops: Some(cfg.max_ops),
                };
                let pull_url = format!("{}/api/citadel/v1/sync/pull", base);
                let resp = match client.post(&pull_url).json(&pull_req).send().await {
                    Ok(r) => r,
                    Err(e) => {
                        debug!("Citadel mesh sync pull failed to {}: {}", base, e);
                        continue;
                    }
                };
                if !resp.status().is_success() {
                    debug!(
                        "Citadel mesh sync pull non-success from {}: {}",
                        base,
                        resp.status()
                    );
                    continue;
                }
                match resp.json::<CitadelSyncPullResponse>().await {
                    Ok(sync) => {
                        peer_frontiers.insert(base.clone(), sync.frontier.clone());
                        if !sync.ops.is_empty() {
                            let mut node = citadel.write().await;
                            let _accepted = node.ingest_batch(round, sync.ops);
                        }
                    }
                    Err(e) => {
                        debug!("Citadel mesh sync pull parse failed from {}: {}", base, e);
                    }
                }
            }
            round = round.saturating_add(1);
        }
    });
}
