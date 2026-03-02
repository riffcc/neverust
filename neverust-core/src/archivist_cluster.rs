//! Archivist-cluster: deterministic multi-node pin orchestration.
//!
//! This module provides a control-plane core that can target both:
//! - remote Archivist/Neverust nodes over HTTP API
//! - local Neverust instances via direct `BlockStore` integration

use crate::cluster::{select_replicas, upload_path_for_cid_str, ClusterNode};
use crate::storage::{Block, BlockStore};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("invalid cid: {0}")]
    InvalidCid(String),
    #[error("source fetch failed: {0}")]
    SourceFetch(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

#[derive(Clone)]
pub enum MemberBackend {
    HttpApi,
    LocalNeverust(Arc<BlockStore>),
}

#[derive(Clone)]
pub struct ClusterMember {
    pub node: ClusterNode,
    pub backend: MemberBackend,
    pub healthy: bool,
}

impl ClusterMember {
    pub fn http(node: ClusterNode) -> Self {
        Self {
            node,
            backend: MemberBackend::HttpApi,
            healthy: true,
        }
    }

    pub fn local(node: ClusterNode, store: Arc<BlockStore>) -> Self {
        Self {
            node,
            backend: MemberBackend::LocalNeverust(store),
            healthy: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PinOutcome {
    pub cid: String,
    pub requested_replicas: usize,
    pub achieved_replicas: usize,
    pub pinned_nodes: Vec<String>,
    pub failed_nodes: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct ArchivistCluster {
    members: Vec<ClusterMember>,
    http_client: reqwest::Client,
    parallelism: usize,
}

impl ArchivistCluster {
    pub fn new(members: Vec<ClusterMember>, parallelism: usize) -> Result<Self, ClusterError> {
        let http_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(parallelism.saturating_mul(2).max(64))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| ClusterError::Http(format!("failed to build http client: {}", e)))?;
        Ok(Self {
            members,
            http_client,
            parallelism: parallelism.max(1),
        })
    }

    pub fn members(&self) -> &[ClusterMember] {
        &self.members
    }

    pub fn set_member_health(&mut self, node_id: &str, healthy: bool) {
        for member in &mut self.members {
            if member.node.id == node_id {
                member.healthy = healthy;
            }
        }
    }

    pub async fn pin_from_source(
        &self,
        cid: &str,
        source_base_url: &str,
        replicas: usize,
    ) -> Result<PinOutcome, ClusterError> {
        let source = source_base_url.trim_end_matches('/');
        let url = format!("{}/api/archivist/v1/data/{}/network/stream", source, cid);
        let resp = self
            .http_client
            .get(url)
            .send()
            .await
            .map_err(|e| ClusterError::SourceFetch(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(ClusterError::SourceFetch(format!(
                "HTTP {}",
                resp.status()
            )));
        }
        let payload = resp
            .bytes()
            .await
            .map_err(|e| ClusterError::SourceFetch(e.to_string()))?;
        self.pin_payload(cid, payload.to_vec(), replicas).await
    }

    pub async fn pin_payload(
        &self,
        cid: &str,
        payload: Vec<u8>,
        replicas: usize,
    ) -> Result<PinOutcome, ClusterError> {
        if cid.trim().is_empty() {
            return Err(ClusterError::InvalidCid("empty cid".to_string()));
        }
        let replicas = replicas.max(1);
        let healthy: Vec<ClusterMember> = self
            .members
            .iter()
            .filter(|m| m.healthy)
            .cloned()
            .collect();
        if healthy.is_empty() {
            return Err(ClusterError::Unsupported(
                "no healthy cluster members".to_string(),
            ));
        }

        let placement_nodes: Vec<ClusterNode> = healthy.iter().map(|m| m.node.clone()).collect();
        let ordered = select_replicas(cid, &placement_nodes, placement_nodes.len());
        let mut member_by_id = HashMap::new();
        for m in healthy {
            member_by_id.insert(m.node.id.clone(), m);
        }

        let upload_path = upload_path_for_cid_str(cid);
        let payload = Arc::new(payload);
        let mut outcome = PinOutcome {
            cid: cid.to_string(),
            requested_replicas: replicas,
            achieved_replicas: 0,
            ..Default::default()
        };

        let mut cursor = 0usize;
        while outcome.achieved_replicas < replicas && cursor < ordered.len() {
            let need = replicas - outcome.achieved_replicas;
            let remaining = ordered.len() - cursor;
            let batch_size = need.min(remaining).min(self.parallelism);
            let mut jobs = FuturesUnordered::new();

            for node in ordered.iter().skip(cursor).take(batch_size) {
                if let Some(member) = member_by_id.get(&node.id).cloned() {
                    let client = self.http_client.clone();
                    let cid = cid.to_string();
                    let upload_path = upload_path.to_string();
                    let payload = Arc::clone(&payload);
                    jobs.push(tokio::spawn(async move {
                        let id = member.node.id.clone();
                        let res =
                            pin_to_member(&client, &member, &cid, &upload_path, payload).await;
                        (id, res)
                    }));
                }
            }
            cursor = cursor.saturating_add(batch_size);

            while let Some(next) = jobs.next().await {
                match next {
                    Ok((node_id, Ok(()))) => {
                        outcome.pinned_nodes.push(node_id);
                        outcome.achieved_replicas += 1;
                    }
                    Ok((node_id, Err(e))) => {
                        outcome.failed_nodes.push((node_id, e.to_string()));
                    }
                    Err(e) => {
                        outcome
                            .failed_nodes
                            .push(("join".to_string(), e.to_string()));
                    }
                }
            }
        }

        Ok(outcome)
    }
}

async fn pin_to_member(
    http_client: &reqwest::Client,
    member: &ClusterMember,
    cid: &str,
    upload_path: &str,
    payload: Arc<Vec<u8>>,
) -> Result<(), ClusterError> {
    match &member.backend {
        MemberBackend::HttpApi => {
            let base = member.node.base_url.trim_end_matches('/');
            if base.is_empty() {
                return Err(ClusterError::Http(format!(
                    "node {} has empty base_url",
                    member.node.id
                )));
            }
            let url = format!("{}{}", base, upload_path);
            let resp = http_client
                .post(url)
                .header("content-type", "application/octet-stream")
                .body((*payload).clone())
                .send()
                .await
                .map_err(|e| ClusterError::Http(e.to_string()))?;
            if !resp.status().is_success() {
                let code = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ClusterError::Http(format!(
                    "upload failed: HTTP {} {}",
                    code, body
                )));
            }
            let returned = resp
                .text()
                .await
                .map_err(|e| ClusterError::Http(e.to_string()))?;
            if returned.trim() != cid {
                return Err(ClusterError::Http(format!(
                    "cid mismatch: expected {}, got {}",
                    cid,
                    returned.trim()
                )));
            }
            Ok(())
        }
        MemberBackend::LocalNeverust(store) => {
            let expected = cid
                .parse()
                .map_err(|e| ClusterError::InvalidCid(format!("{}", e)))?;
            if upload_path != "/api/archivist/v1/data/raw" {
                return Err(ClusterError::Unsupported(
                    "local manifest-mode pin not implemented yet".to_string(),
                ));
            }
            let block = Block::from_cid_and_data(expected, (*payload).clone())
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
            store
                .put(block)
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ArchivistCluster, ClusterMember};
    use crate::cluster::ClusterNode;
    use crate::storage::Block;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_failover_pins_to_requested_replicas() {
        let mut members = Vec::new();
        for i in 0..4 {
            let node = ClusterNode::new(format!("n{}", i), format!("local://{}", i));
            members.push(ClusterMember::local(node, Arc::new(crate::storage::BlockStore::new())));
        }
        // Force first node unhealthy to exercise failover path.
        members[0].healthy = false;

        let cluster = ArchivistCluster::new(members.clone(), 8).expect("cluster");
        let payload = b"cluster-local-pin-payload".to_vec();
        let cid = Block::new(payload.clone()).expect("cid").cid.to_string();

        let out = cluster
            .pin_payload(&cid, payload.clone(), 2)
            .await
            .expect("pin payload");
        assert_eq!(out.achieved_replicas, 2);
        assert_eq!(out.requested_replicas, 2);
        assert!(out.failed_nodes.is_empty());
    }
}
