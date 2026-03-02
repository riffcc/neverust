//! Cluster placement primitives for coordinated pinning.
//!
//! This is a lightweight control-plane foundation inspired by mesh-first
//! coordination: deterministic placement + stateless reconciliation.

use cid::Cid;

/// Logical node descriptor for placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub id: String,
    pub base_url: String,
    pub weight: u32,
}

impl ClusterNode {
    pub fn new(id: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            base_url: base_url.into(),
            weight: 1,
        }
    }
}

/// Upload path selection for CID class.
///
/// - Manifest CID (`0xcd01`): upload through Archivist manifest path.
/// - Non-manifest CID: upload as a raw block to preserve CID identity.
pub fn upload_path_for_cid_str(cid_str: &str) -> &'static str {
    match cid_str.parse::<Cid>() {
        Ok(cid) if cid.codec() == 0xcd01 => "/api/archivist/v1/data",
        _ => "/api/archivist/v1/data/raw",
    }
}

/// Deterministic highest-random-weight selection.
///
/// For each node we compute:
/// `score = blake3(cid_bytes || node_id || base_url) * weight`
/// and pick top-k by descending score.
pub fn select_replicas(cid_str: &str, nodes: &[ClusterNode], replicas: usize) -> Vec<ClusterNode> {
    if nodes.is_empty() || replicas == 0 {
        return Vec::new();
    }
    let cid_bytes = cid_str.as_bytes();
    let mut scored = Vec::with_capacity(nodes.len());
    for node in nodes {
        let mut hasher = blake3::Hasher::new();
        hasher.update(cid_bytes);
        hasher.update(node.id.as_bytes());
        hasher.update(node.base_url.as_bytes());
        let digest = hasher.finalize();
        let bytes = digest.as_bytes();
        let mut hi = [0u8; 8];
        let mut lo = [0u8; 8];
        hi.copy_from_slice(&bytes[0..8]);
        lo.copy_from_slice(&bytes[8..16]);
        let base = ((u128::from(u64::from_le_bytes(hi))) << 64) | u128::from(u64::from_le_bytes(lo));
        let score = base.saturating_mul(node.weight.max(1) as u128);
        scored.push((score, node.clone()));
    }

    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.id.cmp(&b.1.id)));
    scored
        .into_iter()
        .take(replicas.min(nodes.len()))
        .map(|(_, node)| node)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{select_replicas, upload_path_for_cid_str, ClusterNode};

    fn sample_nodes(n: usize) -> Vec<ClusterNode> {
        (0..n)
            .map(|i| ClusterNode::new(format!("n{}", i), format!("http://node{}:8080", i)))
            .collect()
    }

    #[test]
    fn rendezvous_is_deterministic() {
        let nodes = sample_nodes(32);
        let a = select_replicas("bafybeigdyrztx", &nodes, 8);
        let b = select_replicas("bafybeigdyrztx", &nodes, 8);
        assert_eq!(a, b);
    }

    #[test]
    fn selects_exact_replica_count() {
        let nodes = sample_nodes(10);
        let picks = select_replicas("bafybeigdyrztx", &nodes, 6);
        assert_eq!(picks.len(), 6);
    }

    #[test]
    fn clamps_to_node_count() {
        let nodes = sample_nodes(5);
        let picks = select_replicas("bafybeigdyrztx", &nodes, 99);
        assert_eq!(picks.len(), 5);
    }

    #[test]
    fn upload_path_auto_selection() {
        // Unknown/invalid => raw fast-path
        assert_eq!(upload_path_for_cid_str("not-a-cid"), "/api/archivist/v1/data/raw");
    }
}
