//! DHT provider protocol for Archivist content discovery.
//!
//! Implements the addProvider/getProviders/providers message flow
//! on top of the vendored discv5 crate so that blocks stored on this
//! node are discoverable by other Archivist nodes via the DHT.

use cid::Cid;
use discv5::enr::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tiny_keccak::{Hasher, Keccak};
use tracing::{debug, info, warn};

/// Maximum provider records stored per content ID.
const MAX_PROVIDERS_PER_ENTRY: usize = 20;

/// Default TTL for provider records (24 hours).
const PROVIDER_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Convert a CID to a DiscV5 NodeId via keccak256, matching Archivist's
/// `toNodeId` function: `readUintBE[256](keccak256.digest(cid.data.buffer).data)`.
pub fn cid_to_node_id(cid: &Cid) -> NodeId {
    let cid_bytes = cid.to_bytes();
    let mut hasher = Keccak::v256();
    let mut hash = [0u8; 32];
    hasher.update(&cid_bytes);
    hasher.finalize(&mut hash);
    NodeId::new(&hash)
}

/// A single provider record with expiry.
#[derive(Clone, Debug)]
struct ProviderRecord {
    /// The raw SignedPeerRecord bytes (protobuf-encoded).
    signed_peer_record: Vec<u8>,
    /// When this record expires.
    expires: Instant,
}

/// In-memory store of provider records keyed by content NodeId.
pub struct ProviderStore {
    records: HashMap<NodeId, Vec<ProviderRecord>>,
}

impl ProviderStore {
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    /// Add a provider record for a content ID.
    pub fn add(&mut self, content_id: NodeId, signed_peer_record: Vec<u8>) {
        let entry = self.records.entry(content_id).or_default();

        // Remove expired records first.
        let now = Instant::now();
        entry.retain(|r| r.expires > now);

        // Check for duplicate (same SPR bytes).
        if entry.iter().any(|r| r.signed_peer_record == signed_peer_record) {
            // Update expiry of existing record.
            for r in entry.iter_mut() {
                if r.signed_peer_record == signed_peer_record {
                    r.expires = now + PROVIDER_TTL;
                    return;
                }
            }
        }

        // Evict oldest if at capacity.
        if entry.len() >= MAX_PROVIDERS_PER_ENTRY {
            entry.sort_by_key(|r| r.expires);
            entry.remove(0);
        }

        entry.push(ProviderRecord {
            signed_peer_record,
            expires: now + PROVIDER_TTL,
        });
    }

    /// Get provider records for a content ID.
    pub fn get(&mut self, content_id: &NodeId) -> Vec<Vec<u8>> {
        let now = Instant::now();
        if let Some(entry) = self.records.get_mut(content_id) {
            entry.retain(|r| r.expires > now);
            entry.iter().map(|r| r.signed_peer_record.clone()).collect()
        } else {
            Vec::new()
        }
    }

    /// Number of content IDs with provider records.
    pub fn len(&self) -> usize {
        self.records.len()
    }
}

/// Thread-safe provider store.
pub type SharedProviderStore = Arc<RwLock<ProviderStore>>;

/// Create a new shared provider store.
pub fn new_provider_store() -> SharedProviderStore {
    Arc::new(RwLock::new(ProviderStore::new()))
}

/// Handle an inbound AddProvider message.
pub async fn handle_add_provider(
    store: &SharedProviderStore,
    content_id: &[u8],
    provider_record: Vec<u8>,
) {
    if content_id.len() != 32 {
        warn!(
            "AddProvider: invalid content_id length: {}",
            content_id.len()
        );
        return;
    }
    let mut id = [0u8; 32];
    id.copy_from_slice(content_id);
    let node_id = NodeId::new(&id);

    let mut store = store.write().await;
    store.add(node_id, provider_record);
    debug!(
        "Stored provider record for content {}",
        hex::encode(content_id)
    );
}

/// Handle an inbound GetProviders message. Returns (total, provider_records).
pub async fn handle_get_providers(
    store: &SharedProviderStore,
    content_id: &[u8],
) -> (u32, Vec<Vec<u8>>) {
    if content_id.len() != 32 {
        warn!(
            "GetProviders: invalid content_id length: {}",
            content_id.len()
        );
        return (0, Vec::new());
    }
    let mut id = [0u8; 32];
    id.copy_from_slice(content_id);
    let node_id = NodeId::new(&id);

    let mut store = store.write().await;
    let providers = store.get(&node_id);
    let total = providers.len() as u32;
    info!(
        "GetProviders for {}: returning {} records",
        hex::encode(content_id),
        total
    );
    (total, providers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cid_to_node_id_deterministic() {
        let cid: Cid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            .parse()
            .unwrap();
        let id1 = cid_to_node_id(&cid);
        let id2 = cid_to_node_id(&cid);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_cid_to_node_id_different_cids_differ() {
        let cid1: Cid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            .parse()
            .unwrap();
        // Different CID (sha256 of different data)
        let cid2 = crate::cid_blake3::blake3_cid(b"different data").unwrap();
        assert_ne!(cid_to_node_id(&cid1), cid_to_node_id(&cid2));
    }

    #[test]
    fn test_provider_store_add_and_get() {
        let mut store = ProviderStore::new();
        let id = NodeId::new(&[1u8; 32]);
        let record = vec![0xAA, 0xBB, 0xCC];

        store.add(id, record.clone());
        let providers = store.get(&id);
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0], record);
    }

    #[test]
    fn test_provider_store_deduplicates() {
        let mut store = ProviderStore::new();
        let id = NodeId::new(&[1u8; 32]);
        let record = vec![0xAA, 0xBB, 0xCC];

        store.add(id, record.clone());
        store.add(id, record.clone());
        let providers = store.get(&id);
        assert_eq!(providers.len(), 1);
    }

    #[test]
    fn test_provider_store_max_entries() {
        let mut store = ProviderStore::new();
        let id = NodeId::new(&[1u8; 32]);

        for i in 0..25 {
            store.add(id, vec![i as u8]);
        }

        let providers = store.get(&id);
        assert_eq!(providers.len(), MAX_PROVIDERS_PER_ENTRY);
    }

    #[test]
    fn test_provider_store_different_content_ids() {
        let mut store = ProviderStore::new();
        let id1 = NodeId::new(&[1u8; 32]);
        let id2 = NodeId::new(&[2u8; 32]);

        store.add(id1, vec![0xAA]);
        store.add(id2, vec![0xBB]);

        assert_eq!(store.get(&id1).len(), 1);
        assert_eq!(store.get(&id2).len(), 1);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_provider_store_empty_lookup() {
        let mut store = ProviderStore::new();
        let id = NodeId::new(&[1u8; 32]);
        assert_eq!(store.get(&id).len(), 0);
    }
}
