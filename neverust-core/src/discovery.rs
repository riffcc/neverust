//! DiscV5-based peer discovery for Archivist network
//!
//! Implements UDP-based peer discovery using the Kademlia DHT protocol (DiscV5).
//! This enables automatic discovery of peers and content providers in the network.

use cid::Cid;
use discv5::{enr, ConfigBuilder, Discv5, Event as Discv5Event, ListenConfig};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// Re-export PeerId from libp2p for use in discovery
use libp2p::identity::PeerId;

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("DiscV5 error: {0}")]
    Discv5Error(String),

    #[error("ENR error: {0}")]
    EnrError(String),

    #[error("Invalid peer ID")]
    InvalidPeerId,

    #[error("No providers found for CID: {0}")]
    NoProviders(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, DiscoveryError>;

/// Peer discovery service using DiscV5
pub struct Discovery {
    /// DiscV5 protocol instance
    discv5: Arc<Discv5>,

    /// Local peer ID
    peer_id: PeerId,

    /// Provider records: CID -> Set of peer IDs
    providers: Arc<RwLock<HashMap<Cid, Vec<PeerId>>>>,

    /// Announced multiaddrs for this node
    _announce_addrs: Vec<String>,
}

impl Discovery {
    /// Create a new Discovery instance
    pub async fn new(
        keypair: &libp2p::identity::Keypair,
        listen_addr: SocketAddr,
        announce_addrs: Vec<String>,
        bootstrap_peers: Vec<String>,
    ) -> Result<Self> {
        info!("Initializing DiscV5 peer discovery on {}", listen_addr);

        // Extract secp256k1 key bytes from libp2p keypair
        // libp2p v0.56+ uses identity module
        let _key_bytes = keypair
            .to_protobuf_encoding()
            .map_err(|e| DiscoveryError::EnrError(format!("Failed to encode keypair: {}", e)))?;

        // Try to create secp256k1 signing key from encoded bytes
        // For now, we'll generate a fresh key since libp2p keypair extraction is complex
        warn!("Generating fresh secp256k1 key for DiscV5 (libp2p key extraction not yet implemented)");
        let secret_key = enr::k256::ecdsa::SigningKey::random(&mut rand::thread_rng());

        let peer_id = keypair.public().to_peer_id();

        // Create ENR builder
        let enr_key = enr::CombinedKey::Secp256k1(secret_key);
        let mut builder = enr::Enr::builder();

        // Add IP and UDP port
        match listen_addr.ip() {
            IpAddr::V4(ip) => {
                builder.ip4(ip);
                builder.udp4(listen_addr.port());
            }
            IpAddr::V6(ip) => {
                builder.ip6(ip);
                builder.udp6(listen_addr.port());
            }
        }

        // Add libp2p peer ID as custom ENR entry
        builder.add_value("libp2p", &peer_id.to_bytes());

        let enr = builder
            .build(&enr_key)
            .map_err(|e| DiscoveryError::EnrError(e.to_string()))?;

        info!("Local ENR: {}", enr.to_base64());
        info!("Local Peer ID: {}", peer_id);

        // Create listen config
        let listen_config = ListenConfig::Ipv4 {
            ip: match listen_addr.ip() {
                IpAddr::V4(ip) => ip,
                IpAddr::V6(_) => {
                    return Err(DiscoveryError::Discv5Error(
                        "IPv6 not yet supported".into(),
                    ))
                }
            },
            port: listen_addr.port(),
        };

        // Configure DiscV5 with listen config
        let config = ConfigBuilder::new(listen_config).build();

        // Initialize DiscV5
        let mut discv5 = Discv5::new(enr, enr_key, config)
            .map_err(|e| DiscoveryError::Discv5Error(e.to_string()))?;

        // Start listening (no arguments - uses config from constructor)
        discv5
            .start()
            .await
            .map_err(|e| DiscoveryError::Discv5Error(e.to_string()))?;

        info!("DiscV5 listening on {}", listen_addr);

        // Add bootstrap peers
        for peer_str in bootstrap_peers {
            match peer_str.parse::<enr::Enr<enr::CombinedKey>>() {
                Ok(bootstrap_enr) => {
                    match discv5.add_enr(bootstrap_enr.clone()) {
                        Ok(_) => info!("Added bootstrap peer: {}", bootstrap_enr.node_id()),
                        Err(e) => warn!("Failed to add bootstrap peer: {}", e),
                    }
                }
                Err(e) => warn!("Invalid bootstrap ENR {}: {}", peer_str, e),
            }
        }

        Ok(Self {
            discv5: Arc::new(discv5),
            peer_id,
            providers: Arc::new(RwLock::new(HashMap::new())),
            _announce_addrs: announce_addrs,
        })
    }

    /// Get local peer ID
    pub fn local_peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Get local ENR
    pub fn local_enr(&self) -> enr::Enr<enr::CombinedKey> {
        self.discv5.local_enr()
    }

    /// Announce that we provide a specific CID (block)
    pub async fn provide(&self, cid: &Cid) -> Result<()> {
        debug!("Announcing provider record for CID: {}", cid);

        // Store locally that we provide this CID
        let mut providers = self.providers.write().await;
        providers
            .entry(*cid)
            .or_insert_with(Vec::new)
            .push(self.peer_id);

        // In a full implementation, we would publish to the DHT here
        // For now, we just track locally
        info!("Providing CID: {}", cid);

        Ok(())
    }

    /// Find providers for a specific CID
    pub async fn find(&self, cid: &Cid) -> Result<Vec<PeerId>> {
        debug!("Searching for providers of CID: {}", cid);

        // Check local cache first
        let providers = self.providers.read().await;
        if let Some(peers) = providers.get(cid) {
            if !peers.is_empty() {
                debug!("Found {} providers in cache", peers.len());
                return Ok(peers.clone());
            }
        }
        drop(providers);

        // Query DHT for providers
        // In a full implementation, we would query the DHT here
        warn!("DHT provider queries not yet fully implemented");

        Err(DiscoveryError::NoProviders(cid.to_string()))
    }

    /// Find a specific peer by ID
    pub async fn find_peer(&self, peer_id: &PeerId) -> Result<Vec<String>> {
        debug!("Searching for peer: {}", peer_id);

        // Query DHT for peer's ENR
        // Convert PeerId to NodeId for lookup
        // For now, return empty (not yet implemented)
        warn!("DHT peer lookups not yet fully implemented");

        Ok(vec![])
    }

    /// Get connected peer count
    pub fn connected_peers(&self) -> usize {
        self.discv5.connected_peers()
    }

    /// Run the discovery event loop
    pub async fn run(self: Arc<Self>) {
        info!("Starting DiscV5 event loop");

        let mut event_stream = self.discv5.event_stream().await.unwrap();

        loop {
            tokio::select! {
                Some(event) = event_stream.recv() => {
                    self.handle_event(event).await;
                }
            }
        }
    }

    /// Handle DiscV5 events
    async fn handle_event(&self, event: Discv5Event) {
        match event {
            Discv5Event::Discovered(enr) => {
                debug!("Discovered peer: {}", enr.node_id());

                // Extract libp2p peer ID if available
                if let Some(Ok(peer_id_bytes)) = enr.get_decodable::<Vec<u8>>("libp2p") {
                    match PeerId::from_bytes(&peer_id_bytes) {
                        Ok(peer_id) => {
                            info!("Discovered libp2p peer: {} (ENR: {})", peer_id, enr.node_id());
                        }
                        Err(e) => {
                            warn!("Invalid libp2p peer ID in ENR: {}", e);
                        }
                    }
                }
            }
            Discv5Event::NodeInserted { node_id, replaced } => {
                if let Some(old_node) = replaced {
                    debug!("Replaced node {} with {}", old_node, node_id);
                } else {
                    debug!("Inserted new node: {}", node_id);
                }
            }
            Discv5Event::SessionEstablished(enr, socket_addr) => {
                info!("Session established with {} at {}", enr.node_id(), socket_addr);
            }
            _ => {
                // Other events (TalkRequest, etc.)
                debug!("DiscV5 event: {:?}", event);
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> DiscoveryStats {
        DiscoveryStats {
            connected_peers: self.connected_peers(),
            local_peer_id: self.peer_id,
            local_enr: self.local_enr().to_base64(),
        }
    }
}

/// Discovery statistics
#[derive(Debug, Clone)]
pub struct DiscoveryStats {
    pub connected_peers: usize,
    pub local_peer_id: PeerId,
    pub local_enr: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    #[tokio::test]
    async fn test_discovery_creation() {
        let keypair = Keypair::generate_secp256k1();
        let listen_addr = "127.0.0.1:9000".parse().unwrap();
        let announce_addrs = vec!["/ip4/127.0.0.1/tcp/8070".to_string()];

        let discovery = Discovery::new(&keypair, listen_addr, announce_addrs, vec![])
            .await
            .unwrap();

        assert_eq!(discovery.connected_peers(), 0);
        assert_eq!(discovery.local_peer_id(), &keypair.public().to_peer_id());
    }

    #[tokio::test]
    async fn test_provide_and_find() {
        let keypair = Keypair::generate_secp256k1();
        let listen_addr = "127.0.0.1:9001".parse().unwrap();
        let announce_addrs = vec!["/ip4/127.0.0.1/tcp/8070".to_string()];

        let discovery = Discovery::new(&keypair, listen_addr, announce_addrs, vec![])
            .await
            .unwrap();

        // Create a test CID
        let cid: Cid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            .parse()
            .unwrap();

        // Announce we provide this CID
        discovery.provide(&cid).await.unwrap();

        // Should find ourselves as provider
        let providers = discovery.find(&cid).await.unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0], keypair.public().to_peer_id());
    }
}
