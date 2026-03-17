//! DiscV5-based peer discovery for Archivist network
//!
//! Implements UDP-based peer discovery using the Kademlia DHT protocol (DiscV5).
//! This enables automatic discovery of peers and content providers in the network.
//! Uses protobuf-encoded messages matching the Archivist DHT wire format.

use cid::Cid;
use discv5::{
    enr, handler::NodeContact, rpc::RequestBody, rpc::ResponseBody, ConfigBuilder, Discv5,
    Event as Discv5Event, ListenConfig,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::dht_provider::{
    cid_to_node_id, handle_add_provider, handle_get_providers, new_provider_store,
    SharedProviderStore,
};
use crate::spr::{parse_spr_records_full, SprRecord};

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

    /// DHT provider record store
    provider_store: SharedProviderStore,

    /// Our own signed peer record bytes for provider announcements
    local_provider_record: Vec<u8>,
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

        // Generate secp256k1 key for DiscV5
        // Future: extract from libp2p keypair directly
        warn!(
            "Generating fresh secp256k1 key for DiscV5 (libp2p key extraction not yet implemented)"
        );
        let secret_key = enr::k256::ecdsa::SigningKey::random(&mut rand::thread_rng());

        let peer_id = keypair.public().to_peer_id();

        let enr_key = enr::CombinedKey::Secp256k1(secret_key);
        let mut builder = enr::Enr::builder();

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

        builder.add_value("libp2p", &peer_id.to_bytes());

        let enr = builder
            .build(&enr_key)
            .map_err(|e| DiscoveryError::EnrError(e.to_string()))?;

        info!("Local ENR: {}", enr.to_base64());
        info!("Local Peer ID: {}", peer_id);

        let listen_config = ListenConfig::Ipv4 {
            ip: match listen_addr.ip() {
                IpAddr::V4(ip) => ip,
                IpAddr::V6(_) => {
                    return Err(DiscoveryError::Discv5Error("IPv6 not yet supported".into()))
                }
            },
            port: listen_addr.port(),
        };

        let config = ConfigBuilder::new(listen_config).build();

        let mut discv5 = Discv5::new(enr, enr_key, config)
            .map_err(|e| DiscoveryError::Discv5Error(e.to_string()))?;

        discv5
            .start()
            .await
            .map_err(|e| DiscoveryError::Discv5Error(e.to_string()))?;

        info!("DiscV5 listening on {}", listen_addr);

        // Bootstrap from ENR strings
        for peer_str in &bootstrap_peers {
            if peer_str.starts_with("spr:") {
                continue; // SPR records handled below
            }
            match peer_str.parse::<enr::Enr<enr::CombinedKey>>() {
                Ok(bootstrap_enr) => match discv5.add_enr(bootstrap_enr.clone()) {
                    Ok(_) => info!("Added bootstrap peer: {}", bootstrap_enr.node_id()),
                    Err(e) => warn!("Failed to add bootstrap peer: {}", e),
                },
                Err(e) => warn!("Invalid bootstrap ENR {}: {}", peer_str, e),
            }
        }

        // Bootstrap from SPR records — these contain the UDP discovery addresses
        // and secp256k1 public keys of the Archivist devnet bootstrap nodes.
        let discv5_arc = Arc::new(discv5);
        for peer_str in &bootstrap_peers {
            if !peer_str.starts_with("spr:") {
                continue;
            }
            match parse_spr_records_full(peer_str) {
                Ok(records) => {
                    for record in records {
                        if let Err(e) = bootstrap_from_spr(&discv5_arc, &record).await {
                            warn!("Failed to bootstrap from SPR: {}", e);
                        }
                    }
                }
                Err(e) => warn!("Failed to parse SPR bootstrap: {}", e),
            }
        }

        // Build our local provider record (libp2p SignedPeerRecord) from keypair + announce addresses.
        let local_provider_record = build_provider_record(keypair, &announce_addrs);

        Ok(Self {
            discv5: discv5_arc,
            peer_id,
            provider_store: new_provider_store(),
            local_provider_record,
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

    /// Announce that we provide a specific CID to the DHT.
    ///
    /// Finds the K closest nodes to the CID's NodeId and sends
    /// AddProvider messages to each of them.
    pub async fn provide(&self, cid: &Cid) -> Result<()> {
        let node_id = cid_to_node_id(cid);
        let content_id = node_id.raw().to_vec();

        info!("Providing CID {} to DHT (NodeId: {})", cid, node_id);

        // Store locally too
        handle_add_provider(
            &self.provider_store,
            &content_id,
            self.local_provider_record.clone(),
        )
        .await;

        // Find K closest nodes to this content ID.
        // If find_node returns no peers, fall back to all known table entries.
        let closest_nodes = match self.discv5.find_node(node_id).await {
            Ok(nodes) if !nodes.is_empty() => nodes,
            Ok(_) => {
                // No peers from find_node — use all routing table entries as fallback
                let table_entries = self.discv5.table_entries_enr();
                if table_entries.is_empty() {
                    info!("No DHT peers available for provide, stored locally only");
                    return Ok(());
                }
                info!(
                    "find_node returned 0 peers, using {} routing table entries",
                    table_entries.len()
                );
                table_entries
            }
            Err(e) => {
                warn!("Failed to find nodes for provide: {}", e);
                let table_entries = self.discv5.table_entries_enr();
                if table_entries.is_empty() {
                    return Ok(());
                }
                table_entries
            }
        };

        info!(
            "Sending AddProvider to {} closest nodes",
            closest_nodes.len()
        );

        // Send AddProvider to each close node via talk_req
        // (The discv5 crate's talk_req sends type 0x05, but we need type 0x0B.
        //  We use the raw request API to send AddProvider directly.)
        for enr in &closest_nodes {
            let contact = match discv5::handler::NodeContact::try_from_enr(
                enr.clone(),
                self.discv5.ip_mode(),
            ) {
                Ok(c) => c,
                Err(_) => continue,
            };

            // Send AddProvider (0x0B) directly via the discv5 service.
            let discv5_clone = self.discv5.clone();
            let cid_clone = content_id.clone();
            let record_clone = self.local_provider_record.clone();
            let enr_clone = enr.clone();
            tokio::spawn(async move {
                match discv5_clone
                    .send_add_provider(contact, cid_clone, record_clone)
                    .await
                {
                    Ok(()) => info!("AddProvider sent to {}", enr_clone.node_id()),
                    Err(e) => debug!("AddProvider to {} failed: {}", enr_clone.node_id(), e),
                }
            });
        }

        info!("Provided CID {} to {} DHT nodes", cid, closest_nodes.len());
        Ok(())
    }

    /// Find providers for a specific CID from the DHT.
    pub async fn find(&self, cid: &Cid) -> Result<Vec<Vec<u8>>> {
        let node_id = cid_to_node_id(cid);
        let content_id = node_id.raw().to_vec();

        debug!("Finding providers for CID {} (NodeId: {})", cid, node_id);

        // Check local store first
        let (_, local_providers) =
            handle_get_providers(&self.provider_store, &content_id).await;
        if !local_providers.is_empty() {
            info!(
                "Found {} local providers for CID {}",
                local_providers.len(),
                cid
            );
            return Ok(local_providers);
        }

        // Query DHT (future: send GetProviders to K closest nodes)
        warn!("DHT GetProviders queries not yet fully implemented");

        Err(DiscoveryError::NoProviders(cid.to_string()))
    }

    /// Get connected peer count
    pub fn connected_peers(&self) -> usize {
        self.discv5.connected_peers()
    }

    /// Get the provider store for external use
    pub fn provider_store(&self) -> &SharedProviderStore {
        &self.provider_store
    }

    /// Run the discovery event loop
    pub async fn run(self: Arc<Self>) {
        info!("Starting DiscV5 event loop");

        let mut event_stream = match self.discv5.event_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                warn!("DiscV5 event stream failed to start: {}", e);
                return;
            }
        };

        while let Some(event) = event_stream.recv().await {
            self.handle_event(event).await;
        }

        warn!("DiscV5 event stream ended");
    }

    /// Handle DiscV5 events
    async fn handle_event(&self, event: Discv5Event) {
        match event {
            Discv5Event::Discovered(enr) => {
                debug!("Discovered peer: {}", enr.node_id());
                if let Some(Ok(peer_id_bytes)) = enr.get_decodable::<Vec<u8>>("libp2p") {
                    if let Ok(peer_id) = PeerId::from_bytes(&peer_id_bytes) {
                        info!(
                            "Discovered libp2p peer: {} (ENR: {})",
                            peer_id,
                            enr.node_id()
                        );
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
                info!(
                    "Session established with {} at {}",
                    enr.node_id(),
                    socket_addr
                );
            }
            Discv5Event::ProviderRequest(req) => {
                match req.body() {
                    RequestBody::AddProvider {
                        content_id,
                        provider_record,
                    } => {
                        info!(
                            "Received AddProvider from {} for content {}",
                            req.node_id(),
                            hex::encode(&content_id[..8.min(content_id.len())])
                        );
                        handle_add_provider(
                            &self.provider_store,
                            content_id,
                            provider_record.clone(),
                        )
                        .await;
                        // AddProvider is fire-and-forget, no response needed.
                        // Drop the request (which will not send a response since
                        // the Drop impl only auto-responds for GetProviders).
                    }
                    RequestBody::GetProviders { content_id } => {
                        info!(
                            "Received GetProviders from {} for content {}",
                            req.node_id(),
                            hex::encode(&content_id[..8.min(content_id.len())])
                        );
                        let (total, providers) =
                            handle_get_providers(&self.provider_store, content_id).await;
                        req.respond(ResponseBody::Providers { total, providers });
                    }
                    _ => {
                        warn!("Unexpected request body in ProviderRequest");
                    }
                }
            }
            _ => {
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

/// Bootstrap the DiscV5 DHT from an Archivist SPR record.
///
/// Creates a synthetic ENR from the SPR's public key and UDP address,
/// then adds it as a bootstrap peer and pings it to establish a session.
async fn bootstrap_from_spr(
    discv5: &Arc<Discv5>,
    record: &SprRecord,
) -> std::result::Result<(), String> {
    let pubkey_bytes = record
        .secp256k1_pubkey
        .as_ref()
        .ok_or_else(|| "SPR has no secp256k1 public key".to_string())?;

    // Extract UDP address from the SPR multiaddrs
    let mut udp_addr: Option<SocketAddr> = None;
    for addr in &record.addrs {
        let addr_str = addr.to_string();
        // SPR addresses are like /ip4/X.X.X.X/udp/PORT
        let parts: Vec<&str> = addr_str.split('/').collect();
        if parts.len() >= 5 && parts[1] == "ip4" && parts[3] == "udp" {
            if let (Ok(ip), Ok(port)) = (parts[2].parse::<Ipv4Addr>(), parts[4].parse::<u16>()) {
                udp_addr = Some(SocketAddr::new(IpAddr::V4(ip), port));
                break;
            }
        }
    }

    let socket_addr = udp_addr.ok_or_else(|| "No UDP address found in SPR".to_string())?;

    // Build ENR from the SPR's verified public key — this gives us the correct
    // NodeId (keccak256 of the uncompressed secp256k1 key), matching what the
    // Archivist node uses as its own identity in the DHT.
    let (ip4, udp4) = match socket_addr {
        SocketAddr::V4(v4) => (Some(*v4.ip()), Some(v4.port())),
        _ => (None, None),
    };
    let bootstrap_enr =
        enr::Enr::<enr::CombinedKey>::from_verified_spr(pubkey_bytes, 1, ip4, udp4, None)
            .map_err(|e| format!("Failed to build ENR from SPR: {:?}", e))?;

    let node_id = bootstrap_enr.node_id();

    info!(
        "Bootstrapping DiscV5 from SPR: {} at {} (NodeId: {})",
        record.peer_id, socket_addr, node_id
    );

    // Add the ENR to the routing table (even though the signature won't match,
    // the key fields will allow session establishment)
    match discv5.add_enr(bootstrap_enr) {
        Ok(_) => info!("Added SPR bootstrap to routing table"),
        Err(e) => warn!("Failed to add SPR bootstrap to routing table: {}", e),
    }

    // Find nodes close to ourselves — this populates our routing table
    // with the bootstrap node's neighbors.
    let discv5_clone = discv5.clone();
    let our_node_id = discv5.local_enr().node_id();
    tokio::spawn(async move {
        // First find our own neighborhood (distance 0-256 from us)
        match discv5_clone.find_node(our_node_id).await {
            Ok(nodes) => {
                info!(
                    "DHT bootstrap: discovered {} peers in our neighborhood from {}",
                    nodes.len(),
                    socket_addr
                );
            }
            Err(e) => {
                debug!(
                    "DHT bootstrap find_node (self) to {} failed: {}",
                    socket_addr, e
                );
            }
        }
        // Then find nodes close to the bootstrap node to populate more of the table
        match discv5_clone.find_node(node_id).await {
            Ok(nodes) => {
                info!(
                    "DHT bootstrap: discovered {} peers near bootstrap {} from {}",
                    nodes.len(),
                    node_id,
                    socket_addr
                );
            }
            Err(e) => {
                debug!(
                    "DHT bootstrap find_node (target) to {} failed: {}",
                    socket_addr, e
                );
            }
        }
    });

    Ok(())
}

/// Build a proper libp2p SignedPeerRecord for provider announcements.
///
/// This is the format Archivist expects in AddProvider messages: a
/// protobuf-encoded SignedEnvelope containing a PeerRecord with our
/// peer ID and announce addresses.
fn build_provider_record(
    keypair: &libp2p::identity::Keypair,
    announce_addrs: &[String],
) -> Vec<u8> {
    use libp2p::core::{PeerRecord, SignedEnvelope};
    use libp2p::Multiaddr;

    let peer_id = keypair.public().to_peer_id();
    let addrs: Vec<Multiaddr> = announce_addrs
        .iter()
        .filter_map(|a| a.parse().ok())
        .collect();

    let peer_record = PeerRecord::new(keypair, addrs)
        .expect("Failed to create PeerRecord");
    let envelope = peer_record.into_signed_envelope();
    envelope.into_protobuf_encoding()
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
    async fn test_provide_stores_locally() {
        let keypair = Keypair::generate_secp256k1();
        let listen_addr = "127.0.0.1:9001".parse().unwrap();
        let announce_addrs = vec!["/ip4/127.0.0.1/tcp/8070".to_string()];

        let discovery = Discovery::new(&keypair, listen_addr, announce_addrs, vec![])
            .await
            .unwrap();

        let cid: Cid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            .parse()
            .unwrap();

        // Provide should store locally even without DHT peers
        discovery.provide(&cid).await.unwrap();

        // Find should return our local record
        let providers = discovery.find(&cid).await.unwrap();
        assert_eq!(providers.len(), 1);
    }
}
