//! Block-over-TGP (BoTG) Protocol
//!
//! Two-layer fault-tolerant block exchange protocol:
//! - Layer 1: TGP (Two Generals Protocol) provides reliable high-speed transport (12-13x faster than TCP)
//! - Layer 2: BoTG provides rollup-based block exchange with instant convergence
//!
//! Design Philosophy:
//! - Replace WantList-based individual requests with batch rollups
//! - Leverage TGP's linear degradation under packet loss
//! - Achieve instant convergence even at 99% packet loss (TGP: 1+ Mbps)
//! - Optimize for Neverust-to-Neverust block exchange

use cid::Cid;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Stream identifier for BoTG/TGP sessions.
pub type StreamId = u128;

/// Local compatibility struct for TGP configuration.
///
/// The original external `consensus-tgp` crate is not available in this
/// workspace, so BoTG keeps the config surface locally for now.
#[derive(Debug, Clone)]
pub struct TgpConfig {
    pub stream_id: StreamId,
    pub epoch: u32,
    pub local_id: u64,
    pub peer_id: u64,
    pub mtu: usize,
    pub target_mbps: u32,
}

/// Placeholder handle for an active TGP session.
#[derive(Debug, Clone)]
pub struct TgpHandle;

/// UDP transport configuration for BoTG messaging.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub bind: SocketAddr,
    pub batch: usize,
    pub sndbuf: usize,
    pub rcvbuf: usize,
}

/// Lightweight UDP transport handle used by BoTG.
#[derive(Debug)]
pub struct TransportHandle {
    socket: Arc<tokio::net::UdpSocket>,
}

impl TransportHandle {
    pub async fn new(config: TransportConfig) -> Result<Self, std::io::Error> {
        let socket = tokio::net::UdpSocket::bind(config.bind).await?;
        Ok(Self {
            socket: Arc::new(socket),
        })
    }

    pub fn socket(&self) -> Arc<tokio::net::UdpSocket> {
        Arc::clone(&self.socket)
    }
}

/// BoTG message types for UDP communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BoTgMessage {
    /// Announce that we have blocks available
    Announce {
        /// CIDs of blocks we have
        cids: Vec<Vec<u8>>,
    },
    /// Request blocks from a peer
    Request {
        /// CIDs of blocks we want
        cids: Vec<Vec<u8>>,
    },
    /// Response with block data
    Response {
        /// CID of the block
        cid: Vec<u8>,
        /// Block data
        data: Vec<u8>,
    },
}

/// Block identifier (CID-compatible)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BlockId {
    /// Raw CID bytes
    pub cid: Vec<u8>,
}

/// Block rollup - a batch of blocks being exchanged
#[derive(Debug, Clone)]
pub struct BlockRollup {
    /// Unique rollup ID
    pub id: u64,
    /// Block IDs in this rollup
    pub blocks: Vec<BlockId>,
    /// Total size in bytes
    pub total_size: u64,
    /// Priority (higher = more urgent)
    pub priority: u8,
}

/// BoTG protocol configuration
#[derive(Debug, Clone)]
pub struct BoTgConfig {
    /// Maximum rollup size (number of blocks)
    pub max_rollup_size: usize,
    /// Maximum rollup bytes
    pub max_rollup_bytes: u64,
    /// TGP MTU (from PROTOCOL_COMPARISON.md: 1200 bytes optimal)
    pub mtu: usize,
    /// Local peer ID
    pub local_peer_id: u64,
    /// TGP epoch
    pub epoch: u32,
}

impl Default for BoTgConfig {
    fn default() -> Self {
        Self {
            max_rollup_size: 1000,
            max_rollup_bytes: 100 * 1024 * 1024, // 100 MB per rollup
            mtu: 1200,                           // Optimal MTU from TGP benchmarks
            local_peer_id: rand::random(),
            epoch: 0,
        }
    }
}

/// BoTG protocol state machine
pub struct BoTgProtocol {
    config: BoTgConfig,
    /// Active TGP handles by peer
    handles: Arc<RwLock<HashMap<u64, TgpHandle>>>,
    /// Pending outbound rollups
    pending_rollups: Arc<RwLock<Vec<BlockRollup>>>,
    /// Blocks we have locally
    local_blocks: Arc<RwLock<HashSet<BlockId>>>,
    /// Blocks we want from peers
    want_blocks: Arc<RwLock<HashSet<BlockId>>>,
    /// Channel to announce blocks to all connected peers
    _announce_tx: Option<mpsc::Sender<Vec<BlockId>>>,
    /// Known peer addresses (for UDP communication)
    peer_addrs: Arc<RwLock<Vec<SocketAddr>>>,
    /// UDP socket for BoTG messages
    udp_socket: Option<Arc<tokio::net::UdpSocket>>,
    /// Block store for retrieving blocks
    block_store: Option<Arc<crate::storage::BlockStore>>,
    /// Metrics for tracking BoTG traffic
    metrics: Option<crate::metrics::Metrics>,
}

impl BoTgProtocol {
    /// Create a new BoTG protocol instance
    pub fn new(config: BoTgConfig) -> Self {
        Self {
            config,
            handles: Arc::new(RwLock::new(HashMap::new())),
            pending_rollups: Arc::new(RwLock::new(Vec::new())),
            local_blocks: Arc::new(RwLock::new(HashSet::new())),
            want_blocks: Arc::new(RwLock::new(HashSet::new())),
            _announce_tx: None,
            peer_addrs: Arc::new(RwLock::new(Vec::new())),
            udp_socket: None,
            block_store: None,
            metrics: None,
        }
    }

    /// Set the UDP socket for BoTG communication
    pub fn set_udp_socket(&mut self, socket: Arc<tokio::net::UdpSocket>) {
        self.udp_socket = Some(socket);
    }

    /// Set the block store
    pub fn set_block_store(&mut self, store: Arc<crate::storage::BlockStore>) {
        self.block_store = Some(store);
    }

    /// Set the metrics
    pub fn set_metrics(&mut self, metrics: crate::metrics::Metrics) {
        self.metrics = Some(metrics);
    }

    /// Add a peer address for BoTG communication
    pub async fn add_peer(&self, addr: SocketAddr) {
        let mut peers = self.peer_addrs.write().await;
        if !peers.contains(&addr) {
            info!("BoTG: Added peer {}", addr);
            peers.push(addr);
        }
    }

    /// Send a BoTG message to a peer via UDP
    async fn send_message(&self, addr: SocketAddr, msg: &BoTgMessage) -> Result<(), BoTgError> {
        if let Some(socket) = &self.udp_socket {
            let data = bincode::serialize(msg).map_err(|e| {
                BoTgError::EncodingError(format!("Failed to serialize message: {}", e))
            })?;

            socket
                .send_to(&data, addr)
                .await
                .map_err(|e| BoTgError::TgpError(format!("Failed to send UDP: {}", e)))?;

            // Track metrics
            if let Some(metrics) = &self.metrics {
                metrics.block_sent(data.len());
            }

            debug!("BoTG: Sent {} bytes to {}", data.len(), addr);
            Ok(())
        } else {
            Err(BoTgError::TgpError(
                "UDP socket not initialized".to_string(),
            ))
        }
    }

    /// Announce that we have new blocks (called when blocks are stored)
    pub async fn announce_blocks(&self, cids: Vec<Cid>) {
        let block_ids: Vec<BlockId> = cids.iter().map(Self::cid_to_block_id).collect();

        info!("BoTG: Announcing {} new blocks to network", block_ids.len());

        // Add to our local blocks
        {
            let mut local = self.local_blocks.write().await;
            for id in &block_ids {
                local.insert(id.clone());
            }
        }

        // Send announcement to all known peers via UDP
        let peers = self.peer_addrs.read().await;
        if !peers.is_empty() {
            let cid_bytes: Vec<Vec<u8>> = cids.iter().map(|c| c.to_bytes()).collect();
            let msg = BoTgMessage::Announce { cids: cid_bytes };

            for peer_addr in peers.iter() {
                if let Err(e) = self.send_message(*peer_addr, &msg).await {
                    warn!("BoTG: Failed to announce to {}: {}", peer_addr, e);
                }
            }
            info!(
                "BoTG: Announced {} blocks to {} peers via UDP",
                cids.len(),
                peers.len()
            );
        } else {
            debug!("BoTG: No peers to announce to");
        }
    }

    /// Request blocks from the network (called when we need blocks)
    pub async fn request_blocks_by_cid(&self, cids: Vec<Cid>) {
        let block_ids: Vec<BlockId> = cids.iter().map(Self::cid_to_block_id).collect();

        info!("BoTG: Requesting {} blocks from network", block_ids.len());

        // Add to our want list
        {
            let mut wants = self.want_blocks.write().await;
            for id in &block_ids {
                wants.insert(id.clone());
            }
        }

        // Send request to all known peers via UDP
        let peers = self.peer_addrs.read().await;
        if !peers.is_empty() {
            let cid_bytes: Vec<Vec<u8>> = cids.iter().map(|c| c.to_bytes()).collect();
            let msg = BoTgMessage::Request { cids: cid_bytes };

            for peer_addr in peers.iter() {
                if let Err(e) = self.send_message(*peer_addr, &msg).await {
                    warn!("BoTG: Failed to request from {}: {}", peer_addr, e);
                }
            }
            info!(
                "BoTG: Requested {} blocks from {} peers via UDP",
                cids.len(),
                peers.len()
            );
        } else {
            debug!("BoTG: No peers to request from");
        }
    }

    /// Create a new BoTG protocol with UDP transport
    pub async fn new_with_transport(
        config: BoTgConfig,
        bind_addr: SocketAddr,
    ) -> Result<(Self, Arc<TransportHandle>), BoTgError> {
        info!("BoTG: Creating UDP transport on {}", bind_addr);

        // Create UDP transport
        let transport_config = TransportConfig {
            bind: bind_addr,
            batch: 64,               // Batch size for packet processing
            sndbuf: 4 * 1024 * 1024, // 4MB send buffer
            rcvbuf: 4 * 1024 * 1024, // 4MB receive buffer
        };

        let transport =
            Arc::new(TransportHandle::new(transport_config).await.map_err(|e| {
                BoTgError::TgpError(format!("Failed to create UDP transport: {}", e))
            })?);

        info!("BoTG: UDP transport ready on {}", bind_addr);

        let protocol = Self::new(config);
        Ok((protocol, transport))
    }

    /// Convert CID to BlockId
    pub fn cid_to_block_id(cid: &Cid) -> BlockId {
        BlockId {
            cid: cid.to_bytes(),
        }
    }

    /// Convert BlockId to CID
    pub fn block_id_to_cid(block_id: &BlockId) -> Result<Cid, BoTgError> {
        Cid::try_from(&block_id.cid[..])
            .map_err(|e| BoTgError::EncodingError(format!("Invalid CID: {}", e)))
    }

    /// Create a TGP handle for a peer
    pub async fn connect_to_peer(&self, peer_id: u64) -> Result<(), BoTgError> {
        info!("BoTG: Setting up TGP handle for peer {}", peer_id);

        // Generate unique stream ID from epoch + local_id + peer_id
        // StreamId is u128, we'll pack: [epoch:32][local_id:64][peer_id:32]
        let stream_id: StreamId = ((self.config.epoch as u128) << 96)
            | ((self.config.local_peer_id as u128) << 32)
            | (peer_id as u128);

        let _tgp_config = TgpConfig {
            stream_id,
            epoch: self.config.epoch,
            local_id: self.config.local_peer_id,
            peer_id,
            mtu: self.config.mtu,
            target_mbps: 100, // Default 100 Mbps target
        };

        // TODO: Create TGP handle with actual transport and peer address
        // This requires:
        // 1. TransportHandle (UDP socket)
        // 2. Peer SocketAddr (extracted from multiaddr)
        // For now, just track the config
        // let handle = TgpHandle::new(_tgp_config, transport, peer_addr);

        // Store config for future use when transport is wired up
        // self.handles.write().await.insert(peer_id, handle);

        info!(
            "BoTG: TGP config created for peer {} (transport integration pending)",
            peer_id
        );
        Ok(())
    }

    /// Request blocks from a peer using rollup protocol
    pub async fn request_blocks(
        &self,
        peer_id: u64,
        blocks: Vec<BlockId>,
    ) -> Result<(), BoTgError> {
        debug!(
            "BoTG: Requesting {} blocks from peer {}",
            blocks.len(),
            peer_id
        );

        // Create rollup from requested blocks
        let rollup = BlockRollup {
            id: rand::random(),
            blocks,
            total_size: 0, // Will be calculated when blocks arrive
            priority: 128, // Medium priority
        };

        // Add to pending rollups
        self.pending_rollups.write().await.push(rollup.clone());

        // Send rollup request over TGP
        let handles = self.handles.read().await;
        if handles.get(&peer_id).is_some() {
            // Serialize rollup request (TODO: implement proper encoding)
            let _request_bytes = self.encode_rollup_request(&rollup)?;

            // TODO: Use TGP handle's start_streaming to send data
            // For now, just track the rollup request
            debug!(
                "BoTG: Queued rollup request {} for peer {}",
                rollup.id, peer_id
            );
            Ok(())
        } else {
            Err(BoTgError::NoPeerConnection(peer_id))
        }
    }

    /// Handle incoming rollup response
    pub async fn handle_rollup_response(&self, peer_id: u64, data: &[u8]) -> Result<(), BoTgError> {
        debug!("BoTG: Received {} bytes from peer {}", data.len(), peer_id);

        // Decode rollup response (TODO: implement proper decoding)
        let blocks = self.decode_rollup_response(data)?;

        // Store received blocks
        let mut local_blocks = self.local_blocks.write().await;
        for block in blocks {
            local_blocks.insert(block);
        }

        info!("BoTG: Received and stored blocks from peer {}", peer_id);
        Ok(())
    }

    /// Mark blocks as locally available
    pub async fn add_local_blocks(&self, blocks: Vec<BlockId>) {
        let mut local_blocks = self.local_blocks.write().await;
        for block in blocks {
            local_blocks.insert(block);
        }
    }

    /// Mark blocks as wanted
    pub async fn add_wanted_blocks(&self, blocks: Vec<BlockId>) {
        let mut want_blocks = self.want_blocks.write().await;
        for block in blocks {
            want_blocks.insert(block);
        }
    }

    // TODO: Implement proper protobuf encoding/decoding
    fn encode_rollup_request(&self, rollup: &BlockRollup) -> Result<Vec<u8>, BoTgError> {
        // Placeholder: simple binary encoding
        // Format: [rollup_id:8][num_blocks:4][block_cid_len:4][block_cid:N]...
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&rollup.id.to_le_bytes());
        bytes.extend_from_slice(&(rollup.blocks.len() as u32).to_le_bytes());

        for block in &rollup.blocks {
            bytes.extend_from_slice(&(block.cid.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&block.cid);
        }

        Ok(bytes)
    }

    fn decode_rollup_response(&self, _data: &[u8]) -> Result<Vec<BlockId>, BoTgError> {
        // Placeholder: will implement proper decoding
        Ok(Vec::new())
    }

    /// Start UDP receive loop to handle incoming BoTG messages
    pub fn start_receive_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            if let Some(socket) = &self.udp_socket {
                info!("BoTG: Starting UDP receive loop");
                let mut buf = vec![0u8; 65536]; // 64KB buffer

                loop {
                    match socket.recv_from(&mut buf).await {
                        Ok((len, peer_addr)) => {
                            // Track received bytes
                            if let Some(metrics) = &self.metrics {
                                metrics.block_received(len);
                            }

                            debug!("BoTG: Received {} bytes from {}", len, peer_addr);

                            // Deserialize message
                            match bincode::deserialize::<BoTgMessage>(&buf[..len]) {
                                Ok(msg) => {
                                    if let Err(e) = self.handle_message(peer_addr, msg).await {
                                        warn!(
                                            "BoTG: Failed to handle message from {}: {}",
                                            peer_addr, e
                                        );
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "BoTG: Failed to deserialize message from {}: {}",
                                        peer_addr, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!("BoTG: UDP receive error: {}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            } else {
                error!("BoTG: Cannot start receive loop - UDP socket not initialized");
            }
        });
    }

    /// Handle incoming BoTG message
    async fn handle_message(
        &self,
        peer_addr: SocketAddr,
        msg: BoTgMessage,
    ) -> Result<(), BoTgError> {
        match msg {
            BoTgMessage::Announce { cids } => {
                info!(
                    "BoTG: Received announcement of {} blocks from {}",
                    cids.len(),
                    peer_addr
                );
                // Add peer to our known peers
                self.add_peer(peer_addr).await;
                // Could request these blocks if we need them
                Ok(())
            }
            BoTgMessage::Request { cids } => {
                info!(
                    "BoTG: Received request for {} blocks from {}",
                    cids.len(),
                    peer_addr
                );
                self.handle_block_request(peer_addr, cids).await
            }
            BoTgMessage::Response { cid, data } => {
                info!(
                    "BoTG: Received block response ({} bytes) from {}",
                    data.len(),
                    peer_addr
                );
                self.handle_block_response(cid, data).await
            }
        }
    }

    /// Handle block request - send block data if we have it
    async fn handle_block_request(
        &self,
        peer_addr: SocketAddr,
        cids: Vec<Vec<u8>>,
    ) -> Result<(), BoTgError> {
        if let Some(store) = &self.block_store {
            for cid_bytes in cids {
                // Convert to CID
                if let Ok(cid) = Cid::try_from(&cid_bytes[..]) {
                    // Try to fetch block from store
                    if let Ok(block) = store.get(&cid).await {
                        info!(
                            "BoTG: Sending block {} ({} bytes) to {}",
                            cid,
                            block.data.len(),
                            peer_addr
                        );

                        // Send block data as response
                        let response = BoTgMessage::Response {
                            cid: cid_bytes,
                            data: block.data,
                        };

                        self.send_message(peer_addr, &response).await?;
                    } else {
                        debug!("BoTG: Don't have block {} requested by {}", cid, peer_addr);
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle block response - store received block
    async fn handle_block_response(
        &self,
        cid_bytes: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<(), BoTgError> {
        if let Some(store) = &self.block_store {
            if let Ok(cid) = Cid::try_from(&cid_bytes[..]) {
                // Create block and store it
                let block = crate::storage::Block { cid, data };

                match store.put(block).await {
                    Ok(_) => {
                        info!("BoTG: Stored received block {}", cid);
                        Ok(())
                    }
                    Err(e) => Err(BoTgError::TgpError(format!("Failed to store block: {}", e))),
                }
            } else {
                Err(BoTgError::DecodingError("Invalid CID".to_string()))
            }
        } else {
            Err(BoTgError::TgpError("Block store not available".to_string()))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BoTgError {
    #[error("TGP error: {0}")]
    TgpError(String),

    #[error("No connection to peer {0}")]
    NoPeerConnection(u64),

    #[error("Encoding error: {0}")]
    EncodingError(String),

    #[error("Decoding error: {0}")]
    DecodingError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_rollup_creation() {
        let blocks = vec![
            BlockId { cid: vec![1, 2, 3] },
            BlockId { cid: vec![4, 5, 6] },
        ];

        let rollup = BlockRollup {
            id: 42,
            blocks: blocks.clone(),
            total_size: 1024,
            priority: 255,
        };

        assert_eq!(rollup.id, 42);
        assert_eq!(rollup.blocks.len(), 2);
        assert_eq!(rollup.priority, 255);
    }

    #[tokio::test]
    async fn test_botg_protocol_creation() {
        let config = BoTgConfig::default();
        let protocol = BoTgProtocol::new(config);

        let blocks = vec![BlockId { cid: vec![1, 2, 3] }];

        protocol.add_local_blocks(blocks.clone()).await;
        protocol.add_wanted_blocks(blocks).await;
    }

    #[test]
    fn test_encode_rollup_request() {
        let config = BoTgConfig::default();
        let protocol = BoTgProtocol::new(config);

        let rollup = BlockRollup {
            id: 123,
            blocks: vec![BlockId { cid: vec![1, 2, 3] }],
            total_size: 100,
            priority: 128,
        };

        let encoded = protocol.encode_rollup_request(&rollup).unwrap();
        assert!(encoded.len() > 0);

        // Verify format: [rollup_id:8][num_blocks:4][block_cid_len:4][block_cid:3]
        assert_eq!(encoded.len(), 8 + 4 + 4 + 3);
    }
}
