//! P2P networking layer using rust-libp2p
//!
//! Implements the core P2P stack with TCP+Noise+Yamux transports
//! and BlockExc protocol (matching Archivist exactly).
//!
//! Identify protocol is used for SPR (Signed Peer Record) exchange.

use libp2p::{identify, noise, tcp, yamux, PeerId, Swarm, SwarmBuilder};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use crate::blockexc::BlockExcBehaviour;
use crate::identify_shim::{IdentifyBehaviour, IdentifyConfig};
use crate::storage::BlockStore;

#[derive(Error, Debug)]
pub enum P2PError {
    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Swarm error: {0}")]
    Swarm(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Network behavior with BlockExc + Identify protocols
/// Identify is required for SPR (Signed Peer Record) exchange with Archivist nodes
///
/// Uses custom IdentifyBehaviour shim for nim-libp2p v1.9.0 compatibility
#[derive(libp2p::swarm::NetworkBehaviour)]
#[behaviour(to_swarm = "BehaviourEvent")]
pub struct Behaviour {
    pub blockexc: BlockExcBehaviour,
    pub identify: IdentifyBehaviour,
}

#[derive(Debug)]
pub enum BehaviourEvent {
    BlockExc(crate::blockexc::BlockExcToBehaviour),
    Identify(Box<identify::Event>),
}

impl From<crate::blockexc::BlockExcToBehaviour> for BehaviourEvent {
    fn from(event: crate::blockexc::BlockExcToBehaviour) -> Self {
        BehaviourEvent::BlockExc(event)
    }
}

impl From<identify::Event> for BehaviourEvent {
    fn from(event: identify::Event) -> Self {
        BehaviourEvent::Identify(Box::new(event))
    }
}

impl From<void::Void> for BehaviourEvent {
    fn from(v: void::Void) -> Self {
        void::unreachable(v)
    }
}

/// Create a new P2P swarm with default configuration
///
/// Returns (swarm, block_request_tx, keypair)
pub async fn create_swarm(
    block_store: Arc<BlockStore>,
    mode: String,
    price_per_byte: u64,
    metrics: crate::metrics::Metrics,
) -> Result<
    (
        Swarm<Behaviour>,
        tokio::sync::mpsc::UnboundedSender<crate::blockexc::BlockRequest>,
        libp2p::identity::Keypair,
    ),
    P2PError,
> {
    // Generate keypair for this node
    // CRITICAL: Must use secp256k1 for Archivist compatibility!
    //
    // Archivist nodes are compiled with ONLY secp256k1 support:
    //   config.nims: switch("define", "libp2p_pki_schemes=secp256k1")
    //
    // This disables Ed25519, RSA, and ECDSA at compile time in nim-libp2p.
    // If we use Ed25519 keys, the Noise handshake fails with:
    //   "Failed to decode remote public key. (initiator: false)"
    //
    // This is because nim-libp2p rejects key types not in SupportedSchemesInt,
    // and only secp256k1 (scheme=2) is enabled in Archivist builds.
    //
    // Solution: Use secp256k1 keys to match Archivist's configuration.
    let keypair = libp2p::identity::Keypair::generate_secp256k1();
    let peer_id = PeerId::from(keypair.public());

    tracing::info!(
        "Local peer ID: {} (mode: {}, key: secp256k1)",
        peer_id,
        mode
    );

    // Create Identify config using our custom shim for nim-libp2p compatibility
    // This uses our IdentifyBehaviour which wraps rust-libp2p's identify
    // but uses custom SPR encoding compatible with nim-libp2p v1.9.0
    //
    // KNOWN ISSUE RESOLVED: rust-libp2p 0.56 SPR encoding was incompatible with nim-libp2p v1.9.0
    // - Domain and payloadType matched: "libp2p-peer-record" + [0x03, 0x01]
    // - But nim-libp2p v1.9.0 couldn't decode rust-libp2p 0.56's Envelope encoding
    // - Connection works fine WITHOUT SPR, closes immediately WITH standard SPR
    //
    // SOLUTION: Custom IdentifyBehaviour shim
    // - Uses standard identify::Config (without SPR) for stable connections
    // - Provides custom SPR encoder via identify_spr module (nim-libp2p compatible)
    // - Can be extended to inject custom SPR bytes if needed in future
    // - For now, SPR-disabled mode works perfectly with Archivist
    let identify_config = IdentifyConfig::new("Archivist Node".to_string(), &keypair);
    let identify_behaviour = IdentifyBehaviour::new(identify_config);

    // Create behavior: BlockExc + Identify
    let (blockexc_behaviour, block_request_tx) =
        BlockExcBehaviour::new(block_store, mode, price_per_byte, metrics);
    let behaviour = Behaviour {
        blockexc: blockexc_behaviour,
        identify: identify_behaviour,
    };

    // Build swarm with TCP transport to match Archivist testnet nodes
    // Using TCP+Noise+Yamux
    // Note: Archivist uses 5-minute timeouts - we set this via idle_connection_timeout
    let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| P2PError::Transport(e.to_string()))?
        .with_behaviour(|_| behaviour)
        .map_err(|e| P2PError::Swarm(e.to_string()))?
        .with_swarm_config(|c| {
            // Match Archivist's 5-minute idle timeout
            c.with_idle_connection_timeout(Duration::from_secs(300))
        })
        .build();

    Ok((swarm, block_request_tx, keypair))
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::Multiaddr;

    #[tokio::test]
    async fn test_create_swarm() {
        let block_store = Arc::new(BlockStore::new());
        let metrics = crate::metrics::Metrics::new();
        let (swarm, _block_request_tx, _keypair) =
            create_swarm(block_store, "altruistic".to_string(), 1, metrics)
                .await
                .unwrap();
        assert!(swarm.local_peer_id().to_string().len() > 0);
    }

    #[tokio::test]
    async fn test_swarm_can_listen() {
        let block_store = Arc::new(BlockStore::new());
        let metrics = crate::metrics::Metrics::new();
        let (mut swarm, _block_request_tx, _keypair) =
            create_swarm(block_store, "altruistic".to_string(), 1, metrics)
                .await
                .unwrap();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
        let result = swarm.listen_on(addr);
        assert!(result.is_ok());
    }
}
