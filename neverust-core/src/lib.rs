//! Neverust Core
//!
//! Core P2P networking and storage functionality for the Archivist node.

pub mod api;
pub mod archivist_tree;
pub mod blockexc;
pub mod botg;
pub mod chunker;
pub mod cid_blake3;
pub mod config;
pub mod discovery;
pub mod identify_shim;
pub mod identify_spr;
pub mod manifest;
pub mod messages;
pub mod metrics;
pub mod p2p;
pub mod pending_blocks;
pub mod runtime;
pub mod spr;
pub mod storage;
pub mod traffic;

pub use archivist_tree::{ArchivistProof, ArchivistTree, ProofNode};
pub use botg::{BlockId, BlockRollup, BoTgConfig, BoTgError, BoTgProtocol};
pub use chunker::{Chunker, DEFAULT_BLOCK_SIZE};
pub use cid_blake3::{blake3_cid, blake3_hash, verify_blake3, CidError, StreamingVerifier};
pub use config::Config;
pub use discovery::{Discovery, DiscoveryError, DiscoveryStats};
pub use manifest::{
    ErasureInfo, Manifest, ManifestError, StrategyType, VerificationInfo, BLOCK_CODEC,
    BLAKE3_CODEC, MANIFEST_CODEC, SHA256_CODEC,
};
pub use metrics::Metrics;
pub use p2p::{create_swarm, Behaviour, P2PError};
pub use runtime::run_node;
pub use spr::{parse_spr_records, SprError};
pub use storage::{Block, BlockStore, BlockStoreStats, StorageError};
