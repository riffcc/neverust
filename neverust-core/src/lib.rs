//! Neverust Core
//!
//! Core P2P networking and storage functionality for the Archivist node.

pub mod api;
pub mod archivist_cluster;
pub mod archivist_tree;
pub mod blockexc;
pub mod botg;
pub mod chunker;
pub mod cluster;
pub mod citadel;
pub mod citadel_sync;
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
pub mod primitive_lab;
pub mod primitive_pipeline;
pub mod runtime;
pub mod spr;
pub mod storage;
pub mod traffic;

pub use archivist_tree::{ArchivistProof, ArchivistTree, ProofNode};
pub use archivist_cluster::{ArchivistCluster, ClusterError, ClusterMember, MemberBackend, PinOutcome};
pub use botg::{BlockId, BlockRollup, BoTgConfig, BoTgError, BoTgProtocol};
pub use chunker::{Chunker, DEFAULT_BLOCK_SIZE};
pub use cluster::{select_replicas, upload_path_for_cid_str, ClusterNode};
pub use citadel::{
    fetch_flagship_trust_snapshot, run_defederation_simulation, DefederationGuardConfig,
    DefederationNode, DefederationSimulationConfig, DefederationSimulationResult,
    FlagshipTrustSnapshot, IdleBandwidthGateConfig, LensGraph, LensOp, LensOpKind,
};
pub use cid_blake3::{blake3_cid, blake3_hash, verify_blake3, CidError, StreamingVerifier};
pub use config::Config;
pub use discovery::{Discovery, DiscoveryError, DiscoveryStats};
pub use manifest::{
    ErasureInfo, Manifest, ManifestError, StrategyType, VerificationInfo, BLAKE3_CODEC,
    BLOCK_CODEC, MANIFEST_CODEC, SHA256_CODEC,
};
pub use metrics::Metrics;
pub use p2p::{create_swarm, Behaviour, P2PError};
pub use runtime::run_node;
pub use spr::{parse_spr_records, SprError};
pub use storage::{Block, BlockStore, BlockStoreStats, StorageError};
