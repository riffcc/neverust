//! Archivist Manifest implementation
//!
//! Manifests (codec 0xcd01) describe datasets with metadata and tree CID.
//! They are encoded using protobuf and stored as blocks in the network.

use cid::Cid;
use prost::Message as ProstMessage;
use std::io::Cursor;
use thiserror::Error;

use crate::storage::Block;

/// Archivist manifest codec (0xcd01)
pub const MANIFEST_CODEC: u64 = 0xcd01;

/// Default block codec (0xcd02)
pub const BLOCK_CODEC: u64 = 0xcd02;

/// SHA-256 multihash codec
pub const SHA256_CODEC: u64 = 0x12;

/// BLAKE3 multihash codec
pub const BLAKE3_CODEC: u64 = 0x1e;

/// Default block size (64KB)
pub const DEFAULT_BLOCK_SIZE: u64 = 64 * 1024;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("Protobuf encode error: {0}")]
    EncodeError(#[from] prost::EncodeError),

    #[error("Protobuf decode error: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("CID error: {0}")]
    CidError(String),

    #[error("Invalid manifest: {0}")]
    InvalidManifest(String),

    #[error("Multihash error: {0}")]
    MultihashError(String),
}

pub type Result<T> = std::result::Result<T, ManifestError>;

/// Indexing strategy type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyType {
    /// Stepped strategy: slot 0 => blocks [0,3,6], slot 1 => blocks [1,4,7], ...
    SteppedStrategy = 0,
    /// Linear strategy: slot 0 => blocks [0,1,2], slot 1 => blocks [3,4,5], ...
    LinearStrategy = 1,
}

impl From<u32> for StrategyType {
    fn from(value: u32) -> Self {
        match value {
            0 => StrategyType::SteppedStrategy,
            1 => StrategyType::LinearStrategy,
            _ => StrategyType::SteppedStrategy, // Default to 0
        }
    }
}

/// Verification information for verifiable manifests
#[derive(Debug, Clone, PartialEq)]
pub struct VerificationInfo {
    /// Root CID of the verification tree
    pub verify_root: Cid,
    /// Individual slot roots
    pub slot_roots: Vec<Cid>,
    /// Size of each slot cell
    pub cell_size: u64,
    /// Indexing strategy used to build the slot roots
    pub verifiable_strategy: StrategyType,
}

/// Erasure coding information for protected manifests
#[derive(Debug, Clone, PartialEq)]
pub struct ErasureInfo {
    /// Number of blocks to encode
    pub ec_k: u32,
    /// Number of resulting parity blocks
    pub ec_m: u32,
    /// Original root CID before erasure coding
    pub original_tree_cid: Cid,
    /// Original dataset size before erasure coding
    pub original_dataset_size: u64,
    /// Indexing strategy used to build the slot roots
    pub protected_strategy: StrategyType,
    /// Verification information (if verifiable)
    pub verification: Option<VerificationInfo>,
}

/// Archivist Manifest
///
/// Describes a dataset with metadata and tree CID.
/// Encoded with protobuf and stored with codec 0xcd01.
#[derive(Debug, Clone, PartialEq)]
pub struct Manifest {
    /// Root CID of the merkle tree
    pub tree_cid: Cid,
    /// Size of each contained block
    pub block_size: u64,
    /// Total size of all blocks
    pub dataset_size: u64,
    /// Dataset codec (default: BlockCodec = 0xcd02)
    pub codec: u64,
    /// Multihash codec (default: SHA-256 = 0x12)
    pub hcodec: u64,
    /// CID version (default: 1)
    pub version: u32,
    /// Original filename (optional)
    pub filename: Option<String>,
    /// MIME type (optional)
    pub mimetype: Option<String>,
    /// Erasure coding information (if protected)
    pub erasure: Option<ErasureInfo>,
}

impl Manifest {
    /// Create a new unprotected manifest
    pub fn new(
        tree_cid: Cid,
        block_size: u64,
        dataset_size: u64,
        codec: Option<u64>,
        hcodec: Option<u64>,
        version: Option<u32>,
        filename: Option<String>,
        mimetype: Option<String>,
    ) -> Self {
        Self {
            tree_cid,
            block_size,
            dataset_size,
            codec: codec.unwrap_or(BLOCK_CODEC),
            hcodec: hcodec.unwrap_or(SHA256_CODEC),
            version: version.unwrap_or(1),
            filename,
            mimetype,
            erasure: None,
        }
    }

    /// Create a protected manifest with erasure coding
    pub fn new_protected(
        tree_cid: Cid,
        block_size: u64,
        dataset_size: u64,
        codec: u64,
        hcodec: u64,
        version: u32,
        ec_k: u32,
        ec_m: u32,
        original_tree_cid: Cid,
        original_dataset_size: u64,
        protected_strategy: StrategyType,
        filename: Option<String>,
        mimetype: Option<String>,
    ) -> Self {
        Self {
            tree_cid,
            block_size,
            dataset_size,
            codec,
            hcodec,
            version,
            filename,
            mimetype,
            erasure: Some(ErasureInfo {
                ec_k,
                ec_m,
                original_tree_cid,
                original_dataset_size,
                protected_strategy,
                verification: None,
            }),
        }
    }

    /// Check if manifest is protected (has erasure coding)
    pub fn is_protected(&self) -> bool {
        self.erasure.is_some()
    }

    /// Check if manifest is verifiable
    pub fn is_verifiable(&self) -> bool {
        self.erasure
            .as_ref()
            .and_then(|e| e.verification.as_ref())
            .is_some()
    }

    /// Get number of blocks in the dataset
    pub fn blocks_count(&self) -> usize {
        ((self.dataset_size + self.block_size - 1) / self.block_size) as usize
    }

    /// Encode the manifest to protobuf bytes
    ///
    /// Follows the exact protobuf structure used by Archivist:
    /// ```protobuf
    /// Message Header {
    ///   bytes treeCid = 1;
    ///   uint32 blockSize = 2;
    ///   uint64 datasetSize = 3;
    ///   uint32 codec = 4;
    ///   uint32 hcodec = 5;
    ///   uint32 version = 6;
    ///   ErasureInfo erasure = 7;
    ///   string filename = 8;
    ///   string mimetype = 9;
    /// }
    /// ```
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut header = proto::Header::default();

        // Encode tree CID as raw bytes
        header.tree_cid = self.tree_cid.to_bytes();
        header.block_size = self.block_size as u32;
        header.dataset_size = self.dataset_size;
        header.codec = self.codec as u32;
        header.hcodec = self.hcodec as u32;
        header.version = self.version;

        // Encode filename and mimetype if present
        if let Some(ref filename) = self.filename {
            header.filename = filename.clone();
        }
        if let Some(ref mimetype) = self.mimetype {
            header.mimetype = mimetype.clone();
        }

        // Encode erasure info if protected
        if let Some(ref erasure) = self.erasure {
            let mut erasure_info = proto::ErasureInfo::default();
            erasure_info.ec_k = erasure.ec_k;
            erasure_info.ec_m = erasure.ec_m;
            erasure_info.original_tree_cid = erasure.original_tree_cid.to_bytes();
            erasure_info.original_dataset_size = erasure.original_dataset_size;
            erasure_info.protected_strategy = erasure.protected_strategy as u32;

            // Encode verification info if verifiable
            if let Some(ref verification) = erasure.verification {
                let mut verification_info = proto::VerificationInfo::default();
                verification_info.verify_root = verification.verify_root.to_bytes();
                verification_info.slot_roots = verification
                    .slot_roots
                    .iter()
                    .map(|cid| cid.to_bytes())
                    .collect();
                verification_info.cell_size = verification.cell_size as u32;
                verification_info.verifiable_strategy = verification.verifiable_strategy as u32;

                erasure_info.verification = Some(verification_info);
            }

            header.erasure = Some(erasure_info);
        }

        // Encode the header
        let mut buf = Vec::new();
        header.encode(&mut buf)?;

        // Wrap in dag-pb format (field 1 = Data)
        let mut pb_node = proto::DagPbNode::default();
        pb_node.data = buf;

        let mut result = Vec::new();
        pb_node.encode(&mut result)?;

        Ok(result)
    }

    /// Decode a manifest from protobuf bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        // Decode dag-pb wrapper
        let pb_node = proto::DagPbNode::decode(&mut Cursor::new(data))?;

        // Decode header from Data field
        let header = proto::Header::decode(&mut Cursor::new(pb_node.data))?;

        // Parse tree CID
        let tree_cid = Cid::try_from(header.tree_cid)
            .map_err(|e| ManifestError::CidError(format!("Invalid tree CID: {}", e)))?;

        // Parse erasure info if present
        let erasure = if let Some(erasure_info) = header.erasure {
            let original_tree_cid = Cid::try_from(erasure_info.original_tree_cid)
                .map_err(|e| ManifestError::CidError(format!("Invalid original tree CID: {}", e)))?;

            // Parse verification info if present
            let verification = if let Some(verification_info) = erasure_info.verification {
                let verify_root = Cid::try_from(verification_info.verify_root).map_err(|e| {
                    ManifestError::CidError(format!("Invalid verify root CID: {}", e))
                })?;

                let slot_roots: Result<Vec<Cid>> = verification_info
                    .slot_roots
                    .iter()
                    .map(|bytes| {
                        Cid::try_from(bytes.as_slice())
                            .map_err(|e| ManifestError::CidError(format!("Invalid slot root: {}", e)))
                    })
                    .collect();

                Some(VerificationInfo {
                    verify_root,
                    slot_roots: slot_roots?,
                    cell_size: verification_info.cell_size as u64,
                    verifiable_strategy: verification_info.verifiable_strategy.into(),
                })
            } else {
                None
            };

            Some(ErasureInfo {
                ec_k: erasure_info.ec_k,
                ec_m: erasure_info.ec_m,
                original_tree_cid,
                original_dataset_size: erasure_info.original_dataset_size,
                protected_strategy: erasure_info.protected_strategy.into(),
                verification,
            })
        } else {
            None
        };

        Ok(Self {
            tree_cid,
            block_size: header.block_size as u64,
            dataset_size: header.dataset_size,
            codec: header.codec as u64,
            hcodec: header.hcodec as u64,
            version: header.version,
            filename: if header.filename.is_empty() {
                None
            } else {
                Some(header.filename)
            },
            mimetype: if header.mimetype.is_empty() {
                None
            } else {
                Some(header.mimetype)
            },
            erasure,
        })
    }

    /// Create a Block from this manifest
    ///
    /// The block will have codec 0xcd01 (ManifestCodec)
    pub fn to_block(&self) -> Result<Block> {
        let data = self.encode()?;

        // Create CID with manifest codec
        // CID = <version><codec><multihash>
        // Archivist uses SHA-256 for all CIDs (blocks AND manifests)
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let hash_bytes = hasher.finalize();

        // Build multihash: <hash_code><hash_length><hash_bytes>
        let mut multihash = Vec::new();
        // SHA-256 codec (0x12) - Archivist compatibility
        let mut buf = [0u8; 10];
        let encoded = unsigned_varint::encode::u64(SHA256_CODEC, &mut buf);
        multihash.extend_from_slice(encoded);
        // Hash length (32 bytes)
        let encoded = unsigned_varint::encode::u64(32, &mut buf);
        multihash.extend_from_slice(encoded);
        // Hash bytes
        multihash.extend_from_slice(&hash_bytes);

        // Build CID: <version><codec><multihash>
        let mut cid_bytes = Vec::new();
        // CID version 1
        let encoded = unsigned_varint::encode::u64(1, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        // Manifest codec (0xcd01)
        let encoded = unsigned_varint::encode::u64(MANIFEST_CODEC, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        // Multihash
        cid_bytes.extend_from_slice(&multihash);

        let cid = Cid::try_from(cid_bytes)
            .map_err(|e| ManifestError::CidError(format!("Failed to create CID: {}", e)))?;

        Ok(Block { cid, data })
    }

    /// Create a manifest from a Block
    pub fn from_block(block: &Block) -> Result<Self> {
        // Verify codec is ManifestCodec
        let codec = block.cid.codec();
        if codec != MANIFEST_CODEC {
            return Err(ManifestError::InvalidManifest(format!(
                "Block has codec 0x{:x}, expected manifest codec 0x{:x}",
                codec, MANIFEST_CODEC
            )));
        }

        Self::decode(&block.data)
    }
}

/// Protobuf message definitions
mod proto {
    use prost::Message;

    /// Dag-PB node wrapper (field 1 = Data)
    #[derive(Clone, PartialEq, Message)]
    pub struct DagPbNode {
        #[prost(bytes, tag = "1")]
        pub data: Vec<u8>,
    }

    /// Verification information
    #[derive(Clone, PartialEq, Message)]
    pub struct VerificationInfo {
        /// Verify root CID
        #[prost(bytes, tag = "1")]
        pub verify_root: Vec<u8>,
        /// Slot root CIDs
        #[prost(bytes, repeated, tag = "2")]
        pub slot_roots: Vec<Vec<u8>>,
        /// Cell size
        #[prost(uint32, tag = "3")]
        pub cell_size: u32,
        /// Verifiable strategy
        #[prost(uint32, tag = "4")]
        pub verifiable_strategy: u32,
    }

    /// Erasure coding information
    #[derive(Clone, PartialEq, Message)]
    pub struct ErasureInfo {
        /// Number of encoded blocks
        #[prost(uint32, tag = "1")]
        pub ec_k: u32,
        /// Number of parity blocks
        #[prost(uint32, tag = "2")]
        pub ec_m: u32,
        /// Original tree CID
        #[prost(bytes, tag = "3")]
        pub original_tree_cid: Vec<u8>,
        /// Original dataset size
        #[prost(uint64, tag = "4")]
        pub original_dataset_size: u64,
        /// Protected strategy
        #[prost(uint32, tag = "5")]
        pub protected_strategy: u32,
        /// Verification information (optional)
        #[prost(message, optional, tag = "6")]
        pub verification: Option<VerificationInfo>,
    }

    /// Manifest header
    #[derive(Clone, PartialEq, Message)]
    pub struct Header {
        /// Tree root CID
        #[prost(bytes, tag = "1")]
        pub tree_cid: Vec<u8>,
        /// Block size
        #[prost(uint32, tag = "2")]
        pub block_size: u32,
        /// Dataset size
        #[prost(uint64, tag = "3")]
        pub dataset_size: u64,
        /// Dataset codec
        #[prost(uint32, tag = "4")]
        pub codec: u32,
        /// Multihash codec
        #[prost(uint32, tag = "5")]
        pub hcodec: u32,
        /// CID version
        #[prost(uint32, tag = "6")]
        pub version: u32,
        /// Erasure info (optional)
        #[prost(message, optional, tag = "7")]
        pub erasure: Option<ErasureInfo>,
        /// Filename (optional)
        #[prost(string, tag = "8")]
        pub filename: String,
        /// MIME type (optional)
        #[prost(string, tag = "9")]
        pub mimetype: String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cid(data: &[u8]) -> Cid {
        let hash = blake3::hash(data);
        let hash_bytes = hash.as_bytes();

        let mut buf = [0u8; 10];
        let mut multihash = Vec::new();
        let encoded = unsigned_varint::encode::u64(BLAKE3_CODEC, &mut buf);
        multihash.extend_from_slice(encoded);
        let encoded = unsigned_varint::encode::u64(32, &mut buf);
        multihash.extend_from_slice(encoded);
        multihash.extend_from_slice(hash_bytes);

        let mut cid_bytes = Vec::new();
        let encoded = unsigned_varint::encode::u64(1, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        let encoded = unsigned_varint::encode::u64(BLOCK_CODEC, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        cid_bytes.extend_from_slice(&multihash);

        Cid::try_from(cid_bytes).unwrap()
    }

    #[test]
    fn test_manifest_creation() {
        let tree_cid = create_test_cid(b"test tree");

        let manifest = Manifest::new(
            tree_cid,
            DEFAULT_BLOCK_SIZE,
            1024 * 1024, // 1MB dataset
            None,
            None,
            None,
            Some("test.txt".to_string()),
            Some("text/plain".to_string()),
        );

        assert_eq!(manifest.tree_cid, tree_cid);
        assert_eq!(manifest.block_size, DEFAULT_BLOCK_SIZE);
        assert_eq!(manifest.dataset_size, 1024 * 1024);
        assert_eq!(manifest.codec, BLOCK_CODEC);
        assert_eq!(manifest.hcodec, SHA256_CODEC);
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.filename, Some("test.txt".to_string()));
        assert_eq!(manifest.mimetype, Some("text/plain".to_string()));
        assert!(!manifest.is_protected());
        assert!(!manifest.is_verifiable());
    }

    #[test]
    fn test_manifest_encode_decode_roundtrip() {
        let tree_cid = create_test_cid(b"test tree");

        let manifest = Manifest::new(
            tree_cid,
            DEFAULT_BLOCK_SIZE,
            1024 * 1024,
            Some(BLOCK_CODEC),
            Some(BLAKE3_CODEC),
            Some(1),
            Some("roundtrip.dat".to_string()),
            Some("application/octet-stream".to_string()),
        );

        // Encode
        let encoded = manifest.encode().expect("Encode should succeed");
        assert!(!encoded.is_empty());

        // Decode
        let decoded = Manifest::decode(&encoded).expect("Decode should succeed");

        // Verify all fields match
        assert_eq!(decoded.tree_cid, manifest.tree_cid);
        assert_eq!(decoded.block_size, manifest.block_size);
        assert_eq!(decoded.dataset_size, manifest.dataset_size);
        assert_eq!(decoded.codec, manifest.codec);
        assert_eq!(decoded.hcodec, manifest.hcodec);
        assert_eq!(decoded.version, manifest.version);
        assert_eq!(decoded.filename, manifest.filename);
        assert_eq!(decoded.mimetype, manifest.mimetype);
        assert_eq!(decoded.erasure, manifest.erasure);
    }

    #[test]
    fn test_manifest_encode_decode_minimal() {
        let tree_cid = create_test_cid(b"minimal tree");

        let manifest = Manifest::new(tree_cid, DEFAULT_BLOCK_SIZE, 512, None, None, None, None, None);

        let encoded = manifest.encode().expect("Encode should succeed");
        let decoded = Manifest::decode(&encoded).expect("Decode should succeed");

        assert_eq!(decoded.tree_cid, manifest.tree_cid);
        assert_eq!(decoded.block_size, manifest.block_size);
        assert_eq!(decoded.dataset_size, manifest.dataset_size);
        assert_eq!(decoded.filename, None);
        assert_eq!(decoded.mimetype, None);
    }

    #[test]
    fn test_manifest_to_block() {
        let tree_cid = create_test_cid(b"test tree");

        let manifest = Manifest::new(
            tree_cid,
            DEFAULT_BLOCK_SIZE,
            1024 * 1024,
            None,
            None,
            None,
            Some("test.bin".to_string()),
            None,
        );

        let block = manifest.to_block().expect("to_block should succeed");

        // Verify block has correct codec
        assert_eq!(block.cid.codec(), MANIFEST_CODEC);

        // Verify we can decode back
        let decoded = Manifest::from_block(&block).expect("from_block should succeed");
        assert_eq!(decoded.tree_cid, manifest.tree_cid);
        assert_eq!(decoded.filename, manifest.filename);
    }

    #[test]
    fn test_manifest_cid_computation() {
        let tree_cid = create_test_cid(b"test tree");

        let manifest = Manifest::new(tree_cid, DEFAULT_BLOCK_SIZE, 1024, None, None, None, None, None);

        let block1 = manifest.to_block().expect("to_block should succeed");
        let block2 = manifest.to_block().expect("to_block should succeed");

        // Same manifest should produce same CID
        assert_eq!(block1.cid, block2.cid);

        // Different manifest should produce different CID
        let manifest2 = Manifest::new(tree_cid, DEFAULT_BLOCK_SIZE, 2048, None, None, None, None, None);
        let block3 = manifest2.to_block().expect("to_block should succeed");
        assert_ne!(block1.cid, block3.cid);
    }

    #[test]
    fn test_manifest_blocks_count() {
        let tree_cid = create_test_cid(b"test tree");

        // Exactly 1 block
        let manifest = Manifest::new(tree_cid, 1024, 1024, None, None, None, None, None);
        assert_eq!(manifest.blocks_count(), 1);

        // 2 blocks (1025 bytes with 1024 block size)
        let manifest = Manifest::new(tree_cid, 1024, 1025, None, None, None, None, None);
        assert_eq!(manifest.blocks_count(), 2);

        // 10 blocks
        let manifest = Manifest::new(tree_cid, 1024, 10240, None, None, None, None, None);
        assert_eq!(manifest.blocks_count(), 10);
    }

    #[test]
    fn test_manifest_protected() {
        let tree_cid = create_test_cid(b"test tree");
        let original_tree_cid = create_test_cid(b"original tree");

        let manifest = Manifest::new_protected(
            tree_cid,
            DEFAULT_BLOCK_SIZE,
            1024 * 1024,
            BLOCK_CODEC,
            SHA256_CODEC,
            1,
            10, // ec_k
            3,  // ec_m
            original_tree_cid,
            800 * 1024, // original size
            StrategyType::SteppedStrategy,
            Some("protected.dat".to_string()),
            None,
        );

        assert!(manifest.is_protected());
        assert!(!manifest.is_verifiable());

        let erasure = manifest.erasure.as_ref().unwrap();
        assert_eq!(erasure.ec_k, 10);
        assert_eq!(erasure.ec_m, 3);
        assert_eq!(erasure.original_tree_cid, original_tree_cid);
        assert_eq!(erasure.original_dataset_size, 800 * 1024);
        assert_eq!(erasure.protected_strategy, StrategyType::SteppedStrategy);
    }

    #[test]
    fn test_manifest_protected_encode_decode() {
        let tree_cid = create_test_cid(b"test tree");
        let original_tree_cid = create_test_cid(b"original tree");

        let manifest = Manifest::new_protected(
            tree_cid,
            DEFAULT_BLOCK_SIZE,
            2048 * 1024,
            BLOCK_CODEC,
            BLAKE3_CODEC,
            1,
            7,
            2,
            original_tree_cid,
            1024 * 1024,
            StrategyType::LinearStrategy,
            Some("ec.bin".to_string()),
            Some("application/octet-stream".to_string()),
        );

        let encoded = manifest.encode().expect("Encode should succeed");
        let decoded = Manifest::decode(&encoded).expect("Decode should succeed");

        assert_eq!(decoded.tree_cid, manifest.tree_cid);
        assert!(decoded.is_protected());

        let decoded_erasure = decoded.erasure.as_ref().unwrap();
        let original_erasure = manifest.erasure.as_ref().unwrap();

        assert_eq!(decoded_erasure.ec_k, original_erasure.ec_k);
        assert_eq!(decoded_erasure.ec_m, original_erasure.ec_m);
        assert_eq!(
            decoded_erasure.original_tree_cid,
            original_erasure.original_tree_cid
        );
        assert_eq!(
            decoded_erasure.original_dataset_size,
            original_erasure.original_dataset_size
        );
        assert_eq!(
            decoded_erasure.protected_strategy,
            original_erasure.protected_strategy
        );
    }

    #[test]
    fn test_manifest_verifiable() {
        let tree_cid = create_test_cid(b"test tree");
        let original_tree_cid = create_test_cid(b"original tree");
        let verify_root = create_test_cid(b"verify root");
        let slot_root_1 = create_test_cid(b"slot 1");
        let slot_root_2 = create_test_cid(b"slot 2");
        let slot_root_3 = create_test_cid(b"slot 3");

        let mut manifest = Manifest::new_protected(
            tree_cid,
            DEFAULT_BLOCK_SIZE,
            3 * 1024 * 1024,
            BLOCK_CODEC,
            SHA256_CODEC,
            1,
            10,
            3,
            original_tree_cid,
            2 * 1024 * 1024,
            StrategyType::SteppedStrategy,
            None,
            None,
        );

        // Add verification info
        if let Some(ref mut erasure) = manifest.erasure {
            erasure.verification = Some(VerificationInfo {
                verify_root,
                slot_roots: vec![slot_root_1, slot_root_2, slot_root_3],
                cell_size: 2048,
                verifiable_strategy: StrategyType::LinearStrategy,
            });
        }

        assert!(manifest.is_protected());
        assert!(manifest.is_verifiable());

        // Test encode/decode
        let encoded = manifest.encode().expect("Encode should succeed");
        let decoded = Manifest::decode(&encoded).expect("Decode should succeed");

        assert!(decoded.is_verifiable());
        let verification = decoded
            .erasure
            .as_ref()
            .unwrap()
            .verification
            .as_ref()
            .unwrap();

        assert_eq!(verification.verify_root, verify_root);
        assert_eq!(verification.slot_roots.len(), 3);
        assert_eq!(verification.slot_roots[0], slot_root_1);
        assert_eq!(verification.slot_roots[1], slot_root_2);
        assert_eq!(verification.slot_roots[2], slot_root_3);
        assert_eq!(verification.cell_size, 2048);
        assert_eq!(verification.verifiable_strategy, StrategyType::LinearStrategy);
    }

    #[test]
    fn test_manifest_from_block_wrong_codec() {
        // Create a block with wrong codec
        let tree_cid = create_test_cid(b"test");
        let block = Block {
            cid: tree_cid,
            data: vec![1, 2, 3],
        };

        let result = Manifest::from_block(&block);
        assert!(result.is_err());

        match result {
            Err(ManifestError::InvalidManifest(msg)) => {
                assert!(msg.contains("expected manifest codec"));
            }
            _ => panic!("Expected InvalidManifest error"),
        }
    }

    #[test]
    fn test_strategy_type_conversion() {
        assert_eq!(StrategyType::from(0), StrategyType::SteppedStrategy);
        assert_eq!(StrategyType::from(1), StrategyType::LinearStrategy);
        assert_eq!(StrategyType::from(99), StrategyType::SteppedStrategy); // Default
    }
}
