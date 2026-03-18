//! Folder Manifest implementation
//!
//! Folder manifests (codec 0xcd03) group multiple file manifests into a named
//! directory structure. Each entry maps a filename to its file manifest CID.

use cid::Cid;
use prost::Message as ProstMessage;
use sha2::{Digest, Sha256};
use std::io::Cursor;
use thiserror::Error;

use crate::manifest::{BLAKE3_CODEC, SHA256_CODEC};
use crate::storage::Block;

/// Folder manifest codec (0xcd03)
pub const FOLDER_MANIFEST_CODEC: u64 = 0xcd03;

#[derive(Debug, Error)]
pub enum FolderManifestError {
    #[error("Protobuf encode error: {0}")]
    EncodeError(#[from] prost::EncodeError),

    #[error("Protobuf decode error: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("CID error: {0}")]
    CidError(String),

    #[error("Invalid folder manifest: {0}")]
    InvalidManifest(String),

    #[error("Multihash error: {0}")]
    MultihashError(String),
}

pub type Result<T> = std::result::Result<T, FolderManifestError>;

/// A single file entry within a folder.
#[derive(Debug, Clone, PartialEq)]
pub struct FolderEntry {
    /// Filename within the folder.
    pub name: String,
    /// CID of the file's Manifest block (codec 0xcd01).
    pub manifest_cid: Cid,
    /// Total file size in bytes (from the inner manifest's dataset_size).
    pub size: u64,
    /// MIME type (empty string if unset).
    pub mimetype: String,
}

/// A folder manifest grouping multiple file manifests under named entries.
#[derive(Debug, Clone, PartialEq)]
pub struct FolderManifest {
    pub entries: Vec<FolderEntry>,
    /// Multihash codec (default: BLAKE3).
    pub hcodec: u64,
    /// CID version (default: 1).
    pub version: u32,
}

impl FolderManifest {
    /// Create a new folder manifest with default hcodec (BLAKE3) and version (1).
    pub fn new(entries: Vec<FolderEntry>) -> Self {
        Self {
            entries,
            hcodec: BLAKE3_CODEC,
            version: 1,
        }
    }

    /// Look up an entry by filename.
    pub fn find_entry(&self, name: &str) -> Option<&FolderEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    /// Encode the folder manifest to protobuf bytes.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let entries: Vec<proto::FolderEntryProto> = self
            .entries
            .iter()
            .map(|e| proto::FolderEntryProto {
                name: e.name.clone(),
                manifest_cid: e.manifest_cid.to_bytes(),
                size: e.size,
                mimetype: e.mimetype.clone(),
            })
            .collect();

        let header = proto::FolderHeader {
            entries,
            hcodec: self.hcodec as u32,
            version: self.version,
        };

        let mut buf = Vec::new();
        header.encode(&mut buf)?;

        let mut result = Vec::new();
        let pb_node = proto::DagPbNode { data: buf };
        pb_node.encode(&mut result)?;

        Ok(result)
    }

    /// Decode a folder manifest from protobuf bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        let pb_node = proto::DagPbNode::decode(&mut Cursor::new(data))?;
        let header = proto::FolderHeader::decode(&mut Cursor::new(pb_node.data))?;

        let entries: Result<Vec<FolderEntry>> = header
            .entries
            .into_iter()
            .map(|e| {
                let manifest_cid = Cid::try_from(e.manifest_cid).map_err(|err| {
                    FolderManifestError::CidError(format!("Invalid manifest CID: {}", err))
                })?;
                Ok(FolderEntry {
                    name: e.name,
                    manifest_cid,
                    size: e.size,
                    mimetype: e.mimetype,
                })
            })
            .collect();

        Ok(Self {
            entries: entries?,
            hcodec: header.hcodec as u64,
            version: header.version,
        })
    }

    /// Create a Block from this folder manifest.
    ///
    /// The block will have codec 0xcd03 (FolderManifestCodec).
    pub fn to_block(&self) -> Result<Block> {
        let data = self.encode()?;

        let hash_bytes = match self.hcodec {
            BLAKE3_CODEC => blake3::hash(&data).as_bytes().to_vec(),
            SHA256_CODEC => {
                let mut hasher = Sha256::new();
                hasher.update(&data);
                hasher.finalize().to_vec()
            }
            codec => {
                return Err(FolderManifestError::InvalidManifest(format!(
                    "Unsupported hash codec: 0x{:x}",
                    codec
                )))
            }
        };

        // Build multihash
        let mut multihash = Vec::new();
        let mut buf = [0u8; 10];
        let encoded = unsigned_varint::encode::u64(self.hcodec, &mut buf);
        multihash.extend_from_slice(encoded);
        let encoded = unsigned_varint::encode::u64(hash_bytes.len() as u64, &mut buf);
        multihash.extend_from_slice(encoded);
        multihash.extend_from_slice(&hash_bytes);

        // Build CID
        let mut cid_bytes = Vec::new();
        let encoded = unsigned_varint::encode::u64(self.version as u64, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        let encoded = unsigned_varint::encode::u64(FOLDER_MANIFEST_CODEC, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        cid_bytes.extend_from_slice(&multihash);

        let cid = Cid::try_from(cid_bytes)
            .map_err(|e| FolderManifestError::CidError(format!("Failed to create CID: {}", e)))?;

        Ok(Block { cid, data })
    }

    /// Create a folder manifest from a Block.
    pub fn from_block(block: &Block) -> Result<Self> {
        let codec = block.cid.codec();
        if codec != FOLDER_MANIFEST_CODEC {
            return Err(FolderManifestError::InvalidManifest(format!(
                "Block has codec 0x{:x}, expected folder manifest codec 0x{:x}",
                codec, FOLDER_MANIFEST_CODEC
            )));
        }
        Self::decode(&block.data)
    }
}

/// Protobuf message definitions.
mod proto {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct DagPbNode {
        #[prost(bytes, tag = "1")]
        pub data: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct FolderEntryProto {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(bytes, tag = "2")]
        pub manifest_cid: Vec<u8>,
        #[prost(uint64, tag = "3")]
        pub size: u64,
        #[prost(string, tag = "4")]
        pub mimetype: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct FolderHeader {
        #[prost(message, repeated, tag = "1")]
        pub entries: Vec<FolderEntryProto>,
        #[prost(uint32, tag = "2")]
        pub hcodec: u32,
        #[prost(uint32, tag = "3")]
        pub version: u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{BLOCK_CODEC, MANIFEST_CODEC};

    fn create_test_cid(data: &[u8], codec: u64) -> Cid {
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
        let encoded = unsigned_varint::encode::u64(codec, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        cid_bytes.extend_from_slice(&multihash);

        Cid::try_from(cid_bytes).unwrap()
    }

    #[test]
    fn test_folder_manifest_roundtrip() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let cid2 = create_test_cid(b"file2", MANIFEST_CODEC);

        let folder = FolderManifest::new(vec![
            FolderEntry {
                name: "01 - Track One.flac".to_string(),
                manifest_cid: cid1,
                size: 27_000_000,
                mimetype: "audio/flac".to_string(),
            },
            FolderEntry {
                name: "02 - Track Two.flac".to_string(),
                manifest_cid: cid2,
                size: 102_000_000,
                mimetype: "audio/flac".to_string(),
            },
        ]);

        let encoded = folder.encode().expect("encode should succeed");
        let decoded = FolderManifest::decode(&encoded).expect("decode should succeed");

        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].name, "01 - Track One.flac");
        assert_eq!(decoded.entries[0].manifest_cid, cid1);
        assert_eq!(decoded.entries[0].size, 27_000_000);
        assert_eq!(decoded.entries[1].name, "02 - Track Two.flac");
        assert_eq!(decoded.entries[1].manifest_cid, cid2);
    }

    #[test]
    fn test_folder_manifest_to_block() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let folder = FolderManifest::new(vec![FolderEntry {
            name: "test.bin".to_string(),
            manifest_cid: cid1,
            size: 1024,
            mimetype: "application/octet-stream".to_string(),
        }]);

        let block = folder.to_block().expect("to_block should succeed");
        assert_eq!(block.cid.codec(), FOLDER_MANIFEST_CODEC);

        let decoded = FolderManifest::from_block(&block).expect("from_block should succeed");
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].name, "test.bin");
    }

    #[test]
    fn test_folder_manifest_find_entry() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let cid2 = create_test_cid(b"file2", MANIFEST_CODEC);

        let folder = FolderManifest::new(vec![
            FolderEntry {
                name: "hello.txt".to_string(),
                manifest_cid: cid1,
                size: 100,
                mimetype: "text/plain".to_string(),
            },
            FolderEntry {
                name: "world.bin".to_string(),
                manifest_cid: cid2,
                size: 200,
                mimetype: "application/octet-stream".to_string(),
            },
        ]);

        assert!(folder.find_entry("hello.txt").is_some());
        assert!(folder.find_entry("world.bin").is_some());
        assert!(folder.find_entry("missing.txt").is_none());
    }

    #[test]
    fn test_folder_manifest_wrong_codec() {
        let cid = create_test_cid(b"test", BLOCK_CODEC);
        let block = Block {
            cid,
            data: vec![1, 2, 3],
        };

        let result = FolderManifest::from_block(&block);
        assert!(result.is_err());
    }

    #[test]
    fn test_folder_manifest_deterministic_cid() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let folder = FolderManifest::new(vec![FolderEntry {
            name: "test.bin".to_string(),
            manifest_cid: cid1,
            size: 1024,
            mimetype: "".to_string(),
        }]);

        let block1 = folder.to_block().unwrap();
        let block2 = folder.to_block().unwrap();
        assert_eq!(block1.cid, block2.cid);
    }
}
