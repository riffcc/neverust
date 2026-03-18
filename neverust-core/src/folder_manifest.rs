//! Directory Manifest implementation (Archivist-compatible)
//!
//! Directory manifests (codec 0xcd04) group multiple content entries into a
//! named directory structure. Compatible with the Archivist `DirectoryCodec`.
//!
//! Protobuf layout matches the Archivist reference implementation:
//!   - Field 1 (repeated): entries (each a nested protobuf with name/cid/size/isDirectory/mimetype)
//!   - Field 2: totalSize (uint64)
//!   - Field 3: name (string)

use cid::Cid;
use prost::Message as ProstMessage;
use sha2::{Digest, Sha256};
use std::io::Cursor;
use thiserror::Error;

use crate::manifest::SHA256_CODEC;

// Re-export for test use
#[cfg(test)]
use crate::manifest::BLAKE3_CODEC;
use crate::storage::Block;

/// Directory manifest codec (0xcd04) — matches Archivist DirectoryCodec.
pub const DIRECTORY_CODEC: u64 = 0xcd04;

#[derive(Debug, Error)]
pub enum DirectoryManifestError {
    #[error("Protobuf encode error: {0}")]
    EncodeError(#[from] prost::EncodeError),

    #[error("Protobuf decode error: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("CID error: {0}")]
    CidError(String),

    #[error("Invalid directory manifest: {0}")]
    InvalidManifest(String),
}

pub type Result<T> = std::result::Result<T, DirectoryManifestError>;

/// A single entry within a directory.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectoryEntry {
    /// Name of this entry (filename or subdirectory name).
    pub name: String,
    /// CID of the content (file manifest 0xcd01 or nested directory 0xcd04).
    pub cid: Cid,
    /// Total size in bytes.
    pub size: u64,
    /// Whether this entry is a subdirectory.
    pub is_directory: bool,
    /// MIME type (empty string if unset).
    pub mimetype: String,
}

/// A directory manifest grouping entries under a named directory.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectoryManifest {
    pub entries: Vec<DirectoryEntry>,
    /// Total size of all entries.
    pub total_size: u64,
    /// Directory name (empty string if unset).
    pub name: String,
}

impl DirectoryManifest {
    /// Create a new directory manifest, computing total_size from entries.
    pub fn new(entries: Vec<DirectoryEntry>, name: String) -> Self {
        let total_size = entries.iter().map(|e| e.size).sum();
        Self {
            entries,
            total_size,
            name,
        }
    }

    /// Look up an entry by name.
    pub fn find_entry(&self, name: &str) -> Option<&DirectoryEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    /// Return entries sorted: directories first, then files, alphabetically.
    pub fn sorted_entries(&self) -> Vec<&DirectoryEntry> {
        let mut sorted: Vec<&DirectoryEntry> = self.entries.iter().collect();
        sorted.sort_by(|a, b| {
            match (a.is_directory, b.is_directory) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => a.name.cmp(&b.name),
            }
        });
        sorted
    }

    /// Encode the directory manifest to protobuf bytes.
    ///
    /// Layout matches Archivist:
    ///   Field 1 (repeated): serialized entry sub-messages
    ///   Field 2: totalSize
    ///   Field 3: name
    pub fn encode(&self) -> Result<Vec<u8>> {
        let entries: Vec<proto::DirectoryEntryProto> = self
            .entries
            .iter()
            .map(|e| proto::DirectoryEntryProto {
                name: e.name.clone(),
                cid: e.cid.to_bytes(),
                size: e.size,
                is_directory: if e.is_directory { 1 } else { 0 },
                mimetype: e.mimetype.clone(),
            })
            .collect();

        let header = proto::DirectoryHeader {
            entries,
            total_size: self.total_size,
            name: self.name.clone(),
        };

        let mut buf = Vec::new();
        header.encode(&mut buf)?;
        Ok(buf)
    }

    /// Decode a directory manifest from protobuf bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        let header = proto::DirectoryHeader::decode(&mut Cursor::new(data))?;

        let entries: Result<Vec<DirectoryEntry>> = header
            .entries
            .into_iter()
            .map(|e| {
                let cid = Cid::try_from(e.cid).map_err(|err| {
                    DirectoryManifestError::CidError(format!("Invalid CID: {}", err))
                })?;
                Ok(DirectoryEntry {
                    name: e.name,
                    cid,
                    size: e.size,
                    is_directory: e.is_directory != 0,
                    mimetype: e.mimetype,
                })
            })
            .collect();

        Ok(Self {
            entries: entries?,
            total_size: header.total_size,
            name: header.name,
        })
    }

    /// Create a Block from this directory manifest.
    ///
    /// Uses SHA-256 hashing to match Archivist convention for directory blocks.
    pub fn to_block(&self) -> Result<Block> {
        let data = self.encode()?;

        // Archivist uses SHA-256 for directory manifests
        let hcodec = SHA256_CODEC;
        let hash_bytes = {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            hasher.finalize().to_vec()
        };

        // Build multihash
        let mut multihash = Vec::new();
        let mut buf = [0u8; 10];
        let encoded = unsigned_varint::encode::u64(hcodec, &mut buf);
        multihash.extend_from_slice(encoded);
        let encoded = unsigned_varint::encode::u64(hash_bytes.len() as u64, &mut buf);
        multihash.extend_from_slice(encoded);
        multihash.extend_from_slice(&hash_bytes);

        // Build CID (version 1, DirectoryCodec)
        let mut cid_bytes = Vec::new();
        let encoded = unsigned_varint::encode::u64(1, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        let encoded = unsigned_varint::encode::u64(DIRECTORY_CODEC, &mut buf);
        cid_bytes.extend_from_slice(encoded);
        cid_bytes.extend_from_slice(&multihash);

        let cid = Cid::try_from(cid_bytes)
            .map_err(|e| DirectoryManifestError::CidError(format!("Failed to create CID: {}", e)))?;

        Ok(Block { cid, data })
    }

    /// Create a directory manifest from a Block.
    pub fn from_block(block: &Block) -> Result<Self> {
        let codec = block.cid.codec();
        if codec != DIRECTORY_CODEC {
            return Err(DirectoryManifestError::InvalidManifest(format!(
                "Block has codec 0x{:x}, expected directory codec 0x{:x}",
                codec, DIRECTORY_CODEC
            )));
        }
        Self::decode(&block.data)
    }
}

/// Check whether a CID represents a directory manifest.
pub fn is_directory(cid: &Cid) -> bool {
    cid.codec() == DIRECTORY_CODEC
}

/// Protobuf message definitions matching Archivist layout.
mod proto {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct DirectoryEntryProto {
        /// Entry name
        #[prost(string, tag = "1")]
        pub name: String,
        /// CID bytes
        #[prost(bytes, tag = "2")]
        pub cid: Vec<u8>,
        /// Size in bytes
        #[prost(uint64, tag = "3")]
        pub size: u64,
        /// 1 = directory, 0 = file
        #[prost(uint32, tag = "4")]
        pub is_directory: u32,
        /// MIME type (optional, empty = unset)
        #[prost(string, tag = "5")]
        pub mimetype: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct DirectoryHeader {
        /// Repeated directory entries
        #[prost(message, repeated, tag = "1")]
        pub entries: Vec<DirectoryEntryProto>,
        /// Total size of all entries
        #[prost(uint64, tag = "2")]
        pub total_size: u64,
        /// Directory name
        #[prost(string, tag = "3")]
        pub name: String,
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
    fn test_directory_manifest_roundtrip() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let cid2 = create_test_cid(b"file2", MANIFEST_CODEC);

        let dir = DirectoryManifest::new(
            vec![
                DirectoryEntry {
                    name: "01 - Track One.flac".to_string(),
                    cid: cid1,
                    size: 27_000_000,
                    is_directory: false,
                    mimetype: "audio/flac".to_string(),
                },
                DirectoryEntry {
                    name: "02 - Track Two.flac".to_string(),
                    cid: cid2,
                    size: 102_000_000,
                    is_directory: false,
                    mimetype: "audio/flac".to_string(),
                },
            ],
            "The Slip".to_string(),
        );

        assert_eq!(dir.total_size, 129_000_000);

        let encoded = dir.encode().expect("encode should succeed");
        let decoded = DirectoryManifest::decode(&encoded).expect("decode should succeed");

        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.name, "The Slip");
        assert_eq!(decoded.total_size, 129_000_000);
        assert_eq!(decoded.entries[0].name, "01 - Track One.flac");
        assert_eq!(decoded.entries[0].cid, cid1);
        assert_eq!(decoded.entries[0].is_directory, false);
        assert_eq!(decoded.entries[1].name, "02 - Track Two.flac");
    }

    #[test]
    fn test_directory_manifest_to_block() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let dir = DirectoryManifest::new(
            vec![DirectoryEntry {
                name: "test.bin".to_string(),
                cid: cid1,
                size: 1024,
                is_directory: false,
                mimetype: "application/octet-stream".to_string(),
            }],
            "".to_string(),
        );

        let block = dir.to_block().expect("to_block should succeed");
        assert_eq!(block.cid.codec(), DIRECTORY_CODEC);

        let decoded = DirectoryManifest::from_block(&block).expect("from_block should succeed");
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].name, "test.bin");
    }

    #[test]
    fn test_directory_manifest_find_entry() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let cid2 = create_test_cid(b"file2", MANIFEST_CODEC);

        let dir = DirectoryManifest::new(
            vec![
                DirectoryEntry {
                    name: "hello.txt".to_string(),
                    cid: cid1,
                    size: 100,
                    is_directory: false,
                    mimetype: "text/plain".to_string(),
                },
                DirectoryEntry {
                    name: "subdir".to_string(),
                    cid: cid2,
                    size: 200,
                    is_directory: true,
                    mimetype: "".to_string(),
                },
            ],
            "".to_string(),
        );

        assert!(dir.find_entry("hello.txt").is_some());
        assert!(dir.find_entry("subdir").is_some());
        assert!(dir.find_entry("missing.txt").is_none());
        assert!(!dir.find_entry("hello.txt").unwrap().is_directory);
        assert!(dir.find_entry("subdir").unwrap().is_directory);
    }

    #[test]
    fn test_directory_manifest_sorted_entries() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let cid2 = create_test_cid(b"dir1", DIRECTORY_CODEC);
        let cid3 = create_test_cid(b"file2", MANIFEST_CODEC);

        let dir = DirectoryManifest::new(
            vec![
                DirectoryEntry { name: "zebra.txt".to_string(), cid: cid1, size: 10, is_directory: false, mimetype: "".to_string() },
                DirectoryEntry { name: "alpha_dir".to_string(), cid: cid2, size: 20, is_directory: true, mimetype: "".to_string() },
                DirectoryEntry { name: "apple.txt".to_string(), cid: cid3, size: 30, is_directory: false, mimetype: "".to_string() },
            ],
            "".to_string(),
        );

        let sorted = dir.sorted_entries();
        assert_eq!(sorted[0].name, "alpha_dir"); // dir first
        assert_eq!(sorted[1].name, "apple.txt"); // then files alphabetically
        assert_eq!(sorted[2].name, "zebra.txt");
    }

    #[test]
    fn test_directory_manifest_wrong_codec() {
        let cid = create_test_cid(b"test", BLOCK_CODEC);
        let block = Block {
            cid,
            data: vec![1, 2, 3],
        };
        assert!(DirectoryManifest::from_block(&block).is_err());
    }

    #[test]
    fn test_directory_manifest_deterministic_cid() {
        let cid1 = create_test_cid(b"file1", MANIFEST_CODEC);
        let dir = DirectoryManifest::new(
            vec![DirectoryEntry {
                name: "test.bin".to_string(),
                cid: cid1,
                size: 1024,
                is_directory: false,
                mimetype: "".to_string(),
            }],
            "".to_string(),
        );

        let block1 = dir.to_block().unwrap();
        let block2 = dir.to_block().unwrap();
        assert_eq!(block1.cid, block2.cid);
    }

    #[test]
    fn test_is_directory() {
        let dir_cid = create_test_cid(b"dir", DIRECTORY_CODEC);
        let file_cid = create_test_cid(b"file", MANIFEST_CODEC);
        assert!(is_directory(&dir_cid));
        assert!(!is_directory(&file_cid));
    }
}
