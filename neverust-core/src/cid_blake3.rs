//! CID-based content addressing with BLAKE3 streaming verification
//!
//! Implements content-addressed block storage using CIDs (Content Identifiers)
//! with BLAKE3 hashing for fast, secure content verification.

use cid::Cid;
use multihash::Multihash;
use sha2::{Digest, Sha256};
use std::io::{self, Read};
use thiserror::Error;

/// BLAKE3 multihash code
/// See: https://github.com/multiformats/multicodec/blob/master/table.csv
const BLAKE3_CODE: u64 = 0x1e;
/// SHA2-256 multihash code
const SHA2_256_CODE: u64 = 0x12;

/// Archivist block codec (codex-block, NOT codex-manifest!)
/// 0xcd01 = codex-manifest (for metadata)
/// 0xcd02 = codex-block (for actual data blocks)
const ARCHIVIST_BLOCK_CODEC: u64 = 0xcd02; // Changed from 0xcd01!

#[derive(Debug, Error)]
pub enum CidError {
    #[error("Invalid CID: {0}")]
    InvalidCid(String),

    #[error("Hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Multihash error: {0}")]
    Multihash(String),
}

/// Compute BLAKE3 hash of data
pub fn blake3_hash(data: &[u8]) -> Vec<u8> {
    blake3::hash(data).as_bytes().to_vec()
}

/// Compute Archivist-compatible CID for data
/// Uses BLAKE3 hash and codex-block codec (0xcd02)
pub fn blake3_cid(data: &[u8]) -> Result<Cid, CidError> {
    let hash = blake3_hash(data);

    // Create multihash from BLAKE3 hash
    let mh = Multihash::wrap(BLAKE3_CODE, &hash)
        .map_err(|e| CidError::Multihash(format!("Failed to create multihash: {}", e)))?;

    // Create CIDv1 with archivist-block codec (0xcd02)
    Ok(Cid::new_v1(ARCHIVIST_BLOCK_CODEC, mh))
}

/// Compute SHA2-256 hash of data
pub fn sha256_hash(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute Archivist-compatible CID for data using SHA2-256
pub fn sha256_cid(data: &[u8]) -> Result<Cid, CidError> {
    let hash = sha256_hash(data);
    let mh = Multihash::wrap(SHA2_256_CODE, &hash)
        .map_err(|e| CidError::Multihash(format!("Failed to create multihash: {}", e)))?;
    Ok(Cid::new_v1(ARCHIVIST_BLOCK_CODEC, mh))
}

/// Streaming BLAKE3 verifier for blocks
pub struct StreamingVerifier {
    hasher: blake3::Hasher,
    expected_cid: Option<Cid>,
    bytes_processed: usize,
}

impl StreamingVerifier {
    /// Create a new streaming verifier without expected CID
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            expected_cid: None,
            bytes_processed: 0,
        }
    }

    /// Create a new streaming verifier with expected CID
    pub fn new_with_cid(expected_cid: Cid) -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            expected_cid: Some(expected_cid),
            bytes_processed: 0,
        }
    }

    /// Update the hasher with new data
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
        self.bytes_processed += data.len();
    }

    /// Read from a reader and update the hasher
    pub fn update_from_reader<R: Read>(&mut self, reader: &mut R) -> Result<usize, io::Error> {
        let mut buffer = [0u8; 8192];
        let mut total_read = 0;

        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            self.update(&buffer[..n]);
            total_read += n;
        }

        Ok(total_read)
    }

    /// Finalize and get the computed CID
    pub fn finalize(self) -> Cid {
        let hash = self.hasher.finalize();
        let mh =
            Multihash::wrap(BLAKE3_CODE, hash.as_bytes()).expect("BLAKE3 hash length is valid");
        Cid::new_v1(ARCHIVIST_BLOCK_CODEC, mh)
    }

    /// Finalize and verify against expected CID (if set)
    pub fn finalize_and_verify(self) -> Result<Cid, CidError> {
        let expected_cid = self.expected_cid;
        let computed_cid = self.finalize();

        if let Some(expected) = expected_cid {
            if computed_cid != expected {
                return Err(CidError::HashMismatch {
                    expected: expected.to_string(),
                    actual: computed_cid.to_string(),
                });
            }
        }

        Ok(computed_cid)
    }

    /// Get number of bytes processed
    pub fn bytes_processed(&self) -> usize {
        self.bytes_processed
    }
}

impl Default for StreamingVerifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Verify data against a CID using BLAKE3
pub fn verify_blake3(data: &[u8], expected_cid: &Cid) -> Result<(), CidError> {
    let computed_cid = match expected_cid.hash().code() {
        BLAKE3_CODE => blake3_cid(data)?,
        SHA2_256_CODE => sha256_cid(data)?,
        code => {
            return Err(CidError::InvalidCid(format!(
                "Unsupported multihash code: 0x{:x}",
                code
            )))
        }
    };

    if &computed_cid != expected_cid {
        return Err(CidError::HashMismatch {
            expected: expected_cid.to_string(),
            actual: computed_cid.to_string(),
        });
    }

    Ok(())
}

/// Parse a CID from bytes
pub fn parse_cid(bytes: &[u8]) -> Result<Cid, CidError> {
    Cid::try_from(bytes).map_err(|e| CidError::InvalidCid(e.to_string()))
}

/// Parse a CID from string
pub fn parse_cid_str(s: &str) -> Result<Cid, CidError> {
    s.parse()
        .map_err(|e| CidError::InvalidCid(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blake3_hash() {
        let data = b"hello world";
        let hash = blake3_hash(data);

        // BLAKE3 produces 32-byte hashes
        assert_eq!(hash.len(), 32);

        // Same data should produce same hash
        let hash2 = blake3_hash(data);
        assert_eq!(hash, hash2);

        // Different data should produce different hash
        let hash3 = blake3_hash(b"goodbye world");
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_blake3_cid() {
        let data = b"hello world";
        let cid = blake3_cid(data).unwrap();

        // CID should be version 1
        assert_eq!(cid.version(), cid::Version::V1);

        // Should use codex-block codec (0xcd02)
        assert_eq!(cid.codec(), 0xcd02);

        // Same data should produce same CID
        let cid2 = blake3_cid(data).unwrap();
        assert_eq!(cid, cid2);
    }

    #[test]
    fn test_verify_blake3() {
        let data = b"hello world";
        let cid = blake3_cid(data).unwrap();

        // Should verify successfully
        assert!(verify_blake3(data, &cid).is_ok());

        // Should fail with different data
        let result = verify_blake3(b"goodbye world", &cid);
        assert!(result.is_err());
        match result {
            Err(CidError::HashMismatch { .. }) => {}
            _ => panic!("Expected HashMismatch error"),
        }
    }

    #[test]
    fn test_verify_sha256() {
        let data = b"hello world";
        let cid = sha256_cid(data).unwrap();
        assert!(verify_blake3(data, &cid).is_ok());
        assert!(verify_blake3(b"not hello world", &cid).is_err());
    }

    #[test]
    fn test_streaming_verifier() {
        let data = b"hello world";
        let expected_cid = blake3_cid(data).unwrap();

        // Test streaming verification
        let mut verifier = StreamingVerifier::new_with_cid(expected_cid);

        // Update in chunks
        verifier.update(b"hello");
        verifier.update(b" ");
        verifier.update(b"world");

        // Should verify successfully
        let result = verifier.finalize_and_verify();
        assert!(result.is_ok());
    }

    #[test]
    fn test_streaming_verifier_mismatch() {
        let data = b"hello world";
        let expected_cid = blake3_cid(data).unwrap();

        let mut verifier = StreamingVerifier::new_with_cid(expected_cid);

        // Update with different data
        verifier.update(b"goodbye world");

        // Should fail verification
        let result = verifier.finalize_and_verify();
        assert!(result.is_err());
        match result {
            Err(CidError::HashMismatch { .. }) => {}
            _ => panic!("Expected HashMismatch error"),
        }
    }

    #[test]
    fn test_streaming_verifier_without_expected() {
        let data = b"hello world";

        let mut verifier = StreamingVerifier::new();
        verifier.update(data);

        let cid = verifier.finalize_and_verify().unwrap();

        // Should match the CID computed directly
        let expected_cid = blake3_cid(data).unwrap();
        assert_eq!(cid, expected_cid);
    }

    #[test]
    fn test_streaming_verifier_from_reader() {
        let data = b"hello world";
        let expected_cid = blake3_cid(data).unwrap();

        let mut verifier = StreamingVerifier::new_with_cid(expected_cid);
        let mut cursor = std::io::Cursor::new(data);

        let bytes_read = verifier.update_from_reader(&mut cursor).unwrap();
        assert_eq!(bytes_read, data.len());
        assert_eq!(verifier.bytes_processed(), data.len());

        let result = verifier.finalize_and_verify();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_cid_roundtrip() {
        let data = b"hello world";
        let cid = blake3_cid(data).unwrap();

        // Convert to bytes and back
        let cid_bytes = cid.to_bytes();
        let parsed_cid = parse_cid(&cid_bytes).unwrap();

        assert_eq!(cid, parsed_cid);
    }

    #[test]
    fn test_parse_cid_str_roundtrip() {
        let data = b"hello world";
        let cid = blake3_cid(data).unwrap();

        // Convert to string and back
        let cid_str = cid.to_string();
        let parsed_cid = parse_cid_str(&cid_str).unwrap();

        assert_eq!(cid, parsed_cid);
    }

    #[test]
    fn test_large_data() {
        // Test with larger data (1MB)
        let data = vec![0x42u8; 1024 * 1024];
        let cid = blake3_cid(&data).unwrap();

        // Streaming verification
        let mut verifier = StreamingVerifier::new_with_cid(cid);

        // Process in 64KB chunks
        for chunk in data.chunks(64 * 1024) {
            verifier.update(chunk);
        }

        let result = verifier.finalize_and_verify();
        assert!(result.is_ok());
    }

    #[test]
    fn test_decode_archivist_cid() {
        // Example CID from Archivist testnet
        let cid_str = "zDvZRwzmCWBSntHdMBpEaWBpvJTVt1aFoNVcv5BA51EsAPV57ycx";

        let cid: Cid = cid_str.parse().unwrap();

        println!("\nDecoded Archivist CID:");
        println!("  CID: {}", cid);
        println!("  Version: {:?}", cid.version());
        println!("  Codec: 0x{:x}", cid.codec());

        let mh = cid.hash();
        println!("  Hash code: 0x{:x}", mh.code());
        println!("  Hash size: {} bytes", mh.size());
        println!("  Hash digest (hex): {}", hex::encode(mh.digest()));
    }
}
