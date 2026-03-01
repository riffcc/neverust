// Copyright (c) 2025 Neverust Authors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Archivist Tree Structure
//!
//! This module implements the Archivist Merkle tree structure for organizing
//! block CIDs with Merkle proofs. The tree is compatible with the Archivist
//! protocol and uses the same compression and proof generation algorithms.
//!
//! # Tree Structure
//!
//! The tree is built bottom-up from a list of block CIDs:
//! - Leaves are the block CIDs (using BlockCodec 0xcd02)
//! - Internal nodes are SHA256 hashes of their children + a key byte
//! - Root CID uses DatasetRootCodec (0xcd03)
//!
//! # Key Bytes
//!
//! The compression function uses different key bytes based on position:
//! - 0x01: Bottom layer (leaf level)
//! - 0x00: Internal layers
//! - 0x02: Odd node (single child)
//! - 0x03: Odd node at bottom layer

use cid::Cid;
use multihash::Multihash;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Errors that can occur when working with Archivist trees
#[derive(Error, Debug)]
pub enum ArchivistTreeError {
    #[error("Cannot create tree from empty block list")]
    EmptyBlockList,

    #[error("Index {index} out of bounds (tree has {leaves} leaves)")]
    IndexOutOfBounds { index: usize, leaves: usize },

    #[error("Tree has no layers")]
    NoLayers,

    #[error("Invalid tree: root layer has {count} nodes instead of 1")]
    InvalidRootLayer { count: usize },

    #[error("Failed to create multihash: {0}")]
    MultihashError(String),

    #[error("Failed to create CID: {0}")]
    CidError(String),
}

pub type Result<T> = std::result::Result<T, ArchivistTreeError>;

/// Key bytes for the Merkle tree compression function
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum TreeKey {
    /// No special key (internal layers)
    None = 0x00,
    /// Bottom layer (leaf level)
    BottomLayer = 0x01,
    /// Odd node (single child)
    Odd = 0x02,
    /// Odd node at bottom layer
    OddAndBottomLayer = 0x03,
}

impl From<u8> for TreeKey {
    fn from(value: u8) -> Self {
        match value {
            0x00 => TreeKey::None,
            0x01 => TreeKey::BottomLayer,
            0x02 => TreeKey::Odd,
            0x03 => TreeKey::OddAndBottomLayer,
            _ => TreeKey::None, // Default to None for invalid values
        }
    }
}

/// A Merkle proof for verifying a leaf in the tree
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivistProof {
    /// The index of the leaf being proved
    pub index: usize,
    /// The number of leaves in the tree
    pub nleaves: usize,
    /// The proof path from leaf to root (sibling hashes)
    pub path: Vec<Vec<u8>>,
}

/// A proof node in a Merkle proof path (deprecated, use ArchivistProof instead)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofNode {
    /// The hash of the sibling node
    pub hash: Vec<u8>,
}

/// Archivist Merkle Tree
///
/// Organizes block CIDs into a Merkle tree structure with support for
/// generating proofs and computing the root CID.
#[derive(Debug, Clone)]
pub struct ArchivistTree {
    /// All layers of the tree, from leaves to root
    /// layers[0] = leaves, layers[last] = root
    layers: Vec<Vec<Vec<u8>>>,
    /// The original block CIDs in order (for reconstructing data)
    block_cids: Vec<Cid>,
}

impl ArchivistTree {
    /// Create a new Archivist tree from block CIDs
    ///
    /// # Arguments
    ///
    /// * `block_cids` - Vector of block CIDs to include in the tree
    ///
    /// # Returns
    ///
    /// A new `ArchivistTree` instance
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The block_cids vector is empty
    /// - Any CID cannot be parsed
    pub fn new(block_cids: Vec<Cid>) -> Result<Self> {
        if block_cids.is_empty() {
            return Err(ArchivistTreeError::EmptyBlockList);
        }

        // Extract the hash digests from the CIDs
        let leaves: Vec<Vec<u8>> = block_cids
            .iter()
            .map(|cid| cid.hash().digest().to_vec())
            .collect();

        // Build the tree layers
        let layers = Self::build_layers(leaves)?;

        Ok(Self {
            layers,
            block_cids,
        })
    }

    /// Build all layers of the Merkle tree
    fn build_layers(leaves: Vec<Vec<u8>>) -> Result<Vec<Vec<Vec<u8>>>> {
        let mut layers = vec![leaves];
        let mut is_bottom_layer = true;

        loop {
            let current_layer = layers.last().unwrap();

            // If we have only one node, we're done
            if current_layer.len() == 1 && !is_bottom_layer {
                break;
            }

            let next_layer = Self::build_next_layer(current_layer, is_bottom_layer)?;
            layers.push(next_layer);
            is_bottom_layer = false;
        }

        Ok(layers)
    }

    /// Build the next layer from the current layer
    fn build_next_layer(current: &[Vec<u8>], is_bottom_layer: bool) -> Result<Vec<Vec<u8>>> {
        let mut next_layer = Vec::new();
        let len = current.len();
        let half_n = len / 2;
        let is_odd = (len % 2) == 1;

        // Process pairs
        for i in 0..half_n {
            let left = &current[2 * i];
            let right = &current[2 * i + 1];
            let key = if is_bottom_layer {
                TreeKey::BottomLayer
            } else {
                TreeKey::None
            };
            let hash = Self::compress(left, right, key)?;
            next_layer.push(hash);
        }

        // Handle odd node if present
        if is_odd {
            let last = &current[len - 1];
            let zero = vec![0u8; 32]; // Zero hash for missing sibling
            let key = if is_bottom_layer {
                TreeKey::OddAndBottomLayer
            } else {
                TreeKey::Odd
            };
            let hash = Self::compress(last, &zero, key)?;
            next_layer.push(hash);
        }

        Ok(next_layer)
    }

    /// Compress two hashes using SHA256
    ///
    /// This follows the Archivist compression algorithm:
    /// hash = SHA256(left || right || key_byte)
    fn compress(left: &[u8], right: &[u8], key: TreeKey) -> Result<Vec<u8>> {
        let mut hasher = Sha256::new();
        hasher.update(left);
        hasher.update(right);
        hasher.update(&[key as u8]);
        Ok(hasher.finalize().to_vec())
    }

    /// Get the root CID of the tree
    ///
    /// Returns the CID of the tree root using DatasetRootCodec (0xcd03)
    pub fn root_cid(&self) -> Result<Cid> {
        let root_layer = self
            .layers
            .last()
            .ok_or(ArchivistTreeError::NoLayers)?;

        if root_layer.len() != 1 {
            return Err(ArchivistTreeError::InvalidRootLayer {
                count: root_layer.len(),
            });
        }

        let root_hash = &root_layer[0];

        // Create multihash from the root hash (SHA2-256)
        let mh = Multihash::wrap(0x12, root_hash)
            .map_err(|e| ArchivistTreeError::MultihashError(e.to_string()))?;

        // Create CID with DatasetRootCodec (0xcd03)
        // CIDv1, codec 0xcd03 (codex-root), SHA2-256 hash
        Ok(Cid::new_v1(0xcd03, mh))
    }

    /// Get a Merkle proof for a block at the given index
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the block (0-based)
    ///
    /// # Returns
    ///
    /// An `ArchivistProof` containing the proof path and metadata
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds
    pub fn get_proof(&self, index: usize) -> Result<ArchivistProof> {
        let nleaves = self
            .layers
            .first()
            .ok_or(ArchivistTreeError::NoLayers)?
            .len();

        if index >= nleaves {
            return Err(ArchivistTreeError::IndexOutOfBounds {
                index,
                leaves: nleaves,
            });
        }

        let depth = self.layers.len() - 1;
        let mut path = Vec::with_capacity(depth);
        let mut k = index;
        let mut m = nleaves;

        for i in 0..depth {
            let j = k ^ 1; // Sibling index
            let sibling_hash = if j < m {
                self.layers[i][j].clone()
            } else {
                vec![0u8; 32] // Zero hash for missing sibling
            };

            path.push(sibling_hash);

            k >>= 1;
            m = (m + 1) >> 1;
        }

        Ok(ArchivistProof {
            index,
            nleaves,
            path,
        })
    }

    /// Get the number of leaves in the tree
    pub fn leaves_count(&self) -> usize {
        self.layers
            .first()
            .map(|layer| layer.len())
            .unwrap_or(0)
    }

    /// Get the depth of the tree (number of layers - 1)
    pub fn depth(&self) -> usize {
        self.layers.len().saturating_sub(1)
    }

    /// Get the block CIDs in order
    ///
    /// Returns a reference to the ordered list of block CIDs used to build the tree.
    /// This is useful for reconstructing the original data from blocks.
    pub fn block_cids(&self) -> &[Cid] {
        &self.block_cids
    }

    /// Serialize the tree's block CIDs to bytes
    ///
    /// This creates a simple serialization of the block CID list for storage.
    /// Format: [count: u32][cid1_len: u32][cid1_bytes][cid2_len: u32][cid2_bytes]...
    pub fn serialize_block_list(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Write count
        buf.extend_from_slice(&(self.block_cids.len() as u32).to_le_bytes());

        // Write each CID
        for cid in &self.block_cids {
            let cid_bytes = cid.to_bytes();
            buf.extend_from_slice(&(cid_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&cid_bytes);
        }

        buf
    }

    /// Deserialize block CIDs from bytes
    ///
    /// Deserializes the block CID list from the format created by serialize_block_list.
    pub fn deserialize_block_list(data: &[u8]) -> Result<Vec<Cid>> {
        use std::io::Cursor;
        use std::io::Read;

        let mut cursor = Cursor::new(data);

        // Read count
        let mut count_bytes = [0u8; 4];
        cursor.read_exact(&mut count_bytes)
            .map_err(|e| ArchivistTreeError::CidError(format!("Failed to read count: {}", e)))?;
        let count = u32::from_le_bytes(count_bytes) as usize;

        // Read CIDs
        let mut cids = Vec::with_capacity(count);
        for _ in 0..count {
            // Read CID length
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)
                .map_err(|e| ArchivistTreeError::CidError(format!("Failed to read CID length: {}", e)))?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read CID bytes
            let mut cid_bytes = vec![0u8; len];
            cursor.read_exact(&mut cid_bytes)
                .map_err(|e| ArchivistTreeError::CidError(format!("Failed to read CID bytes: {}", e)))?;

            // Parse CID
            let cid = Cid::try_from(cid_bytes)
                .map_err(|e| ArchivistTreeError::CidError(format!("Failed to parse CID: {}", e)))?;
            cids.push(cid);
        }

        Ok(cids)
    }

    /// Verify a Merkle proof
    ///
    /// # Arguments
    ///
    /// * `proof` - The Archivist proof
    /// * `leaf` - The leaf hash
    /// * `expected_root` - The expected root hash
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the proof is valid, `Ok(false)` otherwise
    pub fn verify_proof(
        proof: &ArchivistProof,
        leaf: &[u8],
        expected_root: &[u8],
    ) -> Result<bool> {
        let reconstructed = Self::reconstruct_root(&proof.path, proof.nleaves, proof.index, leaf)?;
        Ok(reconstructed == expected_root)
    }

    /// Reconstruct the root hash from a proof
    ///
    /// This follows the Archivist proof verification algorithm which tracks
    /// the number of nodes at each level to detect odd nodes (single children).
    fn reconstruct_root(
        path: &[Vec<u8>],
        nleaves: usize,
        mut index: usize,
        leaf: &[u8],
    ) -> Result<Vec<u8>> {
        let mut current = leaf.to_vec();
        let mut bottom_flag = TreeKey::BottomLayer;
        let mut m = nleaves; // Number of nodes at current level

        for sibling_hash in path.iter() {
            let is_odd_index = (index & 1) != 0;

            current = if is_odd_index {
                // The index is odd, so the node itself is even (sibling is on left)
                Self::compress(sibling_hash, &current, bottom_flag)?
            } else {
                // The index is even
                if index == m - 1 {
                    // This is the last node at this level => single child => odd node
                    let odd_key = TreeKey::from(bottom_flag as u8 + 2);
                    Self::compress(&current, sibling_hash, odd_key)?
                } else {
                    // Even node with sibling
                    Self::compress(&current, sibling_hash, bottom_flag)?
                }
            };

            bottom_flag = TreeKey::None;
            index >>= 1;
            m = (m + 1) >> 1; // Number of nodes at next level
        }

        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test CID from data
    fn create_block_cid(data: &[u8]) -> Cid {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        let mh = Multihash::wrap(0x12, &hash).expect("Failed to create multihash");
        Cid::new_v1(0xcd02, mh) // BlockCodec = 0xcd02
    }

    #[test]
    fn test_empty_tree_fails() {
        let result = ArchivistTree::new(vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_tree_with_1_block() {
        // Create a tree with a single block
        let block_cid = create_block_cid(b"test block 0");
        let tree = ArchivistTree::new(vec![block_cid.clone()])
            .expect("Failed to create tree");

        // Check tree properties
        assert_eq!(tree.leaves_count(), 1);
        assert_eq!(tree.depth(), 1); // 1 leaf + 1 root = 2 layers, depth = 1

        // Check we can get the root CID
        let root_cid = tree.root_cid().expect("Failed to get root CID");
        assert_eq!(root_cid.version(), cid::Version::V1);
        assert_eq!(root_cid.codec(), 0xcd03); // DatasetRootCodec

        // Check we can get a proof
        let proof = tree.get_proof(0).expect("Failed to get proof");
        assert_eq!(proof.path.len(), 1); // Depth = 1, so proof has 1 node

        // Verify the proof
        let leaf_hash = block_cid.hash().digest();
        let root_hash = root_cid.hash().digest();
        let is_valid = ArchivistTree::verify_proof(&proof, leaf_hash, root_hash)
            .expect("Failed to verify proof");
        assert!(is_valid, "Proof verification failed");
    }

    #[test]
    fn test_tree_with_3_blocks() {
        // Create a tree with 3 blocks
        let block_cids: Vec<Cid> = (0..3)
            .map(|i| create_block_cid(format!("test block {}", i).as_bytes()))
            .collect();

        let tree = ArchivistTree::new(block_cids.clone())
            .expect("Failed to create tree");

        // Check tree properties
        assert_eq!(tree.leaves_count(), 3);
        assert_eq!(tree.depth(), 2); // 3 leaves need 2 levels above

        // Check we can get the root CID
        let root_cid = tree.root_cid().expect("Failed to get root CID");
        assert_eq!(root_cid.version(), cid::Version::V1);
        assert_eq!(root_cid.codec(), 0xcd03);

        // Test proofs for all blocks
        let root_hash = root_cid.hash().digest();
        for (i, block_cid) in block_cids.iter().enumerate() {
            let proof = tree.get_proof(i)
                .unwrap_or_else(|_| panic!("Failed to get proof for block {}", i));
            assert_eq!(proof.path.len(), 2, "Expected depth 2 for block {}", i);

            let leaf_hash = block_cid.hash().digest();
            let is_valid = ArchivistTree::verify_proof(&proof, leaf_hash, root_hash)
                .unwrap_or_else(|_| panic!("Failed to verify proof for block {}", i));
            assert!(is_valid, "Proof verification failed for block {}", i);
        }
    }

    #[test]
    fn test_tree_with_100_blocks() {
        // Create a tree with 100 blocks
        let block_cids: Vec<Cid> = (0..100)
            .map(|i| create_block_cid(format!("test block {}", i).as_bytes()))
            .collect();

        let tree = ArchivistTree::new(block_cids.clone())
            .expect("Failed to create tree");

        // Check tree properties
        assert_eq!(tree.leaves_count(), 100);
        // 100 leaves -> depth should be ceil(log2(100)) = 7
        assert!(tree.depth() >= 6 && tree.depth() <= 8);

        // Check we can get the root CID
        let root_cid = tree.root_cid().expect("Failed to get root CID");
        assert_eq!(root_cid.version(), cid::Version::V1);
        assert_eq!(root_cid.codec(), 0xcd03);

        // Test proofs for first, middle, and last blocks
        let test_indices = vec![0, 49, 99];
        let root_hash = root_cid.hash().digest();

        for &i in &test_indices {
            let proof = tree.get_proof(i)
                .unwrap_or_else(|_| panic!("Failed to get proof for block {}", i));
            assert!(
                !proof.path.is_empty(),
                "Expected non-empty proof for block {}",
                i
            );

            let leaf_hash = block_cids[i].hash().digest();
            let is_valid = ArchivistTree::verify_proof(&proof, leaf_hash, root_hash)
                .unwrap_or_else(|_| panic!("Failed to verify proof for block {}", i));
            assert!(is_valid, "Proof verification failed for block {}", i);
        }
    }

    #[test]
    fn test_proof_out_of_bounds() {
        let block_cid = create_block_cid(b"test block 0");
        let tree = ArchivistTree::new(vec![block_cid])
            .expect("Failed to create tree");

        let result = tree.get_proof(1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    #[test]
    fn test_invalid_proof_fails_verification() {
        // Create a tree with 3 blocks
        let block_cids: Vec<Cid> = (0..3)
            .map(|i| create_block_cid(format!("test block {}", i).as_bytes()))
            .collect();

        let tree = ArchivistTree::new(block_cids.clone())
            .expect("Failed to create tree");

        let root_cid = tree.root_cid().expect("Failed to get root CID");
        let root_hash = root_cid.hash().digest();

        // Get a valid proof for block 0
        let proof = tree.get_proof(0).expect("Failed to get proof");

        // Try to verify with wrong leaf (block 1's hash)
        let wrong_leaf = block_cids[1].hash().digest();
        let is_valid = ArchivistTree::verify_proof(&proof, wrong_leaf, root_hash)
            .expect("Failed to verify proof");

        // Should fail because we're using the wrong leaf
        assert!(!is_valid, "Proof should be invalid with wrong leaf");
    }

    #[test]
    fn test_tree_codec_values() {
        // Verify we're using the correct codec values
        let block_cid = create_block_cid(b"test");
        assert_eq!(block_cid.codec(), 0xcd02, "BlockCodec should be 0xcd02");

        let tree = ArchivistTree::new(vec![block_cid])
            .expect("Failed to create tree");
        let root_cid = tree.root_cid().expect("Failed to get root CID");
        assert_eq!(root_cid.codec(), 0xcd03, "DatasetRootCodec should be 0xcd03");
    }

    #[test]
    fn test_power_of_two_blocks() {
        // Test with power of 2 block counts (2, 4, 8, 16)
        for &count in &[2, 4, 8, 16] {
            let block_cids: Vec<Cid> = (0..count)
                .map(|i| create_block_cid(format!("block {}", i).as_bytes()))
                .collect();

            let tree = ArchivistTree::new(block_cids.clone())
                .expect("Failed to create tree");

            assert_eq!(tree.leaves_count(), count);

            // Verify all proofs
            let root_cid = tree.root_cid().expect("Failed to get root CID");
            let root_hash = root_cid.hash().digest();

            for (i, block_cid) in block_cids.iter().enumerate() {
                let proof = tree.get_proof(i).expect("Failed to get proof");
                let leaf_hash = block_cid.hash().digest();
                let is_valid = ArchivistTree::verify_proof(&proof, leaf_hash, root_hash)
                    .expect("Failed to verify proof");
                assert!(
                    is_valid,
                    "Proof verification failed for block {} in {}-block tree",
                    i,
                    count
                );
            }
        }
    }

    #[test]
    fn test_block_cids_getter() {
        // Create tree with known block CIDs
        let block_cids: Vec<Cid> = (0..5)
            .map(|i| create_block_cid(format!("block {}", i).as_bytes()))
            .collect();

        let tree = ArchivistTree::new(block_cids.clone())
            .expect("Failed to create tree");

        // Verify getter returns same CIDs in order
        let retrieved_cids = tree.block_cids();
        assert_eq!(retrieved_cids.len(), block_cids.len());
        for (i, cid) in block_cids.iter().enumerate() {
            assert_eq!(retrieved_cids[i], *cid);
        }
    }

    #[test]
    fn test_serialize_deserialize_block_list() {
        // Create tree with various block CIDs
        let block_cids: Vec<Cid> = (0..10)
            .map(|i| create_block_cid(format!("test block {}", i).as_bytes()))
            .collect();

        let tree = ArchivistTree::new(block_cids.clone())
            .expect("Failed to create tree");

        // Serialize block list
        let serialized = tree.serialize_block_list();
        assert!(!serialized.is_empty());

        // Deserialize block list
        let deserialized = ArchivistTree::deserialize_block_list(&serialized)
            .expect("Failed to deserialize block list");

        // Verify all CIDs match
        assert_eq!(deserialized.len(), block_cids.len());
        for (i, cid) in block_cids.iter().enumerate() {
            assert_eq!(deserialized[i], *cid, "CID mismatch at index {}", i);
        }
    }

    #[test]
    fn test_serialize_deserialize_single_block() {
        let block_cid = create_block_cid(b"single block");
        let tree = ArchivistTree::new(vec![block_cid])
            .expect("Failed to create tree");

        let serialized = tree.serialize_block_list();
        let deserialized = ArchivistTree::deserialize_block_list(&serialized)
            .expect("Failed to deserialize");

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0], block_cid);
    }

    #[test]
    fn test_serialize_deserialize_large_block_list() {
        // Test with 100 blocks
        let block_cids: Vec<Cid> = (0..100)
            .map(|i| create_block_cid(format!("block {}", i).as_bytes()))
            .collect();

        let tree = ArchivistTree::new(block_cids.clone())
            .expect("Failed to create tree");

        let serialized = tree.serialize_block_list();
        let deserialized = ArchivistTree::deserialize_block_list(&serialized)
            .expect("Failed to deserialize large block list");

        assert_eq!(deserialized.len(), 100);
        for (i, cid) in block_cids.iter().enumerate() {
            assert_eq!(deserialized[i], *cid);
        }
    }

    #[test]
    fn test_deserialize_invalid_data() {
        // Empty data
        let result = ArchivistTree::deserialize_block_list(&[]);
        assert!(result.is_err());

        // Incomplete data (only partial count)
        let result = ArchivistTree::deserialize_block_list(&[1, 2]);
        assert!(result.is_err());

        // Invalid CID data
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        buf.extend_from_slice(&5u32.to_le_bytes()); // cid_len = 5
        buf.extend_from_slice(&[1, 2, 3]); // incomplete CID (only 3 bytes instead of 5)

        let result = ArchivistTree::deserialize_block_list(&buf);
        assert!(result.is_err());
    }
}
