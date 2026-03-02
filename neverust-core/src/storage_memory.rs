//! In-memory block storage
//!
//! Provides CID-indexed block storage with BLAKE3 verification and
//! integration with BoTG (Block-over-TGP) protocol.

use cid::Cid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::cid_blake3::{blake3_cid, sha256_cid, verify_blake3, CidError};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Block not found: {0}")]
    BlockNotFound(String),

    #[error("CID verification failed: {0}")]
    VerificationFailed(#[from] CidError),

    #[error("Block already exists: {0}")]
    BlockExists(String),
}

/// A block with its CID and data
#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    pub cid: Cid,
    pub data: Vec<u8>,
}

impl Block {
    /// Create a new block from data, computing its CID
    pub fn new(data: Vec<u8>) -> Result<Self, CidError> {
        let cid = blake3_cid(&data)?;
        Ok(Self { cid, data })
    }

    /// Create an Archivist-compatible SHA2-256 block.
    pub fn new_sha256(data: Vec<u8>) -> Result<Self, CidError> {
        let cid = sha256_cid(&data)?;
        Ok(Self { cid, data })
    }

    /// Create a block from data and verify it matches the expected CID
    pub fn from_cid_and_data(cid: Cid, data: Vec<u8>) -> Result<Self, CidError> {
        verify_blake3(&data, &cid)?;
        Ok(Self { cid, data })
    }

    /// Get the size of the block in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// In-memory block storage with CID-based indexing
pub struct BlockStore {
    /// Blocks indexed by CID
    blocks: Arc<RwLock<HashMap<String, Block>>>,
    /// Total size of all blocks
    total_size: Arc<RwLock<usize>>,
    /// Number of blocks
    block_count: Arc<RwLock<usize>>,
}

impl BlockStore {
    /// Create a new empty block store
    pub fn new() -> Self {
        Self {
            blocks: Arc::new(RwLock::new(HashMap::new())),
            total_size: Arc::new(RwLock::new(0)),
            block_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Store a block, verifying its CID
    pub async fn put(&self, block: Block) -> Result<(), StorageError> {
        let cid_str = block.cid.to_string();

        // Check if block already exists
        {
            let blocks = self.blocks.read().await;
            if blocks.contains_key(&cid_str) {
                debug!("Block already exists: {}", cid_str);
                return Ok(()); // Not an error, block is idempotent
            }
        }

        // Verify block integrity
        verify_blake3(&block.data, &block.cid)?;

        // Store block
        let size = block.size();
        {
            let mut blocks = self.blocks.write().await;
            blocks.insert(cid_str.clone(), block);
        }

        // Update metrics
        {
            let mut total_size = self.total_size.write().await;
            *total_size += size;
        }
        {
            let mut block_count = self.block_count.write().await;
            *block_count += 1;
        }

        info!("Stored block {}, size: {} bytes", cid_str, size);
        Ok(())
    }

    /// Store raw data, computing and verifying CID
    pub async fn put_data(&self, data: Vec<u8>) -> Result<Cid, StorageError> {
        let block = Block::new(data)?;
        let cid = block.cid;
        self.put(block).await?;
        Ok(cid)
    }

    /// Retrieve a block by CID
    pub async fn get(&self, cid: &Cid) -> Result<Block, StorageError> {
        let cid_str = cid.to_string();
        let blocks = self.blocks.read().await;

        blocks
            .get(&cid_str)
            .cloned()
            .ok_or(StorageError::BlockNotFound(cid_str))
    }

    /// Check if a block exists
    pub async fn has(&self, cid: &Cid) -> bool {
        let cid_str = cid.to_string();
        let blocks = self.blocks.read().await;
        blocks.contains_key(&cid_str)
    }

    /// Delete a block
    pub async fn delete(&self, cid: &Cid) -> Result<(), StorageError> {
        let cid_str = cid.to_string();

        let block = {
            let mut blocks = self.blocks.write().await;
            blocks
                .remove(&cid_str)
                .ok_or_else(|| StorageError::BlockNotFound(cid_str.clone()))?
        };

        // Update metrics
        {
            let mut total_size = self.total_size.write().await;
            *total_size -= block.size();
        }
        {
            let mut block_count = self.block_count.write().await;
            *block_count -= 1;
        }

        info!("Deleted block {}", cid_str);
        Ok(())
    }

    /// Get all CIDs in the store
    pub async fn list_cids(&self) -> Vec<Cid> {
        let blocks = self.blocks.read().await;
        blocks.values().map(|block| block.cid).collect()
    }

    /// Get statistics about the block store
    pub async fn stats(&self) -> BlockStoreStats {
        let block_count = *self.block_count.read().await;
        let total_size = *self.total_size.read().await;

        BlockStoreStats {
            block_count,
            total_size,
        }
    }

    /// Clear all blocks
    pub async fn clear(&self) {
        let mut blocks = self.blocks.write().await;
        blocks.clear();

        let mut total_size = self.total_size.write().await;
        *total_size = 0;

        let mut block_count = self.block_count.write().await;
        *block_count = 0;

        info!("Cleared all blocks from store");
    }
}

impl Default for BlockStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the block store
#[derive(Debug, Clone)]
pub struct BlockStoreStats {
    pub block_count: usize,
    pub total_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_block_new() {
        let data = b"hello world".to_vec();
        let block = Block::new(data.clone()).unwrap();

        assert_eq!(block.data, data);
        assert_eq!(block.size(), data.len());
    }

    #[tokio::test]
    async fn test_block_from_cid_and_data() {
        let data = b"hello world".to_vec();
        let block1 = Block::new(data.clone()).unwrap();

        // Should succeed with matching CID
        let block2 = Block::from_cid_and_data(block1.cid, data.clone()).unwrap();
        assert_eq!(block1, block2);

        // Should fail with mismatched CID
        let other_data = b"goodbye world".to_vec();
        let result = Block::from_cid_and_data(block1.cid, other_data);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_store_put_get() {
        let store = BlockStore::new();
        let data = b"hello world".to_vec();
        let block = Block::new(data).unwrap();
        let cid = block.cid;

        // Store block
        store.put(block.clone()).await.unwrap();

        // Retrieve block
        let retrieved = store.get(&cid).await.unwrap();
        assert_eq!(retrieved, block);
    }

    #[tokio::test]
    async fn test_store_put_data() {
        let store = BlockStore::new();
        let data = b"hello world".to_vec();

        // Store raw data
        let cid = store.put_data(data.clone()).await.unwrap();

        // Retrieve block
        let block = store.get(&cid).await.unwrap();
        assert_eq!(block.data, data);
    }

    #[tokio::test]
    async fn test_store_has() {
        let store = BlockStore::new();
        let data = b"hello world".to_vec();
        let block = Block::new(data).unwrap();
        let cid = block.cid;

        // Should not exist yet
        assert!(!store.has(&cid).await);

        // Store block
        store.put(block).await.unwrap();

        // Should exist now
        assert!(store.has(&cid).await);
    }

    #[tokio::test]
    async fn test_store_delete() {
        let store = BlockStore::new();
        let data = b"hello world".to_vec();
        let block = Block::new(data).unwrap();
        let cid = block.cid;

        // Store block
        store.put(block).await.unwrap();
        assert!(store.has(&cid).await);

        // Delete block
        store.delete(&cid).await.unwrap();
        assert!(!store.has(&cid).await);

        // Should fail to get deleted block
        let result = store.get(&cid).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_store_list_cids() {
        let store = BlockStore::new();

        // Store multiple blocks
        let data1 = b"block 1".to_vec();
        let data2 = b"block 2".to_vec();
        let data3 = b"block 3".to_vec();

        let cid1 = store.put_data(data1).await.unwrap();
        let cid2 = store.put_data(data2).await.unwrap();
        let cid3 = store.put_data(data3).await.unwrap();

        // List CIDs
        let cids = store.list_cids().await;
        assert_eq!(cids.len(), 3);
        assert!(cids.contains(&cid1));
        assert!(cids.contains(&cid2));
        assert!(cids.contains(&cid3));
    }

    #[tokio::test]
    async fn test_store_stats() {
        let store = BlockStore::new();

        // Initially empty
        let stats = store.stats().await;
        assert_eq!(stats.block_count, 0);
        assert_eq!(stats.total_size, 0);

        // Store some blocks
        let data1 = vec![1u8; 100];
        let data2 = vec![2u8; 200];

        store.put_data(data1).await.unwrap();
        store.put_data(data2).await.unwrap();

        // Check stats
        let stats = store.stats().await;
        assert_eq!(stats.block_count, 2);
        assert_eq!(stats.total_size, 300);
    }

    #[tokio::test]
    async fn test_store_clear() {
        let store = BlockStore::new();

        // Store some blocks
        store.put_data(b"block 1".to_vec()).await.unwrap();
        store.put_data(b"block 2".to_vec()).await.unwrap();

        let stats = store.stats().await;
        assert_eq!(stats.block_count, 2);

        // Clear store
        store.clear().await;

        let stats = store.stats().await;
        assert_eq!(stats.block_count, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[tokio::test]
    async fn test_store_idempotent_put() {
        let store = BlockStore::new();
        let data = b"hello world".to_vec();
        let block = Block::new(data).unwrap();

        // Store same block twice
        store.put(block.clone()).await.unwrap();
        store.put(block.clone()).await.unwrap();

        // Should only count once
        let stats = store.stats().await;
        assert_eq!(stats.block_count, 1);
    }

    #[tokio::test]
    async fn test_large_blocks() {
        let store = BlockStore::new();

        // Store a large block (1MB)
        let data = vec![0x42u8; 1024 * 1024];
        let cid = store.put_data(data.clone()).await.unwrap();

        // Retrieve and verify
        let block = store.get(&cid).await.unwrap();
        assert_eq!(block.data.len(), 1024 * 1024);
        assert_eq!(block.data, data);

        // Check stats
        let stats = store.stats().await;
        assert_eq!(stats.block_count, 1);
        assert_eq!(stats.total_size, 1024 * 1024);
    }
}
