//! redb-backed persistent block storage.
//!
//! Provides CID-indexed block storage with BLAKE3 verification and
//! persistence via a single embedded redb database file.

use cid::Cid;
use redb::{Database, ReadableTable, TableDefinition};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::cid_blake3::{blake3_cid, verify_blake3, CidError};

const BLOCKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("blocks");

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Block not found: {0}")]
    BlockNotFound(String),

    #[error("CID verification failed: {0}")]
    VerificationFailed(#[from] CidError),

    #[error("Block already exists: {0}")]
    BlockExists(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
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

/// redb-backed persistent block storage with CID-based indexing.
pub struct BlockStore {
    db: Arc<Database>,
    db_path: PathBuf,
}

impl BlockStore {
    /// Create a new block store with a temp-file backend (for testing).
    pub fn new() -> Self {
        let temp_dir =
            std::env::temp_dir().join(format!("neverust-test-{}", rand::random::<u64>()));
        Self::new_with_path(&temp_dir).expect("Failed to create test BlockStore")
    }

    /// Create a new block store with persistent redb backend.
    ///
    /// If `path` is a directory (or has no extension), the database file is
    /// created at `<path>/store.redb`.
    pub fn new_with_path<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let db_path = Self::resolve_db_path(path.as_ref());
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = if db_path.exists() {
            Database::open(&db_path).map_err(Self::db_err)?
        } else {
            Database::create(&db_path).map_err(Self::db_err)?
        };

        // Ensure the table exists.
        {
            let write_txn = db.begin_write().map_err(Self::db_err)?;
            write_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
            write_txn.commit().map_err(Self::db_err)?;
        }

        info!("Opened redb block store at {:?}", db_path);
        Ok(Self {
            db: Arc::new(db),
            db_path,
        })
    }

    fn resolve_db_path(path: &Path) -> PathBuf {
        if (path.exists() && path.is_dir()) || path.extension().is_none() {
            path.join("store.redb")
        } else {
            path.to_path_buf()
        }
    }

    fn db_err<E: std::fmt::Display>(err: E) -> StorageError {
        StorageError::DatabaseError(err.to_string())
    }

    /// Store a block, verifying its CID.
    pub async fn put(&self, block: Block) -> Result<(), StorageError> {
        let cid_str = block.cid.to_string();

        // Verify block integrity (codec-aware)
        // - Data blocks (0xcd02): verify with blake3_cid
        // - Manifests (0xcd01): skip verification
        // - Tree roots (0xcd03): skip verification
        if block.cid.codec() == 0xcd02 {
            verify_blake3(&block.data, &block.cid)?;
        }

        let db = Arc::clone(&self.db);
        let key = cid_str.clone();
        let value = block.data.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(Self::db_err)?;
            {
                let mut table = write_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
                if table.get(key.as_str()).map_err(Self::db_err)?.is_some() {
                    debug!("Block already exists: {}", key);
                    return Ok::<(), StorageError>(());
                }
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(Self::db_err)?;
            }
            write_txn.commit().map_err(Self::db_err)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))??;

        info!("Stored block {}, size: {} bytes", cid_str, block.data.len());
        Ok(())
    }

    /// Store raw data, computing and verifying CID.
    pub async fn put_data(&self, data: Vec<u8>) -> Result<Cid, StorageError> {
        let block = Block::new(data)?;
        let cid = block.cid;
        self.put(block).await?;
        Ok(cid)
    }

    /// Retrieve a block by CID.
    pub async fn get(&self, cid: &Cid) -> Result<Block, StorageError> {
        let cid_str = cid.to_string();
        let db = Arc::clone(&self.db);
        let key = cid_str.clone();
        let cid_copy = *cid;

        let data = tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(Self::db_err)?;
            let table = read_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
            table
                .get(key.as_str())
                .map_err(Self::db_err)?
                .map(|v| v.value().to_vec())
                .ok_or(StorageError::BlockNotFound(key))
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))??;

        Ok(Block {
            cid: cid_copy,
            data,
        })
    }

    /// Check if a block exists.
    pub async fn has(&self, cid: &Cid) -> bool {
        let cid_str = cid.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(Self::db_err)?;
            let table = read_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
            Ok::<bool, StorageError>(table.get(cid_str.as_str()).map_err(Self::db_err)?.is_some())
        })
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(false)
    }

    /// Delete a block.
    pub async fn delete(&self, cid: &Cid) -> Result<(), StorageError> {
        let cid_str = cid.to_string();
        let db = Arc::clone(&self.db);
        let key = cid_str.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(Self::db_err)?;
            {
                let mut table = write_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
                if table.remove(key.as_str()).map_err(Self::db_err)?.is_none() {
                    return Err(StorageError::BlockNotFound(key));
                }
            }
            write_txn.commit().map_err(Self::db_err)?;
            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))??;

        info!("Deleted block {}", cid_str);
        Ok(())
    }

    /// Get all CIDs in the store.
    pub async fn list_cids(&self) -> Vec<Cid> {
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<Vec<Cid>, StorageError> {
            let read_txn = db.begin_read().map_err(Self::db_err)?;
            let table = read_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
            let mut cids = Vec::new();

            for entry in table.iter().map_err(Self::db_err)? {
                let (key, _) = entry.map_err(Self::db_err)?;
                if let Ok(cid) = key.value().parse::<Cid>() {
                    cids.push(cid);
                }
            }

            Ok(cids)
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r)
        .unwrap_or_else(|e| {
            warn!("Failed to list CIDs from {:?}: {}", self.db_path, e);
            Vec::new()
        })
    }

    /// Get statistics about the block store.
    pub async fn stats(&self) -> BlockStoreStats {
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || -> Result<BlockStoreStats, StorageError> {
            let read_txn = db.begin_read().map_err(Self::db_err)?;
            let table = read_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
            let mut block_count = 0usize;
            let mut total_size = 0usize;

            for entry in table.iter().map_err(Self::db_err)? {
                let (_, value) = entry.map_err(Self::db_err)?;
                block_count += 1;
                total_size += value.value().len();
            }

            Ok(BlockStoreStats {
                block_count,
                total_size,
            })
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r)
        .unwrap_or_else(|e| {
            warn!("Failed to compute store stats from {:?}: {}", self.db_path, e);
            BlockStoreStats {
                block_count: 0,
                total_size: 0,
            }
        })
    }

    /// Clear all blocks.
    pub async fn clear(&self) {
        let db = Arc::clone(&self.db);
        let db_path = self.db_path.clone();

        let res = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let write_txn = db.begin_write().map_err(Self::db_err)?;
            {
                let mut table = write_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
                let mut keys = Vec::new();
                for entry in table.iter().map_err(Self::db_err)? {
                    let (key, _) = entry.map_err(Self::db_err)?;
                    keys.push(key.value().to_string());
                }

                for key in keys {
                    let _ = table.remove(key.as_str()).map_err(Self::db_err)?;
                }
            }
            write_txn.commit().map_err(Self::db_err)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r);

        match res {
            Ok(()) => info!("Cleared all blocks from store"),
            Err(e) => warn!("Failed to clear store at {:?}: {}", db_path, e),
        }
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
