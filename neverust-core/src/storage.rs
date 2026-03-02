//! Persistent block storage backends.
//!
//! Supports:
//! - `redb` (default): embedded key/value database.
//! - `geomtree`: FlatFS-style geometric directory tree.
//! - `deltastore`: classed blockfiles with geometric sharding + redb index.
//! - `deltaflat`: classed blockfiles with live-count probe rotation and no hot-path DB.
//!
//! Backend selection:
//! - `NEVERUST_STORAGE_BACKEND=redb|geomtree|deltastore|deltaflat`

use cid::Cid;
use redb::{Database, ReadableTable, TableDefinition};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

use crate::cid_blake3::{blake3_cid, sha256_cid, verify_blake3, CidError};

const BLOCKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("blocks");
const DELTA_INDEX_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("delta_index");
const DELTA_CLASS_STATE_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("delta_class_state");
const DELTAFLAT_VERSION: u8 = 1;
const DELTAFLAT_STATE_EMPTY: u8 = 0;
const DELTAFLAT_STATE_FULL: u8 = 1;
const DELTAFLAT_STATE_TOMBSTONE: u8 = 2;
const DELTAFLAT_MAX_CID_BYTES: usize = 96;
const DELTAFLAT_MAX_LANES: usize = 4096;

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

const DELTA_SIZE_CLASSES: [usize; 11] = [
    512 * 1024,
    1024 * 1024,
    2 * 1024 * 1024,
    4 * 1024 * 1024,
    8 * 1024 * 1024,
    16 * 1024 * 1024,
    32 * 1024 * 1024,
    64 * 1024 * 1024,
    128 * 1024 * 1024,
    256 * 1024 * 1024,
    512 * 1024 * 1024,
];

#[derive(Clone, Copy, Debug)]
struct DeltaLocation {
    class_id: u8,
    file_id: u32,
    offset: u64,
    len: u32,
}

#[derive(Clone, Copy, Debug)]
struct DeltaClassState {
    file_id: u32,
    next_offset: u64,
}

struct RedbStore {
    db: Arc<Database>,
    db_path: PathBuf,
}

#[derive(Clone)]
struct DeltaStore {
    root: PathBuf,
    blocks_root: PathBuf,
    db: Arc<Database>,
    fsync_writes: bool,
    max_file_bytes: u64,
}

#[derive(Clone)]
struct DeltaFlatStore {
    root: PathBuf,
    blocks_root: PathBuf,
    fsync_writes: bool,
    skip_fcntl: bool,
    max_file_bytes: u64,
    max_probe_steps: u32,
    lane_count: u16,
    lane_locks: Arc<Vec<Arc<RwLock<()>>>>,
}

#[derive(Clone)]
struct GeomTreeStore {
    root: PathBuf,
    fsync_writes: bool,
    shard_levels: usize,
    bytes_per_level: usize,
}

enum StoreBackend {
    Redb(RedbStore),
    DeltaStore(DeltaStore),
    DeltaFlat(DeltaFlatStore),
    GeomTree(GeomTreeStore),
}

/// Persistent block storage with pluggable backend.
pub struct BlockStore {
    backend: StoreBackend,
}

impl BlockStore {
    /// Create a new block store with a temp-file backend (for testing).
    pub fn new() -> Self {
        let temp_dir =
            std::env::temp_dir().join(format!("neverust-test-{}", rand::random::<u64>()));
        Self::new_with_path(&temp_dir).expect("Failed to create test BlockStore")
    }

    /// Create a new block store with persistent backend.
    pub fn new_with_path<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let backend = std::env::var("NEVERUST_STORAGE_BACKEND")
            .unwrap_or_else(|_| "redb".to_string())
            .to_lowercase();
        Self::new_with_backend_path(path.as_ref(), &backend)
    }

    /// Create a new block store with an explicit backend.
    pub fn new_with_backend<P: AsRef<Path>>(path: P, backend: &str) -> Result<Self, StorageError> {
        Self::new_with_backend_path(path.as_ref(), backend)
    }

    fn new_with_backend_path(path: &Path, backend: &str) -> Result<Self, StorageError> {
        let backend = backend.to_lowercase();

        match backend.as_str() {
            "deltastore" | "delta" | "delta-store" => {
                let delta = DeltaStore::open(path)?;
                Ok(Self {
                    backend: StoreBackend::DeltaStore(delta),
                })
            }
            "deltaflat" | "delta-flat" | "deltastore-flat" => {
                let deltaflat = DeltaFlatStore::open(path)?;
                Ok(Self {
                    backend: StoreBackend::DeltaFlat(deltaflat),
                })
            }
            "geomtree" => {
                let root = Self::resolve_geomtree_root(path);
                fs::create_dir_all(&root)?;
                let fsync_writes = Self::env_flag("NEVERUST_GEOMTREE_FSYNC", false);
                let shard_levels = Self::env_usize("NEVERUST_GEOMTREE_SHARD_LEVELS", 3).clamp(1, 8);
                let bytes_per_level =
                    Self::env_usize("NEVERUST_GEOMTREE_BYTES_PER_LEVEL", 2).clamp(1, 8);
                info!(
                    "Opened geomtree block store at {:?} (fsync_writes={}, shard_levels={}, bytes_per_level={})",
                    root, fsync_writes, shard_levels, bytes_per_level
                );
                Ok(Self {
                    backend: StoreBackend::GeomTree(GeomTreeStore {
                        root,
                        fsync_writes,
                        shard_levels,
                        bytes_per_level,
                    }),
                })
            }
            "redb" => {
                let redb = RedbStore::open(path)?;
                Ok(Self {
                    backend: StoreBackend::Redb(redb),
                })
            }
            other => {
                warn!(
                    "Unknown NEVERUST_STORAGE_BACKEND='{}', falling back to redb",
                    other
                );
                let redb = RedbStore::open(path)?;
                Ok(Self {
                    backend: StoreBackend::Redb(redb),
                })
            }
        }
    }

    fn resolve_geomtree_root(path: &Path) -> PathBuf {
        if (path.exists() && path.is_dir()) || path.extension().is_none() {
            path.join("geomtree")
        } else {
            path.with_extension("geomtree")
        }
    }

    fn resolve_deltastore_root(path: &Path) -> PathBuf {
        if (path.exists() && path.is_dir()) || path.extension().is_none() {
            path.join("deltastore")
        } else {
            path.with_extension("deltastore")
        }
    }

    fn resolve_deltaflat_root(path: &Path) -> PathBuf {
        if (path.exists() && path.is_dir()) || path.extension().is_none() {
            path.join("deltaflat")
        } else {
            path.with_extension("deltaflat")
        }
    }

    fn env_flag(name: &str, default: bool) -> bool {
        std::env::var(name)
            .ok()
            .map(|v| {
                let t = v.trim().to_ascii_lowercase();
                matches!(t.as_str(), "1" | "true" | "yes" | "on")
            })
            .unwrap_or(default)
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(default)
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(default)
    }

    fn verify_blocks_on_write() -> bool {
        Self::env_flag("NEVERUST_VERIFY_BLOCKS_ON_WRITE", false)
    }

    /// Store multiple blocks, verifying CID integrity.
    pub async fn put_many(&self, blocks: Vec<Block>) -> Result<(), StorageError> {
        match &self.backend {
            StoreBackend::Redb(redb) => redb.put_many(blocks).await,
            StoreBackend::DeltaStore(delta) => delta.put_many(blocks).await,
            StoreBackend::DeltaFlat(deltaflat) => deltaflat.put_many(blocks).await,
            StoreBackend::GeomTree(tree) => tree.put_many(blocks).await,
        }
    }

    /// Store a block, verifying its CID.
    pub async fn put(&self, block: Block) -> Result<(), StorageError> {
        self.put_many(vec![block]).await
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
        match &self.backend {
            StoreBackend::Redb(redb) => redb.get(cid).await,
            StoreBackend::DeltaStore(delta) => delta.get(cid).await,
            StoreBackend::DeltaFlat(deltaflat) => deltaflat.get(cid).await,
            StoreBackend::GeomTree(tree) => tree.get(cid).await,
        }
    }

    /// Check if a block exists.
    pub async fn has(&self, cid: &Cid) -> bool {
        match &self.backend {
            StoreBackend::Redb(redb) => redb.has(cid).await,
            StoreBackend::DeltaStore(delta) => delta.has(cid).await,
            StoreBackend::DeltaFlat(deltaflat) => deltaflat.has(cid).await,
            StoreBackend::GeomTree(tree) => tree.has(cid).await,
        }
    }

    /// Delete a block.
    pub async fn delete(&self, cid: &Cid) -> Result<(), StorageError> {
        match &self.backend {
            StoreBackend::Redb(redb) => redb.delete(cid).await,
            StoreBackend::DeltaStore(delta) => delta.delete(cid).await,
            StoreBackend::DeltaFlat(deltaflat) => deltaflat.delete(cid).await,
            StoreBackend::GeomTree(tree) => tree.delete(cid).await,
        }
    }

    /// Get all CIDs in the store.
    pub async fn list_cids(&self) -> Vec<Cid> {
        match &self.backend {
            StoreBackend::Redb(redb) => redb.list_cids().await,
            StoreBackend::DeltaStore(delta) => delta.list_cids().await,
            StoreBackend::DeltaFlat(deltaflat) => deltaflat.list_cids().await,
            StoreBackend::GeomTree(tree) => tree.list_cids().await,
        }
    }

    /// Get statistics about the block store.
    pub async fn stats(&self) -> BlockStoreStats {
        match &self.backend {
            StoreBackend::Redb(redb) => redb.stats().await,
            StoreBackend::DeltaStore(delta) => delta.stats().await,
            StoreBackend::DeltaFlat(deltaflat) => deltaflat.stats().await,
            StoreBackend::GeomTree(tree) => tree.stats().await,
        }
    }

    /// Clear all blocks.
    pub async fn clear(&self) {
        match &self.backend {
            StoreBackend::Redb(redb) => redb.clear().await,
            StoreBackend::DeltaStore(delta) => delta.clear().await,
            StoreBackend::DeltaFlat(deltaflat) => deltaflat.clear().await,
            StoreBackend::GeomTree(tree) => tree.clear().await,
        }
    }
}

impl RedbStore {
    fn open(path: &Path) -> Result<Self, StorageError> {
        let db_path = Self::resolve_db_path(path);
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)?;
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

    async fn put_many(&self, blocks: Vec<Block>) -> Result<(), StorageError> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut prepared = Vec::with_capacity(blocks.len());
        for block in blocks {
            // Optional integrity re-verification. By default we trust `Block::new`,
            // which already computes CID from bytes and avoids hashing twice.
            if BlockStore::verify_blocks_on_write() && block.cid.codec() == 0xcd02 {
                verify_blake3(&block.data, &block.cid)?;
            }
            prepared.push((block.cid.to_string(), block.data));
        }

        let stored_count = prepared.len();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(Self::db_err)?;
            {
                let mut table = write_txn.open_table(BLOCKS_TABLE).map_err(Self::db_err)?;
                for (key, value) in &prepared {
                    if table.get(key.as_str()).map_err(Self::db_err)?.is_some() {
                        debug!("Block already exists: {}", key);
                        continue;
                    }
                    table
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::db_err)?;
                }
            }
            write_txn.commit().map_err(Self::db_err)?;
            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))??;

        info!("Stored {} blocks in redb batch", stored_count);
        Ok(())
    }

    async fn get(&self, cid: &Cid) -> Result<Block, StorageError> {
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

    async fn has(&self, cid: &Cid) -> bool {
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

    async fn delete(&self, cid: &Cid) -> Result<(), StorageError> {
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

    async fn list_cids(&self) -> Vec<Cid> {
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

    async fn stats(&self) -> BlockStoreStats {
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
            warn!(
                "Failed to compute store stats from {:?}: {}",
                self.db_path, e
            );
            BlockStoreStats {
                block_count: 0,
                total_size: 0,
            }
        })
    }

    async fn clear(&self) {
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
            Ok(()) => info!("Cleared all blocks from redb store"),
            Err(e) => warn!("Failed to clear redb store at {:?}: {}", db_path, e),
        }
    }
}

impl DeltaStore {
    const LOCATION_ENCODED_LEN: usize = 1 + 1 + 4 + 8 + 4;
    const CLASS_STATE_ENCODED_LEN: usize = 4 + 8;

    fn open(path: &Path) -> Result<Self, StorageError> {
        let root = BlockStore::resolve_deltastore_root(path);
        let blocks_root = root.join("blocks");
        fs::create_dir_all(&blocks_root)?;

        let db_path = root.join("index.redb");
        let db = if db_path.exists() {
            Database::open(&db_path).map_err(RedbStore::db_err)?
        } else {
            Database::create(&db_path).map_err(RedbStore::db_err)?
        };

        {
            let write_txn = db.begin_write().map_err(RedbStore::db_err)?;
            write_txn
                .open_table(DELTA_INDEX_TABLE)
                .map_err(RedbStore::db_err)?;
            write_txn
                .open_table(DELTA_CLASS_STATE_TABLE)
                .map_err(RedbStore::db_err)?;
            write_txn.commit().map_err(RedbStore::db_err)?;
        }

        let fsync_writes = BlockStore::env_flag("NEVERUST_DELTASTORE_FSYNC", false);
        let min_max_bytes = *DELTA_SIZE_CLASSES.last().unwrap_or(&(512 * 1024 * 1024)) as u64;
        let max_file_bytes =
            BlockStore::env_u64("NEVERUST_DELTASTORE_MAX_FILE_BYTES", 1024 * 1024 * 1024)
                .max(min_max_bytes);

        info!(
            "Opened deltastore at {:?} (fsync_writes={}, max_file_bytes={})",
            root, fsync_writes, max_file_bytes
        );

        Ok(Self {
            root,
            blocks_root,
            db: Arc::new(db),
            fsync_writes,
            max_file_bytes,
        })
    }

    fn encode_location(loc: DeltaLocation) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::LOCATION_ENCODED_LEN);
        out.push(1u8);
        out.push(loc.class_id);
        out.extend_from_slice(&loc.file_id.to_le_bytes());
        out.extend_from_slice(&loc.offset.to_le_bytes());
        out.extend_from_slice(&loc.len.to_le_bytes());
        out
    }

    fn decode_location(bytes: &[u8]) -> Result<DeltaLocation, StorageError> {
        if bytes.len() != Self::LOCATION_ENCODED_LEN {
            return Err(StorageError::DatabaseError(format!(
                "delta location length mismatch: expected {}, got {}",
                Self::LOCATION_ENCODED_LEN,
                bytes.len()
            )));
        }
        if bytes[0] != 1 {
            return Err(StorageError::DatabaseError(format!(
                "unsupported delta location version {}",
                bytes[0]
            )));
        }
        let class_id = bytes[1];
        let file_id = u32::from_le_bytes(
            bytes[2..6]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad file_id bytes".to_string()))?,
        );
        let offset = u64::from_le_bytes(
            bytes[6..14]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad offset bytes".to_string()))?,
        );
        let len = u32::from_le_bytes(
            bytes[14..18]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad len bytes".to_string()))?,
        );
        Ok(DeltaLocation {
            class_id,
            file_id,
            offset,
            len,
        })
    }

    fn encode_class_state(state: DeltaClassState) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::CLASS_STATE_ENCODED_LEN);
        out.extend_from_slice(&state.file_id.to_le_bytes());
        out.extend_from_slice(&state.next_offset.to_le_bytes());
        out
    }

    fn decode_class_state(bytes: &[u8]) -> Result<DeltaClassState, StorageError> {
        if bytes.len() != Self::CLASS_STATE_ENCODED_LEN {
            return Err(StorageError::DatabaseError(format!(
                "delta class state length mismatch: expected {}, got {}",
                Self::CLASS_STATE_ENCODED_LEN,
                bytes.len()
            )));
        }
        let file_id = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad class-state file_id".to_string()))?,
        );
        let next_offset =
            u64::from_le_bytes(bytes[4..12].try_into().map_err(|_| {
                StorageError::DatabaseError("bad class-state next_offset".to_string())
            })?);
        Ok(DeltaClassState {
            file_id,
            next_offset,
        })
    }

    fn class_for_len(len: usize) -> (u8, usize) {
        for (i, class) in DELTA_SIZE_CLASSES.iter().copied().enumerate() {
            if len <= class {
                return (i as u8, class);
            }
        }
        (
            (DELTA_SIZE_CLASSES.len() - 1) as u8,
            *DELTA_SIZE_CLASSES.last().unwrap(),
        )
    }

    fn class_state_key(class_id: u8) -> String {
        format!("c{:02}", class_id)
    }

    fn class_dir(&self, class_id: u8) -> PathBuf {
        self.blocks_root.join(format!("c{:02}", class_id))
    }

    fn blockfile_path(&self, class_id: u8, file_id: u32) -> PathBuf {
        self.class_dir(class_id)
            .join(format!("blk-{:08}.dat", file_id))
    }

    async fn put_many(&self, blocks: Vec<Block>) -> Result<(), StorageError> {
        if blocks.is_empty() {
            return Ok(());
        }

        if BlockStore::verify_blocks_on_write() {
            for block in &blocks {
                if block.cid.codec() == 0xcd02 {
                    verify_blake3(&block.data, &block.cid)?;
                }
            }
        }

        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let write_txn = store.db.begin_write().map_err(RedbStore::db_err)?;
            {
                let mut index = write_txn
                    .open_table(DELTA_INDEX_TABLE)
                    .map_err(RedbStore::db_err)?;
                let mut class_state = write_txn
                    .open_table(DELTA_CLASS_STATE_TABLE)
                    .map_err(RedbStore::db_err)?;
                let skip_exists_check =
                    BlockStore::env_flag("NEVERUST_DELTASTORE_SKIP_EXISTS_CHECK", false);
                let mut state_cache: HashMap<u8, DeltaClassState> = HashMap::new();
                let mut file_cache: HashMap<(u8, u32), fs::File> = HashMap::new();

                for block in blocks {
                    let cid_key = block.cid.to_string();
                    if !skip_exists_check
                        && index
                            .get(cid_key.as_str())
                            .map_err(RedbStore::db_err)?
                            .is_some()
                    {
                        continue;
                    }

                    let (class_id, _class_bytes) = Self::class_for_len(block.data.len());
                    let state = if let Some(state) = state_cache.get_mut(&class_id) {
                        state
                    } else {
                        let state_key = Self::class_state_key(class_id);
                        let loaded = match class_state
                            .get(state_key.as_str())
                            .map_err(RedbStore::db_err)?
                        {
                            Some(v) => Self::decode_class_state(v.value())?,
                            None => DeltaClassState {
                                file_id: 0,
                                next_offset: 0,
                            },
                        };
                        state_cache.insert(class_id, loaded);
                        state_cache.get_mut(&class_id).expect("state just inserted")
                    };

                    let loc = loop {
                        if state.next_offset + block.data.len() as u64 > store.max_file_bytes {
                            state.file_id = state.file_id.checked_add(1).ok_or_else(|| {
                                StorageError::DatabaseError("delta file_id overflow".to_string())
                            })?;
                            state.next_offset = 0;
                            continue;
                        }

                        let file_key = (class_id, state.file_id);
                        if !file_cache.contains_key(&file_key) {
                            let path = store.blockfile_path(class_id, state.file_id);
                            if let Some(parent) = path.parent() {
                                fs::create_dir_all(parent)?;
                            }

                            let mut file = fs::OpenOptions::new()
                                .create(true)
                                .read(true)
                                .write(true)
                                .open(&path)?;
                            let file_len = file.metadata()?.len();
                            if file_len != state.next_offset {
                                state.next_offset = file_len;
                            }
                            file.seek(SeekFrom::Start(state.next_offset))?;
                            file_cache.insert(file_key, file);
                        }

                        let file = file_cache
                            .get_mut(&file_key)
                            .expect("file cache entry must exist");
                        file.seek(SeekFrom::Start(state.next_offset))?;
                        file.write_all(&block.data)?;

                        let offset = state.next_offset;
                        state.next_offset = state
                            .next_offset
                            .checked_add(block.data.len() as u64)
                            .ok_or_else(|| {
                                StorageError::DatabaseError("delta offset overflow".to_string())
                            })?;

                        break DeltaLocation {
                            class_id,
                            file_id: state.file_id,
                            offset,
                            len: u32::try_from(block.data.len()).map_err(|_| {
                                StorageError::DatabaseError("block length exceeds u32".to_string())
                            })?,
                        };
                    };
                    let loc_bytes = Self::encode_location(loc);

                    index
                        .insert(cid_key.as_str(), loc_bytes.as_slice())
                        .map_err(RedbStore::db_err)?;
                }

                if store.fsync_writes {
                    for file in file_cache.values_mut() {
                        file.sync_data()?;
                    }
                }

                for (class_id, state) in state_cache {
                    let state_key = Self::class_state_key(class_id);
                    let state_bytes = Self::encode_class_state(state);
                    class_state
                        .insert(state_key.as_str(), state_bytes.as_slice())
                        .map_err(RedbStore::db_err)?;
                }
            }
            write_txn.commit().map_err(RedbStore::db_err)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn get(&self, cid: &Cid) -> Result<Block, StorageError> {
        let cid_copy = *cid;
        let cid_key = cid.to_string();
        let store = self.clone();

        tokio::task::spawn_blocking(move || -> Result<Block, StorageError> {
            let read_txn = store.db.begin_read().map_err(RedbStore::db_err)?;
            let index = read_txn
                .open_table(DELTA_INDEX_TABLE)
                .map_err(RedbStore::db_err)?;
            let loc_bytes = index
                .get(cid_key.as_str())
                .map_err(RedbStore::db_err)?
                .ok_or_else(|| StorageError::BlockNotFound(cid_key.clone()))?;
            let loc = Self::decode_location(loc_bytes.value())?;
            let path = store.blockfile_path(loc.class_id, loc.file_id);

            let mut file = fs::OpenOptions::new().read(true).open(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::BlockNotFound(cid_key.clone())
                } else {
                    StorageError::IoError(e)
                }
            })?;
            file.seek(SeekFrom::Start(loc.offset))?;
            let mut data = vec![0u8; loc.len as usize];
            file.read_exact(&mut data)?;

            Ok(Block {
                cid: cid_copy,
                data,
            })
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn has(&self, cid: &Cid) -> bool {
        let cid_key = cid.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> Result<bool, StorageError> {
            let read_txn = db.begin_read().map_err(RedbStore::db_err)?;
            let index = read_txn
                .open_table(DELTA_INDEX_TABLE)
                .map_err(RedbStore::db_err)?;
            Ok(index
                .get(cid_key.as_str())
                .map_err(RedbStore::db_err)?
                .is_some())
        })
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(false)
    }

    async fn delete(&self, cid: &Cid) -> Result<(), StorageError> {
        let cid_key = cid.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let write_txn = db.begin_write().map_err(RedbStore::db_err)?;
            {
                let mut index = write_txn
                    .open_table(DELTA_INDEX_TABLE)
                    .map_err(RedbStore::db_err)?;
                if index
                    .remove(cid_key.as_str())
                    .map_err(RedbStore::db_err)?
                    .is_none()
                {
                    return Err(StorageError::BlockNotFound(cid_key));
                }
            }
            write_txn.commit().map_err(RedbStore::db_err)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn list_cids(&self) -> Vec<Cid> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> Result<Vec<Cid>, StorageError> {
            let read_txn = db.begin_read().map_err(RedbStore::db_err)?;
            let index = read_txn
                .open_table(DELTA_INDEX_TABLE)
                .map_err(RedbStore::db_err)?;
            let mut out = Vec::new();
            for entry in index.iter().map_err(RedbStore::db_err)? {
                let (k, _) = entry.map_err(RedbStore::db_err)?;
                if let Ok(cid) = k.value().parse::<Cid>() {
                    out.push(cid);
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r)
        .unwrap_or_else(|e| {
            warn!("Failed to list CIDs from deltastore {:?}: {}", self.root, e);
            Vec::new()
        })
    }

    async fn stats(&self) -> BlockStoreStats {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> Result<BlockStoreStats, StorageError> {
            let read_txn = db.begin_read().map_err(RedbStore::db_err)?;
            let index = read_txn
                .open_table(DELTA_INDEX_TABLE)
                .map_err(RedbStore::db_err)?;
            let mut block_count = 0usize;
            let mut total_size = 0usize;
            for entry in index.iter().map_err(RedbStore::db_err)? {
                let (_, value) = entry.map_err(RedbStore::db_err)?;
                let loc = Self::decode_location(value.value())?;
                block_count += 1;
                total_size += loc.len as usize;
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
            warn!(
                "Failed to compute deltastore stats from {:?}: {}",
                self.root, e
            );
            BlockStoreStats {
                block_count: 0,
                total_size: 0,
            }
        })
    }

    async fn clear(&self) {
        let db = Arc::clone(&self.db);
        let blocks_root = self.blocks_root.clone();
        let root = self.root.clone();
        let res = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            if blocks_root.exists() {
                fs::remove_dir_all(&blocks_root)?;
            }
            fs::create_dir_all(&blocks_root)?;

            let write_txn = db.begin_write().map_err(RedbStore::db_err)?;
            {
                let mut index = write_txn
                    .open_table(DELTA_INDEX_TABLE)
                    .map_err(RedbStore::db_err)?;
                let mut keys = Vec::new();
                for entry in index.iter().map_err(RedbStore::db_err)? {
                    let (k, _) = entry.map_err(RedbStore::db_err)?;
                    keys.push(k.value().to_string());
                }
                for k in keys {
                    let _ = index.remove(k.as_str()).map_err(RedbStore::db_err)?;
                }
            }
            {
                let mut class_state = write_txn
                    .open_table(DELTA_CLASS_STATE_TABLE)
                    .map_err(RedbStore::db_err)?;
                let mut keys = Vec::new();
                for entry in class_state.iter().map_err(RedbStore::db_err)? {
                    let (k, _) = entry.map_err(RedbStore::db_err)?;
                    keys.push(k.value().to_string());
                }
                for k in keys {
                    let _ = class_state.remove(k.as_str()).map_err(RedbStore::db_err)?;
                }
            }
            write_txn.commit().map_err(RedbStore::db_err)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r);

        match res {
            Ok(()) => info!("Cleared all blocks from deltastore at {:?}", root),
            Err(e) => warn!("Failed to clear deltastore at {:?}: {}", root, e),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct DeltaFlatControl {
    level: u32,
    split_ptr: u32,
    item_count: u64,
    next_file_id: u32,
    next_offset: u64,
}

#[derive(Clone, Debug)]
struct DeltaFlatEntry {
    fingerprint: u8,
    file_id: u32,
    offset: u64,
    len: u32,
    hash64: u64,
    cid: Vec<u8>,
}

const DELTAFLAT_CONTROL_MAGIC: [u8; 4] = *b"DFC1";
const DELTAFLAT_CONTROL_LEN: usize = 64;
const DELTAFLAT_BUCKET_BYTES: usize = 4096;
const DELTAFLAT_ENTRY_BYTES: usize = 128;
const DELTAFLAT_ENTRY_CID_OFF: usize = 28;
const DELTAFLAT_ENTRIES_PER_BUCKET: usize = DELTAFLAT_BUCKET_BYTES / DELTAFLAT_ENTRY_BYTES;
const DELTAFLAT_LOAD_FACTOR_PCT: u64 = 90;

impl DeltaFlatStore {
    fn open(path: &Path) -> Result<Self, StorageError> {
        let root = BlockStore::resolve_deltaflat_root(path);
        let blocks_root = root.join("blocks");
        fs::create_dir_all(&blocks_root)?;

        let fsync_writes = BlockStore::env_flag("NEVERUST_DELTAFLAT_FSYNC", false);
        let skip_fcntl = BlockStore::env_flag("NEVERUST_DELTAFLAT_SKIP_FCNTL", false);
        let max_file_bytes =
            BlockStore::env_u64("NEVERUST_DELTAFLAT_MAX_FILE_BYTES", 1024 * 1024 * 1024)
                .max(*DELTA_SIZE_CLASSES.last().unwrap_or(&(512 * 1024 * 1024)) as u64);
        let max_probe_steps =
            BlockStore::env_usize("NEVERUST_DELTAFLAT_MAX_PROBE_STEPS", 16384).max(1) as u32;
        let default_lanes = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8)
            .clamp(1, 16);
        let lane_count = BlockStore::env_usize("NEVERUST_DELTAFLAT_LANES", default_lanes)
            .clamp(1, DELTAFLAT_MAX_LANES) as u16;
        let lock_count = DELTA_SIZE_CLASSES
            .len()
            .checked_mul(lane_count as usize)
            .ok_or_else(|| {
                StorageError::DatabaseError("deltaflat lane lock overflow".to_string())
            })?;
        let mut lane_locks = Vec::with_capacity(lock_count);
        for _ in 0..lock_count {
            lane_locks.push(Arc::new(RwLock::new(())));
        }

        info!(
            "Opened deltaflat store at {:?} (fsync_writes={}, skip_fcntl={}, max_file_bytes={}, max_probe_steps={}, lane_count={})",
            root, fsync_writes, skip_fcntl, max_file_bytes, max_probe_steps, lane_count
        );

        Ok(Self {
            root,
            blocks_root,
            fsync_writes,
            skip_fcntl,
            max_file_bytes,
            max_probe_steps,
            lane_count,
            lane_locks: Arc::new(lane_locks),
        })
    }

    fn class_for_len(len: usize) -> (u8, usize) {
        DeltaStore::class_for_len(len)
    }

    fn class_dir(&self, class_id: u8) -> PathBuf {
        self.blocks_root.join(format!("c{:02}", class_id))
    }

    fn lane_dir(&self, class_id: u8, lane_id: u16) -> PathBuf {
        if self.lane_count <= 1 {
            self.class_dir(class_id)
        } else {
            self.class_dir(class_id).join(format!("l{:04}", lane_id))
        }
    }

    fn class_control_path(&self, class_id: u8, lane_id: u16) -> PathBuf {
        self.lane_dir(class_id, lane_id).join("control.bin")
    }

    fn class_index_path(&self, class_id: u8, lane_id: u16) -> PathBuf {
        self.lane_dir(class_id, lane_id).join("index.bin")
    }

    fn blockfile_path(&self, class_id: u8, lane_id: u16, file_id: u32) -> PathBuf {
        self.lane_dir(class_id, lane_id)
            .join(format!("blk-{:08}.dat", file_id))
    }

    fn open_class_files(
        &self,
        class_id: u8,
        lane_id: u16,
    ) -> Result<(fs::File, fs::File), StorageError> {
        let lane_dir = self.lane_dir(class_id, lane_id);
        fs::create_dir_all(&lane_dir)?;
        let control = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.class_control_path(class_id, lane_id))?;
        let index = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.class_index_path(class_id, lane_id))?;
        Ok((control, index))
    }

    #[inline]
    fn lane_for_cid(&self, cid_bytes: &[u8]) -> u16 {
        if self.lane_count <= 1 {
            return 0;
        }
        let (_, _, _, hash64) = Self::split_hash(cid_bytes);
        (hash64 % self.lane_count as u64) as u16
    }

    #[inline]
    fn lane_lock_index(&self, class_id: u8, lane_id: u16) -> usize {
        class_id as usize * self.lane_count as usize + lane_id as usize
    }

    fn lane_read_guard(
        &self,
        class_id: u8,
        lane_id: u16,
    ) -> Result<std::sync::RwLockReadGuard<'_, ()>, StorageError> {
        self.lane_locks[self.lane_lock_index(class_id, lane_id)]
            .read()
            .map_err(|_| {
                StorageError::DatabaseError("deltaflat lane read lock poisoned".to_string())
            })
    }

    fn lane_write_guard(
        &self,
        class_id: u8,
        lane_id: u16,
    ) -> Result<std::sync::RwLockWriteGuard<'_, ()>, StorageError> {
        self.lane_locks[self.lane_lock_index(class_id, lane_id)]
            .write()
            .map_err(|_| {
                StorageError::DatabaseError("deltaflat lane write lock poisoned".to_string())
            })
    }

    #[inline]
    fn lock_control(&self, file: &fs::File, write_lock: bool) -> Result<(), StorageError> {
        if self.skip_fcntl {
            return Ok(());
        }
        Self::lock_range(file, 0, DELTAFLAT_CONTROL_LEN as u64, write_lock)
    }

    #[inline]
    fn unlock_control(&self, file: &fs::File) -> Result<(), StorageError> {
        if self.skip_fcntl {
            return Ok(());
        }
        Self::unlock_range(file, 0, DELTAFLAT_CONTROL_LEN as u64)
    }

    #[cfg(unix)]
    fn lock_range(
        file: &fs::File,
        start: u64,
        len: u64,
        write_lock: bool,
    ) -> Result<(), StorageError> {
        let mut fl = libc::flock {
            l_type: if write_lock {
                libc::F_WRLCK as libc::c_short
            } else {
                libc::F_RDLCK as libc::c_short
            },
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: start as libc::off_t,
            l_len: len as libc::off_t,
            l_pid: 0,
        };
        // SAFETY: `fl` is a valid pointer for the duration of the call.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLKW, &mut fl) };
        if rc == -1 {
            return Err(StorageError::IoError(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn unlock_range(file: &fs::File, start: u64, len: u64) -> Result<(), StorageError> {
        let mut fl = libc::flock {
            l_type: libc::F_UNLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: start as libc::off_t,
            l_len: len as libc::off_t,
            l_pid: 0,
        };
        // SAFETY: `fl` is a valid pointer for the duration of the call.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &mut fl) };
        if rc == -1 {
            return Err(StorageError::IoError(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn lock_range(
        _file: &fs::File,
        _start: u64,
        _len: u64,
        _write_lock: bool,
    ) -> Result<(), StorageError> {
        Err(StorageError::DatabaseError(
            "deltaflat range locking requires unix".to_string(),
        ))
    }

    #[cfg(not(unix))]
    fn unlock_range(_file: &fs::File, _start: u64, _len: u64) -> Result<(), StorageError> {
        Ok(())
    }

    fn split_hash(cid_bytes: &[u8]) -> (u64, u64, u8, u64) {
        let digest = blake3::hash(cid_bytes);
        let bytes = digest.as_bytes();
        let mut even = 0u64;
        let mut odd = 0u64;
        for (idx, b) in bytes.iter().enumerate() {
            if idx % 2 == 0 {
                even = even.rotate_left(7) ^ (*b as u64);
            } else {
                odd = odd.rotate_left(7) ^ (*b as u64);
            }
        }
        let fingerprint = (odd as u8) | 1;
        let hash64 = u64::from_le_bytes(bytes[0..8].try_into().unwrap_or([0u8; 8]));
        (even, odd, fingerprint, hash64)
    }

    fn entry_offset(slot: usize) -> usize {
        slot * DELTAFLAT_ENTRY_BYTES
    }

    fn entry_state(page: &[u8; DELTAFLAT_BUCKET_BYTES], slot: usize) -> u8 {
        page[Self::entry_offset(slot)]
    }

    fn decode_entry(
        page: &[u8; DELTAFLAT_BUCKET_BYTES],
        slot: usize,
    ) -> Result<Option<DeltaFlatEntry>, StorageError> {
        let o = Self::entry_offset(slot);
        if page[o] != DELTAFLAT_STATE_FULL {
            return Ok(None);
        }

        let cid_len = page[o + 2] as usize;
        if cid_len == 0 || cid_len > DELTAFLAT_MAX_CID_BYTES {
            return Err(StorageError::DatabaseError(format!(
                "deltaflat malformed cid length {} at slot {}",
                cid_len, slot
            )));
        }

        let file_id = u32::from_le_bytes(
            page[o + 4..o + 8]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad deltaflat file_id".to_string()))?,
        );
        let offset = u64::from_le_bytes(
            page[o + 8..o + 16]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad deltaflat offset".to_string()))?,
        );
        let len = u32::from_le_bytes(
            page[o + 16..o + 20]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad deltaflat len".to_string()))?,
        );
        let hash64 = u64::from_le_bytes(
            page[o + 20..o + 28]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad deltaflat hash64".to_string()))?,
        );
        let cid = page[o + DELTAFLAT_ENTRY_CID_OFF..o + DELTAFLAT_ENTRY_CID_OFF + cid_len].to_vec();

        Ok(Some(DeltaFlatEntry {
            fingerprint: page[o + 1],
            file_id,
            offset,
            len,
            hash64,
            cid,
        }))
    }

    fn write_entry_to_page(
        page: &mut [u8; DELTAFLAT_BUCKET_BYTES],
        slot: usize,
        entry: &DeltaFlatEntry,
    ) -> Result<(), StorageError> {
        if entry.cid.is_empty() || entry.cid.len() > DELTAFLAT_MAX_CID_BYTES {
            return Err(StorageError::DatabaseError(format!(
                "deltaflat entry cid length {} out of bounds",
                entry.cid.len()
            )));
        }

        let o = Self::entry_offset(slot);
        page[o..o + DELTAFLAT_ENTRY_BYTES].fill(0);
        page[o] = DELTAFLAT_STATE_FULL;
        page[o + 1] = entry.fingerprint;
        page[o + 2] = entry.cid.len() as u8;
        page[o + 4..o + 8].copy_from_slice(&entry.file_id.to_le_bytes());
        page[o + 8..o + 16].copy_from_slice(&entry.offset.to_le_bytes());
        page[o + 16..o + 20].copy_from_slice(&entry.len.to_le_bytes());
        page[o + 20..o + 28].copy_from_slice(&entry.hash64.to_le_bytes());
        page[o + DELTAFLAT_ENTRY_CID_OFF..o + DELTAFLAT_ENTRY_CID_OFF + entry.cid.len()]
            .copy_from_slice(&entry.cid);
        Ok(())
    }

    fn mark_tombstone(page: &mut [u8; DELTAFLAT_BUCKET_BYTES], slot: usize) {
        let o = Self::entry_offset(slot);
        page[o..o + DELTAFLAT_ENTRY_BYTES].fill(0);
        page[o] = DELTAFLAT_STATE_TOMBSTONE;
    }

    fn find_entry_slot(
        page: &[u8; DELTAFLAT_BUCKET_BYTES],
        cid: &[u8],
        fingerprint: u8,
        hash64: u64,
    ) -> Result<Option<(usize, DeltaFlatEntry)>, StorageError> {
        for slot in 0..DELTAFLAT_ENTRIES_PER_BUCKET {
            if Self::entry_state(page, slot) != DELTAFLAT_STATE_FULL {
                continue;
            }
            let Some(entry) = Self::decode_entry(page, slot)? else {
                continue;
            };
            if entry.fingerprint == fingerprint && entry.hash64 == hash64 && entry.cid == cid {
                return Ok(Some((slot, entry)));
            }
        }
        Ok(None)
    }

    fn entry_matches(
        page: &[u8; DELTAFLAT_BUCKET_BYTES],
        slot: usize,
        cid: &[u8],
        fingerprint: u8,
        hash64: u64,
    ) -> Result<bool, StorageError> {
        let o = Self::entry_offset(slot);
        if page[o] != DELTAFLAT_STATE_FULL {
            return Ok(false);
        }
        if page[o + 1] != fingerprint {
            return Ok(false);
        }
        let slot_hash64 = u64::from_le_bytes(
            page[o + 20..o + 28]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("bad deltaflat hash64".to_string()))?,
        );
        if slot_hash64 != hash64 {
            return Ok(false);
        }

        let cid_len = page[o + 2] as usize;
        if cid_len == 0 || cid_len > DELTAFLAT_MAX_CID_BYTES {
            return Err(StorageError::DatabaseError(format!(
                "deltaflat malformed cid length {} at slot {}",
                cid_len, slot
            )));
        }
        if cid_len != cid.len() {
            return Ok(false);
        }
        let start = o + DELTAFLAT_ENTRY_CID_OFF;
        let end = start + cid_len;
        Ok(&page[start..end] == cid)
    }

    fn page_contains_entry(
        page: &[u8; DELTAFLAT_BUCKET_BYTES],
        cid: &[u8],
        fingerprint: u8,
        hash64: u64,
    ) -> Result<bool, StorageError> {
        for slot in 0..DELTAFLAT_ENTRIES_PER_BUCKET {
            if Self::entry_matches(page, slot, cid, fingerprint, hash64)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn find_free_slot(page: &[u8; DELTAFLAT_BUCKET_BYTES]) -> Option<usize> {
        let mut tombstone = None;
        for slot in 0..DELTAFLAT_ENTRIES_PER_BUCKET {
            match Self::entry_state(page, slot) {
                DELTAFLAT_STATE_EMPTY => return Some(tombstone.unwrap_or(slot)),
                DELTAFLAT_STATE_TOMBSTONE => {
                    if tombstone.is_none() {
                        tombstone = Some(slot);
                    }
                }
                _ => {}
            }
        }
        tombstone
    }

    fn ensure_index_buckets(
        index_file: &mut fs::File,
        bucket_count: u32,
    ) -> Result<(), StorageError> {
        let min_len = bucket_count as u64 * DELTAFLAT_BUCKET_BYTES as u64;
        let current = index_file.metadata()?.len();
        if current < min_len {
            index_file.set_len(min_len)?;
        }
        Ok(())
    }

    fn read_bucket(
        index_file: &mut fs::File,
        bucket_id: u32,
    ) -> Result<[u8; DELTAFLAT_BUCKET_BYTES], StorageError> {
        let mut page = [0u8; DELTAFLAT_BUCKET_BYTES];
        let off = bucket_id as u64 * DELTAFLAT_BUCKET_BYTES as u64;
        match Self::read_exact_at(index_file, &mut page, off) {
            Ok(()) => Ok(page),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Ok([0u8; DELTAFLAT_BUCKET_BYTES])
            }
            Err(e) => Err(StorageError::IoError(e)),
        }
    }

    fn write_bucket(
        index_file: &mut fs::File,
        bucket_id: u32,
        page: &[u8; DELTAFLAT_BUCKET_BYTES],
    ) -> Result<(), StorageError> {
        let off = bucket_id as u64 * DELTAFLAT_BUCKET_BYTES as u64;
        Self::write_all_at(index_file, page, off)?;
        Ok(())
    }

    #[cfg(unix)]
    fn read_exact_at(file: &fs::File, buf: &mut [u8], off: u64) -> std::io::Result<()> {
        let mut read_total = 0usize;
        while read_total < buf.len() {
            let n = file.read_at(&mut buf[read_total..], off + read_total as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short read_at",
                ));
            }
            read_total += n;
        }
        Ok(())
    }

    #[cfg(unix)]
    fn write_all_at(file: &fs::File, buf: &[u8], off: u64) -> std::io::Result<()> {
        let mut written = 0usize;
        while written < buf.len() {
            let n = file.write_at(&buf[written..], off + written as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "short write_at",
                ));
            }
            written += n;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn read_exact_at(file: &mut fs::File, buf: &mut [u8], off: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(off))?;
        file.read_exact(buf)
    }

    #[cfg(not(unix))]
    fn write_all_at(file: &mut fs::File, buf: &[u8], off: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(off))?;
        file.write_all(buf)
    }

    fn bucket_count(control: &DeltaFlatControl) -> Result<u32, StorageError> {
        let base = 1u32
            .checked_shl(control.level)
            .ok_or_else(|| StorageError::DatabaseError("deltaflat invalid level".to_string()))?;
        base.checked_add(control.split_ptr).ok_or_else(|| {
            StorageError::DatabaseError("deltaflat bucket count overflow".to_string())
        })
    }

    fn primary_bucket(even_hash: u64, control: &DeltaFlatControl) -> Result<u32, StorageError> {
        let base = 1u32
            .checked_shl(control.level)
            .ok_or_else(|| StorageError::DatabaseError("deltaflat invalid level".to_string()))?;
        if control.split_ptr >= base {
            return Err(StorageError::DatabaseError(format!(
                "deltaflat invalid split_ptr {} for level {}",
                control.split_ptr, control.level
            )));
        }
        let mut bucket = (even_hash % base as u64) as u32;
        if bucket < control.split_ptr {
            let expanded = base.checked_mul(2).ok_or_else(|| {
                StorageError::DatabaseError("deltaflat expanded base overflow".to_string())
            })?;
            bucket = (even_hash % expanded as u64) as u32;
        }
        Ok(bucket)
    }

    fn read_control_raw(
        control_file: &mut fs::File,
    ) -> Result<Option<DeltaFlatControl>, StorageError> {
        let mut buf = [0u8; DELTAFLAT_CONTROL_LEN];
        control_file.seek(SeekFrom::Start(0))?;
        match control_file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(StorageError::IoError(e)),
        }

        if buf[0..4] != DELTAFLAT_CONTROL_MAGIC {
            return Ok(None);
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().map_err(|_| {
            StorageError::DatabaseError("deltaflat bad control version".to_string())
        })?);
        if version != DELTAFLAT_VERSION as u32 {
            return Ok(None);
        }

        let level = u32::from_le_bytes(
            buf[8..12]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("deltaflat bad level".to_string()))?,
        );
        let split_ptr = u32::from_le_bytes(
            buf[12..16]
                .try_into()
                .map_err(|_| StorageError::DatabaseError("deltaflat bad split_ptr".to_string()))?,
        );
        let item_count =
            u64::from_le_bytes(buf[16..24].try_into().map_err(|_| {
                StorageError::DatabaseError("deltaflat bad item_count".to_string())
            })?);
        let next_file_id =
            u32::from_le_bytes(buf[24..28].try_into().map_err(|_| {
                StorageError::DatabaseError("deltaflat bad next_file_id".to_string())
            })?);
        let next_offset =
            u64::from_le_bytes(buf[28..36].try_into().map_err(|_| {
                StorageError::DatabaseError("deltaflat bad next_offset".to_string())
            })?);

        let base = 1u32.checked_shl(level).ok_or_else(|| {
            StorageError::DatabaseError("deltaflat control level overflow".to_string())
        })?;
        if split_ptr >= base {
            return Ok(None);
        }

        Ok(Some(DeltaFlatControl {
            level,
            split_ptr,
            item_count,
            next_file_id,
            next_offset,
        }))
    }

    fn write_control_raw(
        control_file: &mut fs::File,
        control: &DeltaFlatControl,
    ) -> Result<(), StorageError> {
        let mut buf = [0u8; DELTAFLAT_CONTROL_LEN];
        buf[0..4].copy_from_slice(&DELTAFLAT_CONTROL_MAGIC);
        buf[4..8].copy_from_slice(&(DELTAFLAT_VERSION as u32).to_le_bytes());
        buf[8..12].copy_from_slice(&control.level.to_le_bytes());
        buf[12..16].copy_from_slice(&control.split_ptr.to_le_bytes());
        buf[16..24].copy_from_slice(&control.item_count.to_le_bytes());
        buf[24..28].copy_from_slice(&control.next_file_id.to_le_bytes());
        buf[28..36].copy_from_slice(&control.next_offset.to_le_bytes());
        control_file.seek(SeekFrom::Start(0))?;
        control_file.write_all(&buf)?;
        Ok(())
    }

    fn load_or_init_control(
        &self,
        control_file: &mut fs::File,
        index_file: &mut fs::File,
    ) -> Result<DeltaFlatControl, StorageError> {
        if let Some(control) = Self::read_control_raw(control_file)? {
            Self::ensure_index_buckets(index_file, Self::bucket_count(&control)?)?;
            return Ok(control);
        }

        let level =
            BlockStore::env_usize("NEVERUST_DELTAFLAT_INITIAL_LEVEL", 12).clamp(1, 24) as u32;
        let control = DeltaFlatControl {
            level,
            split_ptr: 0,
            item_count: 0,
            next_file_id: 0,
            next_offset: 0,
        };
        Self::ensure_index_buckets(index_file, Self::bucket_count(&control)?)?;
        Self::write_control_raw(control_file, &control)?;
        Ok(control)
    }

    fn should_split(control: &DeltaFlatControl) -> Result<bool, StorageError> {
        let bucket_count = Self::bucket_count(control)? as u64;
        let capacity = bucket_count.saturating_mul(DELTAFLAT_ENTRIES_PER_BUCKET as u64);
        if capacity == 0 {
            return Ok(true);
        }
        Ok(control.item_count.saturating_mul(100)
            >= capacity.saturating_mul(DELTAFLAT_LOAD_FACTOR_PCT))
    }

    fn split_one_bucket(
        &self,
        index_file: &mut fs::File,
        control: &mut DeltaFlatControl,
    ) -> Result<(), StorageError> {
        let base = 1u32.checked_shl(control.level).ok_or_else(|| {
            StorageError::DatabaseError("deltaflat split base overflow".to_string())
        })?;
        if control.split_ptr >= base {
            return Err(StorageError::DatabaseError(format!(
                "deltaflat split pointer out of range: {} >= {}",
                control.split_ptr, base
            )));
        }

        let split_bucket = control.split_ptr;
        let new_bucket = base.checked_add(split_bucket).ok_or_else(|| {
            StorageError::DatabaseError("deltaflat new bucket overflow".to_string())
        })?;

        Self::ensure_index_buckets(
            index_file,
            new_bucket.checked_add(1).ok_or_else(|| {
                StorageError::DatabaseError("deltaflat bucket count overflow".to_string())
            })?,
        )?;

        let mut old_page = Self::read_bucket(index_file, split_bucket)?;
        let mut new_page = Self::read_bucket(index_file, new_bucket)?;

        for slot in 0..DELTAFLAT_ENTRIES_PER_BUCKET {
            let Some(entry) = Self::decode_entry(&old_page, slot)? else {
                continue;
            };
            let (even_hash, _, _, _) = Self::split_hash(&entry.cid);
            let expanded_mod = (base as u64).checked_mul(2).ok_or_else(|| {
                StorageError::DatabaseError("deltaflat expanded mod overflow".to_string())
            })?;
            let target = (even_hash % expanded_mod) as u32;
            if target == new_bucket {
                let Some(dst) = Self::find_free_slot(&new_page) else {
                    return Err(StorageError::DatabaseError(
                        "deltaflat new split bucket is full".to_string(),
                    ));
                };
                Self::write_entry_to_page(&mut new_page, dst, &entry)?;
                Self::mark_tombstone(&mut old_page, slot);
            }
        }

        Self::write_bucket(index_file, split_bucket, &old_page)?;
        Self::write_bucket(index_file, new_bucket, &new_page)?;

        control.split_ptr = control.split_ptr.checked_add(1).ok_or_else(|| {
            StorageError::DatabaseError("deltaflat split pointer overflow".to_string())
        })?;
        if control.split_ptr >= base {
            control.level = control.level.checked_add(1).ok_or_else(|| {
                StorageError::DatabaseError("deltaflat level overflow".to_string())
            })?;
            control.split_ptr = 0;
        }
        Ok(())
    }

    fn append_data(
        &self,
        class_id: u8,
        lane_id: u16,
        control: &mut DeltaFlatControl,
        data: &[u8],
        data_files: &mut HashMap<u32, fs::File>,
        data_positions: &mut HashMap<u32, u64>,
        touched_data: &mut HashSet<u32>,
    ) -> Result<(u32, u64, u32), StorageError> {
        let len = u32::try_from(data.len()).map_err(|_| {
            StorageError::DatabaseError("deltaflat data length exceeds u32".to_string())
        })?;

        while control
            .next_offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| {
                StorageError::DatabaseError("deltaflat next_offset overflow".to_string())
            })?
            > self.max_file_bytes
        {
            control.next_file_id = control.next_file_id.checked_add(1).ok_or_else(|| {
                StorageError::DatabaseError("deltaflat file_id overflow".to_string())
            })?;
            control.next_offset = 0;
        }

        let file_id = control.next_file_id;
        let offset = control.next_offset;
        if !data_files.contains_key(&file_id) {
            let path = self.blockfile_path(class_id, lane_id, file_id);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            data_positions.insert(file_id, offset);
            data_files.insert(file_id, file);
        }

        let file = data_files.get_mut(&file_id).ok_or_else(|| {
            StorageError::DatabaseError("deltaflat data file cache miss".to_string())
        })?;
        let pos = data_positions.entry(file_id).or_insert(offset);
        if *pos != offset {
            *pos = offset;
        }
        Self::write_all_at(file, data, *pos)?;
        *pos = pos.saturating_add(data.len() as u64);
        touched_data.insert(file_id);

        control.next_offset = control
            .next_offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| {
                StorageError::DatabaseError("deltaflat next_offset overflow".to_string())
            })?;

        Ok((file_id, offset, len))
    }

    fn insert_block_in_class(
        &self,
        class_id: u8,
        lane_id: u16,
        block: Block,
        cid_bytes: Vec<u8>,
        skip_exists_check: bool,
        control: &mut DeltaFlatControl,
        index_file: &mut fs::File,
        data_files: &mut HashMap<u32, fs::File>,
        data_positions: &mut HashMap<u32, u64>,
        touched_data: &mut HashSet<u32>,
        touched_index: &mut bool,
    ) -> Result<(), StorageError> {
        if cid_bytes.is_empty() || cid_bytes.len() > DELTAFLAT_MAX_CID_BYTES {
            return Err(StorageError::DatabaseError(format!(
                "deltaflat CID length {} out of bounds for {}",
                cid_bytes.len(),
                block.cid
            )));
        }

        let (even_hash, _odd_hash, fingerprint, hash64) = Self::split_hash(&cid_bytes);
        let mut attempts = 0u32;
        loop {
            attempts = attempts.saturating_add(1);
            if attempts > self.max_probe_steps {
                return Err(StorageError::DatabaseError(format!(
                    "deltaflat insertion exhausted splits for CID {}",
                    block.cid
                )));
            }

            let bucket = Self::primary_bucket(even_hash, control)?;
            let mut page = Self::read_bucket(index_file, bucket)?;
            if !skip_exists_check
                && Self::page_contains_entry(&page, &cid_bytes, fingerprint, hash64)?
            {
                return Ok(());
            }

            if let Some(slot) = Self::find_free_slot(&page) {
                let (file_id, offset, len) = self.append_data(
                    class_id,
                    lane_id,
                    control,
                    &block.data,
                    data_files,
                    data_positions,
                    touched_data,
                )?;
                let entry = DeltaFlatEntry {
                    fingerprint,
                    file_id,
                    offset,
                    len,
                    hash64,
                    cid: cid_bytes.clone(),
                };
                Self::write_entry_to_page(&mut page, slot, &entry)?;
                Self::write_bucket(index_file, bucket, &page)?;
                *touched_index = true;
                control.item_count = control.item_count.saturating_add(1);
                if Self::should_split(control)? {
                    self.split_one_bucket(index_file, control)?;
                    *touched_index = true;
                }
                return Ok(());
            }

            self.split_one_bucket(index_file, control)?;
            *touched_index = true;
        }
    }

    fn lookup_in_class(
        &self,
        index_file: &mut fs::File,
        control: &DeltaFlatControl,
        cid_bytes: &[u8],
    ) -> Result<Option<(u32, usize, DeltaFlatEntry)>, StorageError> {
        let (even_hash, _odd_hash, fingerprint, hash64) = Self::split_hash(cid_bytes);
        let bucket = Self::primary_bucket(even_hash, control)?;
        let page = Self::read_bucket(index_file, bucket)?;
        Ok(
            Self::find_entry_slot(&page, cid_bytes, fingerprint, hash64)?
                .map(|(slot, entry)| (bucket, slot, entry)),
        )
    }

    async fn put_many(&self, blocks: Vec<Block>) -> Result<(), StorageError> {
        if blocks.is_empty() {
            return Ok(());
        }

        if BlockStore::verify_blocks_on_write() {
            for block in &blocks {
                if block.cid.codec() == 0xcd02 {
                    verify_blake3(&block.data, &block.cid)?;
                }
            }
        }

        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let skip_exists_check =
                BlockStore::env_flag("NEVERUST_DELTAFLAT_SKIP_EXISTS_CHECK", false);
            let lane_count = store.lane_count as usize;
            let class_count = DELTA_SIZE_CLASSES.len();
            let bin_count = class_count
                .checked_mul(lane_count)
                .ok_or_else(|| StorageError::DatabaseError("deltaflat bin count overflow".to_string()))?;
            let mut bins: Vec<Vec<(Block, Vec<u8>)>> = (0..bin_count).map(|_| Vec::new()).collect();
            let mut active_bins = Vec::new();
            for block in blocks {
                let cid_bytes = block.cid.to_bytes();
                let (class_id, _) = Self::class_for_len(block.data.len());
                let lane_id = store.lane_for_cid(&cid_bytes);
                let bin_idx = class_id as usize * lane_count + lane_id as usize;
                if bins[bin_idx].is_empty() {
                    active_bins.push(bin_idx);
                }
                bins[bin_idx].push((block, cid_bytes));
            }

            for bin_idx in active_bins {
                let class_id = (bin_idx / lane_count) as u8;
                let lane_id = (bin_idx % lane_count) as u16;
                let class_blocks = std::mem::take(&mut bins[bin_idx]);
                let _lane_guard = store.lane_write_guard(class_id, lane_id)?;
                let (mut control_file, mut index_file) =
                    store.open_class_files(class_id, lane_id)?;
                store.lock_control(&control_file, true)?;
                let mut op_res = (|| -> Result<(), StorageError> {
                    let mut control =
                        store.load_or_init_control(&mut control_file, &mut index_file)?;
                    let mut data_files: HashMap<u32, fs::File> = HashMap::new();
                    let mut data_positions: HashMap<u32, u64> = HashMap::new();
                    let mut touched_data: HashSet<u32> = HashSet::new();
                    let mut touched_index = false;

                    for (block, cid_bytes) in class_blocks {
                        store.insert_block_in_class(
                            class_id,
                            lane_id,
                            block,
                            cid_bytes,
                            skip_exists_check,
                            &mut control,
                            &mut index_file,
                            &mut data_files,
                            &mut data_positions,
                            &mut touched_data,
                            &mut touched_index,
                        )?;
                    }

                    Self::write_control_raw(&mut control_file, &control)?;
                    if store.fsync_writes {
                        for file_id in touched_data {
                            if let Some(file) = data_files.get_mut(&file_id) {
                                file.sync_data()?;
                            }
                        }
                        if touched_index {
                            index_file.sync_data()?;
                        }
                        control_file.sync_data()?;
                    }
                    Ok(())
                })();

                let unlock_res = store.unlock_control(&control_file);
                if let Err(unlock_err) = unlock_res {
                    if op_res.is_ok() {
                        op_res = Err(unlock_err);
                    }
                }
                op_res?;
            }

            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn get(&self, cid: &Cid) -> Result<Block, StorageError> {
        let cid_copy = *cid;
        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Block, StorageError> {
            let cid_key = cid_copy.to_string();
            let cid_bytes = cid_copy.to_bytes();
            let lane_id = store.lane_for_cid(&cid_bytes);

            for class_id in 0..DELTA_SIZE_CLASSES.len() as u8 {
                let lane_dir = store.lane_dir(class_id, lane_id);
                if !lane_dir.exists() {
                    continue;
                }
                let control_path = store.class_control_path(class_id, lane_id);
                let index_path = store.class_index_path(class_id, lane_id);
                if !control_path.exists() || !index_path.exists() {
                    continue;
                }

                let _lane_guard = store.lane_read_guard(class_id, lane_id)?;
                let mut control_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&control_path)?;
                let mut index_file = fs::OpenOptions::new().read(true).open(&index_path)?;

                store.lock_control(&control_file, false)?;
                let lookup_res = (|| -> Result<Option<DeltaFlatEntry>, StorageError> {
                    let Some(control) = Self::read_control_raw(&mut control_file)? else {
                        return Ok(None);
                    };
                    Ok(store
                        .lookup_in_class(&mut index_file, &control, &cid_bytes)?
                        .map(|(_, _, entry)| entry))
                })();
                let unlock_res = store.unlock_control(&control_file);
                if let Err(err) = unlock_res {
                    return Err(err);
                }

                if let Some(entry) = lookup_res? {
                    let path = store.blockfile_path(class_id, lane_id, entry.file_id);
                    let mut file = fs::OpenOptions::new().read(true).open(&path).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            StorageError::BlockNotFound(cid_key.clone())
                        } else {
                            StorageError::IoError(e)
                        }
                    })?;
                    let mut data = vec![0u8; entry.len as usize];
                    Self::read_exact_at(&mut file, &mut data, entry.offset)?;
                    return Ok(Block {
                        cid: cid_copy,
                        data,
                    });
                }
            }
            Err(StorageError::BlockNotFound(cid_key))
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn has(&self, cid: &Cid) -> bool {
        let cid_copy = *cid;
        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<bool, StorageError> {
            let cid_bytes = cid_copy.to_bytes();
            let lane_id = store.lane_for_cid(&cid_bytes);

            for class_id in 0..DELTA_SIZE_CLASSES.len() as u8 {
                let lane_dir = store.lane_dir(class_id, lane_id);
                if !lane_dir.exists() {
                    continue;
                }
                let control_path = store.class_control_path(class_id, lane_id);
                let index_path = store.class_index_path(class_id, lane_id);
                if !control_path.exists() || !index_path.exists() {
                    continue;
                }

                let _lane_guard = store.lane_read_guard(class_id, lane_id)?;
                let mut control_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&control_path)?;
                let mut index_file = fs::OpenOptions::new().read(true).open(&index_path)?;
                store.lock_control(&control_file, false)?;
                let lookup_res = (|| -> Result<bool, StorageError> {
                    let Some(control) = Self::read_control_raw(&mut control_file)? else {
                        return Ok(false);
                    };
                    Ok(store
                        .lookup_in_class(&mut index_file, &control, &cid_bytes)?
                        .is_some())
                })();
                let unlock_res = store.unlock_control(&control_file);
                if let Err(err) = unlock_res {
                    return Err(err);
                }
                if lookup_res? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(false)
    }

    async fn delete(&self, cid: &Cid) -> Result<(), StorageError> {
        let cid_copy = *cid;
        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let cid_key = cid_copy.to_string();
            let cid_bytes = cid_copy.to_bytes();
            let lane_id = store.lane_for_cid(&cid_bytes);

            for class_id in 0..DELTA_SIZE_CLASSES.len() as u8 {
                let lane_dir = store.lane_dir(class_id, lane_id);
                if !lane_dir.exists() {
                    continue;
                }
                let control_path = store.class_control_path(class_id, lane_id);
                let index_path = store.class_index_path(class_id, lane_id);
                if !control_path.exists() || !index_path.exists() {
                    continue;
                }

                let _lane_guard = store.lane_write_guard(class_id, lane_id)?;
                let mut control_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&control_path)?;
                let mut index_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&index_path)?;

                store.lock_control(&control_file, true)?;
                let mut op_res = (|| -> Result<bool, StorageError> {
                    let Some(mut control) = Self::read_control_raw(&mut control_file)? else {
                        return Ok(false);
                    };
                    let Some((bucket, slot, _entry)) =
                        store.lookup_in_class(&mut index_file, &control, &cid_bytes)?
                    else {
                        return Ok(false);
                    };
                    let mut page = Self::read_bucket(&mut index_file, bucket)?;
                    Self::mark_tombstone(&mut page, slot);
                    Self::write_bucket(&mut index_file, bucket, &page)?;
                    if control.item_count > 0 {
                        control.item_count -= 1;
                    }
                    Self::write_control_raw(&mut control_file, &control)?;
                    if store.fsync_writes {
                        index_file.sync_data()?;
                        control_file.sync_data()?;
                    }
                    Ok(true)
                })();
                let unlock_res = store.unlock_control(&control_file);
                if let Err(err) = unlock_res {
                    if op_res.is_ok() {
                        op_res = Err(err);
                    }
                }
                if op_res? {
                    return Ok(());
                }
            }

            Err(StorageError::BlockNotFound(cid_key))
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn list_cids(&self) -> Vec<Cid> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<Cid>, StorageError> {
            let mut out = Vec::new();
            for class_id in 0..DELTA_SIZE_CLASSES.len() as u8 {
                for lane_id in 0..store.lane_count {
                    let lane_dir = store.lane_dir(class_id, lane_id);
                    if !lane_dir.exists() {
                        continue;
                    }
                    let control_path = store.class_control_path(class_id, lane_id);
                    let index_path = store.class_index_path(class_id, lane_id);
                    if !control_path.exists() || !index_path.exists() {
                        continue;
                    }

                    let _lane_guard = store.lane_read_guard(class_id, lane_id)?;
                    let mut control_file = fs::OpenOptions::new().read(true).open(&control_path)?;
                    let mut index_file = fs::OpenOptions::new().read(true).open(&index_path)?;
                    let Some(control) = Self::read_control_raw(&mut control_file)? else {
                        continue;
                    };
                    let buckets = Self::bucket_count(&control)?;
                    for bucket in 0..buckets {
                        let page = Self::read_bucket(&mut index_file, bucket)?;
                        for slot in 0..DELTAFLAT_ENTRIES_PER_BUCKET {
                            let Some(entry) = Self::decode_entry(&page, slot)? else {
                                continue;
                            };
                            if let Ok(cid) = Cid::try_from(entry.cid.as_slice()) {
                                out.push(cid);
                            }
                        }
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r)
        .unwrap_or_else(|e| {
            warn!("Failed to list CIDs from deltaflat {:?}: {}", self.root, e);
            Vec::new()
        })
    }

    async fn stats(&self) -> BlockStoreStats {
        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<BlockStoreStats, StorageError> {
            let mut block_count = 0usize;
            let mut total_size = 0usize;
            for class_id in 0..DELTA_SIZE_CLASSES.len() as u8 {
                for lane_id in 0..store.lane_count {
                    let lane_dir = store.lane_dir(class_id, lane_id);
                    if !lane_dir.exists() {
                        continue;
                    }
                    let control_path = store.class_control_path(class_id, lane_id);
                    let index_path = store.class_index_path(class_id, lane_id);
                    if !control_path.exists() || !index_path.exists() {
                        continue;
                    }

                    let _lane_guard = store.lane_read_guard(class_id, lane_id)?;
                    let mut control_file = fs::OpenOptions::new().read(true).open(&control_path)?;
                    let mut index_file = fs::OpenOptions::new().read(true).open(&index_path)?;
                    let Some(control) = Self::read_control_raw(&mut control_file)? else {
                        continue;
                    };
                    let buckets = Self::bucket_count(&control)?;
                    for bucket in 0..buckets {
                        let page = Self::read_bucket(&mut index_file, bucket)?;
                        for slot in 0..DELTAFLAT_ENTRIES_PER_BUCKET {
                            let Some(entry) = Self::decode_entry(&page, slot)? else {
                                continue;
                            };
                            block_count += 1;
                            total_size += entry.len as usize;
                        }
                    }
                }
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
            warn!(
                "Failed to compute deltaflat stats from {:?}: {}",
                self.root, e
            );
            BlockStoreStats {
                block_count: 0,
                total_size: 0,
            }
        })
    }

    async fn clear(&self) {
        let root = self.root.clone();
        let blocks_root = self.blocks_root.clone();
        let res = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            if blocks_root.exists() {
                fs::remove_dir_all(&blocks_root)?;
            }
            fs::create_dir_all(&blocks_root)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r);

        match res {
            Ok(()) => info!("Cleared all blocks from deltaflat store at {:?}", root),
            Err(e) => warn!("Failed to clear deltaflat store at {:?}: {}", root, e),
        }
    }
}

impl GeomTreeStore {
    const FILE_EXT: &'static str = ".data";

    fn to_hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }

    fn cid_entropy32(cid: &Cid) -> [u8; 32] {
        let digest = cid.hash().digest();
        let mut out = [0u8; 32];
        if digest.len() >= 32 {
            out.copy_from_slice(&digest[..32]);
        } else {
            out.copy_from_slice(blake3::hash(&cid.to_bytes()).as_bytes());
        }
        out
    }

    /// Geometry split inspired by projection-fiber decomposition:
    /// - basepoint: even-byte rail
    /// - fiber/kernel: odd-byte rail
    ///
    /// Together these preserve the full 32-byte entropy.
    fn split_geometry(cid: &Cid) -> ([u8; 16], [u8; 16]) {
        let h = Self::cid_entropy32(cid);

        let mut base = [0u8; 16];
        let mut fiber = [0u8; 16];
        for i in 0..16 {
            base[i] = h[i * 2];
            fiber[i] = h[i * 2 + 1];
        }
        (base, fiber)
    }

    fn block_path(&self, cid: &Cid) -> PathBuf {
        let (base, _fiber) = Self::split_geometry(cid);
        let mut p = self.root.clone();

        // FlatFS-style: shard directory from geometric base rail, full CID in filename.
        // Keeping full CID in filename preserves injective identity independent of sharder.
        for level in 0..self.shard_levels {
            let start = level * self.bytes_per_level;
            if start >= base.len() {
                break;
            }
            let end = (start + self.bytes_per_level).min(base.len());
            p.push(Self::to_hex(&base[start..end]));
        }

        let file_name = format!("{}{}", cid, Self::FILE_EXT);
        p.push(file_name);
        p
    }

    fn walk_block_files(root: &Path) -> Result<Vec<PathBuf>, StorageError> {
        if !root.exists() {
            return Ok(Vec::new());
        }

        let mut stack = vec![root.to_path_buf()];
        let mut files = Vec::new();

        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                let ft = entry.file_type()?;
                if ft.is_dir() {
                    stack.push(path);
                } else if ft.is_file() && path.to_string_lossy().ends_with(Self::FILE_EXT) {
                    files.push(path);
                }
            }
        }

        Ok(files)
    }

    async fn put_many(&self, blocks: Vec<Block>) -> Result<(), StorageError> {
        if blocks.is_empty() {
            return Ok(());
        }

        if BlockStore::verify_blocks_on_write() {
            for block in &blocks {
                if block.cid.codec() == 0xcd02 {
                    verify_blake3(&block.data, &block.cid)?;
                }
            }
        }

        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            for block in blocks {
                let path = store.block_path(&block.cid);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }

                let open_res = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path);

                let mut file = match open_res {
                    Ok(file) => file,
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        continue;
                    }
                    Err(e) => return Err(StorageError::IoError(e)),
                };

                file.write_all(&block.data)?;
                if store.fsync_writes {
                    file.sync_data()?;
                }
            }

            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn get(&self, cid: &Cid) -> Result<Block, StorageError> {
        let path = self.block_path(cid);
        let cid_copy = *cid;
        tokio::task::spawn_blocking(move || {
            let data = fs::read(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::BlockNotFound(cid_copy.to_string())
                } else {
                    StorageError::IoError(e)
                }
            })?;

            Ok(Block {
                cid: cid_copy,
                data,
            })
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn has(&self, cid: &Cid) -> bool {
        let path = self.block_path(cid);
        tokio::task::spawn_blocking(move || path.exists())
            .await
            .unwrap_or(false)
    }

    async fn delete(&self, cid: &Cid) -> Result<(), StorageError> {
        let path = self.block_path(cid);
        let cid_str = cid.to_string();
        tokio::task::spawn_blocking(move || {
            fs::remove_file(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::BlockNotFound(cid_str)
                } else {
                    StorageError::IoError(e)
                }
            })
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn list_cids(&self) -> Vec<Cid> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<Cid>, StorageError> {
            let files = Self::walk_block_files(&root)?;
            let mut cids = Vec::new();

            for path in files {
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                let Some(cid_str) = name.strip_suffix(Self::FILE_EXT) else {
                    continue;
                };
                if let Ok(cid) = cid_str.parse::<Cid>() {
                    cids.push(cid);
                }
            }

            Ok(cids)
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r)
        .unwrap_or_else(|e| {
            warn!("Failed to list CIDs from geomtree {:?}: {}", self.root, e);
            Vec::new()
        })
    }

    async fn stats(&self) -> BlockStoreStats {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || -> Result<BlockStoreStats, StorageError> {
            let files = Self::walk_block_files(&root)?;
            let mut block_count = 0usize;
            let mut total_size = 0usize;
            for path in files {
                let meta = fs::metadata(path)?;
                block_count += 1;
                total_size += meta.len() as usize;
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
            warn!(
                "Failed to compute geomtree stats from {:?}: {}",
                self.root, e
            );
            BlockStoreStats {
                block_count: 0,
                total_size: 0,
            }
        })
    }

    async fn clear(&self) {
        let root = self.root.clone();
        let res = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            if root.exists() {
                fs::remove_dir_all(&root)?;
            }
            fs::create_dir_all(&root)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
        .and_then(|r| r);

        match res {
            Ok(()) => info!("Cleared all blocks from geomtree store"),
            Err(e) => warn!("Failed to clear geomtree store at {:?}: {}", self.root, e),
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
    use std::path::Path;
    use std::sync::Arc;

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

    #[tokio::test]
    async fn test_geomtree_backend_put_get() {
        let temp_dir =
            std::env::temp_dir().join(format!("neverust-geomtree-test-{}", rand::random::<u64>()));
        let store = BlockStore::new_with_backend(Path::new(&temp_dir), "geomtree").unwrap();

        let data = b"geomtree block".to_vec();
        let block = Block::new(data.clone()).unwrap();
        let cid = block.cid;

        store.put(block.clone()).await.unwrap();
        store.put(block).await.unwrap(); // idempotent

        let out = store.get(&cid).await.unwrap();
        assert_eq!(out.data, data);
        assert!(store.has(&cid).await);

        let listed = store.list_cids().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], cid);

        store.delete(&cid).await.unwrap();
        assert!(!store.has(&cid).await);
    }

    #[tokio::test]
    async fn test_deltastore_backend_put_get() {
        let temp_dir = std::env::temp_dir().join(format!(
            "neverust-deltastore-test-{}",
            rand::random::<u64>()
        ));
        let store = BlockStore::new_with_backend(Path::new(&temp_dir), "deltastore").unwrap();

        let data = vec![0xAB; 1024 * 768];
        let block = Block::new(data.clone()).unwrap();
        let cid = block.cid;

        store.put(block.clone()).await.unwrap();
        store.put(block).await.unwrap(); // idempotent

        let out = store.get(&cid).await.unwrap();
        assert_eq!(out.data, data);
        assert!(store.has(&cid).await);

        let listed = store.list_cids().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], cid);

        let stats = store.stats().await;
        assert_eq!(stats.block_count, 1);
        assert_eq!(stats.total_size, data.len());

        store.delete(&cid).await.unwrap();
        assert!(!store.has(&cid).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_deltaflat_concurrent_put_many_integrity() {
        let temp_dir = std::env::temp_dir().join(format!(
            "neverust-deltaflat-concurrent-test-{}",
            rand::random::<u64>()
        ));
        let store =
            Arc::new(BlockStore::new_with_backend(Path::new(&temp_dir), "deltaflat").unwrap());

        let workers = 8usize;
        let blocks_per_worker = 256usize;
        let block_size = 64 * 1024usize;

        let mut joins = Vec::new();
        for worker in 0..workers {
            let store = Arc::clone(&store);
            joins.push(tokio::spawn(async move {
                let mut batch = Vec::with_capacity(blocks_per_worker);
                let mut samples = Vec::new();

                for idx in 0..blocks_per_worker {
                    let seq = (worker * blocks_per_worker + idx) as u64;
                    let mut data = vec![0u8; block_size];
                    let mut x = seq
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                        .wrapping_add(0xA5A5_A5A5_A5A5_A5A5);
                    for byte in &mut data {
                        x ^= x >> 12;
                        x ^= x << 25;
                        x ^= x >> 27;
                        x = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
                        *byte = (x & 0xff) as u8;
                    }

                    let block = Block::new(data.clone()).expect("block");
                    if idx % 64 == 0 {
                        samples.push((block.cid, data));
                    }
                    batch.push(block);
                }

                store.put_many(batch).await.expect("deltaflat put_many");
                samples
            }));
        }

        let mut sample_pairs = Vec::new();
        for join in joins {
            let mut samples = join.await.expect("join");
            sample_pairs.append(&mut samples);
        }

        let stats = store.stats().await;
        assert_eq!(stats.block_count, workers * blocks_per_worker);
        for (cid, expected) in sample_pairs {
            let got = store.get(&cid).await.expect("get");
            assert_eq!(got.data, expected);
        }
    }

    #[test]
    fn test_split_geometry_is_lossless_over_32_byte_entropy() {
        let data = b"geometry entropy lane".to_vec();
        let cid = Block::new(data).unwrap().cid;

        let entropy = GeomTreeStore::cid_entropy32(&cid);
        let (base, fiber) = GeomTreeStore::split_geometry(&cid);

        let mut merged = [0u8; 32];
        for i in 0..16 {
            merged[i * 2] = base[i];
            merged[i * 2 + 1] = fiber[i];
        }
        assert_eq!(merged, entropy);
    }
}
