//! Composable in-memory primitive pipeline benchmarks.
//!
//! This module is designed for fast iteration on primitive composition:
//! transform -> digest -> index -> in-memory store, with optional multinode
//! replication simulation.

use crate::storage::{Block, BlockStore, StorageError};
use cid::Cid;
use sha2::{Digest as ShaDigest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub enum Stage {
    Identity,
    XorScramble,
    Blake3Digest,
    Sha256Digest,
    IndexModulo { buckets: u64 },
    IndexXorFold { buckets: u64 },
}

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub stages: Vec<Stage>,
}

impl Pipeline {
    pub fn new(stages: Vec<Stage>) -> Self {
        Self { stages }
    }

    pub fn from_spec(spec: &str) -> Self {
        let mut stages = Vec::new();
        for token in spec
            .split(',')
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
        {
            let stage = match token {
                "id" | "identity" => Stage::Identity,
                "xor" | "xor_scramble" => Stage::XorScramble,
                "blake3" => Stage::Blake3Digest,
                "sha256" => Stage::Sha256Digest,
                "index_mod" | "index_mod:1048576" => Stage::IndexModulo { buckets: 1 << 20 },
                "index_xorfold" | "index_xorfold:1048576" => {
                    Stage::IndexXorFold { buckets: 1 << 20 }
                }
                _ if token.starts_with("index_mod:") => {
                    let buckets = token
                        .split_once(':')
                        .and_then(|(_, v)| v.parse::<u64>().ok())
                        .unwrap_or(1 << 20)
                        .max(1);
                    Stage::IndexModulo { buckets }
                }
                _ if token.starts_with("index_xorfold:") => {
                    let buckets = token
                        .split_once(':')
                        .and_then(|(_, v)| v.parse::<u64>().ok())
                        .unwrap_or(1 << 20)
                        .max(1);
                    Stage::IndexXorFold { buckets }
                }
                _ => Stage::Identity,
            };
            stages.push(stage);
        }

        if stages.is_empty() {
            stages.push(Stage::Blake3Digest);
            stages.push(Stage::IndexModulo { buckets: 1 << 20 });
        }

        Self { stages }
    }

    pub fn run(&self, mut state: PipelineState) -> PipelineState {
        for stage in &self.stages {
            stage.apply(&mut state);
        }
        if !state.has_digest {
            let digest = blake3::hash(&state.payload);
            state.digest.copy_from_slice(digest.as_bytes());
            state.has_digest = true;
        }
        state
    }
}

#[derive(Debug, Clone)]
pub struct PipelineState {
    pub seq: u64,
    pub payload: Vec<u8>,
    pub digest: [u8; 32],
    pub has_digest: bool,
    pub bucket: u64,
}

impl PipelineState {
    pub fn new(seq: u64, payload: Vec<u8>) -> Self {
        Self {
            seq,
            payload,
            digest: [0u8; 32],
            has_digest: false,
            bucket: 0,
        }
    }
}

impl Stage {
    fn apply(&self, state: &mut PipelineState) {
        match self {
            Stage::Identity => {}
            Stage::XorScramble => {
                let mut x = state
                    .seq
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(0xD1B5_4A32_D192_ED03);
                for byte in &mut state.payload {
                    x ^= x >> 12;
                    x ^= x << 25;
                    x ^= x >> 27;
                    x = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
                    *byte ^= (x & 0xff) as u8;
                }
            }
            Stage::Blake3Digest => {
                let digest = blake3::hash(&state.payload);
                state.digest.copy_from_slice(digest.as_bytes());
                state.has_digest = true;
            }
            Stage::Sha256Digest => {
                let digest = Sha256::digest(&state.payload);
                state.digest.copy_from_slice(&digest);
                state.has_digest = true;
            }
            Stage::IndexModulo { buckets } => {
                ensure_digest(state);
                let mut low = [0u8; 8];
                low.copy_from_slice(&state.digest[0..8]);
                let hash = u64::from_le_bytes(low);
                state.bucket = if *buckets == 0 { 0 } else { hash % *buckets };
            }
            Stage::IndexXorFold { buckets } => {
                ensure_digest(state);
                let mut folded = 0u64;
                for chunk in state.digest.chunks_exact(8) {
                    let mut part = [0u8; 8];
                    part.copy_from_slice(chunk);
                    folded ^= u64::from_le_bytes(part);
                }
                state.bucket = if *buckets == 0 { 0 } else { folded % *buckets };
            }
        }
    }
}

fn ensure_digest(state: &mut PipelineState) {
    if state.has_digest {
        return;
    }
    let digest = blake3::hash(&state.payload);
    state.digest.copy_from_slice(digest.as_bytes());
    state.has_digest = true;
}

#[derive(Debug)]
pub struct ShardedMemoryStore {
    shards: Vec<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    items: AtomicUsize,
    bytes: AtomicUsize,
}

impl ShardedMemoryStore {
    pub fn new(shards: usize) -> Self {
        let shard_count = NonZeroUsize::new(shards).map_or(1, NonZeroUsize::get);
        let mut shard_vec = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shard_vec.push(Mutex::new(HashMap::new()));
        }
        Self {
            shards: shard_vec,
            items: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn shard_index(&self, key: &[u8; 32]) -> usize {
        let mut low = [0u8; 8];
        low.copy_from_slice(&key[0..8]);
        (u64::from_le_bytes(low) as usize) % self.shards.len()
    }

    pub fn put(&self, key: [u8; 32], value: Vec<u8>) {
        let idx = self.shard_index(&key);
        let mut guard = self.shards[idx]
            .lock()
            .expect("mutex poisoned while writing sharded memory store");
        let new_bytes = value.len();
        let old = guard.insert(key, value);
        if let Some(previous) = old {
            let previous_len = previous.len();
            if new_bytes > previous_len {
                self.bytes
                    .fetch_add(new_bytes - previous_len, Ordering::Relaxed);
            } else if previous_len > new_bytes {
                self.bytes
                    .fetch_sub(previous_len - new_bytes, Ordering::Relaxed);
            }
        } else {
            self.items.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(new_bytes, Ordering::Relaxed);
        }
    }

    pub fn get(&self, key: &[u8; 32]) -> Option<Vec<u8>> {
        let idx = self.shard_index(key);
        let guard = self.shards[idx]
            .lock()
            .expect("mutex poisoned while reading sharded memory store");
        guard.get(key).cloned()
    }

    pub fn item_count(&self) -> usize {
        self.items.load(Ordering::Relaxed)
    }

    pub fn total_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryBenchConfig {
    pub total_blocks: usize,
    pub block_size: usize,
    pub workers: usize,
    pub shards: usize,
    pub verify_stride: usize,
    pub pipeline: Pipeline,
}

impl Default for InMemoryBenchConfig {
    fn default() -> Self {
        Self {
            total_blocks: 200_000,
            block_size: 1 << 20,
            workers: 8,
            shards: 256,
            verify_stride: 64,
            pipeline: Pipeline::from_spec("xor,blake3,index_mod:1048576"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultiNodeBenchConfig {
    pub nodes: usize,
    pub replication: usize,
    pub total_blocks: usize,
    pub block_size: usize,
    pub workers_per_node: usize,
    pub shards_per_node: usize,
    pub verify_stride: usize,
    pub pipeline: Pipeline,
}

impl Default for MultiNodeBenchConfig {
    fn default() -> Self {
        Self {
            nodes: 4,
            replication: 2,
            total_blocks: 150_000,
            block_size: 512 << 10,
            workers_per_node: 4,
            shards_per_node: 128,
            verify_stride: 128,
            pipeline: Pipeline::from_spec("xor,blake3,index_mod:1048576"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RealStoreBenchConfig {
    pub backend: String,
    pub root_dir: PathBuf,
    pub total_blocks: usize,
    pub block_size: usize,
    pub workers: usize,
    pub batch_blocks: usize,
    pub verify_stride: usize,
    pub verify_samples_max: usize,
    pub pipeline: Pipeline,
    pub clear_existing: bool,
}

impl Default for RealStoreBenchConfig {
    fn default() -> Self {
        Self {
            backend: "deltaflat".to_string(),
            root_dir: std::env::temp_dir()
                .join(format!("neverust-primitive-real-{}", rand::random::<u64>())),
            total_blocks: 40_000,
            block_size: 1 << 20,
            workers: 8,
            batch_blocks: 256,
            verify_stride: 256,
            verify_samples_max: 1024,
            pipeline: Pipeline::from_spec("xor,blake3,index_mod:1048576"),
            clear_existing: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RealMultiNodeBenchConfig {
    pub backend: String,
    pub root_dir: PathBuf,
    pub nodes: usize,
    pub replication: usize,
    pub total_blocks: usize,
    pub block_size: usize,
    pub workers_per_node: usize,
    pub batch_blocks: usize,
    pub verify_stride: usize,
    pub verify_samples_max: usize,
    pub pipeline: Pipeline,
    pub clear_existing: bool,
}

impl Default for RealMultiNodeBenchConfig {
    fn default() -> Self {
        Self {
            backend: "deltaflat".to_string(),
            root_dir: std::env::temp_dir().join(format!(
                "neverust-primitive-real-multi-{}",
                rand::random::<u64>()
            )),
            nodes: 4,
            replication: 2,
            total_blocks: 32_000,
            block_size: 512 << 10,
            workers_per_node: 4,
            batch_blocks: 128,
            verify_stride: 256,
            verify_samples_max: 1024,
            pipeline: Pipeline::from_spec("xor,blake3,index_mod:1048576"),
            clear_existing: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BenchResult {
    pub mode: String,
    pub elapsed_sec: f64,
    pub blocks_processed: usize,
    pub logical_bytes: usize,
    pub physical_bytes: usize,
    pub throughput_mibps: f64,
    pub blocks_per_sec: f64,
    pub verification_failures: usize,
    pub stores: usize,
}

#[derive(Debug, Default)]
struct WorkerCounters {
    blocks: usize,
    logical_bytes: usize,
    physical_bytes: usize,
    verification_failures: usize,
}

#[derive(Debug, Clone)]
struct VerificationSample {
    node_idx: usize,
    cid: Cid,
    len: usize,
    digest: [u8; 32],
}

fn blake3_digest32(data: &[u8]) -> [u8; 32] {
    let digest = blake3::hash(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_bytes());
    out
}

fn generate_block(seq: u64, block_size: usize) -> Vec<u8> {
    let mut out = vec![0u8; block_size];
    let mut x = seq
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xA5A5_A5A5_A5A5_A5A5);
    for byte in &mut out {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        *byte = (x & 0xff) as u8;
    }
    out
}

pub fn bench_in_memory(config: InMemoryBenchConfig) -> BenchResult {
    let workers = config.workers.max(1);
    let verify_stride = config.verify_stride.max(1);
    let store = Arc::new(ShardedMemoryStore::new(config.shards.max(1)));
    let started = Instant::now();

    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let local_store = Arc::clone(&store);
        let pipeline = config.pipeline.clone();
        let total_blocks = config.total_blocks;
        let block_size = config.block_size;

        handles.push(thread::spawn(move || {
            let mut counters = WorkerCounters::default();
            for block_id in (worker_id..total_blocks).step_by(workers) {
                let payload = generate_block(block_id as u64, block_size);
                let state = PipelineState::new(block_id as u64, payload);
                let processed = pipeline.run(state);
                local_store.put(processed.digest, processed.payload.clone());

                if block_id % verify_stride == 0 {
                    if let Some(stored) = local_store.get(&processed.digest) {
                        if stored != processed.payload {
                            counters.verification_failures += 1;
                        }
                    } else {
                        counters.verification_failures += 1;
                    }
                }

                counters.blocks += 1;
                counters.logical_bytes += block_size;
                counters.physical_bytes += block_size;
            }
            counters
        }));
    }

    let mut totals = WorkerCounters::default();
    for handle in handles {
        if let Ok(stats) = handle.join() {
            totals.blocks += stats.blocks;
            totals.logical_bytes += stats.logical_bytes;
            totals.physical_bytes += stats.physical_bytes;
            totals.verification_failures += stats.verification_failures;
        } else {
            totals.verification_failures += 1;
        }
    }

    let elapsed = started.elapsed().as_secs_f64().max(1e-9);
    BenchResult {
        mode: "inmem".to_string(),
        elapsed_sec: elapsed,
        blocks_processed: totals.blocks,
        logical_bytes: totals.logical_bytes,
        physical_bytes: totals.physical_bytes,
        throughput_mibps: (totals.logical_bytes as f64 / 1_048_576.0) / elapsed,
        blocks_per_sec: totals.blocks as f64 / elapsed,
        verification_failures: totals.verification_failures,
        stores: store.item_count(),
    }
}

pub fn bench_multinode(config: MultiNodeBenchConfig) -> BenchResult {
    let nodes = config.nodes.max(1);
    let workers = (config.workers_per_node.max(1)) * nodes;
    let replication = config.replication.clamp(1, nodes);
    let verify_stride = config.verify_stride.max(1);

    let stores: Vec<Arc<ShardedMemoryStore>> = (0..nodes)
        .map(|_| Arc::new(ShardedMemoryStore::new(config.shards_per_node.max(1))))
        .collect();

    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let stores = stores.clone();
        let pipeline = config.pipeline.clone();
        let total_blocks = config.total_blocks;
        let block_size = config.block_size;

        handles.push(thread::spawn(move || {
            let mut counters = WorkerCounters::default();
            for block_id in (worker_id..total_blocks).step_by(workers) {
                let payload = generate_block(block_id as u64, block_size);
                let state = PipelineState::new(block_id as u64, payload);
                let processed = pipeline.run(state);

                let primary = (processed.bucket as usize) % nodes;
                for r in 0..replication {
                    let node_idx = (primary + r) % nodes;
                    stores[node_idx].put(processed.digest, processed.payload.clone());
                }

                if block_id % verify_stride == 0 {
                    let hit = stores[primary].get(&processed.digest);
                    if hit.as_deref() != Some(processed.payload.as_slice()) {
                        counters.verification_failures += 1;
                    }
                }

                counters.blocks += 1;
                counters.logical_bytes += block_size;
                counters.physical_bytes += block_size * replication;
            }
            counters
        }));
    }

    let mut totals = WorkerCounters::default();
    for handle in handles {
        if let Ok(stats) = handle.join() {
            totals.blocks += stats.blocks;
            totals.logical_bytes += stats.logical_bytes;
            totals.physical_bytes += stats.physical_bytes;
            totals.verification_failures += stats.verification_failures;
        } else {
            totals.verification_failures += 1;
        }
    }

    let elapsed = started.elapsed().as_secs_f64().max(1e-9);
    let total_items = stores.iter().map(|s| s.item_count()).sum();
    BenchResult {
        mode: "multinode".to_string(),
        elapsed_sec: elapsed,
        blocks_processed: totals.blocks,
        logical_bytes: totals.logical_bytes,
        physical_bytes: totals.physical_bytes,
        throughput_mibps: (totals.logical_bytes as f64 / 1_048_576.0) / elapsed,
        blocks_per_sec: totals.blocks as f64 / elapsed,
        verification_failures: totals.verification_failures,
        stores: total_items,
    }
}

async fn run_single_worker(
    worker_id: usize,
    workers: usize,
    total_blocks: usize,
    block_size: usize,
    batch_blocks: usize,
    verify_stride: usize,
    verify_max_per_worker: usize,
    pipeline: Pipeline,
    store: Arc<BlockStore>,
) -> Result<(WorkerCounters, Vec<VerificationSample>), StorageError> {
    let mut counters = WorkerCounters::default();
    let mut batch = Vec::with_capacity(batch_blocks.max(1));
    let mut samples = Vec::new();

    for block_id in (worker_id..total_blocks).step_by(workers) {
        let payload = generate_block(block_id as u64, block_size);
        let processed = pipeline.run(PipelineState::new(block_id as u64, payload));
        let digest = blake3_digest32(&processed.payload);
        let block = Block::new(processed.payload)?;

        if block_id % verify_stride == 0 && samples.len() < verify_max_per_worker {
            samples.push(VerificationSample {
                node_idx: 0,
                cid: block.cid,
                len: block.data.len(),
                digest,
            });
        }

        counters.blocks += 1;
        counters.logical_bytes += block.data.len();
        counters.physical_bytes += block.data.len();
        batch.push(block);

        if batch.len() >= batch_blocks {
            store.put_many(std::mem::take(&mut batch)).await?;
        }
    }

    if !batch.is_empty() {
        store.put_many(batch).await?;
    }

    Ok((counters, samples))
}

async fn run_multinode_worker(
    worker_id: usize,
    workers: usize,
    total_blocks: usize,
    block_size: usize,
    batch_blocks: usize,
    verify_stride: usize,
    verify_max_per_worker: usize,
    nodes: usize,
    replication: usize,
    pipeline: Pipeline,
    stores: Arc<Vec<Arc<BlockStore>>>,
) -> Result<(WorkerCounters, Vec<VerificationSample>), StorageError> {
    let mut counters = WorkerCounters::default();
    let mut node_batches: Vec<Vec<Block>> = (0..nodes)
        .map(|_| Vec::with_capacity(batch_blocks.max(1)))
        .collect();
    let mut samples = Vec::new();

    for block_id in (worker_id..total_blocks).step_by(workers) {
        let payload = generate_block(block_id as u64, block_size);
        let processed = pipeline.run(PipelineState::new(block_id as u64, payload));
        let base_block = Block::new(processed.payload)?;
        let digest = blake3_digest32(&base_block.data);
        let primary = (processed.bucket as usize) % nodes;

        if block_id % verify_stride == 0 && samples.len() < verify_max_per_worker {
            samples.push(VerificationSample {
                node_idx: primary,
                cid: base_block.cid,
                len: base_block.data.len(),
                digest,
            });
        }

        counters.blocks += 1;
        counters.logical_bytes += base_block.data.len();
        counters.physical_bytes += base_block.data.len() * replication;

        for r in 0..replication {
            let node_idx = (primary + r) % nodes;
            let block = if r == 0 {
                base_block.clone()
            } else {
                Block {
                    cid: base_block.cid,
                    data: base_block.data.clone(),
                }
            };
            node_batches[node_idx].push(block);
            if node_batches[node_idx].len() >= batch_blocks {
                stores[node_idx]
                    .put_many(std::mem::take(&mut node_batches[node_idx]))
                    .await?;
            }
        }
    }

    for node_idx in 0..nodes {
        if !node_batches[node_idx].is_empty() {
            stores[node_idx]
                .put_many(std::mem::take(&mut node_batches[node_idx]))
                .await?;
        }
    }

    Ok((counters, samples))
}

async fn verify_samples(
    stores: &[Arc<BlockStore>],
    samples: &[VerificationSample],
) -> Result<usize, StorageError> {
    let mut failures = 0usize;
    for sample in samples {
        let store = &stores[sample.node_idx];
        match store.get(&sample.cid).await {
            Ok(block) => {
                if block.data.len() != sample.len || blake3_digest32(&block.data) != sample.digest {
                    failures += 1;
                }
            }
            Err(_) => failures += 1,
        }
    }
    Ok(failures)
}

pub async fn bench_real_store(config: RealStoreBenchConfig) -> Result<BenchResult, StorageError> {
    let workers = config.workers.max(1);
    let verify_stride = config.verify_stride.max(1);
    let batch_blocks = config.batch_blocks.max(1);
    let verify_per_worker = (config.verify_samples_max.max(1) / workers).max(1);

    if config.clear_existing && config.root_dir.exists() {
        fs::remove_dir_all(&config.root_dir)?;
    }
    fs::create_dir_all(&config.root_dir)?;

    let data_dir = config.root_dir.join("node-000");
    fs::create_dir_all(&data_dir)?;
    let store = Arc::new(BlockStore::new_with_backend(&data_dir, &config.backend)?);
    let stores = vec![Arc::clone(&store)];

    let started = Instant::now();
    let mut set = JoinSet::new();
    for worker_id in 0..workers {
        let pipeline = config.pipeline.clone();
        let local_store = Arc::clone(&store);
        set.spawn(run_single_worker(
            worker_id,
            workers,
            config.total_blocks,
            config.block_size,
            batch_blocks,
            verify_stride,
            verify_per_worker,
            pipeline,
            local_store,
        ));
    }

    let mut totals = WorkerCounters::default();
    let mut samples = Vec::new();
    while let Some(joined) = set.join_next().await {
        let (stats, mut worker_samples) =
            joined.map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))??;
        totals.blocks += stats.blocks;
        totals.logical_bytes += stats.logical_bytes;
        totals.physical_bytes += stats.physical_bytes;
        totals.verification_failures += stats.verification_failures;
        samples.append(&mut worker_samples);
    }

    if samples.len() > config.verify_samples_max {
        samples.truncate(config.verify_samples_max);
    }
    totals.verification_failures += verify_samples(&stores, &samples).await?;

    let elapsed = started.elapsed().as_secs_f64().max(1e-9);
    let stats = store.stats().await;
    Ok(BenchResult {
        mode: format!("real-single-{}", config.backend),
        elapsed_sec: elapsed,
        blocks_processed: totals.blocks,
        logical_bytes: totals.logical_bytes,
        physical_bytes: totals.physical_bytes,
        throughput_mibps: (totals.logical_bytes as f64 / 1_048_576.0) / elapsed,
        blocks_per_sec: totals.blocks as f64 / elapsed,
        verification_failures: totals.verification_failures,
        stores: stats.block_count,
    })
}

pub async fn bench_real_multinode(
    config: RealMultiNodeBenchConfig,
) -> Result<BenchResult, StorageError> {
    let nodes = config.nodes.max(1);
    let replication = config.replication.clamp(1, nodes);
    let workers = config.workers_per_node.max(1) * nodes;
    let verify_stride = config.verify_stride.max(1);
    let batch_blocks = config.batch_blocks.max(1);
    let verify_per_worker = (config.verify_samples_max.max(1) / workers).max(1);

    if config.clear_existing && config.root_dir.exists() {
        fs::remove_dir_all(&config.root_dir)?;
    }
    fs::create_dir_all(&config.root_dir)?;

    let mut stores_vec = Vec::with_capacity(nodes);
    for node_idx in 0..nodes {
        let data_dir = config.root_dir.join(format!("node-{:03}", node_idx));
        fs::create_dir_all(&data_dir)?;
        stores_vec.push(Arc::new(BlockStore::new_with_backend(
            &data_dir,
            &config.backend,
        )?));
    }
    let stores = Arc::new(stores_vec);

    let started = Instant::now();
    let mut set = JoinSet::new();
    for worker_id in 0..workers {
        let pipeline = config.pipeline.clone();
        let local_stores = Arc::clone(&stores);
        set.spawn(run_multinode_worker(
            worker_id,
            workers,
            config.total_blocks,
            config.block_size,
            batch_blocks,
            verify_stride,
            verify_per_worker,
            nodes,
            replication,
            pipeline,
            local_stores,
        ));
    }

    let mut totals = WorkerCounters::default();
    let mut samples = Vec::new();
    while let Some(joined) = set.join_next().await {
        let (stats, mut worker_samples) =
            joined.map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))??;
        totals.blocks += stats.blocks;
        totals.logical_bytes += stats.logical_bytes;
        totals.physical_bytes += stats.physical_bytes;
        totals.verification_failures += stats.verification_failures;
        samples.append(&mut worker_samples);
    }

    if samples.len() > config.verify_samples_max {
        samples.truncate(config.verify_samples_max);
    }
    totals.verification_failures += verify_samples(stores.as_ref().as_slice(), &samples).await?;

    let mut total_items = 0usize;
    for store in stores.iter() {
        total_items += store.stats().await.block_count;
    }

    let elapsed = started.elapsed().as_secs_f64().max(1e-9);
    Ok(BenchResult {
        mode: format!("real-multinode-{}", config.backend),
        elapsed_sec: elapsed,
        blocks_processed: totals.blocks,
        logical_bytes: totals.logical_bytes,
        physical_bytes: totals.physical_bytes,
        throughput_mibps: (totals.logical_bytes as f64 / 1_048_576.0) / elapsed,
        blocks_per_sec: totals.blocks as f64 / elapsed,
        verification_failures: totals.verification_failures,
        stores: total_items,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_parses_and_runs() {
        let p = Pipeline::from_spec("xor,blake3,index_mod:1024");
        let payload = generate_block(42, 4096);
        let out = p.run(PipelineState::new(42, payload));
        assert!(out.has_digest);
        assert!(out.bucket < 1024);
    }

    #[test]
    fn in_memory_bench_stores_blocks() {
        let cfg = InMemoryBenchConfig {
            total_blocks: 8_000,
            block_size: 8 << 10,
            workers: 4,
            shards: 64,
            verify_stride: 64,
            pipeline: Pipeline::from_spec("xor,blake3,index_mod:8192"),
        };
        let res = bench_in_memory(cfg);
        assert_eq!(res.mode, "inmem");
        assert!(res.blocks_processed >= 8_000);
        assert_eq!(res.verification_failures, 0);
        assert!(res.throughput_mibps > 0.0);
    }

    #[test]
    fn multinode_bench_replicates() {
        let cfg = MultiNodeBenchConfig {
            nodes: 3,
            replication: 2,
            total_blocks: 6_000,
            block_size: 4 << 10,
            workers_per_node: 2,
            shards_per_node: 32,
            verify_stride: 64,
            pipeline: Pipeline::from_spec("xor,sha256,index_xorfold:4096"),
        };
        let res = bench_multinode(cfg);
        assert_eq!(res.mode, "multinode");
        assert!(res.blocks_processed >= 6_000);
        assert_eq!(res.verification_failures, 0);
        assert!(res.stores >= 6_000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_single_store_bench_writes_and_reads() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RealStoreBenchConfig {
            backend: "redb".to_string(),
            root_dir: tmp.path().join("real-single"),
            total_blocks: 2_000,
            block_size: 8 << 10,
            workers: 2,
            batch_blocks: 64,
            verify_stride: 32,
            verify_samples_max: 128,
            pipeline: Pipeline::from_spec("xor,blake3,index_mod:4096"),
            clear_existing: true,
        };
        let res = bench_real_store(cfg).await.expect("real single bench");
        assert!(res.blocks_processed >= 2_000);
        assert_eq!(res.verification_failures, 0);
        assert!(res.throughput_mibps > 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_multinode_bench_writes_and_reads() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RealMultiNodeBenchConfig {
            backend: "redb".to_string(),
            root_dir: tmp.path().join("real-multi"),
            nodes: 3,
            replication: 2,
            total_blocks: 2_400,
            block_size: 4 << 10,
            workers_per_node: 1,
            batch_blocks: 32,
            verify_stride: 32,
            verify_samples_max: 128,
            pipeline: Pipeline::from_spec("xor,blake3,index_mod:4096"),
            clear_existing: true,
        };
        let res = bench_real_multinode(cfg)
            .await
            .expect("real multinode bench");
        assert!(res.blocks_processed >= 2_400);
        assert_eq!(res.verification_failures, 0);
        assert!(res.stores >= 2_400);
    }
}
