use neverust_core::storage::{Block, BlockStore};
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::Read;
use std::time::{Duration, Instant};

fn parse_usize_arg(args: &[String], idx: usize, default: usize) -> usize {
    args.get(idx)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <src_file> <data_dir> [block_size_bytes] [batch_blocks]",
            args.first().map_or("raw_store_bench", String::as_str)
        );
        std::process::exit(2);
    }

    let src_file = &args[1];
    let data_dir = &args[2];
    let block_size = parse_usize_arg(&args, 3, 1024 * 1024);
    let batch_blocks = parse_usize_arg(&args, 4, 256);
    let backend = env::var("NEVERUST_STORAGE_BACKEND").unwrap_or_else(|_| "deltaflat".to_string());

    fs::create_dir_all(data_dir)?;
    let store = BlockStore::new_with_path(data_dir)?;

    let mut file = File::open(src_file)?;
    let total_bytes = file.metadata()?.len();
    let mut io_buf = vec![0u8; block_size];
    let mut batch = Vec::with_capacity(batch_blocks);
    let mut written_bytes: u64 = 0;
    let mut written_blocks: u64 = 0;
    let mut hash_secs = 0.0f64;
    let mut store_secs = 0.0f64;

    println!("MODE=raw_store");
    println!("BACKEND={}", backend);
    println!("SRC_FILE={}", src_file);
    println!("DATA_DIR={}", data_dir);
    println!("TOTAL_BYTES={}", total_bytes);
    println!("BLOCK_SIZE={}", block_size);
    println!("BATCH_BLOCKS={}", batch_blocks);

    let started = Instant::now();
    let mut last_report = started;

    loop {
        let mut filled = 0usize;
        while filled < block_size {
            let n = file.read(&mut io_buf[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }

        if filled == 0 {
            break;
        }

        let hash_start = Instant::now();
        let block = Block::new(io_buf[..filled].to_vec())?;
        hash_secs += hash_start.elapsed().as_secs_f64();
        batch.push(block);
        written_bytes = written_bytes.saturating_add(filled as u64);
        written_blocks = written_blocks.saturating_add(1);

        if batch.len() >= batch_blocks {
            let store_start = Instant::now();
            store.put_many(std::mem::take(&mut batch)).await?;
            store_secs += store_start.elapsed().as_secs_f64();
        }

        let now = Instant::now();
        if now.duration_since(last_report) >= Duration::from_secs(1) {
            let elapsed = now.duration_since(started).as_secs_f64();
            let mibps = if elapsed > 0.0 {
                (written_bytes as f64 / 1048576.0) / elapsed
            } else {
                0.0
            };
            eprintln!(
                "PROGRESS bytes={} blocks={} elapsed={:.2}s throughput_mibps={:.2}",
                written_bytes, written_blocks, elapsed, mibps
            );
            last_report = now;
        }
    }

    if !batch.is_empty() {
        let store_start = Instant::now();
        store.put_many(batch).await?;
        store_secs += store_start.elapsed().as_secs_f64();
    }

    let elapsed = started.elapsed().as_secs_f64();
    let mibps = if elapsed > 0.0 {
        (written_bytes as f64 / 1048576.0) / elapsed
    } else {
        0.0
    };
    let hash_mibps = if hash_secs > 0.0 {
        (written_bytes as f64 / 1048576.0) / hash_secs
    } else {
        0.0
    };
    let store_mibps = if store_secs > 0.0 {
        (written_bytes as f64 / 1048576.0) / store_secs
    } else {
        0.0
    };

    println!("WRITTEN_BYTES={}", written_bytes);
    println!("WRITTEN_BLOCKS={}", written_blocks);
    println!("TIME_SEC={:.6}", elapsed);
    println!("HASH_TIME_SEC={:.6}", hash_secs);
    println!("STORE_TIME_SEC={:.6}", store_secs);
    println!("THROUGHPUT_MiBPS={:.2}", mibps);
    println!("HASH_ONLY_MiBPS={:.2}", hash_mibps);
    println!("STORE_ONLY_MiBPS={:.2}", store_mibps);

    Ok(())
}
