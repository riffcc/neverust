use neverust_core::primitive_pipeline::{
    bench_in_memory, bench_multinode, bench_real_multinode, bench_real_store, InMemoryBenchConfig,
    MultiNodeBenchConfig, Pipeline, RealMultiNodeBenchConfig, RealStoreBenchConfig,
};
use std::env;
use std::error::Error;
use std::path::PathBuf;

fn parse_usize_arg(args: &[String], idx: usize, default: usize) -> usize {
    args.get(idx)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn parse_bool_arg(args: &[String], idx: usize, default: bool) -> bool {
    args.get(idx)
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            matches!(t.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(default)
}

fn print_usage(bin: &str) {
    eprintln!("Usage:");
    eprintln!(
        "  {bin} inmem <blocks> <block_size> <workers> [pipeline_spec] [shards] [verify_stride]"
    );
    eprintln!(
        "  {bin} multinode <nodes> <replication> <blocks> <block_size> <workers_per_node> [pipeline_spec] [shards_per_node] [verify_stride]"
    );
    eprintln!(
        "  {bin} real <backend> <data_dir> <blocks> <block_size> <workers> [batch_blocks] [pipeline_spec] [verify_stride] [verify_samples_max] [clear_existing]"
    );
    eprintln!(
        "  {bin} real-multinode <backend> <data_dir> <nodes> <replication> <blocks> <block_size> <workers_per_node> [batch_blocks] [pipeline_spec] [verify_stride] [verify_samples_max] [clear_existing]"
    );
    eprintln!("Examples:");
    eprintln!("  {bin} inmem 300000 1048576 24 xor,blake3,index_mod:4194304 512 64");
    eprintln!("  {bin} multinode 8 2 500000 524288 12 xor,blake3,index_xorfold:4194304 256 128");
    eprintln!(
        "  {bin} real deltaflat /mnt/mfs/bench/real-single 50000 1048576 16 256 xor,blake3,index_mod:4194304 256 2048 true"
    );
    eprintln!(
        "  {bin} real-multinode deltaflat /mnt/mfs/bench/real-multi 4 2 80000 524288 8 128 xor,blake3,index_xorfold:4194304 256 2048 true"
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let bin = args
        .first()
        .map_or("primitive_pipeline_bench", String::as_str);
    if args.len() < 2 {
        print_usage(bin);
        std::process::exit(2);
    }

    match args[1].as_str() {
        "inmem" => {
            if args.len() < 5 {
                print_usage(bin);
                std::process::exit(2);
            }
            let blocks = parse_usize_arg(&args, 2, 200_000);
            let block_size = parse_usize_arg(&args, 3, 1 << 20);
            let workers = parse_usize_arg(&args, 4, 8);
            let pipeline_spec = args
                .get(5)
                .map_or("xor,blake3,index_mod:1048576", String::as_str);
            let shards = parse_usize_arg(&args, 6, 256);
            let verify_stride = parse_usize_arg(&args, 7, 64);

            let cfg = InMemoryBenchConfig {
                total_blocks: blocks,
                block_size,
                workers,
                shards,
                verify_stride,
                pipeline: Pipeline::from_spec(pipeline_spec),
            };
            let result = bench_in_memory(cfg);

            println!("MODE=inmem");
            println!("PIPELINE_SPEC={pipeline_spec}");
            println!("BLOCKS={blocks}");
            println!("BLOCK_SIZE={block_size}");
            println!("WORKERS={workers}");
            println!("SHARDS={shards}");
            println!("VERIFY_STRIDE={verify_stride}");
            println!("ELAPSED_SEC={:.6}", result.elapsed_sec);
            println!("BLOCKS_PROCESSED={}", result.blocks_processed);
            println!("LOGICAL_BYTES={}", result.logical_bytes);
            println!("PHYSICAL_BYTES={}", result.physical_bytes);
            println!("THROUGHPUT_MiBPS={:.2}", result.throughput_mibps);
            println!("BLOCKS_PER_SEC={:.2}", result.blocks_per_sec);
            println!("VERIFICATION_FAILURES={}", result.verification_failures);
            println!("STORE_ITEMS={}", result.stores);
        }
        "multinode" => {
            if args.len() < 7 {
                print_usage(bin);
                std::process::exit(2);
            }
            let nodes = parse_usize_arg(&args, 2, 4);
            let replication = parse_usize_arg(&args, 3, 2);
            let blocks = parse_usize_arg(&args, 4, 150_000);
            let block_size = parse_usize_arg(&args, 5, 512 << 10);
            let workers_per_node = parse_usize_arg(&args, 6, 4);
            let pipeline_spec = args
                .get(7)
                .map_or("xor,blake3,index_mod:1048576", String::as_str);
            let shards_per_node = parse_usize_arg(&args, 8, 128);
            let verify_stride = parse_usize_arg(&args, 9, 128);

            let cfg = MultiNodeBenchConfig {
                nodes,
                replication,
                total_blocks: blocks,
                block_size,
                workers_per_node,
                shards_per_node,
                verify_stride,
                pipeline: Pipeline::from_spec(pipeline_spec),
            };
            let result = bench_multinode(cfg);

            println!("MODE=multinode");
            println!("PIPELINE_SPEC={pipeline_spec}");
            println!("NODES={nodes}");
            println!("REPLICATION={replication}");
            println!("BLOCKS={blocks}");
            println!("BLOCK_SIZE={block_size}");
            println!("WORKERS_PER_NODE={workers_per_node}");
            println!("SHARDS_PER_NODE={shards_per_node}");
            println!("VERIFY_STRIDE={verify_stride}");
            println!("ELAPSED_SEC={:.6}", result.elapsed_sec);
            println!("BLOCKS_PROCESSED={}", result.blocks_processed);
            println!("LOGICAL_BYTES={}", result.logical_bytes);
            println!("PHYSICAL_BYTES={}", result.physical_bytes);
            println!("THROUGHPUT_MiBPS={:.2}", result.throughput_mibps);
            println!("BLOCKS_PER_SEC={:.2}", result.blocks_per_sec);
            println!("VERIFICATION_FAILURES={}", result.verification_failures);
            println!("STORE_ITEMS={}", result.stores);
        }
        "real" => {
            if args.len() < 7 {
                print_usage(bin);
                std::process::exit(2);
            }
            let backend = args[2].clone();
            let data_dir = PathBuf::from(&args[3]);
            let blocks = parse_usize_arg(&args, 4, 40_000);
            let block_size = parse_usize_arg(&args, 5, 1 << 20);
            let workers = parse_usize_arg(&args, 6, 8);
            let batch_blocks = parse_usize_arg(&args, 7, 256);
            let pipeline_spec = args
                .get(8)
                .map_or("xor,blake3,index_mod:1048576", String::as_str);
            let verify_stride = parse_usize_arg(&args, 9, 256);
            let verify_samples_max = parse_usize_arg(&args, 10, 1024);
            let clear_existing = parse_bool_arg(&args, 11, true);

            let cfg = RealStoreBenchConfig {
                backend: backend.clone(),
                root_dir: data_dir,
                total_blocks: blocks,
                block_size,
                workers,
                batch_blocks,
                verify_stride,
                verify_samples_max,
                pipeline: Pipeline::from_spec(pipeline_spec),
                clear_existing,
            };
            let result = bench_real_store(cfg).await?;

            println!("MODE=real");
            println!("BACKEND={backend}");
            println!("PIPELINE_SPEC={pipeline_spec}");
            println!("BLOCKS={blocks}");
            println!("BLOCK_SIZE={block_size}");
            println!("WORKERS={workers}");
            println!("BATCH_BLOCKS={batch_blocks}");
            println!("VERIFY_STRIDE={verify_stride}");
            println!("VERIFY_SAMPLES_MAX={verify_samples_max}");
            println!("CLEAR_EXISTING={clear_existing}");
            println!("ELAPSED_SEC={:.6}", result.elapsed_sec);
            println!("BLOCKS_PROCESSED={}", result.blocks_processed);
            println!("LOGICAL_BYTES={}", result.logical_bytes);
            println!("PHYSICAL_BYTES={}", result.physical_bytes);
            println!("THROUGHPUT_MiBPS={:.2}", result.throughput_mibps);
            println!("BLOCKS_PER_SEC={:.2}", result.blocks_per_sec);
            println!("VERIFICATION_FAILURES={}", result.verification_failures);
            println!("STORE_ITEMS={}", result.stores);
        }
        "real-multinode" => {
            if args.len() < 9 {
                print_usage(bin);
                std::process::exit(2);
            }
            let backend = args[2].clone();
            let data_dir = PathBuf::from(&args[3]);
            let nodes = parse_usize_arg(&args, 4, 4);
            let replication = parse_usize_arg(&args, 5, 2);
            let blocks = parse_usize_arg(&args, 6, 32_000);
            let block_size = parse_usize_arg(&args, 7, 512 << 10);
            let workers_per_node = parse_usize_arg(&args, 8, 4);
            let batch_blocks = parse_usize_arg(&args, 9, 128);
            let pipeline_spec = args
                .get(10)
                .map_or("xor,blake3,index_xorfold:1048576", String::as_str);
            let verify_stride = parse_usize_arg(&args, 11, 256);
            let verify_samples_max = parse_usize_arg(&args, 12, 1024);
            let clear_existing = parse_bool_arg(&args, 13, true);

            let cfg = RealMultiNodeBenchConfig {
                backend: backend.clone(),
                root_dir: data_dir,
                nodes,
                replication,
                total_blocks: blocks,
                block_size,
                workers_per_node,
                batch_blocks,
                verify_stride,
                verify_samples_max,
                pipeline: Pipeline::from_spec(pipeline_spec),
                clear_existing,
            };
            let result = bench_real_multinode(cfg).await?;

            println!("MODE=real-multinode");
            println!("BACKEND={backend}");
            println!("PIPELINE_SPEC={pipeline_spec}");
            println!("NODES={nodes}");
            println!("REPLICATION={replication}");
            println!("BLOCKS={blocks}");
            println!("BLOCK_SIZE={block_size}");
            println!("WORKERS_PER_NODE={workers_per_node}");
            println!("BATCH_BLOCKS={batch_blocks}");
            println!("VERIFY_STRIDE={verify_stride}");
            println!("VERIFY_SAMPLES_MAX={verify_samples_max}");
            println!("CLEAR_EXISTING={clear_existing}");
            println!("ELAPSED_SEC={:.6}", result.elapsed_sec);
            println!("BLOCKS_PROCESSED={}", result.blocks_processed);
            println!("LOGICAL_BYTES={}", result.logical_bytes);
            println!("PHYSICAL_BYTES={}", result.physical_bytes);
            println!("THROUGHPUT_MiBPS={:.2}", result.throughput_mibps);
            println!("BLOCKS_PER_SEC={:.2}", result.blocks_per_sec);
            println!("VERIFICATION_FAILURES={}", result.verification_failures);
            println!("STORE_ITEMS={}", result.stores);
        }
        _ => {
            print_usage(bin);
            std::process::exit(2);
        }
    }

    Ok(())
}
