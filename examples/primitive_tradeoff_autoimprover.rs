use neverust_core::primitive_lab::{
    analyze_runs, load_runs_from_csv, primitive_columns, ScoreWeights,
};
use std::env;
use std::error::Error;
use std::fs;

const TEMPLATE_CSV: &str = "run_id,p_hash,p_index,p_layout,p_commit,p_lock,throughput_mibps,p99_ms,cpu_pct,mem_mb,write_amp,read_amp,durability_score,correctness_failures,reorder_violations,gc_violations\n\
run-a,blake3,linear_hash,flatfs_tree,single_fsync,range_flock,910,18,67,720,1.04,1.09,0.95,0,0,0\n\
run-b,blake3,linear_hash,flatfs_tree,group_commit,range_flock,1110,23,72,758,1.01,1.08,0.93,0,0,0\n\
run-c,sha256,btree_index,segment_log,single_fsync,mutex,640,34,89,910,1.19,1.25,0.98,0,0,0\n\
";

fn print_usage(bin: &str) {
    eprintln!("Usage:");
    eprintln!("  {bin} template <output_csv_path>");
    eprintln!("  {bin} analyze <input_csv_path> [top_runs] [max_suggestions]");
}

fn format_primitives(map: &std::collections::BTreeMap<String, String>) -> String {
    map.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let bin = args
        .first()
        .map_or("primitive_tradeoff_autoimprover", String::as_str);
    if args.len() < 2 {
        print_usage(bin);
        std::process::exit(2);
    }

    match args[1].as_str() {
        "template" => {
            let path = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "examples/primitive_tradeoff_template.csv".to_string());
            fs::write(&path, TEMPLATE_CSV)?;
            println!("WROTE_TEMPLATE={path}");
        }
        "analyze" => {
            if args.len() < 3 {
                print_usage(bin);
                std::process::exit(2);
            }
            let input = &args[2];
            let top_runs = args
                .get(3)
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(12);
            let max_suggestions = args
                .get(4)
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(12);

            let runs = load_runs_from_csv(input)?;
            let cols = primitive_columns(&runs);
            let report = analyze_runs(&runs, ScoreWeights::default(), max_suggestions);

            println!("MODE=primitive_tradeoff_autoimprover");
            println!("INPUT_CSV={input}");
            println!("RUN_COUNT={}", report.scored_runs.len());
            println!("PRIMITIVE_COLUMNS={}", cols.join(","));

            println!("TOP_RUNS_BEGIN");
            for scored in report.scored_runs.iter().take(top_runs) {
                let m = scored.record.metrics;
                println!(
                    "run_id={} score={:.6} throughput_mibps={:.2} p99_ms={:.2} cpu_pct={:.2} mem_mb={:.2} write_amp={:.3} read_amp={:.3} durability={:.3} fail={} reorder={} gc={} primitives={}",
                    scored.record.run_id,
                    scored.score,
                    m.throughput_mibps,
                    m.p99_ms,
                    m.cpu_pct,
                    m.mem_mb,
                    m.write_amp,
                    m.read_amp,
                    m.durability_score,
                    m.correctness_failures,
                    m.reorder_violations,
                    m.gc_violations,
                    format_primitives(&scored.record.primitives),
                );
            }
            println!("TOP_RUNS_END");

            println!("PARETO_BEGIN");
            for scored in &report.pareto_frontier {
                let m = scored.record.metrics;
                println!(
                    "run_id={} score={:.6} throughput_mibps={:.2} p99_ms={:.2} cpu_pct={:.2} mem_mb={:.2} durability={:.3} fail={} reorder={} gc={} primitives={}",
                    scored.record.run_id,
                    scored.score,
                    m.throughput_mibps,
                    m.p99_ms,
                    m.cpu_pct,
                    m.mem_mb,
                    m.durability_score,
                    m.correctness_failures,
                    m.reorder_violations,
                    m.gc_violations,
                    format_primitives(&scored.record.primitives),
                );
            }
            println!("PARETO_END");

            println!("TRADEOFF_MATRIX_BEGIN");
            for (primitive, cells) in &report.tradeoff_matrix {
                for cell in cells {
                    println!(
                        "primitive={} option={} samples={} avg_score={:.6} avg_throughput_mibps={:.2} avg_p99_ms={:.2} avg_cpu_pct={:.2} avg_mem_mb={:.2} failure_rate={:.4}",
                        primitive,
                        cell.option,
                        cell.samples,
                        cell.avg_score,
                        cell.avg_throughput_mibps,
                        cell.avg_p99_ms,
                        cell.avg_cpu_pct,
                        cell.avg_mem_mb,
                        cell.failure_rate,
                    );
                }
            }
            println!("TRADEOFF_MATRIX_END");

            println!("SUGGESTIONS_BEGIN");
            for (idx, suggestion) in report.suggestions.iter().enumerate() {
                println!(
                    "rank={} predicted_score={:.6} rationale={} composition={}",
                    idx + 1,
                    suggestion.predicted_score,
                    suggestion.rationale,
                    format_primitives(&suggestion.composition),
                );
            }
            println!("SUGGESTIONS_END");
        }
        _ => {
            print_usage(bin);
            std::process::exit(2);
        }
    }

    Ok(())
}
