use neverust_core::{select_replicas, upload_path_for_cid_str, ClusterNode};
use reqwest::Client;
use std::env;
use std::error::Error;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

fn normalize_base_url(raw: &str) -> String {
    raw.trim_end_matches('/').to_string()
}

fn parse_usize(args: &[String], idx: usize, default: usize) -> usize {
    args.get(idx)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn parse_bool(args: &[String], idx: usize, default: bool) -> bool {
    args.get(idx).map_or(default, |v| match v.as_str() {
        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON" => true,
        "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF" => false,
        _ => default,
    })
}

fn parse_nodes(spec: &str) -> Result<Vec<ClusterNode>, Box<dyn Error>> {
    let raw = if let Some(path) = spec.strip_prefix('@') {
        fs::read_to_string(path)?
    } else {
        spec.to_string()
    };

    let mut out = Vec::new();
    for (idx, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = trimmed.split(',').map(str::trim).collect();
        let (id, base_url, weight) = match parts.len() {
            0 => continue,
            1 => (
                format!("node-{}", idx),
                normalize_base_url(parts[0]),
                1u32,
            ),
            2 => (
                parts[0].to_string(),
                normalize_base_url(parts[1]),
                1u32,
            ),
            _ => (
                parts[0].to_string(),
                normalize_base_url(parts[1]),
                parts[2].parse::<u32>().unwrap_or(1).max(1),
            ),
        };
        out.push(ClusterNode {
            id,
            base_url,
            weight,
        });
    }
    if out.is_empty() {
        return Err("no nodes parsed".into());
    }
    Ok(out)
}

fn print_usage(bin: &str) {
    eprintln!("Usage:");
    eprintln!(
        "  {bin} <cid> <source_base_url> <replicas> <nodes_spec> [parallelism] [verify_0_or_1]\n\nnodes_spec:\n  - comma/line list of base URLs\n  - or @/path/to/nodes.txt\n\nnodes.txt formats:\n  - <base_url>\n  - <id>,<base_url>\n  - <id>,<base_url>,<weight>"
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let bin = args.first().map_or("cluster_pin_orchestrator", String::as_str);
    if args.len() < 5 {
        print_usage(bin);
        std::process::exit(2);
    }

    let cid = args[1].trim().to_string();
    let source = normalize_base_url(&args[2]);
    let replicas = parse_usize(&args, 3, 3);
    let nodes = parse_nodes(&args[4])?;
    let parallelism = parse_usize(&args, 5, 256);
    let verify = parse_bool(&args, 6, false);
    let upload_path = upload_path_for_cid_str(&cid);

    let selected = select_replicas(&cid, &nodes, replicas);
    let selected_urls: Vec<String> = selected.iter().map(|n| n.base_url.clone()).collect();

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(parallelism.saturating_mul(2).max(64))
        .tcp_nodelay(true)
        .build()?;

    println!("CID={}", cid);
    println!("SOURCE={}", source);
    println!("REPLICAS_REQUESTED={}", replicas);
    println!("REPLICAS_SELECTED={}", selected_urls.len());
    println!("PARALLELISM={}", parallelism);
    println!("VERIFY={}", verify);
    println!("UPLOAD_PATH={}", upload_path);

    // Fetch once from source, fan out to target replicas.
    let fetch_url = format!("{}/api/archivist/v1/data/{}/network/stream", source, cid);
    let src_resp = client.get(fetch_url).send().await?;
    if !src_resp.status().is_success() {
        return Err(format!("source fetch failed: HTTP {}", src_resp.status()).into());
    }
    let payload = src_resp.bytes().await?;
    println!("PAYLOAD_BYTES={}", payload.len());
    let payload = Arc::new(payload.to_vec());

    let sem = Arc::new(Semaphore::new(parallelism.max(1)));
    let mut tasks = Vec::with_capacity(selected_urls.len());
    for base in selected_urls {
        let cid = cid.clone();
        let upload_path = upload_path.to_string();
        let payload = Arc::clone(&payload);
        let client = client.clone();
        let sem = Arc::clone(&sem);
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.map_err(|e| e.to_string())?;
            let upload_url = format!("{}{}", normalize_base_url(&base), upload_path);
            let resp = client
                .post(upload_url)
                .header("content-type", "application/octet-stream")
                .body((*payload).clone())
                .send()
                .await
                .map_err(|e| format!("upload request failed for {}: {}", base, e))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(format!("upload failed for {}: HTTP {} {}", base, status, body));
            }
            let returned = resp
                .text()
                .await
                .map_err(|e| format!("upload cid parse failed for {}: {}", base, e))?;
            let returned = returned.trim().to_string();
            if returned != cid {
                return Err(format!(
                    "cid mismatch on {}: expected {}, got {}",
                    base, cid, returned
                ));
            }
            Ok::<String, String>(base)
        }));
    }

    let mut ok = 0usize;
    let mut failed = Vec::new();
    for t in tasks {
        match t.await {
            Ok(Ok(node)) => {
                ok += 1;
                println!("PIN_OK={}", node);
            }
            Ok(Err(e)) => failed.push(e),
            Err(e) => failed.push(format!("task join error: {}", e)),
        }
    }

    println!("PIN_OK_TOTAL={}", ok);
    println!("PIN_FAIL_TOTAL={}", failed.len());
    for err in failed {
        println!("PIN_FAIL={}", err);
    }

    if verify && ok > 0 {
        let mut verify_ok = 0usize;
        for node in &nodes {
            let url = format!(
                "{}/api/archivist/v1/data/{}/network/stream",
                normalize_base_url(&node.base_url),
                cid
            );
            if let Ok(resp) = client.get(url).send().await {
                if resp.status().is_success() {
                    if let Ok(bytes) = resp.bytes().await {
                        if bytes.as_ref() == payload.as_slice() {
                            verify_ok += 1;
                        }
                    }
                }
            }
        }
        println!("VERIFY_MATCHING_NODES={}", verify_ok);
    }

    Ok(())
}
