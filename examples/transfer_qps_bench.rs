use reqwest::Client;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[derive(Clone)]
struct Route {
    name: String,
    src_base: String,
    dst_base: String,
    salt: u64,
    raw_upload: bool,
}

#[derive(Default)]
struct BenchResult {
    elapsed_sec: f64,
    attempted: usize,
    success: usize,
    upload_fail: usize,
    download_fail: usize,
    mismatch_fail: usize,
    other_fail: usize,
    route_success: Vec<usize>,
}

#[derive(Debug)]
enum TransferError {
    Upload,
    Download,
    Mismatch,
}

struct NodeGuard {
    child: Child,
}

impl Drop for NodeGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct SharedCounters {
    attempted: AtomicUsize,
    success: AtomicUsize,
    upload_fail: AtomicUsize,
    download_fail: AtomicUsize,
    mismatch_fail: AtomicUsize,
    other_fail: AtomicUsize,
    route_success: Vec<AtomicUsize>,
}

impl SharedCounters {
    fn new(route_count: usize) -> Self {
        Self {
            attempted: AtomicUsize::new(0),
            success: AtomicUsize::new(0),
            upload_fail: AtomicUsize::new(0),
            download_fail: AtomicUsize::new(0),
            mismatch_fail: AtomicUsize::new(0),
            other_fail: AtomicUsize::new(0),
            route_success: (0..route_count).map(|_| AtomicUsize::new(0)).collect(),
        }
    }
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

fn cpu_threads_default() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(128)
}

fn normalize_base_url(raw: &str) -> String {
    raw.trim_end_matches('/').to_string()
}

fn now_run_id() -> Result<u64, Box<dyn Error>> {
    let now_nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let pid = std::process::id() as u128;
    Ok((now_nanos ^ (pid << 20)) as u64)
}

fn free_tcp_port() -> Result<u16, Box<dyn Error>> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn free_udp_port() -> Result<u16, Box<dyn Error>> {
    Ok(UdpSocket::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn upload_url(base: &str, raw_upload: bool) -> String {
    if raw_upload {
        format!("{}/api/archivist/v1/data/raw", normalize_base_url(base))
    } else {
        format!("{}/api/archivist/v1/data", normalize_base_url(base))
    }
}

fn download_url(base: &str, cid: &str) -> String {
    format!(
        "{}/api/archivist/v1/data/{}/network/stream",
        normalize_base_url(base),
        cid
    )
}

fn deterministic_payload(id: usize, size: usize, salt: u64) -> Vec<u8> {
    let mut out = vec![0u8; size];
    let mut x = (id as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32_D192_ED03)
        ^ salt;
    for b in &mut out {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        *b = (x & 0xff) as u8;
    }
    out
}

async fn wait_health(client: &Client, base: &str, timeout: Duration) -> Result<(), Box<dyn Error>> {
    let url = format!("{}/health", normalize_base_url(base));
    let started = Instant::now();
    while started.elapsed() < timeout {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    Err(format!("health timeout for {}", base).into())
}

fn default_neverust_bin() -> PathBuf {
    let from_exe = env::current_exe()
        .ok()
        .and_then(|p| p.parent().and_then(Path::parent).map(|d| d.join("neverust")));
    from_exe.unwrap_or_else(|| PathBuf::from("target/release/neverust"))
}

fn start_neverust_node(
    bin: &Path,
    data_dir: &Path,
    api_port: u16,
    p2p_port: u16,
    disc_port: u16,
    backend: &str,
    fallback_peers: &str,
    log_file: &Path,
) -> Result<NodeGuard, Box<dyn Error>> {
    fs::create_dir_all(data_dir)?;
    if let Some(parent) = log_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let log = File::create(log_file)?;
    let log_err = log.try_clone()?;

    let child = Command::new(bin)
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir.as_os_str())
        .arg("--listen-port")
        .arg(p2p_port.to_string())
        .arg("--disc-port")
        .arg(disc_port.to_string())
        .arg("--api-port")
        .arg(api_port.to_string())
        .arg("--log-level")
        .arg("warn")
        .arg("--bootstrap-node")
        .arg("/ip4/127.0.0.1/tcp/1")
        .env("NEVERUST_STORAGE_BACKEND", backend)
        .env("NEVERUST_HTTP_FALLBACK_PEERS", fallback_peers)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .spawn()?;

    Ok(NodeGuard { child })
}

async fn transfer_once(client: &Client, route: &Route, payload: Vec<u8>) -> Result<(), TransferError> {
    let up_url = upload_url(&route.src_base, route.raw_upload);
    let up_resp = client
        .post(up_url)
        .header("content-type", "application/octet-stream")
        .body(payload.clone())
        .send()
        .await
        .map_err(|_| TransferError::Upload)?;

    if !up_resp.status().is_success() {
        let _ = up_resp.bytes().await;
        return Err(TransferError::Upload);
    }

    let cid = up_resp.text().await.map_err(|_| TransferError::Upload)?;
    let cid = cid.trim();
    if cid.is_empty() {
        return Err(TransferError::Upload);
    }

    let down_url = download_url(&route.dst_base, cid);
    let down_resp = client
        .get(down_url)
        .send()
        .await
        .map_err(|_| TransferError::Download)?;

    if !down_resp.status().is_success() {
        let _ = down_resp.bytes().await;
        return Err(TransferError::Download);
    }

    let bytes = down_resp.bytes().await.map_err(|_| TransferError::Download)?;
    if bytes.as_ref() != payload.as_slice() {
        return Err(TransferError::Mismatch);
    }
    Ok(())
}

async fn run_transfer_bench(
    routes: Vec<Route>,
    transfers: usize,
    concurrency: usize,
    payload_bytes: usize,
    unique_payloads: bool,
) -> Result<BenchResult, Box<dyn Error>> {
    let routes = Arc::new(routes);
    let route_count = routes.len();
    let workers = concurrency.max(1).min(transfers.max(1));
    let next_idx = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(SharedCounters::new(route_count));

    let client = Arc::new(
        Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(20))
            .pool_max_idle_per_host(workers.saturating_mul(4).max(256))
            .tcp_nodelay(true)
            .http1_only()
            .build()?,
    );

    let progress_started = Instant::now();
    let progress_stats = Arc::clone(&stats);
    let progress_done = Arc::clone(&done);
    let progress_next_idx = Arc::clone(&next_idx);
    let progress_task = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            let attempted = progress_stats.attempted.load(Ordering::Relaxed);
            let success = progress_stats.success.load(Ordering::Relaxed);
            let upload_fail = progress_stats.upload_fail.load(Ordering::Relaxed);
            let download_fail = progress_stats.download_fail.load(Ordering::Relaxed);
            let mismatch_fail = progress_stats.mismatch_fail.load(Ordering::Relaxed);
            let fail_total = upload_fail + download_fail + mismatch_fail;
            let elapsed = progress_started.elapsed().as_secs_f64().max(1e-9);
            let tps = success as f64 / elapsed;
            let claimed = progress_next_idx.load(Ordering::Relaxed).min(transfers);
            eprintln!(
                "PROGRESS elapsed={:.2}s claimed={} attempted={} success={} fail={} tps={:.2}",
                elapsed, claimed, attempted, success, fail_total, tps
            );
            if progress_done.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    let started = Instant::now();
    let mut joins = Vec::with_capacity(workers);
    for _ in 0..workers {
        let routes = Arc::clone(&routes);
        let next_idx = Arc::clone(&next_idx);
        let stats = Arc::clone(&stats);
        let client = Arc::clone(&client);
        joins.push(tokio::spawn(async move {
            loop {
                let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= transfers {
                    break;
                }
                let route_idx = idx % route_count;
                let route = &routes[route_idx];
                let payload_id = if unique_payloads { idx } else { route_idx };
                let payload = deterministic_payload(payload_id, payload_bytes, route.salt);

                stats.attempted.fetch_add(1, Ordering::Relaxed);
                match transfer_once(&client, route, payload).await {
                    Ok(()) => {
                        stats.success.fetch_add(1, Ordering::Relaxed);
                        stats.route_success[route_idx].fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TransferError::Upload) => {
                        stats.upload_fail.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TransferError::Download) => {
                        stats.download_fail.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TransferError::Mismatch) => {
                        stats.mismatch_fail.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for join in joins {
        if join.await.is_err() {
            stats.other_fail.fetch_add(1, Ordering::Relaxed);
        }
    }

    let elapsed_sec = started.elapsed().as_secs_f64().max(1e-9);
    done.store(true, Ordering::Relaxed);
    let _ = progress_task.await;

    let mut out = BenchResult {
        elapsed_sec,
        attempted: stats.attempted.load(Ordering::Relaxed),
        success: stats.success.load(Ordering::Relaxed),
        upload_fail: stats.upload_fail.load(Ordering::Relaxed),
        download_fail: stats.download_fail.load(Ordering::Relaxed),
        mismatch_fail: stats.mismatch_fail.load(Ordering::Relaxed),
        other_fail: stats.other_fail.load(Ordering::Relaxed),
        route_success: Vec::with_capacity(route_count),
    };
    for slot in &stats.route_success {
        out.route_success.push(slot.load(Ordering::Relaxed));
    }

    Ok(out)
}

fn print_result(header: &str, routes: &[Route], res: &BenchResult) {
    let tps = res.success as f64 / res.elapsed_sec;
    let reqps = (res.success as f64 * 2.0) / res.elapsed_sec;

    println!("BENCH={}", header);
    println!("ELAPSED_SEC={:.6}", res.elapsed_sec);
    println!("ATTEMPTED={}", res.attempted);
    println!("SUCCESS={}", res.success);
    println!("UPLOAD_FAIL={}", res.upload_fail);
    println!("DOWNLOAD_FAIL={}", res.download_fail);
    println!("MISMATCH_FAIL={}", res.mismatch_fail);
    println!("OTHER_FAIL={}", res.other_fail);
    println!("TRANSFER_PER_SEC={:.2}", tps);
    println!("HTTP_REQ_PER_SEC_EST={:.2}", reqps);
    for (i, route) in routes.iter().enumerate() {
        println!("ROUTE_{}_NAME={}", i, route.name);
        println!("ROUTE_{}_SUCCESS={}", i, res.route_success[i]);
    }
}

fn print_usage(bin: &str) {
    eprintln!("Usage:");
    eprintln!(
        "  {bin} n2n [transfers] [concurrency] [payload_bytes] [backend] [unique_payloads_0_or_1]\n    Starts two Neverust nodes and runs bidirectional transfer benchmark."
    );
    eprintln!(
        "  {bin} n2n-multi [pairs] [transfers_per_pair] [concurrency_per_pair] [payload_bytes] [backend] [unique_payloads_0_or_1]\n    Starts many Neverust pairs in one process and runs a combined benchmark."
    );
    eprintln!(
        "  {bin} n2a <archivist_base_url> [transfers] [concurrency] [payload_bytes] [backend] [unique_payloads_0_or_1]\n    Starts one Neverust node and tests Archivist->Neverust and Neverust->Archivist directions."
    );
    eprintln!(
        "  {bin} pair <src_base_url> <dst_base_url> [transfers] [concurrency] [payload_bytes] [unique_payloads_0_or_1]\n    One-way upload(src) + download(dst) loop."
    );
}

fn ensure_neverust_bin_exists(bin: &Path) -> Result<(), Box<dyn Error>> {
    if bin.exists() {
        return Ok(());
    }
    Err(format!(
        "Neverust binary not found at {}. Build first: cargo build --release --bin neverust",
        bin.display()
    )
    .into())
}

async fn run_n2n(args: &[String]) -> Result<(), Box<dyn Error>> {
    let transfers = parse_usize(args, 2, 20_000);
    let concurrency = parse_usize(args, 3, cpu_threads_default());
    let payload_bytes = parse_usize(args, 4, 256);
    let backend = args.get(5).map_or("deltaflat", String::as_str);
    let unique_payloads = parse_bool(args, 6, true);
    let run_id = now_run_id()?;

    let bin = default_neverust_bin();
    ensure_neverust_bin_exists(&bin)?;

    let root = PathBuf::from(format!("/tmp/neverust-transfer-qps-{}", run_id));

    println!("MODE=n2n");
    println!("RUN_ID={}", run_id);
    println!("TRANSFERS={}", transfers);
    println!("CONCURRENCY={}", concurrency);
    println!("PAYLOAD_BYTES={}", payload_bytes);
    println!("BACKEND={}", backend);
    println!("UNIQUE_PAYLOADS={}", unique_payloads);
    let health_client = Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .http1_only()
        .build()?;

    let setup = {
        let mut out = None;
        for attempt in 0..8usize {
            let api_a = free_tcp_port()?;
            let p2p_a = free_tcp_port()?;
            let disc_a = free_udp_port()?;
            let api_b = free_tcp_port()?;
            let p2p_b = free_tcp_port()?;
            let disc_b = free_udp_port()?;

            let base_a = format!("http://127.0.0.1:{}", api_a);
            let base_b = format!("http://127.0.0.1:{}", api_b);
            let data_a = root.join(format!("node-a-{}", attempt));
            let data_b = root.join(format!("node-b-{}", attempt));
            let log_a = root.join(format!("node-a-{}.log", attempt));
            let log_b = root.join(format!("node-b-{}.log", attempt));

            let mut node_a = match start_neverust_node(
                &bin, &data_a, api_a, p2p_a, disc_a, backend, &base_b, &log_a,
            ) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let mut node_b = match start_neverust_node(
                &bin, &data_b, api_b, p2p_b, disc_b, backend, &base_a, &log_b,
            ) {
                Ok(n) => n,
                Err(_) => continue,
            };

            // Fast-fail if either node exits immediately (common with transient port races).
            if node_a.child.try_wait()?.is_some() || node_b.child.try_wait()?.is_some() {
                continue;
            }

            let health_ok = wait_health(&health_client, &base_a, Duration::from_secs(10))
                .await
                .is_ok()
                && wait_health(&health_client, &base_b, Duration::from_secs(10))
                    .await
                    .is_ok();
            if health_ok {
                out = Some((node_a, node_b, base_a, base_b, log_a, log_b));
                break;
            }
        }
        out
    };
    let (_node_a, _node_b, base_a, base_b, log_a, log_b) =
        setup.ok_or("failed to start healthy n2n pair after retries")?;
    println!("NODE_A_BASE={}", base_a);
    println!("NODE_B_BASE={}", base_b);
    println!("LOG_A={}", log_a.display());
    println!("LOG_B={}", log_b.display());

    let routes = vec![
        Route {
            name: "A_TO_B".to_string(),
            src_base: base_a.clone(),
            dst_base: base_b.clone(),
            salt: 0xA1A1_A1A1_A1A1_A1A1,
            raw_upload: true,
        },
        Route {
            name: "B_TO_A".to_string(),
            src_base: base_b,
            dst_base: base_a,
            salt: 0xB2B2_B2B2_B2B2_B2B2,
            raw_upload: true,
        },
    ];
    let res = run_transfer_bench(
        routes.clone(),
        transfers,
        concurrency,
        payload_bytes,
        unique_payloads,
    )
    .await?;
    print_result("N2N_BIDIRECTIONAL", &routes, &res);
    Ok(())
}

async fn run_n2n_multi(args: &[String]) -> Result<(), Box<dyn Error>> {
    let pairs = parse_usize(args, 2, 25);
    let transfers_per_pair = parse_usize(args, 3, 20_000);
    let concurrency_per_pair = parse_usize(args, 4, cpu_threads_default());
    let payload_bytes = parse_usize(args, 5, 1);
    let backend = args.get(6).map_or("deltaflat", String::as_str);
    let unique_payloads = parse_bool(args, 7, true);
    let run_id = now_run_id()?;

    let bin = default_neverust_bin();
    ensure_neverust_bin_exists(&bin)?;

    let root = PathBuf::from(format!("/tmp/neverust-transfer-qps-multi-{}", run_id));
    fs::create_dir_all(&root)?;

    let health_client = Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .http1_only()
        .build()?;

    println!("MODE=n2n-multi");
    println!("RUN_ID={}", run_id);
    println!("PAIRS={}", pairs);
    println!("NODES={}", pairs.saturating_mul(2));
    println!("TRANSFERS_PER_PAIR={}", transfers_per_pair);
    println!("CONCURRENCY_PER_PAIR={}", concurrency_per_pair);
    println!("PAYLOAD_BYTES={}", payload_bytes);
    println!("BACKEND={}", backend);
    println!("UNIQUE_PAYLOADS={}", unique_payloads);

    let mut guards: Vec<NodeGuard> = Vec::with_capacity(pairs.saturating_mul(2));
    let mut routes: Vec<Route> = Vec::with_capacity(pairs.saturating_mul(2));

    for pair_idx in 0..pairs {
        let mut started = None;
        for attempt in 0..8usize {
            let api_a = free_tcp_port()?;
            let p2p_a = free_tcp_port()?;
            let disc_a = free_udp_port()?;
            let api_b = free_tcp_port()?;
            let p2p_b = free_tcp_port()?;
            let disc_b = free_udp_port()?;

            let base_a = format!("http://127.0.0.1:{}", api_a);
            let base_b = format!("http://127.0.0.1:{}", api_b);
            let data_a = root.join(format!("pair-{}-a-{}", pair_idx, attempt));
            let data_b = root.join(format!("pair-{}-b-{}", pair_idx, attempt));
            let log_a = root.join(format!("pair-{}-a-{}.log", pair_idx, attempt));
            let log_b = root.join(format!("pair-{}-b-{}.log", pair_idx, attempt));

            let mut node_a = match start_neverust_node(
                &bin, &data_a, api_a, p2p_a, disc_a, backend, &base_b, &log_a,
            ) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let mut node_b = match start_neverust_node(
                &bin, &data_b, api_b, p2p_b, disc_b, backend, &base_a, &log_b,
            ) {
                Ok(n) => n,
                Err(_) => continue,
            };

            if let Some(_) = node_a.child.try_wait()? {
                continue;
            }
            if let Some(_) = node_b.child.try_wait()? {
                continue;
            }

            let health_ok = wait_health(&health_client, &base_a, Duration::from_secs(10))
                .await
                .is_ok()
                && wait_health(&health_client, &base_b, Duration::from_secs(10))
                    .await
                    .is_ok();
            if health_ok {
                started = Some((node_a, node_b, base_a, base_b));
                break;
            }
        }

        let (node_a, node_b, base_a, base_b) =
            started.ok_or("failed to start healthy node pair in n2n-multi")?;
        guards.push(node_a);
        guards.push(node_b);
        routes.push(Route {
            name: format!("P{}_A_TO_B", pair_idx),
            src_base: base_a.clone(),
            dst_base: base_b.clone(),
            salt: 0xA1A1_A1A1_A1A1_A1A1 ^ (pair_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
            raw_upload: true,
        });
        routes.push(Route {
            name: format!("P{}_B_TO_A", pair_idx),
            src_base: base_b,
            dst_base: base_a,
            salt: 0xB2B2_B2B2_B2B2_B2B2 ^ (pair_idx as u64).wrapping_mul(0xD1B5_4A32_D192_ED03),
            raw_upload: true,
        });
    }

    let transfers_total = transfers_per_pair.saturating_mul(pairs);
    let concurrency_total = concurrency_per_pair.saturating_mul(pairs).max(1);
    println!("TRANSFERS_TOTAL={}", transfers_total);
    println!("CONCURRENCY_TOTAL={}", concurrency_total);

    let res = run_transfer_bench(
        routes.clone(),
        transfers_total,
        concurrency_total,
        payload_bytes,
        unique_payloads,
    )
    .await?;
    print_result("N2N_MULTI", &routes, &res);
    drop(guards);
    Ok(())
}

async fn run_n2a(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 3 {
        return Err("n2a requires <archivist_base_url>".into());
    }
    let archivist_base = normalize_base_url(&args[2]);
    let transfers = parse_usize(args, 3, 10_000);
    let concurrency = parse_usize(args, 4, cpu_threads_default());
    let payload_bytes = parse_usize(args, 5, 256);
    let backend = args.get(6).map_or("deltaflat", String::as_str);
    let unique_payloads = parse_bool(args, 7, true);
    let run_id = now_run_id()?;

    let bin = default_neverust_bin();
    ensure_neverust_bin_exists(&bin)?;

    let api_n = free_tcp_port()?;
    let p2p_n = free_tcp_port()?;
    let disc_n = free_udp_port()?;
    let neverust_base = format!("http://127.0.0.1:{}", api_n);
    let root = PathBuf::from(format!("/tmp/neverust-transfer-qps-n2a-{}", run_id));
    let data_n = root.join("node-neverust");
    let log_n = root.join("node-neverust.log");

    println!("MODE=n2a");
    println!("RUN_ID={}", run_id);
    println!("TRANSFERS={}", transfers);
    println!("CONCURRENCY={}", concurrency);
    println!("PAYLOAD_BYTES={}", payload_bytes);
    println!("BACKEND={}", backend);
    println!("UNIQUE_PAYLOADS={}", unique_payloads);
    println!("NEVERUST_BASE={}", neverust_base);
    println!("ARCHIVIST_BASE={}", archivist_base);
    println!("LOG_NEVERUST={}", log_n.display());

    let _neverust = start_neverust_node(
        &bin,
        &data_n,
        api_n,
        p2p_n,
        disc_n,
        backend,
        &archivist_base,
        &log_n,
    )?;
    let health_client = Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .http1_only()
        .build()?;
    wait_health(&health_client, &neverust_base, Duration::from_secs(60)).await?;

    let a2n = vec![Route {
        name: "ARCHIVIST_TO_NEVERUST".to_string(),
        src_base: archivist_base.clone(),
        dst_base: neverust_base.clone(),
        salt: 0xC3C3_C3C3_C3C3_C3C3,
        raw_upload: false,
    }];
    let n2a = vec![Route {
        name: "NEVERUST_TO_ARCHIVIST".to_string(),
        src_base: neverust_base,
        dst_base: archivist_base,
        salt: 0xD4D4_D4D4_D4D4_D4D4,
        raw_upload: true,
    }];

    let res_a2n = run_transfer_bench(
        a2n.clone(),
        transfers,
        concurrency,
        payload_bytes,
        unique_payloads,
    )
    .await?;
    print_result("A2N", &a2n, &res_a2n);
    let res_n2a = run_transfer_bench(
        n2a.clone(),
        transfers,
        concurrency,
        payload_bytes,
        unique_payloads,
    )
    .await?;
    print_result("N2A", &n2a, &res_n2a);
    Ok(())
}

async fn run_pair(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        return Err("pair requires <src_base_url> <dst_base_url>".into());
    }
    let src = normalize_base_url(&args[2]);
    let dst = normalize_base_url(&args[3]);
    let transfers = parse_usize(args, 4, 10_000);
    let concurrency = parse_usize(args, 5, cpu_threads_default());
    let payload_bytes = parse_usize(args, 6, 256);
    let unique_payloads = parse_bool(args, 7, true);

    println!("MODE=pair");
    println!("SRC_BASE={}", src);
    println!("DST_BASE={}", dst);
    println!("TRANSFERS={}", transfers);
    println!("CONCURRENCY={}", concurrency);
    println!("PAYLOAD_BYTES={}", payload_bytes);
    println!("UNIQUE_PAYLOADS={}", unique_payloads);

    let routes = vec![Route {
        name: "PAIR".to_string(),
        src_base: src,
        dst_base: dst,
        salt: 0xE5E5_E5E5_E5E5_E5E5,
        raw_upload: false,
    }];
    let res = run_transfer_bench(
        routes.clone(),
        transfers,
        concurrency,
        payload_bytes,
        unique_payloads,
    )
    .await?;
    print_result("PAIR_ONE_WAY", &routes, &res);
    Ok(())
}

async fn async_main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let bin = args.first().map_or("transfer_qps_bench", String::as_str);
    if args.len() < 2 {
        print_usage(bin);
        std::process::exit(2);
    }

    match args[1].as_str() {
        "n2n" => run_n2n(&args).await,
        "n2n-multi" => run_n2n_multi(&args).await,
        "n2a" => run_n2a(&args).await,
        "pair" => run_pair(&args).await,
        _ => {
            print_usage(bin);
            std::process::exit(2);
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let rt_threads = env::var("NEVERUST_BENCH_RT_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);

    if rt_threads == 1 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async_main())
    } else {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(rt_threads)
            .enable_all()
            .build()?;
        rt.block_on(async_main())
    }
}
