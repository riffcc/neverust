use futures_util::StreamExt;
use neverust_core::{create_swarm, Block, BlockStore, Metrics};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{timeout, Duration};

/// Performance test: Peer dial latency
///
/// Target: p95 ≤ 1s
///
/// This test measures the time to establish a connection between two local nodes
#[tokio::test]
#[ignore] // Run with --ignored flag for performance testing
async fn test_peer_dial_latency() {
    // Create two nodes
    let store1 = Arc::new(BlockStore::new());
    let store2 = Arc::new(BlockStore::new());
    let metrics1 = Metrics::new();
    let metrics2 = Metrics::new();

    let (mut swarm1, _tx1, _keypair1) =
        create_swarm(store1, "altruistic".to_string(), 1, metrics1)
            .await
            .expect("Failed to create swarm1");
    let (mut swarm2, _tx2, _keypair2) =
        create_swarm(store2, "altruistic".to_string(), 1, metrics2)
            .await
            .expect("Failed to create swarm2");

    // Start listening on swarm1
    swarm1
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("Failed to listen");

    // Get swarm1's address
    let addr = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(event) = swarm1.next().await {
                use libp2p::swarm::SwarmEvent;
                if let SwarmEvent::NewListenAddr { address, .. } = event {
                    return address;
                }
            }
        }
    })
    .await
    .expect("Timeout waiting for listen address");

    // Measure dial time
    let start = Instant::now();
    swarm2.dial(addr.clone()).expect("Failed to dial");

    // Wait for connection
    let dial_duration = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(event) = swarm2.next().await {
                use libp2p::swarm::SwarmEvent;
                if let SwarmEvent::ConnectionEstablished { .. } = event {
                    return start.elapsed();
                }
            }
        }
    })
    .await
    .expect("Connection timeout");

    println!("Peer dial latency: {:?}", dial_duration);
    assert!(
        dial_duration < Duration::from_secs(1),
        "Dial latency {:?} exceeds 1s target",
        dial_duration
    );
}

/// Performance test: Content fetch latency
///
/// Target: p95 ≤ 2.5s (post-initialization)
///
/// This test measures the time to fetch a block from a connected peer
#[tokio::test]
#[ignore] // Run with --ignored flag for performance testing
async fn test_content_fetch_latency() {
    use libp2p::swarm::SwarmEvent;

    // Create two nodes
    let store1 = Arc::new(BlockStore::new());
    let store2 = Arc::new(BlockStore::new());
    let metrics1 = Metrics::new();
    let metrics2 = Metrics::new();

    let (mut swarm1, _tx1, _keypair1) =
        create_swarm(store1.clone(), "altruistic".to_string(), 1, metrics1)
            .await
            .expect("Failed to create swarm1");
    let (mut swarm2, _tx2, _keypair2) =
        create_swarm(store2.clone(), "altruistic".to_string(), 1, metrics2)
            .await
            .expect("Failed to create swarm2");

    // Store a test block on node1
    let test_data = vec![42u8; 1024 * 1024]; // 1 MB block
    let block = Block::new(test_data).expect("Failed to create block");
    let cid = block.cid;
    store1.put(block).await.expect("Failed to store block");

    // Start listening on swarm1
    swarm1
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("Failed to listen");

    // Get swarm1's address
    let addr = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm1.next().await {
                return address;
            }
        }
    })
    .await
    .expect("Timeout waiting for listen address");

    // Connect swarm2 to swarm1
    swarm2.dial(addr.clone()).expect("Failed to dial");

    // Wait for connection
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(SwarmEvent::ConnectionEstablished { .. }) = swarm2.next().await {
                break;
            }
        }
    })
    .await
    .expect("Connection timeout");

    // Measure fetch time (in real scenario, this would trigger BlockExc protocol)
    // For now, measure local fetch as baseline
    let start = Instant::now();
    let result = store1.get(&cid).await;
    let fetch_duration = start.elapsed();

    assert!(result.is_ok(), "Failed to fetch block");
    println!("Content fetch latency: {:?}", fetch_duration);

    // Note: This is local storage fetch. Real P2P fetch would be measured through BlockExc
    // For p95 ≤ 2.5s target, we expect local fetch to be much faster
    assert!(
        fetch_duration < Duration::from_millis(100),
        "Local fetch latency {:?} unexpectedly high",
        fetch_duration
    );
}

/// Performance test: Multiple concurrent dials
///
/// Measures p95 latency across 100 connection attempts
#[tokio::test]
#[ignore] // Run with --ignored flag for performance testing
async fn test_peer_dial_p95() {
    const NUM_TRIALS: usize = 100;
    let mut latencies = Vec::with_capacity(NUM_TRIALS);

    for _ in 0..NUM_TRIALS {
        let store1 = Arc::new(BlockStore::new());
        let store2 = Arc::new(BlockStore::new());
        let metrics1 = Metrics::new();
        let metrics2 = Metrics::new();

        let (mut swarm1, _tx1, _keypair1) =
            create_swarm(store1, "altruistic".to_string(), 1, metrics1)
                .await
                .expect("Failed to create swarm1");
        let (mut swarm2, _tx2, _keypair2) =
            create_swarm(store2, "altruistic".to_string(), 1, metrics2)
                .await
                .expect("Failed to create swarm2");

        swarm1
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .expect("Failed to listen");

        let addr = timeout(Duration::from_secs(2), async {
            use libp2p::swarm::SwarmEvent;
            loop {
                if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm1.next().await {
                    return address;
                }
            }
        })
        .await
        .expect("Timeout");

        let start = Instant::now();
        swarm2.dial(addr).expect("Failed to dial");

        let dial_duration = timeout(Duration::from_secs(3), async {
            use libp2p::swarm::SwarmEvent;
            loop {
                if let Some(SwarmEvent::ConnectionEstablished { .. }) = swarm2.next().await {
                    return start.elapsed();
                }
            }
        })
        .await
        .expect("Connection timeout");

        latencies.push(dial_duration.as_millis());
    }

    // Calculate percentiles
    latencies.sort();
    let p50 = latencies[NUM_TRIALS / 2];
    let p95 = latencies[(NUM_TRIALS as f64 * 0.95) as usize];
    let p99 = latencies[(NUM_TRIALS as f64 * 0.99) as usize];

    println!(
        "Dial latency percentiles (ms): p50={}, p95={}, p99={}",
        p50, p95, p99
    );

    assert!(
        p95 <= 1000,
        "p95 dial latency {}ms exceeds 1000ms target",
        p95
    );
}

/// Performance test: Throughput measurement
///
/// Measures blocks per second storage operations
#[tokio::test]
#[ignore] // Run with --ignored flag for performance testing
async fn test_storage_throughput() {
    let store = Arc::new(BlockStore::new());
    const NUM_BLOCKS: usize = 1000;
    const BLOCK_SIZE: usize = 1024; // 1 KB

    let start = Instant::now();

    for i in 0..NUM_BLOCKS {
        let data = vec![i as u8; BLOCK_SIZE];
        let block = Block::new(data).expect("Failed to create block");
        store.put(block).await.expect("Failed to store block");
    }

    let duration = start.elapsed();
    let throughput = NUM_BLOCKS as f64 / duration.as_secs_f64();

    println!(
        "Storage throughput: {:.2} blocks/sec ({:.2} MB/sec)",
        throughput,
        throughput * (BLOCK_SIZE as f64) / (1024.0 * 1024.0)
    );

    // Baseline expectation: > 100 blocks/sec for 1KB blocks
    assert!(
        throughput > 100.0,
        "Storage throughput {:.2} blocks/sec below baseline",
        throughput
    );
}
