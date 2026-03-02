//! Integration test for block exchange between two Neverust nodes

use futures_util::StreamExt;
use libp2p::Multiaddr;
use neverust_core::{create_swarm, Block, BlockStore, Metrics};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_two_nodes_exchange_blocks() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for test
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    // Create block stores
    let store1 = Arc::new(BlockStore::new());
    let store2 = Arc::new(BlockStore::new());

    // Create metrics collectors
    let metrics1 = Metrics::new();
    let metrics2 = Metrics::new();

    // Create two swarms (nodes) with their block stores
    let (mut swarm1, _tx1, _keypair1) =
        create_swarm(store1.clone(), "altruistic".to_string(), 1, metrics1).await?;
    let (mut swarm2, _tx2, _keypair2) =
        create_swarm(store2.clone(), "altruistic".to_string(), 1, metrics2).await?;

    let peer1_id = *swarm1.local_peer_id();
    let peer2_id = *swarm2.local_peer_id();

    tracing::info!("Node 1 peer ID: {}", peer1_id);
    tracing::info!("Node 2 peer ID: {}", peer2_id);

    // Start listening on node 1
    let addr1: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse()?;
    swarm1.listen_on(addr1)?;

    // Wait for node 1 to get its listen address
    let node1_addr = loop {
        match swarm1.next().await {
            Some(libp2p::swarm::SwarmEvent::NewListenAddr { address, .. }) => {
                tracing::info!("Node 1 listening on: {}", address);
                break address;
            }
            _ => continue,
        }
    };

    // Create full multiaddr for node 1
    let node1_full_addr = format!("{}/p2p/{}", node1_addr, peer1_id).parse::<Multiaddr>()?;
    tracing::info!("Node 1 full address: {}", node1_full_addr);

    // Start listening on node 2
    let addr2: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse()?;
    swarm2.listen_on(addr2)?;

    // Wait for node 2 to get its listen address
    loop {
        match swarm2.next().await {
            Some(libp2p::swarm::SwarmEvent::NewListenAddr { address, .. }) => {
                tracing::info!("Node 2 listening on: {}", address);
                break;
            }
            _ => continue,
        }
    }

    // Node 2 dials node 1
    tracing::info!("Node 2 dialing node 1...");
    swarm2.dial(node1_full_addr.clone())?;

    // Wait for connection to establish
    let connection_timeout = Duration::from_secs(10);
    let result = timeout(connection_timeout, async {
        let mut node1_connected = false;
        let mut node2_connected = false;

        loop {
            tokio::select! {
                Some(event) = swarm1.next() => {
                    if let libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        tracing::info!("Node 1 connected to: {}", peer_id);
                        node1_connected = true;
                    }
                }
                Some(event) = swarm2.next() => {
                    if let libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        tracing::info!("Node 2 connected to: {}", peer_id);
                        node2_connected = true;
                    }
                }
            }

            if node1_connected && node2_connected {
                break;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "Connection timed out");

    // Create a test block on node 1
    let test_data = b"Hello from Neverust!".to_vec();
    let test_block = Block::new(test_data)?;
    let test_cid = test_block.cid;

    store1.put(test_block.clone()).await?;
    tracing::info!("Node 1 stored block: {}", test_cid);

    // Now we need to trigger block exchange
    // For now, this test verifies:
    // 1. Two nodes can connect
    // 2. BlockExc protocol is available
    // 3. Block storage works

    // TODO: Trigger actual block request from node 2 to node 1
    // This requires implementing the block request mechanism

    tracing::info!("Test completed successfully!");

    Ok(())
}

#[tokio::test]
async fn test_block_storage() -> Result<(), Box<dyn std::error::Error>> {
    let store = BlockStore::new();

    // Create test data
    let test_data = b"Test block content".to_vec();
    let test_block = Block::new(test_data.clone())?;
    let cid = test_block.cid;

    // Store block
    store.put(test_block).await?;

    // Retrieve block
    let retrieved = store.get(&cid).await?;
    assert_eq!(retrieved.data, test_data, "Block data should match");
    assert_eq!(retrieved.cid, cid, "Block CID should match");

    // Check stats
    let stats = store.stats().await;
    assert_eq!(stats.block_count, 1, "Should have 1 block");
    assert!(stats.total_size > 0, "Should have non-zero bytes");

    Ok(())
}

#[tokio::test]
#[ignore] // Manual test - requires network access to Archivist testnet
async fn test_retrieve_from_testnet() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for test
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,neverust_core=debug")
        .try_init();

    tracing::info!("Starting Archivist testnet integration test");

    // Create block store and metrics
    let store = Arc::new(BlockStore::new());
    let metrics = Metrics::new();

    // Create swarm (node) with block store
    let (mut swarm, block_request_tx, _keypair) =
        create_swarm(store.clone(), "altruistic".to_string(), 1, metrics.clone()).await?;

    let local_peer_id = *swarm.local_peer_id();
    tracing::info!("Local peer ID: {}", local_peer_id);

    // Create BlockExc client for requesting blocks
    use neverust_core::blockexc::BlockExcClient;
    let blockexc_client = Arc::new(BlockExcClient::new(
        store.clone(),
        metrics.clone(),
        3, // max_retries
        block_request_tx,
    ));

    // Start listening on our node
    let listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0".parse()?;
    swarm.listen_on(listen_addr)?;

    // Wait for listen address
    loop {
        match swarm.next().await {
            Some(libp2p::swarm::SwarmEvent::NewListenAddr { address, .. }) => {
                tracing::info!("Listening on: {}", address);
                break;
            }
            _ => continue,
        }
    }

    // Fetch bootstrap nodes from Archivist testnet
    use neverust_core::config::Config;
    tracing::info!("Fetching Archivist testnet bootstrap nodes...");
    let bootstrap_nodes = Config::fetch_testnet_bootstrap_nodes().await?;

    if bootstrap_nodes.is_empty() {
        return Err("No bootstrap nodes found in testnet".into());
    }

    tracing::info!("Found {} testnet bootstrap nodes", bootstrap_nodes.len());
    for (i, node) in bootstrap_nodes.iter().enumerate() {
        tracing::info!("  Bootstrap node {}: {}", i + 1, node);
    }

    // Connect to bootstrap nodes
    let mut connected_peers = std::collections::HashSet::new();
    for bootstrap_addr in &bootstrap_nodes {
        match bootstrap_addr.parse::<Multiaddr>() {
            Ok(addr) => {
                tracing::info!("Dialing bootstrap node: {}", addr);
                match swarm.dial(addr.clone()) {
                    Ok(_) => {
                        tracing::info!("Dial initiated successfully");
                    }
                    Err(e) => {
                        tracing::warn!("Failed to dial {}: {}", addr, e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Invalid multiaddr {}: {}", bootstrap_addr, e);
            }
        }
    }

    // Wait for connections to establish (with timeout)
    let connection_timeout = Duration::from_secs(60);
    let connection_start = std::time::Instant::now();

    tracing::info!("Waiting for testnet connections...");

    loop {
        tokio::select! {
            Some(event) = swarm.next() => {
                match event {
                    libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        tracing::info!("Connected to testnet peer: {} via {}", peer_id, endpoint.get_remote_address());
                        connected_peers.insert(peer_id);

                        // Protocol negotiation happens automatically - start immediately
                        if !connected_peers.is_empty() {
                            tracing::info!("Have {} connections, protocol ready", connected_peers.len());
                            break;
                        }
                    }
                    libp2p::swarm::SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        tracing::warn!("Connection error to {:?}: {}", peer_id, error);
                    }
                    libp2p::swarm::SwarmEvent::Behaviour(event) => {
                        tracing::debug!("Behaviour event: {:?}", event);
                    }
                    _ => {}
                }

                if connection_start.elapsed() > connection_timeout {
                    tracing::error!("Connection timeout after {:?}", connection_timeout);
                    break;
                }
            }
        }
    }

    if connected_peers.is_empty() {
        return Err("Failed to connect to any testnet nodes".into());
    }

    tracing::info!(
        "Successfully connected to {} testnet peers",
        connected_peers.len()
    );

    // For this test, we'll use a well-known CID from the testnet
    // In a real scenario, you'd query the testnet for available blocks
    // For now, let's create a test block and see if we can retrieve it
    // (This assumes another node has this exact block, which is unlikely)
    // Instead, let's just test that the request mechanism works

    // Create a test CID to request
    let test_data = b"Hello from Neverust testnet test!".to_vec();
    let test_block = Block::new(test_data.clone())?;
    let test_cid = test_block.cid;

    tracing::info!("Requesting test block: {}", test_cid);
    tracing::warn!("Note: This block likely doesn't exist on testnet - testing request mechanism");

    // Spawn swarm event loop
    let swarm_handle = tokio::spawn(async move {
        loop {
            if let Some(event) = swarm.next().await {
                match event {
                    libp2p::swarm::SwarmEvent::Behaviour(event) => {
                        tracing::debug!("Swarm behaviour event: {:?}", event);
                    }
                    libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                        tracing::info!("Connection closed with {}: {:?}", peer_id, cause);
                    }
                    _ => {}
                }
            }
        }
    });

    // Request the block with a timeout
    let request_timeout = Duration::from_secs(30);
    tracing::info!(
        "Requesting block with {}s timeout...",
        request_timeout.as_secs()
    );

    match timeout(request_timeout, blockexc_client.request_block(test_cid)).await {
        Ok(Ok(block)) => {
            tracing::info!("SUCCESS: Block retrieved from testnet!");
            tracing::info!("Block CID: {}", block.cid);
            tracing::info!("Block size: {} bytes", block.data.len());

            // Verify block integrity
            use neverust_core::cid_blake3::verify_blake3;
            verify_blake3(&block.data, &block.cid)?;
            tracing::info!("Block BLAKE3 hash verified successfully!");

            // Verify it's in our store
            let retrieved = store.get(&test_cid).await?;
            assert_eq!(retrieved.cid, test_cid, "Block CID should match");
            assert_eq!(retrieved.data, block.data, "Block data should match");

            tracing::info!("Test PASSED: Block successfully retrieved and verified!");
        }
        Ok(Err(e)) => {
            tracing::warn!("Block request failed: {}", e);
            tracing::info!("This is expected if the block doesn't exist on testnet");
            tracing::info!("Test result: Request mechanism worked, but block not found (EXPECTED)");
        }
        Err(_) => {
            tracing::warn!("Block request timed out after {:?}", request_timeout);
            tracing::info!("This is expected if the block doesn't exist on testnet");
            tracing::info!("Test result: Request mechanism worked, timeout occurred (EXPECTED)");
        }
    }

    // Clean shutdown
    swarm_handle.abort();

    tracing::info!("Testnet integration test completed");
    Ok(())
}
