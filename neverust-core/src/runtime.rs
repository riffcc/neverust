//! Async runtime and event loop for the node
//!
//! Handles the main event loop, processing Swarm events and managing
//! the lifecycle of the P2P node.

use crate::{
    api,
    blockexc::BlockExcClient,
    botg::{BoTgConfig, BoTgProtocol},
    config::Config,
    discovery::Discovery,
    metrics::Metrics,
    p2p::{create_swarm, P2PError},
    storage::BlockStore,
    traffic,
};
use futures::StreamExt;
use libp2p::{swarm::SwarmEvent, Multiaddr};
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info, warn};

/// Run the Archivist node with the given configuration
pub async fn run_node(config: Config) -> Result<(), P2PError> {
    // Create block store with persistent redb backend
    let blocks_path = config.data_dir.join("blocks");
    let block_store = Arc::new(
        BlockStore::new_with_path(&blocks_path)
            .map_err(|e| P2PError::Swarm(format!("Failed to open block store: {}", e)))?,
    );
    info!("Initialized persistent block store at {:?}", blocks_path);

    // Create metrics collector
    let metrics = Metrics::new();
    info!("Initialized metrics collector");

    // Create swarm first to get peer ID (pass metrics for P2P traffic tracking)
    let (mut swarm, block_request_tx, keypair) = create_swarm(
        block_store.clone(),
        config.mode.clone(),
        config.price_per_byte,
        metrics.clone(),
    )
    .await?;
    let peer_id = swarm.local_peer_id().to_string();

    // Initialize BlockExc client for requesting blocks from peers (via channel to swarm)
    let _blockexc_client = Arc::new(BlockExcClient::new(
        block_store.clone(),
        metrics.clone(),
        3, // max_retries
        block_request_tx,
    ));
    info!("Initialized BlockExc client with 3 max retries");

    // Initialize BoTG (Block-over-TGP) protocol for high-speed block exchange
    // Use disc_port + 1 for BoTG since DiscV5 uses disc_port (8090)
    let botg_port = config.disc_port + 1;
    info!(
        "Initializing BoTG protocol on UDP port {}",
        botg_port
    );
    let botg_config = BoTgConfig {
        local_peer_id: rand::random(), // Generate random peer ID for TGP
        epoch: 0,
        ..Default::default()
    };

    let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{}", botg_port)
        .parse()
        .map_err(|e| P2PError::Transport(format!("Invalid bind address: {}", e)))?;

    // Create UDP socket for BoTG
    let udp_socket = Arc::new(
        tokio::net::UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| P2PError::Transport(format!("Failed to bind UDP socket: {}", e)))?,
    );
    info!("BoTG: UDP socket bound to {}", bind_addr);

    // Create BoTG protocol and configure it
    let mut botg_protocol = BoTgProtocol::new(botg_config);
    botg_protocol.set_udp_socket(udp_socket);
    botg_protocol.set_block_store(block_store.clone());
    botg_protocol.set_metrics(metrics.clone());

    let botg = Arc::new(botg_protocol);

    // Start BoTG receive loop
    botg.clone().start_receive_loop();
    info!("BoTG ready for high-speed block exchange via UDP");

    // Initialize DiscV5 peer discovery on the main discovery port
    let discv5_addr: std::net::SocketAddr = format!("0.0.0.0:{}", config.disc_port)
        .parse()
        .map_err(|e| P2PError::Transport(format!("Invalid DiscV5 address: {}", e)))?;

    info!("Initializing DiscV5 peer discovery on UDP port {}", config.disc_port);

    // Fetch bootstrap ENRs for DiscV5
    let bootstrap_enrs = Config::fetch_bootstrap_enrs()
        .await
        .map_err(|e| P2PError::Transport(format!("Failed to fetch bootstrap ENRs: {}", e)))?;

    // Get announce addresses for this node (DiscV5 on disc_port, libp2p on listen_port)
    let announce_addrs = vec![
        format!("/ip4/0.0.0.0/tcp/{}", config.listen_port), // libp2p TCP
        format!("/ip4/0.0.0.0/udp/{}", config.disc_port),  // DiscV5 UDP
    ];

    let discovery = match Discovery::new(&keypair, discv5_addr, announce_addrs, bootstrap_enrs).await {
        Ok(disc) => {
            info!("DiscV5 initialized successfully on {}", discv5_addr);
            Arc::new(disc)
        }
        Err(e) => {
            warn!("Failed to initialize DiscV5: {}. Continuing without peer discovery.", e);
            warn!("This is expected if you're using secp256k1 keys - DiscV5 is optional.");
            // Continue without DiscV5 - we can still use manual bootstrap nodes
            // Return early to avoid starting the discovery event loop
            // (We'll use a None/Option pattern in future iterations)
            // For now, we just log and continue
            info!("Continuing without DiscV5 peer discovery");
            // Create a dummy Arc to satisfy the type system (won't be used)
            // In future we'll make this Option<Arc<Discovery>>
            Arc::new(Discovery::new(&keypair, discv5_addr, vec![], vec![]).await.unwrap_or_else(|_| {
                panic!("Critical: Could not initialize DiscV5 even with empty config")
            }))
        }
    };

    // Start DiscV5 event loop in background
    tokio::spawn({
        let discovery = discovery.clone();
        async move {
            info!("Starting DiscV5 event loop");
            discovery.run().await;
        }
    });

    // Add peers to BoTG for P2P communication (Docker network autodiscovery)
    tokio::spawn({
        let botg = botg.clone();
        async move {
            // Wait for nodes to start
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            // Generate all 50 Docker network peers
            let mut docker_peers = vec![
                "172.25.0.10:8090".to_string(), // bootstrap
            ];

            // Add node1-49 (172.25.1.2 through 172.25.1.50)
            for i in 1..=49 {
                docker_peers.push(format!("172.25.1.{}:8090", i + 1));
            }

            let mut added = 0;
            for peer_str in &docker_peers {
                if let Ok(peer_addr) = peer_str.parse() {
                    botg.add_peer(peer_addr).await;
                    added += 1;
                }
            }
            info!(
                "BoTG: Added {} Docker network peers for P2P communication",
                added
            );
        }
    });

    // Prepare listen addresses collection (will be populated as we receive NewListenAddr events)
    let listen_addrs = Arc::new(std::sync::RwLock::new(Vec::new()));

    // Start REST API server in background with peer ID and BoTG
    let api_block_store = block_store.clone();
    let api_metrics = metrics.clone();
    let api_peer_id = peer_id.clone();
    let api_botg = botg.clone();
    let api_keypair = Arc::new(keypair);
    let api_listen_addrs = listen_addrs.clone();
    let api_port = config.api_port;
    tokio::spawn(async move {
        let app = api::create_router(
            api_block_store,
            api_metrics,
            api_peer_id,
            api_botg,
            api_keypair,
            api_listen_addrs,
        );
        let addr = format!("0.0.0.0:{}", api_port);
        info!("Starting REST API on {}", addr);

        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // Start autonomous traffic generator if enabled
    if traffic::is_enabled() {
        info!("Traffic generator enabled - starting autonomous P2P traffic");
        let traffic_config = traffic::config_from_env(peer_id.clone(), config.api_port);
        let traffic_store = block_store.clone();
        let traffic_botg = botg.clone();

        // Create P2P command channel for traffic generator
        let (p2p_tx, mut p2p_rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            traffic::start_traffic_generator(traffic_config, traffic_store, traffic_botg, p2p_tx)
                .await;
        });

        // Handle P2P commands from traffic generator
        let cmd_botg = botg.clone();
        tokio::spawn(async move {
            while let Some(cmd) = p2p_rx.recv().await {
                match cmd {
                    traffic::P2PCommand::RequestBlock(cid) => {
                        info!("[P2P-BoTG] Requesting block {} via BoTG/TGP", cid);
                        cmd_botg.request_blocks_by_cid(vec![cid]).await;
                    }
                    traffic::P2PCommand::AdvertiseBlock(cid) => {
                        info!("[P2P-BoTG] Advertising block {} via BoTG/TGP", cid);
                        cmd_botg.announce_blocks(vec![cid]).await;
                    }
                }
            }
        });
    }

    // Start listening on TCP (Archivist uses TCP+Noise+Mplex, NOT QUIC)
    let tcp_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", config.listen_port)
        .parse()
        .map_err(|e| P2PError::Transport(format!("Invalid TCP address: {}", e)))?;

    swarm
        .listen_on(tcp_addr.clone())
        .map_err(|e| P2PError::Transport(format!("Failed to listen on TCP {}: {}", tcp_addr, e)))?;

    info!("Node started with peer ID: {}", swarm.local_peer_id());

    // Fetch bootstrap nodes early
    let bootstrap_addrs = if config.bootstrap_nodes.is_empty() {
        info!("No bootstrap nodes configured, fetching...");
        Config::fetch_bootstrap_nodes()
            .await
            .map_err(|e| P2PError::Transport(format!("Failed to fetch bootstrap nodes: {}", e)))?
    } else {
        config.bootstrap_nodes.clone()
    };

    // Track if we've established listen addresses
    let mut tcp_listening = false;
    let mut bootstrapped = false;

    // Main event loop
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        // Track transport types
                        if address.to_string().contains("/tcp/") {
                            info!("Listening on TCP: {}", address);
                            tcp_listening = true;
                        } else {
                            info!("Listening on {}", address);
                        }

                        // Add to listen addresses collection
                        listen_addrs.write().unwrap().push(address.clone());

                        // Once TCP is listening, dial bootstrap nodes
                        if tcp_listening && !bootstrapped {
                            info!("TCP transport ready, dialing bootstrap nodes...");

                            // Dial all bootstrap peers directly
                            // (Archivist doesn't use Kademlia - uses custom BlockExc protocol)
                            for node_addr in &bootstrap_addrs {
                                info!("Dialing bootstrap: {}", node_addr);
                                if let Ok(addr) = node_addr.parse::<Multiaddr>() {
                                    if let Err(e) = swarm.dial(addr.clone()) {
                                        error!("Failed to dial bootstrap peer {}: {:?}", node_addr, e);
                                    } else {
                                        info!("Dialing {}", node_addr);
                                    }
                                } else {
                                    warn!("Invalid bootstrap address: {}", node_addr);
                                }
                            }

                            bootstrapped = true;
                        }
                    }
                    SwarmEvent::ConnectionEstablished {
                        peer_id,
                        endpoint,
                        ..
                    } => {
                        info!(
                            "Connected to peer: {} at {}",
                            peer_id,
                            endpoint.get_remote_address()
                        );
                        metrics.peer_connected();
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        cause,
                        ..
                    } => {
                        // Idle connection timeouts are expected when neither side has blocks to exchange
                        // Only warn on unexpected disconnect reasons
                        if let Some(ref error) = cause {
                            if error.to_string().contains("UnexpectedEof") {
                                info!("Connection closed with {} (idle timeout)", peer_id);
                            } else {
                                warn!("Connection closed with {}: {:?}", peer_id, cause);
                            }
                        } else {
                            info!("Connection gracefully closed with {}", peer_id);
                        }
                        metrics.peer_disconnected();
                    }
                    SwarmEvent::Behaviour(event) => {
                        use crate::p2p::BehaviourEvent;
                        match event {
                            BehaviourEvent::BlockExc(blockexc_event) => {
                                use crate::blockexc::BlockExcToBehaviour;

                                match blockexc_event {
                                    BlockExcToBehaviour::BlockReceived { cid, data } => {
                                        info!(
                                            "Block received via BlockExc: {} ({} bytes)",
                                            cid,
                                            data.len()
                                        );

                                        // Block is automatically stored by BlockExcBehaviour
                                        // Pending request is automatically completed by BlockExcBehaviour
                                        // Just track metrics here
                                        metrics.block_received(data.len());
                                    }
                                    BlockExcToBehaviour::BlockPresence { cid, has_block } => {
                                        info!(
                                            "Block presence notification: {} - {}",
                                            cid,
                                            if has_block { "available" } else { "not available" }
                                        );
                                        // Future enhancement: track which peers have which blocks
                                        // for smarter routing and retry logic
                                    }
                                }
                            }
                            BehaviourEvent::Identify(identify_event) => {
                                use libp2p::identify::Event;
                                match *identify_event {
                                    Event::Received { peer_id, info, .. } => {
                                        info!(
                                            "Identified peer {}: protocol_version={}, agent_version={}",
                                            peer_id, info.protocol_version, info.agent_version
                                        );

                                        // Log supported protocols
                                        info!("Peer {} protocols: {:?}", peer_id, info.protocols);
                                    }
                                    Event::Sent { peer_id, .. } => {
                                        info!("Sent identify info to {}", peer_id);
                                    }
                                    Event::Pushed { peer_id, .. } => {
                                        info!("Pushed identify update to {}", peer_id);
                                    }
                                    Event::Error { peer_id, error, .. } => {
                                        warn!("Identify error with {}: {}", peer_id, error);
                                    }
                                }
                            }
                        }
                    }
                    SwarmEvent::IncomingConnection { local_addr, send_back_addr, .. } => {
                        info!("Incoming connection from {} on {}", send_back_addr, local_addr);
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        error!("Outgoing connection error to {:?}: {}", peer_id, error);
                    }
                    SwarmEvent::IncomingConnectionError { local_addr, send_back_addr, error, .. } => {
                        error!("Incoming connection error from {} on {}: {}", send_back_addr, local_addr, error);
                    }
                    _ => {}
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received Ctrl+C, shutting down...");
                break;
            }
        }
    }

    info!("Node stopped");
    Ok(())
}
