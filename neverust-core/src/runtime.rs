//! Async runtime and event loop for the node
//!
//! Handles the main event loop, processing Swarm events and managing
//! the lifecycle of the P2P node.

use crate::{
    api,
    blockexc::BlockExcClient,
    botg::{BoTgConfig, BoTgProtocol},
    citadel::{fetch_flagship_trust_snapshot, DefederationGuardConfig, DefederationNode},
    citadel_sync::{configured_citadel_mesh_sync, spawn_citadel_mesh_sync},
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
use tokio::sync::RwLock as AsyncRwLock;
use tracing::{error, info, warn};

fn derive_citadel_host_id(config: &Config) -> u8 {
    if let Some(host_id) = config.citadel_host_id {
        return host_id;
    }
    if let Some(host_id) = std::env::var("NEVERUST_CITADEL_HOST_ID")
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
    {
        return host_id;
    }
    let host_name = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
    blake3::hash(host_name.as_bytes()).as_bytes()[0]
}

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

    // Optional Citadel/Lens mode for defederation modeling and local control-plane APIs.
    let citadel_node: Option<Arc<AsyncRwLock<DefederationNode>>> = if config.citadel_mode {
        let mut trusted = std::collections::HashSet::new();
        for origin in &config.citadel_trusted_origins {
            trusted.insert(*origin);
        }
        let guard = DefederationGuardConfig {
            base_pow_bits: config.citadel_pow_bits,
            trusted_pow_bits: config.citadel_trusted_pow_bits,
            max_ops_per_origin_per_round: config.citadel_max_ops_per_origin_per_round,
            max_new_origins_per_host_per_round: config
                .citadel_max_new_origins_per_host_per_round,
            max_pending_per_origin: 512,
        };
        let node_id = if config.citadel_node_id == 0 {
            let digest = blake3::hash(peer_id.as_bytes());
            let bytes = digest.as_bytes();
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        } else {
            config.citadel_node_id
        };
        let host_id = derive_citadel_host_id(&config);
        let mut node =
            DefederationNode::new(node_id, host_id, config.citadel_site_id, trusted, guard);
        node.set_idle_bandwidth_bytes_per_sec(
            config.citadel_idle_bandwidth_kib.saturating_mul(1024),
        );
        if let Some(ref url) = config.citadel_flagship_url {
            match fetch_flagship_trust_snapshot(url).await {
                Ok(snapshot) => {
                    for origin in snapshot.trusted_origins {
                        node.trust_origin(origin, true);
                    }
                    for site in snapshot.bootstrap_sites {
                        node.emit_local_follow(site, true);
                    }
                    info!("Citadel: loaded initial flagship trust snapshot from {}", url);
                }
                Err(e) => {
                    warn!("Citadel: failed to load flagship trust snapshot {}: {}", url, e);
                }
            }
        }
        info!(
            "Citadel mode enabled: node_id={}, host_id={}, site_id={}, idle_cap={}KiB/s",
            node_id, host_id, config.citadel_site_id, config.citadel_idle_bandwidth_kib
        );
        Some(Arc::new(AsyncRwLock::new(node)))
    } else {
        None
    };

    if let (Some(citadel), Some(flagship_url)) =
        (citadel_node.clone(), config.citadel_flagship_url.clone())
    {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                tick.tick().await;
                match fetch_flagship_trust_snapshot(&flagship_url).await {
                    Ok(snapshot) => {
                        let mut node = citadel.write().await;
                        for origin in snapshot.trusted_origins {
                            node.trust_origin(origin, true);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Citadel: periodic flagship refresh failed ({}): {}",
                            flagship_url, e
                        );
                    }
                }
            }
        });
    }

    if let Some(citadel) = citadel_node.clone() {
        if let Some(sync_cfg) = configured_citadel_mesh_sync(config.api_port) {
            spawn_citadel_mesh_sync(citadel, sync_cfg);
        } else {
            info!("Citadel sync peers not configured; running local-only Citadel state");
        }
    }

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
    info!("Initializing BoTG protocol on UDP port {}", botg_port);
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

    info!(
        "Initializing DiscV5 peer discovery on UDP port {}",
        config.disc_port
    );

    // Fetch bootstrap ENRs for DiscV5
    let bootstrap_enrs = Config::fetch_bootstrap_enrs()
        .await
        .map_err(|e| P2PError::Transport(format!("Failed to fetch bootstrap ENRs: {}", e)))?;

    // Get announce addresses for this node (DiscV5 on disc_port, libp2p on listen_port)
    let announce_addrs = vec![
        format!("/ip4/0.0.0.0/tcp/{}", config.listen_port), // libp2p TCP
        format!("/ip4/0.0.0.0/udp/{}", config.disc_port),   // DiscV5 UDP
    ];

    let discovery =
        match Discovery::new(&keypair, discv5_addr, announce_addrs, bootstrap_enrs).await {
            Ok(disc) => {
                info!("DiscV5 initialized successfully on {}", discv5_addr);
                Some(Arc::new(disc))
            }
            Err(e) => {
                warn!(
                    "Failed to initialize DiscV5: {}. Continuing without peer discovery.",
                    e
                );
                None
            }
        };

    // Start DiscV5 event loop in background when discovery is available.
    if let Some(discovery) = discovery {
        tokio::spawn(async move {
            info!("Starting DiscV5 event loop");
            discovery.run().await;
        });
    }

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
    let api_citadel = citadel_node.clone();
    tokio::spawn(async move {
        let app = api::create_router_with_citadel(
            api_block_store,
            api_metrics,
            api_peer_id,
            api_botg,
            api_keypair,
            api_listen_addrs,
            api_citadel,
        );
        let addr = format!("0.0.0.0:{}", api_port);
        info!("Starting REST API on {}", addr);

        match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app).await {
                    error!("REST API server failed: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to bind REST API listener on {}: {}", addr, e);
            }
        }
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
                        if let Ok(mut addrs) = listen_addrs.write() {
                            addrs.push(address.clone());
                        } else {
                            warn!("Failed to record listen address due to poisoned lock");
                        }

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
