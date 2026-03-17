use neverust_core::{Block, Discovery};
use libp2p::identity::Keypair;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();
}

fn reserve_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("bind ephemeral udp port")
        .local_addr()
        .expect("local addr")
        .port()
}

#[tokio::test]
#[ignore]
async fn test_devnet_discv5_provide_roundtrip() {
    init_tracing();

    let bootstrap_response = reqwest::get("https://config.archivist.storage/devnet/spr")
        .await
        .expect("fetch devnet spr")
        .text()
        .await
        .expect("read devnet spr response");
    let bootstrap_nodes: Vec<String> = bootstrap_response
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("spr:"))
        .map(ToString::to_string)
        .collect();
    assert!(
        !bootstrap_nodes.is_empty(),
        "devnet SPR endpoint returned no bootstrap nodes"
    );

    let port1 = reserve_udp_port();
    let port2 = reserve_udp_port();

    let key1 = Keypair::generate_secp256k1();
    let key2 = Keypair::generate_secp256k1();

    let disc1 = Arc::new(
        Discovery::new(
            &key1,
            format!("0.0.0.0:{port1}").parse().expect("socket addr 1"),
            vec!["/ip4/127.0.0.1/tcp/28070".to_string()],
            bootstrap_nodes.clone(),
        )
        .await
        .expect("create discovery 1"),
    );
    let disc2 = Arc::new(
        Discovery::new(
            &key2,
            format!("0.0.0.0:{port2}").parse().expect("socket addr 2"),
            vec!["/ip4/127.0.0.1/tcp/28071".to_string()],
            bootstrap_nodes,
        )
        .await
        .expect("create discovery 2"),
    );

    let run1 = tokio::spawn(disc1.clone().run());
    let run2 = tokio::spawn(disc2.clone().run());

    sleep(Duration::from_secs(2)).await;
    info!(
        "Bootstrap soak complete: node1 connected_peers={}, node2 connected_peers={}",
        disc1.connected_peers(),
        disc2.connected_peers()
    );

    let block = Block::new_sha256(b"neverust-devnet-discv5-provide-proof".to_vec())
        .expect("create test block");
    let cid = block.cid;
    info!("Providing test CID on devnet: {}", cid);

    disc1.provide(&cid).await.expect("provide cid");

    let providers = timeout(Duration::from_secs(15), async {
        loop {
            match disc2.find(&cid).await {
                Ok(providers) if !providers.is_empty() => return providers,
                _ => sleep(Duration::from_millis(500)).await,
            }
        }
    })
    .await
    .expect("timed out waiting for remote providers");

    info!(
        "Found {} provider record(s) for CID {} via devnet",
        providers.len(),
        cid
    );
    assert!(
        !providers.is_empty(),
        "devnet did not return any provider records for CID {}",
        cid
    );

    run1.abort();
    run2.abort();
}
