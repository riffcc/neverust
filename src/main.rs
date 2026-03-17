//! Neverust - Archivist Storage Node in Rust
//!
//! A high-performance P2P storage node implementation using rust-libp2p.

use neverust_core::{load_or_generate_eth_key, run_node, Config};
use neverust_core::config::CliAction;
use std::error::Error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    match Config::parse_cli()? {
        CliAction::GenerateKey(path) => {
            // Standalone key generation — no node startup needed.
            init_logging("info");
            let key = load_or_generate_eth_key(&path)?;
            println!("ETH address: {}", key.address_string());
            println!("Key file:    {}", path.display());
        }
        CliAction::Start(mut config) => {
            init_logging(&config.log_level);
            tracing::info!("Starting Neverust node...");

            // Auto-load or generate ETH key if no account is set.
            if config.eth_account.is_none() {
                let key_path = config
                    .eth_private_key
                    .clone()
                    .unwrap_or_else(|| config.data_dir.join("eth.key"));
                let key = load_or_generate_eth_key(&key_path)?;
                config.eth_account = Some(key.address_string());
                config.eth_private_key = Some(key_path);
            }

            run_node(config).await?;
        }
    }

    Ok(())
}

fn init_logging(level: &str) {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}
