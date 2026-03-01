//! Configuration management for Neverust
//!
//! Handles CLI argument parsing, config file loading, and defaults.

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML parsing error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Invalid configuration: {0}")]
    Invalid(String),
}

#[derive(Parser, Debug)]
#[command(name = "neverust")]
#[command(about = "Archivist Storage Node in Rust", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the Archivist node
    Start(StartCommand),
}

#[derive(Parser, Debug, Clone)]
pub struct StartCommand {
    /// Data directory for node configuration and storage
    #[arg(long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// TCP port for P2P transport
    #[arg(long, default_value_t = 8070)]
    pub listen_port: u16,

    /// UDP port for peer discovery
    #[arg(long, default_value_t = 8090)]
    pub disc_port: u16,

    /// HTTP port for REST API
    #[arg(long, default_value_t = 8080)]
    pub api_port: u16,

    /// Node operating mode: altruistic (free blocks) or marketplace (paid blocks)
    #[arg(long, default_value = "altruistic")]
    pub mode: String,

    /// Price per byte in marketplace mode (in smallest currency unit)
    #[arg(long, default_value_t = 1)]
    pub price_per_byte: u64,

    /// Logging level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Bootstrap node multiaddr (can be specified multiple times)
    #[arg(long)]
    pub bootstrap_node: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    pub listen_port: u16,
    pub disc_port: u16,
    pub api_port: u16,
    pub log_level: String,
    #[serde(default)]
    pub bootstrap_nodes: Vec<String>,
    pub mode: String,
    pub price_per_byte: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            listen_port: 8070,
            disc_port: 8090,
            api_port: 8080,
            log_level: "info".to_string(),
            bootstrap_nodes: Vec::new(),
            mode: "altruistic".to_string(),
            price_per_byte: 1,
        }
    }
}

impl Config {
    /// Create config from CLI arguments
    pub fn from_cli() -> Result<Self, ConfigError> {
        let cli = Cli::parse();

        match cli.command {
            Commands::Start(cmd) => Ok(cmd.into()),
        }
    }

    /// Load config from TOML file, merging with CLI overrides
    pub fn load_from_file(path: &PathBuf) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Fetch or determine bootstrap nodes
    ///
    /// Priority:
    /// 1. Check BOOTSTRAP_NODE environment variable
    /// 2. Fall back to Archivist testnet
    pub async fn fetch_bootstrap_nodes() -> Result<Vec<String>, ConfigError> {
        // Check for BOOTSTRAP_NODE environment variable first
        if let Ok(bootstrap_node) = std::env::var("BOOTSTRAP_NODE") {
            tracing::info!(
                "Using local bootstrap node from BOOTSTRAP_NODE env: {}",
                bootstrap_node
            );

            // Parse the bootstrap node address (format: "host:port" or "ip:port")
            // We need to fetch the peer ID from the node's API
            let url = format!("http://{}/api/archivist/v1/peer-id", bootstrap_node);

            match reqwest::get(&url).await {
                Ok(response) => {
                    if let Ok(peer_id) = response.text().await {
                        let peer_id = peer_id.trim().trim_matches('"');
                        // Extract host and port
                        let parts: Vec<&str> = bootstrap_node.split(':').collect();
                        if parts.len() == 2 {
                            let host = parts[0];
                            // Resolve hostname to IP if needed
                            let resolved_host = if host.parse::<std::net::IpAddr>().is_ok() {
                                host.to_string()
                            } else {
                                // Try to resolve hostname to IP
                                match tokio::net::lookup_host(format!("{}:0", host)).await {
                                    Ok(mut addrs) => {
                                        if let Some(addr) = addrs.next() {
                                            addr.ip().to_string()
                                        } else {
                                            host.to_string()
                                        }
                                    }
                                    Err(_) => host.to_string(),
                                }
                            };
                            // Use port 8070 for P2P (not the API port)
                            let multiaddr =
                                format!("/ip4/{}/tcp/8070/p2p/{}", resolved_host, peer_id);
                            tracing::info!("Resolved local bootstrap multiaddr: {}", multiaddr);
                            return Ok(vec![multiaddr]);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch peer ID from bootstrap node {}: {}",
                        bootstrap_node,
                        e
                    );
                }
            }
        }

        // Fall back to Archivist testnet bootstrap nodes
        tracing::info!("Falling back to Archivist testnet bootstrap nodes");
        Self::fetch_testnet_bootstrap_nodes().await
    }

    /// Fetch bootstrap nodes from Archivist testnet
    pub async fn fetch_testnet_bootstrap_nodes() -> Result<Vec<String>, ConfigError> {
        use crate::spr::parse_spr_records;

        // Fetch SPR records from testnet
        let response = reqwest::get("https://spr.archivist.storage/testnet")
            .await
            .map_err(|e| ConfigError::Io(std::io::Error::other(e.to_string())))?
            .text()
            .await
            .map_err(|e| ConfigError::Io(std::io::Error::other(e.to_string())))?;

        // Parse SPR records
        let records = parse_spr_records(&response).map_err(|e| {
            ConfigError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))
        })?;

        // Convert UDP discovery addresses to TCP for actual connections
        // Archivist testnet nodes use TCP+Noise+Mplex (NOT QUIC)
        let mut multiaddrs = Vec::new();
        for (peer_id, addrs) in records {
            for addr in addrs {
                let addr_str = addr.to_string();
                // SPR contains UDP addresses - convert to TCP multiaddrs
                if addr_str.contains("/udp/") {
                    // Convert /ip4/X.X.X.X/udp/PORT to /ip4/X.X.X.X/tcp/PORT/p2p/PEER_ID
                    let tcp_addr = addr_str.replace("/udp/", "/tcp/");
                    let full_addr = format!("{}/p2p/{}", tcp_addr, peer_id);
                    tracing::info!("Converted UDP to TCP: {} -> {}", addr_str, full_addr);
                    multiaddrs.push(full_addr);
                } else {
                    // For other protocols, just add peer ID
                    let full_addr = format!("{}/p2p/{}", addr, peer_id);
                    tracing::info!("Other protocol: {}", full_addr);
                    multiaddrs.push(full_addr);
                }
            }
        }

        Ok(multiaddrs)
    }

    /// Fetch bootstrap ENRs for DiscV5
    ///
    /// For now, returns an empty list since DiscV5 bootstrap integration
    /// requires ENR parsing from Archivist testnet (future work)
    pub async fn fetch_bootstrap_enrs() -> Result<Vec<String>, ConfigError> {
        // TODO: Convert Archivist SPRs to DiscV5 ENRs
        // For now, we'll bootstrap via local BOOTSTRAP_NODE if provided
        if let Ok(bootstrap_node) = std::env::var("BOOTSTRAP_NODE") {
            tracing::info!(
                "DiscV5: Would use bootstrap node {} (ENR conversion not yet implemented)",
                bootstrap_node
            );
        }

        tracing::info!("DiscV5: No bootstrap ENRs configured (will rely on local discovery)");
        Ok(vec![])
    }
}

impl From<StartCommand> for Config {
    fn from(cmd: StartCommand) -> Self {
        Config {
            data_dir: cmd.data_dir,
            listen_port: cmd.listen_port,
            disc_port: cmd.disc_port,
            api_port: cmd.api_port,
            log_level: cmd.log_level,
            bootstrap_nodes: cmd.bootstrap_node,
            mode: cmd.mode,
            price_per_byte: cmd.price_per_byte,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.listen_port, 8070);
        assert_eq!(config.disc_port, 8090);
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn test_config_from_start_command() {
        let cmd = StartCommand {
            data_dir: PathBuf::from("./test-data"),
            listen_port: 9000,
            disc_port: 9001,
            api_port: 9002,
            mode: "marketplace".to_string(),
            price_per_byte: 100,
            log_level: "debug".to_string(),
            bootstrap_node: vec!["/ip4/1.2.3.4/tcp/8070/p2p/12D3KooTest".to_string()],
        };

        let config: Config = cmd.into();
        assert_eq!(config.data_dir, PathBuf::from("./test-data"));
        assert_eq!(config.listen_port, 9000);
        assert_eq!(config.disc_port, 9001);
        assert_eq!(config.api_port, 9002);
        assert_eq!(config.mode, "marketplace");
        assert_eq!(config.price_per_byte, 100);
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.bootstrap_nodes.len(), 1);
    }
}
