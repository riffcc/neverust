//! Configuration management for Neverust
//!
//! Handles CLI argument parsing, config file loading, and defaults.

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

    /// Bind address for the REST API (e.g. 127.0.0.1 to restrict to localhost)
    #[arg(long, default_value = "0.0.0.0")]
    pub api_bind: String,

    /// Node operating mode: altruistic (free blocks) or marketplace (paid blocks)
    #[arg(long, default_value = "altruistic")]
    pub mode: String,

    /// Price per byte in marketplace mode (in smallest currency unit)
    #[arg(long, default_value_t = 1)]
    pub price_per_byte: u64,

    /// Enable marketplace persistence and stateful API flows.
    #[arg(long)]
    pub persistence: bool,

    /// Maximum local storage quota exposed through the Archivist-compatible API.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pub quota_bytes: u64,

    /// Ethereum RPC endpoint used for marketplace integration.
    #[arg(long)]
    pub eth_provider: Option<String>,

    /// Explicit Ethereum account/address for the node.
    #[arg(long)]
    pub eth_account: Option<String>,

    /// Path to the Ethereum private key file.
    #[arg(long)]
    pub eth_private_key: Option<PathBuf>,

    /// Marketplace contract address.
    #[arg(long)]
    pub marketplace_address: Option<String>,

    /// Contracts map as JSON, matching Archivist CLI usage.
    #[arg(long)]
    pub contracts_addresses: Option<String>,

    /// Enable validator-side marketplace behavior.
    #[arg(long)]
    pub validator: bool,

    /// Enable prover-side marketplace behavior.
    #[arg(long)]
    pub prover: bool,

    /// Logging level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Bootstrap node multiaddr (can be specified multiple times)
    #[arg(long)]
    pub bootstrap_node: Vec<String>,

    /// Enable Citadel/Lens mode inside Neverust.
    #[arg(long)]
    pub citadel_mode: bool,

    /// Local Lens Site ID used by Citadel mode.
    #[arg(long, default_value_t = 1)]
    pub citadel_site_id: u64,

    /// Optional explicit Citadel origin/node ID. If 0, derived from peer ID.
    #[arg(long, default_value_t = 0)]
    pub citadel_node_id: u32,

    /// Optional host/domain bucket ID for Citadel admission guards.
    #[arg(long)]
    pub citadel_host_id: Option<u8>,

    /// Optional Flagship trust snapshot URL (JSON).
    #[arg(long)]
    pub citadel_flagship_url: Option<String>,

    /// Trusted origin IDs (can be specified multiple times).
    #[arg(long)]
    pub citadel_trusted_origin: Vec<u32>,

    /// Idle control-plane bandwidth cap in KiB/s (per node).
    #[arg(long, default_value_t = 100)]
    pub citadel_idle_bandwidth_kib: u64,

    /// Base PoW bits required for unknown origins.
    #[arg(long, default_value_t = 8)]
    pub citadel_pow_bits: u8,

    /// Reduced PoW bits required for trusted origins.
    #[arg(long, default_value_t = 4)]
    pub citadel_trusted_pow_bits: u8,

    /// Per-origin op rate cap per simulation/runtime round.
    #[arg(long, default_value_t = 96)]
    pub citadel_max_ops_per_origin_per_round: u32,

    /// Max new origins admitted per host per round.
    #[arg(long, default_value_t = 12)]
    pub citadel_max_new_origins_per_host_per_round: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    pub listen_port: u16,
    pub disc_port: u16,
    pub api_port: u16,
    #[serde(default = "default_api_bind")]
    pub api_bind: String,
    pub log_level: String,
    #[serde(default)]
    pub bootstrap_nodes: Vec<String>,
    pub mode: String,
    pub price_per_byte: u64,
    #[serde(default)]
    pub persistence: bool,
    #[serde(default = "default_quota_bytes")]
    pub quota_bytes: u64,
    #[serde(default)]
    pub eth_provider: Option<String>,
    #[serde(default)]
    pub eth_account: Option<String>,
    #[serde(default)]
    pub eth_private_key: Option<PathBuf>,
    #[serde(default)]
    pub marketplace_address: Option<String>,
    #[serde(default)]
    pub contracts_addresses: Option<String>,
    #[serde(default)]
    pub validator: bool,
    #[serde(default)]
    pub prover: bool,
    #[serde(default)]
    pub citadel_mode: bool,
    #[serde(default = "default_citadel_site_id")]
    pub citadel_site_id: u64,
    #[serde(default)]
    pub citadel_node_id: u32,
    #[serde(default)]
    pub citadel_host_id: Option<u8>,
    #[serde(default)]
    pub citadel_flagship_url: Option<String>,
    #[serde(default)]
    pub citadel_trusted_origins: Vec<u32>,
    #[serde(default = "default_citadel_idle_bandwidth_kib")]
    pub citadel_idle_bandwidth_kib: u64,
    #[serde(default = "default_citadel_pow_bits")]
    pub citadel_pow_bits: u8,
    #[serde(default = "default_citadel_trusted_pow_bits")]
    pub citadel_trusted_pow_bits: u8,
    #[serde(default = "default_citadel_max_ops_per_origin_per_round")]
    pub citadel_max_ops_per_origin_per_round: u32,
    #[serde(default = "default_citadel_max_new_origins_per_host_per_round")]
    pub citadel_max_new_origins_per_host_per_round: u32,
}

fn default_api_bind() -> String {
    "0.0.0.0".to_string()
}

fn default_citadel_site_id() -> u64 {
    1
}

fn default_quota_bytes() -> u64 {
    1024 * 1024 * 1024
}

fn default_citadel_idle_bandwidth_kib() -> u64 {
    100
}

fn default_citadel_pow_bits() -> u8 {
    8
}

fn default_citadel_trusted_pow_bits() -> u8 {
    4
}

fn default_citadel_max_ops_per_origin_per_round() -> u32 {
    96
}

fn default_citadel_max_new_origins_per_host_per_round() -> u32 {
    12
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            listen_port: 8070,
            disc_port: 8090,
            api_port: 8080,
            api_bind: default_api_bind(),
            log_level: "info".to_string(),
            bootstrap_nodes: Vec::new(),
            mode: "altruistic".to_string(),
            price_per_byte: 1,
            persistence: false,
            quota_bytes: default_quota_bytes(),
            eth_provider: None,
            eth_account: None,
            eth_private_key: None,
            marketplace_address: None,
            contracts_addresses: None,
            validator: false,
            prover: false,
            citadel_mode: false,
            citadel_site_id: 1,
            citadel_node_id: 0,
            citadel_host_id: None,
            citadel_flagship_url: None,
            citadel_trusted_origins: Vec::new(),
            citadel_idle_bandwidth_kib: 100,
            citadel_pow_bits: 8,
            citadel_trusted_pow_bits: 4,
            citadel_max_ops_per_origin_per_round: 96,
            citadel_max_new_origins_per_host_per_round: 12,
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
            // and fetch peer ID from the node's API.
            // Real Archivist exposes `/peerid`, while Neverust exposes both.
            let candidate_urls = [
                format!("http://{}/api/archivist/v1/peer-id", bootstrap_node),
                format!("http://{}/api/archivist/v1/peerid", bootstrap_node),
            ];

            let mut resolved_peer_id: Option<String> = None;
            for url in candidate_urls {
                match reqwest::get(&url).await {
                    Ok(response) => {
                        if !response.status().is_success() {
                            tracing::debug!(
                                "Bootstrap peer ID endpoint returned non-success status: {} {}",
                                url,
                                response.status()
                            );
                            continue;
                        }

                        match response.text().await {
                            Ok(body) => {
                                let body = body.trim();
                                let peer_id = if body.starts_with('{') {
                                    serde_json::from_str::<Value>(body).ok().and_then(|v| {
                                        v.get("id")
                                            .and_then(|id| id.as_str())
                                            .map(|id| id.to_string())
                                    })
                                } else {
                                    Some(body.trim_matches('"').to_string())
                                };

                                if let Some(peer_id) = peer_id {
                                    if !peer_id.is_empty() {
                                        resolved_peer_id = Some(peer_id);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::debug!(
                                    "Failed reading bootstrap peer ID response body from {}: {}",
                                    url,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Failed to query bootstrap peer ID URL {}: {}", url, e);
                    }
                }
            }

            if let Some(peer_id) = resolved_peer_id {
                // Extract host and port
                if let Some((host, api_port_str)) = bootstrap_node.rsplit_once(':') {
                    let host = host.trim_matches(['[', ']']);
                    let api_port = api_port_str.parse::<u16>().ok();
                    let p2p_port = std::env::var("BOOTSTRAP_P2P_PORT")
                        .ok()
                        .and_then(|v| v.parse::<u16>().ok())
                        .or_else(|| {
                            api_port.and_then(|port| if port > 10 { Some(port - 10) } else { None })
                        })
                        .unwrap_or(8070);
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
                    let multiaddr =
                        format!("/ip4/{}/tcp/{}/p2p/{}", resolved_host, p2p_port, peer_id);
                    tracing::info!("Resolved local bootstrap multiaddr: {}", multiaddr);
                    return Ok(vec![multiaddr]);
                }
            } else {
                tracing::warn!(
                    "Failed to fetch peer ID from bootstrap node {}; falling back to testnet",
                    bootstrap_node
                );
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
            api_bind: cmd.api_bind,
            log_level: cmd.log_level,
            bootstrap_nodes: cmd.bootstrap_node,
            mode: cmd.mode,
            price_per_byte: cmd.price_per_byte,
            persistence: cmd.persistence,
            quota_bytes: cmd.quota_bytes,
            eth_provider: cmd.eth_provider,
            eth_account: cmd.eth_account,
            eth_private_key: cmd.eth_private_key,
            marketplace_address: cmd.marketplace_address,
            contracts_addresses: cmd.contracts_addresses,
            validator: cmd.validator,
            prover: cmd.prover,
            citadel_mode: cmd.citadel_mode,
            citadel_site_id: cmd.citadel_site_id,
            citadel_node_id: cmd.citadel_node_id,
            citadel_host_id: cmd.citadel_host_id,
            citadel_flagship_url: cmd.citadel_flagship_url,
            citadel_trusted_origins: cmd.citadel_trusted_origin,
            citadel_idle_bandwidth_kib: cmd.citadel_idle_bandwidth_kib,
            citadel_pow_bits: cmd.citadel_pow_bits,
            citadel_trusted_pow_bits: cmd.citadel_trusted_pow_bits,
            citadel_max_ops_per_origin_per_round: cmd.citadel_max_ops_per_origin_per_round,
            citadel_max_new_origins_per_host_per_round: cmd
                .citadel_max_new_origins_per_host_per_round,
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
        assert!(!config.citadel_mode);
        assert_eq!(config.citadel_idle_bandwidth_kib, 100);
    }

    #[test]
    fn test_config_from_start_command() {
        let cmd = StartCommand {
            data_dir: PathBuf::from("./test-data"),
            listen_port: 9000,
            disc_port: 9001,
            api_port: 9002,
            api_bind: "127.0.0.1".to_string(),
            mode: "marketplace".to_string(),
            price_per_byte: 100,
            persistence: true,
            quota_bytes: 123456,
            eth_provider: Some("https://rpc.example".to_string()),
            eth_account: Some("0xabc".to_string()),
            eth_private_key: Some(PathBuf::from("/tmp/key")),
            marketplace_address: Some("0xdef".to_string()),
            contracts_addresses: Some("{\"Marketplace\":\"0xdef\"}".to_string()),
            validator: true,
            prover: true,
            log_level: "debug".to_string(),
            bootstrap_node: vec!["/ip4/1.2.3.4/tcp/8070/p2p/12D3KooTest".to_string()],
            citadel_mode: true,
            citadel_site_id: 42,
            citadel_node_id: 7,
            citadel_host_id: Some(2),
            citadel_flagship_url: Some("http://127.0.0.1:9999/trust".to_string()),
            citadel_trusted_origin: vec![1, 2, 3],
            citadel_idle_bandwidth_kib: 64,
            citadel_pow_bits: 9,
            citadel_trusted_pow_bits: 5,
            citadel_max_ops_per_origin_per_round: 32,
            citadel_max_new_origins_per_host_per_round: 6,
        };

        let config: Config = cmd.into();
        assert_eq!(config.data_dir, PathBuf::from("./test-data"));
        assert_eq!(config.listen_port, 9000);
        assert_eq!(config.disc_port, 9001);
        assert_eq!(config.api_port, 9002);
        assert_eq!(config.mode, "marketplace");
        assert_eq!(config.price_per_byte, 100);
        assert!(config.persistence);
        assert_eq!(config.quota_bytes, 123456);
        assert_eq!(config.eth_provider.as_deref(), Some("https://rpc.example"));
        assert_eq!(config.eth_account.as_deref(), Some("0xabc"));
        assert_eq!(config.marketplace_address.as_deref(), Some("0xdef"));
        assert!(config.validator);
        assert!(config.prover);
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.bootstrap_nodes.len(), 1);
        assert!(config.citadel_mode);
        assert_eq!(config.citadel_site_id, 42);
        assert_eq!(config.citadel_node_id, 7);
        assert_eq!(config.citadel_host_id, Some(2));
        assert_eq!(config.citadel_trusted_origins, vec![1, 2, 3]);
        assert_eq!(config.citadel_idle_bandwidth_kib, 64);
    }
}
