//! SPR (Signed Peer Record) parsing
//!
//! Decodes base64-encoded SPR records into multiaddrs for bootstrap nodes.
//!
//! ## Format Investigation
//!
//! Archivist's SPR format is a libp2p `SignedEnvelope` containing a custom peer record:
//! - Base64 URL-safe encoded protobuf
//! - Contains `SignedEnvelope` with peer public key and signature
//! - Payload contains peer information including multiaddrs
//!
//! The exact payload structure differs from standard libp2p `PeerRecord`.
//! For now, we attempt standard `PeerRecord` decoding, which works for the
//! peer ID but may fail for multiaddr extraction with Archivist's custom format.
//!
//! ## Known Issues
//! - Archivist uses custom protobuf structure for multiaddrs
//! - Standard `PeerRecord::from_signed_envelope` may fail with "payload extraction error"
//! - Alternative: manual protobuf parsing of the envelope payload
//!
//! ## References
//! - Archivist testnet SPR endpoint: https://spr.archivist.storage/testnet
//! - libp2p SignedEnvelope: https://github.com/libp2p/specs/blob/master/RFC/0003-routing-records.md

use libp2p::{identity::Keypair, Multiaddr, PeerId};
use prost::Message;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SprError {
    #[error("Base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("Protobuf decode error: {0}")]
    Protobuf(#[from] prost::DecodeError),

    #[error("Invalid peer ID: {0}")]
    InvalidPeerId(String),

    #[error("Invalid multiaddr: {0}")]
    InvalidMultiaddr(String),

    #[error("Signature error: {0}")]
    Signature(String),

    #[error("Encoding error: {0}")]
    Encoding(String),
}

/// Archivist's SPR format (actual structure from testnet)
#[derive(Clone, PartialEq, Message)]
struct ArchivistSpr {
    #[prost(bytes = "vec", optional, tag = "1")]
    peer_id: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "2")]
    seq_bytes: Option<Vec<u8>>, // 2 bytes
    #[prost(bytes = "vec", repeated, tag = "3")] // Contains nested PeerInfo protobuf
    peer_record: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "5")] // Signature (DER format)
    signature: Vec<Vec<u8>>,
}

/// Nested PeerRecord inside field 3 (libp2p PeerRecord format)
#[derive(Clone, PartialEq, Message)]
struct PeerInfo {
    #[prost(bytes = "vec", optional, tag = "1")]
    peer_id: Option<Vec<u8>>,
    #[prost(uint64, tag = "2")]
    seq: u64,
    #[prost(bytes = "vec", repeated, tag = "3")] // Multiaddrs are in field 3!
    addrs: Vec<Vec<u8>>,
}

/// Parse SPR records from testnet endpoint response
pub fn parse_spr_records(spr_text: &str) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, SprError> {
    let mut results = Vec::new();

    for line in spr_text.lines() {
        if let Some(spr_data) = line.strip_prefix("spr:") {
            match parse_single_spr(spr_data) {
                Ok((peer_id, addrs)) => {
                    tracing::info!("Parsed SPR: peer_id={}, addrs={:?}", peer_id, addrs);
                    results.push((peer_id, addrs));
                }
                Err(e) => {
                    tracing::warn!("Failed to parse SPR record: {}", e);
                    continue;
                }
            }
        }
    }

    Ok(results)
}

/// Parsed SPR record with full details.
#[derive(Clone, Debug)]
pub struct SprRecord {
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
    /// Raw secp256k1 compressed public key bytes (33 bytes).
    pub secp256k1_pubkey: Option<Vec<u8>>,
}

/// Parse SPR records returning full details including raw public keys.
pub fn parse_spr_records_full(spr_text: &str) -> Result<Vec<SprRecord>, SprError> {
    let mut results = Vec::new();

    for line in spr_text.lines() {
        if let Some(spr_data) = line.strip_prefix("spr:") {
            match parse_single_spr_full(spr_data) {
                Ok(record) => {
                    tracing::info!(
                        "Parsed SPR: peer_id={}, addrs={:?}",
                        record.peer_id,
                        record.addrs
                    );
                    results.push(record);
                }
                Err(e) => {
                    tracing::warn!("Failed to parse SPR record: {}", e);
                    continue;
                }
            }
        }
    }

    Ok(results)
}

/// Parse a single SPR with full details.
fn parse_single_spr_full(spr_base64: &str) -> Result<SprRecord, SprError> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use libp2p::identity::PublicKey;

    let bytes = URL_SAFE_NO_PAD.decode(spr_base64)?;
    let spr = ArchivistSpr::decode(&bytes[..])?;

    let peer_id_bytes = spr
        .peer_id
        .ok_or_else(|| SprError::Protobuf(prost::DecodeError::new("Missing peer_id")))?;

    let public_key = PublicKey::try_decode_protobuf(&peer_id_bytes)
        .map_err(|e| SprError::InvalidPeerId(e.to_string()))?;

    // Extract raw secp256k1 compressed key if available
    let secp256k1_pubkey = if let Ok(secp_key) = public_key.clone().try_into_secp256k1() {
        Some(secp_key.to_bytes().to_vec())
    } else {
        None
    };

    let peer_id = public_key.to_peer_id();

    let mut addrs = Vec::new();
    for peer_record_bytes in &spr.peer_record {
        if let Ok(peer_info) = PeerInfo::decode(&peer_record_bytes[..]) {
            for addr_bytes in peer_info.addrs {
                #[derive(Clone, PartialEq, Message)]
                struct AddrWrapper {
                    #[prost(bytes = "vec", optional, tag = "1")]
                    addr: Option<Vec<u8>>,
                }

                if let Ok(wrapper) = AddrWrapper::decode(&addr_bytes[..]) {
                    if let Some(raw_addr) = wrapper.addr {
                        if let Ok(addr) = Multiaddr::try_from(raw_addr) {
                            addrs.push(addr);
                        }
                    }
                }
            }
        }
    }

    Ok(SprRecord {
        peer_id,
        addrs,
        secp256k1_pubkey,
    })
}

/// Parse a single base64-encoded SPR record
fn parse_single_spr(spr_base64: &str) -> Result<(PeerId, Vec<Multiaddr>), SprError> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use libp2p::identity::PublicKey;

    // Decode URL-safe base64 (SPR records use URL-safe encoding)
    let bytes = URL_SAFE_NO_PAD.decode(spr_base64)?;

    // Decode ArchivistSpr protobuf
    let spr = ArchivistSpr::decode(&bytes[..])?;

    // Extract peer_id bytes
    let peer_id_bytes = spr
        .peer_id
        .ok_or_else(|| SprError::Protobuf(prost::DecodeError::new("Missing peer_id")))?;

    // The peer_id field contains a PublicKey protobuf (not raw peer ID bytes)
    let public_key = PublicKey::try_decode_protobuf(&peer_id_bytes)
        .map_err(|e| SprError::InvalidPeerId(e.to_string()))?;

    // Derive PeerId from public key
    let peer_id = public_key.to_peer_id();

    // Field 3 contains nested PeerInfo protobuf with multiaddrs
    let mut addrs = Vec::new();
    for peer_record_bytes in &spr.peer_record {
        if let Ok(peer_info) = PeerInfo::decode(&peer_record_bytes[..]) {
            for addr_bytes in peer_info.addrs {
                // The addr_bytes are wrapped in a protobuf message with field 1
                // Try decoding as nested protobuf first
                #[derive(Clone, PartialEq, Message)]
                struct AddrWrapper {
                    #[prost(bytes = "vec", optional, tag = "1")]
                    addr: Option<Vec<u8>>,
                }

                if let Ok(wrapper) = AddrWrapper::decode(&addr_bytes[..]) {
                    if let Some(raw_addr) = wrapper.addr {
                        if let Ok(addr) = Multiaddr::try_from(raw_addr) {
                            addrs.push(addr);
                        }
                    }
                }
            }
        }
    }

    Ok((peer_id, addrs))
}

/// Generate an SPR (Signed Peer Record) for this node
///
/// Creates an Archivist-compatible SPR that can be shared with other nodes
/// for bootstrapping and peer discovery.
pub fn generate_spr(keypair: &Keypair, addrs: &[Multiaddr], seq: u64) -> Result<String, SprError> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    // Get public key bytes (protobuf-encoded)
    let public_key = keypair.public();
    let peer_id_bytes = public_key.encode_protobuf();

    // Convert sequence number to 2-byte big-endian
    let seq_bytes = (seq as u16).to_be_bytes().to_vec();

    // Create PeerInfo with addresses
    let mut peer_info = PeerInfo {
        peer_id: Some(peer_id_bytes.clone()),
        seq,
        addrs: Vec::new(),
    };

    // Wrap each address in AddrWrapper protobuf
    #[derive(Clone, PartialEq, Message)]
    struct AddrWrapper {
        #[prost(bytes = "vec", optional, tag = "1")]
        addr: Option<Vec<u8>>,
    }

    for addr in addrs {
        let addr_bytes = addr.to_vec();
        let wrapper = AddrWrapper {
            addr: Some(addr_bytes),
        };
        let mut wrapper_bytes = Vec::new();
        wrapper
            .encode(&mut wrapper_bytes)
            .map_err(|e| SprError::Encoding(format!("Failed to encode addr wrapper: {}", e)))?;
        peer_info.addrs.push(wrapper_bytes);
    }

    // Encode PeerInfo
    let mut peer_info_bytes = Vec::new();
    peer_info
        .encode(&mut peer_info_bytes)
        .map_err(|e| SprError::Encoding(format!("Failed to encode peer info: {}", e)))?;

    // Create ArchivistSpr (without signature first)
    let spr_unsigned = ArchivistSpr {
        peer_id: Some(peer_id_bytes),
        seq_bytes: Some(seq_bytes),
        peer_record: vec![peer_info_bytes.clone()],
        signature: Vec::new(), // Will be filled after signing
    };

    // Encode unsigned SPR
    let mut unsigned_bytes = Vec::new();
    spr_unsigned
        .encode(&mut unsigned_bytes)
        .map_err(|e| SprError::Encoding(format!("Failed to encode SPR: {}", e)))?;

    // Sign the peer_record payload
    let signature = keypair
        .sign(&peer_info_bytes)
        .map_err(|e| SprError::Signature(format!("Failed to sign: {}", e)))?;

    // Create final SPR with signature
    let spr_signed = ArchivistSpr {
        peer_id: spr_unsigned.peer_id,
        seq_bytes: spr_unsigned.seq_bytes,
        peer_record: spr_unsigned.peer_record,
        signature: vec![signature],
    };

    // Encode final SPR
    let mut final_bytes = Vec::new();
    spr_signed
        .encode(&mut final_bytes)
        .map_err(|e| SprError::Encoding(format!("Failed to encode signed SPR: {}", e)))?;

    // Encode as base64 URL-safe
    let spr_base64 = URL_SAFE_NO_PAD.encode(&final_bytes);

    // Return in spr: format
    Ok(format!("spr:{}", spr_base64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_spr_records() {
        // Real SPR from Archivist testnet
        let spr_text = "spr:CiUIAhIhA5mg11LZgFQ4XzIRb1T5xw9muFW1ALNKTijyKhQmvKYXEgIDARpJCicAJQgCEiEDmaDXUtmAVDhfMhFvVPnHD2a4VbUAs0pOKPIqFCa8phcQl-XFxQYaCwoJBE4vqKqRAnU6GgsKCQROL6iqkQJ1OipHMEUCIQDfzVYbN6A_O4i29e_FtDDUo7GJS3bkXRQtoteYbPSFtgIgcc8Kgj2ggVJyK16EY9xi4bY2lpTTeNIRjvslXSRdN5w";

        match parse_spr_records(spr_text) {
            Ok(records) => {
                println!("Parsed {} records", records.len());
                if records.is_empty() {
                    panic!("No records parsed - check parse_spr_records logic");
                }
                assert_eq!(records.len(), 1);
                let (peer_id, addrs) = &records[0];
                println!("Parsed SPR successfully!");
                println!("  Peer ID: {}", peer_id);
                println!("  Addresses: {:?}", addrs);
                assert!(addrs.len() > 0, "Should have at least one address");
            }
            Err(e) => {
                panic!("Failed to parse SPR: {}", e);
            }
        }
    }

    #[test]
    fn test_parse_single_spr_direct() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

        let spr_data = "CiUIAhIhA5mg11LZgFQ4XzIRb1T5xw9muFW1ALNKTijyKhQmvKYXEgIDARpJCicAJQgCEiEDmaDXUtmAVDhfMhFvVPnHD2a4VbUAs0pOKPIqFCa8phcQl-XFxQYaCwoJBE4vqKqRAnU6GgsKCQROL6iqkQJ1OipHMEUCIQDfzVYbN6A_O4i29e_FtDDUo7GJS3bkXRQtoteYbPSFtgIgcc8Kgj2ggVJyK16EY9xi4bY2lpTTeNIRjvslXSRdN5w";

        let bytes = URL_SAFE_NO_PAD.decode(spr_data).unwrap();
        println!("Decoded {} bytes", bytes.len());

        // Try decoding as ArchivistSpr
        match ArchivistSpr::decode(&bytes[..]) {
            Ok(spr) => {
                println!("ArchivistSpr decoded!");
                if let Some(peer_id) = &spr.peer_id {
                    println!("  peer_id (field 1): {} bytes", peer_id.len());
                }
                if let Some(seq) = &spr.seq_bytes {
                    println!(
                        "  seq_bytes (field 2): {} bytes - {}",
                        seq.len(),
                        hex::encode(seq)
                    );
                }
                println!("  peer_record (field 3) count: {}", spr.peer_record.len());
                for (i, rec) in spr.peer_record.iter().enumerate() {
                    println!(
                        "    peer_record[{}]: {} bytes - full hex: {}",
                        i,
                        rec.len(),
                        hex::encode(rec)
                    );
                    match PeerInfo::decode(&rec[..]) {
                        Ok(peer_info) => {
                            println!("      -> PeerInfo decoded: {} addrs", peer_info.addrs.len());
                            for (j, addr) in peer_info.addrs.iter().enumerate() {
                                println!(
                                    "         addr[{}]: {} bytes - {}",
                                    j,
                                    addr.len(),
                                    hex::encode(&addr[..addr.len().min(20)])
                                );
                            }
                        }
                        Err(e) => {
                            println!("      -> PeerInfo decode FAILED: {}", e);
                        }
                    }
                }
                println!("  signature (field 5) count: {}", spr.signature.len());
                for (i, sig) in spr.signature.iter().enumerate() {
                    println!("    signature[{}]: {} bytes", i, sig.len());
                }
            }
            Err(e) => {
                panic!("ArchivistSpr decode failed: {}", e);
            }
        }

        match parse_single_spr(spr_data) {
            Ok((peer_id, addrs)) => {
                println!("\nFull parse successful!");
                println!("  Peer ID: {}", peer_id);
                println!("  Addresses: {:?}", addrs);
                assert!(addrs.len() > 0, "Should have at least one address");
            }
            Err(e) => {
                panic!("parse_single_spr failed: {}", e);
            }
        }
    }
}
