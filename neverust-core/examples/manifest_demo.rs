//! Demonstration of Archivist Manifest encoding/decoding
//!
//! Run with: cargo run --example manifest_demo

use neverust_core::{Manifest, BLOCK_CODEC, MANIFEST_CODEC, SHA256_CODEC};

fn main() {
    println!("=== Archivist Manifest Demo ===\n");

    // Create a test CID for the tree root
    let tree_data = b"example merkle tree root";
    let hash = blake3::hash(tree_data);
    let hash_bytes = hash.as_bytes();

    let mut buf = [0u8; 10];
    let mut multihash = Vec::new();
    let encoded = unsigned_varint::encode::u64(0x1e, &mut buf); // BLAKE3
    multihash.extend_from_slice(encoded);
    let encoded = unsigned_varint::encode::u64(32, &mut buf);
    multihash.extend_from_slice(encoded);
    multihash.extend_from_slice(hash_bytes);

    let mut cid_bytes = Vec::new();
    let encoded = unsigned_varint::encode::u64(1, &mut buf); // CIDv1
    cid_bytes.extend_from_slice(encoded);
    let encoded = unsigned_varint::encode::u64(BLOCK_CODEC, &mut buf);
    cid_bytes.extend_from_slice(encoded);
    cid_bytes.extend_from_slice(&multihash);

    let tree_cid = cid::Cid::try_from(cid_bytes).expect("Failed to create CID");

    // Create a simple manifest
    println!("1. Creating a simple unprotected manifest...");
    let manifest = Manifest::new(
        tree_cid,
        65536,              // 64KB block size
        10 * 1024 * 1024,   // 10MB dataset
        Some(BLOCK_CODEC),
        Some(SHA256_CODEC),
        Some(1),
        Some("example.bin".to_string()),
        Some("application/octet-stream".to_string()),
    );

    println!("   Tree CID: {}", manifest.tree_cid);
    println!("   Block size: {} bytes", manifest.block_size);
    println!("   Dataset size: {} bytes", manifest.dataset_size);
    println!("   Number of blocks: {}", manifest.blocks_count());
    println!("   Codec: 0x{:x}", manifest.codec);
    println!("   Hash codec: 0x{:x}", manifest.hcodec);
    println!("   Filename: {:?}", manifest.filename);
    println!("   MIME type: {:?}", manifest.mimetype);
    println!("   Protected: {}", manifest.is_protected());
    println!();

    // Encode to protobuf
    println!("2. Encoding manifest to protobuf...");
    let encoded = manifest.encode().expect("Encode failed");
    println!("   Encoded size: {} bytes", encoded.len());
    println!("   First 32 bytes (hex): {}", hex::encode(&encoded[..32.min(encoded.len())]));
    println!();

    // Decode from protobuf
    println!("3. Decoding manifest from protobuf...");
    let decoded = Manifest::decode(&encoded).expect("Decode failed");
    println!("   Tree CID matches: {}", decoded.tree_cid == manifest.tree_cid);
    println!("   Block size matches: {}", decoded.block_size == manifest.block_size);
    println!("   Dataset size matches: {}", decoded.dataset_size == manifest.dataset_size);
    println!("   Filename matches: {}", decoded.filename == manifest.filename);
    println!();

    // Create a block from the manifest
    println!("4. Creating a block from the manifest...");
    let block = manifest.to_block().expect("to_block failed");
    println!("   Block CID: {}", block.cid);
    println!("   Block codec: 0x{:x} (should be 0x{:x})", block.cid.codec(), MANIFEST_CODEC);
    println!("   Block data size: {} bytes", block.data.len());
    println!();

    // Recover manifest from block
    println!("5. Recovering manifest from block...");
    let recovered = Manifest::from_block(&block).expect("from_block failed");
    println!("   Tree CID matches: {}", recovered.tree_cid == manifest.tree_cid);
    println!("   All fields match: {}", recovered == manifest);
    println!();

    println!("=== Demo Complete ===");
}
