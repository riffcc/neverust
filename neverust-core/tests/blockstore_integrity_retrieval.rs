use neverust_core::storage::{Block, BlockStore, StorageError};
use tempfile::tempdir;

fn deterministic_payload(i: usize) -> Vec<u8> {
    let len = 256 + ((i * 131) % 8192);
    let mut out = vec![0u8; len];
    let mut x = (i as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    for b in &mut out {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        *b = (x & 0xff) as u8;
    }
    out
}

#[tokio::test]
async fn blockstore_integrity_and_retrieval_deltaflat() -> Result<(), StorageError> {
    let dir = tempdir().map_err(StorageError::IoError)?;
    std::env::set_var("NEVERUST_STORAGE_BACKEND", "deltaflat");
    std::env::set_var("NEVERUST_DELTAFLAT_FSYNC", "0");
    std::env::set_var("NEVERUST_DELTAFLAT_SKIP_EXISTS_CHECK", "0");
    std::env::set_var("NEVERUST_DELTAFLAT_INITIAL_LEVEL", "10");

    let store = BlockStore::new_with_path(dir.path())?;
    let total_blocks = 50_000usize;
    let batch_size = 512usize;

    let mut expected: Vec<(cid::Cid, Vec<u8>)> = Vec::with_capacity(total_blocks);
    let mut batch = Vec::with_capacity(batch_size);

    for i in 0..total_blocks {
        let data = deterministic_payload(i);
        let block = Block::new(data.clone()).map_err(StorageError::VerificationFailed)?;
        expected.push((block.cid, data));
        batch.push(block);

        if batch.len() >= batch_size {
            store.put_many(std::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        store.put_many(batch).await?;
    }

    let stats = store.stats().await;
    assert_eq!(stats.block_count, total_blocks);

    for (idx, (cid, data)) in expected.iter().enumerate() {
        let has = store.has(cid).await;
        assert!(has, "missing cid at index {}", idx);

        let got = store.get(cid).await?;
        assert_eq!(&got.data, data, "data mismatch at index {}", idx);

        Block::from_cid_and_data(*cid, got.data).map_err(StorageError::VerificationFailed)?;
    }

    for (cid, _) in expected.iter().take(5_000) {
        store.delete(cid).await?;
        assert!(!store.has(cid).await);
        let missing = store.get(cid).await;
        assert!(matches!(missing, Err(StorageError::BlockNotFound(_))));
    }

    Ok(())
}
