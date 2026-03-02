use neverust_core::storage::Block;
use std::error::Error;
use std::time::Instant;

fn make_payload(i: u64) -> [u8; 64] {
    let mut out = [0u8; 64];
    let mut x = i
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

fn main() -> Result<(), Box<dyn Error>> {
    let target = 10_000_000usize;
    let started = Instant::now();
    let mut digests: Vec<[u8; 32]> = Vec::with_capacity(target);

    println!("MODE=cid_collision_test");
    println!("TARGET_CIDS={}", target);
    println!("PAYLOAD_BYTES=64");

    for i in 0..target {
        let payload = make_payload(i as u64);
        let block = Block::new(payload.to_vec())?;
        let d = block.cid.hash().digest();
        if d.len() != 32 {
            return Err(format!("unexpected digest length: {}", d.len()).into());
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(d);
        digests.push(arr);

        if (i + 1) % 1_000_000 == 0 {
            let elapsed = started.elapsed().as_secs_f64();
            let rate = (i + 1) as f64 / elapsed.max(1e-9);
            eprintln!(
                "PROGRESS generated={} elapsed_sec={:.2} cids_per_sec={:.0}",
                i + 1,
                elapsed,
                rate
            );
        }
    }

    let gen_done = Instant::now();
    digests.sort_unstable();
    let sort_done = Instant::now();

    let mut collisions = 0usize;
    let mut unique = 0usize;
    if !digests.is_empty() {
        unique = 1;
        for i in 1..digests.len() {
            if digests[i] == digests[i - 1] {
                collisions += 1;
            } else {
                unique += 1;
            }
        }
    }

    let total_elapsed = started.elapsed().as_secs_f64();
    println!("GENERATED={}", target);
    println!("UNIQUE={}", unique);
    println!("COLLISIONS={}", collisions);
    println!("GEN_TIME_SEC={:.6}", (gen_done - started).as_secs_f64());
    println!("SORT_TIME_SEC={:.6}", (sort_done - gen_done).as_secs_f64());
    println!("TOTAL_TIME_SEC={:.6}", total_elapsed);
    Ok(())
}
