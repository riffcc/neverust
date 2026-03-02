use reqwest::blocking::{Body, Client};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{Read, Result as IoResult};
use std::time::{Duration, Instant};

struct ProgressReader<R: Read> {
    inner: R,
    total_bytes: u64,
    sent_bytes: u64,
    started: Instant,
    last_report: Instant,
}

impl<R: Read> ProgressReader<R> {
    fn new(inner: R, total_bytes: u64) -> Self {
        let now = Instant::now();
        Self {
            inner,
            total_bytes,
            sent_bytes: 0,
            started: now,
            last_report: now,
        }
    }

    fn report(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_report) < Duration::from_secs(1) {
            return;
        }
        let elapsed = now.duration_since(self.started).as_secs_f64();
        let pct = if self.total_bytes > 0 {
            (self.sent_bytes as f64 * 100.0) / self.total_bytes as f64
        } else {
            0.0
        };
        let mibps = if elapsed > 0.0 {
            (self.sent_bytes as f64 / 1048576.0) / elapsed
        } else {
            0.0
        };
        eprintln!(
            "UPLOAD_PROGRESS sent_bytes={} pct={:.2}% elapsed={:.2}s throughput_mibps={:.2}",
            self.sent_bytes, pct, elapsed, mibps
        );
        self.last_report = now;
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.sent_bytes = self.sent_bytes.saturating_add(n as u64);
            self.report();
        }
        Ok(n)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <src_file> <upload_url>",
            args.first().map_or("native_upload_client", String::as_str)
        );
        std::process::exit(2);
    }

    let src_file = &args[1];
    let upload_url = &args[2];
    let file = File::open(src_file)?;
    let total_bytes = file.metadata()?.len();
    let progress_reader = ProgressReader::new(file, total_bytes);

    println!("MODE=native_upload");
    println!("SRC_FILE={}", src_file);
    println!("UPLOAD_URL={}", upload_url);
    println!("TOTAL_BYTES={}", total_bytes);

    let client = Client::builder()
        .timeout(Duration::from_secs(21600))
        .build()?;

    let started = Instant::now();
    let response = client
        .post(upload_url)
        .header("content-type", "application/octet-stream")
        .header("content-length", total_bytes.to_string())
        .body(Body::new(progress_reader))
        .send()?;
    let status = response.status();
    let resp_text = response.text().unwrap_or_else(|_| String::new());
    let elapsed = started.elapsed().as_secs_f64();
    let mibps = if elapsed > 0.0 {
        (total_bytes as f64 / 1048576.0) / elapsed
    } else {
        0.0
    };

    println!("HTTP_STATUS={}", status.as_u16());
    println!("TIME_SEC={:.6}", elapsed);
    println!("THROUGHPUT_MiBPS={:.2}", mibps);
    println!("RESP_PREVIEW_BEGIN");
    println!("{}", resp_text.chars().take(160).collect::<String>());
    println!("RESP_PREVIEW_END");

    Ok(())
}
