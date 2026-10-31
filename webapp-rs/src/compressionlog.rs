//! Optional lossless-compression probe. Responses are only borrowed/cloned; their bytes never change.
//! One background worker, one queued job, and a byte cap keep profiling from building an unbounded backlog.
//! See docs/compression-measurement.md for configuration and the JSONL schema.

use std::io::{self, BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{mpsc, Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use flate2::write::{DeflateEncoder, GzEncoder};
use serde_json::json;

use crate::http::Req;

static LOG: OnceLock<Logger> = OnceLock::new();
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const ACTIONS: [&str; 2] = ["get_image", "export_posts"];

#[derive(Default)]
struct Counts {
    seen: AtomicU64,
    selected: AtomicU64,
    queued: AtomicU64,
    completed: AtomicU64,
    skipped_busy: AtomicU64,
    skipped_large: AtomicU64,
    errors: AtomicU64,
}

struct Logger {
    tx: mpsc::SyncSender<Job>,
    counts: Arc<[Counts; 2]>,
    sample_every: u64,
    max_bytes: usize,
}

struct Job {
    action: usize,
    start_unix_us: i64,
    path: String,
    accept_encoding: String,
    body: Bytes,
    // None: a standalone JPEG; Some: the original images in the export ZIP.
    images: Option<Vec<Bytes>>,
}

fn positive_env(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Ok(s) => s
            .parse::<u64>()
            .ok()
            .filter(|&n| n > 0)
            .ok_or_else(|| format!("{name} must be a positive integer")),
        Err(e) => Err(format!("{name}: {e}")),
    }
}

pub fn init() {
    let path = match std::env::var("ISUREAL_COMPRESSION_LOG") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };
    let settings = positive_env("ISUREAL_COMPRESSION_SAMPLE_EVERY", 100).and_then(|every| {
        let max = positive_env("ISUREAL_COMPRESSION_MAX_BYTES", 64 * 1024 * 1024)?;
        Ok((
            every,
            usize::try_from(max).map_err(|_| "ISUREAL_COMPRESSION_MAX_BYTES is too large".to_string())?,
        ))
    });
    let (sample_every, max_bytes) = match settings {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[compressionlog] disabled: {e}");
            return;
        }
    };
    let file = match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[compressionlog] cannot open {path}: {e}");
            return;
        }
    };
    let counts = Arc::new([Counts::default(), Counts::default()]);
    let (tx, rx) = mpsc::sync_channel(1);
    let worker_counts = counts.clone();
    if let Err(e) = std::thread::Builder::new().name("compressionlog".into()).spawn(move || {
        if let Err(e) = worker(rx, worker_counts, BufWriter::new(file), sample_every, max_bytes) {
            eprintln!("[compressionlog] worker stopped: {e}");
        }
    }) {
        eprintln!("[compressionlog] cannot start worker: {e}");
        return;
    }
    let _ = LOG.set(Logger {
        tx,
        counts,
        sample_every,
        max_bytes,
    });
    eprintln!("[compressionlog] writing to {path}; every={sample_every}, max_bytes={max_bytes}, level=1");
}

/// Called only for successful image responses. Disabled: one OnceLock lookup, no allocation/compression.
pub fn image(req: &Req, body: &Bytes) {
    if let Some(log) = LOG.get() {
        log.record(0, req, body, None);
    }
}

/// Called after building the existing Store-image ZIP, outside the state lock.
pub fn export(req: &Req, body: &Bytes, images: &[Bytes]) {
    if let Some(log) = LOG.get() {
        log.record(1, req, body, Some(images));
    }
}

impl Logger {
    fn record(&self, action: usize, req: &Req, body: &Bytes, images: Option<&[Bytes]>) {
        let c = &self.counts[action];
        let n = c.seen.fetch_add(1, Relaxed);
        // Separate counters avoid the much more frequent image requests starving export samples.
        if n % self.sample_every != 0 {
            return;
        }
        c.selected.fetch_add(1, Relaxed);
        let retained_bytes = images
            .unwrap_or_default()
            .iter()
            .fold(body.len(), |sum, b| sum.saturating_add(b.len()));
        if retained_bytes > self.max_bytes {
            c.skipped_large.fetch_add(1, Relaxed);
            return;
        }
        let job = Job {
            action,
            start_unix_us: crate::model::now_us() - req.start.elapsed().as_micros() as i64,
            path: req.path.clone(), // no signed query strings or Authorization values
            accept_encoding: crate::accesslog::header_value(&req.headers, http::header::ACCEPT_ENCODING),
            body: body.clone(),
            images: images.map(<[Bytes]>::to_vec),
        };
        self.enqueue(job);
    }

    fn enqueue(&self, job: Job) {
        let c = &self.counts[job.action];
        match self.tx.try_send(job) {
            Ok(()) => {
                c.queued.fetch_add(1, Relaxed);
            }
            Err(mpsc::TrySendError::Full(_)) => {
                c.skipped_busy.fetch_add(1, Relaxed);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                c.errors.fetch_add(1, Relaxed);
            }
        }
    }
}

// Count compressed output without retaining another copy of a potentially large image/ZIP.
#[derive(Default)]
struct ByteCount(u64);
impl Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn compressed_size(data: &[u8], gzip: bool) -> io::Result<u64> {
    let level = flate2::Compression::fast();
    if gzip {
        let mut enc = GzEncoder::new(ByteCount::default(), level);
        enc.write_all(data)?;
        Ok(enc.finish()?.0)
    } else {
        let mut enc = DeflateEncoder::new(ByteCount::default(), level);
        enc.write_all(data)?;
        Ok(enc.finish()?.0)
    }
}

fn measure(job: &Job, sample_every: u64) -> io::Result<serde_json::Value> {
    let start = Instant::now();
    let gzip_bytes = compressed_size(&job.body, true)?;
    let gzip_us = start.elapsed().as_micros() as u64;
    let images = job.images.as_deref().unwrap_or_else(|| std::slice::from_ref(&job.body));
    let start = Instant::now();
    let mut image_bytes = 0u64;
    let mut image_deflate_bytes = 0u64;
    for img in images {
        image_bytes += img.len() as u64;
        image_deflate_bytes += compressed_size(img, false)?;
    }
    let image_deflate_us = start.elapsed().as_micros() as u64;
    // Only the image payload lengths change; ZIP headers/CRC/names/posts.json stay identical in size.
    let zip_deflate_bytes = job
        .images
        .as_ref()
        .map(|_| job.body.len() as u64 - image_bytes + image_deflate_bytes);
    Ok(json!({
        "event": "sample", "start_unix_us": job.start_unix_us, "action": ACTIONS[job.action],
        "path": job.path, "accept_encoding": job.accept_encoding, "level": 1, "sample_every": sample_every,
        "raw_bytes": job.body.len(), "gzip_bytes": gzip_bytes, "gzip_us": gzip_us,
        "image_count": images.len(), "image_bytes": image_bytes,
        "image_deflate_bytes": image_deflate_bytes, "image_deflate_us": image_deflate_us,
        "zip_deflate_bytes": zip_deflate_bytes,
    }))
}

fn write_json(out: &mut impl Write, value: &serde_json::Value) -> io::Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    out.write_all(b"\n")
}

fn worker(rx: mpsc::Receiver<Job>, counts: Arc<[Counts; 2]>, mut out: impl Write, sample_every: u64, max_bytes: usize) -> io::Result<()> {
    let mut last_flush = Instant::now();
    let mut previous = String::new();
    loop {
        let disconnected = match rx.recv_timeout(FLUSH_INTERVAL.saturating_sub(last_flush.elapsed())) {
            Ok(job) => {
                match measure(&job, sample_every) {
                    Ok(sample) => {
                        write_json(&mut out, &sample)?;
                        counts[job.action].completed.fetch_add(1, Relaxed);
                    }
                    Err(e) => {
                        counts[job.action].errors.fetch_add(1, Relaxed);
                        eprintln!("[compressionlog] sample failed: {e}");
                    }
                }
                false
            }
            Err(mpsc::RecvTimeoutError::Timeout) => false,
            Err(mpsc::RecvTimeoutError::Disconnected) => true,
        };
        if disconnected || last_flush.elapsed() >= FLUSH_INTERVAL {
            let values: Vec<_> = counts
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    json!({
                        "action": ACTIONS[i], "seen": c.seen.load(Relaxed), "selected": c.selected.load(Relaxed),
                        "queued": c.queued.load(Relaxed), "completed": c.completed.load(Relaxed),
                        "skipped_busy": c.skipped_busy.load(Relaxed), "skipped_large": c.skipped_large.load(Relaxed),
                        "errors": c.errors.load(Relaxed),
                    })
                })
                .collect();
            let current = serde_json::to_string(&values)?;
            if current != previous {
                write_json(
                    &mut out,
                    &json!({
                        "event": "stats", "unix_us": crate::model::now_us(), "pid": std::process::id(),
                        "sample_every": sample_every, "max_bytes": max_bytes, "actions": values,
                    }),
                )?;
                previous = current;
            }
            out.flush()?;
            last_flush = Instant::now();
        }
        if disconnected {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn job(body: Bytes, images: Option<Vec<Bytes>>) -> Job {
        Job {
            action: usize::from(images.is_some()),
            start_unix_us: 0,
            path: "/test".into(),
            accept_encoding: "gzip".into(),
            body,
            images,
        }
    }

    #[test]
    fn sampling_is_per_endpoint_and_export_cap_includes_images() {
        let (tx, rx) = mpsc::sync_channel(1);
        let counts = Arc::new([Counts::default(), Counts::default()]);
        let logger = Logger {
            tx,
            counts: counts.clone(),
            sample_every: 2,
            max_bytes: 8,
        };
        let req = Req {
            method: http::Method::GET,
            path: "/api/images/example".into(),
            params: Default::default(),
            query: Some("exp=1&sig=secret".into()),
            headers: Default::default(),
            body: Arc::new(parking_lot::Mutex::new(None)),
            body_bytes: None,
            ip: "127.0.0.1".into(),
            start: Instant::now(),
            budget: None,
        };
        let body = Bytes::from_static(b"jpeg");
        for n in 0..3 {
            logger.record(0, &req, &body, None);
            if n % 2 == 0 {
                let sample = rx.try_recv().unwrap();
                assert_eq!(sample.path, req.path);
                assert_eq!(sample.accept_encoding, "-");
            } else {
                assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            }
        }
        // Export has its own sequence: its first candidate is over the combined body + images cap.
        logger.record(1, &req, &body, Some(&[Bytes::from_static(b"12345")]));
        assert_eq!(counts[1].skipped_large.load(Relaxed), 1);
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        logger.record(1, &req, &body, Some(std::slice::from_ref(&body))); // not selected
        logger.record(1, &req, &body, Some(std::slice::from_ref(&body))); // exactly at the cap
        assert_eq!(rx.try_recv().unwrap().action, 1);
        assert_eq!(counts[0].seen.load(Relaxed), 3);
        assert_eq!(counts[0].queued.load(Relaxed), 2);
        assert_eq!(counts[1].seen.load(Relaxed), 3);
        assert_eq!(counts[1].selected.load(Relaxed), 2);
        assert_eq!(counts[1].queued.load(Relaxed), 1);
    }

    #[test]
    fn sizes_match_lossless_encoders_including_finish() {
        for data in [Vec::new(), vec![42; 65536], (0..65536).map(|i| (i * 73 + i / 13) as u8).collect()] {
            let mut gzip = GzEncoder::new(Vec::new(), flate2::Compression::fast());
            gzip.write_all(&data).unwrap();
            let gzip = gzip.finish().unwrap();
            assert_eq!(compressed_size(&data, true).unwrap(), gzip.len() as u64);
            let mut restored = Vec::new();
            flate2::read::GzDecoder::new(&gzip[..]).read_to_end(&mut restored).unwrap();
            assert_eq!(restored, data);
            let mut deflate = DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
            deflate.write_all(&data).unwrap();
            let deflate = deflate.finish().unwrap();
            assert_eq!(compressed_size(&data, false).unwrap(), deflate.len() as u64);
            restored.clear();
            flate2::read::DeflateDecoder::new(&deflate[..]).read_to_end(&mut restored).unwrap();
            assert_eq!(restored, data);
        }
    }

    #[test]
    fn export_estimate_accounts_for_each_image_and_zip_overhead() {
        let images = vec![Bytes::from(vec![0; 10000]), Bytes::from_static(b"\xff\xd8abc\xff\xd9")];
        let zip = Bytes::from(crate::api::export::build_zip(&images, b"[]\n", 0));
        let original = zip.clone();
        let measured = measure(&job(zip, Some(images.clone())), 100).unwrap();
        let image_bytes = images.iter().map(|b| b.len() as u64).sum::<u64>();
        let compressed = images.iter().map(|b| compressed_size(b, false).unwrap()).sum::<u64>();
        assert_eq!(measured["image_count"], 2);
        assert_eq!(measured["image_bytes"], image_bytes);
        assert_eq!(measured["zip_deflate_bytes"], original.len() as u64 - image_bytes + compressed);
        assert_eq!(&original[..], crate::api::export::build_zip(&images, b"[]\n", 0));
        let empty = Bytes::from(crate::api::export::build_zip(&[], b"[]\n", 0));
        assert_eq!(
            measure(&job(empty.clone(), Some(vec![])), 1).unwrap()["zip_deflate_bytes"],
            empty.len()
        );
    }

    #[test]
    fn full_queue_drops_without_waiting_and_worker_flushes_valid_jsonl() {
        let (tx, rx) = mpsc::sync_channel(1);
        let counts = Arc::new([Counts::default(), Counts::default()]);
        let logger = Logger {
            tx,
            counts: counts.clone(),
            sample_every: 1,
            max_bytes: 1024,
        };
        logger.enqueue(job(Bytes::from_static(b"jpeg"), None));
        logger.enqueue(job(Bytes::from_static(b"drop"), None));
        assert_eq!(counts[0].queued.load(Relaxed), 1);
        assert_eq!(counts[0].skipped_busy.load(Relaxed), 1);
        drop(logger);
        let mut out = Vec::new();
        worker(rx, counts, &mut out, 1, 1024).unwrap();
        let lines: Vec<serde_json::Value> = std::str::from_utf8(&out)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["event"], "sample");
        assert_eq!(lines[1]["actions"][0]["completed"], 1);
        assert_eq!(lines[1]["actions"][0]["skipped_busy"], 1);
    }
}
