//! Optional per-request access log (`ISUREAL_ACCESS_LOG=<path>`, off by default) for offline analysis of the
//! benchmark's user flow: one TSV line per request, buffered, flushed every 500 ms by a background thread.
//! Columns: start_unix_us, dur_us, body_recv_us (-1 = not read), repl_wait_us, status, method, action, ip,
//! token (last 8 chars of Authorization, `-` if none), x-country-code, user-agent (≤ 60 chars), req_bytes,
//! resp_bytes, path.

use parking_lot::Mutex;
use std::io::{BufWriter, Write};
use std::sync::OnceLock;

static LOG: OnceLock<Mutex<BufWriter<std::fs::File>>> = OnceLock::new();

pub fn init() {
    let path = match std::env::var("ISUREAL_ACCESS_LOG") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };
    match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => {
            let _ = LOG.set(Mutex::new(BufWriter::with_capacity(1 << 20, f)));
            std::thread::spawn(|| loop {
                std::thread::sleep(std::time::Duration::from_millis(500));
                if let Some(l) = LOG.get() {
                    let _ = l.lock().flush();
                }
            });
            eprintln!("[accesslog] writing to {path}");
        }
        Err(e) => eprintln!("[accesslog] cannot open {path}: {e}"),
    }
}

#[inline]
pub fn enabled() -> bool {
    LOG.get().is_some()
}

pub fn write(line: std::fmt::Arguments) {
    if let Some(l) = LOG.get() {
        let _ = l.lock().write_fmt(line);
    }
}
