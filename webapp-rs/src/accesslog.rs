//! Optional per-request access log (`ISUREAL_ACCESS_LOG=<path>`, off by default) for offline analysis of the
//! benchmark's user flow: one TSV line per request, buffered, flushed every 500 ms by a background thread.
//! Columns: start_unix_us, dur_us, body_recv_us (-1 = not read), repl_wait_us, status, method, action, ip,
//! token (last 8 chars of Authorization, `-` if none), x-country-code, user-agent (≤ 60 chars), req_bytes,
//! resp_bytes, path, accept-encoding, content-type, content-encoding.
//! The first 14 columns are unchanged. Header fields use `-` for a missing header and an empty field for
//! an explicitly empty value; repeated values are comma-joined, with backslashes/control bytes escaped.

use http::{HeaderMap, HeaderName, HeaderValue};
use parking_lot::Mutex;
use std::io::{BufWriter, Write};
use std::sync::OnceLock;

static LOG: OnceLock<Mutex<BufWriter<std::fs::File>>> = OnceLock::new();

/// All values of a header as a single TSV-safe field, preserving an absent vs. explicitly empty header.
pub fn header_value(headers: &HeaderMap, name: HeaderName) -> String {
    header_values(headers.get_all(name).iter())
}

/// Also accepts the response's small header list without building a temporary HeaderMap.
pub fn header_values<'a>(values: impl Iterator<Item = &'a HeaderValue>) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let mut present = false;
    for value in values {
        if present {
            out.push(',');
        }
        present = true;
        for &b in value.as_bytes() {
            match b {
                b'\\' => out.push_str("\\\\"),
                b'\t' => out.push_str("\\t"),
                b' '..=b'~' => out.push(b as char),
                _ => write!(out, "\\x{b:02x}").expect("write to String"),
            }
        }
    }
    if !present {
        out.push('-');
    }
    out
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::ACCEPT_ENCODING;

    #[test]
    fn header_field_preserves_absence_empty_and_repeated_values() {
        let mut headers = HeaderMap::new();
        assert_eq!(header_value(&headers, ACCEPT_ENCODING), "-");
        headers.append(ACCEPT_ENCODING, HeaderValue::from_static(""));
        assert_eq!(header_value(&headers, ACCEPT_ENCODING), "");
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip;q=0"));
        headers.append(ACCEPT_ENCODING, HeaderValue::from_static("br, deflate"));
        assert_eq!(header_value(&headers, ACCEPT_ENCODING), "gzip;q=0,br, deflate");
    }

    #[test]
    fn header_field_escapes_tsv_delimiters_and_non_ascii_bytes() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_bytes(b"gzip\t, \\x80\x80").unwrap());
        assert_eq!(header_value(&headers, ACCEPT_ENCODING), "gzip\\t, \\\\x80\\x80");
    }
}
