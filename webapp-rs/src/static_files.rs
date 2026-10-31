//! Static frontend served from memory (replaces nginx `try_files $uri /index.html`).

use std::path::Path;

use bytes::Bytes;
use rustc_hash::FxHashMap;

pub struct StaticFiles {
    files: FxHashMap<String, (Bytes, &'static str)>,
    index: Option<Bytes>,
}

fn ctype(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" | "htm" => "text/html",
        "js" | "mjs" => "application/javascript",
        "css" => "text/css",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        "json" => "application/json",
        "txt" => "text/plain",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

fn walk(base: &Path, dir: &Path, out: &mut FxHashMap<String, (Bytes, &'static str)>) -> std::io::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        if e.file_type()?.is_dir() {
            walk(base, &p, out)?;
        } else {
            let rel = p.strip_prefix(base).unwrap().to_string_lossy().replace('\\', "/");
            let key = format!("/{rel}");
            let data = std::fs::read(&p)?;
            out.insert(key.clone(), (Bytes::from(data), ctype(&key)));
        }
    }
    Ok(())
}

impl StaticFiles {
    pub fn load(dir: &Path) -> StaticFiles {
        let mut files = FxHashMap::default();
        if let Err(e) = walk(dir, dir, &mut files) {
            eprintln!("[static] WARN cannot read {}: {e}", dir.display());
        }
        let index = files.get("/index.html").map(|(b, _)| b.clone());
        StaticFiles { files, index }
    }

    /// (body, content type) for a non-API path: exact file, else index.html.
    pub fn get(&self, path: &str) -> Option<(Bytes, &'static str)> {
        let decoded = percent_encoding::percent_decode_str(path).decode_utf8_lossy();
        if let Some((b, t)) = self.files.get(decoded.as_ref()) {
            return Some((b.clone(), t));
        }
        if decoded.ends_with('/') {
            if let Some((b, t)) = self.files.get(&format!("{decoded}index.html")) {
                return Some((b.clone(), t));
            }
        }
        self.index.clone().map(|b| (b, "text/html"))
    }
}
