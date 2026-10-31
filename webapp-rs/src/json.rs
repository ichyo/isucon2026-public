//! Compact JSON writer with Go `encoding/json` compatible string escaping and float formatting, plus
//! deferred image-URL signing (placeholders patched in `finish`, off the state lock).
//!
//! ```ignore
//! let mut w = ctx.json();
//! w.obj();
//! w.k("id").id(post.id).k("caption").s(&post.caption).k("tags").arr();
//! w.s("a").s("b");
//! w.end(); // tags
//! w.end(); // object
//! ctx.finish(w, 200).await
//! ```

use std::io::Write as _;

use crate::model::{Date, Id, Ts};
use crate::sign::{self, Signer, SIG_LEN};
use crate::timefmt;

pub struct JsonOut {
    pub buf: Vec<u8>,
    /// one entry per open container: ("already has an item", closing bracket)
    stack: Vec<(bool, u8)>,
    after_key: bool,
    exp: i64,
    signer: &'static Signer,
    pending: Vec<(usize, Id, [u8; 32])>,
}

impl JsonOut {
    pub fn new(exp: i64, signer: &'static Signer) -> JsonOut {
        JsonOut { buf: Vec::with_capacity(1024), stack: Vec::with_capacity(8), after_key: false, exp, signer, pending: Vec::new() }
    }

    /// The `exp` used for every image URL of this response.
    pub fn exp(&self) -> i64 {
        self.exp
    }

    #[inline]
    fn pre_value(&mut self) {
        if self.after_key {
            self.after_key = false;
            return;
        }
        if let Some(top) = self.stack.last_mut() {
            if top.0 {
                self.buf.push(b',');
            } else {
                top.0 = true;
            }
        }
    }

    /// Starts an object (as a value).
    pub fn obj(&mut self) -> &mut Self {
        self.pre_value();
        self.buf.push(b'{');
        self.stack.push((false, b'}'));
        self
    }
    /// Starts an array (as a value).
    pub fn arr(&mut self) -> &mut Self {
        self.pre_value();
        self.buf.push(b'[');
        self.stack.push((false, b']'));
        self
    }
    /// Closes the innermost object/array (the writer tracks which).
    pub fn end(&mut self) -> &mut Self {
        let (_, closer) = self.stack.pop().expect("unbalanced end()");
        self.buf.push(closer);
        self
    }

    /// Writes an object key (handles the comma). Must be followed by exactly one value.
    pub fn k(&mut self, key: &str) -> &mut Self {
        if let Some(top) = self.stack.last_mut() {
            if top.0 {
                self.buf.push(b',');
            } else {
                top.0 = true;
            }
        }
        write_str(&mut self.buf, key);
        self.buf.push(b':');
        self.after_key = true;
        self
    }

    pub fn s(&mut self, v: &str) -> &mut Self {
        self.pre_value();
        write_str(&mut self.buf, v);
        self
    }
    pub fn opt_s(&mut self, v: Option<&str>) -> &mut Self {
        match v {
            Some(v) => self.s(v),
            None => self.null(),
        }
    }
    pub fn id(&mut self, v: Id) -> &mut Self {
        self.pre_value();
        self.buf.push(b'"');
        v.write_to(&mut self.buf);
        self.buf.push(b'"');
        self
    }
    pub fn opt_id(&mut self, v: Option<Id>) -> &mut Self {
        match v {
            Some(v) => self.id(v),
            None => self.null(),
        }
    }
    pub fn i(&mut self, v: i64) -> &mut Self {
        self.pre_value();
        let _ = write!(self.buf, "{v}");
        self
    }
    pub fn b(&mut self, v: bool) -> &mut Self {
        self.pre_value();
        self.buf.extend_from_slice(if v { b"true" } else { b"false" });
        self
    }
    pub fn null(&mut self) -> &mut Self {
        self.pre_value();
        self.buf.extend_from_slice(b"null");
        self
    }
    pub fn f64(&mut self, v: f64) -> &mut Self {
        self.pre_value();
        write_f64_go(&mut self.buf, v);
        self
    }
    /// RFC3339Nano UTC timestamp string.
    pub fn ts(&mut self, v: Ts) -> &mut Self {
        self.pre_value();
        self.buf.push(b'"');
        timefmt::write_ts(&mut self.buf, v);
        self.buf.push(b'"');
        self
    }
    /// `YYYY-MM-DD` string.
    pub fn date(&mut self, d: Date) -> &mut Self {
        self.pre_value();
        self.buf.push(b'"');
        timefmt::write_date(&mut self.buf, d);
        self.buf.push(b'"');
        self
    }
    /// Raw, already-encoded JSON value.
    pub fn raw(&mut self, json: &[u8]) -> &mut Self {
        self.pre_value();
        self.buf.extend_from_slice(json);
        self
    }

    /// Signed relative image URL string: `/api/images/{id}?exp={exp}&sig={sig}`. `key` = owner's secret_hash.
    pub fn image_url(&mut self, image: Id, key: &[u8; 32]) -> &mut Self {
        self.pre_value();
        self.buf.extend_from_slice(b"\"/api/images/");
        image.write_to(&mut self.buf);
        let _ = write!(self.buf, "?exp={}&sig=", self.exp);
        match self.signer.peek(image, self.exp) {
            Some(sig) => self.buf.extend_from_slice(&sign::encode_sig(&sig)),
            None => {
                let pos = self.buf.len();
                self.buf.extend_from_slice(&[b'?'; SIG_LEN]);
                self.pending.push((pos, image, *key));
            }
        }
        self.buf.push(b'"');
        self
    }

    /// Number of signatures still to compute in `finish`.
    pub fn pending_signatures(&self) -> usize {
        self.pending.len()
    }

    /// Patch work completed by another request, the background warmer, or a peer while this response
    /// was waiting for CPU. No signature computation occurs here.
    fn resolve_cached(&mut self) {
        self.pending.retain(|(pos, image, _)| {
            if let Some(sig) = self.signer.peek(*image, self.exp) {
                self.buf[*pos..*pos + SIG_LEN].copy_from_slice(&sign::encode_sig(&sig));
                false
            } else {
                true
            }
        });
    }

    /// Resolve misses in bounded passes, releasing the CPU budget between passes. Cache notifications
    /// can complete a response even while every signing permit is occupied.
    pub async fn finish_async(mut self) -> Result<Vec<u8>, tokio::task::JoinError> {
        while !self.pending.is_empty() {
            self.resolve_cached();
            if self.pending.is_empty() {
                break;
            }
            let permits = if self.pending.len() <= 8 { crate::rt::small_sign_permits() } else { crate::rt::sign_permits() };
            let acquire = permits.acquire();
            tokio::pin!(acquire);
            let permit = loop {
                // Register first, then check: insertions between the check and the await cannot be lost.
                let changed = self.signer.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                self.resolve_cached();
                if self.pending.is_empty() {
                    return Ok(self.finish());
                }
                tokio::select! {
                    p = &mut acquire => break p.expect("signature semaphore is never closed"),
                    _ = &mut changed => {}
                }
            };
            // One SIMD pass at most, regardless of the number of URLs in a response. In particular,
            // history responses no longer retain a semaphore permit for all their images.
            let mut jobs = Vec::with_capacity(16);
            for &(_, image, key) in &self.pending {
                if !jobs.iter().any(|(i, _)| *i == image) {
                    jobs.push((image, key));
                    if jobs.len() == 16 {
                        break;
                    }
                }
            }
            let signer = self.signer;
            let exp = self.exp;
            tokio::task::spawn_blocking(move || {
                // Keep the permit with the actual CPU work, including when the client task is cancelled.
                let _permit = permit;
                signer.sign_many_serial(&jobs, exp);
            })
            .await?;
        }
        Ok(self.finish())
    }

    /// Computes missing signatures, patches them in, appends the trailing `\n`. CPU-heavy when
    /// `pending_signatures() > 0` — prefer `App::finish` (runs it in the blocking pool).
    pub fn finish(mut self) -> Vec<u8> {
        debug_assert!(self.stack.is_empty(), "unclosed JSON container");
        if !self.pending.is_empty() {
            let jobs: Vec<(Id, [u8; 32])> = self.pending.iter().map(|(_, i, k)| (*i, *k)).collect();
            let sigs = self.signer.sign_many(&jobs, self.exp);
            for ((pos, _, _), sig) in self.pending.iter().zip(sigs.iter()) {
                self.buf[*pos..*pos + SIG_LEN].copy_from_slice(&sign::encode_sig(sig));
            }
        }
        self.buf.push(b'\n');
        self.buf
    }
}

/// Go encoding/json string escaping (HTML-safe: `<`,`>`,`&`, U+2028/2029).
pub fn write_str(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    let b = s.as_bytes();
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let esc: Option<&[u8]> = match c {
            b'"' => Some(b"\\\""),
            b'\\' => Some(b"\\\\"),
            b'\n' => Some(b"\\n"),
            b'\r' => Some(b"\\r"),
            b'\t' => Some(b"\\t"),
            0x08 => Some(b"\\b"),
            0x0c => Some(b"\\f"),
            b'<' => Some(b"\\u003c"),
            b'>' => Some(b"\\u003e"),
            b'&' => Some(b"\\u0026"),
            _ => None,
        };
        if let Some(e) = esc {
            buf.extend_from_slice(&b[start..i]);
            buf.extend_from_slice(e);
            i += 1;
            start = i;
            continue;
        }
        if c < 0x20 {
            buf.extend_from_slice(&b[start..i]);
            let _ = write!(buf, "\\u00{:02x}", c);
            i += 1;
            start = i;
            continue;
        }
        if c == 0xE2 && i + 2 < b.len() && b[i + 1] == 0x80 && (b[i + 2] == 0xA8 || b[i + 2] == 0xA9) {
            buf.extend_from_slice(&b[start..i]);
            buf.extend_from_slice(if b[i + 2] == 0xA8 { b"\\u2028" } else { b"\\u2029" });
            i += 3;
            start = i;
            continue;
        }
        i += 1;
    }
    buf.extend_from_slice(&b[start..]);
    buf.push(b'"');
}

/// Go encoding/json float64 formatting.
pub fn write_f64_go(buf: &mut Vec<u8>, f: f64) {
    if !f.is_finite() {
        buf.extend_from_slice(b"null");
        return;
    }
    let abs = f.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let s = format!("{:e}", f);
        let (m, e) = s.split_once('e').expect("exp format");
        let e: i32 = e.parse().expect("exp digits");
        buf.extend_from_slice(m.as_bytes());
        buf.push(b'e');
        if e < 0 {
            let _ = write!(buf, "-{}", -e);
        } else {
            let _ = write!(buf, "+{:02}", e);
        }
    } else {
        let _ = write!(buf, "{}", f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waiting_json_finishes_on_peer_cache_without_a_permit() {
        let signer: &'static Signer = Box::leak(Box::new(Signer::new(1)));
        signer.enable_sharing();
        let image = Id::new_v4();
        let key = [19; 32];
        let exp = 1_900_000_000;
        let sig = sign::compute(&key, image, exp);
        let mut w = JsonOut::new(exp, signer);
        w.image_url(image, &key);
        let _busy = crate::rt::small_sign_permits().acquire().await.unwrap();
        let request = tokio::spawn(w.finish_async());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!request.is_finished());
        signer.insert_peer(&[(image, exp, sig)]);
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), request).await.unwrap().unwrap().unwrap();
        let expected = format!("\"/api/images/{image}?exp={exp}&sig={}\"\n", std::str::from_utf8(&sign::encode_sig(&sig)).unwrap());
        assert_eq!(result, expected.into_bytes());
        assert!(signer.take_outbox().is_empty(), "peer cache completion must not compute and share a new signature");
    }

    #[tokio::test]
    async fn chunked_json_preserves_every_signature_and_duplicate() {
        let signer: &'static Signer = Box::leak(Box::new(Signer::new(1)));
        let mut w = JsonOut::new(12345, signer);
        let mut expected = JsonOut::new(12345, signer);
        w.arr();
        expected.arr();
        let jobs: Vec<_> = (0..37).map(|i| (Id::new_v4(), [i; 32])).collect();
        for (image, key) in jobs.iter().chain(jobs.iter()) {
            w.image_url(*image, key);
            expected.image_url(*image, key);
        }
        w.end();
        expected.end();
        let result = w.finish_async().await.unwrap();
        assert_eq!(result, expected.finish());
        for (image, key) in jobs {
            assert_eq!(signer.peek(image, 12345), Some(sign::compute(&key, image, 12345)));
        }
        assert_eq!(signer.len(), 37);
    }

    fn f(v: f64) -> String {
        let mut b = Vec::new();
        write_f64_go(&mut b, v);
        String::from_utf8(b).unwrap()
    }

    #[test]
    fn floats() {
        assert_eq!(f(17.23191016431788), "17.23191016431788");
        assert_eq!(f(35.0), "35");
        assert_eq!(f(-0.5), "-0.5");
        assert_eq!(f(0.0), "0");
        assert_eq!(f(1e-7), "1e-7");
        assert_eq!(f(-2.5e-8), "-2.5e-8");
        assert_eq!(f(1e-10), "1e-10");
        assert_eq!(f(0.000001), "0.000001");
        assert_eq!(f(1e21), "1e+21");
        assert_eq!(f(139.7671), "139.7671");
    }

    #[test]
    fn strings() {
        let mut b = Vec::new();
        write_str(&mut b, "a\"b\\c\n<>&\u{2028}\u{1}é渋谷\u{7f}");
        assert_eq!(String::from_utf8(b).unwrap(), "\"a\\\"b\\\\c\\n\\u003c\\u003e\\u0026\\u2028\\u0001é渋谷\u{7f}\"");
    }

    #[test]
    fn writer() {
        let signer: &'static Signer = Box::leak(Box::new(Signer::new(1)));
        let mut w = JsonOut::new(100, signer);
        w.obj();
        w.k("a").i(1).k("b").arr();
        w.s("x").b(true).null();
        w.obj().end();
        w.end();
        w.k("c").obj().k("d").f64(1.5).end();
        w.k("e").image_url(Id::NIL, &[1u8; 32]);
        w.end();
        assert_eq!(w.pending_signatures(), 1);
        let out = String::from_utf8(w.finish()).unwrap();
        let sig = sign::encode_sig(&sign::compute(&[1u8; 32], Id::NIL, 100));
        let sig = std::str::from_utf8(&sig).unwrap();
        assert_eq!(out, format!("{{\"a\":1,\"b\":[\"x\",true,null,{{}}],\"c\":{{\"d\":1.5}},\"e\":\"/api/images/00000000-0000-0000-0000-000000000000?exp=100&sig={sig}\"}}\n"));
        // cached now
        let mut w = JsonOut::new(100, signer);
        w.image_url(Id::NIL, &[1u8; 32]);
        assert_eq!(w.pending_signatures(), 0);
    }
}
