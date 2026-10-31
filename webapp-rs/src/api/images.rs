//! Group C: GET /api/images/{imageId} (§5.3) — the hottest endpoint.
//!
//! Every response from this handler carries `Cache-Control: no-store` (errors too), so errors are returned as
//! `Ok(resp)`; the signature-mismatch abuse flag is therefore fed to the ban detector here (`ctx.on_abuse`).
//! Cost of a hit: a strict, allocation-free query parse, one short state read lock (image meta + owner key +
//! liveness), one signature-cache lookup and the bytes (zero-copy mmap slice / one WAL pread — inline when
//! the pages are resident, else in the blocking pool; never under the state lock).

use std::borrow::Cow;

use http::header::CACHE_CONTROL;
use http::HeaderValue;

use super::prelude::*;

const FORBIDDEN_BODY: &[u8] = b"{\"error\":\"image_forbidden\"}\n";

fn no_store(mut r: Resp) -> Resp {
    r.headers.push((CACHE_CONTROL, HeaderValue::from_static("no-store")));
    r
}

fn forbidden() -> Resp {
    no_store(Resp::bytes(403, "application/json", Bytes::from_static(FORBIDDEN_BODY)))
}

enum Deny {
    /// 403 image_forbidden (checks 1–4, 6, 7: not abuse)
    Forbidden,
    /// 403 image_forbidden + abuse `invalid_signature` (check 5)
    BadSignature,
    /// 500 (missing bytes, I/O error)
    Internal(String),
    /// 503 (the origin node of a large upload could not be reached in time; the benchmarker retries 503)
    Unavailable(String),
}

// GET /api/images/{imageId}?exp=…&sig=… — no auth, not pinned.
pub async fn get_image(ctx: Ctx, req: Req) -> HResult {
    Ok(match serve(ctx, req.param(0), req.raw_query()).await {
        Ok(bytes) => {
            crate::compressionlog::image(&req, &bytes);
            no_store(Resp::bytes(200, "image/jpeg", bytes))
        }
        Err(Deny::Forbidden) => forbidden(),
        Err(Deny::BadSignature) => {
            ctx.on_abuse(&req.ip, Abuse::InvalidSignature);
            forbidden()
        }
        Err(Deny::Internal(msg)) => {
            eprintln!("[images] GET {} failed: {msg}", req.path);
            no_store(ApiError::internal(msg).into_resp())
        }
        Err(Deny::Unavailable(msg)) => {
            eprintln!("[images] GET {} unavailable: {msg}", req.path);
            no_store(ApiError::unavailable("image temporarily unavailable").into_resp())
        }
    })
}

#[inline]
fn now_s() -> i64 {
    now_us().div_euclid(1_000_000)
}

async fn serve(ctx: Ctx, image_id: &str, query: Option<&str>) -> Result<Bytes, Deny> {
    use Deny::Forbidden;
    // 1. the raw query parses (Go url.ParseQuery) with exactly one `exp` and exactly one `sig`
    let (exp, sig) = parse_query(query.unwrap_or("")).ok_or(Forbidden)?;
    // 2. canonical base-10 int64, in the future
    let exp = parse_exp(&exp).ok_or(Forbidden)?;
    if exp <= now_s() {
        return Err(Forbidden);
    }
    // 3. strict unpadded base64url of exactly 32 bytes
    let sig = std::str::from_utf8(&sig).ok().and_then(sign::decode_sig).ok_or(Forbidden)?;
    // 4. the image exists and its owner is active; snapshot everything else needed (no I/O under the lock)
    let img = Id::parse(image_id).ok_or(Forbidden)?;
    let (key, live, loc) = {
        let st = ctx.read();
        let meta = st.images.get(&img).ok_or(Forbidden)?;
        let owner = st.user(meta.owner);
        if !owner.is_active() {
            return Err(Forbidden);
        }
        (owner.secret_hash, rules::image_live(&st, img, &meta), meta.loc)
    };
    // 5. signature (cache hit for every URL this node rendered; a miss costs 1000 HMACs, off the runtime)
    let ok = match ctx.signer.verify_cached(img, exp, &sig) {
        Some(ok) => ok,
        // miss: computed together with concurrent misses (one SIMD pass), cached + shared with the region peer
        None => sign::ct_eq(&ctx.sigbatch.expected(&ctx.signer, img, key, exp).await, &sig),
    };
    if !ok {
        return Err(Deny::BadSignature);
    }
    // 6. liveness (deleted post / replaced or deleted RealMoji / old profile image ⇒ dead at once)
    if !live {
        return Err(Forbidden);
    }
    // 7. exp re-checked
    if exp <= now_s() {
        return Err(Forbidden);
    }
    // 8. the stored bytes: straight from RAM when resident (no blocking-pool hop), else via the blocking pool;
    //    a large upload of another node is fetched from its origin (single-flight) and cached locally
    match ctx.images.try_read_cached(img, loc) {
        Some(bytes) => Ok(bytes),
        None => ctx.images.read(img, loc).await.map_err(|e| {
            if crate::images::ImageStore::is_unavailable(loc, &e) {
                Deny::Unavailable(format!("read {loc:?}: {e}"))
            } else {
                Deny::Internal(format!("read {loc:?}: {e}"))
            }
        }),
    }
}

/// Go `url.QueryUnescape`: `%XX` (hex) and `+` ⇒ space; a malformed escape is an error.
fn unescape(s: &[u8]) -> Option<Cow<'_, [u8]>> {
    if !s.iter().any(|&c| c == b'%' || c == b'+') {
        return Some(Cow::Borrowed(s));
    }
    fn hex(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'%' => {
                if i + 2 >= s.len() {
                    return None;
                }
                out.push(hex(s[i + 1])? << 4 | hex(s[i + 2])?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Some(Cow::Owned(out))
}

/// A query value after unescaping (usually borrowed: no `%` / `+`).
type Raw<'a> = Cow<'a, [u8]>;

/// Go `url.ParseQuery` semantics (`;` anywhere ⇒ error, malformed escapes ⇒ error, empty pieces skipped,
/// `key` without `=` ⇒ empty value), then exactly one `exp` and exactly one `sig`. Returns their raw values.
fn parse_query(q: &str) -> Option<(Raw<'_>, Raw<'_>)> {
    let q = q.as_bytes();
    if q.contains(&b';') {
        return None;
    }
    let (mut exp, mut sig) = (None, None);
    let (mut n_exp, mut n_sig) = (0u32, 0u32);
    for piece in q.split(|&c| c == b'&') {
        if piece.is_empty() {
            continue;
        }
        let (k, v) = match piece.iter().position(|&c| c == b'=') {
            Some(i) => (&piece[..i], &piece[i + 1..]),
            None => (piece, &piece[piece.len()..]),
        };
        let k = unescape(k)?;
        let v = unescape(v)?;
        match &*k {
            b"exp" => {
                n_exp += 1;
                exp.get_or_insert(v);
            }
            b"sig" => {
                n_sig += 1;
                sig.get_or_insert(v);
            }
            _ => {}
        }
    }
    if n_exp != 1 || n_sig != 1 {
        return None;
    }
    Some((exp?, sig?))
}

/// Canonical base-10 int64 (`strconv.FormatInt(ParseInt(s)) == s`: no sign `+`, no leading zeros, no spaces).
/// Negative values (and 0) can never satisfy `exp > now`, so they are rejected here already (same 403).
fn parse_exp(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 19 || s[0] == b'0' {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
    }
    i64::try_from(v).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pq(q: &str) -> Option<(String, String)> {
        parse_query(q).map(|(e, s)| (String::from_utf8_lossy(&e).into_owned(), String::from_utf8_lossy(&s).into_owned()))
    }

    #[test]
    fn query_parsing() {
        let ok = |e: &str, s: &str| Some((e.to_string(), s.to_string()));
        assert_eq!(pq("exp=1&sig=abc"), ok("1", "abc"));
        assert_eq!(pq("sig=abc&exp=1"), ok("1", "abc"));
        assert_eq!(pq("&&exp=1&&sig=abc&"), ok("1", "abc"));
        assert_eq!(pq("exp=1&sig=abc&x=y&z"), ok("1", "abc"));
        assert_eq!(pq("%65xp=%31&s%69g=a+b"), ok("1", "a b"));
        assert_eq!(pq("exp&sig="), ok("", ""));
        assert_eq!(pq("exp=1&sig=a=b"), ok("1", "a=b"));
        assert_eq!(pq(""), None);
        assert_eq!(pq("exp=1"), None);
        assert_eq!(pq("sig=abc"), None);
        assert_eq!(pq("exp=1&exp=1&sig=abc"), None);
        assert_eq!(pq("exp=1&sig=abc&sig=abc"), None);
        assert_eq!(pq("exp=1;sig=abc"), None);
        assert_eq!(pq("exp=1&sig=abc&x=;"), None);
        assert_eq!(pq("exp=1&sig=abc&x=%"), None);
        assert_eq!(pq("exp=1&sig=abc&x=%4"), None);
        assert_eq!(pq("exp=1&sig=abc&x=%zz"), None);
        assert_eq!(pq("exp=1&sig=abc&%=x"), None);
        assert_eq!(pq("exp=1&sig=abc&x=%41"), ok("1", "abc"));
        assert_eq!(pq("EXP=1&sig=abc"), None); // keys are case-sensitive
    }

    #[test]
    fn exp_parsing() {
        assert_eq!(parse_exp(b"1793495817"), Some(1793495817));
        assert_eq!(parse_exp(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_exp(b"9223372036854775808"), None);
        assert_eq!(parse_exp(b"99999999999999999999"), None);
        for bad in ["", "0", "01793495817", "+1793495817", "-5", " 1", "1 ", "1e9", "１"] {
            assert_eq!(parse_exp(bad.as_bytes()), None, "{bad:?}");
        }
    }
}
