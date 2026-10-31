//! Image URL signatures (§5.2): `sig = b64url_nopad(HMAC^1000(key, "{exp}:{imageId}"))` + cache.
//! Signing semantics follow the reference `webapp/rust/src/image_signature.rs`; iterations 2..1000 use a
//! hand-rolled HMAC over pre-keyed SHA-256 states (exactly 2 compressions per iteration).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use sha2::digest::generic_array::GenericArray;
use sha2::Sha256;

use crate::model::Id;

pub const ITERATIONS: usize = 1000;
pub const URL_LIFETIME_S: i64 = 86400;
pub const SIG_LEN: usize = 43;

const IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

fn keyed_state(key: &[u8; 32], pad: u8) -> [u32; 8] {
    let mut block = [pad; 64];
    for i in 0..32 {
        block[i] ^= key[i];
    }
    let mut st = IV;
    sha2::compress256(&mut st, std::slice::from_ref(GenericArray::from_slice(&block)));
    st
}

#[inline]
fn compress_32(state: &[u32; 8], msg: &[u8; 32]) -> [u8; 32] {
    // one block: 32-byte message + 0x80 + zeros + bit length of (64 + 32) bytes
    let mut block = [0u8; 64];
    block[..32].copy_from_slice(msg);
    block[32] = 0x80;
    block[56..].copy_from_slice(&768u64.to_be_bytes());
    let mut st = *state;
    sha2::compress256(&mut st, std::slice::from_ref(GenericArray::from_slice(&block)));
    let mut out = [0u8; 32];
    for (i, w) in st.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
    }
    out
}

/// Computes the raw 32-byte signature (uncached).
pub fn compute(key: &[u8; 32], image: Id, exp: i64) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("hmac key");
    let mut msg = Vec::with_capacity(64);
    msg.extend_from_slice(exp.to_string().as_bytes());
    msg.push(b':');
    image.write_to(&mut msg);
    mac.update(&msg);
    let mut v: [u8; 32] = mac.finalize().into_bytes().into();
    let inner = keyed_state(key, 0x36);
    let outer = keyed_state(key, 0x5c);
    for _ in 1..ITERATIONS {
        let ih = compress_32(&inner, &v);
        v = compress_32(&outer, &ih);
    }
    v
}

pub fn encode_sig(sig: &[u8; 32]) -> [u8; SIG_LEN] {
    let mut out = [0u8; SIG_LEN];
    let n = URL_SAFE_NO_PAD.encode_slice(sig, &mut out).expect("b64 len");
    debug_assert_eq!(n, SIG_LEN);
    out
}

/// Strict unpadded base64url that decodes to exactly 32 bytes and re-encodes to the same string (§5.3 step 3).
pub fn decode_sig(s: &str) -> Option<[u8; 32]> {
    if s.len() != SIG_LEN {
        return None;
    }
    let v = URL_SAFE_NO_PAD.decode(s.as_bytes()).ok()?;
    let arr: [u8; 32] = v.try_into().ok()?;
    if &encode_sig(&arr)[..] != s.as_bytes() {
        return None;
    }
    Some(arr)
}

/// Constant-time comparison.
pub fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut d = 0u8;
    for i in 0..32 {
        d |= a[i] ^ b[i];
    }
    d == 0
}

pub struct Signer {
    cache: Mutex<FxHashMap<(Id, i64), [u8; 32]>>,
    quantum: i64,
}

impl Signer {
    pub fn new(quantum: i64) -> Signer {
        Signer { cache: Mutex::new(FxHashMap::default()), quantum: quantum.max(1) }
    }

    /// `exp` for a response generated at `now_s` (spec: floor(now) + 86400; optionally quantized down).
    pub fn exp_for(&self, now_s: i64) -> i64 {
        let e = now_s + URL_LIFETIME_S;
        e - e.rem_euclid(self.quantum)
    }

    pub fn cached(&self, image: Id, exp: i64) -> Option<[u8; 32]> {
        self.cache.lock().get(&(image, exp)).copied()
    }

    pub fn insert(&self, image: Id, exp: i64, sig: [u8; 32]) {
        self.cache.lock().insert((image, exp), sig);
    }

    /// Cached signature (computing + caching on miss). Expensive on miss (~0.1–0.5 ms): call off-lock.
    pub fn sign(&self, key: &[u8; 32], image: Id, exp: i64) -> [u8; 32] {
        if let Some(s) = self.cached(image, exp) {
            return s;
        }
        let s = compute(key, image, exp);
        self.insert(image, exp, s);
        s
    }

    /// Verification against the cache only: `Some(result)` if (image, exp) is cached, `None` otherwise
    /// (then call `verify` off-lock / in `spawn_blocking`).
    pub fn verify_cached(&self, image: Id, exp: i64, sig: &[u8; 32]) -> Option<bool> {
        self.cached(image, exp).map(|s| ct_eq(&s, sig))
    }

    /// Full verification (computes + caches on miss — expensive, call off-lock).
    pub fn verify(&self, key: &[u8; 32], image: Id, exp: i64, sig: &[u8; 32]) -> bool {
        ct_eq(&self.sign(key, image, exp), sig)
    }

    /// Signs many (image, key) pairs for one exp; uses 2 threads when there are many misses.
    pub fn sign_many(&self, jobs: &[(Id, [u8; 32])], exp: i64) -> Vec<[u8; 32]> {
        let mut out = vec![[0u8; 32]; jobs.len()];
        let mut missing: Vec<usize> = Vec::new();
        {
            let c = self.cache.lock();
            for (i, (img, _)) in jobs.iter().enumerate() {
                match c.get(&(*img, exp)) {
                    Some(s) => out[i] = *s,
                    None => missing.push(i),
                }
            }
        }
        if missing.is_empty() {
            return out;
        }
        let computed: Vec<(usize, [u8; 32])> = if missing.len() >= 8 {
            let mid = missing.len() / 2;
            let (a, b) = missing.split_at(mid);
            std::thread::scope(|s| {
                let h = s.spawn(|| a.iter().map(|&i| (i, compute(&jobs[i].1, jobs[i].0, exp))).collect::<Vec<_>>());
                let mut rb: Vec<_> = b.iter().map(|&i| (i, compute(&jobs[i].1, jobs[i].0, exp))).collect();
                let mut ra = h.join().expect("sign thread");
                ra.append(&mut rb);
                ra
            })
        } else {
            missing.iter().map(|&i| (i, compute(&jobs[i].1, jobs[i].0, exp))).collect()
        };
        let mut c = self.cache.lock();
        for (i, s) in computed {
            out[i] = s;
            c.insert((jobs[i].0, exp), s);
        }
        out
    }

    /// Drops entries whose exp has passed.
    pub fn purge(&self, now_s: i64) {
        self.cache.lock().retain(|(_, exp), _| *exp > now_s);
    }

    pub fn clear(&self) {
        self.cache.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex32(s: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap();
        }
        out
    }

    #[test]
    fn spec_test_vector() {
        let key = hex32("eb10e9f6a56989e845c41c78108cee3a3515badd12d8c87185a2f27a99c9a00f");
        let img = Id::parse("95a1b321-f2a8-4d4a-b068-4634710884b5").unwrap();
        let sig = compute(&key, img, 1793495817);
        assert_eq!(std::str::from_utf8(&encode_sig(&sig)).unwrap(), "Eu9_AZHks4gcdTAtvh37qqurEHf3vZNkKUVVlRVsWFk");
        assert_eq!(decode_sig("Eu9_AZHks4gcdTAtvh37qqurEHf3vZNkKUVVlRVsWFk"), Some(sig));
        assert_eq!(decode_sig("Eu9_AZHks4gcdTAtvh37qqurEHf3vZNkKUVVlRVsWFl"), None); // non-canonical tail
        assert_eq!(decode_sig("Eu9_AZHks4gcdTAtvh37qqurEHf3vZNkKUVVlRVsWFk="), None);
    }

    #[test]
    fn matches_plain_hmac_chain() {
        let key = [7u8; 32];
        let img = Id::new_v4();
        let mut v = format!("{}:{}", 1234567890, img).into_bytes();
        for _ in 0..ITERATIONS {
            let mut m = <Hmac<Sha256> as Mac>::new_from_slice(&key).unwrap();
            m.update(&v);
            v = m.finalize().into_bytes().to_vec();
        }
        assert_eq!(compute(&key, img, 1234567890).to_vec(), v);
        let s = Signer::new(1);
        let jobs: Vec<_> = (0..10).map(|_| (Id::new_v4(), key)).collect();
        let r = s.sign_many(&jobs, 99);
        for (i, (img, k)) in jobs.iter().enumerate() {
            assert_eq!(r[i], compute(k, *img, 99));
        }
        assert_eq!(s.len(), 10);
    }
}
