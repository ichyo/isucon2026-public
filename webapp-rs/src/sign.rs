//! Image URL signatures (§5.2): `sig = b64url_nopad(HMAC^1000(key, "{exp}:{imageId}"))` + cache.
//! Signing semantics follow the reference `webapp/rust/src/image_signature.rs`; iterations 2..1000 use a
//! hand-rolled HMAC over pre-keyed SHA-256 states (exactly 2 compressions per iteration). Batches (`sign_many`)
//! run on the multi-buffer SIMD backend picked by `sha256mb::Backend::get()` (AVX-512 / AVX2 on CPUs without
//! SHA-NI) and are split over the available cores.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use sha2::digest::generic_array::GenericArray;
use sha2::Sha256;

use crate::model::Id;
use crate::sha256mb::{self, Backend};

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

/// The signed message `"{exp}:{imageId}"`.
fn message(image: Id, exp: i64) -> Vec<u8> {
    let mut msg = Vec::with_capacity(64);
    msg.extend_from_slice(exp.to_string().as_bytes());
    msg.push(b':');
    image.write_to(&mut msg);
    msg
}

/// Computes the raw 32-byte signature (uncached).
pub fn compute(key: &[u8; 32], image: Id, exp: i64) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("hmac key");
    mac.update(&message(image, exp));
    let mut v: [u8; 32] = mac.finalize().into_bytes().into();
    let inner = keyed_state(key, 0x36);
    let outer = keyed_state(key, 0x5c);
    for _ in 1..ITERATIONS {
        let ih = compress_32(&inner, &v);
        v = compress_32(&outer, &ih);
    }
    v
}

/// Raw signatures (uncached) of `(image, key)` jobs for one `exp`, on the calling thread. `Scalar` is `compute`
/// per job; a SIMD backend computes `backend.lanes()` signatures per pass (bit-identical results).
pub fn compute_batch(backend: Backend, jobs: &[(Id, [u8; 32])], exp: i64) -> Vec<[u8; 32]> {
    if backend == Backend::Scalar {
        return jobs.iter().map(|(img, key)| compute(key, *img, exp)).collect();
    }
    let keys: Vec<[u8; 32]> = jobs.iter().map(|j| j.1).collect();
    let msgs: Vec<Vec<u8>> = jobs.iter().map(|(img, _)| message(*img, exp)).collect();
    let refs: Vec<&[u8]> = msgs.iter().map(|m| &m[..]).collect();
    let mut out = vec![[0u8; 32]; jobs.len()];
    sha256mb::hmac_chain(backend, &keys, &refs, ITERATIONS, &mut out);
    out
}

/// One raw signature (uncached) on the fastest path: a 1-lane AVX-512VL pass is ~1.5x faster than scalar
/// software SHA-256 (0.41 vs 0.61 ms on Cascade Lake); AVX2 is not, and SHA-NI machines use `Scalar`.
pub fn compute_one(key: &[u8; 32], image: Id, exp: i64) -> [u8; 32] {
    match Backend::get() {
        b @ (Backend::Avx512 | Backend::Avx512Vl) => compute_batch(b, &[(image, *key)], exp)[0],
        _ => compute(key, image, exp),
    }
}

fn max_threads() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
}

/// `compute_batch` split over up to `available_parallelism` threads: whole SIMD passes per thread (a batch
/// that fits one pass stays on the calling thread); scalar: at least 4 jobs per thread.
pub fn compute_parallel(backend: Backend, jobs: &[(Id, [u8; 32])], exp: i64) -> Vec<[u8; 32]> {
    let (unit, min_units) = if backend == Backend::Scalar { (1, 4) } else { (backend.lanes(), 1) };
    let units = jobs.len().div_ceil(unit);
    let threads = (units / min_units).clamp(1, max_threads());
    if threads == 1 {
        return compute_batch(backend, jobs, exp);
    }
    let per = units.div_ceil(threads) * unit;
    std::thread::scope(|s| {
        let mut parts = jobs.chunks(per);
        let first = parts.next().expect("non-empty");
        let handles: Vec<_> = parts.map(|p| s.spawn(move || compute_batch(backend, p, exp))).collect();
        let mut out = compute_batch(backend, first, exp);
        for h in handles {
            out.extend(h.join().expect("sign thread"));
        }
        out
    })
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

/// A cached signature and where it came from (computed here, or pushed by a same-region peer node).
#[derive(Clone, Copy)]
struct Entry {
    sig: [u8; 32],
    peer: bool,
}

/// Cache hit/miss counters by path and source (`/metrics` → `signatures`).
#[derive(Default)]
pub struct SigStats {
    pub render_hit_local: AtomicU64,
    pub render_hit_peer: AtomicU64,
    pub render_miss: AtomicU64,
    pub verify_hit_local: AtomicU64,
    pub verify_hit_peer: AtomicU64,
    pub verify_miss: AtomicU64,
    pub computed: AtomicU64,
    pub peer_received: AtomicU64,
    pub shared_dropped: AtomicU64,
}

/// Newly computed signatures waiting to be pushed to the same-region peers (sigshare.rs).
pub const OUTBOX_MAX: usize = 65_536;

pub struct Signer {
    cache: Mutex<FxHashMap<(Id, i64), Entry>>,
    quantum: i64,
    /// Some(..) when same-region sharing is enabled
    outbox: Mutex<Option<Vec<(Id, i64, [u8; 32])>>>,
    pub stats: SigStats,
}

impl Signer {
    pub fn new(quantum: i64) -> Signer {
        Signer { cache: Mutex::new(FxHashMap::default()), quantum: quantum.max(1), outbox: Mutex::new(None), stats: SigStats::default() }
    }

    /// `exp` for a response generated at `now_s` (spec: floor(now) + 86400; optionally quantized down).
    pub fn exp_for(&self, now_s: i64) -> i64 {
        let e = now_s + URL_LIFETIME_S;
        e - e.rem_euclid(self.quantum)
    }

    fn hit(&self, e: &Entry, local: &AtomicU64, peer: &AtomicU64) {
        if e.peer { peer } else { local }.fetch_add(1, Relaxed);
    }

    /// Cache lookup on the render path (`JsonOut::image_url`).
    pub fn cached(&self, image: Id, exp: i64) -> Option<[u8; 32]> {
        let r = self.cache.lock().get(&(image, exp)).copied();
        match &r {
            Some(e) => self.hit(e, &self.stats.render_hit_local, &self.stats.render_hit_peer),
            None => {
                self.stats.render_miss.fetch_add(1, Relaxed);
            }
        }
        r.map(|e| e.sig)
    }

    /// Lookup without touching the hit/miss counters.
    pub fn peek(&self, image: Id, exp: i64) -> Option<[u8; 32]> {
        self.cache.lock().get(&(image, exp)).map(|e| e.sig)
    }

    /// Inserts a signature computed on this node (and queues it for the same-region peers).
    pub fn insert(&self, image: Id, exp: i64, sig: [u8; 32]) {
        self.cache.lock().insert((image, exp), Entry { sig, peer: false });
        self.share(&[(image, exp, sig)]);
    }

    fn share(&self, items: &[(Id, i64, [u8; 32])]) {
        self.stats.computed.fetch_add(items.len() as u64, Relaxed);
        let mut ob = self.outbox.lock();
        if let Some(v) = ob.as_mut() {
            let room = OUTBOX_MAX.saturating_sub(v.len());
            v.extend_from_slice(&items[..items.len().min(room)]);
            if items.len() > room {
                self.stats.shared_dropped.fetch_add((items.len() - room) as u64, Relaxed);
            }
        }
    }

    /// Enables queueing of computed signatures for `take_outbox` (same-region peers exist).
    pub fn enable_sharing(&self) {
        let mut ob = self.outbox.lock();
        if ob.is_none() {
            *ob = Some(Vec::new());
        }
    }

    /// Drains the queued signatures (sigshare.rs sends them).
    pub fn take_outbox(&self) -> Vec<(Id, i64, [u8; 32])> {
        self.outbox.lock().as_mut().map(std::mem::take).unwrap_or_default()
    }

    pub fn outbox_len(&self) -> usize {
        self.outbox.lock().as_ref().map_or(0, |v| v.len())
    }

    /// Signatures pushed by a same-region peer (computed by our own code there): cached, never re-shared.
    pub fn insert_peer(&self, items: &[(Id, i64, [u8; 32])]) {
        self.stats.peer_received.fetch_add(items.len() as u64, Relaxed);
        let mut c = self.cache.lock();
        for &(img, exp, sig) in items {
            c.entry((img, exp)).or_insert(Entry { sig, peer: true });
        }
    }

    /// Cached signature (computing + caching on miss). Expensive on miss (~0.1–0.5 ms): call off-lock.
    pub fn sign(&self, key: &[u8; 32], image: Id, exp: i64) -> [u8; 32] {
        if let Some(e) = self.cache.lock().get(&(image, exp)).copied() {
            return e.sig;
        }
        let s = compute_one(key, image, exp);
        self.insert(image, exp, s);
        s
    }

    /// Verification against the cache only: `Some(result)` if (image, exp) is cached, `None` otherwise
    /// (then compute the expected signature off-lock, e.g. `sigbatch::VerifyBatcher`).
    pub fn verify_cached(&self, image: Id, exp: i64, sig: &[u8; 32]) -> Option<bool> {
        let r = self.cache.lock().get(&(image, exp)).copied();
        match &r {
            Some(e) => self.hit(e, &self.stats.verify_hit_local, &self.stats.verify_hit_peer),
            None => {
                self.stats.verify_miss.fetch_add(1, Relaxed);
            }
        }
        r.map(|e| ct_eq(&e.sig, sig))
    }

    /// Full verification (computes + caches on miss — expensive, call off-lock).
    pub fn verify(&self, key: &[u8; 32], image: Id, exp: i64, sig: &[u8; 32]) -> bool {
        ct_eq(&self.sign(key, image, exp), sig)
    }

    /// Signs many (image, key) pairs for one exp; misses are computed in SIMD batches (`Backend::get()`) and
    /// split over the available cores when there are many.
    pub fn sign_many(&self, jobs: &[(Id, [u8; 32])], exp: i64) -> Vec<[u8; 32]> {
        let mut out = vec![[0u8; 32]; jobs.len()];
        let mut missing: Vec<usize> = Vec::new();
        // the same image often appears many times in one response (an author's profile image on every
        // memory/export item): compute it once, copy it to the duplicates afterwards
        let mut dups: Vec<(usize, usize)> = Vec::new();
        {
            let c = self.cache.lock();
            let mut first: FxHashMap<Id, usize> = FxHashMap::default();
            for (i, (img, _)) in jobs.iter().enumerate() {
                match c.get(&(*img, exp)) {
                    Some(e) => out[i] = e.sig,
                    None => match first.entry(*img) {
                        std::collections::hash_map::Entry::Occupied(e) => dups.push((i, *e.get())),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(i);
                            missing.push(i);
                        }
                    },
                }
            }
        }
        if missing.is_empty() {
            return out;
        }
        let todo: Vec<(Id, [u8; 32])> = missing.iter().map(|&i| jobs[i]).collect();
        let computed: Vec<(usize, [u8; 32])> = missing.iter().copied().zip(compute_parallel(Backend::get(), &todo, exp)).collect();
        let mut fresh = Vec::with_capacity(computed.len());
        {
            let mut c = self.cache.lock();
            for &(i, s) in &computed {
                out[i] = s;
                c.insert((jobs[i].0, exp), Entry { sig: s, peer: false });
                fresh.push((jobs[i].0, exp, s));
            }
        }
        self.share(&fresh);
        for (i, j) in dups {
            out[i] = out[j];
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

    pub fn stats_json(&self) -> serde_json::Value {
        let s = &self.stats;
        let g = |a: &AtomicU64| a.load(Relaxed);
        serde_json::json!({
            "backend": Backend::get().name(),
            "cache_entries": self.len(),
            "render": {"hit_local": g(&s.render_hit_local), "hit_peer": g(&s.render_hit_peer), "miss": g(&s.render_miss)},
            "verify": {"hit_local": g(&s.verify_hit_local), "hit_peer": g(&s.verify_hit_peer), "miss": g(&s.verify_miss)},
            "computed": g(&s.computed),
            "peer_received": g(&s.peer_received),
            "share_outbox": self.outbox_len(),
            "share_dropped": g(&s.shared_dropped),
        })
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
        // duplicates within one call (computed once, copied)
        let img = Id::new_v4();
        let jobs: Vec<_> = (0..12).map(|i| if i % 3 == 0 { (img, key) } else { (Id::new_v4(), key) }).collect();
        let r = s.sign_many(&jobs, 99);
        for (i, (img, k)) in jobs.iter().enumerate() {
            assert_eq!(r[i], compute(k, *img, 99));
        }
        assert_eq!(s.len(), 10 + 1 + 8);
    }

    /// Every backend this CPU supports gives `compute`'s bytes, for batch sizes 1..=17 (partial, full and
    /// multi-pass SIMD batches), random keys/images and exps of different lengths, plus the spec test vector.
    #[test]
    fn batch_backends_match_scalar() {
        let spec_key = hex32("eb10e9f6a56989e845c41c78108cee3a3515badd12d8c87185a2f27a99c9a00f");
        let spec_img = Id::parse("95a1b321-f2a8-4d4a-b068-4634710884b5").unwrap();
        let spec_exp = 1793495817;
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        // message lengths 39..57 bytes: one and two SHA blocks
        let exps = [spec_exp, 99, -5, 1_000_000_000_000_000_000, i64::MIN];
        for b in Backend::ALL.into_iter().filter(|b| b.is_supported()) {
            for n in 1..=17usize {
                let exp = exps[n % exps.len()];
                let jobs: Vec<(Id, [u8; 32])> = (0..n)
                    .map(|i| {
                        if exp == spec_exp && i == n / 2 {
                            return (spec_img, spec_key);
                        }
                        let mut k = [0u8; 32];
                        k.iter_mut().for_each(|x| *x = rnd() as u8);
                        (Id::from_u128(((rnd() as u128) << 64) | rnd() as u128), k)
                    })
                    .collect();
                let got = compute_batch(b, &jobs, exp);
                let par = compute_parallel(b, &jobs, exp);
                for (i, (img, k)) in jobs.iter().enumerate() {
                    let want = compute(k, *img, exp);
                    assert_eq!(got[i], want, "{b:?} n={n} i={i} exp={exp}");
                    assert_eq!(par[i], want, "{b:?} parallel n={n} i={i} exp={exp}");
                }
                let (img, k) = jobs[0];
                assert_eq!(compute_one(&k, img, exp), compute(&k, img, exp));
                assert_eq!(Signer::new(1).sign(&k, img, exp), compute(&k, img, exp));
                if exp == spec_exp {
                    let sig = encode_sig(&got[n / 2]);
                    assert_eq!(std::str::from_utf8(&sig).unwrap(), "Eu9_AZHks4gcdTAtvh37qqurEHf3vZNkKUVVlRVsWFk");
                }
            }
        }
    }
}
