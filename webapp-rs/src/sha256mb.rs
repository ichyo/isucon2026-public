//! Multi-buffer SHA-256 for the image-signature HMAC chain (§5.2) on x86_64 CPUs without SHA extensions.
//!
//! Computes many independent `HMAC-SHA256^n(key_i, msg_i)` chains at once, one chain per SIMD lane:
//! 16 lanes per AVX-512 zmm register, 8 per ymm register (AVX-512VL or AVX2). Every lane has its own key.
//!
//! Chain: `v_1 = HMAC(k, m)`, `v_{j+1} = HMAC(k, v_j)`. With the pre-keyed inner/outer states (IV compressed
//! with `k ^ ipad` / `k ^ opad`), each `HMAC(k, v)` of a 32-byte `v` is two compressions of a fixed-shape
//! block: 8 variable message words + constant padding (0x80, zeros, bit length 8 * (64 + 32) = 768). Between
//! iterations digests stay as big-endian state words: they are exactly the message words of the next block,
//! so nothing is byte-swapped inside the chain. The constant half of the block is folded into the round
//! constants and the first 16 schedule steps.
//!
//! Everything (key states, the first variable-length message, all fixed-shape iterations) runs in SIMD.
//! The scalar reference is `sign::compute`. The SIMD backends are compiled only on x86_64.

use std::sync::OnceLock;

/// Lanes of the widest backend; lane-sliced buffers are always this wide (unused lanes are ignored).
pub const MAX_LANES: usize = 16;

/// Env override for `Backend::get` (`scalar`, `avx2`, `avx512vl`, `avx512`, `auto`); unsupported ⇒ auto.
pub const BACKEND_ENV: &str = "ISUREAL_SIGN_BACKEND";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    /// One chain at a time through the `sha2` crate (which uses SHA-NI when present): `sign::compute`.
    Scalar,
    /// 8 lanes in ymm registers, AVX2 only.
    Avx2,
    /// 8 lanes in ymm registers with AVX-512VL rotates/ternary logic (all three vector ports on Intel).
    Avx512Vl,
    /// 16 lanes in zmm registers (AVX-512F); chunks of at most 8 lanes run on `Avx512Vl` instead.
    Avx512,
}

impl Backend {
    pub const ALL: [Backend; 4] = [Backend::Scalar, Backend::Avx2, Backend::Avx512Vl, Backend::Avx512];

    /// Chains computed per SIMD pass.
    pub fn lanes(self) -> usize {
        match self {
            Backend::Scalar => 1,
            Backend::Avx2 | Backend::Avx512Vl => 8,
            Backend::Avx512 => 16,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Backend::Scalar => "scalar",
            Backend::Avx2 => "avx2",
            Backend::Avx512Vl => "avx512vl",
            Backend::Avx512 => "avx512",
        }
    }

    pub fn parse(s: &str) -> Option<Backend> {
        Backend::ALL.into_iter().find(|b| b.name() == s)
    }

    /// Whether this CPU can run the backend.
    pub fn is_supported(self) -> bool {
        match self {
            Backend::Scalar => true,
            #[cfg(target_arch = "x86_64")]
            Backend::Avx2 => is_x86_feature_detected!("avx2"),
            #[cfg(target_arch = "x86_64")]
            Backend::Avx512Vl | Backend::Avx512 => {
                is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512vl")
            }
            #[cfg(not(target_arch = "x86_64"))]
            _ => false,
        }
    }

    /// Fastest backend for this CPU: SHA-NI ⇒ scalar (`sha2` with SHA-NI beats SIMD per signature and needs
    /// no batching), else AVX-512 ⇒ 16 lanes, else AVX2 ⇒ 8 lanes, else scalar.
    pub fn detect() -> Backend {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sha") {
                return Backend::Scalar;
            }
            if Backend::Avx512.is_supported() {
                return Backend::Avx512;
            }
            if Backend::Avx2.is_supported() {
                return Backend::Avx2;
            }
        }
        Backend::Scalar
    }

    /// The backend this process uses (cached): `detect()`, unless `ISUREAL_SIGN_BACKEND` names a supported one.
    pub fn get() -> Backend {
        static B: OnceLock<Backend> = OnceLock::new();
        *B.get_or_init(|| match std::env::var(BACKEND_ENV).ok().as_deref().and_then(Backend::parse) {
            Some(b) if b.is_supported() => b,
            _ => Backend::detect(),
        })
    }
}

/// `out[i] = HMAC-SHA256^iterations(keys[i], msgs[i])` (`iterations >= 1`), `backend.lanes()` chains per pass.
/// All messages must have the same length. Panics for `Backend::Scalar` (use `sign::compute`) and for a backend
/// this CPU does not support.
pub fn hmac_chain(backend: Backend, keys: &[[u8; 32]], msgs: &[&[u8]], iterations: usize, out: &mut [[u8; 32]]) {
    assert!(keys.len() == msgs.len() && keys.len() == out.len(), "hmac_chain: length mismatch");
    assert!(iterations >= 1, "hmac_chain: iterations must be >= 1");
    assert!(backend != Backend::Scalar && backend.is_supported(), "hmac_chain: {backend:?} unavailable");
    #[cfg(target_arch = "x86_64")]
    {
        let half: RunFn = x86::avx512vl::run;
        match backend {
            Backend::Avx2 => run_chunks(8, x86::avx2::run, None, keys, msgs, iterations, out),
            Backend::Avx512Vl => run_chunks(8, half, None, keys, msgs, iterations, out),
            Backend::Avx512 => run_chunks(16, x86::avx512::run, Some((8, half)), keys, msgs, iterations, out),
            Backend::Scalar => unreachable!(),
        }
    }
}

// ---------------------------------------------------------------------------------------------------------
// Lane-sliced buffers and the backend-independent setup

/// `[word][lane]`.
type Lanes = [u32; MAX_LANES];
type Block = [Lanes; 16];
type State = [Lanes; 8];
/// `(ipad key block, opad key block, padded message blocks, iterations) -> chain value`.
#[allow(dead_code)]
type RunFn = unsafe fn(&Block, &Block, &[Block], usize) -> State;

#[allow(dead_code)]
const IV: [u32; 8] = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];

#[allow(dead_code)]
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
    0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
    0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
    0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2,
];

// Fixed-shape block (32-byte message after one 64-byte key block): words 8..16 are constant.
#[allow(dead_code)]
const PAD8: u32 = 0x8000_0000;
#[allow(dead_code)]
const PAD15: u32 = 8 * (64 + 32);
#[allow(dead_code)]
const SSIG1_PAD15: u32 = ssig1c(PAD15);
#[allow(dead_code)]
const SSIG0_PAD8: u32 = ssig0c(PAD8);
#[allow(dead_code)]
const SSIG0_PAD15: u32 = ssig0c(PAD15);
/// `K[8 + i] + W[8 + i]` of the fixed-shape block.
#[allow(dead_code)]
const KW_PAD: [u32; 8] = {
    let w = [PAD8, 0, 0, 0, 0, 0, 0, PAD15];
    let mut o = [0u32; 8];
    let mut i = 0;
    while i < 8 {
        o[i] = K[8 + i].wrapping_add(w[i]);
        i += 1;
    }
    o
};

const fn ssig0c(x: u32) -> u32 {
    x.rotate_right(7) ^ x.rotate_right(18) ^ (x >> 3)
}
const fn ssig1c(x: u32) -> u32 {
    x.rotate_right(17) ^ x.rotate_right(19) ^ (x >> 10)
}

/// Lane-sliced `key ^ pad` block (keys are 32 bytes, zero-extended to the 64-byte block size).
#[allow(dead_code)]
fn key_block(keys: &[[u8; 32]], pad: u8) -> Block {
    let p = u32::from_ne_bytes([pad; 4]);
    let mut b = [[p; MAX_LANES]; 16];
    for (l, k) in keys.iter().enumerate() {
        for (j, w) in k.as_chunks::<4>().0.iter().enumerate() {
            b[j][l] ^= u32::from_be_bytes(*w);
        }
    }
    b
}

/// Lane-sliced, SHA-padded message blocks for messages hashed after one 64-byte key block.
#[allow(dead_code)]
fn msg_blocks(msgs: &[&[u8]]) -> Vec<Block> {
    let len = msgs.first().map_or(0, |m| m.len());
    let n = (len + 9).div_ceil(64);
    let bits = ((64 + len) as u64) * 8;
    let mut buf = vec![0u8; n * 64];
    let mut blocks = vec![[[0u32; MAX_LANES]; 16]; n];
    for (l, m) in msgs.iter().enumerate() {
        assert_eq!(m.len(), len, "hmac_chain: messages must have equal length");
        buf.fill(0);
        buf[..len].copy_from_slice(m);
        buf[len] = 0x80;
        buf[n * 64 - 8..].copy_from_slice(&bits.to_be_bytes());
        for (blk, bytes) in blocks.iter_mut().zip(buf.as_chunks::<64>().0) {
            for (j, w) in bytes.as_chunks::<4>().0.iter().enumerate() {
                blk[j][l] = u32::from_be_bytes(*w);
            }
        }
    }
    blocks
}

/// Runs `run` over chunks of `lanes` chains; a final chunk of at most `small.0` chains uses `small.1`.
#[allow(dead_code)]
fn run_chunks(
    lanes: usize,
    run: RunFn,
    small: Option<(usize, RunFn)>,
    keys: &[[u8; 32]],
    msgs: &[&[u8]],
    iterations: usize,
    out: &mut [[u8; 32]],
) {
    debug_assert!(lanes <= MAX_LANES);
    for ((k, m), o) in keys.chunks(lanes).zip(msgs.chunks(lanes)).zip(out.chunks_mut(lanes)) {
        let f = match small {
            Some((n, f)) if k.len() <= n => f,
            _ => run,
        };
        // SAFETY: `hmac_chain` checked that the CPU supports the backend's target features.
        let v = unsafe { f(&key_block(k, 0x36), &key_block(k, 0x5c), &msg_blocks(m), iterations) };
        for (l, o) in o.iter_mut().enumerate() {
            for (j, b) in o.as_chunks_mut::<4>().0.iter_mut().enumerate() {
                *b = v[j][l].to_be_bytes();
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------
// The algorithm, written once over a lane-vector type `V`. A backend module defines `V` and the primitives
// `splat add bsig0 bsig1 ssig0 ssig1 ch maj load store` (inlinable, same target features), then invokes
// `multibuffer!(<target_feature attributes>)`.

/// One round on the lane-vector state array `$s`, whose roles rotate by one slot per round (no moves).
#[allow(unused_macros)]
macro_rules! rnd {
    ($s:ident, $t:literal, $kw:expr) => {{
        const A: usize = (8 - ($t % 8)) % 8;
        let (a, b, c, d) = ($s[A], $s[(A + 1) % 8], $s[(A + 2) % 8], $s[(A + 3) % 8]);
        let (e, f, g, h) = ($s[(A + 4) % 8], $s[(A + 5) % 8], $s[(A + 6) % 8], $s[(A + 7) % 8]);
        let x = add(add(h, $kw), ch(e, f, g));
        let s1 = bsig1(e);
        $s[(A + 3) % 8] = add(add(d, x), s1);
        $s[(A + 7) % 8] = add(add(x, s1), add(bsig0(a), maj(a, b, c)));
    }};
}

/// Rounds whose message word is already in the 16-word ring `$w`.
#[allow(unused_macros)]
macro_rules! rnd_w {
    ($s:ident, $w:ident; $($t:literal)*) => { $( rnd!($s, $t, add(splat(K[$t]), $w[$t & 15])); )* };
}

/// Rounds with the general message schedule over the ring `$w` (t >= 16).
#[allow(unused_macros)]
macro_rules! rnd_sched {
    ($s:ident, $w:ident; $($t:literal)*) => { $( {
        const J: usize = $t & 15;
        $w[J] = add(add(ssig1($w[(J + 14) & 15]), $w[(J + 9) & 15]), add(ssig0($w[(J + 1) & 15]), $w[J]));
        rnd!($s, $t, add(splat(K[$t]), $w[J]));
    } )* };
}

#[allow(unused_macros)]
macro_rules! multibuffer {
    ($(#[$attr:meta])*) => {
        $(#[$attr])*
        #[inline]
        unsafe fn feed(st: &[V; 8], s: &[V; 8]) -> [V; 8] {
            let mut o = *st;
            for i in 0..8 {
                o[i] = add(o[i], s[i]);
            }
            o
        }

        /// Compression of a general 16-word block (with feed-forward).
        $(#[$attr])*
        #[inline]
        unsafe fn compress_block(st: &[V; 8], w: &[V; 16]) -> [V; 8] {
            let mut s = *st;
            let mut w = *w;
            rnd_w!(s, w; 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15);
            rnd_sched!(s, w; 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31);
            rnd_sched!(s, w; 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47);
            rnd_sched!(s, w; 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63);
            feed(st, &s)
        }

        /// Compression of the fixed-shape block `m || 0x80 || 0... || 768` (with feed-forward).
        $(#[$attr])*
        #[inline]
        unsafe fn compress_digest(st: &[V; 8], m: &[V; 8]) -> [V; 8] {
            let mut s = *st;
            let z = splat(0);
            let mut w = [m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7], z, z, z, z, z, z, z, z];
            rnd_w!(s, w; 0 1 2 3 4 5 6 7);
            rnd!(s, 8, splat(KW_PAD[0]));
            rnd!(s, 9, splat(KW_PAD[1]));
            rnd!(s, 10, splat(KW_PAD[2]));
            rnd!(s, 11, splat(KW_PAD[3]));
            rnd!(s, 12, splat(KW_PAD[4]));
            rnd!(s, 13, splat(KW_PAD[5]));
            rnd!(s, 14, splat(KW_PAD[6]));
            rnd!(s, 15, splat(KW_PAD[7]));
            // W16..W31 with the constant words W8 = PAD8, W9..W14 = 0, W15 = PAD15 folded in (ring: W_t at t & 15)
            w[0] = add(ssig0(w[1]), w[0]);
            rnd_w!(s, w; 16);
            w[1] = add(add(ssig0(w[2]), w[1]), splat(SSIG1_PAD15));
            rnd_w!(s, w; 17);
            w[2] = add(add(ssig1(w[0]), ssig0(w[3])), w[2]);
            rnd_w!(s, w; 18);
            w[3] = add(add(ssig1(w[1]), ssig0(w[4])), w[3]);
            rnd_w!(s, w; 19);
            w[4] = add(add(ssig1(w[2]), ssig0(w[5])), w[4]);
            rnd_w!(s, w; 20);
            w[5] = add(add(ssig1(w[3]), ssig0(w[6])), w[5]);
            rnd_w!(s, w; 21);
            w[6] = add(add(ssig1(w[4]), ssig0(w[7])), add(w[6], splat(PAD15)));
            rnd_w!(s, w; 22);
            w[7] = add(add(ssig1(w[5]), w[0]), add(w[7], splat(SSIG0_PAD8)));
            rnd_w!(s, w; 23);
            w[8] = add(add(ssig1(w[6]), w[1]), splat(PAD8));
            rnd_w!(s, w; 24);
            w[9] = add(ssig1(w[7]), w[2]);
            rnd_w!(s, w; 25);
            w[10] = add(ssig1(w[8]), w[3]);
            rnd_w!(s, w; 26);
            w[11] = add(ssig1(w[9]), w[4]);
            rnd_w!(s, w; 27);
            w[12] = add(ssig1(w[10]), w[5]);
            rnd_w!(s, w; 28);
            w[13] = add(ssig1(w[11]), w[6]);
            rnd_w!(s, w; 29);
            w[14] = add(add(ssig1(w[12]), w[7]), splat(SSIG0_PAD15));
            rnd_w!(s, w; 30);
            w[15] = add(add(ssig1(w[13]), w[8]), add(ssig0(w[0]), splat(PAD15)));
            rnd_w!(s, w; 31);
            rnd_sched!(s, w; 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47);
            rnd_sched!(s, w; 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63);
            feed(st, &s)
        }

        $(#[$attr])*
        #[inline]
        unsafe fn load_block(b: &Block) -> [V; 16] {
            let mut o = [splat(0); 16];
            for i in 0..16 {
                o[i] = load(&b[i]);
            }
            o
        }

        /// HMAC chains of all lanes: see `RunFn`.
        $(#[$attr])*
        pub unsafe fn run(key_in: &Block, key_out: &Block, msg: &[Block], iterations: usize) -> State {
            let mut iv = [splat(0); 8];
            for i in 0..8 {
                iv[i] = splat(IV[i]);
            }
            let inner = compress_block(&iv, &load_block(key_in));
            let outer = compress_block(&iv, &load_block(key_out));
            let mut h = inner;
            for b in msg {
                h = compress_block(&h, &load_block(b));
            }
            let mut v = compress_digest(&outer, &h);
            for _ in 1..iterations {
                let ih = compress_digest(&inner, &v);
                v = compress_digest(&outer, &ih);
            }
            let mut out = [[0u32; MAX_LANES]; 8];
            for i in 0..8 {
                store(v[i], &mut out[i]);
            }
            out
        }
    };
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    /// 16 lanes in zmm registers.
    pub mod avx512 {
        use super::super::*;
        use core::arch::x86_64::*;

        type V = __m512i;

        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn splat(x: u32) -> V {
            _mm512_set1_epi32(x as i32)
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn add(a: V, b: V) -> V {
            _mm512_add_epi32(a, b)
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn bsig0(x: V) -> V {
            _mm512_ternarylogic_epi32::<0x96>(
                _mm512_ror_epi32::<2>(x),
                _mm512_ror_epi32::<13>(x),
                _mm512_ror_epi32::<22>(x),
            )
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn bsig1(x: V) -> V {
            _mm512_ternarylogic_epi32::<0x96>(
                _mm512_ror_epi32::<6>(x),
                _mm512_ror_epi32::<11>(x),
                _mm512_ror_epi32::<25>(x),
            )
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn ssig0(x: V) -> V {
            _mm512_ternarylogic_epi32::<0x96>(
                _mm512_ror_epi32::<7>(x),
                _mm512_ror_epi32::<18>(x),
                _mm512_srli_epi32::<3>(x),
            )
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn ssig1(x: V) -> V {
            _mm512_ternarylogic_epi32::<0x96>(
                _mm512_ror_epi32::<17>(x),
                _mm512_ror_epi32::<19>(x),
                _mm512_srli_epi32::<10>(x),
            )
        }
        /// `e ? f : g`
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn ch(e: V, f: V, g: V) -> V {
            _mm512_ternarylogic_epi32::<0xCA>(e, f, g)
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn maj(a: V, b: V, c: V) -> V {
            _mm512_ternarylogic_epi32::<0xE8>(a, b, c)
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn load(p: &Lanes) -> V {
            _mm512_loadu_si512(p.as_ptr().cast())
        }
        #[inline]
        #[target_feature(enable = "avx512f")]
        unsafe fn store(v: V, p: &mut Lanes) {
            _mm512_storeu_si512(p.as_mut_ptr().cast(), v)
        }

        multibuffer!(#[target_feature(enable = "avx512f")]);
    }

    /// 8 lanes in ymm registers with AVX-512VL.
    pub mod avx512vl {
        use super::super::*;
        use core::arch::x86_64::*;

        type V = __m256i;

        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn splat(x: u32) -> V {
            _mm256_set1_epi32(x as i32)
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn add(a: V, b: V) -> V {
            _mm256_add_epi32(a, b)
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn bsig0(x: V) -> V {
            _mm256_ternarylogic_epi32::<0x96>(
                _mm256_ror_epi32::<2>(x),
                _mm256_ror_epi32::<13>(x),
                _mm256_ror_epi32::<22>(x),
            )
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn bsig1(x: V) -> V {
            _mm256_ternarylogic_epi32::<0x96>(
                _mm256_ror_epi32::<6>(x),
                _mm256_ror_epi32::<11>(x),
                _mm256_ror_epi32::<25>(x),
            )
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn ssig0(x: V) -> V {
            _mm256_ternarylogic_epi32::<0x96>(
                _mm256_ror_epi32::<7>(x),
                _mm256_ror_epi32::<18>(x),
                _mm256_srli_epi32::<3>(x),
            )
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn ssig1(x: V) -> V {
            _mm256_ternarylogic_epi32::<0x96>(
                _mm256_ror_epi32::<17>(x),
                _mm256_ror_epi32::<19>(x),
                _mm256_srli_epi32::<10>(x),
            )
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn ch(e: V, f: V, g: V) -> V {
            _mm256_ternarylogic_epi32::<0xCA>(e, f, g)
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn maj(a: V, b: V, c: V) -> V {
            _mm256_ternarylogic_epi32::<0xE8>(a, b, c)
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn load(p: &Lanes) -> V {
            _mm256_loadu_si256(p.as_ptr().cast())
        }
        #[inline]
        #[target_feature(enable = "avx512f,avx512vl")]
        unsafe fn store(v: V, p: &mut Lanes) {
            _mm256_storeu_si256(p.as_mut_ptr().cast(), v)
        }

        multibuffer!(#[target_feature(enable = "avx512f,avx512vl")]);
    }

    /// 8 lanes in ymm registers, AVX2 only (rotates are shift+shift+or).
    pub mod avx2 {
        use super::super::*;
        use core::arch::x86_64::*;

        type V = __m256i;

        macro_rules! ror {
            ($x:expr, $n:literal) => {
                _mm256_or_si256(_mm256_srli_epi32::<$n>($x), _mm256_slli_epi32::<{ 32 - $n }>($x))
            };
        }

        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn splat(x: u32) -> V {
            _mm256_set1_epi32(x as i32)
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn add(a: V, b: V) -> V {
            _mm256_add_epi32(a, b)
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn xor3(a: V, b: V, c: V) -> V {
            _mm256_xor_si256(_mm256_xor_si256(a, b), c)
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn bsig0(x: V) -> V {
            xor3(ror!(x, 2), ror!(x, 13), ror!(x, 22))
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn bsig1(x: V) -> V {
            xor3(ror!(x, 6), ror!(x, 11), ror!(x, 25))
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn ssig0(x: V) -> V {
            xor3(ror!(x, 7), ror!(x, 18), _mm256_srli_epi32::<3>(x))
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn ssig1(x: V) -> V {
            xor3(ror!(x, 17), ror!(x, 19), _mm256_srli_epi32::<10>(x))
        }
        /// `((f ^ g) & e) ^ g`
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn ch(e: V, f: V, g: V) -> V {
            _mm256_xor_si256(_mm256_and_si256(_mm256_xor_si256(f, g), e), g)
        }
        /// `((a ^ b) & (b ^ c)) ^ b`; the next round's `b ^ c` is this round's `a ^ b` (CSE'd).
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn maj(a: V, b: V, c: V) -> V {
            _mm256_xor_si256(_mm256_and_si256(_mm256_xor_si256(a, b), _mm256_xor_si256(b, c)), b)
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn load(p: &Lanes) -> V {
            _mm256_loadu_si256(p.as_ptr().cast())
        }
        #[inline]
        #[target_feature(enable = "avx2")]
        unsafe fn store(v: V, p: &mut Lanes) {
            _mm256_storeu_si256(p.as_mut_ptr().cast(), v)
        }

        multibuffer!(#[target_feature(enable = "avx2")]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    /// The same algorithm over plain `[u32; 4]` lanes, so it is tested on every architecture.
    mod portable {
        use super::super::*;

        type V = [u32; 4];

        fn map(a: V, f: impl Fn(u32) -> u32) -> V {
            [f(a[0]), f(a[1]), f(a[2]), f(a[3])]
        }
        fn zip3(a: V, b: V, c: V, f: impl Fn(u32, u32, u32) -> u32) -> V {
            [f(a[0], b[0], c[0]), f(a[1], b[1], c[1]), f(a[2], b[2], c[2]), f(a[3], b[3], c[3])]
        }
        unsafe fn splat(x: u32) -> V {
            [x; 4]
        }
        unsafe fn add(a: V, b: V) -> V {
            zip3(a, b, b, |a, b, _| a.wrapping_add(b))
        }
        unsafe fn bsig0(x: V) -> V {
            map(x, |x| x.rotate_right(2) ^ x.rotate_right(13) ^ x.rotate_right(22))
        }
        unsafe fn bsig1(x: V) -> V {
            map(x, |x| x.rotate_right(6) ^ x.rotate_right(11) ^ x.rotate_right(25))
        }
        unsafe fn ssig0(x: V) -> V {
            map(x, ssig0c)
        }
        unsafe fn ssig1(x: V) -> V {
            map(x, ssig1c)
        }
        unsafe fn ch(e: V, f: V, g: V) -> V {
            zip3(e, f, g, |e, f, g| (e & f) ^ (!e & g))
        }
        unsafe fn maj(a: V, b: V, c: V) -> V {
            zip3(a, b, c, |a, b, c| (a & b) ^ (a & c) ^ (b & c))
        }
        unsafe fn load(p: &Lanes) -> V {
            [p[0], p[1], p[2], p[3]]
        }
        unsafe fn store(v: V, p: &mut Lanes) {
            p[..4].copy_from_slice(&v);
        }

        multibuffer!();
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            // splitmix64
            self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^ (z >> 31)
        }
        fn bytes(&mut self, n: usize) -> Vec<u8> {
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    fn reference(key: &[u8; 32], msg: &[u8], iterations: usize) -> [u8; 32] {
        let mut v = msg.to_vec();
        for _ in 0..iterations {
            let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).unwrap();
            m.update(&v);
            v = m.finalize().into_bytes().to_vec();
        }
        v.try_into().unwrap()
    }

    /// Checks `run` (with `lanes` lanes, optional small-chunk runner) against the reference.
    fn check(lanes: usize, run: RunFn, small: Option<(usize, RunFn)>) {
        let mut rng = Rng(lanes as u64 * 1000 + small.is_some() as u64);
        // lane counts 1..=17 (partial, full and multi-chunk); message lengths covering 1, 2 and 3 blocks
        for n in 1..=17usize {
            for &(len, iterations) in
                &[(47usize, 3usize), (0, 1), (55, 2), (56, 2), (64, 1), (119, 2), (120, 1), (130, 4)]
            {
                let keys: Vec<[u8; 32]> = (0..n).map(|_| rng.bytes(32).try_into().unwrap()).collect();
                let msgs: Vec<Vec<u8>> = (0..n).map(|_| rng.bytes(len)).collect();
                let refs: Vec<&[u8]> = msgs.iter().map(|m| &m[..]).collect();
                let mut out = vec![[0u8; 32]; n];
                run_chunks(lanes, run, small, &keys, &refs, iterations, &mut out);
                for i in 0..n {
                    assert_eq!(
                        out[i],
                        reference(&keys[i], &msgs[i], iterations),
                        "lanes={lanes} n={n} len={len} i={i}"
                    );
                }
            }
        }
        // full-length chains
        let keys: Vec<[u8; 32]> = (0..lanes + 1).map(|_| rng.bytes(32).try_into().unwrap()).collect();
        let msgs: Vec<Vec<u8>> = (0..lanes + 1).map(|_| rng.bytes(47)).collect();
        let refs: Vec<&[u8]> = msgs.iter().map(|m| &m[..]).collect();
        let mut out = vec![[0u8; 32]; lanes + 1];
        run_chunks(lanes, run, small, &keys, &refs, 1000, &mut out);
        for i in 0..lanes + 1 {
            assert_eq!(out[i], reference(&keys[i], &msgs[i], 1000));
        }
    }

    #[test]
    fn portable_matches_reference() {
        check(4, portable::run, None);
        check(4, portable::run, Some((2, portable::run)));
    }

    #[test]
    fn simd_backends_match_reference() {
        let mut rng = Rng(7);
        for b in Backend::ALL {
            if b == Backend::Scalar {
                continue;
            }
            if !b.is_supported() {
                eprintln!("skipping {b:?} (not supported by this CPU)");
                continue;
            }
            for n in [1usize, 3, 8, 9, 15, 16, 17, 33] {
                let keys: Vec<[u8; 32]> = (0..n).map(|_| rng.bytes(32).try_into().unwrap()).collect();
                let msgs: Vec<Vec<u8>> = (0..n).map(|_| rng.bytes(47)).collect();
                let refs: Vec<&[u8]> = msgs.iter().map(|m| &m[..]).collect();
                let mut out = vec![[0u8; 32]; n];
                hmac_chain(b, &keys, &refs, 5, &mut out);
                for i in 0..n {
                    assert_eq!(out[i], reference(&keys[i], &msgs[i], 5), "{b:?} n={n} i={i}");
                }
            }
            #[cfg(target_arch = "x86_64")]
            {
                let run: RunFn = match b {
                    Backend::Avx2 => x86::avx2::run,
                    Backend::Avx512Vl => x86::avx512vl::run,
                    _ => x86::avx512::run,
                };
                check(b.lanes(), run, None);
            }
        }
    }

    #[test]
    fn backend_names_roundtrip() {
        for b in Backend::ALL {
            assert_eq!(Backend::parse(b.name()), Some(b));
        }
        assert!(Backend::get().is_supported());
        assert!(Backend::detect().is_supported());
    }
}
