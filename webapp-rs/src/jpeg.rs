//! JPEG validation: full decode with `jpeg-decoder` (same acceptance as the reference Rust
//! implementation, `webapp/rust/src/jpeg.rs`, which mirrors Go `image/jpeg`).
//!
//! The decode runs at 1/8 IDCT scale: every scan's entropy-coded data is still Huffman-decoded in full
//! (that is where every decode error comes from), but the IDCT / upsampling / color conversion work on
//! 1/64 of the pixels. Acceptance is identical to a full-size decode (see the tests); dimensions are
//! taken from the frame header before scaling.

use bytes::Bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dims {
    pub width: u16,
    pub height: u16,
}

/// Full (entropy) decode; `None` if the data is not a decodable JPEG. CPU-heavy: use `decode` from async
/// code.
pub fn decode_sync(data: &[u8]) -> Option<Dims> {
    let mut decoder = jpeg_decoder::Decoder::new(data);
    decoder.read_info().ok()?;
    let info = decoder.info()?;
    let dims = Dims { width: info.width, height: info.height };
    decoder.scale(1, 1).ok()?;
    decoder.decode().ok()?;
    Some(dims)
}

/// Reference behaviour (full-size decode), kept for tests.
#[cfg(test)]
pub fn decode_full_sync(data: &[u8]) -> Option<Dims> {
    let mut decoder = jpeg_decoder::Decoder::new(data);
    decoder.decode().ok()?;
    let info = decoder.info()?;
    Some(Dims { width: info.width, height: info.height })
}

/// Decode in the blocking pool, at most `ISUREAL_JPEG_CONCURRENCY` at a time (CPU budget, rt.rs).
pub async fn decode(data: Bytes) -> Option<Dims> {
    let _permit = crate::rt::jpeg_permits().acquire().await.ok()?;
    tokio::task::spawn_blocking(move || decode_sync(&data)).await.ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for dir in ["testdata/jpeg", "../webapp-rs/testdata/jpeg"] {
            if let Ok(rd) = std::fs::read_dir(dir) {
                for e in rd.flatten() {
                    if e.path().extension().map(|x| x == "jpg" || x == "jpeg").unwrap_or(false) {
                        out.push(std::fs::read(e.path()).unwrap());
                    }
                }
                break;
            }
        }
        out
    }

    #[test]
    fn scaled_decode_matches_full_decode() {
        let samples = samples();
        if samples.is_empty() {
            eprintln!("no testdata/jpeg samples; skipping");
            return;
        }
        let mut rng: u64 = 0x9e3779b97f4a7c15;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut checked = 0;
        for s in &samples {
            assert_eq!(decode_sync(s), decode_full_sync(s));
            // truncations
            for k in 1..40 {
                let cut = s.len() * k / 40;
                let t = &s[..cut];
                assert_eq!(decode_sync(t), decode_full_sync(t), "truncated at {cut}");
                checked += 1;
            }
            // random byte corruptions
            for _ in 0..300 {
                let mut c = s.clone();
                let n = 1 + (next() % 4) as usize;
                for _ in 0..n {
                    let i = (next() as usize) % c.len();
                    c[i] = (next() & 0xff) as u8;
                }
                assert_eq!(decode_sync(&c), decode_full_sync(&c));
                checked += 1;
            }
        }
        eprintln!("checked {checked} variants of {} samples", samples.len());
    }

    #[test]
    #[ignore]
    fn bench_scaled_vs_full() {
        let samples = samples();
        let big = samples.iter().max_by_key(|s| s.len()).unwrap();
        let n = 30;
        let t = std::time::Instant::now();
        for _ in 0..n {
            decode_full_sync(big).unwrap();
        }
        let full = t.elapsed() / n;
        let t = std::time::Instant::now();
        for _ in 0..n {
            decode_sync(big).unwrap();
        }
        let scaled = t.elapsed() / n;
        eprintln!("{} bytes: full {:?}, scaled {:?}", big.len(), full, scaled);
    }
}
