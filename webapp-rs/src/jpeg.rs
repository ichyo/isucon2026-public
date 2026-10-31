//! JPEG validation with exactly the acceptance of `jpeg-decoder` 0.3 (the reference Rust implementation,
//! `webapp/rust/src/jpeg.rs`, does a full `jpeg_decoder::Decoder::decode`; any error ⇒ invalid).
//!
//! Two paths:
//! * `fast::validate` — an entropy-decode-only re-implementation of jpeg-decoder's baseline/extended
//!   sequential Huffman path: the same marker loop, segment parsers and checks, and every scan's
//!   entropy-coded data is Huffman-decoded in full (that is where every decode error comes from), but no
//!   coefficients are stored and there is no dequantization / IDCT / upsampling / color conversion and no
//!   worker threads. It only ever answers "accept" when jpeg-decoder certainly accepts; for anything it
//!   does not handle (progressive, lossless, arithmetic, 12-bit, MJPEG, > 4096 px, …) or any error it
//!   returns `None` and the verdict comes from:
//! * `decode_jd` — jpeg-decoder itself at 1/8 IDCT scale (same acceptance as a full-size decode, see the
//!   tests).
//!
//! Dimensions are taken from the frame header. The tests check `fast` against jpeg-decoder on a large
//! corpus of valid and corrupted variants (truncations, bit/byte flips, marker edits, …).

use bytes::Bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dims {
    pub width: u16,
    pub height: u16,
}

/// Full (entropy) decode; `None` if the data is not a decodable JPEG. CPU-heavy: use `decode` from async
/// code.
pub fn decode_sync(data: &[u8]) -> Option<Dims> {
    match fast::validate(data) {
        Some(d) => Some(d),
        // jpeg-decoder panics on some malformed inputs (Go's image/jpeg returns an error): treat as invalid
        None => std::panic::catch_unwind(|| decode_jd(data)).unwrap_or(None),
    }
}

/// jpeg-decoder at 1/8 IDCT scale: read_info → dims from the header → scale(1,1) → decode.
pub fn decode_jd(data: &[u8]) -> Option<Dims> {
    let mut decoder = jpeg_decoder::Decoder::new(data);
    decoder.read_info().ok()?;
    let info = decoder.info()?;
    let dims = Dims { width: info.width, height: info.height };
    decoder.scale(1, 1).ok()?;
    decoder.decode().ok()?;
    Some(dims)
}

/// The fast path alone (`None` = not accepted by the fast path; jpeg-decoder may still accept). For tests
/// and benchmarks.
pub fn decode_fast_only(data: &[u8]) -> Option<Dims> {
    fast::validate(data)
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

/// Entropy-decode-only validator mirroring jpeg-decoder 0.3.2 (`decoder.rs`, `parser.rs`, `huffman.rs`).
/// Every `None` means "not certain": the caller falls back to jpeg-decoder. Comments name the jpeg-decoder
/// code each check mirrors.
mod fast {
    use super::Dims;
    use std::cell::RefCell;

    /// Images larger than this in either dimension go to jpeg-decoder (they are rejected by every caller
    /// anyway, and it keeps all loop arithmetic far from jpeg-decoder's u16 wrap-arounds).
    const MAX_DIM: u16 = 4096;

    const SOF0: u8 = 0xC0;
    const SOF1: u8 = 0xC1;
    const DHT: u8 = 0xC4;
    const RST0: u8 = 0xD0;
    const RST7: u8 = 0xD7;
    const SOI: u8 = 0xD8;
    const EOI: u8 = 0xD9;
    const SOS: u8 = 0xDA;
    const DQT: u8 = 0xDB;
    const DRI: u8 = 0xDD;
    const APP0: u8 = 0xE0;
    const APP14: u8 = 0xEE;
    const APP15: u8 = 0xEF;
    const COM: u8 = 0xFE;

    struct Reader<'a> {
        d: &'a [u8],
        p: usize,
    }

    impl<'a> Reader<'a> {
        #[inline]
        fn u8(&mut self) -> Option<u8> {
            let b = *self.d.get(self.p)?;
            self.p += 1;
            Some(b)
        }
        #[inline]
        fn u16(&mut self) -> Option<u16> {
            let b = self.bytes(2)?;
            Some(u16::from_be_bytes([b[0], b[1]]))
        }
        #[inline]
        fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
            let end = self.p.checked_add(n)?;
            let b = self.d.get(self.p..end)?;
            self.p = end;
            Some(b)
        }
        /// parser.rs `read_length`: big-endian length including itself, must be ≥ 2.
        fn length(&mut self) -> Option<usize> {
            let l = self.u16()? as usize;
            if l < 2 {
                return None;
            }
            Some(l - 2)
        }
        /// decoder.rs `read_marker`: skips any bytes up to 0xFF, fill bytes, and stuffed 0xFF00.
        fn marker(&mut self) -> Option<u8> {
            loop {
                let rest = &self.d[self.p.min(self.d.len())..];
                let i = memchr::memchr(0xFF, rest)?;
                self.p += i + 1;
                let mut b = self.u8()?;
                while b == 0xFF {
                    b = self.u8()?;
                }
                if b != 0x00 {
                    return Some(b);
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    struct Component {
        id: u8,
        h: u8,
        v: u8,
        tq: u8,
        block_w: u32,
        block_h: u32,
    }

    struct Frame {
        baseline: bool,
        width: u16,
        height: u16,
        mcu_w: u32,
        mcu_h: u32,
        comps: Vec<Component>,
    }

    const LUT_BITS: u32 = 11;

    /// `Huff::ac` entry: bits to consume (5) | coefficient-index advance (6) << 5 | kind << 11. The entry
    /// applies only if index + advance ≤ 64, which is exactly when jpeg-decoder would read all of its
    /// symbols without hitting the end of the block (no run-length overflow).
    const AC_MORE: u16 = 1 << 11; // one or two coefficients, block continues
    const AC_END: u16 = 2 << 11; // optionally one coefficient, then EOB (EOBRUN 0): block ends

    /// Canonical Huffman table (huffman.rs `HuffmanTable`): LUT for codes ≤ LUT_BITS, maxcode/delta
    /// for longer codes. `decode` returns exactly the (length, symbol) jpeg-decoder's `decode` returns.
    struct Huff {
        /// (code length << 8) | symbol; 0 = code longer than LUT_BITS (or no code)
        lut: [u16; 1 << LUT_BITS],
        /// AC tables: up to two symbols per lookup, for the common cases only (a coefficient, a
        /// coefficient + coefficient, a coefficient + EOB, EOB); 0 = take the symbol-by-symbol path.
        ac: [u16; 1 << LUT_BITS],
        maxcode: [i32; 17],
        delta: [i32; 17],
        values: [u8; 256],
    }

    impl Huff {
        /// huffman.rs `HuffmanTable::new` + `derive_huffman_codes` (the only error: over-subscribed lengths).
        fn new(counts: &[u8], values: &[u8], is_ac: bool) -> Option<Box<Huff>> {
            let mut t = Self::canonical(counts, values)?;
            if is_ac {
                t.build_ac();
            }
            Some(t)
        }

        fn build_ac(&mut self) {
            for p in 0..1usize << LUT_BITS {
                let e1 = self.lut[p];
                self.ac[p] = 0;
                if e1 == 0 {
                    continue;
                }
                let (len1, r1, s1) = ((e1 >> 8) as u32, ((e1 >> 4) & 15) as u32, (e1 & 15) as u32);
                if s1 == 0 {
                    if r1 == 0 {
                        self.ac[p] = AC_END | len1 as u16; // EOB
                    }
                    continue; // ZRL / EOBRUN > 0: symbol path
                }
                let t1 = len1 + s1;
                let mut e = AC_MORE | ((r1 + 1) << 5) as u16 | t1 as u16;
                if t1 < LUT_BITS {
                    // a second symbol whose code lies entirely within the looked-up bits
                    let e2 = self.lut[(p << t1) & ((1 << LUT_BITS) - 1)];
                    let len2 = (e2 >> 8) as u32;
                    if e2 != 0 && len2 <= LUT_BITS - t1 {
                        let (r2, s2) = (((e2 >> 4) & 15) as u32, (e2 & 15) as u32);
                        if s2 == 0 && r2 == 0 {
                            // "advance" r1 + 2: the EOB is only read if the index after the
                            // coefficient is still < 64 (the block ends either way)
                            e = AC_END | ((r1 + 2) << 5) as u16 | (t1 + len2) as u16;
                        } else if s2 != 0 {
                            e = AC_MORE | ((r1 + r2 + 2) << 5) as u16 | (t1 + len2 + s2) as u16;
                        }
                    }
                }
                self.ac[p] = e;
            }
        }

        fn canonical(counts: &[u8], values: &[u8]) -> Option<Box<Huff>> {
            let mut t = Box::new(Huff {
                lut: [0; 1 << LUT_BITS],
                ac: [0; 1 << LUT_BITS],
                maxcode: [-1; 17],
                delta: [0; 17],
                values: [0; 256],
            });
            t.values[..values.len()].copy_from_slice(values);
            let mut k = 0usize; // symbol index
            let mut code: u32 = 0;
            let mut code_size: u32 = 0;
            let mut first = true;
            for len in 1..=16u32 {
                let n = counts[len as usize - 1] as usize;
                if n == 0 {
                    continue;
                }
                if first {
                    // derive_huffman_codes starts at code 0 with code_size = huffsize[0]
                    code_size = len;
                    first = false;
                }
                while code_size < len {
                    code <<= 1;
                    code_size += 1;
                }
                t.delta[len as usize] = k as i32 - code as i32;
                for _ in 0..n {
                    if code >= (1u32 << len) {
                        return None; // "bad huffman code length"
                    }
                    if len <= LUT_BITS {
                        let shift = LUT_BITS - len;
                        let start = (code << shift) as usize;
                        let e = ((len as u16) << 8) | values[k] as u16;
                        for x in &mut t.lut[start..start + (1usize << shift)] {
                            *x = e;
                        }
                    }
                    code += 1;
                    k += 1;
                }
                t.maxcode[len as usize] = code as i32 - 1;
            }
            Some(t)
        }

        /// `bits`: the next ≥ 16 bits of the stream, left-aligned. Returns (length, symbol).
        #[inline(always)]
        fn decode(&self, bits: u64) -> Option<(u32, u32)> {
            let e = self.lut[(bits >> (64 - LUT_BITS)) as usize];
            if e != 0 {
                return Some(((e >> 8) as u32, (e & 0xFF) as u32));
            }
            self.decode_slow(bits)
        }

        #[cold]
        fn decode_slow(&self, bits: u64) -> Option<(u32, u32)> {
            let code16 = (bits >> 48) as i32;
            for len in LUT_BITS + 1..=16 {
                let c = code16 >> (16 - len);
                if c <= self.maxcode[len as usize] {
                    return Some((len, self.values[(c + self.delta[len as usize]) as usize] as u32));
                }
            }
            None // "failed to decode huffman code"
        }
    }

    /// Bit reader over one de-stuffed entropy-coded segment followed by zeros (jpeg-decoder's
    /// `HuffmanDecoder` feeds zero bits once it has hit the marker that ends the segment).
    /// `BRANCHLESS`: refill before every symbol (faster on x86-64) instead of only when < 32 bits are left;
    /// both decode the same bit stream (a refill with ≥ 56 bits buffered re-ORs bits that are already there).
    struct Bits<'a, const BRANCHLESS: bool> {
        buf: &'a [u8],
        i: usize,
        bits: u64,
        n: u32,
    }

    impl<'a, const BRANCHLESS: bool> Bits<'a, BRANCHLESS> {
        fn new(buf: &'a [u8]) -> Self {
            Bits { buf, i: 0, bits: 0, n: 0 }
        }
        /// Brings `n` to 56..=63. `buf` ends with ≥ 8 zero bytes, so any 8-byte window that does not fit
        /// lies entirely in the zero tail.
        #[inline(always)]
        fn refill(&mut self) {
            let x = match self.buf.get(self.i..self.i + 8) {
                Some(b) => u64::from_be_bytes(b.try_into().unwrap()),
                None => 0,
            };
            self.bits |= x >> self.n;
            let k = (63 - self.n) >> 3;
            self.i += k as usize;
            self.n += k * 8;
        }
        #[inline(always)]
        fn ensure32(&mut self) {
            if BRANCHLESS || self.n < 32 {
                self.refill();
            }
        }
        #[inline(always)]
        fn consume(&mut self, c: u32) {
            debug_assert!(c <= self.n);
            self.bits <<= c;
            self.n -= c;
        }
        /// Bits consumed so far.
        fn consumed(&self) -> u64 {
            self.i as u64 * 8 - self.n as u64
        }
    }

    /// One block of a sequential scan (decoder.rs `decode_block` with spectral selection 0..64, Al = 0;
    /// huffman.rs `decode`, `decode_fast_ac`, `get_bits`, `receive_extend`). Only bit consumption and the
    /// error conditions matter: DC magnitude category > 11, or no Huffman code matching.
    #[inline(always)]
    fn block<const BL: bool>(b: &mut Bits<BL>, dc: &Huff, ac: &Huff, eob_run: &mut u32) -> Option<()> {
        b.ensure32();
        let (len, s) = dc.decode(b.bits)?;
        if s > 11 {
            return None; // "invalid DC difference magnitude category"
        }
        b.consume(len + s);
        if *eob_run > 0 {
            *eob_run -= 1;
            return Some(());
        }
        let mut k = 1u32;
        while k < 64 {
            b.ensure32();
            // common cases, up to two symbols at once; exactly the bits the symbol-by-symbol path below
            // consumes as long as the coefficient index stays ≤ 64
            let e = ac.ac[(b.bits >> (64 - LUT_BITS)) as usize] as u32;
            let adv = (e >> 5) & 63;
            if e != 0 && k + adv <= 64 {
                b.consume(e & 31);
                k += adv;
                if e & AC_END as u32 != 0 {
                    break; // EOBRUN = 0 (it is 0 here already)
                }
                continue;
            }
            let (len, rs) = ac.decode(b.bits)?;
            let r = rs >> 4;
            let s = rs & 15;
            if s == 0 {
                b.consume(len);
                if r == 15 {
                    k += 16;
                    continue;
                }
                // end of band: EOBRUN (also for r > 0 in a sequential scan, like jpeg-decoder)
                *eob_run = (1 << r) - 1;
                if r > 0 {
                    *eob_run += (b.bits >> (64 - r)) as u32;
                    b.consume(r);
                }
                break;
            }
            k += r;
            if k >= 64 {
                // jpeg-decoder's `decode_fast_ac` (taken iff code length + size ≤ 8) consumes the
                // magnitude bits before this check; its slow path does not.
                b.consume(if len + s <= 8 { len + s } else { len });
                break;
            }
            b.consume(len + s);
            k += 1;
        }
        Some(())
    }

    /// Entropy-coded segment starting at `p`: de-stuffed bytes (+ 8 zero bytes) into `out`; returns
    /// (marker code that ends it, position after the marker). `None` (= let jpeg-decoder decide) if the
    /// data ends first, or on FF FF.. 00 (an error in jpeg-decoder's `read_bits` iff it reads that far).
    fn segment(d: &[u8], mut p: usize, out: &mut Vec<u8>) -> Option<(u8, usize)> {
        out.clear();
        loop {
            let i = memchr::memchr(0xFF, d.get(p..)?)?;
            out.extend_from_slice(&d[p..p + i]);
            p += i;
            let mut q = p + 1;
            let mut b = *d.get(q)?;
            if b == 0x00 {
                out.push(0xFF);
                p += 2;
                continue;
            }
            while b == 0xFF {
                q += 1;
                b = *d.get(q)?;
            }
            if b == 0x00 {
                return None; // "FF 00 found where marker was expected"
            }
            out.extend_from_slice(&[0u8; 8]);
            return Some((b, q + 1));
        }
    }

    struct State {
        frame: Option<Frame>,
        dc: [Option<Box<Huff>>; 4],
        ac: [Option<Box<Huff>>; 4],
        qt: [bool; 4],
        restart_interval: u16,
        is_jfif: bool,
        adobe: Option<u8>,
        has_data: [bool; 4],
    }

    /// Measured on the c5 Cascade Lake: branchless refill ~13% faster; on Apple M the conditional one is.
    const BRANCHLESS_REFILL: bool = cfg!(target_arch = "x86_64");

    thread_local! {
        static SEG: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    }

    pub fn validate(d: &[u8]) -> Option<Dims> {
        validate_variant::<BRANCHLESS_REFILL>(d)
    }

    pub(super) fn validate_variant<const BL: bool>(d: &[u8]) -> Option<Dims> {
        SEG.with(|seg| {
            let mut seg = seg.try_borrow_mut().ok()?;
            let res = validate_with::<BL>(d, &mut seg);
            if seg.capacity() > 8 << 20 {
                *seg = Vec::new();
            }
            res
        })
    }

    fn validate_with<const BL: bool>(d: &[u8], seg: &mut Vec<u8>) -> Option<Dims> {
        // decode_internal: "first two bytes are not an SOI marker"
        if d.len() < 2 || d[0] != 0xFF || d[1] != SOI {
            return None;
        }
        let mut r = Reader { d, p: 2 };
        let mut st = State {
            frame: None,
            dc: [None, None, None, None],
            ac: [None, None, None, None],
            qt: [false; 4],
            restart_interval: 0,
            is_jfif: false,
            adobe: None,
            has_data: [false; 4],
        };
        let mut previous = SOI;
        let mut pending: Option<u8> = None;
        loop {
            let m = match pending.take() {
                Some(m) => m,
                None => r.marker()?,
            };
            match m {
                SOF0 | SOF1 => {
                    if st.frame.is_some() {
                        return None; // hierarchical
                    }
                    st.frame = Some(parse_sof(&mut r, m == SOF0)?);
                }
                SOS => {
                    let scan = parse_sos(&mut r, st.frame.as_ref()?)?;
                    let (m, p) = decode_scan::<BL>(&mut st, &scan, d, r.p, seg)?;
                    r.p = p;
                    pending = Some(m);
                }
                DQT => parse_dqt(&mut r, &mut st.qt)?,
                DHT => parse_dht(&mut r, &mut st)?,
                DRI => {
                    // parse_dri
                    if r.length()? != 2 {
                        return None;
                    }
                    st.restart_interval = r.u16()?;
                }
                COM => {
                    let l = r.length()?;
                    r.bytes(l)?;
                }
                APP0..=APP15 => parse_app(&mut r, m, &mut st)?,
                RST0..=RST7 => {
                    // "RST found outside of entropy-coded data"
                    if previous != SOS {
                        return None;
                    }
                }
                EOI => break,
                // other SOFn (progressive / lossless / arithmetic / differential), DAC, DNL, DHP, EXP,
                // SOI, TEM, RES, JPG, JPGn: errors or unsupported here
                _ => return None,
            }
            previous = m;
        }
        // decode_planes / compute_image
        let f = st.frame.as_ref()?;
        if (0..f.comps.len()).any(|i| !st.has_data[i]) {
            return None; // "not all components have data"
        }
        if f.comps.len() != 1 && !color_transform_ok(&st, f) {
            return None;
        }
        Some(Dims { width: f.width, height: f.height })
    }

    /// decoder.rs `determine_color_transform` + `choose_color_convert_func` (3 or 4 components).
    fn color_transform_ok(st: &State, f: &Frame) -> bool {
        #[derive(PartialEq)]
        enum T {
            Rgb,
            YCbCr,
            Cmyk,
            Ycck,
            Unsupported,
        }
        let n = f.comps.len();
        let t = (|| {
            if n == 3 {
                match (f.comps[0].id, f.comps[1].id, f.comps[2].id) {
                    (1, 2, 3) => return T::YCbCr,
                    (1, 34, 35) | (114, 103, 98) => return T::Unsupported, // JcsBgYcc / JcsBgRgb
                    (82, 71, 66) => return T::Rgb,
                    _ => {}
                }
                if st.is_jfif {
                    return T::YCbCr;
                }
            }
            match st.adobe {
                Some(0) => {
                    if n == 3 {
                        return T::Rgb;
                    } else if n == 4 {
                        return T::Cmyk;
                    }
                }
                Some(1) => return T::YCbCr,
                Some(2) => return T::Ycck,
                Some(_) => return T::Unsupported, // unreachable: rejected in parse_app
                None => {
                    if n == 4 {
                        return T::Cmyk;
                    }
                }
            }
            if n == 4 {
                T::Ycck
            } else if n == 3 {
                T::YCbCr
            } else {
                T::Unsupported
            }
        })();
        match n {
            3 => t == T::Rgb || t == T::YCbCr,
            4 => t == T::Cmyk || t == T::Ycck,
            _ => false,
        }
    }

    /// parser.rs `parse_sof` (SOF0/SOF1 = Huffman sequential DCT) + the checks in decode_internal.
    fn parse_sof(r: &mut Reader, baseline: bool) -> Option<Frame> {
        let length = r.length()?;
        if length <= 6 {
            return None;
        }
        // 8-bit only (12-bit: error in baseline, unsupported otherwise; others invalid for DCT)
        if r.u8()? != 8 {
            return None;
        }
        let height = r.u16()?;
        let width = r.u16()?;
        if height == 0 || width == 0 {
            return None; // DNL unsupported / zero width
        }
        if height > MAX_DIM || width > MAX_DIM {
            return None;
        }
        let count = r.u8()? as usize;
        if count == 0 || length != 6 + 3 * count {
            return None;
        }
        let mut comps: Vec<Component> = Vec::with_capacity(count);
        for _ in 0..count {
            let id = r.u8()?;
            if comps.iter().any(|c| c.id == id) {
                return None;
            }
            let hv = r.u8()?;
            let (h, v) = (hv >> 4, hv & 15);
            if !(1..=4).contains(&h) || !(1..=4).contains(&v) {
                return None;
            }
            let tq = r.u8()?;
            if tq > 3 {
                return None;
            }
            comps.push(Component { id, h, v, tq, block_w: 0, block_h: 0 });
        }
        if count != 1 && count != 3 && count != 4 {
            return None; // UnsupportedFeature::ComponentCount
        }
        // update_component_sizes
        let h_max = comps.iter().map(|c| c.h).max().unwrap();
        let v_max = comps.iter().map(|c| c.v).max().unwrap();
        let mcu_w = (width as u32).div_ceil(h_max as u32 * 8);
        let mcu_h = (height as u32).div_ceil(v_max as u32 * 8);
        for c in &mut comps {
            c.block_w = mcu_w * c.h as u32;
            c.block_h = mcu_h * c.v as u32;
        }
        // Upsampler::new / choose_upsampler at the full image size
        for c in &comps {
            let h1 = c.h == h_max || width == 1;
            let v1 = c.v == v_max || height == 1;
            let h2 = c.h * 2 == h_max;
            let v2 = c.v * 2 == v_max;
            let simple = (h1 || h2) && (v1 || v2);
            if !simple && (h_max % c.h != 0 || v_max % c.v != 0) {
                return None; // NonIntegerSubsamplingRatio
            }
        }
        Some(Frame { baseline, width, height, mcu_w, mcu_h, comps })
    }

    struct Scan {
        /// frame component index, dc table, ac table
        comps: Vec<(usize, usize, usize)>,
    }

    /// parser.rs `parse_sos` for a sequential frame.
    fn parse_sos(r: &mut Reader, f: &Frame) -> Option<Scan> {
        let length = r.length()?;
        if length == 0 {
            return None;
        }
        let count = r.u8()? as usize;
        if count == 0 || count > 4 || length != 4 + 2 * count {
            return None;
        }
        let mut comps: Vec<(usize, usize, usize)> = Vec::with_capacity(count);
        for _ in 0..count {
            let id = r.u8()?;
            let idx = f.comps.iter().position(|c| c.id == id)?;
            if comps.iter().any(|c| c.0 == idx) {
                return None; // duplicate
            }
            if idx < comps.iter().map(|c| c.0).max().unwrap_or(0) {
                return None; // order
            }
            let b = r.u8()?;
            let (td, ta) = ((b >> 4) as usize, (b & 15) as usize);
            if td > 3 || (f.baseline && td > 1) || ta > 3 || (f.baseline && ta > 1) {
                return None;
            }
            comps.push((idx, td, ta));
        }
        let blocks_per_mcu: u32 = comps.iter().map(|c| f.comps[c.0].h as u32 * f.comps[c.0].v as u32).sum();
        if count > 1 && blocks_per_mcu > 10 {
            return None;
        }
        let ss = r.u8()?;
        let mut se = r.u8()?;
        let b = r.u8()?;
        let (ah, al) = (b >> 4, b & 15);
        if al >= 8 {
            return None; // point transform ≥ precision
        }
        if se == 0 {
            se = 63;
        }
        if ss != 0 || se != 63 || ah != 0 || al != 0 {
            return None;
        }
        Some(Scan { comps })
    }

    /// parser.rs `parse_dqt`.
    fn parse_dqt(r: &mut Reader, qt: &mut [bool; 4]) -> Option<()> {
        let mut length = r.length()?;
        let mut defined = [false; 4];
        while length > 0 {
            let b = r.u8()?;
            let (pq, tq) = ((b >> 4) as usize, (b & 15) as usize);
            if pq > 1 || tq > 3 || length < 65 + 64 * pq {
                return None;
            }
            let t = r.bytes(64 * (1 + pq))?;
            let zero = if pq == 0 { t.contains(&0) } else { t.chunks_exact(2).any(|c| c == [0, 0]) };
            if zero {
                return None;
            }
            defined[tq] = true;
            length -= 65 + 64 * pq;
        }
        for i in 0..4 {
            qt[i] |= defined[i];
        }
        Some(())
    }

    /// parser.rs `parse_dht`.
    fn parse_dht(r: &mut Reader, st: &mut State) -> Option<()> {
        let mut length = r.length()?;
        let is_baseline = st.frame.as_ref().map(|f| f.baseline);
        let mut dc: [Option<Box<Huff>>; 4] = [None, None, None, None];
        let mut ac: [Option<Box<Huff>>; 4] = [None, None, None, None];
        while length > 17 {
            let b = r.u8()?;
            let (class, index) = (b >> 4, (b & 15) as usize);
            if class > 1 || (is_baseline == Some(true) && index > 1) || index > 3 {
                return None;
            }
            let counts = r.bytes(16)?;
            let size: usize = counts.iter().map(|&c| c as usize).sum();
            if size == 0 || size > 256 || size > length - 17 {
                return None;
            }
            let values = r.bytes(size)?;
            let t = Huff::new(counts, values, class == 1)?;
            if class == 0 {
                dc[index] = Some(t);
            } else {
                ac[index] = Some(t);
            }
            length -= 17 + size;
        }
        if length != 0 {
            return None;
        }
        for i in 0..4 {
            if let Some(t) = dc[i].take() {
                st.dc[i] = Some(t);
            }
            if let Some(t) = ac[i].take() {
                st.ac[i] = Some(t);
            }
        }
        Some(())
    }

    /// parser.rs `parse_app`: needs `length` bytes; JFIF / AVI1 / Adobe are interpreted.
    fn parse_app(r: &mut Reader, m: u8, st: &mut State) -> Option<()> {
        let length = r.length()?;
        let seg = r.bytes(length)?;
        if m == APP0 && length >= 5 {
            if &seg[..5] == b"JFIF\0" {
                st.is_jfif = true;
            } else if &seg[..5] == b"AVI1\0" {
                return None; // MJPEG (default Huffman tables): leave to jpeg-decoder
            }
        } else if m == APP14 && length >= 12 && &seg[..6] == b"Adobe\0" {
            if seg[11] > 2 {
                return None; // "invalid color transform in adobe app segment"
            }
            st.adobe = Some(seg[11]);
        }
        Some(())
    }

    /// decoder.rs `decode_scan` for a sequential Huffman scan. Returns the marker that follows the scan
    /// and the position after it.
    fn decode_scan<const BL: bool>(st: &mut State, scan: &Scan, d: &[u8], start: usize, seg: &mut Vec<u8>) -> Option<(u8, usize)> {
        let f = st.frame.as_ref()?;
        // "use of unset quantization table", "scan makes use of unset dc/ac huffman table"
        let mut tables: Vec<(&Huff, &Huff)> = Vec::with_capacity(4);
        for &(ci, td, ta) in &scan.comps {
            if !st.qt[f.comps[ci].tq as usize] {
                return None;
            }
            tables.push((st.dc[td].as_deref()?, st.ac[ta].as_deref()?));
        }
        // blocks of one MCU, in order
        let interleaved = scan.comps.len() > 1;
        let mut mcu: Vec<(&Huff, &Huff)> = Vec::with_capacity(10);
        let (max_x, max_y) = if interleaved {
            for (k, &(ci, _, _)) in scan.comps.iter().enumerate() {
                let c = &f.comps[ci];
                for _ in 0..c.h as u32 * c.v as u32 {
                    mcu.push(tables[k]);
                }
            }
            (f.mcu_w, f.mcu_h)
        } else {
            mcu.push(tables[0]);
            let c = &f.comps[scan.comps[0].0];
            (c.block_w, c.block_h)
        };
        // jpeg-decoder's loops stop at mcu * 8 >= image size (in pixels, not MCUs)
        let max_y = max_y.min((f.height as u32).div_ceil(8));
        let max_x = max_x.min((f.width as u32).div_ceil(8));

        let restart = st.restart_interval as u32;
        let mut mcus_left = restart;
        let mut expected_rst = 0u8;
        let mut eob_run = 0u32;

        let (mut marker, mut pos) = segment(d, start, seg)?;
        let mut b = Bits::<BL>::new(&seg[..]);
        for _y in 0..max_y {
            for _x in 0..max_x {
                if restart > 0 {
                    if mcus_left == 0 {
                        // take_marker: jpeg-decoder has certainly reached the RST marker iff fewer than 57
                        // unread data bits remain in the segment (its refill stops at > 56 buffered bits).
                        let data_bits = (seg.len() as u64 - 8) * 8;
                        if data_bits > b.consumed() + 56 || marker != RST0 + expected_rst {
                            return None;
                        }
                        let (m, p) = segment(d, pos, seg)?;
                        marker = m;
                        pos = p;
                        b = Bits::<BL>::new(&seg[..]);
                        eob_run = 0;
                        expected_rst = (expected_rst + 1) % 8;
                        mcus_left = restart;
                    }
                    mcus_left -= 1;
                }
                for &(dc, ac) in &mcu {
                    block(&mut b, dc, ac, &mut eob_run)?;
                }
            }
        }
        // After the scan jpeg-decoder skips RST markers with a slightly different loop than the main
        // one; a scan ended by RST is left to it.
        if (RST0..=RST7).contains(&marker) {
            return None;
        }
        for &(ci, _, _) in &scan.comps {
            st.has_data[ci] = true;
        }
        Some((marker, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn samples() -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for dir in ["testdata/jpeg", "../webapp-rs/testdata/jpeg"] {
            if let Ok(rd) = std::fs::read_dir(dir) {
                let mut paths: Vec<_> = rd.flatten().map(|e| e.path()).collect();
                paths.sort();
                for p in paths {
                    if p.extension().map(|x| x == "jpg" || x == "jpeg").unwrap_or(false) {
                        out.push(std::fs::read(p).unwrap());
                    }
                }
                break;
            }
        }
        out
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n.max(1) as u64) as usize
        }
    }

    /// Encodes a w×h image (smooth pattern + noise) with jpeg-encoder.
    fn encode(w: u16, h: u16, cfg: &EncCfg, seed: u64) -> Vec<u8> {
        use jpeg_encoder::{ColorType, Encoder, SamplingFactor};
        let mut rng = Rng(seed);
        let (ct, bpp) = match cfg.color {
            0 => (ColorType::Rgb, 3),
            1 => (ColorType::Luma, 1),
            2 => (ColorType::Cmyk, 4),
            3 => (ColorType::CmykAsYcck, 4),
            _ => (ColorType::Ycbcr, 3),
        };
        let mut px = vec![0u8; w as usize * h as usize * bpp];
        for y in 0..h as usize {
            for x in 0..w as usize {
                for c in 0..bpp {
                    let base = (x * (c + 1) * 3 + y * (5 - c) * 2) as u64;
                    let noise = if cfg.noisy { rng.next() % 96 } else { rng.next() % 8 };
                    px[(y * w as usize + x) * bpp + c] = ((base + noise) & 0xff) as u8;
                }
            }
        }
        let mut out = Vec::new();
        let mut enc = Encoder::new(&mut out, cfg.quality);
        let sf = [
            SamplingFactor::R_4_4_4,
            SamplingFactor::R_4_2_2,
            SamplingFactor::R_4_2_0,
            SamplingFactor::R_4_4_0,
            SamplingFactor::R_4_1_1,
            SamplingFactor::R_4_2_1,
            SamplingFactor::R_4_1_0,
            SamplingFactor::R_4_4_1,
        ][cfg.sampling % 8];
        enc.set_sampling_factor(sf);
        enc.set_progressive(cfg.progressive);
        enc.set_restart_interval(cfg.restart);
        enc.set_optimized_huffman_tables(cfg.optimize);
        enc.encode(&px, w, h, ct).expect("encode");
        out
    }

    #[derive(Clone, Debug)]
    struct EncCfg {
        color: u8,
        sampling: usize,
        progressive: bool,
        restart: u16,
        optimize: bool,
        quality: u8,
        noisy: bool,
    }

    /// Deterministic set of generated base images covering the encoder options.
    fn generated_bases(count: usize, max_side: u16) -> Vec<(String, Vec<u8>)> {
        let mut rng = Rng(0x5eed);
        let sizes: [(u16, u16); 12] =
            [(1, 1), (8, 8), (9, 7), (16, 16), (17, 33), (33, 17), (64, 48), (127, 65), (100, 100), (200, 150), (321, 241), (640, 480)];
        let mut out = Vec::new();
        for i in 0..count {
            let cfg = EncCfg {
                color: [0, 0, 0, 1, 1, 2, 3, 4][i % 8],
                sampling: rng.below(8),
                progressive: i % 7 == 3,
                restart: [0, 0, 0, 1, 2, 3, 5, 16][rng.below(8)],
                optimize: rng.below(2) == 0,
                quality: [5, 30, 50, 75, 90, 100][rng.below(6)],
                noisy: rng.below(3) == 0,
            };
            let (w, h) = sizes[rng.below(sizes.len())];
            let (w, h) = (w.min(max_side), h.min(max_side));
            out.push((format!("gen{i} {w}x{h} {cfg:?}"), encode(w, h, &cfg, i as u64)));
        }
        out
    }

    /// Marker segments: (start, end, marker); SOS segments include their entropy-coded data.
    fn segments(d: &[u8]) -> Vec<(usize, usize, u8)> {
        let mut out = Vec::new();
        if d.len() < 2 {
            return out;
        }
        out.push((0, 2, 0xD8));
        let mut p = 2;
        while p + 1 < d.len() && d[p] == 0xFF {
            let m = d[p + 1];
            if m == 0xD9 || (0xD0..=0xD7).contains(&m) {
                out.push((p, p + 2, m));
                if m == 0xD9 {
                    break;
                }
                p += 2;
                continue;
            }
            if p + 3 >= d.len() {
                break;
            }
            let mut end = p + 2 + (((d[p + 2] as usize) << 8) | d[p + 3] as usize);
            if end > d.len() {
                break;
            }
            if m == 0xDA {
                while end + 1 < d.len() && !(d[end] == 0xFF && d[end + 1] != 0 && !(0xD0..=0xD7).contains(&d[end + 1])) {
                    end += 1;
                }
                if end + 1 >= d.len() {
                    end = d.len();
                }
            }
            out.push((p, end, m));
            p = end;
        }
        out
    }

    /// Start of the first scan's entropy-coded data (= end of headers).
    fn header_end(d: &[u8]) -> usize {
        for (s, _, m) in segments(d) {
            if m == 0xDA {
                let len = ((d[s + 2] as usize) << 8) | d[s + 3] as usize;
                return (s + 2 + len).min(d.len());
            }
        }
        d.len().min(64)
    }

    /// Variants of `s`. `level` scales how many random mutations are made.
    fn variants(s: &[u8], level: usize, rng: &mut Rng) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = Vec::new();
        let n = s.len();
        let he = header_end(s);
        let segs = segments(s);
        let eoi = s.len().saturating_sub(2);
        out.push(s.to_vec());
        // truncations: every 1%, every byte of the headers (+32), the last 24 bytes; also with EOI appended
        let mut cuts: Vec<usize> = (0..100).map(|k| n * k / 100).collect();
        cuts.extend(0..(he + 32).min(n));
        cuts.extend(n.saturating_sub(24)..n);
        cuts.sort();
        cuts.dedup();
        for &c in &cuts {
            out.push(s[..c].to_vec());
            if c >= he.saturating_sub(4) || c % 7 == 0 {
                let mut t = s[..c].to_vec();
                t.extend_from_slice(&[0xFF, 0xD9]);
                out.push(t);
                if c % 3 == 0 {
                    let mut t = s[..c].to_vec();
                    t.extend_from_slice(&[0xFF, 0xD9, 0x12, 0x34]);
                    out.push(t);
                }
            }
        }
        // header bytes: every byte with a few edits
        for i in 0..he.min(n) {
            for e in 0..5u8 {
                let mut c = s.to_vec();
                c[i] = match e {
                    0 => c[i] ^ 0xFF,
                    1 => c[i].wrapping_add(1),
                    2 => c[i].wrapping_sub(1),
                    3 => 0,
                    _ => 0xFF,
                };
                if c[i] != s[i] {
                    out.push(c);
                }
            }
        }
        // single bit flips: all header bits for small headers, random elsewhere
        for i in 0..he.min(n).min(4096) {
            if (i & 3) == 0 || he < 400 {
                for bit in 0..8 {
                    let mut c = s.to_vec();
                    c[i] ^= 1 << bit;
                    out.push(c);
                }
            }
        }
        for _ in 0..40 * level {
            let mut c = s.to_vec();
            let i = rng.below(n);
            c[i] ^= 1 << rng.below(8);
            out.push(c);
        }
        // bit flips concentrated in the scan data / near its end
        for _ in 0..10 * level {
            let mut c = s.to_vec();
            let i = he + rng.below(n.saturating_sub(he).max(1));
            if i < n {
                c[i] ^= 1 << rng.below(8);
            }
            let j = n.saturating_sub(1 + rng.below(64.min(n)));
            c[j] ^= 1 << rng.below(8);
            out.push(c);
        }
        // byte replacements with special values anywhere
        const SPECIAL: [u8; 16] = [0x00, 0xFF, 0xD0, 0xD1, 0xD7, 0xD8, 0xD9, 0xDA, 0xC4, 0xDB, 0xDD, 0xC0, 0xC2, 0x01, 0x7F, 0x80];
        for _ in 0..30 * level {
            let mut c = s.to_vec();
            for _ in 0..1 + rng.below(3) {
                let i = rng.below(n);
                c[i] = if rng.below(3) == 0 { rng.next() as u8 } else { SPECIAL[rng.below(SPECIAL.len())] };
            }
            out.push(c);
        }
        // insertions into the scan data
        const INS: [&[u8]; 14] = [
            &[0xFF, 0xD0],
            &[0xFF, 0xD1],
            &[0xFF, 0xD7],
            &[0xFF, 0xD9],
            &[0xFF, 0x00],
            &[0xFF, 0xFF, 0x00],
            &[0xFF, 0xFF, 0xD9],
            &[0xFF, 0xFF, 0xD0],
            &[0xFF],
            &[0x00],
            &[0xFF, 0xC4, 0x00, 0x02],
            &[0xFF, 0xFE, 0x00, 0x04, 0x41, 0x42],
            &[0xFF, 0xD0, 0xFF, 0xD1],
            &[0xFF, 0xDA],
        ];
        for _ in 0..10 * level {
            let mut c = s.to_vec();
            let i = he + rng.below(n.saturating_sub(he) + 1);
            let i = i.min(n);
            let ins = INS[rng.below(INS.len())];
            c.splice(i..i, ins.iter().copied());
            out.push(c);
        }
        // deletions of byte runs in the scan data / headers
        for _ in 0..5 * level {
            let mut c = s.to_vec();
            let i = rng.below(n);
            let len = 1 + rng.below(8);
            let e = (i + len).min(n);
            c.drain(i..e);
            out.push(c);
        }
        // segment edits: remove / duplicate / swap with next / garbage before
        for (k, &(a, b, _m)) in segs.iter().enumerate() {
            let mut c = s.to_vec();
            c.drain(a..b);
            out.push(c);
            let mut c = s.to_vec();
            let dup = s[a..b].to_vec();
            c.splice(b..b, dup);
            out.push(c);
            if let Some(&(_, b2, _)) = segs.get(k + 1) {
                let mut c = s[..a].to_vec();
                c.extend_from_slice(&s[b..b2]);
                c.extend_from_slice(&s[a..b]);
                c.extend_from_slice(&s[b2..]);
                out.push(c);
            }
            if k > 0 {
                let mut c = s.to_vec();
                c.splice(a..a, [0x12u8, 0x00, 0xFF, 0x00, 0x34]);
                out.push(c);
            }
        }
        // DRI edits: insert a DRI before the first SOS, or change an existing one
        if let Some(&(a, _, _)) = segs.iter().find(|x| x.2 == 0xDA) {
            for ri in [0u16, 1, 2, 3, 7, 100, 0xFFFF] {
                let mut c = s.to_vec();
                let mut seg = vec![0xFF, 0xDD, 0x00, 0x04];
                seg.extend_from_slice(&ri.to_be_bytes());
                c.splice(a..a, seg);
                out.push(c);
            }
        }
        // EOI edits
        if n >= 2 && s[n - 2] == 0xFF && s[n - 1] == 0xD9 {
            for m in [0xD0u8, 0xD8, 0xDA, 0xC4, 0x00, 0xFE] {
                let mut c = s.to_vec();
                c[eoi + 1] = m;
                out.push(c);
            }
            let mut c = s.to_vec();
            c.extend_from_slice(b"trailing garbage \xFF\xD8\xFF");
            out.push(c);
            let mut c = s.to_vec();
            c.splice(eoi..eoi, [0xFF, 0xD0]);
            out.push(c);
            let mut c = s.to_vec();
            c.splice(eoi..eoi, [0xFF, 0xD0, 0xFF, 0xD1]);
            out.push(c);
            let mut c = s.to_vec();
            c.splice(eoi..eoi, [0xFF, 0xFF, 0xFF]);
            out.push(c);
        }
        out.extend(structural_edits(s));
        out
    }

    /// Targeted header edits: frame / component fields, color-transform cases, tables.
    fn structural_edits(s: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let segs = segments(s);
        let find = |m: u8| segs.iter().find(|x| x.2 == m).copied();
        let sof = segs.iter().find(|x| matches!(x.2, 0xC0..=0xCF) && x.2 != 0xC4 && x.2 != 0xC8 && x.2 != 0xCC).copied();
        if let Some((a, _, _)) = sof {
            // marker type
            for m in [0xC0u8, 0xC1, 0xC2, 0xC3, 0xC5, 0xC9, 0xCB, 0xCD] {
                let mut c = s.to_vec();
                c[a + 1] = m;
                out.push(c);
            }
            // precision
            for p in [0u8, 7, 12, 16] {
                let mut c = s.to_vec();
                c[a + 4] = p;
                out.push(c);
            }
            // dimensions
            for (h, w) in [(0u16, 8u16), (8, 0), (4097, 8), (8, 4097), (4096, 4096), (65535, 65535), (1, 1), (2, 3000), (3000, 2)] {
                let mut c = s.to_vec();
                c[a + 5..a + 7].copy_from_slice(&h.to_be_bytes());
                c[a + 7..a + 9].copy_from_slice(&w.to_be_bytes());
                out.push(c);
            }
            let nc = s[a + 9] as usize;
            // component ids / sampling / tq
            if nc == 3 {
                for ids in [[1u8, 34, 35], [82, 71, 66], [114, 103, 98], [1, 2, 4], [0, 1, 2], [1, 1, 2]] {
                    let mut c = s.to_vec();
                    for k in 0..3 {
                        c[a + 10 + 3 * k] = ids[k];
                    }
                    // keep the scan header consistent
                    if let Some((sa, _, _)) = find(0xDA) {
                        for k in 0..3 {
                            if sa + 5 + 2 * k < c.len() {
                                c[sa + 5 + 2 * k] = ids[k];
                            }
                        }
                    }
                    out.push(c.clone());
                    // ... and without JFIF
                    if let Some((ja, jb, _)) = find(0xE0) {
                        let mut c2 = c.clone();
                        c2.drain(ja..jb);
                        out.push(c2);
                    }
                }
            }
            for k in 0..nc {
                for hv in [0x00u8, 0x10, 0x01, 0x11, 0x12, 0x21, 0x22, 0x31, 0x13, 0x33, 0x41, 0x44, 0x45, 0x54] {
                    let mut c = s.to_vec();
                    c[a + 11 + 3 * k] = hv;
                    out.push(c);
                }
                for tq in [0u8, 1, 2, 3, 4] {
                    let mut c = s.to_vec();
                    c[a + 12 + 3 * k] = tq;
                    out.push(c);
                }
            }
            // component count
            for cnt in [0u8, 1, 2, 4, 5] {
                let mut c = s.to_vec();
                c[a + 9] = cnt;
                out.push(c);
            }
        }
        // APP segments: Adobe with each transform, AVI1, JFIF variants, bad lengths
        let app_at = 2;
        for t in [0u8, 1, 2, 3, 0xFF] {
            let mut seg = vec![0xFF, 0xEE, 0x00, 0x0E];
            seg.extend_from_slice(b"Adobe\0");
            seg.extend_from_slice(&[0x00, 0x64, 0x00, 0x00, 0x00, t]);
            let mut c = s.to_vec();
            c.splice(app_at..app_at, seg.clone());
            out.push(c);
            // after the frame header too
            if let Some((_, b, _)) = sof {
                let mut c = s.to_vec();
                c.splice(b..b, seg);
                out.push(c);
            }
        }
        for payload in [&b"AVI1\0"[..], b"JFIF\0", b"JFIF", b"JFXX\0", b""] {
            let mut seg = vec![0xFF, 0xE0];
            seg.extend_from_slice(&((payload.len() + 2) as u16).to_be_bytes());
            seg.extend_from_slice(payload);
            let mut c = s.to_vec();
            c.splice(app_at..app_at, seg);
            out.push(c);
        }
        for m in [0xE1u8, 0xE2, 0xED, 0xEE, 0xFE, 0xEF] {
            for len in [0u16, 1, 2, 3, 0xFFFF] {
                let mut c = s.to_vec();
                let mut seg = vec![0xFF, m];
                seg.extend_from_slice(&len.to_be_bytes());
                seg.extend_from_slice(b"Exif\0\0ICC_PROFILE\0\x01\x01");
                c.splice(app_at..app_at, seg);
                out.push(c);
            }
        }
        // DQT edits: zero entry, 16-bit precision, bad index, bad length
        if let Some((a, _, _)) = find(0xDB) {
            for (off, v) in [(4usize, 0x10u8), (4, 0x04), (4, 0x20), (5, 0), (6, 0), (2, 0), (3, 0x42), (3, 0x44)] {
                let mut c = s.to_vec();
                if a + off < c.len() {
                    c[a + off] = v;
                    out.push(c);
                }
            }
        }
        // DHT edits: counts / values / class / index
        if let Some((a, b, _)) = find(0xC4) {
            for i in a + 4..(a + 4 + 34).min(b) {
                for v in [0u8, 1, 2, 0x10, 0x11, 0x13, 0xFF] {
                    let mut c = s.to_vec();
                    c[i] = v;
                    out.push(c);
                }
            }
            // DC symbol values > 11
            for i in a + 21..(a + 33).min(b) {
                let mut c = s.to_vec();
                c[i] = 12 + (i % 4) as u8;
                out.push(c);
            }
        }
        // every DHT table: replace one AC symbol by every value class (r/s combinations)
        for &(a, b, m) in &segs {
            if m != 0xC4 {
                continue;
            }
            let mut p = a + 4;
            while p + 17 <= b {
                let counts = &s[p + 1..p + 17];
                let size: usize = counts.iter().map(|&x| x as usize).sum();
                if p + 17 + size > b {
                    break;
                }
                for j in [0usize, 1, size / 2, size.saturating_sub(1)] {
                    for v in [0x00u8, 0x0F, 0x10, 0x1F, 0xE0, 0xEF, 0xF0, 0xFA, 0xFF, 0x0B, 0x0C] {
                        if j < size {
                            let mut c = s.to_vec();
                            c[p + 17 + j] = v;
                            out.push(c);
                        }
                    }
                }
                p += 17 + size;
            }
        }
        // SOS edits
        if let Some((a, _, _)) = find(0xDA) {
            let ns = s.get(a + 4).copied().unwrap_or(0) as usize;
            let base = a + 5 + 2 * ns;
            for (off, v) in [(0usize, 1u8), (1, 0x3F), (1, 0x00), (1, 0x3E), (2, 0x01), (2, 0x10), (2, 0x11)] {
                let mut c = s.to_vec();
                if base + off < c.len() {
                    c[base + off] = v;
                    out.push(c);
                }
            }
            for k in 0..ns {
                for v in [0x00u8, 0x01, 0x10, 0x11, 0x22, 0x23, 0x32, 0x33, 0x44] {
                    let mut c = s.to_vec();
                    if a + 6 + 2 * k < c.len() {
                        c[a + 6 + 2 * k] = v;
                        out.push(c);
                    }
                }
            }
            for v in [0u8, 1, 2, 4, 5] {
                let mut c = s.to_vec();
                c[a + 4] = v;
                out.push(c);
            }
        }
        // the frame / tables after the scan, two frames, two SOIs
        if let (Some(sos), Some(sofs)) = (find(0xDA), sof) {
            let mut c = s.to_vec();
            let frame = s[sofs.0..sofs.1].to_vec();
            c.splice(sos.0..sos.0, frame);
            out.push(c);
            let mut c = s.to_vec();
            c.splice(sos.0..sos.0, [0xFF, 0xD8]);
            out.push(c);
        }
        out
    }

    /// Inputs that are not JPEGs at all (or barely).
    fn junk() -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = vec![
            vec![],
            vec![0xFF],
            vec![0xFF, 0xD8],
            vec![0xFF, 0xD8, 0xFF],
            vec![0xFF, 0xD8, 0xFF, 0xD9],
            vec![0xFF, 0xD9],
            vec![0xD8, 0xFF],
            b"this is not a jpeg".to_vec(),
            b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR\0\0\0\x10\0\0\0\x10\x08\x02\0\0\0\x90\x91h6".to_vec(),
            b"GIF89a\x01\0\x01\0\0\0\0;".to_vec(),
            vec![0u8; 1000],
            vec![0xFFu8; 1000],
        ];
        let mut rng = Rng(77);
        for len in [3usize, 10, 100, 1000, 10000] {
            let mut v: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
            out.push(v.clone());
            v[0] = 0xFF;
            v[1.min(len - 1)] = 0xD8;
            out.push(v);
        }
        out
    }

    #[derive(Default)]
    struct Tally {
        checked: usize,
        fast_accept: usize,
        jd_only_accept: usize,
        both_reject: usize,
        jd_panics: usize,
        skipped_huge: usize,
        mismatches: Vec<String>,
    }

    impl Tally {
        fn check(&mut self, name: &str, v: &[u8]) {
            let fast = decode_fast_only(v);
            // both refill strategies (x86-64 uses the branchless one) decode the same stream
            let (a, b) = (fast::validate_variant::<true>(v), fast::validate_variant::<false>(v));
            assert!(a == fast && b == fast, "{name}: refill variants disagree: {a:?} {b:?}");
            self.checked += 1;
            if fast.is_none() {
                // the verdict is jpeg-decoder's own; only classify. Absurd sizes (which the fast path
                // never accepts) can take jpeg-decoder seconds.
                let mut d = jpeg_decoder::Decoder::new(v);
                if d.read_info().is_ok() && d.info().is_some_and(|i| i.width > 4096 || i.height > 4096) {
                    self.skipped_huge += 1;
                    return;
                }
            }
            let jd = catch_unwind(AssertUnwindSafe(|| decode_jd(v)));
            let jd = match jd {
                Ok(j) => j,
                Err(_) => {
                    self.jd_panics += 1;
                    if fast.is_some() {
                        self.mismatches.push(format!("{name}: fast accepted {fast:?}, jpeg-decoder panicked ({} bytes)", v.len()));
                    }
                    return;
                }
            };
            match (fast, jd) {
                (Some(f), Some(j)) if f == j => self.fast_accept += 1,
                (Some(f), j) => {
                    if self.mismatches.len() < 20 {
                        let path = std::env::temp_dir().join(format!("jpeg-mismatch-{}.jpg", self.mismatches.len()));
                        let _ = std::fs::write(&path, v);
                        self.mismatches.push(format!("{name}: fast {f:?} vs jpeg-decoder {j:?} (saved {})", path.display()));
                    } else {
                        self.mismatches.push(String::new());
                    }
                }
                (None, Some(_)) => self.jd_only_accept += 1,
                (None, None) => self.both_reject += 1,
            }
            // decode_sync takes the fast path whenever it accepts
            if fast.is_some() {
                assert_eq!(decode_sync(v), fast, "{name}");
            }
        }

        fn finish(&self, what: &str) {
            eprintln!(
                "{what}: checked {} variants: fast-path accepts {} (= jpeg-decoder), jpeg-decoder-only accepts {}, both reject {}, jpeg-decoder panics {}, fast rejects with > 4096 px (not run through jpeg-decoder) {}, MISMATCHES {}",
                self.checked,
                self.fast_accept,
                self.jd_only_accept,
                self.both_reject,
                self.jd_panics,
                self.skipped_huge,
                self.mismatches.len()
            );
            assert!(self.mismatches.is_empty(), "fast path accepted where jpeg-decoder did not:\n{}", self.mismatches.join("\n"));
        }

        fn merge(&mut self, o: Tally) {
            self.checked += o.checked;
            self.fast_accept += o.fast_accept;
            self.jd_only_accept += o.jd_only_accept;
            self.both_reject += o.both_reject;
            self.jd_panics += o.jd_panics;
            self.skipped_huge += o.skipped_huge;
            self.mismatches.extend(o.mismatches);
        }
    }

    /// Checks every variant of every base, on all cores (deterministic: one RNG per base).
    fn run_corpus(bases: &[(String, Vec<u8>)], level: usize, tally: &mut Tally) {
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get()).min(16);
        let next = std::sync::atomic::AtomicUsize::new(0);
        let results: Vec<Tally> = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..threads)
                .map(|_| {
                    sc.spawn(|| {
                        let mut t = Tally::default();
                        loop {
                            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let Some((name, s)) = bases.get(i) else { break };
                            let mut rng = Rng(0xC0FFEE ^ (i as u64) << 20);
                            for (k, v) in variants(s, level, &mut rng).iter().enumerate() {
                                t.check(&format!("{name} #{k}"), v);
                            }
                        }
                        t
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for t in results {
            tally.merge(t);
        }
    }

    /// The fast path accepts only what jpeg-decoder accepts (with the same dimensions), on valid images of
    /// many encoder configurations and on ~100k corrupted variants of them.
    #[test]
    fn fast_path_matches_jpeg_decoder() {
        let mut bases = generated_bases(48, 200);
        for (i, s) in samples().into_iter().enumerate() {
            if s.len() < 100_000 {
                bases.push((format!("testdata#{i}"), s));
            }
        }
        // every valid baseline base must take the fast path
        for (name, s) in &bases {
            let jd = decode_jd(s);
            if jd.is_some() && !name.contains("progressive: true") && jd.unwrap().width <= 4096 {
                assert_eq!(decode_fast_only(s), jd, "{name}");
            }
        }
        let mut tally = Tally::default();
        for (k, v) in junk().iter().enumerate() {
            tally.check(&format!("junk#{k}"), v);
        }
        run_corpus(&bases, 2, &mut tally);
        tally.finish("fast_path_matches_jpeg_decoder");
        assert!(tally.fast_accept > 1000);
    }

    /// The benchmark-like post image (large; fewer variants in the default test run).
    #[test]
    fn fast_path_matches_jpeg_decoder_large() {
        let Some(big) = samples().into_iter().max_by_key(|s| s.len()).filter(|s| s.len() > 100_000) else {
            eprintln!("no large testdata/jpeg sample; skipping");
            return;
        };
        let mut tally = Tally::default();
        let mut rng = Rng(1);
        let n = big.len();
        tally.check("big", &big);
        let he = header_end(&big);
        for k in 0..60 {
            let cut = n * k / 60;
            tally.check("big trunc", &big[..cut]);
            let mut t = big[..cut].to_vec();
            t.extend_from_slice(&[0xFF, 0xD9]);
            tally.check("big trunc+EOI", &t);
        }
        for _ in 0..60 {
            let mut c = big.clone();
            let i = he + rng.below(n - he);
            c[i] ^= 1 << rng.below(8);
            tally.check("big bitflip", &c);
        }
        tally.finish("fast_path_matches_jpeg_decoder_large");
    }

    /// Much larger corpus (run with `cargo test --release -- --ignored fast_path_matches_jpeg_decoder_extended`);
    /// `JPEG_CORPUS_DIR=dir` adds every *.jpg in `dir` (e.g. real uploads) as a base image.
    #[test]
    #[ignore]
    fn fast_path_matches_jpeg_decoder_extended() {
        let mut bases = generated_bases(400, 640);
        for (i, s) in samples().into_iter().enumerate() {
            bases.push((format!("testdata#{i}"), s));
        }
        if let Ok(dir) = std::env::var("JPEG_CORPUS_DIR") {
            let mut paths: Vec<_> = std::fs::read_dir(&dir).unwrap().flatten().map(|e| e.path()).collect();
            paths.sort();
            for p in paths {
                if p.extension().map(|x| x == "jpg").unwrap_or(false) {
                    bases.push((p.display().to_string(), std::fs::read(&p).unwrap()));
                }
            }
        }
        let level: usize = std::env::var("JPEG_CORPUS_LEVEL").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
        let mut tally = Tally::default();
        for (k, v) in junk().iter().enumerate() {
            tally.check(&format!("junk#{k}"), v);
        }
        run_corpus(&bases, level, &mut tally);
        tally.finish("fast_path_matches_jpeg_decoder_extended");
    }

    #[test]
    fn scaled_decode_matches_full_decode() {
        let samples = samples();
        if samples.is_empty() {
            eprintln!("no testdata/jpeg samples; skipping");
            return;
        }
        let mut rng = Rng(0x9e3779b97f4a7c15);
        let mut checked = 0;
        for s in &samples {
            assert_eq!(decode_jd(s), decode_full_sync(s));
            // truncations
            for k in 1..40 {
                let cut = s.len() * k / 40;
                let t = &s[..cut];
                assert_eq!(decode_jd(t), decode_full_sync(t), "truncated at {cut}");
                checked += 1;
            }
            // random byte corruptions
            for _ in 0..300 {
                let mut c = s.clone();
                let n = 1 + rng.below(4);
                for _ in 0..n {
                    let i = rng.below(c.len());
                    c[i] = rng.next() as u8;
                }
                assert_eq!(decode_jd(&c), decode_full_sync(&c));
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
            decode_jd(big).unwrap();
        }
        let scaled = t.elapsed() / n;
        let t = std::time::Instant::now();
        for _ in 0..n {
            decode_fast_only(big).unwrap();
        }
        let fast = t.elapsed() / n;
        eprintln!("{} bytes: full {:?}, scaled {:?}, fast {:?}", big.len(), full, scaled, fast);
    }
}
