//! Group B: GET /api/me/export (§6.3) — zip: images/NNNNNN.jpg stored, posts.json deflated.
//! Copy what you need under the read lock, release it, then read image bytes (ctx.images.read) and zip.

use std::io::Write as _;

use super::prelude::*;

// GET /api/me/export — auth user, pinned.
pub async fn export_posts(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    // Under the read lock: posts.json (signatures still pending) + the image locations, in export order.
    let mut locs: Vec<(Id, ImageLoc)> = Vec::new();
    let w = ctx.render(|st, w| {
        w.arr();
        for &p in &st.user(me.idx).posts {
            let post = st.post(p);
            if post.deleted_at.is_some() {
                continue;
            }
            let meta = st.images.get(&post.image).ok_or_else(|| ApiError::internal(format!("export: image {} of post {} missing", post.image, post.id)))?;
            locs.push((post.image, meta.loc));
            let path = image_path(locs.len());
            views::post(w, st, p, |w| {
                w.k("imagePath").s(&path);
            });
        }
        w.end();
        Ok(())
    })?;
    // Large uploads of other nodes (rare: the user changed region) are fetched from their origin first.
    let mut fetched: Vec<Option<Bytes>> = vec![None; locs.len()];
    for (i, (id, loc)) in locs.iter().enumerate() {
        if matches!(loc, ImageLoc::Remote { .. }) {
            let b = ctx.images.read(*id, *loc).await.map_err(|e| {
                eprintln!("[export] fetch of {id} failed: {e}");
                ApiError::unavailable("image temporarily unavailable")
            })?;
            fetched[i] = Some(b);
        }
    }
    // Off-lock (blocking pool): signatures, image bytes, zip.
    let zip = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, ApiError> {
        let json = w.finish();
        let mut images = Vec::with_capacity(locs.len());
        for ((id, loc), f) in locs.iter().zip(fetched) {
            match f {
                Some(b) => images.push(b),
                None => images.push(ctx.images.read_sync(*id, *loc).map_err(|e| ApiError::internal(format!("export: read image: {e}")))?),
            }
        }
        Ok(build_zip(&images, &json, now_us()))
    })
    .await
    .map_err(ApiError::internal)??;
    Ok(Resp::bytes(200, "application/zip", zip)
        .header("content-disposition", "attachment; filename=\"isureal-posts.zip\"")
        .header("cache-control", "no-store"))
}

/// `images/000001.jpg` (1-based, 6-digit zero-padded).
pub fn image_path(n: usize) -> String {
    format!("images/{n:06}.jpg")
}

/// MS-DOS (time, date) of a UTC timestamp (2-second resolution, clamped to 1980..=2107).
fn dos_time(us: Ts) -> (u16, u16) {
    let secs = us.div_euclid(1_000_000);
    let (y, m, d) = timefmt::civil_from_days(secs.div_euclid(86400));
    let sod = secs.rem_euclid(86400) as u32;
    if !(1980..=2107).contains(&y) {
        return (0, (1 << 5) | 1);
    }
    let time = ((sod / 3600) << 11) | ((sod / 60 % 60) << 5) | (sod % 60 / 2);
    let date = (((y - 1980) as u32) << 9) | (m << 5) | d;
    (time as u16, date as u16)
}

struct CdEntry {
    name: String,
    method: u16,
    crc: u32,
    csize: u32,
    usize: u32,
    offset: u32,
}

fn put16(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn put32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}

/// Appends one local file header + data; returns its central-directory entry.
fn put_entry(out: &mut Vec<u8>, name: String, method: u16, crc: u32, data: &[u8], usize: u32, dos: (u16, u16)) -> CdEntry {
    let offset = out.len() as u32;
    put32(out, 0x0403_4b50);
    put16(out, if method == 8 { 20 } else { 10 }); // version needed
    put16(out, 0); // flags
    put16(out, method);
    put16(out, dos.0);
    put16(out, dos.1);
    put32(out, crc);
    put32(out, data.len() as u32);
    put32(out, usize);
    put16(out, name.len() as u16);
    put16(out, 0); // extra
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(data);
    CdEntry { name, method, crc, csize: data.len() as u32, usize, offset }
}

/// The export zip: `images/NNNNNN.jpg` stored (method 0) in order, then `posts.json` deflated (method 8).
pub fn build_zip(images: &[Bytes], posts_json: &[u8], now: Ts) -> Vec<u8> {
    let dos = dos_time(now);
    let mut deflated = flate2::write::DeflateEncoder::new(Vec::with_capacity(posts_json.len() / 4 + 64), flate2::Compression::fast());
    deflated.write_all(posts_json).expect("deflate to Vec");
    let deflated = deflated.finish().expect("deflate to Vec");

    let total: usize = images.iter().map(|b| b.len()).sum::<usize>() + deflated.len() + (images.len() + 1) * 128 + 64;
    let mut out = Vec::with_capacity(total);
    let mut cd: Vec<CdEntry> = Vec::with_capacity(images.len() + 1);
    for (i, img) in images.iter().enumerate() {
        let crc = crc32fast::hash(img);
        cd.push(put_entry(&mut out, image_path(i + 1), 0, crc, img, img.len() as u32, dos));
    }
    cd.push(put_entry(&mut out, "posts.json".to_string(), 8, crc32fast::hash(posts_json), &deflated, posts_json.len() as u32, dos));

    let cd_start = out.len() as u32;
    for e in &cd {
        put32(&mut out, 0x0201_4b50);
        put16(&mut out, (3 << 8) | 20); // made by: unix, 2.0
        put16(&mut out, if e.method == 8 { 20 } else { 10 });
        put16(&mut out, 0); // flags
        put16(&mut out, e.method);
        put16(&mut out, dos.0);
        put16(&mut out, dos.1);
        put32(&mut out, e.crc);
        put32(&mut out, e.csize);
        put32(&mut out, e.usize);
        put16(&mut out, e.name.len() as u16);
        put16(&mut out, 0); // extra
        put16(&mut out, 0); // comment
        put16(&mut out, 0); // disk number start
        put16(&mut out, 0); // internal attrs
        put32(&mut out, 0o100644 << 16); // external attrs: regular file rw-r--r--
        put32(&mut out, e.offset);
        out.extend_from_slice(e.name.as_bytes());
    }
    let cd_size = out.len() as u32 - cd_start;
    put32(&mut out, 0x0605_4b50);
    put16(&mut out, 0); // this disk
    put16(&mut out, 0); // disk with the central directory
    put16(&mut out, cd.len() as u16);
    put16(&mut out, cd.len() as u16);
    put32(&mut out, cd_size);
    put32(&mut out, cd_start);
    put16(&mut out, 0); // comment length
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dos_time_of_known_instant() {
        let t = timefmt::parse_rfc3339("2026-10-31T01:17:22Z").unwrap();
        let (time, date) = dos_time(t);
        assert_eq!(time, (1 << 11) | (17 << 5) | 11);
        assert_eq!(date, (46 << 9) | (10 << 5) | 31);
    }

    #[test]
    fn zip_layout() {
        let imgs = vec![Bytes::from_static(b"\xff\xd8abc\xff\xd9"), Bytes::from_static(b"\xff\xd8defg\xff\xd9")];
        let json = b"[{\"a\":1}]\n";
        let z = build_zip(&imgs, json, 0);
        // local header 1
        assert_eq!(&z[0..4], &[0x50, 0x4b, 0x03, 0x04]);
        assert_eq!(u16::from_le_bytes([z[8], z[9]]), 0); // stored
        assert_eq!(&z[30..47], b"images/000001.jpg");
        assert_eq!(&z[47..54], &imgs[0][..]);
        // EOCD
        let e = z.len() - 22;
        assert_eq!(&z[e..e + 4], &[0x50, 0x4b, 0x05, 0x06]);
        assert_eq!(u16::from_le_bytes([z[e + 10], z[e + 11]]), 3);
        let cd_off = u32::from_le_bytes(z[e + 16..e + 20].try_into().unwrap()) as usize;
        assert_eq!(&z[cd_off..cd_off + 4], &[0x50, 0x4b, 0x01, 0x02]);
        // posts.json inflates back
        let name = b"posts.json";
        let pos = z.windows(name.len()).position(|w| w == name).unwrap();
        let hdr = pos - 30;
        assert_eq!(u16::from_le_bytes([z[hdr + 8], z[hdr + 9]]), 8);
        let csize = u32::from_le_bytes(z[hdr + 18..hdr + 22].try_into().unwrap()) as usize;
        let data = &z[pos + name.len()..pos + name.len() + csize];
        let mut dec = flate2::read::DeflateDecoder::new(data);
        let mut s = Vec::new();
        std::io::Read::read_to_end(&mut dec, &mut s).unwrap();
        assert_eq!(s, json);
        assert_eq!(u32::from_le_bytes(z[hdr + 14..hdr + 18].try_into().unwrap()), crc32fast::hash(json));
    }
}
