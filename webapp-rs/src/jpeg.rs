//! JPEG validation: full decode with `jpeg-decoder` (same acceptance as the reference Rust
//! implementation, `webapp/rust/src/jpeg.rs`, which mirrors Go `image/jpeg`).

use bytes::Bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dims {
    pub width: u16,
    pub height: u16,
}

/// Full decode; `None` if the data is not a decodable JPEG. CPU-heavy: use `decode` from async code.
pub fn decode_sync(data: &[u8]) -> Option<Dims> {
    let mut decoder = jpeg_decoder::Decoder::new(data);
    decoder.decode().ok()?;
    let info = decoder.info()?;
    Some(Dims { width: info.width, height: info.height })
}

/// Full decode in the blocking pool.
pub async fn decode(data: Bytes) -> Option<Dims> {
    tokio::task::spawn_blocking(move || decode_sync(&data)).await.ok().flatten()
}
