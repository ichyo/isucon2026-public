use anyhow::anyhow;
use bytes::Bytes;

use crate::api_error::Error;

pub struct Image {
    pub width: u16,
    pub height: u16,
}

pub async fn decode(data: Bytes) -> Result<Image, Error> {
    tokio::task::spawn_blocking(move || {
        let mut decoder = jpeg_decoder::Decoder::new(&data[..]);
        decoder.decode().map_err(anyhow::Error::from)?;
        let info = decoder
            .info()
            .ok_or_else(|| anyhow!("missing image info"))?;
        Ok(Image {
            width: info.width,
            height: info.height,
        })
    })
    .await
    .map_err(anyhow::Error::from)?
}
