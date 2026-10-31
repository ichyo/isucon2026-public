//! Replication wire protocol: frames `[u32 len LE][u8 type][payload]` (len counts type + payload).

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::command::{Command, Outcome};
use crate::http::ApiError;
use crate::model::Region;

pub const HELLO: u8 = 1;
pub const FORWARD: u8 = 2;
pub const ACK: u8 = 3;
pub const RESET: u8 = 10;
pub const RECORDS: u8 = 11;
pub const RESPONSE: u8 = 12;
/// L→F after the catch-up stream (payload: Ack{epoch, seq} = what the follower now has): it may serve reads
pub const CAUGHT_UP: u8 = 13;
/// L→F when the link is idle (keeps the follower's read lease alive)
pub const HEARTBEAT: u8 = 14;

pub const MAX_FRAME: usize = 96 << 20;

#[derive(Serialize, Deserialize, Debug)]
pub struct Hello {
    pub region: Region,
    pub leader_region: Region,
    pub epoch: u64,
    pub seq: u64,
    pub last_crc: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Forward {
    pub req_id: u64,
    pub cmd: Command,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Ack {
    pub epoch: u64,
    pub seq: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reset {
    pub epoch: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseMsg {
    pub req_id: u64,
    pub result: Result<Outcome, ApiError>,
}

pub fn frame(ty: u8, payload: &[u8]) -> Bytes {
    let mut v = Vec::with_capacity(5 + payload.len());
    v.extend_from_slice(&((payload.len() + 1) as u32).to_le_bytes());
    v.push(ty);
    v.extend_from_slice(payload);
    Bytes::from(v)
}

pub fn frame_msg<T: Serialize>(ty: u8, msg: &T) -> Bytes {
    let payload = postcard::to_stdvec(msg).expect("postcard encode");
    frame(ty, &payload)
}

pub fn decode<'a, T: Deserialize<'a>>(payload: &'a [u8]) -> std::io::Result<T> {
    postcard::from_bytes(payload).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Reads one frame: (type, payload).
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<(u8, Vec<u8>)> {
    let len = r.read_u32_le().await? as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad frame len {len}")));
    }
    let ty = r.read_u8().await?;
    let mut payload = vec![0u8; len - 1];
    r.read_exact(&mut payload).await?;
    Ok((ty, payload))
}

/// Writer task: drains the queue, coalescing everything currently queued into one flush.
pub async fn writer_loop<W: tokio::io::AsyncWrite + Unpin>(w: W, mut rx: tokio::sync::mpsc::UnboundedReceiver<Bytes>) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut bw = tokio::io::BufWriter::with_capacity(256 * 1024, w);
    while let Some(f) = rx.recv().await {
        bw.write_all(&f).await?;
        while let Ok(f) = rx.try_recv() {
            bw.write_all(&f).await?;
        }
        bw.flush().await?;
    }
    Ok(())
}
