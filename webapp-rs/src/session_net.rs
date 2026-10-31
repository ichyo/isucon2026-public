//! Private regional-session RPC. A bounded pool keeps connections alive without serializing all
//! authentication behind one slow migration. A failed exchange is never retried: its mutation may
//! already have committed even when the response was lost.

use std::io;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::app::App;
use crate::http::ApiError;
use crate::model::NodeId;
use crate::sessions::{Reply, Request};

const CONNECTIONS_PER_PEER: usize = 16;
const MAX_FRAME: usize = 64 * 1024;
const VERSION: u8 = 1;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);

struct PeerState {
    addr: Option<String>,
    generation: u64,
    idle: Vec<TcpStream>,
}

struct Peer {
    state: Mutex<PeerState>,
    slots: Semaphore,
}

pub struct SessionNet {
    peers: [Peer; NodeId::COUNT],
}

impl SessionNet {
    pub fn new(peers: [Option<String>; NodeId::COUNT]) -> Self {
        Self {
            peers: peers
                .map(|addr| Peer { state: Mutex::new(PeerState { addr, generation: 0, idle: Vec::new() }), slots: Semaphore::new(CONNECTIONS_PER_PEER) }),
        }
    }

    /// Tests/local clusters bind ephemeral ports. An address change retires every old connection.
    pub fn set_peer(&self, node: NodeId, addr: String) {
        let mut state = self.peers[node.idx()].state.lock();
        if state.addr.as_deref() != Some(&addr) {
            state.addr = Some(addr);
            state.generation += 1;
            state.idle.clear();
        }
    }

    pub async fn call(&'static self, node: NodeId, request: Request) -> Result<Reply, ApiError> {
        let peer = self.peers.get(node.idx()).ok_or_else(|| ApiError::unavailable("invalid session owner"))?;
        let payload = encode(&request).map_err(|_| ApiError::unavailable("invalid session request"))?;
        // Includes the queue wait. Cancellation drops an in-use socket, so a partial response is never
        // accidentally reused as the response to a later request.
        match tokio::time::timeout(REQUEST_TIMEOUT, async {
            let _permit = peer.slots.acquire().await.map_err(|_| io::Error::other("session pool closed"))?;
            let (addr, generation, idle) = {
                let mut state = peer.state.lock();
                (state.addr.clone(), state.generation, state.idle.pop())
            };
            let mut stream = match idle {
                Some(stream) => stream,
                None => {
                    let addr = addr.ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "session owner is not configured"))?;
                    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
                        .await
                        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "session connect timed out"))??;
                    stream.set_nodelay(true)?;
                    stream
                }
            };
            if peer.state.lock().generation != generation {
                return Err(io::Error::new(io::ErrorKind::NotConnected, "session peer changed"));
            }
            write_frame(&mut stream, &payload).await?;
            let result: Result<Reply, ApiError> = read_frame(&mut stream).await?;
            let mut state = peer.state.lock();
            if state.generation == generation {
                state.idle.push(stream);
            }
            Ok::<_, io::Error>(result)
        })
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ApiError::unavailable("session owner unavailable")),
            Err(_) => Err(ApiError::unavailable("session owner timeout")),
        }
    }
}

fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let payload = postcard::to_stdvec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if payload.len() + 1 > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "session frame too large"));
    }
    let mut frame = Vec::with_capacity(payload.len() + 1);
    frame.push(VERSION);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, payload: &[u8]) -> io::Result<()> {
    writer.write_u32_le(payload.len() as u32).await?;
    writer.write_all(payload).await?;
    writer.flush().await
}

async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(reader: &mut R) -> io::Result<T> {
    let len = reader.read_u32_le().await? as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid session frame length"));
    }
    let mut payload = vec![0; len];
    // Idle connections are allowed indefinitely; a peer that starts a frame must finish it promptly.
    tokio::time::timeout(REQUEST_TIMEOUT, reader.read_exact(&mut payload))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "session frame timed out"))??;
    if payload[0] != VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported session protocol"));
    }
    let (message, trailing) = postcard::take_from_bytes(&payload[1..]).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if !trailing.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "trailing session frame bytes"));
    }
    Ok(message)
}

/// Bind only on the internal interface, like the replication/blob services. This protocol is not a
/// public authentication endpoint: cluster members are trusted to submit its operations.
pub async fn serve(app: &'static App, listener: TcpListener) {
    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("[sessions] accept: {e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        tokio::spawn(async move {
            while let Ok(request) = read_frame::<_, Request>(&mut stream).await {
                // Do not cancel the handler when a client disconnects or times out. In particular, a
                // migration must retain its per-user lock until its replicated handoff is complete.
                let result = crate::sessions::handle(app, request).await;
                let Ok(payload) = encode(&result) else { break };
                if !matches!(tokio::time::timeout(REQUEST_TIMEOUT, write_frame(&mut stream, &payload)).await, Ok(Ok(()))) {
                    break;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn framing_preserves_multiple_messages() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        write_frame(&mut writer, &encode(&42_u64).unwrap()).await.unwrap();
        write_frame(&mut writer, &encode(&"second").unwrap()).await.unwrap();
        assert_eq!(read_frame::<_, u64>(&mut reader).await.unwrap(), 42);
        assert_eq!(read_frame::<_, String>(&mut reader).await.unwrap(), "second");
    }

    #[tokio::test]
    async fn malformed_frames_are_rejected() {
        for len in [0, MAX_FRAME as u32 + 1] {
            let mut data = &len.to_le_bytes()[..];
            assert_eq!(read_frame::<_, u64>(&mut data).await.unwrap_err().kind(), io::ErrorKind::InvalidData);
        }
        for payload in [vec![VERSION + 1, 0], vec![VERSION, 0, 0], vec![VERSION, 255]] {
            let mut bytes = (payload.len() as u32).to_le_bytes().to_vec();
            bytes.extend_from_slice(&payload);
            assert_eq!(read_frame::<_, u64>(&mut &bytes[..]).await.unwrap_err().kind(), io::ErrorKind::InvalidData);
        }
        assert!(encode(&vec![0_u8; MAX_FRAME]).is_err());
    }

    #[tokio::test]
    async fn truncated_frame_is_rejected() {
        let mut bytes = 8_u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[VERSION, 1]);
        assert_eq!(read_frame::<_, u64>(&mut &bytes[..]).await.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }
}
