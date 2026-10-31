//! Notification hub for `GET /api/notifications` (§7). The HTTP handler (group C) calls
//! `ctx.sse.subscribe(user_idx, now)` and returns `Resp::sse(body)`; delivery is driven by apply effects.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::Frame;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use tokio::time::Sleep;

use crate::json::JsonOut;
use crate::model::*;
use crate::state::State;

/// Idle streams are closed without an event after this long (mimics nginx proxy_read_timeout).
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Deterministic notification id for (user, window): UUIDv5-like (sha256 based), lowercase 36 chars.
pub fn notification_id(user: Id, window: Id) -> Id {
    let mut h = Sha256::new();
    h.update(b"isureal-notification\0");
    h.update(user.to_bytes());
    h.update(window.to_bytes());
    let d = h.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&d[..16]);
    b[6] = (b[6] & 0x0f) | 0x50;
    b[8] = (b[8] & 0x3f) | 0x80;
    Id::from_bytes(b)
}

/// `data: {...}\n\n` for one notification.
pub fn event_bytes(st: &State, u: UIdx, w: WIdx) -> Bytes {
    let win = st.window(w);
    let user = st.user(u);
    // JsonOut needs a signer only for image URLs; none here.
    static NO_SIGNER: std::sync::OnceLock<crate::sign::Signer> = std::sync::OnceLock::new();
    let signer: &'static crate::sign::Signer = NO_SIGNER.get_or_init(|| crate::sign::Signer::new(1));
    let mut j = JsonOut::new(0, signer);
    j.obj()
        .k("id")
        .id(notification_id(user.id, win.id))
        .k("postingWindowId")
        .id(win.id)
        .k("region")
        .s(win.region.as_str())
        .k("createdAt")
        .ts(win.starts_at)
        .end();
    let mut out = Vec::with_capacity(200);
    out.extend_from_slice(b"data: ");
    let mut body = j.finish();
    body.pop(); // trailing \n
    out.extend_from_slice(&body);
    out.extend_from_slice(b"\n\n");
    Bytes::from(out)
}

struct Sub {
    connected_at: Ts,
    tx: oneshot::Sender<Bytes>,
}

#[derive(Default)]
pub struct SseHub {
    subs: Mutex<FxHashMap<UIdx, Vec<Sub>>>,
}

impl SseHub {
    /// Registers a subscriber; the returned body yields one event then ends (or ends empty on
    /// close / idle timeout / client disconnect).
    pub fn subscribe(&self, user: UIdx, connected_at: Ts) -> SseBody {
        let (tx, rx) = oneshot::channel();
        self.subs.lock().entry(user).or_default().push(Sub { connected_at, tx });
        SseBody { rx: Some(rx), sleep: Box::pin(tokio::time::sleep(IDLE_TIMEOUT)), done: false }
    }

    /// Called with the write lock held, right after the window was applied.
    pub fn on_window_opened(&self, st: &State, w: WIdx) {
        let win = st.window(w);
        let mut subs = self.subs.lock();
        subs.retain(|&u, list| {
            list.retain(|s| !s.tx.is_closed());
            let user = st.user(u);
            if user.is_active() && user.region == win.region {
                let mut keep = Vec::new();
                for s in list.drain(..) {
                    if win.starts_at > s.connected_at {
                        let _ = s.tx.send(event_bytes(st, u, w));
                    } else {
                        keep.push(s);
                    }
                }
                *list = keep;
            }
            !list.is_empty()
        });
    }

    /// User deleted or region changed: their streams end without an event.
    pub fn on_user_gone(&self, u: UIdx) {
        self.subs.lock().remove(&u);
    }

    pub fn close_all(&self) {
        self.subs.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.subs.lock().values().map(|v| v.len()).sum()
    }
}

pub struct SseBody {
    rx: Option<oneshot::Receiver<Bytes>>,
    sleep: Pin<Box<Sleep>>,
    done: bool,
}

impl SseBody {
    pub fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, std::convert::Infallible>>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        if let Some(rx) = this.rx.as_mut() {
            match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(b)) => {
                    this.done = true;
                    this.rx = None;
                    return Poll::Ready(Some(Ok(Frame::data(b))));
                }
                Poll::Ready(Err(_)) => {
                    this.done = true;
                    this.rx = None;
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }
        }
        if this.sleep.as_mut().poll(cx).is_ready() {
            this.done = true;
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}

use std::future::Future as _;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notif_id_format() {
        let a = Id::new_v4();
        let b = Id::new_v4();
        let n = notification_id(a, b);
        assert_eq!(n, notification_id(a, b));
        let s = n.to_string();
        assert_eq!(s.len(), 36);
        assert_eq!(&s[14..15], "5");
    }
}
