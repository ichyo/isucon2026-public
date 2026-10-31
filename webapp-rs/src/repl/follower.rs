//! Follower: forwards commands to the leader (multiplexed by req_id), applies the leader's records in order,
//! acks cumulatively after the local group-commit fsync, reconnects forever.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};

use super::proto::{self, Ack, Forward, Hello, Reset, ResponseMsg};
use crate::app::App;
use crate::apply::apply;
use crate::command::{Command, Outcome};
use crate::entry::{decode_record, encode_record, BlobIn, BlobOut};
use crate::http::ApiError;
use crate::model::ImageLoc;

pub const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_DELAY: Duration = Duration::from_millis(500);

struct Pending {
    tx: oneshot::Sender<Result<Outcome, ApiError>>,
    blob: Option<Bytes>,
}

#[derive(Default)]
pub struct Follower {
    conn: Mutex<Option<mpsc::UnboundedSender<Bytes>>>,
    pending: Mutex<FxHashMap<u64, Pending>>,
    next_req: AtomicU64,
    connected: AtomicBool,
    reconnects: AtomicU64,
}

impl Follower {
    pub fn new() -> Follower {
        Follower::default()
    }

    /// Forwards a command to the leader and waits for its outcome (by then the resulting records are applied
    /// locally: they precede the response on the connection).
    pub async fn forward(&self, cmd: Command) -> Result<Outcome, ApiError> {
        let unavailable = || ApiError::unavailable("leader unavailable");
        let conn = self.conn.lock().clone().ok_or_else(unavailable)?;
        let req_id = self.next_req.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(req_id, Pending { tx, blob: cmd.blob().cloned() });
        let frame = proto::frame_msg(proto::FORWARD, &Forward { req_id, cmd });
        if conn.send(frame).is_err() {
            self.pending.lock().remove(&req_id);
            return Err(unavailable());
        }
        match tokio::time::timeout(FORWARD_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            _ => {
                self.pending.lock().remove(&req_id);
                Err(unavailable())
            }
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub fn status_json(&self) -> String {
        format!(
            "{{\"connected\":{},\"pending_forwards\":{},\"reconnects\":{}}}",
            self.is_connected(),
            self.pending.lock().len(),
            self.reconnects.load(Ordering::Relaxed)
        )
    }

    fn fail_all_pending(&self) {
        let drained: Vec<Pending> = self.pending.lock().drain().map(|(_, p)| p).collect();
        for p in drained {
            let _ = p.tx.send(Err(ApiError::unavailable("leader connection lost")));
        }
    }

    fn blob_of(&self, req_id: u64) -> Option<Bytes> {
        self.pending.lock().get(&req_id).and_then(|p| p.blob.clone())
    }
}

fn me(app: &App) -> &Follower {
    match &app.repl {
        super::Repl::Follower(f) => f,
        _ => unreachable!("follower code on the leader"),
    }
}

/// Connection loop (runs forever).
pub async fn run(app: &'static App) {
    let f = me(app);
    let mut logged_down = false;
    loop {
        match TcpStream::connect(&app.cfg.leader_addr).await {
            Ok(s) => {
                logged_down = false;
                let _ = s.set_nodelay(true);
                eprintln!("[repl] connected to leader {}", app.cfg.leader_addr);
                let r = session(app, s).await;
                f.connected.store(false, Ordering::SeqCst);
                *f.conn.lock() = None;
                f.fail_all_pending();
                f.reconnects.fetch_add(1, Ordering::Relaxed);
                eprintln!("[repl] leader connection closed: {:?}", r.err());
            }
            Err(e) => {
                if !logged_down {
                    eprintln!("[repl] cannot connect to leader {}: {e} (retrying every {:?})", app.cfg.leader_addr, RECONNECT_DELAY);
                    logged_down = true;
                }
            }
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn session(app: &'static App, stream: TcpStream) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let f = me(app);
    let (mut rd, mut wr) = stream.into_split();
    let (epoch, seq, lsn, crc) = {
        let w = app.wal.inner.lock();
        (w.epoch, w.last_seq, w.lsn, w.last_crc())
    };
    let hello = Hello { region: app.cfg.region, leader_region: app.cfg.leader_region, epoch, seq, last_crc: crc };
    wr.write_all(&proto::frame_msg(proto::HELLO, &hello)).await?;
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
    let writer = tokio::spawn(proto::writer_loop(wr, rx));
    *f.conn.lock() = Some(tx.clone());
    f.connected.store(true, Ordering::SeqCst);

    // acker: cumulative ACK after the local fsync covering the applied records
    let (applied_tx, mut applied_rx) = watch::channel((epoch, seq, lsn));
    let ack_tx = tx.clone();
    let acker = tokio::spawn(async move {
        while applied_rx.changed().await.is_ok() {
            let (e, s, l) = *applied_rx.borrow_and_update();
            app.wal.wait_synced(l).await;
            if ack_tx.send(proto::frame_msg(proto::ACK, &Ack { epoch: e, seq: s })).is_err() {
                break;
            }
        }
    });

    let res: std::io::Result<()> = async {
        loop {
            let (ty, payload) = proto::read_frame(&mut rd).await?;
            match ty {
                proto::RESET => {
                    let r: Reset = proto::decode(&payload)?;
                    let t0 = std::time::Instant::now();
                    let fresh = super::clone_pristine(app).await;
                    let (old, lsn) = {
                        let mut st = app.state.write();
                        let mut wal = app.wal.inner.lock();
                        app.wal.reset(&mut wal, r.epoch)?;
                        (app.swap_state(&mut st, fresh), wal.lsn)
                    };
                    super::drop_in_background(old);
                    eprintln!("[repl] reset to epoch {:016x} in {:.1?}", r.epoch, t0.elapsed());
                    let _ = applied_tx.send((r.epoch, 0, lsn));
                }
                proto::RECORDS => {
                    let v = apply_records(app, f, &payload)?;
                    let _ = applied_tx.send(v);
                }
                proto::RESPONSE => {
                    let m: ResponseMsg = proto::decode(&payload)?;
                    if let Some(p) = f.pending.lock().remove(&m.req_id) {
                        let _ = p.tx.send(m.result);
                    }
                }
                other => return Err(std::io::Error::other(format!("unexpected frame {other}"))),
            }
        }
    }
    .await;
    acker.abort();
    writer.abort();
    res
}

/// Applies a RECORDS payload under one write-lock acquisition. Returns (epoch, last_seq, lsn).
fn apply_records(app: &'static App, f: &Follower, payload: &[u8]) -> std::io::Result<(u64, u64, u64)> {
    let mut st = app.state.write();
    let mut wal = app.wal.inner.lock();
    let mut fx = Vec::new();
    let mut pos = 0;
    let mut lsn = wal.lsn;
    while pos < payload.len() {
        let d = decode_record(&payload[pos..]).map_err(|e| std::io::Error::other(format!("bad record: {e:?}")))?;
        let rec = &payload[pos..pos + d.len];
        pos += d.len;
        if d.seq <= wal.last_seq {
            continue; // duplicate
        }
        if d.seq != wal.last_seq + 1 {
            return Err(std::io::Error::other(format!("gap: got seq {} after {}", d.seq, wal.last_seq)));
        }
        let (bytes, blob): (std::borrow::Cow<'_, [u8]>, Option<(usize, usize)>) = match d.blob {
            BlobIn::None => (rec.into(), None),
            BlobIn::Inline { off, len } => (rec.into(), Some((off, len))),
            BlobIn::FromForward(req) => {
                let Some(b) = f.blob_of(req) else {
                    return Err(std::io::Error::other(format!("blob of forward {req} unavailable")));
                };
                let (r2, off) = encode_record(d.seq, &d.entry, BlobOut::Inline(&b));
                (r2.into(), off.map(|o| (o, b.len())))
            }
        };
        let (off, l) = app.wal.append(&mut wal, &bytes, d.seq)?;
        lsn = l;
        let loc = blob.map(|(o, len)| ImageLoc::Wal { off: off + o as u64, len: len as u32 });
        apply(&mut st, &d.entry, loc, &mut fx);
    }
    app.handle_effects(&st, &fx);
    Ok((wal.epoch, wal.last_seq, lsn))
}
