//! Follower: forwards commands to the leader (multiplexed by req_id), applies the leader's records in order,
//! acks cumulatively after the local group-commit fsync, reconnects forever.
//!
//! Read gating: the follower answers API requests only while it is *in sync* — connected, caught up (the
//! leader's CAUGHT_UP marker after the catch-up stream), not resetting, and a frame (records or heartbeat)
//! arrived within `LEASE`. Otherwise requests wait up to `SYNC_WAIT` and then get 503 — never stale data.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};

use super::proto::{self, Ack, Early, Forward, Hello, Reset, ResponseMsg};
use crate::app::App;
use crate::apply::apply;
use crate::command::{Blob, Command, Outcome};
use crate::entry::{decode_record, encode_record, BlobIn, BlobOut};
use crate::http::ApiError;
use crate::model::ImageLoc;

/// Longer than the leader's ACK_TIMEOUT (a slow peer follower can delay the response that long).
pub const FORWARD_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_millis(500);
/// Default read lease (ISUREAL_GATE_SILENCE_MS): without any frame from the leader for this long the follower
/// stops serving (the leader sends heartbeats every 250 ms and drops a silent follower only after 25 s).
pub const LEASE: Duration = Duration::from_millis(10_000);
/// How long an API request waits for the follower to become in sync before answering 503.
pub const SYNC_WAIT: Duration = Duration::from_secs(3);
/// ACK_MODE=direct: how long the origin waits for direct acks before relying on the leader's RESPONSE.
pub const DIRECT_WAIT: Duration = Duration::from_secs(2);

fn mono_us() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_micros() as u64
}

struct Pending {
    tx: oneshot::Sender<Result<Outcome, ApiError>>,
    /// small inline blob (the leader sends `FromForward` back instead of the bytes)
    blob: Option<Bytes>,
}

pub struct Follower {
    conn: Mutex<Option<mpsc::UnboundedSender<Bytes>>>,
    pending: Mutex<FxHashMap<u64, Pending>>,
    next_req: AtomicU64,
    connected: AtomicBool,
    caught_up: AtomicBool,
    resetting: AtomicBool,
    last_frame_us: AtomicU64,
    reconnects: AtomicU64,
    gate_waits: AtomicU64,
    gate_503: AtomicU64,
    /// ACK_MODE=direct: forwarded writes completed from direct acks / left to the leader's RESPONSE
    early_done: AtomicU64,
    early_fallback: AtomicU64,
    early_wait_us: AtomicU64,
    lease_us: u64,
}

impl Follower {
    pub fn new(gate_silence_ms: u64) -> Follower {
        Follower {
            conn: Mutex::new(None),
            pending: Mutex::new(FxHashMap::default()),
            next_req: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            caught_up: AtomicBool::new(false),
            resetting: AtomicBool::new(false),
            last_frame_us: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            gate_waits: AtomicU64::new(0),
            gate_503: AtomicU64::new(0),
            early_done: AtomicU64::new(0),
            early_fallback: AtomicU64::new(0),
            early_wait_us: AtomicU64::new(0),
            lease_us: gate_silence_ms.max(1) * 1000,
        }
    }

    /// Forwards a command to the leader and waits for its outcome (by then the resulting records are applied
    /// locally: they precede the response on the connection).
    pub async fn forward(&self, cmd: Command) -> Result<Outcome, ApiError> {
        let unavailable = || ApiError::unavailable("leader unavailable");
        let conn = self.conn.lock().clone().ok_or_else(unavailable)?;
        let req_id = self.next_req.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = oneshot::channel();
        let blob = match cmd.blob() {
            Some(Blob::Inline(b)) => Some(b.clone()),
            _ => None,
        };
        self.pending.lock().insert(req_id, Pending { tx, blob });
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

    /// May this follower answer reads right now?
    #[inline]
    pub fn in_sync(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
            && self.caught_up.load(Ordering::SeqCst)
            && !self.resetting.load(Ordering::SeqCst)
            && mono_us().saturating_sub(self.last_frame_us.load(Ordering::SeqCst)) < self.lease_us
    }

    /// Waits (bounded) until in sync. `false` ⇒ answer 503.
    pub async fn wait_in_sync(&self, max: Duration) -> bool {
        if self.in_sync() {
            return true;
        }
        self.gate_waits.fetch_add(1, Ordering::Relaxed);
        let deadline = Instant::now() + max;
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if self.in_sync() {
                return true;
            }
            if Instant::now() >= deadline {
                self.gate_503.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }
    }

    pub fn status_json(&self) -> String {
        format!(
            "{{\"connected\":{},\"caught_up\":{},\"resetting\":{},\"in_sync\":{},\"last_frame_ms_ago\":{:.1},\"pending_forwards\":{},\"reconnects\":{},\"gate_waits\":{},\"gate_503\":{},\"direct_done\":{},\"direct_fallback\":{},\"direct_avg_ms\":{:.1}}}",
            self.is_connected(),
            self.caught_up.load(Ordering::SeqCst),
            self.resetting.load(Ordering::SeqCst),
            self.in_sync(),
            mono_us().saturating_sub(self.last_frame_us.load(Ordering::SeqCst)) as f64 / 1000.0,
            self.pending.lock().len(),
            self.reconnects.load(Ordering::Relaxed),
            self.gate_waits.load(Ordering::Relaxed),
            self.gate_503.load(Ordering::Relaxed),
            self.early_done.load(Ordering::Relaxed),
            self.early_fallback.load(Ordering::Relaxed),
            {
                let n = self.early_done.load(Ordering::Relaxed);
                if n == 0 { 0.0 } else { self.early_wait_us.load(Ordering::Relaxed) as f64 / n as f64 / 1000.0 }
            }
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

    fn touch(&self) {
        self.last_frame_us.store(mono_us(), Ordering::SeqCst);
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
                f.caught_up.store(false, Ordering::SeqCst);
                *f.conn.lock() = None;
                f.fail_all_pending();
                f.reconnects.fetch_add(1, Ordering::Relaxed);
                eprintln!("[repl] leader connection closed (not serving until back in sync): {:?}", r.err());
            }
            Err(e) => {
                if !logged_down {
                    eprintln!(
                        "[repl] cannot connect to leader {} (leader node {}): {e} (retrying every {:?}; not serving meanwhile)",
                        app.cfg.leader_addr,
                        app.cfg.leader_node.name(),
                        RECONNECT_DELAY
                    );
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
    let hello = Hello { node: app.cfg.node, region: app.cfg.region, leader_node: app.cfg.leader_node, epoch, seq, last_crc: crc };
    wr.write_all(&proto::frame_msg(proto::HELLO, &hello)).await?;
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
    let writer = tokio::spawn(proto::writer_loop(wr, rx));
    *f.conn.lock() = Some(tx.clone());
    f.caught_up.store(false, Ordering::SeqCst);
    f.touch();
    f.connected.store(true, Ordering::SeqCst);

    // acker: cumulative ACK after the local fsync covering the applied records (to the leader, and in
    // ACK_MODE=direct also to every other follower)
    let (applied_tx, mut applied_rx) = watch::channel((epoch, seq, lsn));
    let ack_tx = tx.clone();
    let direct = app.cfg.ack_mode == crate::config::AckMode::Direct;
    let acker = tokio::spawn(async move {
        while applied_rx.changed().await.is_ok() {
            let (e, s, l) = *applied_rx.borrow_and_update();
            app.wal.wait_synced(l).await;
            if direct {
                app.images.net.my_ack.send_replace((e, s));
            }
            if ack_tx.send(proto::frame_msg(proto::ACK, &Ack { epoch: e, seq: s })).is_err() {
                break;
            }
        }
    });

    let res: std::io::Result<()> = async {
        loop {
            let (ty, payload) = proto::read_frame(&mut rd).await?;
            f.touch();
            match ty {
                proto::HEARTBEAT => {}
                proto::LEADER_SYNCED => {
                    let a: Ack = proto::decode(&payload)?;
                    app.images.net.peer_acks[app.cfg.leader_node.idx()].send_replace((a.epoch, a.seq));
                }
                proto::EARLY => {
                    // the records of this write precede this frame on the connection: already applied here
                    let m: Early = proto::decode(&payload)?;
                    let lsn = app.wal.inner.lock().lsn;
                    let t0 = Instant::now();
                    tokio::spawn(async move {
                        let net = &app.images.net;
                        let all = async {
                            app.wal.wait_synced(lsn).await;
                            net.wait_peer_ack(app.cfg.leader_node, m.epoch, m.seq).await;
                            for n in m.wait_for.iter().filter(|&&n| n != app.cfg.node) {
                                net.wait_peer_ack(*n, m.epoch, m.seq).await;
                            }
                        };
                        if tokio::time::timeout(DIRECT_WAIT, all).await.is_ok() {
                            if let Some(p) = f.pending.lock().remove(&m.req_id) {
                                f.early_done.fetch_add(1, Ordering::Relaxed);
                                f.early_wait_us.fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
                                let _ = p.tx.send(Ok(m.outcome));
                            }
                        } else {
                            // membership / acks uncertain: the leader's RESPONSE (all acked) completes it
                            f.early_fallback.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
                proto::RESET => {
                    let r: Reset = proto::decode(&payload)?;
                    f.resetting.store(true, Ordering::SeqCst);
                    let t0 = Instant::now();
                    let fresh = super::clone_pristine(app).await;
                    let (old, lsn, new_epoch) = {
                        let mut st = app.state.write();
                        let mut wal = app.wal.inner.lock();
                        app.wal.reset(&mut wal, r.epoch)?;
                        let old = app.swap_state(&mut st, fresh);
                        // a new epoch (initialize) drops uploaded blobs; a same-epoch resync must keep them
                        // (the leader's log still references blobs this node is the origin of)
                        let new_epoch = app.images.reset_uploads_for_epoch(r.epoch);
                        (old, wal.lsn, new_epoch)
                    };
                    super::drop_in_background(old);
                    f.resetting.store(false, Ordering::SeqCst);
                    eprintln!("[repl] reset to epoch {:016x} ({}) in {:.1?}", r.epoch, if new_epoch { "new epoch" } else { "resync" }, t0.elapsed());
                    let _ = applied_tx.send((r.epoch, 0, lsn));
                }
                proto::RECORDS => {
                    let v = apply_records(app, f, &payload)?;
                    let _ = applied_tx.send(v);
                }
                proto::CAUGHT_UP => {
                    let a: Ack = proto::decode(&payload)?;
                    let (e, s) = {
                        let w = app.wal.inner.lock();
                        (w.epoch, w.last_seq)
                    };
                    if e != a.epoch || s < a.seq {
                        return Err(std::io::Error::other(format!("CAUGHT_UP {}:{} but local is {e}:{s}", a.epoch, a.seq)));
                    }
                    if !f.caught_up.swap(true, Ordering::SeqCst) {
                        eprintln!("[repl] caught up with the leader at seq {s}; serving");
                    }
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
    f.connected.store(false, Ordering::SeqCst);
    f.caught_up.store(false, Ordering::SeqCst);
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
        // (bytes to append to the local WAL, image location; inline blobs are relative to the record start)
        enum Loc {
            None,
            InRecord { off: u64, len: u32 },
            At(ImageLoc),
        }
        let (bytes, loc): (std::borrow::Cow<'_, [u8]>, Loc) = match d.blob {
            BlobIn::None => (rec.into(), Loc::None),
            BlobIn::Inline { off, len } => (rec.into(), Loc::InRecord { off: off as u64, len: len as u32 }),
            BlobIn::Ref(r) => (rec.into(), Loc::At(r.loc_on(app.cfg.node))),
            BlobIn::FromForward(req) => {
                let Some(b) = f.blob_of(req) else {
                    return Err(std::io::Error::other(format!("blob of forward {req} unavailable")));
                };
                let (r2, off) = encode_record(d.seq, &d.entry, BlobOut::Inline(&b));
                (r2.into(), Loc::InRecord { off: off.unwrap_or(0) as u64, len: b.len() as u32 })
            }
        };
        let (off, l) = app.wal.append(&mut wal, &bytes, d.seq)?;
        lsn = l;
        let loc = match loc {
            Loc::None => None,
            Loc::InRecord { off: o, len } => Some(ImageLoc::Wal { off: off + o, len }),
            Loc::At(l) => Some(l),
        };
        apply(&mut st, &d.entry, loc, &mut fx);
    }
    app.handle_effects(&st, &fx);
    Ok((wal.epoch, wal.last_seq, lsn))
}
