//! Leader: executes commands, appends + applies + streams records, tracks cumulative acks, serves catch-up.
//!
//! Invariant: a client write is acknowledged only after every *connected* follower (catching up or not) has
//! applied + persisted its records. Slow links are waited for (warning after 1 s); a link is dropped only on a
//! connection error or after `ACK_TIMEOUT`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Notify};

use super::proto::{self, Ack, Forward, Hello, Reset, ResponseMsg};
use super::Origin;
use crate::app::{App, WaitPolicy};
use crate::config::AckMode;
use crate::apply::apply;
use crate::command::{self, Blob, Command, Outcome, Tx};
use crate::entry::{encode_record, BlobOut};
use crate::http::ApiError;
use crate::model::*;

/// A connected follower that has not acked after this long is dropped (it reconnects and catches up).
pub const ACK_TIMEOUT: Duration = Duration::from_secs(25);
/// Acks slower than this are logged (rate-limited) and counted.
pub const SLOW_ACK: Duration = Duration::from_secs(1);
pub const RESET_TIMEOUT: Duration = Duration::from_secs(30);
/// Idle links get a heartbeat this often (keeps the follower's read lease alive).
pub const HEARTBEAT_EVERY: Duration = Duration::from_millis(250);
const CATCHUP_CHUNK: u64 = 4 << 20;

fn mono_us() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_micros() as u64
}

pub struct Link {
    pub id: u64,
    pub node: NodeId,
    tx: mpsc::UnboundedSender<Bytes>,
    /// cumulative (epoch, seq) acked by the follower
    acked: watch::Sender<(u64, u64)>,
    /// informational: acked ≥ catch-up target
    in_sync: AtomicBool,
    /// (epoch, seq) the follower must reach to count as caught up
    target: Mutex<(u64, u64)>,
    kill: Notify,
    dead: AtomicBool,
    /// bytes queued for this link but not yet written to the socket
    queue_bytes: AtomicU64,
    max_ack_us: AtomicU64,
    slow_acks: AtomicU64,
    last_warn_us: AtomicU64,
    /// max (leader seq − acked seq) observed (follower lag in records)
    max_lag: AtomicU64,
    connected_at: Instant,
}

impl Link {
    fn send(&self, f: Bytes) {
        self.queue_bytes.fetch_add(f.len() as u64, Ordering::Relaxed);
        if self.tx.send(f).is_err() {
            self.queue_bytes.store(0, Ordering::Relaxed);
        }
    }
    fn kill(&self) {
        if !self.dead.swap(true, Ordering::SeqCst) {
            self.in_sync.store(false, Ordering::SeqCst);
            self.kill.notify_waiters();
            self.kill.notify_one();
        }
    }
    fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }
    fn lag(&self, epoch: u64, leader_seq: u64) -> u64 {
        let (e, s) = *self.acked.borrow();
        if e == epoch {
            leader_seq.saturating_sub(s)
        } else {
            leader_seq
        }
    }
    fn on_ack(&self, a: Ack) {
        self.acked.send_replace((a.epoch, a.seq));
        let t = *self.target.lock();
        if a.epoch == t.0 && a.seq >= t.1 && !self.in_sync.swap(true, Ordering::SeqCst) {
            eprintln!("[repl] link {} ({}) caught up at seq {}", self.id, self.node.name(), a.seq);
        }
    }
    /// true when `pred` holds within `timeout`; false on timeout or kill.
    async fn wait_ack(&self, pred: impl Fn(&(u64, u64)) -> bool, timeout: Duration) -> bool {
        if self.is_dead() {
            return false;
        }
        let mut rx = self.acked.subscribe();
        tokio::select! {
            r = tokio::time::timeout(timeout, rx.wait_for(|v| pred(v))) => matches!(r, Ok(Ok(_))),
            _ = self.kill.notified() => false,
        }
    }
    /// Waits for (epoch, seq) with slow-ack warnings; drops the link after ACK_TIMEOUT.
    async fn wait_applied(&self, epoch: u64, seq: u64) {
        let t0 = Instant::now();
        let mut counted = false;
        loop {
            if self.wait_ack(|&(e, s)| e != epoch || s >= seq, SLOW_ACK).await {
                break;
            }
            if self.is_dead() {
                break;
            }
            let el = t0.elapsed();
            if el >= ACK_TIMEOUT {
                eprintln!("[repl] link {} ({}) did not ack seq {seq} within {:?}; dropping it", self.id, self.node.name(), ACK_TIMEOUT);
                self.kill();
                break;
            }
            if !counted {
                counted = true;
                self.slow_acks.fetch_add(1, Ordering::Relaxed);
            }
            let now = mono_us();
            let last = self.last_warn_us.load(Ordering::Relaxed);
            if (last == 0 || now.saturating_sub(last) > 1_000_000) && self.last_warn_us.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                let (ae, asq) = *self.acked.borrow();
                eprintln!(
                    "[repl] WARN slow ack: link {} ({}) seq {seq} not applied after {:.1?} (acked {}:{}, queued {} bytes)",
                    self.id,
                    self.node.name(),
                    el,
                    ae,
                    asq,
                    self.queue_bytes.load(Ordering::Relaxed)
                );
            }
        }
        self.max_ack_us.fetch_max(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
    }
}

pub struct Leader {
    links: Mutex<Vec<Arc<Link>>>,
    /// (epoch, last seq) of the log, for lag metrics
    head: Mutex<(u64, u64)>,
    next_link: AtomicU64,
    init_lock: tokio::sync::Mutex<()>,
}

impl Default for Leader {
    fn default() -> Self {
        Leader::new()
    }
}

impl Leader {
    pub fn new() -> Leader {
        Leader { links: Mutex::new(Vec::new()), head: Mutex::new((0, 0)), next_link: AtomicU64::new(1), init_lock: tokio::sync::Mutex::new(()) }
    }

    /// Executes a command and replicates its entries. Pipelined: the lock is released before waiting.
    pub async fn submit(&self, app: &'static App, cmd: Command, origin: Origin, policy: WaitPolicy) -> Result<Outcome, ApiError> {
        if let Command::Initialize = cmd {
            self.initialize(app).await?;
            return Ok(Outcome::Unit);
        }
        if let Command::AbuseReport { ip, abuse } = &cmd {
            app.count_abuse_globally(ip, *abuse);
            return Ok(Outcome::Unit);
        }
        let (outcome, epoch, seq, lsn, links) = {
            let mut st = app.state.write();
            let mut tx = Tx::new(now_us());
            let outcome = command::exec(&st, &mut tx, &cmd)?;
            if tx.is_empty() {
                return Ok(outcome);
            }
            let mut wal = app.wal.inner.lock();
            let links: Vec<Arc<Link>> = self.links.lock().clone();
            let mut payloads: Vec<Vec<u8>> = vec![Vec::new(); links.len()];
            let mut fx = Vec::new();
            let mut lsn = 0;
            for (entry, blob) in tx.entries.drain(..) {
                let seq = wal.last_seq + 1;
                let out = match &blob {
                    None => BlobOut::None,
                    Some(Blob::Inline(b)) => BlobOut::Inline(b),
                    Some(Blob::Stored(r)) => BlobOut::Ref(*r),
                };
                let (rec, blob_off) = encode_record(seq, &entry, out);
                let (off, l) = app.wal.append(&mut wal, &rec, seq).unwrap_or_else(|e| {
                    eprintln!("[wal] FATAL append failed: {e}");
                    std::process::abort();
                });
                lsn = l;
                let loc = match &blob {
                    None => None,
                    Some(Blob::Inline(b)) => blob_off.map(|o| ImageLoc::Wal { off: off + o as u64, len: b.len() as u32 }),
                    Some(Blob::Stored(r)) => Some(r.loc_on(app.cfg.node)),
                };
                apply(&mut st, &entry, loc, &mut fx);
                for (i, link) in links.iter().enumerate() {
                    match (origin, &blob) {
                        // the origin follower already has the (small) bytes: do not send them back
                        (Origin::Link { link: lid, req_id }, Some(Blob::Inline(_))) if lid == link.id => {
                            let (r2, _) = encode_record(seq, &entry, BlobOut::FromForward(req_id));
                            payloads[i].extend_from_slice(&r2);
                        }
                        _ => payloads[i].extend_from_slice(&rec),
                    }
                }
            }
            for (i, link) in links.iter().enumerate() {
                link.send(proto::frame(proto::RECORDS, &payloads[i]));
                link.max_lag.fetch_max(link.lag(wal.epoch, wal.last_seq), Ordering::Relaxed);
            }
            *self.head.lock() = (wal.epoch, wal.last_seq);
            // ACK_MODE=direct: the origin follower gets the outcome right behind the records and completes its
            // client once it has direct acks from every node this leader waits for (it falls back to RESPONSE)
            if let (Origin::Link { link: lid, req_id }, AckMode::Direct, WaitPolicy::AllReplicas) = (origin, app.cfg.ack_mode, policy) {
                if let Some(o) = links.iter().find(|l| l.id == lid) {
                    let wait_for: Vec<NodeId> = links.iter().filter(|l| l.id != lid && !l.is_dead()).map(|l| l.node).collect();
                    let early = proto::Early { req_id, outcome: outcome.clone(), epoch: wal.epoch, seq: wal.last_seq, wait_for };
                    o.send(proto::frame_msg(proto::EARLY, &early));
                }
            }
            app.handle_effects(&st, &fx);
            (outcome, wal.epoch, wal.last_seq, lsn, links)
        };
        if policy == WaitPolicy::AllReplicas {
            // every connected follower (also one still catching up: it applies these right after its
            // catch-up stream) must have applied the records before the client sees success
            for link in links.iter() {
                if !link.is_dead() {
                    link.wait_applied(epoch, seq).await;
                }
            }
        }
        app.wal.wait_synced(lsn).await;
        Ok(outcome)
    }

    /// POST /api/initialize: new epoch everywhere.
    pub async fn initialize(&self, app: &'static App) -> Result<(), ApiError> {
        let _g = self.init_lock.lock().await;
        let t0 = Instant::now();
        let fresh = super::clone_pristine(app).await;
        let mut epoch = rand_u64();
        let (old, links) = {
            let mut st = app.state.write();
            let mut wal = app.wal.inner.lock();
            while epoch == 0 || epoch == wal.epoch {
                epoch = rand_u64();
            }
            app.wal.reset(&mut wal, epoch).map_err(|e| ApiError::internal(format!("wal reset: {e}")))?;
            *self.head.lock() = (epoch, 0);
            let old = app.swap_state(&mut st, fresh);
            app.images.reset_uploads_for_epoch(epoch);
            let links: Vec<Arc<Link>> = self.links.lock().clone();
            for l in &links {
                l.in_sync.store(false, Ordering::SeqCst);
                *l.target.lock() = (epoch, 0);
                l.send(proto::frame_msg(proto::RESET, &Reset { epoch }));
            }
            (old, links)
        };
        super::drop_in_background(old);
        for l in &links {
            if !l.wait_ack(|&(e, _)| e == epoch, RESET_TIMEOUT).await && !l.is_dead() {
                eprintln!("[repl] link {} ({}) did not ack reset; dropping it", l.id, l.node.name());
                l.kill();
            }
        }
        eprintln!("[init] initialized epoch {epoch:016x} in {:.1?} ({} followers)", t0.elapsed(), links.len());
        Ok(())
    }

    /// Clears the per-link maxima (`POST /metrics/reset`).
    pub fn reset_stats(&self) {
        for l in self.links.lock().iter() {
            l.max_ack_us.store(0, Ordering::Relaxed);
            l.slow_acks.store(0, Ordering::Relaxed);
            l.max_lag.store(0, Ordering::Relaxed);
        }
    }

    /// Per-follower replication metrics for /metrics.
    pub fn metrics_json(&self) -> serde_json::Value {
        let (epoch, head) = *self.head.lock();
        let links = self.links.lock();
        serde_json::json!({
            "head_seq": head,
            "followers": links.iter().map(|l| serde_json::json!({
                "node": l.node.name(),
                "lag_now": l.lag(epoch, head),
                "max_lag": l.max_lag.load(Ordering::Relaxed),
                "max_ack_ms": l.max_ack_us.load(Ordering::Relaxed) as f64 / 1000.0,
                "slow_acks": l.slow_acks.load(Ordering::Relaxed),
                "queue_bytes": l.queue_bytes.load(Ordering::Relaxed),
            })).collect::<Vec<_>>(),
        })
    }

    pub fn status_json(&self) -> String {
        let links = self.links.lock();
        let v: Vec<String> = links
            .iter()
            .map(|l| {
                let (e, s) = *l.acked.borrow();
                format!(
                    "{{\"id\":{},\"node\":\"{}\",\"caught_up\":{},\"acked_epoch\":{},\"acked_seq\":{},\"queue_bytes\":{},\"max_ack_ms\":{:.1},\"slow_acks\":{},\"max_lag\":{},\"connected_s\":{}}}",
                    l.id,
                    l.node.name(),
                    l.in_sync.load(Ordering::SeqCst),
                    e,
                    s,
                    l.queue_bytes.load(Ordering::Relaxed),
                    l.max_ack_us.load(Ordering::Relaxed) as f64 / 1000.0,
                    l.slow_acks.load(Ordering::Relaxed),
                    l.max_lag.load(Ordering::Relaxed),
                    l.connected_at.elapsed().as_secs()
                )
            })
            .collect();
        format!("{{\"links\":[{}]}}", v.join(","))
    }

    fn remove_link(&self, id: u64) {
        self.links.lock().retain(|l| l.id != id);
    }
}

fn rand_u64() -> u64 {
    Id::new_v4().lo
}

/// ACK_MODE=direct: streams the leader's own durable seq to every follower (`LEADER_SYNCED`), so an origin
/// follower can tell when the leader's copy of a forwarded write is persisted.
pub async fn run_synced_broadcast(app: &'static App) {
    let me = leader(app);
    let mut rx = app.wal.subscribe_synced();
    let mut last = (u64::MAX, u64::MAX);
    loop {
        if rx.changed().await.is_err() {
            return;
        }
        let lsn = *rx.borrow_and_update();
        let cur = {
            let w = app.wal.inner.lock();
            (w.epoch, crate::wal::Wal::durable_seq(&w, lsn))
        };
        if cur == last {
            continue;
        }
        last = cur;
        let f = proto::frame_msg(proto::LEADER_SYNCED, &Ack { epoch: cur.0, seq: cur.1 });
        for l in me.links.lock().iter() {
            l.send(f.clone());
        }
    }
}

/// Accept loop for follower connections.
pub async fn serve(app: &'static App, listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((s, peer)) => {
                let _ = s.set_nodelay(true);
                tokio::spawn(async move {
                    if let Err(e) = serve_link(app, s).await {
                        eprintln!("[repl] link from {peer} closed: {e}");
                    }
                });
            }
            Err(e) => {
                eprintln!("[repl] accept error: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn leader(app: &App) -> &Leader {
    match &app.repl {
        super::Repl::Leader(l) => l,
        _ => unreachable!("leader code on a follower"),
    }
}

/// Live writer: coalesces queued frames into one flush, heartbeats when idle, tracks queued bytes.
async fn live_writer(wr: tokio::net::tcp::OwnedWriteHalf, mut rx: mpsc::UnboundedReceiver<Bytes>, link: Arc<Link>) -> std::io::Result<()> {
    let mut bw = tokio::io::BufWriter::with_capacity(256 * 1024, wr);
    let hb = proto::frame(proto::HEARTBEAT, &[]);
    loop {
        let f = tokio::select! {
            f = rx.recv() => match f {
                Some(f) => f,
                None => return Ok(()),
            },
            _ = tokio::time::sleep(HEARTBEAT_EVERY) => {
                bw.write_all(&hb).await?;
                bw.flush().await?;
                continue;
            }
            _ = link.kill.notified() => return Ok(()),
        };
        let mut n = f.len() as u64;
        bw.write_all(&f).await?;
        while let Ok(f) = rx.try_recv() {
            n += f.len() as u64;
            bw.write_all(&f).await?;
        }
        bw.flush().await?;
        let _ = link.queue_bytes.try_update(Ordering::Relaxed, Ordering::Relaxed, |q| Some(q.saturating_sub(n)));
    }
}

async fn serve_link(app: &'static App, stream: TcpStream) -> std::io::Result<()> {
    let me = leader(app);
    let (mut rd, wr) = stream.into_split();
    let (ty, payload) = proto::read_frame(&mut rd).await?;
    if ty != proto::HELLO {
        return Err(std::io::Error::other("expected HELLO"));
    }
    let hello: Hello = proto::decode(&payload)?;
    if hello.leader_node != app.cfg.leader_node || hello.node == app.cfg.node || hello.region != hello.node.region() {
        eprintln!(
            "[repl] !!!!! CONFIG MISMATCH: follower {} (region {}) believes the leader is {}, but this leader is {} configured with leader {} — refusing link !!!!!",
            hello.node.name(),
            hello.region.as_str(),
            hello.leader_node.name(),
            app.cfg.node.name(),
            app.cfg.leader_node.name()
        );
        return Err(std::io::Error::other("leader node mismatch"));
    }
    // a new connection from a node supersedes any older link of it (half-open after a crash/reboot):
    // never wait 25 s for acks from a dead incarnation
    let stale: Vec<Arc<Link>> = me.links.lock().iter().filter(|l| l.node == hello.node).cloned().collect();
    for l in stale {
        eprintln!("[repl] link {} ({}) superseded by a new connection; dropping it", l.id, l.node.name());
        l.kill();
        me.remove_link(l.id);
    }
    let (tx, rx) = mpsc::unbounded_channel();
    let id = me.next_link.fetch_add(1, Ordering::SeqCst);
    let link = Arc::new(Link {
        id,
        node: hello.node,
        tx,
        acked: watch::channel((hello.epoch, hello.seq)).0,
        in_sync: AtomicBool::new(false),
        target: Mutex::new((0, 0)),
        kill: Notify::new(),
        dead: AtomicBool::new(false),
        queue_bytes: AtomicU64::new(0),
        max_ack_us: AtomicU64::new(0),
        slow_acks: AtomicU64::new(0),
        last_warn_us: AtomicU64::new(0),
        max_lag: AtomicU64::new(0),
        connected_at: Instant::now(),
    });
    // plan the catch-up and register atomically w.r.t. the sequencer (wal lock): every record after `target`
    // goes through the live queue (and is waited for by its writer).
    let (reset, from, target, epoch, offsets) = {
        let wal = app.wal.inner.lock();
        let same = hello.epoch == wal.epoch && hello.seq <= wal.last_seq && wal.crc_at(hello.seq) == hello.last_crc;
        let from = if same { hello.seq + 1 } else { 1 };
        let target = wal.last_seq;
        *link.target.lock() = (wal.epoch, target);
        if same && hello.seq == target {
            link.in_sync.store(true, Ordering::SeqCst);
        }
        let offsets = wal.record_offsets(from, target);
        let span = wal.span(from, target);
        me.links.lock().push(link.clone());
        (!same, from, target, wal.epoch, (offsets, span))
    };
    eprintln!(
        "[repl] link {id} from {} (epoch {:016x} seq {}) -> {}catch-up {}..={} of epoch {:016x}",
        hello.node.name(),
        hello.epoch,
        hello.seq,
        if reset { "RESET + " } else { "" },
        from,
        target,
        epoch
    );

    // writer: reset/catch-up first, then CAUGHT_UP, then the live queue
    let wlink = link.clone();
    let writer = tokio::spawn(async move {
        let mut wr = wr;
        if reset {
            wr.write_all(&proto::frame_msg(proto::RESET, &Reset { epoch })).await?;
        }
        let (offs, (_start, end)) = offsets;
        let mut i = 0;
        while i < offs.len() {
            let cstart = offs[i];
            let mut j = i + 1;
            while j < offs.len() && offs[j] - cstart < CATCHUP_CHUNK {
                j += 1;
            }
            let cend = if j < offs.len() { offs[j] } else { end };
            if app.wal.inner.lock().epoch != epoch {
                return Err(std::io::Error::other("epoch changed during catch-up"));
            }
            let data = tokio::task::spawn_blocking(move || app.wal.read_at(cstart, cend)).await.map_err(std::io::Error::other)??;
            if app.wal.inner.lock().epoch != epoch {
                return Err(std::io::Error::other("epoch changed during catch-up"));
            }
            wr.write_all(&proto::frame(proto::RECORDS, &data)).await?;
            i = j;
        }
        wr.write_all(&proto::frame_msg(proto::CAUGHT_UP, &Ack { epoch, seq: target })).await?;
        wr.flush().await?;
        live_writer(wr, rx, wlink).await
    });

    // reader: acks + forwarded commands
    let rlink = link.clone();
    let res: std::io::Result<()> = async {
        loop {
            let (ty, payload) = tokio::select! {
                r = proto::read_frame(&mut rd) => r?,
                _ = rlink.kill.notified() => return Err(std::io::Error::other("link killed")),
            };
            match ty {
                proto::ACK => rlink.on_ack(proto::decode(&payload)?),
                proto::FORWARD => {
                    let fwd: Forward = proto::decode(&payload)?;
                    let l = rlink.clone();
                    tokio::spawn(async move {
                        let origin = Origin::Link { link: l.id, req_id: fwd.req_id };
                        let policy = app.policy_for(&fwd.cmd);
                        let result = leader(app).submit(app, fwd.cmd, origin, policy).await;
                        l.send(proto::frame_msg(proto::RESPONSE, &ResponseMsg { req_id: fwd.req_id, result }));
                    });
                }
                other => return Err(std::io::Error::other(format!("unexpected frame {other}"))),
            }
        }
    }
    .await;
    link.kill();
    me.remove_link(id);
    writer.abort();
    res
}
