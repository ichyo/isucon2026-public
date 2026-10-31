//! Leader: executes commands, appends + applies + streams records, tracks cumulative acks, serves catch-up.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Notify};

use super::proto::{self, Ack, Forward, Hello, Reset, ResponseMsg};
use super::Origin;
use crate::app::{App, WaitPolicy};
use crate::apply::apply;
use crate::command::{self, Command, Outcome, Tx};
use crate::entry::{encode_record, BlobOut};
use crate::http::ApiError;
use crate::model::*;

pub const ACK_TIMEOUT: Duration = Duration::from_secs(3);
pub const RESET_TIMEOUT: Duration = Duration::from_secs(10);
const CATCHUP_CHUNK: u64 = 4 << 20;

pub struct Link {
    pub id: u64,
    pub region: Region,
    tx: mpsc::UnboundedSender<Bytes>,
    /// cumulative (epoch, seq) acked by the follower
    acked: watch::Sender<(u64, u64)>,
    in_sync: AtomicBool,
    /// (epoch, seq) the follower must reach to count as in-sync
    target: Mutex<(u64, u64)>,
    kill: Notify,
    dead: AtomicBool,
}

impl Link {
    fn send(&self, f: Bytes) {
        let _ = self.tx.send(f);
    }
    fn kill(&self) {
        if !self.dead.swap(true, Ordering::SeqCst) {
            self.in_sync.store(false, Ordering::SeqCst);
            self.kill.notify_waiters();
            self.kill.notify_one();
        }
    }
    fn on_ack(&self, a: Ack) {
        self.acked.send_replace((a.epoch, a.seq));
        let t = *self.target.lock();
        if a.epoch == t.0 && a.seq >= t.1 && !self.in_sync.swap(true, Ordering::SeqCst) {
            eprintln!("[repl] link {} ({}) in sync at seq {}", self.id, self.region.as_str(), a.seq);
        }
    }
    async fn wait_ack(&self, pred: impl Fn(&(u64, u64)) -> bool, timeout: Duration) -> bool {
        if self.dead.load(Ordering::SeqCst) {
            return false;
        }
        let mut rx = self.acked.subscribe();
        tokio::select! {
            r = tokio::time::timeout(timeout, rx.wait_for(|v| pred(v))) => matches!(r, Ok(Ok(_))),
            _ = self.kill.notified() => false,
        }
    }
}

pub struct Leader {
    links: Mutex<Vec<Arc<Link>>>,
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
        Leader { links: Mutex::new(Vec::new()), next_link: AtomicU64::new(1), init_lock: tokio::sync::Mutex::new(()) }
    }

    /// Executes a command and replicates its entries. Pipelined: the lock is released before waiting.
    pub async fn submit(&self, app: &'static App, cmd: Command, origin: Origin, policy: WaitPolicy) -> Result<Outcome, ApiError> {
        if let Command::Initialize = cmd {
            self.initialize(app).await?;
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
                let (rec, blob_off) = encode_record(seq, &entry, blob.as_deref().map_or(BlobOut::None, BlobOut::Inline));
                let (off, l) = app.wal.append(&mut wal, &rec, seq).unwrap_or_else(|e| {
                    eprintln!("[wal] FATAL append failed: {e}");
                    std::process::abort();
                });
                lsn = l;
                let loc = blob_off.map(|o| ImageLoc::Wal { off: off + o as u64, len: blob.as_ref().map_or(0, |b| b.len()) as u32 });
                apply(&mut st, &entry, loc, &mut fx);
                for (i, link) in links.iter().enumerate() {
                    match origin {
                        Origin::Link { link: lid, req_id } if lid == link.id && blob.is_some() => {
                            let (r2, _) = encode_record(seq, &entry, BlobOut::FromForward(req_id));
                            payloads[i].extend_from_slice(&r2);
                        }
                        _ => payloads[i].extend_from_slice(&rec),
                    }
                }
            }
            for (i, link) in links.iter().enumerate() {
                link.send(proto::frame(proto::RECORDS, &payloads[i]));
            }
            app.handle_effects(&st, &fx);
            (outcome, wal.epoch, wal.last_seq, lsn, links)
        };
        if policy == WaitPolicy::AllReplicas {
            for link in links.iter() {
                if !link.in_sync.load(Ordering::SeqCst) {
                    continue;
                }
                let ok = link.wait_ack(|&(e, s)| e != epoch || s >= seq, ACK_TIMEOUT).await;
                if !ok && !link.dead.load(Ordering::SeqCst) {
                    eprintln!("[repl] link {} ({}) did not ack seq {seq} in time; dropping it", link.id, link.region.as_str());
                    link.kill();
                }
            }
        }
        app.wal.wait_synced(lsn).await;
        Ok(outcome)
    }

    /// POST /api/initialize: new epoch everywhere.
    pub async fn initialize(&self, app: &'static App) -> Result<(), ApiError> {
        let _g = self.init_lock.lock().await;
        let t0 = std::time::Instant::now();
        let fresh = super::clone_pristine(app).await;
        let mut epoch = rand_u64();
        let (old, links) = {
            let mut st = app.state.write();
            let mut wal = app.wal.inner.lock();
            while epoch == 0 || epoch == wal.epoch {
                epoch = rand_u64();
            }
            app.wal.reset(&mut wal, epoch).map_err(|e| ApiError::internal(format!("wal reset: {e}")))?;
            let old = app.swap_state(&mut st, fresh);
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
            if !l.wait_ack(|&(e, _)| e == epoch, RESET_TIMEOUT).await {
                eprintln!("[repl] link {} ({}) did not ack reset; dropping it", l.id, l.region.as_str());
                l.kill();
            }
        }
        eprintln!("[init] initialized epoch {epoch:016x} in {:.1?} ({} followers)", t0.elapsed(), links.len());
        Ok(())
    }

    pub fn status_json(&self) -> String {
        let links = self.links.lock();
        let v: Vec<String> = links
            .iter()
            .map(|l| {
                let (e, s) = *l.acked.borrow();
                format!(
                    "{{\"id\":{},\"region\":\"{}\",\"in_sync\":{},\"acked_epoch\":{},\"acked_seq\":{}}}",
                    l.id,
                    l.region.as_str(),
                    l.in_sync.load(Ordering::SeqCst),
                    e,
                    s
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

async fn serve_link(app: &'static App, stream: TcpStream) -> std::io::Result<()> {
    let me = leader(app);
    let (mut rd, wr) = stream.into_split();
    let (ty, payload) = proto::read_frame(&mut rd).await?;
    if ty != proto::HELLO {
        return Err(std::io::Error::other("expected HELLO"));
    }
    let hello: Hello = proto::decode(&payload)?;
    if hello.leader_region != app.cfg.leader_region {
        eprintln!(
            "[repl] !!!!! CONFIG MISMATCH: follower {} believes the leader region is {}, but this leader is configured with {} — refusing link !!!!!",
            hello.region.as_str(),
            hello.leader_region.as_str(),
            app.cfg.leader_region.as_str()
        );
        return Err(std::io::Error::other("leader region mismatch"));
    }
    let (tx, rx) = mpsc::unbounded_channel();
    let id = me.next_link.fetch_add(1, Ordering::SeqCst);
    let link = Arc::new(Link {
        id,
        region: hello.region,
        tx,
        acked: watch::channel((hello.epoch, hello.seq)).0,
        in_sync: AtomicBool::new(false),
        target: Mutex::new((0, 0)),
        kill: Notify::new(),
        dead: AtomicBool::new(false),
    });
    // plan the catch-up and register atomically w.r.t. the sequencer (wal lock)
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
        "[repl] link {id} from {} (epoch {:016x} seq {}) -> {} catch-up {}..={} of epoch {:016x}",
        hello.region.as_str(),
        hello.epoch,
        hello.seq,
        if reset { "RESET +" } else { "" },
        from,
        target,
        epoch
    );

    // writer: reset/catch-up first, then the live queue
    let wlink = link.clone();
    let writer = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let mut wr = wr;
        if reset {
            wr.write_all(&proto::frame_msg(proto::RESET, &Reset { epoch })).await?;
        }
        let (offs, (start, end)) = offsets;
        let mut i = 0;
        while i < offs.len() {
            let cstart = offs[i];
            let mut j = i + 1;
            while j < offs.len() && offs[j] - cstart < CATCHUP_CHUNK {
                j += 1;
            }
            let cend = if j < offs.len() { offs[j] } else { end };
            debug_assert!(cstart >= start);
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
        wr.flush().await?;
        tokio::select! {
            r = proto::writer_loop(wr, rx) => r,
            _ = wlink.kill.notified() => Ok(()),
        }
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
                        let result = leader(app).submit(app, fwd.cmd, origin, WaitPolicy::AllReplicas).await;
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
