//! Large uploaded images (> `ISUREAL_BLOB_INLINE_MAX`, i.e. post images) are NOT replicated in the log.
//!
//! * `BlobStore` — this node's durable blob file `${data_dir}/blobs.dat`: records `[b"IBLB"][u64 key][u32 len]
//!   [data]`, written with `pwrite` and group-commit fsynced *before* the write that references them is
//!   submitted. The log entry only carries a `BlobRef { origin, key, off, len }`.
//! * `BlobCache` — non-durable local cache `${data_dir}/blobcache.dat` of blobs fetched from other nodes
//!   (index in RAM, truncated at startup and on initialize).
//! * `BlobNet` — internal blob service (`ISUREAL_BLOB_LISTEN`, default :7001) on every node + a client with one
//!   persistent multiplexed connection per peer and single-flight per image id.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use rustc_hash::FxHashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};

use crate::model::{BlobRef, Id, NodeId};
use crate::repl::proto;

pub const HDR: u64 = 16;
const MAGIC: &[u8; 4] = b"IBLB";

/// Bounded wait of a request for a remote blob (then it answers 503, which the benchmarker retries).
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(4);
/// The transfer itself keeps going this long in the background (a retry joins it / finds it cached).
const BACKGROUND_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);

fn rand_u64() -> u64 {
    Id::new_v4().lo
}

// ------------------------------------------------------------------------------------------------
// durable local blob store
// ------------------------------------------------------------------------------------------------

struct StoreInner {
    end: u64,
    lsn: u64,
}

pub struct BlobStore {
    file: Arc<File>,
    path: PathBuf,
    /// epoch the file belongs to (`blobs.epoch`); the file is truncated only when the epoch changes
    epoch: Mutex<u64>,
    epoch_path: PathBuf,
    inner: Mutex<StoreInner>,
    fsync: bool,
    written: Arc<(Mutex<u64>, Condvar)>,
    synced: watch::Sender<u64>,
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
}

impl BlobStore {
    pub fn open(dir: &Path, fsync: bool) -> io::Result<BlobStore> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("blobs.dat");
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        let end = file.metadata()?.len();
        let file = Arc::new(file);
        let epoch_path = dir.join("blobs.epoch");
        let epoch = std::fs::read(&epoch_path).ok().and_then(|v| v.try_into().ok()).map(u64::from_le_bytes).unwrap_or(0);
        let (synced, _) = watch::channel(0u64);
        let s = BlobStore {
            file,
            path,
            epoch: Mutex::new(epoch),
            epoch_path,
            inner: Mutex::new(StoreInner { end, lsn: 0 }),
            fsync,
            written: Arc::new((Mutex::new(0), Condvar::new())),
            synced,
            puts: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
        };
        if fsync {
            let file = s.file.clone();
            let written = s.written.clone();
            let synced = s.synced.clone();
            std::thread::Builder::new()
                .name("blob-fsync".into())
                .spawn(move || {
                    let mut done = 0u64;
                    loop {
                        let target = {
                            let (m, cv) = &*written;
                            let mut g = m.lock();
                            while *g <= done {
                                cv.wait(&mut g);
                            }
                            *g
                        };
                        if let Err(e) = file.sync_data() {
                            eprintln!("[blobs] fsync failed: {e}");
                        }
                        done = target;
                        synced.send_replace(target);
                    }
                })
                .expect("spawn blob-fsync");
        }
        Ok(s)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn size(&self) -> u64 {
        self.inner.lock().end
    }

    /// Stores `data` durably (fsynced when enabled) and returns its reference.
    pub async fn put(&'static self, data: Bytes, origin: NodeId) -> io::Result<BlobRef> {
        let key = rand_u64();
        let len = data.len() as u32;
        let (off, lsn) = tokio::task::spawn_blocking(move || -> io::Result<(u64, u64)> {
            let mut buf = Vec::with_capacity(HDR as usize + data.len());
            buf.extend_from_slice(MAGIC);
            buf.extend_from_slice(&key.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&data);
            let mut g = self.inner.lock();
            let off = g.end;
            self.file.write_all_at(&buf, off)?;
            g.end = off + buf.len() as u64;
            g.lsn += 1;
            Ok((off, g.lsn))
        })
        .await
        .map_err(io::Error::other)??;
        if self.fsync {
            {
                let (m, cv) = &*self.written;
                let mut g = m.lock();
                if *g < lsn {
                    *g = lsn;
                }
                cv.notify_one();
            }
            let mut rx = self.synced.subscribe();
            let _ = rx.wait_for(|&v| v >= lsn).await;
        }
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.put_bytes.fetch_add(len as u64, Ordering::Relaxed);
        Ok(BlobRef { origin, key, off: off + HDR, len })
    }

    fn check(&self, buf: &[u8], key: u64, len: u32) -> io::Result<()> {
        if &buf[..4] != MAGIC || u64::from_le_bytes(buf[4..12].try_into().unwrap()) != key || u32::from_le_bytes(buf[12..16].try_into().unwrap()) != len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "blob header mismatch"));
        }
        Ok(())
    }

    /// Reads + verifies a blob (blocking pread of header + data in one call).
    pub fn read(&self, key: u64, off: u64, len: u32) -> io::Result<Bytes> {
        if off < HDR {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad blob offset"));
        }
        let mut v = vec![0u8; HDR as usize + len as usize];
        self.file.read_exact_at(&mut v, off - HDR)?;
        self.check(&v, key, len)?;
        Ok(Bytes::from(v).slice(HDR as usize..))
    }

    /// Page-cache-only read (Linux `RWF_NOWAIT`); `None` if it would block.
    pub fn try_read_nowait(&self, key: u64, off: u64, len: u32) -> Option<Bytes> {
        if off < HDR {
            return None;
        }
        let v = pread_nowait(&self.file, off - HDR, HDR as usize + len as usize)?;
        self.check(&v, key, len).ok()?;
        Some(Bytes::from(v).slice(HDR as usize..))
    }

    /// Makes the store belong to `epoch`: truncated iff the epoch differs from the one it holds blobs of
    /// (initialize). A same-epoch resync keeps them — the leader's log may reference blobs of this origin.
    /// Returns whether it was truncated.
    pub fn reset_for_epoch(&self, epoch: u64) -> io::Result<bool> {
        let mut e = self.epoch.lock();
        if *e == epoch {
            return Ok(false);
        }
        let mut g = self.inner.lock();
        self.file.set_len(0)?;
        if self.fsync {
            self.file.sync_all()?;
        }
        g.end = 0;
        std::fs::write(&self.epoch_path, epoch.to_le_bytes())?;
        if self.fsync {
            File::open(&self.epoch_path)?.sync_all()?;
        }
        *e = epoch;
        Ok(true)
    }

    pub fn epoch(&self) -> u64 {
        *self.epoch.lock()
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn pread_nowait(f: &File, off: u64, len: usize) -> Option<Vec<u8>> {
    use std::os::unix::io::AsRawFd;
    let mut v = Vec::<u8>::with_capacity(len);
    let iov = libc::iovec { iov_base: v.as_mut_ptr() as *mut libc::c_void, iov_len: len };
    let n = unsafe { libc::preadv2(f.as_raw_fd(), &iov, 1, off as libc::off_t, libc::RWF_NOWAIT) };
    if n < 0 || n as usize != len {
        return None;
    }
    // SAFETY: the kernel initialised exactly `len` bytes.
    unsafe { v.set_len(len) };
    Some(v)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn pread_nowait(_f: &File, _off: u64, _len: usize) -> Option<Vec<u8>> {
    None
}

// ------------------------------------------------------------------------------------------------
// non-durable cache of remote blobs
// ------------------------------------------------------------------------------------------------

struct CacheInner {
    end: u64,
    index: FxHashMap<Id, (u64, u32)>,
}

pub struct BlobCache {
    file: File,
    inner: Mutex<CacheInner>,
}

impl BlobCache {
    pub fn open(dir: &Path) -> io::Result<BlobCache> {
        std::fs::create_dir_all(dir)?;
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.join("blobcache.dat"))?;
        Ok(BlobCache { file, inner: Mutex::new(CacheInner { end: 0, index: FxHashMap::default() }) })
    }
    pub fn lookup(&self, id: &Id) -> Option<(u64, u32)> {
        self.inner.lock().index.get(id).copied()
    }
    pub fn read(&self, off: u64, len: u32) -> io::Result<Bytes> {
        let mut v = vec![0u8; len as usize];
        self.file.read_exact_at(&mut v, off)?;
        Ok(Bytes::from(v))
    }
    pub fn try_read_nowait(&self, id: &Id) -> Option<Bytes> {
        let (off, len) = self.lookup(id)?;
        pread_nowait(&self.file, off, len as usize).map(Bytes::from)
    }
    /// Appends (no fsync). Blocking: call from the blocking pool.
    pub fn insert(&self, id: Id, data: &[u8]) -> io::Result<()> {
        let mut g = self.inner.lock();
        if g.index.contains_key(&id) {
            return Ok(());
        }
        let off = g.end;
        self.file.write_all_at(data, off)?;
        g.end = off + data.len() as u64;
        g.index.insert(id, (off, data.len() as u32));
        Ok(())
    }
    pub fn reset(&self) -> io::Result<()> {
        let mut g = self.inner.lock();
        self.file.set_len(0)?;
        g.end = 0;
        g.index.clear();
        Ok(())
    }
    pub fn stats(&self) -> (usize, u64) {
        let g = self.inner.lock();
        (g.index.len(), g.end)
    }
}

// ------------------------------------------------------------------------------------------------
// blob service (server + client)
// ------------------------------------------------------------------------------------------------

const REQ_GET: u8 = 1;
const RESP_OK: u8 = 2;
const RESP_ERR: u8 = 3;
/// node-to-node cumulative ack broadcast (ACK_MODE=direct): `[u8 node][u64 epoch][u64 seq]`, no response
const ACK_BCAST: u8 = 4;

type FetchResult = Result<Bytes, String>;

struct ClientConn {
    tx: mpsc::UnboundedSender<Bytes>,
    pending: Mutex<FxHashMap<u64, oneshot::Sender<FetchResult>>>,
    dead: AtomicBool,
}

#[derive(Default)]
pub struct NetStats {
    pub fetches: AtomicU64,
    pub fetch_bytes: AtomicU64,
    pub fetch_errors: AtomicU64,
    pub fetch_us_total: AtomicU64,
    pub fetch_us_max: AtomicU64,
    pub singleflight_joins: AtomicU64,
    /// requests that gave up waiting (answered 503) while the transfer continued
    pub wait_timeouts: AtomicU64,
    pub served: AtomicU64,
    pub served_bytes: AtomicU64,
    pub serve_errors: AtomicU64,
}

pub struct BlobNet {
    peers: Mutex<[Option<String>; NodeId::COUNT]>,
    conns: [tokio::sync::Mutex<Option<Arc<ClientConn>>>; NodeId::COUNT],
    inflight: Mutex<FxHashMap<Id, Vec<oneshot::Sender<FetchResult>>>>,
    next_req: AtomicU64,
    pub stats: NetStats,
    /// latest (epoch, seq) applied + persisted by each node, as broadcast by that node (ACK_MODE=direct;
    /// the leader's comes over the replication link as LEADER_SYNCED)
    pub peer_acks: [watch::Sender<(u64, u64)>; NodeId::COUNT],
    /// this node's own cumulative ack, pushed to the per-peer broadcast tasks
    pub my_ack: watch::Sender<(u64, u64)>,
    pub ack_bcasts_received: AtomicU64,
}

impl BlobNet {
    pub fn new(peers: [Option<String>; NodeId::COUNT]) -> BlobNet {
        BlobNet {
            peers: Mutex::new(peers),
            conns: Default::default(),
            inflight: Mutex::new(FxHashMap::default()),
            next_req: AtomicU64::new(1),
            stats: NetStats::default(),
            peer_acks: std::array::from_fn(|_| watch::channel((0, 0)).0),
            my_ack: watch::channel((0, 0)).0,
            ack_bcasts_received: AtomicU64::new(0),
        }
    }

    /// ACK_MODE=direct: one task per peer pushes this node's latest cumulative ack to it (coalesced; reconnects
    /// lazily over the blob-service connection).
    pub fn spawn_ack_broadcast(&'static self, me: NodeId, peers: Vec<NodeId>) {
        for peer in peers {
            let mut rx = self.my_ack.subscribe();
            tokio::spawn(async move {
                loop {
                    if rx.changed().await.is_err() {
                        return;
                    }
                    let (epoch, seq) = *rx.borrow_and_update();
                    let mut p = Vec::with_capacity(17);
                    p.push(me.0);
                    p.extend_from_slice(&epoch.to_le_bytes());
                    p.extend_from_slice(&seq.to_le_bytes());
                    match self.conn(peer).await {
                        Ok(c) => {
                            if c.tx.send(proto::frame(ACK_BCAST, &p)).is_err() {
                                c.dead.store(true, Ordering::SeqCst);
                            }
                        }
                        Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
                    }
                }
            });
        }
    }

    /// Waits until `node` has (directly) acked `(epoch, seq)`.
    pub async fn wait_peer_ack(&self, node: NodeId, epoch: u64, seq: u64) {
        let mut rx = self.peer_acks[node.idx()].subscribe();
        let _ = rx.wait_for(|&(e, s)| e == epoch && s >= seq).await;
    }

    /// Changes a peer address (tests / local clusters with ephemeral ports).
    pub fn set_peer(&self, n: NodeId, addr: String) {
        self.peers.lock()[n.idx()] = Some(addr);
    }

    async fn conn(&self, origin: NodeId) -> Result<Arc<ClientConn>, String> {
        let mut slot = self.conns[origin.idx()].lock().await;
        if let Some(c) = slot.as_ref() {
            if !c.dead.load(Ordering::SeqCst) {
                return Ok(c.clone());
            }
        }
        let addr = self.peers.lock()[origin.idx()].clone().ok_or_else(|| format!("no blob peer for {}", origin.name()))?;
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr))
            .await
            .map_err(|_| format!("connect {addr}: timeout"))?
            .map_err(|e| format!("connect {addr}: {e}"))?;
        let _ = stream.set_nodelay(true);
        let (mut rd, wr) = stream.into_split();
        let (tx, rx) = mpsc::unbounded_channel();
        let c = Arc::new(ClientConn { tx, pending: Mutex::new(FxHashMap::default()), dead: AtomicBool::new(false) });
        tokio::spawn(proto::writer_loop(wr, rx));
        let rc = c.clone();
        tokio::spawn(async move {
            let r: io::Result<()> = async {
                loop {
                    let (ty, payload) = proto::read_frame(&mut rd).await?;
                    if payload.len() < 8 {
                        return Err(io::Error::other("short blob response"));
                    }
                    let req = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                    let res = match ty {
                        RESP_OK => Ok(Bytes::from(payload).slice(8..)),
                        _ => Err(String::from_utf8_lossy(&payload[8..]).into_owned()),
                    };
                    if let Some(tx) = rc.pending.lock().remove(&req) {
                        let _ = tx.send(res);
                    }
                }
            }
            .await;
            rc.dead.store(true, Ordering::SeqCst);
            let drained: Vec<_> = rc.pending.lock().drain().map(|(_, t)| t).collect();
            for t in drained {
                let _ = t.send(Err(format!("blob connection closed: {r:?}")));
            }
        });
        *slot = Some(c.clone());
        Ok(c)
    }

    async fn fetch_once(&self, origin: NodeId, key: u64, off: u64, len: u32, timeout: Duration) -> FetchResult {
        let c = self.conn(origin).await?;
        let req = self.next_req.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        c.pending.lock().insert(req, tx);
        let mut p = Vec::with_capacity(28);
        p.extend_from_slice(&req.to_le_bytes());
        p.extend_from_slice(&key.to_le_bytes());
        p.extend_from_slice(&off.to_le_bytes());
        p.extend_from_slice(&len.to_le_bytes());
        if c.tx.send(proto::frame(REQ_GET, &p)).is_err() {
            c.pending.lock().remove(&req);
            c.dead.store(true, Ordering::SeqCst);
            return Err("blob connection closed".into());
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err("blob connection closed".into()),
            Err(_) => {
                c.pending.lock().remove(&req);
                Err(format!("fetch from {} timed out", origin.name()))
            }
        }
    }

    /// Fetches a remote blob and stores it in `cache`. Single-flight per image id: the transfer runs in a
    /// background task that is NOT cancelled when a waiting request gives up (`FETCH_TIMEOUT` ⇒ that request
    /// answers 503; a retry then joins the same transfer or hits the cache).
    pub async fn fetch(&'static self, cache: &'static BlobCache, id: Id, origin: NodeId, key: u64, off: u64, len: u32) -> FetchResult {
        enum Step {
            Cached(u64, u32),
            Wait(oneshot::Receiver<FetchResult>, bool),
        }
        let step = {
            let mut inf = self.inflight.lock();
            let (tx, rx) = oneshot::channel();
            match inf.get_mut(&id) {
                Some(waiters) => {
                    waiters.push(tx);
                    Step::Wait(rx, false)
                }
                None => match cache.lookup(&id) {
                    // finished between the caller's cache check and here
                    Some((coff, clen)) => Step::Cached(coff, clen),
                    None => {
                        inf.insert(id, vec![tx]);
                        Step::Wait(rx, true)
                    }
                },
            }
        };
        let (rx, start) = match step {
            Step::Cached(coff, clen) => {
                return tokio::task::spawn_blocking(move || cache.read(coff, clen)).await.map_err(|e| e.to_string())?.map_err(|e| e.to_string());
            }
            Step::Wait(rx, start) => (rx, start),
        };
        if start {
            tokio::spawn(async move {
                let t0 = Instant::now();
                let mut res = self.fetch_once(origin, key, off, len, BACKGROUND_FETCH_TIMEOUT).await;
                if res.is_err() {
                    // one retry on a fresh connection (the previous one may have just died)
                    res = self.fetch_once(origin, key, off, len, BACKGROUND_FETCH_TIMEOUT).await;
                }
                let us = t0.elapsed().as_micros() as u64;
                match &res {
                    Ok(b) => {
                        self.stats.fetches.fetch_add(1, Ordering::Relaxed);
                        self.stats.fetch_bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
                        self.stats.fetch_us_total.fetch_add(us, Ordering::Relaxed);
                        self.stats.fetch_us_max.fetch_max(us, Ordering::Relaxed);
                        let data = b.clone();
                        // cached before the in-flight entry is removed: later requests hit the cache
                        let _ = tokio::task::spawn_blocking(move || cache.insert(id, &data)).await;
                    }
                    Err(e) => {
                        self.stats.fetch_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("[blobs] fetch of {id} from {} failed: {e}", origin.name());
                    }
                }
                let waiters = self.inflight.lock().remove(&id).unwrap_or_default();
                for w in waiters {
                    let _ = w.send(res.clone());
                }
            });
        } else {
            self.stats.singleflight_joins.fetch_add(1, Ordering::Relaxed);
        }
        match tokio::time::timeout(FETCH_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err("fetch abandoned".into()),
            Err(_) => {
                self.stats.wait_timeouts.fetch_add(1, Ordering::Relaxed);
                Err(format!("fetch from {} still in progress after {:?}", origin.name(), FETCH_TIMEOUT))
            }
        }
    }

    pub fn status_json(&self, store: &BlobStore, cache: &BlobCache) -> String {
        let s = &self.stats;
        let n = s.fetches.load(Ordering::Relaxed);
        let avg_ms = if n == 0 { 0.0 } else { s.fetch_us_total.load(Ordering::Relaxed) as f64 / n as f64 / 1000.0 };
        let (cn, cb) = cache.stats();
        format!(
            "{{\"store_puts\":{},\"store_put_bytes\":{},\"store_file_bytes\":{},\"fetches\":{},\"fetch_bytes\":{},\"fetch_errors\":{},\"fetch_avg_ms\":{:.1},\"fetch_max_ms\":{:.1},\"singleflight_joins\":{},\"wait_timeouts\":{},\"inflight\":{},\"cache_entries\":{},\"cache_bytes\":{},\"served\":{},\"served_bytes\":{},\"serve_errors\":{}}}",
            store.puts.load(Ordering::Relaxed),
            store.put_bytes.load(Ordering::Relaxed),
            store.size(),
            n,
            s.fetch_bytes.load(Ordering::Relaxed),
            s.fetch_errors.load(Ordering::Relaxed),
            avg_ms,
            s.fetch_us_max.load(Ordering::Relaxed) as f64 / 1000.0,
            s.singleflight_joins.load(Ordering::Relaxed),
            s.wait_timeouts.load(Ordering::Relaxed),
            self.inflight.lock().len(),
            cn,
            cb,
            s.served.load(Ordering::Relaxed),
            s.served_bytes.load(Ordering::Relaxed),
            s.serve_errors.load(Ordering::Relaxed),
        )
    }
}

/// Blob service: serves this node's own `blobs.dat` to peers.
pub async fn serve(store: &'static BlobStore, net: &'static BlobNet, listener: TcpListener) {
    loop {
        let (s, _peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                eprintln!("[blobs] accept error: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let _ = s.set_nodelay(true);
        tokio::spawn(async move {
            let (mut rd, wr) = s.into_split();
            let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
            let writer = tokio::spawn(proto::writer_loop(wr, rx));
            loop {
                let Ok((ty, p)) = proto::read_frame(&mut rd).await else { break };
                if ty == ACK_BCAST && p.len() == 17 {
                    if let Some(n) = NodeId::from_u8(p[0]) {
                        let epoch = u64::from_le_bytes(p[1..9].try_into().unwrap());
                        let seq = u64::from_le_bytes(p[9..17].try_into().unwrap());
                        net.ack_bcasts_received.fetch_add(1, Ordering::Relaxed);
                        net.peer_acks[n.idx()].send_if_modified(|v| {
                            let newer = (epoch, seq) != *v;
                            *v = (epoch, seq);
                            newer
                        });
                    }
                    continue;
                }
                if ty != REQ_GET || p.len() != 28 {
                    break;
                }
                let req = u64::from_le_bytes(p[0..8].try_into().unwrap());
                let key = u64::from_le_bytes(p[8..16].try_into().unwrap());
                let off = u64::from_le_bytes(p[16..24].try_into().unwrap());
                let len = u32::from_le_bytes(p[24..28].try_into().unwrap());
                let tx = tx.clone();
                tokio::spawn(async move {
                    let r = match store.try_read_nowait(key, off, len) {
                        Some(b) => Ok(b),
                        None => tokio::task::spawn_blocking(move || store.read(key, off, len)).await.unwrap_or_else(|e| Err(io::Error::other(e))),
                    };
                    let mut out = Vec::with_capacity(13 + len as usize);
                    match r {
                        Ok(b) => {
                            net.stats.served.fetch_add(1, Ordering::Relaxed);
                            net.stats.served_bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
                            out.extend_from_slice(&((9 + b.len()) as u32).to_le_bytes());
                            out.push(RESP_OK);
                            out.extend_from_slice(&req.to_le_bytes());
                            out.extend_from_slice(&b);
                        }
                        Err(e) => {
                            net.stats.serve_errors.fetch_add(1, Ordering::Relaxed);
                            let msg = e.to_string();
                            out.extend_from_slice(&((9 + msg.len()) as u32).to_le_bytes());
                            out.push(RESP_ERR);
                            out.extend_from_slice(&req.to_le_bytes());
                            out.extend_from_slice(msg.as_bytes());
                        }
                    }
                    let _ = tx.send(Bytes::from(out));
                });
            }
            writer.abort();
        });
    }
}

/// Parses `asia1=host:port,asia2=host:port,europe1=host:port,america1=host:port` (a bare region name means that
/// region's primary node).
pub fn parse_peers(s: &str) -> anyhow::Result<[Option<String>; NodeId::COUNT]> {
    let mut out: [Option<String>; NodeId::COUNT] = Default::default();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (n, a) = part.split_once('=').ok_or_else(|| anyhow::anyhow!("bad blob peer {part:?}"))?;
        let n = NodeId::parse(n.trim()).ok_or_else(|| anyhow::anyhow!("bad node in blob peer {part:?}"))?;
        out[n.idx()] = Some(a.trim().to_string());
    }
    Ok(out)
}
