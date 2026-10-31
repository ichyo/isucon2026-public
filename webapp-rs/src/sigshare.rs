//! Signature work sharing.
//!
//! * Same-region cache sharing: HAProxy spreads asia traffic over asia1/asia2, so an image URL rendered on one
//!   node is often verified on the other. Every signature a node computes (render or verify) is queued in the
//!   signer's outbox and pushed every `FLUSH_EVERY` (or `BATCH` entries) to the other nodes of its region over
//!   the blob-service connections (frame `SIG_SHARE`), where it is inserted into the cache. Best-effort: the
//!   outbox is bounded and entries are dropped when a peer is unreachable; correctness is unaffected (a
//!   peer-provided value is our own code's output, used as the expected signature).
//! * `VerifyBatcher`: concurrent verification misses (image GETs of URLs nobody here computed) are collected for
//!   ≤ 1 ms / up to one SIMD pass (`Backend::lanes()`) and computed together, deduplicated per (image, exp).

use std::time::Duration;

use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, oneshot};

use crate::model::{Id, NodeId};
use crate::sha256mb::Backend;
use crate::sign::{self, Signer};

pub const SIG_SHARE: u8 = 5;
const FLUSH_EVERY: Duration = Duration::from_millis(5);
const BATCH: usize = 512;
const ENTRY: usize = 16 + 8 + 32;
const COLLECT: Duration = Duration::from_millis(1);

pub fn encode(items: &[(Id, i64, [u8; 32])]) -> Vec<u8> {
    let mut p = Vec::with_capacity(2 + items.len() * ENTRY);
    p.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for (img, exp, sig) in items {
        p.extend_from_slice(&img.to_bytes());
        p.extend_from_slice(&exp.to_le_bytes());
        p.extend_from_slice(sig);
    }
    p
}

pub fn decode(p: &[u8]) -> Option<Vec<(Id, i64, [u8; 32])>> {
    if p.len() < 2 {
        return None;
    }
    let n = u16::from_le_bytes([p[0], p[1]]) as usize;
    if p.len() != 2 + n * ENTRY {
        return None;
    }
    Some(
        p[2..]
            .chunks_exact(ENTRY)
            .map(|c| (Id::from_bytes(c[0..16].try_into().unwrap()), i64::from_le_bytes(c[16..24].try_into().unwrap()), c[24..56].try_into().unwrap()))
            .collect(),
    )
}

/// Starts pushing this node's computed signatures to the other nodes of its region (no-op without peers).
pub fn spawn_sharing(app: &'static crate::app::App) {
    let me = app.cfg.node;
    let peers: Vec<NodeId> = NodeId::ALL.into_iter().filter(|&n| n != me && n.region() == me.region()).collect();
    if peers.is_empty() {
        return;
    }
    app.signer.enable_sharing();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(FLUSH_EVERY);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            iv.tick().await;
            let items = app.signer.take_outbox();
            if items.is_empty() {
                continue;
            }
            for chunk in items.chunks(BATCH) {
                let payload = encode(chunk);
                for &p in &peers {
                    if app.images.net.has_peer(p) {
                        let _ = app.images.net.send_frame(p, SIG_SHARE, &payload).await;
                    }
                }
            }
        }
    });
}

struct Job {
    image: Id,
    key: [u8; 32],
    exp: i64,
    done: oneshot::Sender<[u8; 32]>,
}

/// Batches concurrent verification misses into SIMD passes.
pub struct VerifyBatcher {
    tx: mpsc::UnboundedSender<Job>,
    rx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<Job>>>,
    backend: Backend,
    lanes: usize,
}

impl Default for VerifyBatcher {
    fn default() -> Self {
        VerifyBatcher::with_backend(Backend::get(), Backend::get().lanes())
    }
}

impl VerifyBatcher {
    /// `lanes` = max jobs per batch (1 = no batching: compute each miss directly).
    pub fn with_backend(backend: Backend, lanes: usize) -> VerifyBatcher {
        let (tx, rx) = mpsc::unbounded_channel();
        VerifyBatcher { tx, rx: parking_lot::Mutex::new(Some(rx)), backend, lanes: lanes.max(1) }
    }

    /// Starts the collector task (once; the App must be 'static).
    pub fn start(&self, signer: &'static Signer) {
        if let Some(rx) = self.rx.lock().take() {
            tokio::spawn(collect(rx, signer, self.backend, self.lanes));
        }
    }

    /// The expected signature of (image, exp), computed (and cached) together with concurrent misses.
    pub async fn expected(&self, signer: &'static Signer, image: Id, key: [u8; 32], exp: i64) -> [u8; 32] {
        if let Some(sig) = signer.peek(image, exp) {
            return sig;
        }
        if self.lanes > 1 {
            let (done, rx) = oneshot::channel();
            if self.tx.send(Job { image, key, exp, done }).is_ok() {
                tokio::pin!(rx);
                loop {
                    let changed = signer.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if let Some(sig) = signer.peek(image, exp) {
                        return sig;
                    }
                    tokio::select! {
                        r = &mut rx => {
                            if let Ok(sig) = r {
                                return sig;
                            }
                            break;
                        }
                        _ = &mut changed => {}
                    }
                }
            }
        }
        let acquire = crate::rt::small_sign_permits().acquire();
        tokio::pin!(acquire);
        let permit = loop {
            let changed = signer.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(sig) = signer.peek(image, exp) {
                return sig;
            }
            tokio::select! {
                p = &mut acquire => break p.expect("signature semaphore is never closed"),
                _ = &mut changed => {}
            }
        };
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            signer.sign(&key, image, exp)
        })
        .await
        .unwrap_or_else(|_| sign::compute(&key, image, exp))
    }
}

async fn collect(mut rx: mpsc::UnboundedReceiver<Job>, signer: &'static Signer, backend: Backend, lanes: usize) {
    while let Some(first) = rx.recv().await {
        let mut jobs = vec![first];
        let deadline = tokio::time::Instant::now() + COLLECT;
        while jobs.len() < lanes {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(j)) => jobs.push(j),
                Ok(None) => return,
                Err(_) => break,
            }
        }
        tokio::spawn(run_batch(jobs, signer, backend));
    }
}

async fn run_batch(mut jobs: Vec<Job>, signer: &'static Signer, backend: Backend) {
    let acquire = crate::rt::small_sign_permits().acquire();
    tokio::pin!(acquire);
    let permit = loop {
        let changed = signer.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        // Reply immediately to jobs filled while queued. Their callers need not wait for the rest of
        // the SIMD batch, and cancelled requests no longer occupy CPU on a later pass.
        let mut pending = Vec::with_capacity(jobs.len());
        for job in jobs {
            if job.done.is_closed() {
                continue;
            }
            if let Some(sig) = signer.peek(job.image, job.exp) {
                let _ = job.done.send(sig);
            } else {
                pending.push(job);
            }
        }
        jobs = pending;
        if jobs.is_empty() {
            return;
        }
        tokio::select! {
            p = &mut acquire => break p.expect("signature semaphore is never closed"),
            _ = &mut changed => {}
        }
    };
    // dedupe per (image, exp), group per exp (one SIMD pass computes one exp)
    let mut groups: FxHashMap<i64, Vec<(Id, [u8; 32])>> = FxHashMap::default();
    let mut seen: FxHashMap<(Id, i64), ()> = FxHashMap::default();
    for j in &jobs {
        if seen.insert((j.image, j.exp), ()).is_none() {
            groups.entry(j.exp).or_default().push((j.image, j.key));
        }
    }
    let results = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut out: FxHashMap<(Id, i64), [u8; 32]> = FxHashMap::default();
        for (exp, g) in groups {
            // another request may have computed some of them meanwhile
            let (todo, done): (Vec<_>, Vec<_>) = g.into_iter().partition(|(img, _)| signer.peek(*img, exp).is_none());
            for (img, _) in done {
                if let Some(s) = signer.peek(img, exp) {
                    out.insert((img, exp), s);
                }
            }
            if todo.is_empty() {
                continue;
            }
            let sigs = sign::compute_batch(backend, &todo, exp);
            for ((img, _), s) in todo.iter().zip(sigs) {
                signer.insert(*img, exp, s);
                out.insert((*img, exp), s);
            }
        }
        out
    })
    .await
    .unwrap_or_default();
    for j in jobs {
        let s = match results.get(&(j.image, j.exp)) {
            Some(s) => *s,
            None => sign::compute(&j.key, j.image, j.exp),
        };
        let _ = j.done.send(s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn verification_waiter_uses_peer_cache_while_cpu_lane_is_full() {
        for lanes in [1, 8] {
            let signer: &'static Signer = Box::leak(Box::new(Signer::new(1)));
            let batcher: &'static VerifyBatcher = Box::leak(Box::new(VerifyBatcher::with_backend(Backend::Scalar, lanes)));
            batcher.start(signer);
            signer.enable_sharing();
            let image = Id::new_v4();
            let exp = 1_900_000_000;
            let key = [27; 32];
            let sig = sign::compute(&key, image, exp);
            let _busy = crate::rt::small_sign_permits().acquire().await.unwrap();
            let request = tokio::spawn(async move { batcher.expected(signer, image, key, exp).await });
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(!request.is_finished());
            signer.insert_peer(&[(image, exp, sig)]);
            assert_eq!(tokio::time::timeout(Duration::from_secs(1), request).await.unwrap().unwrap(), sig);
            assert!(signer.take_outbox().is_empty(), "peer cache completion must not compute and share a new signature");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batched_verification_matches_scalar() {
        let signer: &'static Signer = Box::leak(Box::new(Signer::new(1)));
        let b: &'static VerifyBatcher = Box::leak(Box::new(VerifyBatcher::with_backend(Backend::Scalar, 8)));
        b.start(signer);
        let imgs: Vec<Id> = (0..10).map(|_| Id::new_v4()).collect();
        let mut hs = Vec::new();
        for i in 0..24 {
            let img = imgs[i % imgs.len()];
            let exp = 1_900_000_000 + (i % 2) as i64;
            hs.push(tokio::spawn(async move { (img, exp, b.expected(signer, img, [3u8; 32], exp).await) }));
        }
        for h in hs {
            let (img, exp, s) = h.await.unwrap();
            assert_eq!(s, sign::compute(&[3u8; 32], img, exp));
            assert_eq!(signer.peek(img, exp), Some(s));
        }
    }

    #[test]
    fn frame_roundtrip() {
        let items: Vec<_> = (0..3).map(|i| (Id::new_v4(), 1_800_000_000 + i, [i as u8; 32])).collect();
        assert_eq!(decode(&encode(&items)).unwrap(), items);
        assert!(decode(&[1, 0, 0]).is_none());
    }
}
