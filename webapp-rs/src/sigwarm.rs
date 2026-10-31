//! Proactive signatures, on a dedicated low-priority CPU thread. Initial profile/post images
//! precede RealMojis; newly applied images interrupt bulk work and overlap replication ACKs.
//! Cache entries are only expected signatures: image existence, liveness and expiry checks stay intact.

use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use crate::app::App;
use crate::model::{now_us, Id};
use crate::state::State;

type Job = (Id, [u8; 32]);
const CHUNK: usize = 64;
const FRESH_BATCH: usize = 16;
const FRESH_COLLECT: Duration = Duration::from_millis(1);

/// A burst of freshly replicated images should fill SIMD lanes instead of running one scalar-sized
/// pass per image. Bound the wait from the first image, so an isolated write adds at most 1 ms.
fn collect_fresh(rx: &Receiver<Job>, first: Job) -> Vec<Job> {
    let deadline = Instant::now() + FRESH_COLLECT;
    let mut batch = Vec::with_capacity(FRESH_BATCH);
    batch.push(first);
    while batch.len() < FRESH_BATCH {
        // Already queued work adds no coalescing wait, even if this thread was descheduled.
        match rx.try_recv() {
            Ok(job) => { batch.push(job); continue; }
            Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else { break; };
        match rx.recv_timeout(remaining) {
            Ok(job) => batch.push(job),
            Err(_) => break,
        }
    }
    batch
}

pub struct Warmer {
    tx: SyncSender<Job>,
    rx: parking_lot::Mutex<Option<Receiver<Job>>>,
    enabled: bool,
    core_exp: AtomicI64,
    complete_exp: AtomicI64,
    target_exp: AtomicI64,
    changed: tokio::sync::Notify,
}

impl Default for Warmer {
    fn default() -> Self {
        let (tx, rx) = mpsc::sync_channel(16_384);
        Self {
            tx,
            rx: parking_lot::Mutex::new(Some(rx)),
            enabled: std::env::var("ISUREAL_SIGN_PREWARM").as_deref() != Ok("0"),
            core_exp: AtomicI64::new(0),
            complete_exp: AtomicI64::new(0),
            target_exp: AtomicI64::new(0),
            changed: tokio::sync::Notify::new(),
        }
    }
}

/// Always include pristine images (initialize may restore a deleted/replaced image).
/// Add live images created since the pristine dataset without copying the full State.
fn jobs(st: &State, initial: Option<&State>) -> (Vec<Job>, usize) {
    let mut jobs = Vec::with_capacity(st.posts.len() + st.realmojis.len() + st.users.len());
    for (i, u) in st.users.iter().enumerate().filter(|(_, u)| u.is_active()) {
        if let Some(img) = u.profile_image {
            if initial.and_then(|p| p.users.get(i)).and_then(|u| u.profile_image) != Some(img) {
                jobs.push((img, u.secret_hash));
            }
        }
    }
    for p in st.posts.iter().skip(initial.map_or(0, |p| p.posts.len())) {
        let u = st.user(p.author);
        if p.deleted_at.is_none() && u.is_active() {
            jobs.push((p.image, u.secret_hash));
        }
    }
    let core = jobs.len();
    for r in st.realmojis.iter().skip(initial.map_or(0, |p| p.realmojis.len())) {
        let u = st.user(r.reactor);
        let p = st.post(r.post);
        if r.is_current() && u.is_active() && p.deleted_at.is_none() && st.user(p.author).is_active() {
            jobs.push((r.image, u.secret_hash));
        }
    }
    (jobs, core)
}

impl Warmer {
    pub fn start(&'static self, app: &'static App) {
        let Some(rx) = self.rx.lock().take() else {
            return;
        };
        if !self.enabled {
            return;
        }
        std::thread::Builder::new()
            .name("isureal-sigwarm".into())
            .spawn(move || {
                crate::rt::set_thread_nice(10);
                self.run(app, rx);
            })
            .expect("signature warmup thread");
    }

    /// Called under the replication state lock: only a nonblocking bounded enqueue.
    pub fn image_created(&self, image: Id, key: [u8; 32]) {
        if self.enabled {
            // Normal request-path signing remains available if the bounded queue is full.
            let _ = self.tx.try_send((image, key));
        }
    }

    fn fresh_many(&self, app: &App, jobs: &[Job]) {
        if jobs.is_empty() { return; }
        let exp = app.signer.exp_for(now_us() / 1_000_000);
        app.signer.sign_many_serial(jobs, exp);
        let next = self.target_exp.load(Relaxed);
        if next > exp {
            app.signer.precompute(jobs, next);
        }
    }

    fn run(&self, app: &'static App, rx: Receiver<Job>) {
        'generations: loop {
            let now = now_us() / 1_000_000;
            let current = app.signer.exp_for(now);
            // Tiny expiry quanta are supported, but do not justify repeatedly warming millions of images.
            if app.cfg.exp_quantum >= 60 {
                let lead = (app.cfg.exp_quantum / 4).min(300);
                let target = if self.complete_exp.load(Relaxed) < current { current } else { app.signer.exp_for(now + lead) };
                if target > self.complete_exp.load(Relaxed) {
                    self.target_exp.store(target, Relaxed);
                    let (mut work, core) = jobs(&app.pristine, None);
                    let (extra, _) = {
                        let st = app.read();
                        jobs(&st, Some(&app.pristine))
                    };
                    work.extend(extra);
                    let mut scanned = 0;
                    if core == 0 {
                        self.core_exp.store(target, Relaxed);
                        self.changed.notify_waiters();
                    }
                    for chunk in work.chunks(CHUNK) {
                        if app.signer.exp_for(now_us() / 1_000_000) > target {
                            // A slow cold start may straddle a bucket boundary; prioritize the new URLs.
                            continue 'generations;
                        }
                        // New images are few and latency-sensitive; bulk warmup never delays them by more than a chunk.
                        let fresh: Vec<_> = rx.try_iter().take(FRESH_BATCH).collect();
                        self.fresh_many(app, &fresh);
                        app.signer.precompute(chunk, target);
                        scanned += chunk.len();
                        if scanned >= core && self.core_exp.load(Relaxed) < target {
                            self.core_exp.store(target, Relaxed);
                            self.changed.notify_waiters();
                        }
                    }
                    self.complete_exp.store(target, Relaxed);
                    self.changed.notify_waiters();
                    continue;
                }
            }
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(job) => self.fresh_many(app, &collect_fresh(&rx, job)),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Reserve ample time for the 60-second initialize budget. Warmup survives reset because image
    /// IDs and signing keys are immutable. Background work and request-path fallback cover any remainder.
    pub async fn initialize(&self, app: &App) {
        if !self.enabled || app.cfg.exp_quantum < 60 {
            return;
        }
        let exp = app.signer.exp_for(now_us() / 1_000_000);
        let wait = async {
            loop {
                let ready = self.changed.notified();
                tokio::pin!(ready);
                ready.as_mut().enable();
                if self.core_exp.load(Relaxed) >= exp {
                    return;
                }
                ready.await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(45), wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustc_hash::FxHashMap;

    #[test]
    fn fresh_batches_are_bounded_and_preserve_remaining_images() {
        let (tx, rx) = mpsc::sync_channel(32);
        let jobs: Vec<_> = (0..20).map(|i| (Id::new_v4(), [i; 32])).collect();
        for job in &jobs { tx.send(*job).unwrap(); }
        drop(tx);
        let batch = collect_fresh(&rx, rx.recv().unwrap());
        assert_eq!(batch, jobs[..FRESH_BATCH]);
        let tail = collect_fresh(&rx, rx.recv().unwrap());
        assert_eq!(tail, jobs[FRESH_BATCH..]);
        assert!(rx.try_recv().is_err());
    }

    fn fixture() -> State {
        let dir = std::env::temp_dir().join(format!("isureal-sigwarm-test-{}", Id::new_v4()));
        gen_testdata::generate(&dir, gen_testdata::Scale::Small).unwrap();
        let st = crate::loader::load(&dir).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
        st
    }

    #[test]
    fn warmup_orders_core_images_first_and_uses_each_image_owners_key() {
        let st = fixture();
        let (work, core) = jobs(&st, None);
        assert!(core > 0 && core < work.len());
        let mut expected = FxHashMap::default();
        for image in st.users.iter().filter_map(|u| u.profile_image)
            .chain(st.posts.iter().map(|p| p.image))
            .chain(st.realmojis.iter().map(|r| r.image)) {
            let meta = st.images.get(&image).unwrap();
            if st.user(meta.owner).is_active() && crate::rules::image_live(&st, image, &meta) {
                expected.insert(image, st.user(meta.owner).secret_hash);
            }
        }
        let actual: FxHashMap<_, _> = work.iter().copied().collect();
        assert_eq!(actual.len(), work.len(), "initial image jobs are unique");
        assert_eq!(actual, expected, "all live initial images use their actual owner's signing key");
        for (i, (image, _)) in work.iter().enumerate() {
            let is_reaction = matches!(st.images.get(image).unwrap().kind, crate::model::ImageKind::RealMoji);
            assert_eq!(is_reaction, i >= core, "profiles and posts must precede reactions");
        }
    }

    #[test]
    fn refreshed_generation_keeps_restorable_initial_images_and_adds_live_updates() {
        let pristine = fixture();
        let mut live = pristine.clone();
        let owner = live.users.iter().position(|u| u.is_active() && u.profile_image.is_some()).unwrap();
        let old_profile = live.users[owner].profile_image.unwrap();
        let new_profile = Id::new_v4();
        live.users[owner].profile_image = Some(new_profile);
        let mut new_post = live.posts.iter().find(|p| p.deleted_at.is_none() && live.user(p.author).is_active()).unwrap().clone();
        new_post.id = Id::new_v4();
        new_post.image = Id::new_v4();
        let new_post_image = new_post.image;
        let post_author = new_post.author;
        let post_idx = live.posts.len() as u32;
        live.posts.push(new_post.clone());
        let mut removed_post = new_post;
        removed_post.id = Id::new_v4();
        removed_post.image = Id::new_v4();
        removed_post.deleted_at = crate::model::OptTs::some(1);
        let removed_post_image = removed_post.image;
        live.posts.push(removed_post);
        let mut reaction = *live.realmojis.iter().find(|r| r.is_current() && live.user(r.reactor).is_active()).unwrap();
        reaction.id = Id::new_v4();
        reaction.image = Id::new_v4();
        reaction.post = post_idx;
        let new_reaction_image = reaction.image;
        let reactor = reaction.reactor;
        live.realmojis.push(reaction);
        let (mut all, _) = jobs(&pristine, None);
        let (extra, extra_core) = jobs(&live, Some(&pristine));
        assert_eq!(extra_core, 2, "only replaced profile and new live post are extra core jobs");
        assert_eq!(extra.len(), 3, "initial entries and deleted new posts are not repeated");
        all.extend(extra);
        let by_image: FxHashMap<_, _> = all.into_iter().collect();
        assert_eq!(by_image.get(&old_profile), Some(&pristine.users[owner].secret_hash), "initialize can restore the original profile");
        assert_eq!(by_image.get(&new_profile), Some(&live.users[owner].secret_hash));
        assert_eq!(by_image.get(&new_post_image), Some(&live.user(post_author).secret_hash));
        assert_eq!(by_image.get(&new_reaction_image), Some(&live.user(reactor).secret_hash));
        assert!(!by_image.contains_key(&removed_post_image));
    }
}
