//! Durable, regional session overrides. Business replication carries a session only on ownership
//! handoff; this journal records subsequent replacements and revocations at that owner. Epoch and
//! generation are part of the key so initialize and migration can never revive an older override.
//!
//! `set` and `freeze` are blocking: call them on a blocking worker, without holding application state
//! locks, then recheck the ownership/epoch fence. Concurrent updates share a disk sync and become
//! visible only after it succeeds. Authentication reads never acquire the journal I/O lock.
//! `identify` is only a routing hint, never proof of a valid session.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::command::Command;
use crate::model::Id;

const MAGIC: &[u8; 8] = b"ISURSES2";
const MAX_RECORD: usize = 64 * 1024;
const RECORD_HEADER: usize = 12;
const MAX_BATCH: usize = 256;
const BATCH_DELAY: Duration = Duration::from_micros(200);
type Key = (u64, Id, u64);

#[derive(Serialize, Deserialize)]
enum Record {
    Set {
        epoch: u64,
        user: Id,
        generation: u64,
        token: Option<Id>,
    },
    Freeze {
        epoch: u64,
        user: Id,
        generation: u64,
        cmd: Command,
    },
}

struct Queued {
    ticket: u64,
    record: Record,
    encoded: Vec<u8>,
}

#[derive(Clone, Copy, Default, Serialize)]
pub struct Stats {
    pub records: u64,
    pub batches: u64,
    pub syncs: u64,
    pub disk_us: u64,
    pub disk_max_us: u64,
    pub commit_wait_us: u64,
    pub commit_wait_max_us: u64,
    pub failures: u64,
}

struct Inner {
    values: FxHashMap<(u64, Id, u64), Option<Id>>,
    tokens: FxHashMap<(u64, Id), Id>,
    pending: FxHashMap<Key, Command>,
    // Enqueued freezes fence later mutations immediately, but pending() exposes only durable ones.
    freezing: FxHashSet<Key>,
    queue: VecDeque<Queued>,
    enqueued: u64,
    completed: u64,
    flushing: bool,
    stats: Stats,
    /// Once durability is uncertain, stop exposing any valid token until the process is restarted.
    poisoned: bool,
}

impl Inner {
    fn apply(&mut self, r: &Record) {
        match r {
            Record::Set {
                epoch,
                user,
                generation,
                token,
            } => {
                self.values.insert((*epoch, *user, *generation), *token);
                if let Some(token) = token {
                    self.tokens.insert((*epoch, *token), *user);
                }
            }
            Record::Freeze {
                epoch,
                user,
                generation,
                cmd,
            } => {
                self.pending
                    .entry((*epoch, *user, *generation))
                    .or_insert_with(|| cmd.clone());
            }
        }
    }

    fn check(&self, r: &Record) -> io::Result<()> {
        if self.poisoned {
            return Err(poisoned());
        }
        let (epoch, user, generation, freeze) = match r {
            Record::Set {
                epoch,
                user,
                generation,
                ..
            } => (*epoch, *user, *generation, false),
            Record::Freeze {
                epoch,
                user,
                generation,
                ..
            } => (*epoch, *user, *generation, true),
        };
        if self.pending.contains_key(&(epoch, user, generation))
            || self.freezing.contains(&(epoch, user, generation))
        {
            return Err(io::Error::new(
                if freeze {
                    io::ErrorKind::AlreadyExists
                } else {
                    io::ErrorKind::Other
                },
                "session ownership migration is pending",
            ));
        }
        Ok(())
    }
}

fn poisoned() -> io::Error {
    io::Error::other("session journal is poisoned after a durability failure")
}

fn encode(r: &Record) -> io::Result<Vec<u8>> {
    let bytes = postcard::to_stdvec(r).map_err(io::Error::other)?;
    if bytes.len() > MAX_RECORD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session journal record too large",
        ));
    }
    let size = bytes.len() as u32;
    let mut encoded = Vec::with_capacity(RECORD_HEADER + bytes.len());
    encoded.extend_from_slice(&size.to_le_bytes());
    // Protect framing separately so corrupting a complete record's length cannot masquerade as
    // an incomplete append and silently drop a revocation or migration freeze.
    encoded.extend_from_slice(&(!size).to_le_bytes());
    encoded.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    encoded.extend_from_slice(&bytes);
    Ok(encoded)
}

pub struct SessionStore {
    inner: Mutex<Inner>,
    file: Mutex<File>,
    committed: Condvar,
    fsync: bool,
    #[cfg(test)]
    pause: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

impl SessionStore {
    /// An I/O failure makes durability uncertain for the whole journal. The RPC entry point should
    /// return 503 while unhealthy; read methods independently fail closed as a final safeguard.
    pub fn healthy(&self) -> bool {
        !self.inner.lock().poisoned
    }

    /// Cumulative counters since opening the journal. Disk time excludes the batch collection delay;
    /// commit wait includes queueing and durability, but never application-level per-user lock waits.
    pub fn stats(&self) -> Stats {
        self.inner.lock().stats
    }

    #[cfg(test)]
    pub(crate) fn pause_next_sync(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        assert!(self
            .pause
            .lock()
            .replace((entered_tx, release_rx))
            .is_none());
        (entered_rx, release_tx)
    }

    pub fn open(dir: &Path, fsync: bool) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("sessions.log");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            file.write_all(MAGIC)?;
            if fsync {
                file.sync_all()?;
                File::open(dir)?.sync_all()?;
            }
        } else {
            let mut magic = [0; 8];
            file.read_exact(&mut magic)?;
            if &magic != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid session journal header",
                ));
            }
        }
        let mut inner = Inner {
            values: FxHashMap::default(),
            tokens: FxHashMap::default(),
            pending: FxHashMap::default(),
            freezing: FxHashSet::default(),
            queue: VecDeque::new(),
            enqueued: 0,
            completed: 0,
            flushing: false,
            stats: Stats::default(),
            poisoned: false,
        };
        let mut end = MAGIC.len() as u64;
        while end < len {
            // An incomplete append is discarded. A complete but corrupted record is a hard startup
            // error: silently discarding a durable revocation could resurrect its previous token.
            if len - end < RECORD_HEADER as u64 {
                break;
            }
            let mut header = [0; RECORD_HEADER];
            file.read_exact(&mut header)?;
            let size = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
            let complement = u32::from_le_bytes(header[4..8].try_into().unwrap());
            let crc = u32::from_le_bytes(header[8..].try_into().unwrap());
            if size == 0 || size > MAX_RECORD || complement != !(size as u32) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid session journal record length",
                ));
            }
            if len - end - (RECORD_HEADER as u64) < size as u64 {
                break;
            }
            let mut bytes = vec![0; size];
            file.read_exact(&mut bytes)?;
            if crc32fast::hash(&bytes) != crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "session journal checksum mismatch",
                ));
            }
            let r: Record = postcard::from_bytes(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            inner.apply(&r);
            end += RECORD_HEADER as u64 + size as u64;
        }
        if len > end {
            file.set_len(end)?;
            if fsync {
                file.sync_all()?;
            }
        }
        file.seek(SeekFrom::Start(end))?;
        Ok(Self {
            inner: Mutex::new(inner),
            file: Mutex::new(file),
            committed: Condvar::new(),
            fsync,
            #[cfg(test)]
            pause: Mutex::new(None),
        })
    }

    /// None: use the replicated handoff snapshot. Some(None): explicitly revoked at this generation.
    /// A poisoned journal always returns revoked, preventing fallback to an older handoff token.
    pub fn get(&self, epoch: u64, user: Id, generation: u64) -> Option<Option<Id>> {
        let inner = self.inner.lock();
        if inner.poisoned {
            return Some(None);
        }
        inner.values.get(&(epoch, user, generation)).copied()
    }

    /// Routing only. Replaced tokens remain indexed so their original issuer can locate the current
    /// authority after migration; callers must check the current generation's token there.
    pub fn identify(&self, epoch: u64, token: Id) -> Option<Id> {
        let inner = self.inner.lock();
        if inner.poisoned {
            return None;
        }
        inner.tokens.get(&(epoch, token)).copied()
    }

    pub fn set(&self, epoch: u64, user: Id, generation: u64, token: Option<Id>) -> io::Result<()> {
        self.persist(Record::Set {
            epoch,
            user,
            generation,
            token,
        })
    }

    fn persist(&self, record: Record) -> io::Result<()> {
        let started = Instant::now();
        let encoded = encode(&record)?;
        let mut inner = self.inner.lock();
        inner.check(&record)?;
        if let Record::Freeze {
            epoch,
            user,
            generation,
            ..
        } = &record
        {
            inner.freezing.insert((*epoch, *user, *generation));
        }
        inner.enqueued += 1;
        let ticket = inner.enqueued;
        inner.queue.push_back(Queued {
            ticket,
            record,
            encoded,
        });
        loop {
            // Even a previously committed operation must fail closed if a later I/O failed before
            // it woke. Authentication cannot distinguish a partially appended later revocation.
            if inner.poisoned {
                return Err(poisoned());
            }
            if inner.completed >= ticket {
                let elapsed = started.elapsed().as_micros() as u64;
                inner.stats.commit_wait_us += elapsed;
                inner.stats.commit_wait_max_us = inner.stats.commit_wait_max_us.max(elapsed);
                return Ok(());
            }
            if inner.flushing {
                self.committed.wait(&mut inner);
                continue;
            }
            inner.flushing = true;
            drop(inner);
            // A bounded collection window lets independent users share one fsync. Updates arriving
            // during disk I/O are collected by the next flusher, not serialized into one fsync each.
            std::thread::sleep(BATCH_DELAY);
            let batch: Vec<_> = {
                let mut inner = self.inner.lock();
                let count = inner.queue.len().min(MAX_BATCH);
                inner.queue.drain(..count).collect()
            };
            let mut bytes = Vec::with_capacity(batch.iter().map(|q| q.encoded.len()).sum());
            for q in &batch {
                bytes.extend_from_slice(&q.encoded);
            }
            let disk_started = Instant::now();
            let result =
                {
                    let mut file = self.file.lock();
                    #[cfg(test)]
                    if let Some((entered, release)) = self.pause.lock().take() {
                        let _ = entered.send(());
                        let _ = release.recv();
                    }
                    file.write_all(&bytes).and_then(|()| {
                        if self.fsync {
                            file.sync_data()
                        } else {
                            Ok(())
                        }
                    })
                };
            let disk_us = disk_started.elapsed().as_micros() as u64;
            inner = self.inner.lock();
            inner.stats.disk_us += disk_us;
            inner.stats.disk_max_us = inner.stats.disk_max_us.max(disk_us);
            if result.is_err() {
                inner.stats.failures += 1;
                inner.poisoned = true;
                inner.queue.clear();
            } else {
                inner.stats.records += batch.len() as u64;
                inner.stats.batches += 1;
                inner.stats.syncs += u64::from(self.fsync);
                for q in &batch {
                    inner.apply(&q.record);
                    if let Record::Freeze {
                        epoch,
                        user,
                        generation,
                        ..
                    } = &q.record
                    {
                        inner.freezing.remove(&(*epoch, *user, *generation));
                    }
                    inner.completed = q.ticket;
                }
            }
            inner.flushing = false;
            self.committed.notify_all();
            // Return the concrete I/O error to the flusher; the other waiters observe poison.
            result?;
        }
    }

    /// Durable intent to hand off this generation. Retry this command after a lost response or a
    /// restart; never accept another login/logout until replicated state advances the generation.
    pub fn pending(&self, epoch: u64, user: Id, generation: u64) -> Option<Command> {
        self.inner
            .lock()
            .pending
            .get(&(epoch, user, generation))
            .cloned()
    }

    /// The first frozen command wins. A duplicate returns AlreadyExists, and the caller can recover
    /// the original with `pending`. The existing active token is preserved while the handoff runs.
    pub fn freeze(&self, epoch: u64, user: Id, generation: u64, cmd: Command) -> io::Result<()> {
        self.persist(Record::Freeze {
            epoch,
            user,
            generation,
            cmd,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("isureal-session-store-{}", Id::new_v4()));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !predicate() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for journal worker"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn authentication_reads_progress_while_sync_is_stalled() {
        let dir = TempDir::new();
        let s = std::sync::Arc::new(SessionStore::open(&dir.0, true).unwrap());
        let user = Id::new_v4();
        let old = Id::new_v4();
        let new = Id::new_v4();
        s.set(1, user, 0, Some(old)).unwrap();
        let (entered, release) = s.pause_next_sync();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = {
            let s = s.clone();
            std::thread::spawn(move || done_tx.send(s.set(1, user, 0, Some(new))).unwrap())
        };
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let (read_tx, read_rx) = std::sync::mpsc::channel();
        let reader = {
            let s = s.clone();
            std::thread::spawn(move || {
                read_tx
                    .send((
                        s.healthy(),
                        s.get(1, user, 0),
                        s.identify(1, old),
                        s.identify(1, new),
                        s.pending(1, user, 0).is_none(),
                    ))
                    .unwrap();
            })
        };
        assert_eq!(
            read_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("authentication waited for disk I/O"),
            (true, Some(Some(old)), Some(user), None, true)
        );
        assert!(matches!(
            done_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        reader.join().unwrap();
        writer.join().unwrap();
        assert_eq!(s.get(1, user, 0), Some(Some(new)));
    }

    #[test]
    fn concurrent_updates_share_sync_and_survive_reopen() {
        let dir = TempDir::new();
        let s = std::sync::Arc::new(SessionStore::open(&dir.0, true).unwrap());
        let records: Vec<_> = (0..17).map(|_| (Id::new_v4(), Id::new_v4())).collect();
        let (entered, release) = s.pause_next_sync();
        let mut writers = Vec::new();
        for (index, &(user, token)) in records.iter().enumerate() {
            let s = s.clone();
            writers.push(std::thread::spawn(move || s.set(1, user, 0, Some(token))));
            if index == 0 {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        // The first batch is paused before disk I/O. All remaining records are enqueued for the
        // next batch, so this verifies batching deterministically rather than relying on timing.
        wait_until(|| s.inner.lock().enqueued == records.len() as u64);
        assert_eq!(s.stats().records, 0);
        assert!(records.iter().all(|&(user, _)| s.get(1, user, 0).is_none()));
        release.send(()).unwrap();
        for writer in writers {
            writer.join().unwrap().unwrap();
        }
        assert_eq!(s.stats().records, records.len() as u64);
        assert_eq!(s.stats().batches, 2);
        assert_eq!(s.stats().syncs, 2);
        drop(s);
        let reopened = SessionStore::open(&dir.0, true).unwrap();
        for (user, token) in records {
            assert_eq!(reopened.get(1, user, 0), Some(Some(token)));
            assert_eq!(reopened.identify(1, token), Some(user));
        }
    }

    #[test]
    fn failed_batch_rejects_all_queued_waiters_and_fails_closed() {
        let dir = TempDir::new();
        let s = std::sync::Arc::new(SessionStore::open(&dir.0, false).unwrap());
        let user = Id::new_v4();
        let token = Id::new_v4();
        s.set(1, user, 0, Some(token)).unwrap();
        *s.file.lock() = File::open(dir.0.join("sessions.log")).unwrap();
        let (entered, release) = s.pause_next_sync();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut writers = Vec::new();
        for index in 0..8 {
            let s = s.clone();
            let done = done_tx.clone();
            writers.push(std::thread::spawn(move || {
                let result = s.set(1, if index == 0 { user } else { Id::new_v4() }, 0, None);
                done.send(result.is_err()).unwrap();
            }));
            if index == 0 {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        wait_until(|| s.inner.lock().enqueued == 9);
        release.send(()).unwrap();
        for _ in 0..8 {
            assert!(done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("poisoned journal waiter stuck"));
        }
        for writer in writers {
            writer.join().unwrap();
        }
        assert!(!s.healthy());
        assert_eq!(s.get(1, user, 0), Some(None));
        assert_eq!(s.identify(1, token), None);
        assert_eq!(s.stats().records, 1);
        assert_eq!(s.stats().failures, 1);
    }

    #[test]
    fn queued_freeze_fences_later_mutations_before_publication() {
        let dir = TempDir::new();
        let s = std::sync::Arc::new(SessionStore::open(&dir.0, true).unwrap());
        let user = Id::new_v4();
        let token = Id::new_v4();
        s.set(1, user, 0, Some(token)).unwrap();
        let (entered, release) = s.pause_next_sync();
        let first = {
            let s = s.clone();
            std::thread::spawn(move || s.set(1, Id::new_v4(), 0, None))
        };
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let freeze = {
            let s = s.clone();
            std::thread::spawn(move || s.freeze(1, user, 0, Command::Initialize))
        };
        wait_until(|| s.inner.lock().freezing.contains(&(1, user, 0)));
        assert!(s.pending(1, user, 0).is_none());
        assert_eq!(s.get(1, user, 0), Some(Some(token)));
        assert!(s.set(1, user, 0, None).is_err());
        assert_eq!(
            s.freeze(1, user, 0, Command::Initialize)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        release.send(()).unwrap();
        first.join().unwrap().unwrap();
        freeze.join().unwrap().unwrap();
        assert!(s.pending(1, user, 0).is_some());
        assert!(s.set(1, user, 0, None).is_err());
    }

    #[test]
    fn replacement_and_revocation_survive_reopen() {
        let dir = TempDir::new();
        let user = Id::new_v4();
        let old = Id::new_v4();
        let new = Id::new_v4();
        {
            let s = SessionStore::open(&dir.0, true).unwrap();
            assert_eq!(s.get(10, user, 0), None);
            s.set(10, user, 0, Some(old)).unwrap();
            s.set(10, user, 0, Some(new)).unwrap();
        }
        {
            let s = SessionStore::open(&dir.0, true).unwrap();
            assert_eq!(s.get(10, user, 0), Some(Some(new)));
            assert_eq!(s.identify(10, old), Some(user));
            assert_eq!(s.identify(10, new), Some(user));
            s.set(10, user, 0, None).unwrap();
        }
        let s = SessionStore::open(&dir.0, true).unwrap();
        assert_eq!(s.get(10, user, 0), Some(None));
        assert_eq!(s.identify(10, new), Some(user));
    }

    #[test]
    fn epochs_and_migration_generations_are_isolated() {
        let dir = TempDir::new();
        let user = Id::new_v4();
        let old = Id::new_v4();
        let new = Id::new_v4();
        {
            let s = SessionStore::open(&dir.0, false).unwrap();
            s.set(10, user, 0, Some(old)).unwrap();
            s.set(10, user, 0, None).unwrap();
            s.set(10, user, 2, Some(new)).unwrap();
            s.set(20, user, 0, Some(new)).unwrap();
        }
        let s = SessionStore::open(&dir.0, false).unwrap();
        assert_eq!(s.get(10, user, 0), Some(None));
        assert_eq!(s.get(10, user, 1), None);
        assert_eq!(s.get(10, user, 2), Some(Some(new)));
        assert_eq!(s.get(20, user, 0), Some(Some(new)));
        assert_eq!(s.get(20, user, 2), None);
        assert_eq!(s.identify(20, old), None);
    }

    #[test]
    fn migration_freeze_survives_restart_and_prevents_mutation() {
        let dir = TempDir::new();
        let user = Id::new_v4();
        let token = Id::new_v4();
        let cmd = Command::MoveSession {
            epoch: 1,
            user,
            from: crate::model::Region::Asia,
            generation: 0,
            token: Some(token),
            display_name: Some("migration".into()),
            bio: None,
            region: crate::model::Region::Europe,
        };
        let encoded = postcard::to_stdvec(&cmd).unwrap();
        {
            let s = SessionStore::open(&dir.0, true).unwrap();
            s.set(1, user, 0, Some(token)).unwrap();
            s.freeze(1, user, 0, cmd.clone()).unwrap();
            assert_eq!(s.get(1, user, 0), Some(Some(token)));
            assert!(s.set(1, user, 0, None).is_err());
            assert_eq!(
                s.freeze(1, user, 0, Command::Initialize)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::AlreadyExists
            );
        }
        let s = SessionStore::open(&dir.0, true).unwrap();
        assert_eq!(
            postcard::to_stdvec(&s.pending(1, user, 0).unwrap()).unwrap(),
            encoded
        );
        assert_eq!(s.get(1, user, 0), Some(Some(token)));
        assert!(s.set(1, user, 0, Some(Id::new_v4())).is_err());
        assert!(s.pending(1, user, 1).is_none());
        assert!(s.pending(2, user, 0).is_none());
        s.set(1, user, 1, None).unwrap();
        s.set(2, user, 0, Some(token)).unwrap();
    }

    #[test]
    fn incomplete_tail_is_truncated_before_next_append() {
        let dir = TempDir::new();
        let user = Id::new_v4();
        let token = Id::new_v4();
        let path = dir.0.join("sessions.log");
        {
            let s = SessionStore::open(&dir.0, false).unwrap();
            s.set(1, user, 0, Some(token)).unwrap();
        }
        let complete = std::fs::metadata(&path).unwrap().len();
        let bytes = postcard::to_stdvec(&Record::Set {
            epoch: 1,
            user,
            generation: 0,
            token: None,
        })
        .unwrap();
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&(!(bytes.len() as u32)).to_le_bytes()).unwrap();
            f.write_all(&crc32fast::hash(&bytes).to_le_bytes()).unwrap();
            f.write_all(&bytes[..bytes.len() / 2]).unwrap();
        }
        {
            let s = SessionStore::open(&dir.0, false).unwrap();
            assert_eq!(std::fs::metadata(&path).unwrap().len(), complete);
            assert_eq!(s.get(1, user, 0), Some(Some(token)));
            s.set(1, user, 0, None).unwrap();
        }
        assert_eq!(
            SessionStore::open(&dir.0, false).unwrap().get(1, user, 0),
            Some(None)
        );
    }

    #[test]
    fn corrupt_complete_revocation_does_not_resurrect_token() {
        let dir = TempDir::new();
        let user = Id::new_v4();
        {
            let s = SessionStore::open(&dir.0, false).unwrap();
            s.set(1, user, 0, Some(Id::new_v4())).unwrap();
            s.set(1, user, 0, None).unwrap();
        }
        let path = dir.0.join("sessions.log");
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(SessionStore::open(&dir.0, false).is_err());
    }

    #[test]
    fn corrupt_length_does_not_masquerade_as_incomplete_tail() {
        let dir = TempDir::new();
        {
            let s = SessionStore::open(&dir.0, false).unwrap();
            s.set(1, Id::new_v4(), 0, None).unwrap();
        }
        let path = dir.0.join("sessions.log");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[MAGIC.len()] ^= 128;
        std::fs::write(&path, bytes).unwrap();
        assert!(SessionStore::open(&dir.0, false).is_err());
    }

    #[test]
    fn failed_write_poison_prevents_valid_reads_and_further_mutations() {
        let dir = TempDir::new();
        let user = Id::new_v4();
        let token = Id::new_v4();
        let s = SessionStore::open(&dir.0, false).unwrap();
        assert!(s.healthy());
        s.set(1, user, 0, Some(token)).unwrap();
        // A read-only descriptor deterministically makes write_all fail on every supported platform.
        *s.file.lock() = File::open(dir.0.join("sessions.log")).unwrap();
        assert!(s.set(1, user, 0, None).is_err());
        assert!(!s.healthy());
        assert_eq!(s.get(1, user, 0), Some(None));
        assert_eq!(s.get(1, Id::new_v4(), 99), Some(None));
        assert_eq!(s.identify(1, token), None);
        assert!(s.set(1, user, 0, Some(Id::new_v4())).is_err());
        assert!(s.freeze(1, user, 0, Command::Initialize).is_err());
    }
}
