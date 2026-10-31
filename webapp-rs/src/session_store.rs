//! Durable, regional session overrides. Business replication carries a session only on ownership
//! handoff; this journal records subsequent replacements and revocations at that owner. Epoch and
//! generation are part of the key so initialize and migration can never revive an older override.
//!
//! `set` is blocking: call it on a blocking worker while holding the ownership/epoch fence. It writes
//! and (when configured) fsyncs before publishing the new value. Authentication must consult this
//! store on every request; `identify` is only a routing hint, never proof of a valid session.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::command::Command;
use crate::model::Id;

const MAGIC: &[u8; 8] = b"ISURSES2";
const MAX_RECORD: usize = 64 * 1024;
const RECORD_HEADER: usize = 12;

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

struct Inner {
    file: File,
    values: FxHashMap<(u64, Id, u64), Option<Id>>,
    tokens: FxHashMap<(u64, Id), Id>,
    pending: FxHashMap<(u64, Id, u64), Command>,
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

    fn persist(&mut self, r: &Record, fsync: bool) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "session journal is poisoned after a durability failure",
            ));
        }
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
        let result = self.file.write_all(&encoded).and_then(|()| {
            if fsync {
                self.file.sync_data()
            } else {
                Ok(())
            }
        });
        if let Err(e) = result {
            self.poisoned = true;
            return Err(e);
        }
        self.apply(r);
        Ok(())
    }
}

pub struct SessionStore {
    inner: Mutex<Inner>,
    fsync: bool,
}

impl SessionStore {
    /// An I/O failure makes durability uncertain for the whole journal. The RPC entry point should
    /// return 503 while unhealthy; read methods independently fail closed as a final safeguard.
    pub fn healthy(&self) -> bool {
        !self.inner.lock().poisoned
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
            file,
            values: FxHashMap::default(),
            tokens: FxHashMap::default(),
            pending: FxHashMap::default(),
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
            inner.file.read_exact(&mut header)?;
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
            inner.file.read_exact(&mut bytes)?;
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
            inner.file.set_len(end)?;
            if fsync {
                inner.file.sync_all()?;
            }
        }
        inner.file.seek(SeekFrom::Start(end))?;
        Ok(Self {
            inner: Mutex::new(inner),
            fsync,
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
        let r = Record::Set {
            epoch,
            user,
            generation,
            token,
        };
        let mut inner = self.inner.lock();
        if inner.pending.contains_key(&(epoch, user, generation)) {
            return Err(io::Error::other("session ownership migration is pending"));
        }
        inner.persist(&r, self.fsync)
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
        let mut inner = self.inner.lock();
        if inner.pending.contains_key(&(epoch, user, generation)) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "session migration already frozen",
            ));
        }
        inner.persist(
            &Record::Freeze {
                epoch,
                user,
                generation,
                cmd,
            },
            self.fsync,
        )
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
        s.inner.lock().file = File::open(dir.0.join("sessions.log")).unwrap();
        assert!(s.set(1, user, 0, None).is_err());
        assert!(!s.healthy());
        assert_eq!(s.get(1, user, 0), Some(None));
        assert_eq!(s.get(1, Id::new_v4(), 99), Some(None));
        assert_eq!(s.identify(1, token), None);
        assert!(s.set(1, user, 0, Some(Id::new_v4())).is_err());
        assert!(s.freeze(1, user, 0, Command::Initialize).is_err());
    }
}
