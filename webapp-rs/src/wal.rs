//! Append-only write-ahead log: `${data_dir}/wal.log` = header (`ISURWAL1` + epoch u64) + records
//! (format in entry.rs). Writes are `pwrite` (bytes are in the page cache before the state change is
//! visible); a dedicated thread fsyncs in groups (group commit) and publishes the synced LSN.

use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use tokio::sync::watch;

use crate::entry::{self, DecodeErr, Decoded, REC_HDR};

pub const MAGIC: &[u8; 8] = b"ISURWAL1";
pub const HEADER_LEN: u64 = 16;

pub struct WalInner {
    pub epoch: u64,
    pub last_seq: u64,
    /// logical end of file (next record offset)
    pub end: u64,
    /// monotonically increasing log position (never reset; used for durability waits)
    pub lsn: u64,
    /// seq of a record = its lsn + seq_offset (within the current epoch)
    seq_offset: i64,
    /// offsets[i] = file offset of record with seq i+1
    offsets: Vec<u64>,
    crcs: Vec<u32>,
}

impl WalInner {
    /// Byte range [start, end) of records (from..=to].
    pub fn span(&self, from_seq: u64, to_seq: u64) -> (u64, u64) {
        if from_seq > to_seq || from_seq == 0 || to_seq > self.last_seq {
            return (self.end, self.end);
        }
        let start = self.offsets[(from_seq - 1) as usize];
        let end = if to_seq == self.last_seq { self.end } else { self.offsets[to_seq as usize] };
        (start, end)
    }
    /// CRC of the record with this seq (0 for seq 0 / unknown).
    pub fn crc_at(&self, seq: u64) -> u32 {
        if seq == 0 || seq > self.last_seq {
            0
        } else {
            self.crcs[(seq - 1) as usize]
        }
    }
    pub fn last_crc(&self) -> u32 {
        self.crc_at(self.last_seq)
    }
    /// Offsets of every record in (from..=to], for chunking catch-up reads at record boundaries.
    pub fn record_offsets(&self, from_seq: u64, to_seq: u64) -> Vec<u64> {
        if from_seq > to_seq || from_seq == 0 || to_seq > self.last_seq {
            return Vec::new();
        }
        self.offsets[(from_seq - 1) as usize..to_seq as usize].to_vec()
    }
}

pub struct Wal {
    pub inner: Mutex<WalInner>,
    file: Arc<File>,
    path: PathBuf,
    fsync: bool,
    written: Arc<(Mutex<u64>, Condvar)>,
    synced: watch::Sender<u64>,
}

fn write_header(file: &File, epoch: u64) -> io::Result<()> {
    let mut h = [0u8; HEADER_LEN as usize];
    h[..8].copy_from_slice(MAGIC);
    h[8..].copy_from_slice(&epoch.to_le_bytes());
    file.write_all_at(&h, 0)
}

impl Wal {
    /// Opens (or creates) the WAL. Call `replay` once afterwards.
    pub fn open(dir: &Path, fsync: bool) -> io::Result<Wal> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("wal.log");
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        let len = file.metadata()?.len();
        let mut epoch = 0;
        let mut ok = false;
        if len >= HEADER_LEN {
            let mut h = [0u8; HEADER_LEN as usize];
            file.read_exact_at(&mut h, 0)?;
            if &h[..8] == MAGIC {
                epoch = u64::from_le_bytes(h[8..].try_into().unwrap());
                ok = true;
            }
        }
        if !ok {
            if len > 0 {
                let bak = dir.join(format!("wal.log.bad.{}", crate::model::now_us()));
                eprintln!("[wal] unrecognized WAL header, moving it to {}", bak.display());
                std::fs::copy(&path, &bak)?;
            }
            file.set_len(0)?;
            write_header(&file, 0)?;
            file.sync_all()?;
        }
        let file = Arc::new(file);
        let (synced, _) = watch::channel(0u64);
        let written = Arc::new((Mutex::new(0u64), Condvar::new()));
        let wal = Wal {
            inner: Mutex::new(WalInner { epoch, last_seq: 0, end: HEADER_LEN, lsn: 0, seq_offset: 0, offsets: Vec::new(), crcs: Vec::new() }),
            file,
            path,
            fsync,
            written,
            synced,
        };
        if fsync {
            wal.spawn_syncer();
        }
        Ok(wal)
    }

    fn spawn_syncer(&self) {
        let file = self.file.clone();
        let written = self.written.clone();
        let synced = self.synced.clone();
        std::thread::Builder::new()
            .name("wal-fsync".into())
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
                        eprintln!("[wal] fsync failed: {e}");
                    }
                    done = target;
                    synced.send_replace(target);
                }
            })
            .expect("spawn wal-fsync");
    }

    pub fn file(&self) -> Arc<File> {
        self.file.clone()
    }
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replays every valid record (seq must be contiguous from 1); truncates a torn/garbled tail.
    /// `f(decoded, record_bytes, record_file_offset)`.
    pub fn replay(&self, mut f: impl FnMut(&Decoded, &[u8], u64)) -> io::Result<u64> {
        let mut inner = self.inner.lock();
        let file_len = self.file.metadata()?.len();
        let mut reader = io::BufReader::with_capacity(8 << 20, &*self.file);
        // skip header
        let mut h = [0u8; HEADER_LEN as usize];
        reader.read_exact(&mut h)?;
        let mut off = HEADER_LEN;
        let mut n = 0u64;
        let mut buf: Vec<u8> = Vec::new();
        loop {
            if off + REC_HDR as u64 > file_len {
                break;
            }
            let mut hdr = [0u8; REC_HDR];
            if reader.read_exact(&mut hdr).is_err() {
                break;
            }
            let total = entry::record_len(&hdr).unwrap();
            if off + total as u64 > file_len {
                break;
            }
            buf.clear();
            buf.extend_from_slice(&hdr);
            buf.resize(total, 0);
            if reader.read_exact(&mut buf[REC_HDR..]).is_err() {
                break;
            }
            let d = match entry::decode_record(&buf) {
                Ok(d) => d,
                Err(DecodeErr::Short) => break,
                Err(DecodeErr::Corrupt(why)) => {
                    eprintln!("[wal] corrupt record at offset {off}: {why}; truncating");
                    break;
                }
            };
            if d.seq != inner.last_seq + 1 {
                eprintln!("[wal] non-contiguous seq {} after {} at offset {off}; truncating", d.seq, inner.last_seq);
                break;
            }
            f(&d, &buf, off);
            inner.offsets.push(off);
            inner.crcs.push(d.crc);
            inner.last_seq = d.seq;
            off += total as u64;
            n += 1;
        }
        if off != file_len {
            eprintln!("[wal] truncating WAL from {file_len} to {off} bytes");
            self.file.set_len(off)?;
            self.file.sync_all()?;
        }
        inner.end = off;
        // replayed records are durable; lsn 0 stands for the last replayed seq
        inner.seq_offset = inner.last_seq as i64 - inner.lsn as i64;
        Ok(n)
    }

    /// Appends one encoded record (must have seq == last_seq + 1). Returns (file offset, lsn).
    pub fn append(&self, inner: &mut WalInner, rec: &[u8], seq: u64) -> io::Result<(u64, u64)> {
        debug_assert_eq!(seq, inner.last_seq + 1);
        let off = inner.end;
        self.file.write_all_at(rec, off)?;
        let crc = u32::from_le_bytes(rec[4..8].try_into().unwrap());
        inner.offsets.push(off);
        inner.crcs.push(crc);
        inner.last_seq = seq;
        inner.end = off + rec.len() as u64;
        inner.lsn += 1;
        let lsn = inner.lsn;
        if self.fsync {
            let (m, cv) = &*self.written;
            *m.lock() = lsn;
            cv.notify_one();
        } else {
            self.synced.send_replace(lsn);
        }
        Ok((off, lsn))
    }

    /// Truncates the log and starts a new epoch (initialize / follower reset). Synchronous fsync.
    pub fn reset(&self, inner: &mut WalInner, epoch: u64) -> io::Result<()> {
        self.file.set_len(0)?;
        write_header(&self.file, epoch)?;
        if self.fsync {
            self.file.sync_all()?;
        }
        inner.epoch = epoch;
        inner.last_seq = 0;
        inner.end = HEADER_LEN;
        inner.offsets.clear();
        inner.crcs.clear();
        // everything before is gone; treat the current lsn as durable
        inner.lsn += 1;
        let lsn = inner.lsn;
        inner.seq_offset = -(lsn as i64);
        if self.fsync {
            let (m, cv) = &*self.written;
            *m.lock() = lsn;
            cv.notify_one();
        } else {
            self.synced.send_replace(lsn);
        }
        Ok(())
    }

    /// Highest seq of the current epoch that is durable, given the synced lsn.
    pub fn durable_seq(inner: &WalInner, synced_lsn: u64) -> u64 {
        (synced_lsn as i64 + inner.seq_offset).clamp(0, inner.last_seq as i64) as u64
    }

    pub fn synced_lsn(&self) -> u64 {
        *self.synced.borrow()
    }

    pub fn subscribe_synced(&self) -> watch::Receiver<u64> {
        self.synced.subscribe()
    }

    /// Waits until everything up to `lsn` is durable.
    pub async fn wait_synced(&self, lsn: u64) {
        let mut rx = self.synced.subscribe();
        let _ = rx.wait_for(|&v| v >= lsn).await;
    }

    /// Reads raw bytes [start, end) of the file.
    pub fn read_at(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        let mut v = vec![0u8; (end - start) as usize];
        self.file.read_exact_at(&mut v, start)?;
        Ok(v)
    }
}
