//! Image bytes: initial images from `images.pack` (mmap, served zero-copy), uploaded images from the WAL
//! file (pread). Metadata (owner/kind/liveness) lives in `State::images`.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use memmap2::{Advice, Mmap};

use crate::blobs::{BlobCache, BlobNet, BlobStore};
use crate::model::{Id, ImageLoc};

struct PackSlice {
    map: Arc<Mmap>,
    off: usize,
    len: usize,
}
impl AsRef<[u8]> for PackSlice {
    fn as_ref(&self) -> &[u8] {
        &self.map[self.off..self.off + self.len]
    }
}

pub struct ImageStore {
    pack: Option<Arc<Mmap>>,
    wal: Arc<File>,
    /// this node's durable store of large uploads (origin copy)
    pub blobs: BlobStore,
    /// non-durable cache of large uploads fetched from their origin node
    pub cache: BlobCache,
    /// internal blob service client/server state
    pub net: BlobNet,
}

impl ImageStore {
    pub fn open(initial_dir: &Path, wal: Arc<File>, blobs: BlobStore, cache: BlobCache, net: BlobNet) -> io::Result<ImageStore> {
        let path = initial_dir.join("images.pack");
        let pack = match File::open(&path) {
            Ok(f) => {
                if f.metadata()?.len() == 0 {
                    None
                } else {
                    // SAFETY: the pack file is immutable while we run.
                    let m = unsafe { Mmap::map(&f)? };
                    let _ = m.advise(Advice::Random);
                    Some(Arc::new(m))
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                eprintln!("[images] {} not found; initial images unavailable", path.display());
                None
            }
            Err(e) => return Err(e),
        };
        Ok(ImageStore { pack, wal, blobs, cache, net })
    }

    /// Reads the bytes synchronously (may block on disk I/O). Remote blobs only from the local cache
    /// (`NotFound` if not cached yet) — use `read` for them.
    pub fn read_sync(&self, id: Id, loc: ImageLoc) -> io::Result<Bytes> {
        match loc {
            ImageLoc::Pack { off, len } => {
                let map = self.pack.as_ref().ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no images.pack"))?;
                let (off, len) = (off as usize, len as usize);
                if off + len > map.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "image beyond pack end"));
                }
                // ask the kernel for the whole range at once, then pre-fault it here (blocking pool)
                let _ = map.advise_range(Advice::WillNeed, off, len);
                let mut acc = 0u8;
                let mut i = off;
                while i < off + len {
                    acc = acc.wrapping_add(unsafe { std::ptr::read_volatile(map.as_ptr().add(i)) });
                    i += 4096;
                }
                std::hint::black_box(acc);
                Ok(Bytes::from_owner(PackSlice { map: map.clone(), off, len }))
            }
            ImageLoc::Wal { off, len } => {
                let mut v = vec![0u8; len as usize];
                self.wal.read_exact_at(&mut v, off)?;
                Ok(Bytes::from(v))
            }
            ImageLoc::Blob { key, off, len } => self.blobs.read(key, off, len),
            ImageLoc::Remote { .. } => match self.cache.lookup(&id) {
                Some((off, len)) => self.cache.read(off, len),
                None => Err(io::Error::new(io::ErrorKind::NotFound, "remote blob not cached")),
            },
        }
    }

    /// Reads the bytes in the blocking pool (never stalls a tokio worker on a cold page). Remote blobs are
    /// fetched from their origin node (single-flight, bounded wait) and cached locally; a failed fetch is an
    /// error of kind `TimedOut`/`Other` — answer 503 then (`is_unavailable`).
    pub async fn read(&'static self, id: Id, loc: ImageLoc) -> io::Result<Bytes> {
        if let ImageLoc::Remote { origin, key, off, len } = loc {
            if self.cache.lookup(&id).is_none() {
                return self.net.fetch(&self.cache, id, origin, key, off, len).await.map_err(|e| io::Error::new(io::ErrorKind::TimedOut, e));
            }
        }
        tokio::task::spawn_blocking(move || self.read_sync(id, loc)).await.map_err(io::Error::other)?
    }

    /// True for errors that mean "temporarily unavailable" (remote fetch failed) ⇒ 503.
    pub fn is_unavailable(loc: ImageLoc, e: &io::Error) -> bool {
        matches!(loc, ImageLoc::Remote { .. }) || e.kind() == io::ErrorKind::TimedOut
    }

    /// Non-blocking fast path (no blocking-pool hop): the bytes when they can be produced without waiting for
    /// disk I/O, else `None` (then use `read`). Initial images: every page of the range is resident (`mincore`)
    /// ⇒ zero-copy mmap slice. Uploaded images / blobs / cached remote blobs: one `preadv2(RWF_NOWAIT)` from
    /// the page cache (Linux only).
    pub fn try_read_cached(&self, id: Id, loc: ImageLoc) -> Option<Bytes> {
        match loc {
            ImageLoc::Pack { off, len } => {
                let map = self.pack.as_ref()?;
                let (off, len) = (off as usize, len as usize);
                if len == 0 || off + len > map.len() || !resident(map, off, len) {
                    return None;
                }
                Some(Bytes::from_owner(PackSlice { map: map.clone(), off, len }))
            }
            ImageLoc::Wal { off, len } => wal_read_nowait(&self.wal, off, len as usize),
            ImageLoc::Blob { key, off, len } => self.blobs.try_read_nowait(key, off, len),
            ImageLoc::Remote { .. } => self.cache.try_read_nowait(&id),
        }
    }

    /// Drops all uploaded blobs and the remote cache when `epoch` is a new epoch (initialize); no-op for a
    /// same-epoch resync. Returns whether anything was dropped.
    pub fn reset_uploads_for_epoch(&self, epoch: u64) -> bool {
        match self.blobs.reset_for_epoch(epoch) {
            Ok(false) => false,
            Ok(true) => {
                if let Err(e) = self.cache.reset() {
                    eprintln!("[images] blob cache reset failed: {e}");
                }
                true
            }
            Err(e) => {
                eprintln!("[images] blob store reset failed: {e}");
                false
            }
        }
    }
}

fn page_size() -> usize {
    static PS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *PS.get_or_init(|| match unsafe { libc::sysconf(libc::_SC_PAGESIZE) } {
        n if n > 0 => n as usize,
        _ => 4096,
    })
}

/// All pages of `map[off..off+len]` are in RAM (so touching them cannot block on the disk).
fn resident(map: &Mmap, off: usize, len: usize) -> bool {
    let ps = page_size();
    let start = off & !(ps - 1);
    let span = off + len - start;
    let pages = span.div_ceil(ps);
    let mut vec = [0u8; 64];
    if pages > vec.len() {
        return false;
    }
    // SAFETY: the range lies inside the mapping; the base of an mmap is page-aligned, so is `start`.
    let r = unsafe { libc::mincore(map.as_ptr().add(start) as _, span, vec.as_mut_ptr() as _) };
    r == 0 && vec[..pages].iter().all(|&b| b & 1 != 0)
}

/// EAGAIN (not cached) / EOPNOTSUPP / short read ⇒ None (caller falls back to the blocking pool)
fn wal_read_nowait(f: &File, off: u64, len: usize) -> Option<Bytes> {
    crate::blobs::pread_nowait(f, off, len).map(Bytes::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_reads_match_blocking_reads() {
        let dir = std::env::temp_dir().join(format!("isureal-images-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i * 7 % 251) as u8).collect();
        std::fs::write(dir.join("images.pack"), &data).unwrap();
        std::fs::write(dir.join("wal.log"), &data).unwrap();
        let store = ImageStore::open(
            &dir,
            Arc::new(File::open(dir.join("wal.log")).unwrap()),
            BlobStore::open(&dir, false).unwrap(),
            BlobCache::open(&dir).unwrap(),
            BlobNet::new(Default::default()),
        )
        .unwrap();
        let id = Id::NIL;
        for (off, len) in [(0u64, 1u32), (5000, 3000), (4095, 2), (150_000, 50_000)] {
            let pack = ImageLoc::Pack { off, len };
            let slow = store.read_sync(id, pack).unwrap();
            assert_eq!(&slow[..], &data[off as usize..(off + len as u64) as usize]);
            assert_eq!(store.try_read_cached(id, pack).expect("resident after read_sync"), slow);
            let wal = ImageLoc::Wal { off, len };
            let slow = store.read_sync(id, wal).unwrap();
            match store.try_read_cached(id, wal) {
                Some(fast) => assert_eq!(fast, slow),
                #[cfg(target_os = "linux")]
                None => panic!("RWF_NOWAIT read of a cached file must succeed"),
                #[cfg(not(target_os = "linux"))]
                None => {}
            }
        }
        assert!(store.try_read_cached(id, ImageLoc::Pack { off: 199_999, len: 2 }).is_none(), "beyond the end");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
