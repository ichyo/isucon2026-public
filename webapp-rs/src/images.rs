//! Image bytes: initial images from `images.pack` (mmap, served zero-copy), uploaded images from the WAL
//! file (pread). Metadata (owner/kind/liveness) lives in `State::images`.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use memmap2::{Advice, Mmap};

use crate::model::ImageLoc;

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
}

impl ImageStore {
    pub fn open(initial_dir: &Path, wal: Arc<File>) -> io::Result<ImageStore> {
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
        Ok(ImageStore { pack, wal })
    }

    /// Reads the bytes synchronously (may block on disk I/O). Prefer `read`.
    pub fn read_sync(&self, loc: ImageLoc) -> io::Result<Bytes> {
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
        }
    }

    /// Reads the bytes in the blocking pool (never stalls a tokio worker on a cold page).
    pub async fn read(&'static self, loc: ImageLoc) -> io::Result<Bytes> {
        tokio::task::spawn_blocking(move || self.read_sync(loc)).await.map_err(io::Error::other)?
    }

    /// Non-blocking fast path (no blocking-pool hop): the bytes when they can be produced without waiting for
    /// disk I/O, else `None` (then use `read`). Initial images: every page of the range is resident (`mincore`)
    /// ⇒ zero-copy mmap slice. Uploaded images: one `preadv2(RWF_NOWAIT)` from the page cache (Linux only).
    pub fn try_read_cached(&self, loc: ImageLoc) -> Option<Bytes> {
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

#[cfg(target_os = "linux")]
fn wal_read_nowait(f: &File, off: u64, len: usize) -> Option<Bytes> {
    use std::os::unix::io::AsRawFd;
    let mut v = Vec::<u8>::with_capacity(len);
    let iov = libc::iovec { iov_base: v.as_mut_ptr() as *mut libc::c_void, iov_len: len };
    // EAGAIN (not cached) / EOPNOTSUPP / short read ⇒ None (caller falls back to the blocking pool)
    let n = unsafe { libc::preadv2(f.as_raw_fd(), &iov, 1, off as libc::off_t, libc::RWF_NOWAIT) };
    if n < 0 || n as usize != len {
        return None;
    }
    // SAFETY: the kernel initialised exactly `len` bytes.
    unsafe { v.set_len(len) };
    Some(Bytes::from(v))
}

#[cfg(not(target_os = "linux"))]
fn wal_read_nowait(_f: &File, _off: u64, _len: usize) -> Option<Bytes> {
    None
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
        let store = ImageStore::open(&dir, Arc::new(File::open(dir.join("wal.log")).unwrap())).unwrap();
        for (off, len) in [(0u64, 1u32), (5000, 3000), (4095, 2), (150_000, 50_000)] {
            let pack = ImageLoc::Pack { off, len };
            let slow = store.read_sync(pack).unwrap();
            assert_eq!(&slow[..], &data[off as usize..(off + len as u64) as usize]);
            assert_eq!(store.try_read_cached(pack).expect("resident after read_sync"), slow);
            let wal = ImageLoc::Wal { off, len };
            let slow = store.read_sync(wal).unwrap();
            match store.try_read_cached(wal) {
                Some(fast) => assert_eq!(fast, slow),
                #[cfg(target_os = "linux")]
                None => panic!("RWF_NOWAIT read of a cached file must succeed"),
                #[cfg(not(target_os = "linux"))]
                None => {}
            }
        }
        assert!(store.try_read_cached(ImageLoc::Pack { off: 199_999, len: 2 }).is_none(), "beyond the end");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
