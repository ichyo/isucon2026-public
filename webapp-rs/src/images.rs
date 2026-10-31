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
}
