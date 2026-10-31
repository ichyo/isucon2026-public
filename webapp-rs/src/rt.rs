//! Runtimes and CPU budgets.
//!
//! * `repl()` — a small dedicated tokio runtime (own worker threads, own I/O driver) for everything on the
//!   replication path: leader link reader/writer + ack processing + command execution (`submit`), follower
//!   session (reader/apply/acker/writer), direct-ack broadcasts and the blob service. Its threads run at
//!   nice `REPL_NICE` (−10), so records and acks keep flowing within ~RTT while the request runtime is at
//!   100 % CPU. Raising priority needs `CAP_SYS_NICE` (systemd `AmbientCapabilities`); without it a warning is
//!   logged once and the threads run at normal priority (still isolated from the request runtime's queue).
//!   Sockets used by these tasks must be created on this runtime (its driver handles their readiness).
//! * `jpeg_permits()` / `sign_permits()` — global semaphores bounding concurrent JPEG decodes
//!   (`ISUREAL_JPEG_CONCURRENCY`) and off-lock signature batches (`ISUREAL_SIGN_CONCURRENCY`), default = CPUs.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use tokio::sync::Semaphore;

pub const REPL_NICE: i32 = -10;
const REPL_THREADS: usize = 2;

static REPL: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static JPEG: OnceLock<Semaphore> = OnceLock::new();
static SIGN: OnceLock<Semaphore> = OnceLock::new();
// A short request must not queue behind every pending feed/export batch. This lane is bounded
// independently and only accepts one SIMD pass (or a handful of scalar signatures) at a time.
static SIGN_SMALL: OnceLock<Semaphore> = OnceLock::new();
static WARNED: AtomicBool = AtomicBool::new(false);
pub fn cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2)
}

/// Sets the calling thread's nice value (Linux: per-thread via setpriority on the tid). Returns success.
pub fn set_thread_nice(nice: i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: plain syscalls on the current thread.
        let tid = unsafe { libc::syscall(libc::SYS_gettid) } as libc::id_t;
        let ok = unsafe { libc::setpriority(libc::PRIO_PROCESS, tid, nice) } == 0;
        if !ok && nice < 0 && !WARNED.swap(true, Ordering::Relaxed) {
            eprintln!("[rt] WARN cannot raise replication thread priority to nice {nice}: {} (grant CAP_SYS_NICE via AmbientCapabilities)", std::io::Error::last_os_error());
        }
        ok
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (nice, &WARNED, Ordering::Relaxed);
        false
    }
}

/// The replication runtime (created on first use).
pub fn repl() -> &'static tokio::runtime::Handle {
    REPL.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(REPL_THREADS)
            .thread_name("isureal-repl")
            .on_thread_start(|| {
                set_thread_nice(REPL_NICE);
            })
            .enable_all()
            .build()
            .expect("replication runtime")
    })
    .handle()
}

/// Spawns onto the replication runtime.
pub fn spawn<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    repl().spawn(f)
}

/// Sets the CPU budgets (call once at startup; later calls are ignored). 0 = number of CPUs.
pub fn init_budgets(jpeg: usize, sign: usize) {
    let n = |v: usize| if v == 0 { cpus() } else { v };
    let _ = JPEG.set(Semaphore::new(n(jpeg)));
    let _ = SIGN.set(Semaphore::new(n(sign)));
}

pub fn jpeg_permits() -> &'static Semaphore {
    JPEG.get_or_init(|| Semaphore::new(cpus()))
}

pub fn sign_permits() -> &'static Semaphore {
    SIGN.get_or_init(|| Semaphore::new(cpus()))
}

pub fn small_sign_permits() -> &'static Semaphore {
    SIGN_SMALL.get_or_init(|| Semaphore::new(1))
}
