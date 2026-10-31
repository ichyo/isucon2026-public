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
//!   JPEG reserves one slot for profile/RealMoji images; posts use the remaining slots (at least one).

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::Semaphore;

pub const REPL_NICE: i32 = -10;
const REPL_THREADS: usize = 2;

static REPL: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static JPEG: OnceLock<JpegBudgets> = OnceLock::new();
static SIGN: OnceLock<Semaphore> = OnceLock::new();
// A short request must not queue behind every pending feed/export batch. This lane is bounded
// independently and only accepts one SIMD pass (or a handful of scalar signatures) at a time.
static SIGN_SMALL: OnceLock<Semaphore> = OnceLock::new();
static WARNED: AtomicBool = AtomicBool::new(false);
/// 0 = not tried, 1 = raised, 2 = failed
static PRIO: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

#[derive(Clone, Copy, Debug)]
pub enum JpegLane {
    Post,
    Small,
}

struct JpegBudgets {
    post: Arc<Semaphore>,
    small: Arc<Semaphore>,
}

impl JpegBudgets {
    fn new(total: usize) -> Self {
        // Independent queues guarantee a small upload can proceed even with many posts queued.
        // On a single-CPU host each lane still needs one slot to avoid starving either class.
        Self { post: Arc::new(Semaphore::new(total.saturating_sub(1).max(1))), small: Arc::new(Semaphore::new(1)) }
    }

    fn permits(&self, lane: JpegLane) -> &Arc<Semaphore> {
        match lane {
            JpegLane::Post => &self.post,
            JpegLane::Small => &self.small,
        }
    }
}

/// Whether the replication threads got their elevated priority ("raised" / "failed" / "n/a").
pub fn prio_state() -> &'static str {
    match PRIO.load(Ordering::Relaxed) {
        1 => "raised",
        2 => "failed",
        _ => "n/a",
    }
}

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
        if nice < 0 {
            PRIO.store(if ok { 1 } else { 2 }, Ordering::Relaxed);
        }
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
    let _ = JPEG.set(JpegBudgets::new(n(jpeg)));
    let _ = SIGN.set(Semaphore::new(n(sign)));
}

pub fn jpeg_permits(lane: JpegLane) -> &'static Arc<Semaphore> {
    JPEG.get_or_init(|| JpegBudgets::new(cpus())).permits(lane)
}

pub fn sign_permits() -> &'static Semaphore {
    SIGN.get_or_init(|| Semaphore::new(cpus()))
}

pub fn small_sign_permits() -> &'static Semaphore {
    SIGN_SMALL.get_or_init(|| Semaphore::new(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_jpeg_lane_remains_available_when_posts_are_full() {
        for total in [1, 2, 4, 16] {
            let budgets = JpegBudgets::new(total);
            let post = budgets.permits(JpegLane::Post);
            let _busy = post.try_acquire_many(total.saturating_sub(1).max(1) as u32).unwrap();
            assert!(post.try_acquire().is_err());
            let small = budgets.permits(JpegLane::Small);
            let _small = small.try_acquire().expect("small images must not wait behind posts");
            assert!(small.try_acquire().is_err(), "small images are also bounded");
        }
    }
}
