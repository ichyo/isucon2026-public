//! Cheap per-request metrics, aggregated in-process and exposed on the control server
//! (`GET /metrics`, `POST /metrics/reset`). The hot path is a handful of relaxed atomic adds — no locks.
//!
//! Per action (route action name, plus `static`, `not_found`, `method_not_allowed`, `options`, `banned`,
//! `too_large`, `not_in_sync`): count, status classes, per-code counters (top 3 reported), latency histogram
//! (quarter-octave log buckets ⇒ p50/p90/p99 within ~±10%), sum/max, request/response bytes, and for writes the
//! time spent in replication (leader: submit → all acked; follower: forward round trip), measured via a
//! task-local set around the handler. A 120 s ring keeps per-second request counts per action and process CPU%.

use std::cell::Cell;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// Non-route actions (ids 0..SPECIAL.len()); route actions follow in router order.
pub const SPECIAL: [&str; 8] = ["static", "not_found", "method_not_allowed", "options", "banned", "too_large", "not_in_sync", "health"];
pub const A_STATIC: usize = 0;
pub const A_NOT_FOUND: usize = 1;
pub const A_METHOD_NOT_ALLOWED: usize = 2;
pub const A_OPTIONS: usize = 3;
pub const A_BANNED: usize = 4;
pub const A_TOO_LARGE: usize = 5;
pub const A_NOT_IN_SYNC: usize = 6;
pub const A_HEALTH: usize = 7;

const NB: usize = 100;
const RING: usize = 120;
const CODES: [u16; 15] = [200, 201, 204, 304, 400, 401, 403, 404, 405, 409, 413, 415, 500, 501, 503];

tokio::task_local! {
    /// (number of replicated writes, µs spent waiting for replication) of the current request
    pub static REPL_WAIT: Cell<(u64, u64)>;
}

/// Adds replication wait time to the current request (no-op outside a request scope).
pub fn add_repl_wait(d: Duration) {
    let us = d.as_micros() as u64;
    let _ = REPL_WAIT.try_with(|c| {
        let (n, t) = c.get();
        c.set((n + 1, t + us));
    });
}

#[inline]
fn bucket(us: u64) -> usize {
    if us < 4 {
        return us as usize;
    }
    let lz = 63 - us.leading_zeros() as u64; // floor(log2)
    let frac = (us >> (lz - 2)) & 3;
    ((4 + (lz - 2) * 4 + frac) as usize).min(NB - 1)
}

/// Exclusive upper bound (µs) of a bucket.
fn bucket_upper(b: usize) -> u64 {
    if b < 4 {
        return b as u64 + 1;
    }
    let lz = (b as u64 - 4) / 4 + 2;
    let frac = (b as u64 - 4) % 4;
    (5 + frac) << (lz - 2)
}

struct Hist {
    b: [AtomicU64; NB],
    n: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Hist {
    fn new() -> Hist {
        Hist { b: std::array::from_fn(|_| AtomicU64::new(0)), n: AtomicU64::new(0), sum: AtomicU64::new(0), max: AtomicU64::new(0) }
    }
    #[inline]
    fn add(&self, us: u64) {
        self.b[bucket(us)].fetch_add(1, Relaxed);
        self.n.fetch_add(1, Relaxed);
        self.sum.fetch_add(us, Relaxed);
        self.max.fetch_max(us, Relaxed);
    }
    fn reset(&self) {
        for x in &self.b {
            x.store(0, Relaxed);
        }
        self.n.store(0, Relaxed);
        self.sum.store(0, Relaxed);
        self.max.store(0, Relaxed);
    }
    fn json(&self) -> Value {
        let counts: Vec<u64> = self.b.iter().map(|x| x.load(Relaxed)).collect();
        let n: u64 = counts.iter().sum();
        let max = self.max.load(Relaxed);
        let sum = self.sum.load(Relaxed);
        let pct = |p: f64| -> f64 {
            if n == 0 {
                return 0.0;
            }
            let rank = ((p * n as f64).ceil() as u64).max(1);
            let mut acc = 0;
            for (i, c) in counts.iter().enumerate() {
                acc += c;
                if acc >= rank {
                    return ms(bucket_upper(i).min(max));
                }
            }
            ms(max)
        };
        json!({
            "count": n,
            "sum_ms": ms(sum),
            "avg_ms": sum.checked_div(n).map_or(0.0, ms),
            "p50_ms": pct(0.50),
            "p90_ms": pct(0.90),
            "p99_ms": pct(0.99),
            "max_ms": ms(max),
        })
    }
}

fn ms(us: u64) -> f64 {
    (us as f64 / 10.0).round() / 100.0
}

struct ActionStats {
    count: AtomicU64,
    class: [AtomicU64; 5],
    codes: [AtomicU64; CODES.len() + 1],
    lat: Hist,
    repl: Hist,
    req_bytes: AtomicU64,
    resp_bytes: AtomicU64,
    /// requests whose body the handler left (partly) unread and the HTTP layer drained before responding
    drained: AtomicU64,
    drained_bytes: AtomicU64,
    /// requests answered with body data still unread (413 / ip_banned / drain failure) ⇒ connection closed
    early_close: AtomicU64,
}

impl ActionStats {
    fn new() -> ActionStats {
        ActionStats {
            count: AtomicU64::new(0),
            class: std::array::from_fn(|_| AtomicU64::new(0)),
            codes: std::array::from_fn(|_| AtomicU64::new(0)),
            lat: Hist::new(),
            repl: Hist::new(),
            req_bytes: AtomicU64::new(0),
            resp_bytes: AtomicU64::new(0),
            drained: AtomicU64::new(0),
            drained_bytes: AtomicU64::new(0),
            early_close: AtomicU64::new(0),
        }
    }
    fn reset(&self) {
        self.count.store(0, Relaxed);
        for x in self.class.iter().chain(self.codes.iter()) {
            x.store(0, Relaxed);
        }
        self.lat.reset();
        self.repl.reset();
        self.req_bytes.store(0, Relaxed);
        self.resp_bytes.store(0, Relaxed);
        self.drained.store(0, Relaxed);
        self.drained_bytes.store(0, Relaxed);
        self.early_close.store(0, Relaxed);
    }
}

struct Slot {
    sec: AtomicU64,
    total: AtomicU32,
    /// process CPU% × 10 during that second (u32::MAX = unknown)
    cpu_x10: AtomicU32,
    per: Vec<AtomicU32>,
}

pub struct Metrics {
    names: Vec<&'static str>,
    actions: Vec<ActionStats>,
    ring: Vec<Slot>,
    started: Instant,
    reset_at: AtomicU64,
    cpu_last: AtomicU64,
    wall_last: AtomicU64,
}

fn unix_s() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Process CPU time (user + system) in µs.
fn cpu_us() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: plain syscall filling a caller-owned struct.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return 0;
    }
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

impl Metrics {
    /// `routes` = route action names in router order (action id = SPECIAL.len() + route index).
    pub fn new(routes: &[&'static str]) -> Metrics {
        let names: Vec<&'static str> = SPECIAL.iter().copied().chain(routes.iter().copied()).collect();
        let n = names.len();
        Metrics {
            actions: (0..n).map(|_| ActionStats::new()).collect(),
            ring: (0..RING)
                .map(|_| Slot { sec: AtomicU64::new(0), total: AtomicU32::new(0), cpu_x10: AtomicU32::new(u32::MAX), per: (0..n).map(|_| AtomicU32::new(0)).collect() })
                .collect(),
            names,
            started: Instant::now(),
            reset_at: AtomicU64::new(unix_s()),
            cpu_last: AtomicU64::new(cpu_us()),
            wall_last: AtomicU64::new(0),
        }
    }

    pub fn route_action(route_idx: usize) -> usize {
        SPECIAL.len() + route_idx
    }

    fn slot(&self, sec: u64) -> &Slot {
        let s = &self.ring[(sec % RING as u64) as usize];
        let cur = s.sec.load(Relaxed);
        if cur < sec && s.sec.compare_exchange(cur, sec, Relaxed, Relaxed).is_ok() {
            s.total.store(0, Relaxed);
            s.cpu_x10.store(u32::MAX, Relaxed);
            for c in &s.per {
                c.store(0, Relaxed);
            }
        }
        s
    }

    /// Records one finished request. `repl` = (writes, µs waiting for replication).
    #[inline]
    pub fn record(&self, action: usize, status: u16, lat: Duration, repl: (u64, u64), req_bytes: u64, resp_bytes: u64) {
        let Some(a) = self.actions.get(action) else { return };
        a.count.fetch_add(1, Relaxed);
        a.class[((status / 100).clamp(1, 5) - 1) as usize].fetch_add(1, Relaxed);
        let ci = CODES.iter().position(|&c| c == status).unwrap_or(CODES.len());
        a.codes[ci].fetch_add(1, Relaxed);
        a.lat.add(lat.as_micros() as u64);
        if repl.0 > 0 {
            a.repl.add(repl.1);
        }
        a.req_bytes.fetch_add(req_bytes, Relaxed);
        a.resp_bytes.fetch_add(resp_bytes, Relaxed);
        let s = self.slot(unix_s());
        s.total.fetch_add(1, Relaxed);
        s.per[action].fetch_add(1, Relaxed);
    }

    /// Records what happened to the unread rest of a request body.
    #[inline]
    pub fn record_drain(&self, action: usize, d: crate::http::req::Drain) {
        let Some(a) = self.actions.get(action) else { return };
        match d {
            crate::http::req::Drain::None => {}
            crate::http::req::Drain::Drained(n) => {
                a.drained.fetch_add(1, Relaxed);
                a.drained_bytes.fetch_add(n, Relaxed);
            }
            crate::http::req::Drain::Failed => {
                a.early_close.fetch_add(1, Relaxed);
            }
        }
    }

    /// Once per second (background task): process CPU% of the second that just ended.
    pub fn tick(&self) {
        let now_wall = self.started.elapsed().as_micros() as u64;
        let now_cpu = cpu_us();
        let dw = now_wall.saturating_sub(self.wall_last.swap(now_wall, Relaxed));
        let dc = now_cpu.saturating_sub(self.cpu_last.swap(now_cpu, Relaxed));
        if dw == 0 {
            return;
        }
        let pct_x10 = (dc as f64 * 1000.0 / dw as f64).round() as u32;
        let sec = unix_s().saturating_sub(1);
        self.slot(sec).cpu_x10.store(pct_x10, Relaxed);
    }

    pub fn reset(&self) {
        for a in &self.actions {
            a.reset();
        }
        self.reset_at.store(unix_s(), Relaxed);
    }

    pub fn json(&self, node: &str, role: &str) -> String {
        self.json_with(node, role, Value::Null)
    }

    /// `extra` is reported as `"replication"` (leader: per-follower lag / ack latency; follower: direct-ack stats).
    pub fn json_with(&self, node: &str, role: &str, extra: Value) -> String {
        let now = unix_s();
        let mut acts: Vec<(u64, Value)> = Vec::new();
        let (mut total_n, mut total_us) = (0u64, 0u64);
        for (i, a) in self.actions.iter().enumerate() {
            let n = a.count.load(Relaxed);
            if n == 0 {
                continue;
            }
            let lat_sum = a.lat.sum.load(Relaxed);
            let repl_sum = a.repl.sum.load(Relaxed);
            total_n += n;
            total_us += lat_sum;
            let mut codes: Vec<(u64, Value)> = a
                .codes
                .iter()
                .enumerate()
                .map(|(j, c)| (c.load(Relaxed), if j < CODES.len() { json!(CODES[j]) } else { json!("other") }))
                .filter(|(c, _)| *c > 0)
                .collect();
            codes.sort_by_key(|x| std::cmp::Reverse(x.0));
            codes.truncate(3);
            let cls: Vec<u64> = a.class.iter().map(|x| x.load(Relaxed)).collect();
            let mut v = json!({
                "action": self.names[i],
                "count": n,
                "2xx": cls[1],
                "3xx": cls[2],
                "4xx": cls[3],
                "5xx": cls[4],
                "top_codes": codes.into_iter().map(|(c, code)| json!([code, c])).collect::<Vec<_>>(),
                "latency": a.lat.json(),
                "handler_ms_sum": ms(lat_sum.saturating_sub(repl_sum)),
                "req_bytes": a.req_bytes.load(Relaxed),
                "resp_bytes": a.resp_bytes.load(Relaxed),
                "body_drained": a.drained.load(Relaxed),
                "body_drained_bytes": a.drained_bytes.load(Relaxed),
                "early_close": a.early_close.load(Relaxed),
            });
            if a.repl.n.load(Relaxed) > 0 {
                v["repl_wait"] = a.repl.json();
            }
            acts.push((lat_sum, v));
        }
        acts.sort_by_key(|x| std::cmp::Reverse(x.0));

        // timeline: last RING-1 complete seconds (+ current), top-10 actions by count in the window
        let mut secs: Vec<&Slot> = self.ring.iter().filter(|s| {
            let t = s.sec.load(Relaxed);
            t > 0 && t + RING as u64 > now && t <= now
        }).collect();
        secs.sort_by_key(|s| s.sec.load(Relaxed));
        let mut per_action: Vec<(u64, usize)> = (0..self.names.len()).map(|i| (secs.iter().map(|s| s.per[i].load(Relaxed) as u64).sum(), i)).filter(|x| x.0 > 0).collect();
        per_action.sort_by_key(|x| std::cmp::Reverse(x.0));
        per_action.truncate(10);
        let top: Vec<usize> = per_action.iter().map(|x| x.1).collect();
        let seconds: Vec<Value> = secs
            .iter()
            .map(|s| {
                let cpu = s.cpu_x10.load(Relaxed);
                json!({
                    "t": s.sec.load(Relaxed),
                    "rps": s.total.load(Relaxed),
                    "cpu_pct": if cpu == u32::MAX { Value::Null } else { json!(cpu as f64 / 10.0) },
                    "top": top.iter().map(|&i| s.per[i].load(Relaxed)).collect::<Vec<_>>(),
                })
            })
            .collect();
        json!({
            "node": node,
            "role": role,
            "now": now,
            "uptime_s": self.started.elapsed().as_secs(),
            "since_reset_s": now.saturating_sub(self.reset_at.load(Relaxed)),
            "total": {"count": total_n, "latency_sum_ms": ms(total_us)},
            "replication": extra,
            "actions": acts.into_iter().map(|x| x.1).collect::<Vec<_>>(),
            "timeline": {
                "actions": top.iter().map(|&i| self.names[i]).collect::<Vec<_>>(),
                "seconds": seconds,
            },
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_tight() {
        let mut prev = 0;
        for us in [0u64, 1, 3, 4, 5, 7, 8, 9, 15, 16, 100, 1000, 12_345, 1_000_000, 60_000_000, u64::MAX / 2] {
            let b = bucket(us);
            assert!(b >= prev, "{us}");
            prev = b;
            if b < NB - 1 {
                assert!(us < bucket_upper(b), "{us} < upper({b})={}", bucket_upper(b));
                assert!(b == 0 || us >= bucket_upper(b - 1), "{us} >= upper({})", b - 1);
                assert!(bucket_upper(b) as f64 <= (us.max(1) as f64) * 1.26 + 1.0);
            }
        }
    }

    #[test]
    fn record_and_report() {
        let m = Metrics::new(&["get_feed", "create_post"]);
        let a = Metrics::route_action(0);
        for i in 1..=100u64 {
            m.record(a, 200, Duration::from_micros(i * 1000), (0, 0), 0, 10);
        }
        m.record(a, 403, Duration::from_millis(1), (0, 0), 0, 5);
        m.record(Metrics::route_action(1), 201, Duration::from_millis(120), (1, 100_000), 500_000, 300);
        m.record(A_STATIC, 200, Duration::from_micros(30), (0, 0), 0, 400);
        m.tick();
        let v: Value = serde_json::from_str(&m.json("asia", "leader")).unwrap();
        let acts = v["actions"].as_array().unwrap();
        let feed = acts.iter().find(|x| x["action"] == "get_feed").unwrap();
        assert_eq!(feed["count"], 101);
        assert_eq!(feed["4xx"], 1);
        assert_eq!(feed["top_codes"][0], json!([200, 100]));
        let p50 = feed["latency"]["p50_ms"].as_f64().unwrap();
        assert!((45.0..=60.0).contains(&p50), "{p50}");
        assert_eq!(feed["latency"]["max_ms"], 100.0);
        let post = acts.iter().find(|x| x["action"] == "create_post").unwrap();
        assert_eq!(post["repl_wait"]["count"], 1);
        assert_eq!(post["handler_ms_sum"], 20.0);
        assert_eq!(acts[0]["action"], "get_feed", "sorted by latency sum");
        assert_eq!(v["timeline"]["actions"][0], "get_feed");
        assert!(v["timeline"]["seconds"].as_array().unwrap().iter().map(|s| s["rps"].as_u64().unwrap()).sum::<u64>() >= 103);
        m.reset();
        let v: Value = serde_json::from_str(&m.json("asia", "leader")).unwrap();
        assert!(v["actions"].as_array().unwrap().is_empty());
    }
}
