//! Request wrapper and binding rules (§2.5).

use std::borrow::Cow;
use std::time::Instant;

use bytes::Bytes;
use http::{HeaderMap, Method};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use serde_json::{Map, Value};
use smallvec::SmallVec;

use super::resp::ApiError;
use crate::model::Ts;
use crate::timefmt;

pub const MAX_BODY: usize = 32 * 1024 * 1024;

pub type BodySlot = std::sync::Arc<parking_lot::Mutex<Option<Incoming>>>;

// ---- bounded body buffering ------------------------------------------------------------------------------

/// Global budget for buffered request bodies (KiB permits; ISUREAL_BODY_BUDGET_MB, default 600). A request
/// reserves min(Content-Length, its cap) before reading and waits when the budget is exhausted.
static BUDGET: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
static BUDGET_KIB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(600 * 1024);
/// bytes currently reserved by buffered bodies / peak / requests that had to wait
pub static BODY_BYTES_IN_USE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static BODY_BYTES_PEAK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static BODY_BUDGET_WAITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// uploads rejected without buffering (over their cap / not a JPEG prefix)
pub static BODY_REJECTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn init_body_budget(mb: u64) {
    let kib = (mb.max(64) * 1024).min(u32::MAX as u64 / 2);
    BUDGET_KIB.store(kib, std::sync::atomic::Ordering::Relaxed);
    let _ = BUDGET.set(std::sync::Arc::new(tokio::sync::Semaphore::new(kib as usize)));
}

fn budget() -> &'static std::sync::Arc<tokio::sync::Semaphore> {
    BUDGET.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(BUDGET_KIB.load(std::sync::atomic::Ordering::Relaxed) as usize)))
}

pub fn budget_kib() -> u64 {
    BUDGET_KIB.load(std::sync::atomic::Ordering::Relaxed)
}

/// A reservation in the body budget (released on drop, i.e. when the request finishes).
pub struct BudgetGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
    bytes: u64,
}
impl Drop for BudgetGuard {
    fn drop(&mut self) {
        BODY_BYTES_IN_USE.fetch_sub(self.bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn reserve(bytes: u64) -> Option<BudgetGuard> {
    use std::sync::atomic::Ordering::Relaxed;
    let kib = (bytes.div_ceil(1024)).clamp(1, budget_kib()) as u32;
    let sem = budget().clone();
    let permit = match sem.clone().try_acquire_many_owned(kib) {
        Ok(p) => p,
        Err(_) => {
            BODY_BUDGET_WAITS.fetch_add(1, Relaxed);
            sem.acquire_many_owned(kib).await.ok()?
        }
    };
    let now = BODY_BYTES_IN_USE.fetch_add(bytes, Relaxed) + bytes;
    BODY_BYTES_PEAK.fetch_max(now, Relaxed);
    Some(BudgetGuard { _permit: permit, bytes })
}

// ---- upload gate (FIFO admission of large bodies) ---------------------------------------------------------

/// At most `slots` bodies > SMALL_DRAIN are received at once, admitted in arrival order (tokio's semaphore is
/// fair); the others wait unread so their (small, ISUREAL_RCVBUF) receive windows close and the router's
/// per-region bandwidth goes to the admitted ones: uploads complete one after another instead of all at the end.
/// 0 = off (ISUREAL_UPLOAD_SLOTS; runtime: POST control /uploadgate?slots=N).
static GATE: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
static GATE_SLOTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static GATE_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
pub static GATE_WAITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static GATE_WAIT_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static GATE_PASSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn gate() -> &'static std::sync::Arc<tokio::sync::Semaphore> {
    GATE.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(0)))
}

pub fn gate_slots() -> usize {
    GATE_SLOTS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Resizes the gate (shrinking takes effect as admitted uploads finish).
pub fn set_gate_slots(n: usize) {
    use std::sync::atomic::Ordering::Relaxed;
    let _l = GATE_LOCK.lock();
    let cur = GATE_SLOTS.load(Relaxed);
    if n > cur {
        gate().add_permits(n - cur);
    } else if n < cur {
        let sem = gate().clone();
        let k = (cur - n) as u32;
        tokio::spawn(async move {
            if let Ok(p) = sem.acquire_many(k).await {
                p.forget();
            }
        });
    }
    GATE_SLOTS.store(n, Relaxed);
}

async fn gate_enter() -> Option<tokio::sync::OwnedSemaphorePermit> {
    use std::sync::atomic::Ordering::Relaxed;
    if gate_slots() == 0 {
        return None;
    }
    GATE_PASSES.fetch_add(1, Relaxed);
    let sem = gate().clone();
    match sem.clone().try_acquire_owned() {
        Ok(p) => Some(p),
        Err(_) => {
            let t = std::time::Instant::now();
            GATE_WAITS.fetch_add(1, Relaxed);
            let p = sem.acquire_owned().await.ok();
            GATE_WAIT_US.fetch_add(t.elapsed().as_micros() as u64, Relaxed);
            p
        }
    }
}

/// Result of a capped body read.
pub enum Capped {
    Body(Bytes),
    /// over the cap, or not starting with the required prefix: the rest was read and discarded unstored
    Rejected,
}

/// Without any body data for this long a drain gives up (the connection is then closed).
pub const DRAIN_IDLE: std::time::Duration = std::time::Duration::from_secs(120);

/// Outcome of draining the unread rest of a request body before responding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Drain {
    /// nothing left to read
    None,
    /// read and discarded this many bytes (connection stays usable)
    Drained(u64),
    /// could not consume it (over 32 MiB, client error, idle timeout): hyper will close the connection
    Failed,
}

/// Unread rests up to this size are drained before an early response (keep-alive); larger ones are not received.
pub const SMALL_DRAIN: usize = 64 * 1024;

/// Whether request body data may still be unread.
pub fn body_pending(slot: &BodySlot) -> bool {
    use http_body::Body as _;
    slot.lock().as_ref().is_some_and(|b| !b.is_end_stream())
}

/// Reads and discards the rest of a request body (bounded by MAX_BODY, per-chunk idle timeout).
pub async fn drain(slot: &BodySlot) -> Drain {
    use http_body::Body as _;
    let Some(mut b) = slot.lock().take() else { return Drain::None };
    if b.is_end_stream() {
        return Drain::None;
    }
    let mut n: u64 = 0;
    loop {
        match tokio::time::timeout(DRAIN_IDLE, b.frame()).await {
            Ok(Some(Ok(f))) => {
                if let Some(d) = f.data_ref() {
                    n += d.len() as u64;
                    if n > MAX_BODY as u64 {
                        return Drain::Failed;
                    }
                }
            }
            Ok(None) => return if n == 0 { Drain::None } else { Drain::Drained(n) },
            Ok(Some(Err(_))) | Err(_) => return Drain::Failed,
        }
    }
}

pub struct Req {
    pub method: Method,
    pub path: String,
    pub(crate) params: SmallVec<[String; 2]>,
    pub(crate) query: Option<String>,
    pub headers: HeaderMap,
    /// shared with the HTTP layer, which drains whatever the handler did not read before responding
    pub(crate) body: BodySlot,
    pub(crate) body_bytes: Option<Bytes>,
    /// client IP (X-Client-IP verbatim, fallback TCP peer)
    pub ip: String,
    pub start: Instant,
    /// body-budget reservation held until the request finishes
    pub(crate) budget: Option<BudgetGuard>,
}

impl Req {
    /// Path parameter `i` in pattern order (percent-decoded; may be empty).
    pub fn param(&self, i: usize) -> &str {
        self.params.get(i).map(|s| s.as_str()).unwrap_or("")
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    pub fn raw_query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// First value of query param `key` (exact name, then ASCII case-insensitive).
    pub fn query(&self, key: &str) -> Option<Cow<'_, str>> {
        let q = self.query.as_deref()?;
        let mut ci = None;
        for (k, v) in form_urlencoded::parse(q.as_bytes()) {
            if k == key {
                return Some(v);
            }
            if ci.is_none() && k.eq_ignore_ascii_case(key) {
                ci = Some(v);
            }
        }
        ci
    }

    /// Optional string query param; empty value counts as absent (id cursors).
    pub fn query_nonempty(&self, key: &str) -> Option<String> {
        self.query(key).filter(|v| !v.is_empty()).map(|v| v.into_owned())
    }

    /// Optional integer query param: empty ⇒ Some(0) (Echo semantics), unparsable ⇒ 400 bind error.
    pub fn query_int(&self, key: &str) -> Result<Option<i64>, ApiError> {
        match self.query(key) {
            None => Ok(None),
            Some(v) if v.is_empty() => Ok(Some(0)),
            Some(v) => v.parse::<i64>().map(Some).map_err(|e| ApiError::bind(format!("{key}: {e}"))),
        }
    }

    /// Optional RFC3339 query param (µs): empty or unparsable ⇒ 400 bind error.
    pub fn query_time(&self, key: &str) -> Result<Option<Ts>, ApiError> {
        match self.query(key) {
            None => Ok(None),
            Some(v) => timefmt::parse_rfc3339(&v).map(Some).ok_or_else(|| ApiError::bind(format!("{key}: invalid time {v:?}"))),
        }
    }

    /// Reads the whole body (≤ 32 MiB, else 413). Cached: may be called more than once.
    pub async fn body_bytes(&mut self) -> Result<Bytes, ApiError> {
        match self.body_capped(MAX_BODY, None).await? {
            Capped::Body(b) => Ok(b),
            Capped::Rejected => Err(ApiError::new(413, "Request Entity Too Large")),
        }
    }

    /// Reads the body as it arrives, buffering at most `cap` bytes (reserved in the global body budget first).
    /// Content-Length over `cap` ⇒ `Capped::Rejected` without receiving anything; over `cap` mid-stream or first
    /// bytes not matching `prefix` ⇒ stop receiving, `Capped::Rejected` (the rest stays unread; the HTTP layer
    /// answers with `Connection: close`). > 32 MiB ⇒ 413.
    pub async fn body_capped(&mut self, cap: usize, prefix: Option<&[u8]>) -> Result<Capped, ApiError> {
        use http_body::Body as _;
        if let Some(b) = &self.body_bytes {
            return Ok(Capped::Body(b.clone()));
        }
        let Some(mut body) = self.body.lock().take() else { return Ok(Capped::Body(Bytes::new())) };
        if body.is_end_stream() {
            let b = Bytes::new();
            self.body_bytes = Some(b.clone());
            return Ok(match prefix {
                Some(p) if !p.is_empty() => Capped::Rejected,
                _ => Capped::Body(b),
            });
        }
        let declared = self.header("content-length").and_then(|v| v.parse::<u64>().ok());
        if declared.is_some_and(|n| n > cap as u64) {
            // decided from the header: nothing is received (the HTTP layer answers with Connection: close)
            *self.body.lock() = Some(body);
            BODY_REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(Capped::Rejected);
        }
        let want = declared.map_or(cap as u64, |n| n.min(cap as u64));
        // held until the body is fully received (or rejected), not until the response
        let _gate = if want > SMALL_DRAIN as u64 { gate_enter().await } else { None };
        if self.budget.is_none() {
            self.budget = reserve(want).await;
        }
        // with a prefix check (profile image) start small: abusive bodies are usually rejected by their first bytes
        let initial = if prefix.is_some() { want.min(64 * 1024) } else { want };
        let mut buf = bytes::BytesMut::with_capacity(initial as usize);
        let mut total: usize = 0;
        let mut rejected = false;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| ApiError::bind(format!("read body: {e}")))?;
            let Ok(data) = frame.into_data() else { continue };
            total += data.len();
            if total > MAX_BODY {
                *self.body.lock() = Some(body);
                return Err(ApiError::new(413, "Request Entity Too Large"));
            }
            if buf.len() + data.len() > cap {
                rejected = true;
                break;
            }
            buf.extend_from_slice(&data);
            if let Some(p) = prefix {
                let n = buf.len().min(p.len());
                if buf[..n] != p[..n] {
                    rejected = true;
                    break;
                }
            }
        }
        if rejected {
            // stop receiving: the rest stays unread (the HTTP layer answers with Connection: close + lingering close)
            *self.body.lock() = Some(body);
        }
        if let Some(p) = prefix {
            if !rejected && buf.len() < p.len() {
                rejected = true;
            }
        }
        crate::metrics::set_body_recv(self.start.elapsed());
        if rejected {
            BODY_REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(Capped::Rejected);
        }
        let b = buf.freeze();
        self.body_bytes = Some(b.clone());
        Ok(Capped::Body(b))
    }

    /// `bind` with the whole body capped at `cap` bytes: `Ok(None)` when it is larger (nothing buffered beyond
    /// the cap; the caller answers with the error its own validation would give, e.g. 400 invalid_post).
    pub async fn bind_capped(&mut self, cap: usize) -> Result<Option<Body>, ApiError> {
        match self.body_capped(cap, None).await? {
            Capped::Rejected => Ok(None),
            Capped::Body(_) => self.bind().await.map(Some),
        }
    }

    /// Binds the body per §2.5: empty ⇒ `Body::Empty`; `application/json` ⇒ object; form-urlencoded /
    /// multipart ⇒ fields (+ URL query appended after body values); anything else ⇒ 415.
    pub async fn bind(&mut self) -> Result<Body, ApiError> {
        let bytes = self.body_bytes().await?;
        if bytes.is_empty() {
            return Ok(Body::Empty);
        }
        let ct = self.header("content-type").unwrap_or("").to_string();
        if ct.starts_with("application/json") {
            let mut it = serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>();
            return match it.next() {
                Some(Ok(Value::Object(m))) => Ok(Body::Json(m)),
                Some(Ok(Value::Null)) => Ok(Body::Json(Map::new())),
                Some(Ok(_)) => Err(ApiError::bind("json: body must be an object")),
                Some(Err(e)) => Err(ApiError::bind(format!("json: {e}"))),
                None => Ok(Body::Json(Map::new())),
            };
        }
        let mut fields: Vec<(String, String)> = Vec::new();
        let mut files: Vec<FilePart> = Vec::new();
        if ct.starts_with("application/x-www-form-urlencoded") {
            for (k, v) in form_urlencoded::parse(&bytes) {
                fields.push((k.into_owned(), v.into_owned()));
            }
        } else if ct.starts_with("multipart/form-data") {
            let boundary = multer::parse_boundary(&ct).map_err(|e| ApiError::bind(format!("multipart: {e}")))?;
            let stream = futures_util::stream::once(async move { Ok::<Bytes, std::convert::Infallible>(bytes) });
            let mut mp = multer::Multipart::new(stream, boundary);
            loop {
                let field = match mp.next_field().await {
                    Ok(Some(f)) => f,
                    Ok(None) => break,
                    Err(e) => return Err(ApiError::bind(format!("multipart: {e}"))),
                };
                let name = field.name().unwrap_or("").to_string();
                let file_name = field.file_name().map(|s| s.to_string());
                let content_type = field.content_type().map(|m| m.to_string());
                let data = field.bytes().await.map_err(|e| ApiError::bind(format!("multipart: {e}")))?;
                if name.is_empty() {
                    continue;
                }
                match file_name {
                    Some(f) if !f.is_empty() => files.push(FilePart { name, file_name: f, content_type, data }),
                    _ => {
                        let v = String::from_utf8(data.to_vec()).map_err(|_| ApiError::bind("multipart: invalid utf-8 value"))?;
                        fields.push((name, v));
                    }
                }
            }
        } else {
            return Err(ApiError::unsupported_media_type());
        }
        if let Some(q) = &self.query {
            for (k, v) in form_urlencoded::parse(q.as_bytes()) {
                fields.push((k.into_owned(), v.into_owned()));
            }
        }
        Ok(Body::Form { fields, files })
    }
}

pub struct FilePart {
    pub name: String,
    pub file_name: String,
    pub content_type: Option<String>,
    pub data: Bytes,
}

/// A bound request body.
pub enum Body {
    Empty,
    Json(Map<String, Value>),
    Form { fields: Vec<(String, String)>, files: Vec<FilePart> },
}

impl Body {
    fn json_get<'a>(m: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
        if let Some(v) = m.get(key) {
            return Some(v);
        }
        m.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v)
    }
    fn form_get<'a>(fields: &'a [(String, String)], key: &str) -> Option<&'a str> {
        fields.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    /// Field present (JSON: non-null; form: exists, even if empty).
    pub fn has(&self, key: &str) -> bool {
        match self {
            Body::Empty => false,
            Body::Json(m) => Body::json_get(m, key).is_some_and(|v| !v.is_null()),
            Body::Form { fields, .. } => Body::form_get(fields, key).is_some(),
        }
    }

    /// String field: absent/null ⇒ None; wrong JSON type ⇒ 400 bind error.
    pub fn str(&self, key: &str) -> Result<Option<String>, ApiError> {
        match self {
            Body::Empty => Ok(None),
            Body::Json(m) => match Body::json_get(m, key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(s)) => Ok(Some(s.clone())),
                Some(_) => Err(ApiError::bind(format!("{key}: expected string"))),
            },
            Body::Form { fields, .. } => Ok(Body::form_get(fields, key).map(|s| s.to_string())),
        }
    }

    /// Integer field: JSON integer (no fraction/exponent) or form value (empty ⇒ 0).
    pub fn int(&self, key: &str) -> Result<Option<i64>, ApiError> {
        match self {
            Body::Empty => Ok(None),
            Body::Json(m) => match Body::json_get(m, key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Number(n)) => n.as_i64().map(Some).ok_or_else(|| ApiError::bind(format!("{key}: expected int"))),
                Some(_) => Err(ApiError::bind(format!("{key}: expected int"))),
            },
            Body::Form { fields, .. } => match Body::form_get(fields, key) {
                None => Ok(None),
                Some("") => Ok(Some(0)),
                Some(v) => v.parse().map(Some).map_err(|_| ApiError::bind(format!("{key}: expected int"))),
            },
        }
    }

    /// Float field: JSON number or form value (empty ⇒ 0.0; out of range ⇒ bind error).
    pub fn float(&self, key: &str) -> Result<Option<f64>, ApiError> {
        match self {
            Body::Empty => Ok(None),
            Body::Json(m) => match Body::json_get(m, key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Number(n)) => n.as_f64().map(Some).ok_or_else(|| ApiError::bind(format!("{key}: expected number"))),
                Some(_) => Err(ApiError::bind(format!("{key}: expected number"))),
            },
            Body::Form { fields, .. } => match Body::form_get(fields, key) {
                None => Ok(None),
                Some("") => Ok(Some(0.0)),
                Some(v) => match v.parse::<f64>() {
                    Ok(f) if f.is_infinite() && !v.to_ascii_lowercase().contains("inf") => Err(ApiError::bind(format!("{key}: out of range"))),
                    Ok(f) => Ok(Some(f)),
                    Err(_) => Err(ApiError::bind(format!("{key}: expected number"))),
                },
            },
        }
    }

    /// First file part with this field name (multipart parts with a non-empty filename).
    pub fn file(&self, name: &str) -> Option<&FilePart> {
        match self {
            Body::Form { files, .. } => files.iter().find(|f| f.name == name),
            _ => None,
        }
    }
}
