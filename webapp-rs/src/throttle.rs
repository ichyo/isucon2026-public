//! Artificial response delay (contest strategy; default off) + the local control server
//! (`ISUREAL_CONTROL_LISTEN`, 127.0.0.1:9100): throttle, status, debug session.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

use crate::app::{App, Ctx};
use crate::command::{Command, Outcome};
use crate::json::JsonOut;
use crate::model::Id;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThrottleKind {
    None,
    Normal,
    Feed,
}

/// Which delay class a request path belongs to (§9 of the design doc).
pub fn kind_of(path: &str) -> ThrottleKind {
    if !path.starts_with("/api/") || path == "/api/initialize" || path == "/api/notifications" || path.starts_with("/api/admin/") {
        ThrottleKind::None
    } else if path.starts_with("/api/feeds/") {
        ThrottleKind::Feed
    } else {
        ThrottleKind::Normal
    }
}

pub struct Throttle {
    ms: AtomicU64,
    feed_ms: AtomicU64,
}

impl Throttle {
    pub fn new(ms: u64, feed_ms: u64) -> Throttle {
        Throttle { ms: AtomicU64::new(ms), feed_ms: AtomicU64::new(feed_ms) }
    }
    pub fn set(&self, ms: u64, feed_ms: u64) {
        self.ms.store(ms, Ordering::Relaxed);
        self.feed_ms.store(feed_ms, Ordering::Relaxed);
    }
    pub fn get(&self) -> (u64, u64) {
        (self.ms.load(Ordering::Relaxed), self.feed_ms.load(Ordering::Relaxed))
    }
    /// Delay for this request class; `None` when zero (no timer created at all).
    #[inline]
    pub fn delay(&self, k: ThrottleKind) -> Option<Duration> {
        let ms = match k {
            ThrottleKind::None => 0,
            ThrottleKind::Normal => self.ms.load(Ordering::Relaxed),
            ThrottleKind::Feed => self.ms.load(Ordering::Relaxed) + self.feed_ms.load(Ordering::Relaxed),
        };
        (ms > 0).then(|| Duration::from_millis(ms))
    }
}

fn q<'a>(query: &'a str, key: &str) -> Option<std::borrow::Cow<'a, str>> {
    form_urlencoded::parse(query.as_bytes()).find(|(k, _)| k == key).map(|(_, v)| v)
}

fn text(status: u16, body: String) -> Response<Full<Bytes>> {
    Response::builder().status(status).header("content-type", "application/json").body(Full::new(Bytes::from(body + "\n"))).unwrap()
}

async fn control(app: Ctx, req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let method = req.method().clone();
    let _ = req.into_body().collect().await;
    let resp = match (method.as_str(), path.as_str()) {
        ("GET", "/throttle") => {
            let (ms, f) = app.throttle.get();
            text(200, format!("{{\"ms\":{ms},\"feed_ms\":{f}}}"))
        }
        ("POST", "/throttle") => {
            let (mut ms, mut f) = app.throttle.get();
            if let Some(v) = q(&query, "ms") {
                match v.parse() {
                    Ok(v) => ms = v,
                    Err(_) => return Ok(text(400, "{\"error\":\"bad ms\"}".into())),
                }
            }
            if let Some(v) = q(&query, "feed_ms") {
                match v.parse() {
                    Ok(v) => f = v,
                    Err(_) => return Ok(text(400, "{\"error\":\"bad feed_ms\"}".into())),
                }
            }
            app.throttle.set(ms, f);
            eprintln!("[throttle] set ms={ms} feed_ms={f}");
            text(200, format!("{{\"ms\":{ms},\"feed_ms\":{f}}}"))
        }
        ("GET", "/status") => text(200, app.status_json()),
        ("GET", "/metrics") => {
            let (role, repl) = match &app.repl {
                crate::repl::Repl::Leader(l) => ("leader", l.metrics_json()),
                crate::repl::Repl::Follower(f) => ("follower", serde_json::from_str(&f.status_json()).unwrap_or_default()),
            };
            let mut repl = repl;
            repl["ack_mode"] = serde_json::json!(format!("{:?}", app.cfg.ack_mode).to_lowercase());
            repl["repl_priority"] = serde_json::json!(crate::rt::prio_state());
            repl["gate_silence_ms"] = serde_json::json!(app.cfg.gate_silence_ms);
            repl["async_actions"] = serde_json::json!(app.cfg.async_actions);
            text(200, app.metrics.json_with(app.cfg.node.name(), role, repl))
        }
        ("POST", "/metrics/reset") => {
            app.metrics.reset();
            if let crate::repl::Repl::Leader(l) = &app.repl {
                l.reset_stats();
            }
            text(200, "{\"reset\":true}".into())
        }
        ("POST", "/debug/session") => {
            let Some(user) = q(&query, "user").and_then(|u| Id::parse(&u)) else {
                return Ok(text(400, "{\"error\":\"user=<uuid> required\"}".into()));
            };
            match app.write(Command::DebugSession { user }).await {
                Ok(Outcome::Login { token, .. }) => {
                    let st = app.read();
                    let Some(u) = st.user_idx(user) else { return Ok(text(500, "{}".into())) };
                    let mut w = JsonOut::new(0, &app.signer);
                    w.obj().k("token").id(token).k("user");
                    crate::views::session_user(&mut w, st.user(u));
                    w.end();
                    drop(st);
                    let mut b = w.finish();
                    b.pop();
                    text(200, String::from_utf8(b).unwrap())
                }
                Ok(_) => text(500, "{}".into()),
                Err(e) => text(e.status, String::from_utf8_lossy(&e.body()).trim_end().to_string()),
            }
        }
        _ => text(404, "{\"error\":\"Not Found\"}".into()),
    };
    Ok(resp)
}

/// Runs the control server (127.0.0.1 only by default).
pub async fn serve_control(app: &'static App, listener: tokio::net::TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req| control(app, req));
            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), svc).await;
        });
    }
}
