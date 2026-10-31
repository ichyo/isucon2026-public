//! HTTP server (hyper 1, HTTP/1.1 keep-alive) and per-request pipeline (docs/design.md §11).

pub mod req;
pub mod resp;
pub mod router;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Instant;

use http::{HeaderValue, Method, Request, Response};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

pub use req::{Body, FilePart, Req, MAX_BODY};
pub use resp::{Abuse, ApiError, HBody, HResult, Resp, RespBody};
use router::Match;

use crate::app::App;
use crate::model::now_us;
use crate::throttle;

pub async fn serve(app: &'static App, listener: TcpListener) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                eprintln!("[http] accept error: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req| handle(app, req, peer));
            let _ = hyper::server::conn::http1::Builder::new().keep_alive(true).serve_connection(TokioIo::new(stream), svc).await;
        });
    }
}

fn client_ip(req: &Request<Incoming>, peer: SocketAddr) -> String {
    match req.headers().get("x-client-ip").and_then(|v| v.to_str().ok()) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => peer.ip().to_string(),
    }
}

async fn handle(app: &'static App, req: Request<Incoming>, peer: SocketAddr) -> Result<Response<HBody>, Infallible> {
    let start = Instant::now();
    let ip = client_ip(&req, peer);
    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();
    let kind = throttle::kind_of(&path);
    let mut action = "-";

    let too_large = parts
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > MAX_BODY as u64);

    let resp: Resp = if too_large {
        ApiError::new(413, "Request Entity Too Large").into_resp()
    } else if app.bans.is_banned(&ip, now_us()) {
        ApiError::forbidden("ip_banned").into_resp()
    } else if !path.starts_with("/api/") {
        if parts.method == Method::GET || parts.method == Method::HEAD {
            match app.statics.get(&path) {
                Some((b, ct)) => Resp::bytes(200, ct, b),
                None => ApiError::not_found("Not Found").into_resp(),
            }
        } else {
            ApiError::new(405, "Method Not Allowed").into_resp()
        }
    } else {
        match app.router.find(&parts.method, &path) {
            Match::NotFound => ApiError::not_found("Not Found").into_resp(),
            Match::MethodNotAllowed(allow) => ApiError::new(405, "Method Not Allowed").into_resp().header("allow", &allow),
            Match::Options(allow) => Resp::no_content().header("allow", &allow),
            Match::Found(route, params) => {
                action = route.name;
                let r = Req {
                    method: parts.method.clone(),
                    path: path.clone(),
                    params,
                    query: parts.uri.query().map(|s| s.to_string()),
                    headers: parts.headers,
                    body: Some(body),
                    body_bytes: None,
                    ip: ip.clone(),
                    start,
                };
                match (route.handler)(app, r).await {
                    Ok(r) => r,
                    Err(e) => {
                        if let Some(a) = e.abuse {
                            app.on_abuse(&ip, a);
                        }
                        if e.status >= 500 && e.status != 503 {
                            eprintln!("[http] {} {} -> {} {}", parts.method, path, e.status, e.code);
                        }
                        e.into_resp()
                    }
                }
            }
        }
    };

    if let Some(d) = app.throttle.delay(kind) {
        tokio::time::sleep(d).await;
    }
    if app.cfg.log_debug {
        eprintln!("[req] {} {} {} action={} ip={} {:.1?}", parts.method, path, resp.status, action, ip, start.elapsed());
    }
    Ok(to_hyper(resp))
}

pub fn to_hyper(resp: Resp) -> Response<HBody> {
    let body = match resp.body {
        RespBody::Empty => HBody::Empty,
        RespBody::Full(b) => HBody::Full(Some(b)),
        RespBody::Sse(s) => HBody::Sse(s),
    };
    let mut r = Response::new(body);
    *r.status_mut() = http::StatusCode::from_u16(resp.status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    let h = r.headers_mut();
    if let Some(ct) = resp.ctype {
        h.insert(http::header::CONTENT_TYPE, HeaderValue::from_static(ct));
    }
    for (k, v) in resp.headers {
        h.append(k, v);
    }
    r
}
