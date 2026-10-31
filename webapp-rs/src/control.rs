//! Read-only node status for startup and replication checks (loopback by default).

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

use crate::app::App;

async fn control(app: &'static App, req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let status = req.method() == hyper::Method::GET && req.uri().path() == "/status";
    let _ = req.into_body().collect().await;
    let (code, body) = if status {
        (200, app.status_json())
    } else {
        (404, "{\"error\":\"Not Found\"}".into())
    };
    Ok(Response::builder().status(code).header("content-type", "application/json").body(Full::new(Bytes::from(body + "\n"))).unwrap())
}

pub async fn serve(app: &'static App, listener: tokio::net::TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req| control(app, req));
            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), svc).await;
        });
    }
}
