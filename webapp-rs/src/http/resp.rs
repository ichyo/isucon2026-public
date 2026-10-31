//! Responses and the error type (§2.2, §2.3).

use std::borrow::Cow;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{HeaderName, HeaderValue};
use http_body::{Frame, SizeHint};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::sse::SseBody;

/// Abuse kinds that feed the IP-ban detector (§8.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Abuse {
    FailedLogin,
    InvalidImage,
    InvalidSignature,
}

/// `{"error":"<code>"}` with an HTTP status (+ optional abuse flag).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub status: u16,
    pub code: Cow<'static, str>,
    pub abuse: Option<Abuse>,
}

impl ApiError {
    pub fn new(status: u16, code: impl Into<Cow<'static, str>>) -> ApiError {
        ApiError { status, code: code.into(), abuse: None }
    }
    pub fn bad_request(code: &'static str) -> ApiError {
        ApiError::new(400, code)
    }
    pub fn unauthorized(code: &'static str) -> ApiError {
        ApiError::new(401, code)
    }
    pub fn forbidden(code: &'static str) -> ApiError {
        ApiError::new(403, code)
    }
    pub fn not_found(code: &'static str) -> ApiError {
        ApiError::new(404, code)
    }
    pub fn conflict(code: &'static str) -> ApiError {
        ApiError::new(409, code)
    }
    /// Binding-layer error: `400 {"error":"code=400, message=Bad Request, err=<detail>"}`.
    pub fn bind(detail: impl std::fmt::Display) -> ApiError {
        ApiError::new(400, format!("code=400, message=Bad Request, err={detail}"))
    }
    /// Binding-layer 415 (unsupported body Content-Type).
    pub fn unsupported_media_type() -> ApiError {
        ApiError::new(415, "code=415, message=Unsupported Media Type")
    }
    pub fn internal(msg: impl std::fmt::Display) -> ApiError {
        ApiError::new(500, format!("{msg}"))
    }
    /// Something that "cannot happen" (e.g. a row just written is missing).
    pub fn internal_unknown() -> ApiError {
        ApiError::new(500, "internal error")
    }
    pub fn unavailable(msg: &'static str) -> ApiError {
        ApiError::new(503, msg)
    }
    pub fn not_implemented() -> ApiError {
        ApiError::new(501, "not implemented")
    }
    pub fn abuse(mut self, a: Abuse) -> ApiError {
        self.abuse = Some(a);
        self
    }
    pub fn body(&self) -> Bytes {
        let mut b = Vec::with_capacity(self.code.len() + 16);
        b.extend_from_slice(b"{\"error\":");
        crate::json::write_str(&mut b, &self.code);
        b.extend_from_slice(b"}\n");
        Bytes::from(b)
    }
    pub fn into_resp(self) -> Resp {
        let body = self.body();
        Resp::bytes(self.status, "application/json", body)
    }
}

pub type HResult = Result<Resp, ApiError>;

pub enum RespBody {
    Empty,
    Full(Bytes),
    Sse(SseBody),
}

pub struct Resp {
    pub status: u16,
    pub ctype: Option<&'static str>,
    pub headers: SmallVec<[(HeaderName, HeaderValue); 2]>,
    pub body: RespBody,
}

impl Resp {
    /// JSON body (must already contain the trailing `\n`).
    pub fn json(status: u16, body: impl Into<Bytes>) -> Resp {
        Resp::bytes(status, "application/json", body)
    }
    pub fn bytes(status: u16, ctype: &'static str, body: impl Into<Bytes>) -> Resp {
        Resp { status, ctype: Some(ctype), headers: SmallVec::new(), body: RespBody::Full(body.into()) }
    }
    /// 204 No Content.
    pub fn no_content() -> Resp {
        Resp { status: 204, ctype: None, headers: SmallVec::new(), body: RespBody::Empty }
    }
    pub fn empty(status: u16) -> Resp {
        Resp { status, ctype: None, headers: SmallVec::new(), body: RespBody::Empty }
    }
    pub fn sse(body: SseBody) -> Resp {
        Resp { status: 200, ctype: Some("text/event-stream"), headers: SmallVec::new(), body: RespBody::Sse(body) }
    }
    pub fn header(mut self, name: &'static str, value: &str) -> Resp {
        if let Ok(v) = HeaderValue::from_str(value) {
            self.headers.push((HeaderName::from_static(name), v));
        }
        self
    }
}

/// hyper body type for all responses.
pub enum HBody {
    Empty,
    Full(Option<Bytes>),
    Sse(SseBody),
}

impl http_body::Body for HBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match self.get_mut() {
            HBody::Empty => Poll::Ready(None),
            HBody::Full(b) => Poll::Ready(b.take().map(|b| Ok(Frame::data(b)))),
            HBody::Sse(s) => Pin::new(s).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            HBody::Empty => true,
            HBody::Full(b) => b.is_none(),
            HBody::Sse(_) => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            HBody::Empty => SizeHint::with_exact(0),
            HBody::Full(Some(b)) => SizeHint::with_exact(b.len() as u64),
            HBody::Full(None) => SizeHint::with_exact(0),
            HBody::Sse(_) => SizeHint::default(),
        }
    }
}
