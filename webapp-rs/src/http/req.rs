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

pub struct Req {
    pub method: Method,
    pub path: String,
    pub(crate) params: SmallVec<[String; 2]>,
    pub(crate) query: Option<String>,
    pub headers: HeaderMap,
    pub(crate) body: Option<Incoming>,
    pub(crate) body_bytes: Option<Bytes>,
    /// client IP (X-Client-IP verbatim, fallback TCP peer)
    pub ip: String,
    pub start: Instant,
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
        if let Some(b) = &self.body_bytes {
            return Ok(b.clone());
        }
        let Some(body) = self.body.take() else { return Ok(Bytes::new()) };
        let limited = http_body_util::Limited::new(body, MAX_BODY);
        let b = match limited.collect().await {
            Ok(c) => c.to_bytes(),
            Err(e) => {
                if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
                    return Err(ApiError::new(413, "Request Entity Too Large"));
                }
                return Err(ApiError::bind(format!("read body: {e}")));
            }
        };
        self.body_bytes = Some(b.clone());
        Ok(b)
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
