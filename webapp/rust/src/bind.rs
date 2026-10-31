use std::collections::HashMap;
use std::convert::Infallible;
use std::num::IntErrorKind;

use axum::extract::{FromRequest, FromRequestParts, Path, Request};
use axum::http::request::Parts;
use axum::http::{Method, StatusCode, header};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;

use crate::api_error::{Error, HttpError};
use crate::timefmt::parse_rfc3339;

pub type Values = HashMap<String, Vec<String>>;

#[derive(Debug, Clone, Default)]
pub struct FileHeader {
    pub filename: String,
    data: Bytes,
}

impl FileHeader {
    pub fn open(&self) -> Bytes {
        self.data.clone()
    }
}

pub trait Bindable: Default + DeserializeOwned + Send {
    fn bind(&mut self, _b: &Binder) -> Result<(), String> {
        Ok(())
    }
}

#[derive(PartialEq)]
enum Tag {
    Param,
    Query,
    Form,
}

pub struct Binder<'a> {
    tag: Tag,
    data: &'a Values,
    files: &'a HashMap<String, Vec<FileHeader>>,
}

pub trait FromParam: Sized {
    fn from_param(value: &str) -> Result<Self, String>;
}

impl FromParam for String {
    fn from_param(value: &str) -> Result<Self, String> {
        Ok(value.to_owned())
    }
}

impl FromParam for i64 {
    fn from_param(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Ok(0);
        }
        value
            .parse()
            .map_err(|err: std::num::ParseIntError| match err.kind() {
                IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => {
                    format!("strconv.ParseInt: parsing {value:?}: value out of range")
                }
                _ => format!("strconv.ParseInt: parsing {value:?}: invalid syntax"),
            })
    }
}

impl FromParam for f64 {
    fn from_param(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Ok(0.0);
        }
        value
            .parse()
            .map_err(|_| format!("strconv.ParseFloat: parsing {value:?}: invalid syntax"))
    }
}

impl FromParam for DateTime<Utc> {
    fn from_param(value: &str) -> Result<Self, String> {
        parse_rfc3339(value)
            .ok_or_else(|| format!("parsing time {value:?} as \"2006-01-02T15:04:05Z07:00\""))
    }
}

impl<T: FromParam> FromParam for Option<T> {
    fn from_param(value: &str) -> Result<Self, String> {
        T::from_param(value).map(Some)
    }
}

impl Binder<'_> {
    pub fn param<T: FromParam>(&self, name: &str, field: &mut T) -> Result<(), String> {
        self.bind_value(Tag::Param, name, field)
    }

    pub fn query<T: FromParam>(&self, name: &str, field: &mut T) -> Result<(), String> {
        self.bind_value(Tag::Query, name, field)
    }

    pub fn form<T: FromParam>(&self, name: &str, field: &mut T) -> Result<(), String> {
        self.bind_value(Tag::Form, name, field)
    }

    pub fn form_file(&self, name: &str, field: &mut Option<FileHeader>) -> Result<(), String> {
        if self.tag == Tag::Form
            && let Some(file) = self.files.get(name).and_then(|files| files.first())
        {
            *field = Some(file.clone());
        }
        Ok(())
    }

    fn bind_value<T: FromParam>(&self, tag: Tag, name: &str, field: &mut T) -> Result<(), String> {
        if self.tag != tag {
            return Ok(());
        }
        let values = self.data.get(name).or_else(|| {
            self.data
                .iter()
                .find(|(key, _)| key.to_lowercase() == name.to_lowercase())
                .map(|(_, values)| values)
        });
        let Some(value) = values.and_then(|values| values.first()) else {
            return Ok(());
        };
        *field = T::from_param(value).map_err(|err| format!("{name}: {err}"))?;
        Ok(())
    }
}

pub struct Bind<T>(pub T);

impl<S: Send + Sync, T: Bindable> FromRequest<S> for Bind<T> {
    type Rejection = Error;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = req.into_parts();
        let path_values: Values =
            match Path::<HashMap<String, String>>::from_request_parts(&mut parts, state).await {
                Ok(Path(params)) => params
                    .into_iter()
                    .map(|(name, value)| (name, vec![value]))
                    .collect(),
                Err(_) => Values::new(),
            };
        let body = axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(bad_request)?;
        let mediatype = parts
            .headers
            .get(header::CONTENT_TYPE)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
            .unwrap_or_default();
        let mediatype = mediatype.split(';').next().unwrap_or_default().trim();

        let no_files = HashMap::new();
        let mut target = if !body.is_empty() && mediatype == "application/json" {
            serde_json::from_slice(&body).map_err(bad_request)?
        } else {
            T::default()
        };
        target
            .bind(&Binder {
                tag: Tag::Param,
                data: &path_values,
                files: &no_files,
            })
            .map_err(bad_request)?;
        if matches!(parts.method, Method::GET | Method::DELETE | Method::HEAD) {
            let query_values = query_params(parts.uri.query().unwrap_or_default());
            target
                .bind(&Binder {
                    tag: Tag::Query,
                    data: &query_values,
                    files: &no_files,
                })
                .map_err(bad_request)?;
        }
        if body.is_empty() {
            return Ok(Bind(target));
        }
        match mediatype {
            "application/json" => {}
            "application/x-www-form-urlencoded" => {
                let values = form_values(&parts, &body).map_err(bad_request)?;
                target
                    .bind(&Binder {
                        tag: Tag::Form,
                        data: &values,
                        files: &no_files,
                    })
                    .map_err(bad_request)?;
            }
            "multipart/form-data" => {
                let (values, files) = multipart_form(&parts, body).await.map_err(bad_request)?;
                target
                    .bind(&Binder {
                        tag: Tag::Form,
                        data: &values,
                        files: &files,
                    })
                    .map_err(bad_request)?;
            }
            _ => {
                return Err(Error::Http(HttpError {
                    status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    message: "code=415, message=Unsupported Media Type".to_owned(),
                }));
            }
        }
        Ok(Bind(target))
    }
}

fn bad_request(err: impl std::fmt::Display) -> Error {
    Error::Http(HttpError::wrap(StatusCode::BAD_REQUEST, err))
}

fn form_values(parts: &Parts, body: &[u8]) -> Result<Values, String> {
    let mut values = Values::new();
    if matches!(parts.method, Method::POST | Method::PUT | Method::PATCH) {
        values = parse_query(&String::from_utf8_lossy(body))?;
    }
    for (key, query_values) in parse_query(parts.uri.query().unwrap_or_default())? {
        values.entry(key).or_default().extend(query_values);
    }
    Ok(values)
}

async fn multipart_form(
    parts: &Parts,
    body: Bytes,
) -> Result<(Values, HashMap<String, Vec<FileHeader>>), multer::Error> {
    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default();
    let boundary = multer::parse_boundary(content_type)?;
    let stream = futures_util::stream::once(async move { Ok::<_, Infallible>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut values = Values::new();
    let mut files: HashMap<String, Vec<FileHeader>> = HashMap::new();
    while let Some(field) = multipart.next_field().await? {
        let Some(name) = field
            .name()
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
        else {
            continue;
        };
        let filename = field.file_name().unwrap_or_default().to_owned();
        let data = field.bytes().await?;
        if filename.is_empty() {
            values
                .entry(name)
                .or_default()
                .push(String::from_utf8_lossy(&data).into_owned());
        } else {
            files
                .entry(name)
                .or_default()
                .push(FileHeader { filename, data });
        }
    }
    Ok((values, files))
}

pub fn query_params(query: &str) -> Values {
    parse_query_values(query).0
}

pub fn parse_query(query: &str) -> Result<Values, String> {
    match parse_query_values(query) {
        (values, None) => Ok(values),
        (_, Some(err)) => Err(err),
    }
}

fn parse_query_values(query: &str) -> (Values, Option<String>) {
    let mut values = Values::new();
    let mut first_err = None;
    for pair in query.split('&') {
        if pair.contains(';') {
            first_err.get_or_insert_with(|| "invalid semicolon separator in query".to_owned());
            continue;
        }
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match (query_unescape(key), query_unescape(value)) {
            (Ok(key), Ok(value)) => values.entry(key).or_default().push(value),
            (Err(err), _) | (_, Err(err)) => {
                first_err.get_or_insert(err);
            }
        }
    }
    (values, first_err)
}

fn query_unescape(s: &str) -> Result<String, String> {
    let bytes = s.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes
                    .get(i + 1..i + 3)
                    .filter(|hex| hex.iter().all(u8::is_ascii_hexdigit))
                    .and_then(|hex| std::str::from_utf8(hex).ok());
                let Some(byte) = hex.and_then(|hex| u8::from_str_radix(hex, 16).ok()) else {
                    let end = (i + 3).min(bytes.len());
                    return Err(format!(
                        "invalid URL escape {:?}",
                        String::from_utf8_lossy(&bytes[i..end])
                    ));
                };
                decoded.push(byte);
                i += 3;
            }
            b'+' => {
                decoded.push(b' ');
                i += 1;
            }
            byte => {
                decoded.push(byte);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}
