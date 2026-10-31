use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::logger;
use crate::response::json;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub abuse: Option<&'static str>,
}

#[derive(Clone, Copy)]
pub struct AbuseEvent(pub &'static str);

#[derive(Debug)]
pub struct HttpError {
    pub status: StatusCode,
    pub message: String,
}

impl HttpError {
    pub fn new(status: StatusCode) -> HttpError {
        HttpError {
            status,
            message: status.canonical_reason().unwrap_or_default().to_owned(),
        }
    }

    pub fn wrap(status: StatusCode, err: impl std::fmt::Display) -> HttpError {
        HttpError {
            status,
            message: format!(
                "code={}, message={}, err={}",
                status.as_u16(),
                status.canonical_reason().unwrap_or_default(),
                err
            ),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Api(ApiError),
    Http(HttpError),
    Internal(anyhow::Error),
}

#[derive(Serialize)]
struct ApiErrorResponse {
    error: String,
}

pub fn new_api_error(status: StatusCode, code: &'static str) -> Error {
    Error::Api(ApiError {
        status,
        code,
        abuse: None,
    })
}

pub fn new_abuse_error(status: StatusCode, code: &'static str, abuse: &'static str) -> Error {
    Error::Api(ApiError {
        status,
        code,
        abuse: Some(abuse),
    })
}

impl Error {
    pub fn is_no_rows(&self) -> bool {
        match self {
            Error::Internal(err) => {
                matches!(
                    err.downcast_ref::<sqlx::Error>(),
                    Some(sqlx::Error::RowNotFound)
                )
            }
            _ => false,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Api(err) => write!(f, "{}", err.code),
            Error::Http(err) => write!(f, "{}", err.message),
            Error::Internal(err) => write!(f, "{err:#}"),
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Error {
        Error::Internal(err)
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Error {
        Error::Internal(err.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Internal(err.into())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        Error::Internal(err.into())
    }
}

impl From<zip::result::ZipError> for Error {
    fn from(err: zip::result::ZipError) -> Error {
        Error::Internal(err.into())
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let abuse = match &self {
            Error::Api(err) => err.abuse,
            _ => None,
        };
        let status = match &self {
            Error::Api(err) => err.status,
            Error::Http(err) => err.status,
            Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = self.to_string();
        if status.is_server_error() {
            logger::error("request failed", &message);
        }
        let body = ApiErrorResponse { error: message };
        let mut response = json(status, body).unwrap_or_else(|err| {
            logger::error("failed to write error response", &err);
            status.into_response()
        });
        if let Some(abuse) = abuse {
            response.extensions_mut().insert(AbuseEvent(abuse));
        }
        response
    }
}
