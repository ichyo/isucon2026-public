use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::api_error::Error;

pub fn json<T: Serialize>(status: StatusCode, body: T) -> Result<Response, Error> {
    let mut data = serde_json::to_vec(&body)?;
    data.push(b'\n');
    blob(status, "application/json", data)
}

pub fn blob(status: StatusCode, content_type: &str, data: Vec<u8>) -> Result<Response, Error> {
    Ok((
        status,
        [(header::CONTENT_TYPE, content_type.to_owned())],
        data,
    )
        .into_response())
}

pub fn no_content(status: StatusCode) -> Result<Response, Error> {
    Ok(status.into_response())
}
