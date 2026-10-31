use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use crate::api_error::{Error, new_api_error};
use crate::server::HandlerContext;
use crate::session::get_session;

pub async fn region_middleware(
    State(h): State<Arc<HandlerContext>>,
    req: Request,
    next: Next,
) -> Result<Response, Error> {
    let Some(session) = get_session(&req) else {
        return Err(new_api_error(StatusCode::UNAUTHORIZED, "invalid_session"));
    };
    if session.region != h.config.region {
        return Err(new_api_error(StatusCode::FORBIDDEN, "region_mismatch"));
    }
    Ok(next.run(req).await)
}
