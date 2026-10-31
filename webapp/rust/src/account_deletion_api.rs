use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;

use crate::api_error::{Error, new_api_error};
use crate::response::no_content;
use crate::server::HandlerContext;
use crate::session::Session;

pub async fn delete_account_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
) -> Result<Response, Error> {
    let user_id = &session.user_id;
    let mut tx = h.db.begin().await?;
    let locked_id: Option<String> = sqlx::query_scalar(
        "SELECT id FROM users WHERE id=$1 AND status='active' FOR NO KEY UPDATE",
    )
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;
    if locked_id.is_none() {
        return Err(new_api_error(StatusCode::NOT_FOUND, "account_not_found"));
    }
    sqlx::query("UPDATE users SET status='deleted', deleted_at=clock_timestamp(), updated_at=clock_timestamp() WHERE id=$1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE login_sessions SET active=false, invalidated_at=clock_timestamp(), reason='account_deletion' WHERE user_id=$1 AND active=true")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}
