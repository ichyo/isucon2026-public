use std::sync::Arc;

use anyhow::Context as _;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{Executor, PgPool, Postgres};
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};
use crate::server::HandlerContext;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AdminSession {
    #[sqlx(skip)]
    pub token: String,
    pub administrator_id: String,
    pub token_hash: String,
    pub created_at: DateTime<Utc>,
}

pub fn get_admin_session(req: &Request) -> Option<&AdminSession> {
    req.extensions().get::<AdminSession>()
}

async fn get_admin_session_from_header(
    headers: &HeaderMap,
    db: &PgPool,
) -> Result<Option<AdminSession>, Error> {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default();
    if auth_header.is_empty() {
        return Ok(None);
    }
    let Some(token) = auth_header.strip_prefix("Bearer ") else {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_administrator_session",
        ));
    };
    let hash = Sha256::digest(token.as_bytes());
    let session: Option<AdminSession> = sqlx::query_as(
        r#"
  SELECT administrator_id, token_hash, created_at
  FROM administrator_sessions
  WHERE token_hash = $1 AND active = true
 "#,
    )
    .bind(hex::encode(hash))
    .fetch_optional(db)
    .await
    .context("failed to get admin session")?;
    Ok(session)
}

pub async fn admin_session_middleware(
    State((h, auth_required)): State<(Arc<HandlerContext>, bool)>,
    mut req: Request,
    next: Next,
) -> Result<Response, Error> {
    let session = get_admin_session_from_header(req.headers(), &h.db).await?;
    if session.is_none() && auth_required {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_administrator_session",
        ));
    }
    if let Some(session) = session {
        req.extensions_mut().insert(session);
    }
    Ok(next.run(req).await)
}

pub async fn create_admin_session(
    db: &PgPool,
    administrator_id: &str,
) -> Result<AdminSession, Error> {
    let mut tx = db.begin().await.context("failed to begin transaction")?;
    let (session_id, token) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let hash = Sha256::digest(token.as_bytes());
    sqlx::query(
        r#"
  INSERT INTO administrator_sessions (id, administrator_id, token_hash)
  VALUES ($1, $2, $3)
 "#,
    )
    .bind(&session_id)
    .bind(administrator_id)
    .bind(hex::encode(hash))
    .execute(&mut *tx)
    .await
    .context("failed to insert admin session")?;
    let mut session = get_admin_session_by_id(&mut *tx, &session_id).await?;
    tx.commit().await.context("failed to commit transaction")?;
    session.token = token;
    Ok(session)
}

async fn get_admin_session_by_id<'c>(
    db: impl Executor<'c, Database = Postgres>,
    session_id: &str,
) -> Result<AdminSession, Error> {
    let session = sqlx::query_as(
        "SELECT administrator_id, token_hash, created_at FROM administrator_sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_one(db)
    .await
    .context("failed to get admin session")?;
    Ok(session)
}

pub async fn invalidate_admin_session(
    db: &PgPool,
    administrator_id: &str,
    token_hash: &str,
) -> Result<(), Error> {
    sqlx::query(
        r#"
  UPDATE administrator_sessions
  SET active = false, invalidated_at = clock_timestamp()
  WHERE administrator_id = $1 AND token_hash = $2 AND active = true
 "#,
    )
    .bind(administrator_id)
    .bind(token_hash)
    .execute(db)
    .await
    .context("failed to invalidate admin session")?;
    Ok(())
}
