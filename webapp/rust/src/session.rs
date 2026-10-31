use std::sync::Arc;

use anyhow::Context as _;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{Executor, PgConnection, PgPool, Postgres};
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};
use crate::logger;
use crate::server::HandlerContext;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Session {
    #[sqlx(default)]
    pub region: String,
    #[sqlx(skip)]
    pub token: String,
    pub user_id: String,
    pub token_hash: String,
    pub created_at: DateTime<Utc>,
}

pub fn get_session(req: &Request) -> Option<&Session> {
    req.extensions().get::<Session>()
}

async fn get_session_from_header(
    headers: &HeaderMap,
    db: &PgPool,
) -> Result<Option<Session>, Error> {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default();
    if auth_header.is_empty() {
        return Ok(None);
    }
    let Some(token) = auth_header.strip_prefix("Bearer ") else {
        logger::warn("Invalid authorization header format(not started with Bearer)");
        return Err(new_api_error(StatusCode::UNAUTHORIZED, "invalid_session"));
    };

    let token_hash = hex::encode(Sha256::digest(token.as_bytes()));
    let session: Option<Session> = sqlx::query_as(
        r#"
		SELECT s.user_id, s.token_hash, s.created_at, u.region
		FROM login_sessions s JOIN users u ON u.id = s.user_id
		WHERE s.token_hash = $1
		  AND s.active = true AND u.status = 'active'
	"#,
    )
    .bind(&token_hash)
    .fetch_optional(db)
    .await
    .context("failed to get session")?;
    Ok(session)
}

pub async fn session_middleware(
    State((handler_context, auth_required)): State<(Arc<HandlerContext>, bool)>,
    mut req: Request,
    next: Next,
) -> Result<Response, Error> {
    let session = get_session_from_header(req.headers(), &handler_context.db).await?;
    if session.is_none() && auth_required {
        return Err(new_api_error(StatusCode::UNAUTHORIZED, "invalid_session"));
    }
    if let Some(session) = &session {
        req.extensions_mut().insert(session.clone());
    }
    let mut response = next.run(req).await;
    if let Some(session) = session {
        response.extensions_mut().insert(session);
    }
    Ok(response)
}

pub async fn invalidate_login_session(
    db: &PgPool,
    user_id: &str,
    token_hash: &str,
) -> Result<(), Error> {
    let mut tx = db.begin().await.context("failed to begin transaction")?;

    let _locked_user_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE")
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await
            .context("failed to lock user")?;

    sqlx::query(
        r#"
		UPDATE login_sessions
		SET active = false, invalidated_at = clock_timestamp(), reason = 'logout'
		WHERE user_id = $1 AND active = true AND token_hash = $2
	"#,
    )
    .bind(user_id)
    .bind(token_hash)
    .execute(&mut *tx)
    .await
    .context("failed to invalidate sessions")?;

    tx.commit().await.context("failed to commit transaction")?;

    Ok(())
}

// 2026-09-03: リファクタリング後にこの関数の利用がないようです。消すのはいかがでしょうか
pub async fn create_login_session(db: &PgPool, user_id: &str) -> Result<Session, Error> {
    let mut tx = db.begin().await.context("failed to begin transaction")?;
    let session = create_login_session_tx(&mut tx, user_id).await?;
    tx.commit().await.context("failed to commit transaction")?;
    Ok(session)
}

pub async fn create_login_session_tx(
    tx: &mut PgConnection,
    user_id: &str,
) -> Result<Session, Error> {
    let random_token = Uuid::new_v4().to_string();
    let token_hash = hex::encode(Sha256::digest(random_token.as_bytes()));
    let _locked_user_id: String = sqlx::query_scalar(
        "SELECT id FROM users WHERE id = $1 AND status = 'active' FOR NO KEY UPDATE",
    )
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await
    .context("failed to lock user")?;
    sqlx::query(
        r#"
  UPDATE login_sessions
  SET active = false, invalidated_at = clock_timestamp(), reason = 'replaced'
  WHERE user_id = $1 AND active = true
 "#,
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await
    .context("failed to invalidate sessions")?;
    let session_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
  INSERT INTO login_sessions (id, user_id, token_hash)
  VALUES ($1, $2, $3)
 "#,
    )
    .bind(&session_id)
    .bind(user_id)
    .bind(&token_hash)
    .execute(&mut *tx)
    .await
    .context("failed to insert session")?;
    let mut session = get_login_session(&mut *tx, &session_id).await?;
    session.token = random_token;
    Ok(session)
}

async fn get_login_session<'c>(
    db: impl Executor<'c, Database = Postgres>,
    session_id: &str,
) -> Result<Session, Error> {
    let session =
        sqlx::query_as("SELECT user_id, token_hash, created_at FROM login_sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(db)
            .await
            .context("failed to get session")?;
    Ok(session)
}
