use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgQueryResult;
use sqlx::{Executor, PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FriendRequest {
    pub id: String,
    pub requester_id: String,
    pub recipient_id: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub async fn get_friend_request<'c>(
    db: impl Executor<'c, Database = Postgres>,
    id: &str,
) -> Result<FriendRequest, Error> {
    let request = sqlx::query_as("SELECT id, requester_id, recipient_id, status, created_at, updated_at FROM friend_requests WHERE id=$1")
        .bind(id)
        .fetch_one(db)
        .await?;
    Ok(request)
}

pub async fn begin_relationship_tx(
    db: &PgPool,
    actor: &str,
    target: &str,
) -> Result<Transaction<'static, Postgres>, Error> {
    if actor == target || target.is_empty() {
        return Err(new_api_error(
            StatusCode::BAD_REQUEST,
            "invalid_relationship",
        ));
    }
    let mut tx = db.begin().await?;
    for id in [actor.min(target), actor.max(target)] {
        let locked_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM users WHERE id=$1 AND status='active' FOR NO KEY UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        if locked_id.is_none() {
            return Err(new_api_error(
                StatusCode::NOT_FOUND,
                "relationship_not_found",
            ));
        }
    }
    Ok(tx)
}

pub async fn is_blocked<'c>(
    db: impl Executor<'c, Database = Postgres>,
    actor: &str,
    target: &str,
) -> Result<bool, Error> {
    let blocked = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM block_relations WHERE (blocker_id=$1 AND blocked_id=$2) OR (blocker_id=$2 AND blocked_id=$1))")
        .bind(actor)
        .bind(target)
        .fetch_one(db)
        .await?;
    Ok(blocked)
}

pub async fn insert_friendship(
    tx: &mut PgConnection,
    request: &FriendRequest,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO friendships (id, user_low_id, user_high_id, source_request_id) VALUES ($1,$2,$3,$4)")
        .bind(Uuid::new_v4().to_string())
        .bind(request.requester_id.as_str().min(&request.recipient_id))
        .bind(request.requester_id.as_str().max(&request.recipient_id))
        .bind(&request.id)
        .execute(tx)
        .await?;
    Ok(())
}

pub fn require_relationship_change(
    result: Result<PgQueryResult, sqlx::Error>,
    status: StatusCode,
    code: &'static str,
) -> Result<(), Error> {
    let count = result?.rows_affected();
    if count == 0 {
        return Err(new_api_error(status, code));
    }
    Ok(())
}
