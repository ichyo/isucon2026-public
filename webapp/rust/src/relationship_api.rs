use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder};
use crate::relationship::{begin_relationship_tx, is_blocked, require_relationship_change};
use crate::response::no_content;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::user::get_user;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct DeleteFriendshipRequest {
    #[serde(skip)]
    pub user_id: String,
}

impl Bindable for DeleteFriendshipRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("userId", &mut self.user_id)
    }
}

pub async fn delete_friendship_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<DeleteFriendshipRequest>,
) -> Result<Response, Error> {
    let actor = session.user_id.as_str();
    let mut tx = begin_relationship_tx(&h.db, actor, &input.user_id).await?;
    let result = sqlx::query("DELETE FROM friendships WHERE user_low_id=$1 AND user_high_id=$2")
        .bind(actor.min(&input.user_id))
        .bind(actor.max(&input.user_id))
        .execute(&mut *tx)
        .await;
    require_relationship_change(result, StatusCode::CONFLICT, "relationship_conflict")?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct FollowRequest {
    #[serde(skip)]
    pub user_id: String,
}

impl Bindable for FollowRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("userId", &mut self.user_id)
    }
}

pub async fn follow_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<FollowRequest>,
) -> Result<Response, Error> {
    let actor = &session.user_id;
    let mut tx = begin_relationship_tx(&h.db, actor, &input.user_id).await?;
    let target = get_user(&mut *tx, &input.user_id).await?;
    if target.account_type != "certified_public" {
        return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
    }
    let blocked = is_blocked(&mut *tx, actor, &target.id).await?;
    if blocked {
        return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
    }
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM follows WHERE follower_id=$1 AND followee_id=$2)",
    )
    .bind(actor)
    .bind(&target.id)
    .fetch_one(&mut *tx)
    .await?;
    if exists {
        return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
    }
    sqlx::query("INSERT INTO follows (id, follower_id, followee_id) VALUES ($1,$2,$3)")
        .bind(Uuid::new_v4().to_string())
        .bind(actor)
        .bind(&target.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct UnfollowRequest {
    #[serde(skip)]
    pub user_id: String,
}

impl Bindable for UnfollowRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("userId", &mut self.user_id)
    }
}

pub async fn unfollow_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<UnfollowRequest>,
) -> Result<Response, Error> {
    let actor = &session.user_id;
    let mut tx = begin_relationship_tx(&h.db, actor, &input.user_id).await?;
    let result = sqlx::query("DELETE FROM follows WHERE follower_id=$1 AND followee_id=$2")
        .bind(actor)
        .bind(&input.user_id)
        .execute(&mut *tx)
        .await;
    require_relationship_change(result, StatusCode::CONFLICT, "relationship_conflict")?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct BlockRequest {
    #[serde(skip)]
    pub user_id: String,
}

impl Bindable for BlockRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("userId", &mut self.user_id)
    }
}

pub async fn block_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<BlockRequest>,
) -> Result<Response, Error> {
    let actor = session.user_id.as_str();
    let mut tx = begin_relationship_tx(&h.db, actor, &input.user_id).await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM block_relations WHERE blocker_id=$1 AND blocked_id=$2)",
    )
    .bind(actor)
    .bind(&input.user_id)
    .fetch_one(&mut *tx)
    .await?;
    if exists {
        return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
    }
    sqlx::query("INSERT INTO block_relations (id, blocker_id, blocked_id) VALUES ($1,$2,$3)")
        .bind(Uuid::new_v4().to_string())
        .bind(actor)
        .bind(&input.user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM friendships WHERE user_low_id=$1 AND user_high_id=$2")
        .bind(actor.min(&input.user_id))
        .bind(actor.max(&input.user_id))
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM follows WHERE (follower_id=$1 AND followee_id=$2) OR (follower_id=$2 AND followee_id=$1)")
        .bind(actor)
        .bind(&input.user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE friend_requests SET status='cancelled', updated_at=clock_timestamp() WHERE status='pending' AND ((requester_id=$1 AND recipient_id=$2) OR (requester_id=$2 AND recipient_id=$1))")
        .bind(actor)
        .bind(&input.user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct UnblockRequest {
    #[serde(skip)]
    pub user_id: String,
}

impl Bindable for UnblockRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("userId", &mut self.user_id)
    }
}

pub async fn unblock_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<UnblockRequest>,
) -> Result<Response, Error> {
    let actor = &session.user_id;
    let mut tx = begin_relationship_tx(&h.db, actor, &input.user_id).await?;
    let result = sqlx::query("DELETE FROM block_relations WHERE blocker_id=$1 AND blocked_id=$2")
        .bind(actor)
        .bind(&input.user_id)
        .execute(&mut *tx)
        .await;
    require_relationship_change(
        result,
        StatusCode::FORBIDDEN,
        "relationship_action_forbidden",
    )?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}
