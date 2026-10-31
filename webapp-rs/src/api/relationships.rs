//! Group A: friend requests, friendships, follows, blocks, relationship lists (§6.10).

use super::prelude::*;

// POST /api/friend-requests — auth user, pinned. TODO(group A)
pub async fn create_friend_request(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `FriendRequestCreated`, or (reverse pending request) `FriendRequestUpdated{Accepted}` + `FriendshipCreated`.
/// Outcome::Id(request id).
pub fn exec_create_friend_request(_st: &State, _tx: &mut Tx, _actor: Id, _recipient: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// POST /api/friend-requests/{requestId}/{action} — auth user, pinned. TODO(group A)
pub async fn transition_friend_request(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `FriendRequestUpdated` (+ `FriendshipCreated` on accept).
pub fn exec_transition_friend_request(_st: &State, _tx: &mut Tx, _actor: Id, _request: &str, _action: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// GET /api/relationships/{kind} — auth user, pinned. TODO(group A)
pub async fn get_relationships(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// DELETE /api/friendships/{userId} — auth user, pinned. TODO(group A)
pub async fn delete_friendship(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `FriendshipRemoved`.
pub fn exec_delete_friendship(_st: &State, _tx: &mut Tx, _actor: Id, _target: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// PUT /api/follows/{userId} — auth user, pinned. TODO(group A)
pub async fn follow(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `FollowCreated`.
pub fn exec_follow(_st: &State, _tx: &mut Tx, _actor: Id, _target: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// DELETE /api/follows/{userId} — auth user, pinned. TODO(group A)
pub async fn unfollow(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `FollowRemoved`.
pub fn exec_unfollow(_st: &State, _tx: &mut Tx, _actor: Id, _target: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// PUT /api/blocks/{userId} — auth user, pinned. TODO(group A)
pub async fn block(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `BlockCreated` (apply performs the cascade).
pub fn exec_block(_st: &State, _tx: &mut Tx, _actor: Id, _target: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// DELETE /api/blocks/{userId} — auth user, pinned. TODO(group A)
pub async fn unblock(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `BlockRemoved`.
pub fn exec_unblock(_st: &State, _tx: &mut Tx, _actor: Id, _target: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}
