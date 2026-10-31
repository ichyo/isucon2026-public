//! Group C: comments (§6.9).

use super::prelude::*;

// POST /api/posts/{postId}/comments — auth user, pinned. TODO(group C)
pub async fn create_comment(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `CommentCreated`. Outcome::Id(comment id).
pub fn exec_create_comment(_st: &State, _tx: &mut Tx, _actor: Id, _post: &str, _body: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// GET /api/posts/{postId}/comments — auth user, pinned. TODO(group C)
pub async fn get_comments(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// DELETE /api/comments/{commentId} — auth user, pinned. TODO(group C)
pub async fn delete_comment(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `CommentDeleted`.
pub fn exec_delete_comment(_st: &State, _tx: &mut Tx, _actor: Id, _comment: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}
