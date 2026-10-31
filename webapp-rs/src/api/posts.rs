//! Group B: posts (§6.6).

use super::prelude::*;

// POST /api/posts — auth user, pinned, multipart. Decode the JPEG (jpeg::decode) BEFORE ctx.write. TODO(group B)
pub async fn create_post(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `tx.push_blob(LogEntry::PostCreated { .. }, image.clone())`. Outcome::Id(post id).
pub fn exec_create_post(_st: &State, _tx: &mut Tx, _actor: Id, _image: &Bytes, _caption: &str, _visibility: Visibility, _location: &Location) -> ExecResult {
    Err(ApiError::not_implemented())
}

// GET /api/posts/me/current — auth user, pinned. TODO(group B)
pub async fn get_my_current_post(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// GET /api/posts/{postId} — auth user, pinned. TODO(group B)
pub async fn get_post(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// PATCH /api/posts/{postId} — auth user, pinned. TODO(group B)
pub async fn update_post(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `PostUpdated`.
pub fn exec_update_post(_st: &State, _tx: &mut Tx, _actor: Id, _post: &str, _caption: Option<&str>, _visibility: Option<Visibility>) -> ExecResult {
    Err(ApiError::not_implemented())
}

// DELETE /api/posts/{postId} — auth user, pinned. TODO(group B)
pub async fn delete_post(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `PostDeleted`.
pub fn exec_delete_post(_st: &State, _tx: &mut Tx, _actor: Id, _post: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}
