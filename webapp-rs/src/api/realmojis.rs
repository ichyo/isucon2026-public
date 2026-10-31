//! Group C: RealMojis (§6.9).

use super::prelude::*;

// POST /api/posts/{postId}/realmojis — auth user, pinned, multipart. Decode the JPEG before ctx.write. TODO(group C)
pub async fn replace_real_moji(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `tx.push_blob(LogEntry::RealMojiCreated { .. }, image.clone())` (apply marks the previous one replaced).
/// Outcome::Id(realmoji id).
pub fn exec_put_realmoji(_st: &State, _tx: &mut Tx, _actor: Id, _post: &str, _reaction: Reaction, _image: &Bytes) -> ExecResult {
    Err(ApiError::not_implemented())
}

// GET /api/posts/{postId}/realmojis — auth user, pinned. TODO(group C)
pub async fn get_real_mojis(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// DELETE /api/realmojis/{realMojiId} — auth user, pinned. TODO(group C)
pub async fn delete_real_moji(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `RealMojiDeleted`.
pub fn exec_delete_realmoji(_st: &State, _tx: &mut Tx, _actor: Id, _realmoji: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}
