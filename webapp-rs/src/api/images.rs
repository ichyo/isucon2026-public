//! Group C: GET /api/images/{imageId} (§5.3). Every response carries `Cache-Control: no-store`.
//! Signature mismatch => ApiError::forbidden("image_forbidden").abuse(Abuse::InvalidSignature).
//! Bytes: ctx.images.read(meta.loc).await (zero-copy for initial images). Verify via ctx.signer (cache).

use super::prelude::*;

// GET /api/images/{imageId} — no auth, not pinned. TODO(group C)
pub async fn get_image(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}
