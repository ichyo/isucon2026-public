//! Group B: GET /api/me/export (§6.3) — zip: images/NNNNNN.jpg stored, posts.json deflated.
//! Copy what you need under the read lock, release it, then read image bytes (ctx.images.read) and zip.

use super::prelude::*;

// GET /api/me/export — auth user, pinned. TODO(group B)
pub async fn export_posts(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}
