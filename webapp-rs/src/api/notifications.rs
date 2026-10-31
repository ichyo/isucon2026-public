//! Group C: GET /api/notifications (SSE, §7). Use `ctx.sse.subscribe(me.idx, now_us())` and
//! `Resp::sse(body)`; delivery/closing is driven by apply effects (sse.rs).

use super::prelude::*;

// GET /api/notifications — auth user, pinned. TODO(group C)
pub async fn get_notifications(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}
