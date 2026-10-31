//! Group C: GET /api/notifications (SSE, §7) on top of `sse.rs`.
//!
//! `200 text/event-stream` right away (no bytes until the event), then exactly one `data: {…}\n\n` when a
//! window of the user's region opens after `connectedAt`, then the response ends. Ends without an event on
//! account deletion / region change (`UserGone` effect), on initialize, and after 60 s idle. Delivery is pushed
//! by the apply effect (`SseHub::on_window_opened`), which runs under the state write lock.

use super::prelude::*;

// GET /api/notifications — auth user, pinned.
pub async fn get_notifications(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    // Register under the state read lock: a WindowOpened apply (write lock) is then either entirely before
    // the registration (its startsAt < connectedAt ⇒ correctly not delivered) or after it (sees us), and a
    // deletion / region change since the auth check cannot slip in between (it would leave a dangling stream).
    let body = {
        let st = ctx.read();
        if st.user_idx(me.id) != Some(me.idx) || !st.user(me.idx).is_active() {
            return Err(ApiError::unauthorized("invalid_session"));
        }
        if st.user(me.idx).region != ctx.region() {
            return Err(ApiError::forbidden("region_mismatch"));
        }
        ctx.sse.subscribe(me.idx, now_us())
    };
    Ok(Resp::sse(body))
}
