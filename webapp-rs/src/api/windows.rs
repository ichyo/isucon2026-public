//! Group B: GET /api/posting-window/current, POST /api/admin/posting-window (§6.5, §6.11).

use super::prelude::*;

// GET /api/posting-window/current — auth user, pinned. TODO(group B)
pub async fn get_current_posting_window(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// POST /api/admin/posting-window — admin. TODO(group B)
pub async fn open_posting_window(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `WindowOpened { starts_at: tx.now, on_time_deadline: tx.now + secs·1e6 (saturating) }` (apply ends the
/// previous window; SSE fan-out via effects). Conflict check: `st.last_date[region]`. Outcome::Id(window id).
pub fn exec_open_window(_st: &State, _tx: &mut Tx, _region: Region, _local_date: Date, _on_time_seconds: i64) -> ExecResult {
    Err(ApiError::not_implemented())
}
