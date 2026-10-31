//! Group B: GET /api/feeds/{feed} (§6.7). Candidates: only posts of open windows (st.open_window[*]).

use super::prelude::*;

// GET /api/feeds/{feed} — auth user, pinned. TODO(group B)
pub async fn get_feed(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}
