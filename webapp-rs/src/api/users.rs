//! Group A: GET /api/users?q=, GET /api/users/{userId} (§6.4). Read-only.

use super::prelude::*;

// GET /api/users?q=&beforeUsername=&beforeId= — auth user, pinned. TODO(group A)
// Hints: text::fold(q) vs User.fold_username / fold_display; iterate st.users_sorted from the cursor.
pub async fn search_users(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// GET /api/users/{userId} — auth user, pinned. TODO(group A)
pub async fn get_user(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}
