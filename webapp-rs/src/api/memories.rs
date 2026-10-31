//! Group B: GET /api/memories?month=, GET /api/memories/{date} (§6.8).

use super::prelude::*;

// GET /api/memories?month=YYYY-MM — auth user, pinned. TODO(group B)
pub async fn get_memories(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

// GET /api/memories/{date} — auth user, pinned. TODO(group B)
pub async fn get_memories_by_date(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}
