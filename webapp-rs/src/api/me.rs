//! Group A: /api/me (GET implemented by core), PATCH, profile image, account deletion (§6.3).

use super::prelude::*;

// GET /api/me — auth user, pinned
pub async fn get_me(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req)?;
    ctx.respond(200, |st, w| {
        views::me(w, st.user(me.idx));
        Ok(())
    })
    .await
}

// PATCH /api/me — auth user, NOT pinned. TODO(group A)
pub async fn update_me(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `LogEntry::ProfileUpdated` (apply closes SSE streams on region change).
pub fn exec_update_profile(_st: &State, _tx: &mut Tx, _user: Id, _display_name: Option<&str>, _bio: Option<&str>, _region: Option<Region>) -> ExecResult {
    Err(ApiError::not_implemented())
}

// PUT /api/me/profile-image — auth user, not pinned; decode failure => .abuse(Abuse::InvalidImage). TODO(group A)
pub async fn put_profile_image(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `tx.push_blob(LogEntry::ProfileImageSet { image: Some(new_id), .. }, image.clone())`.
/// (exec implemented by core to exercise the blob replication path; the handler above is group A's.)
pub fn exec_set_profile_image(st: &State, tx: &mut Tx, user: Id, image: &Bytes) -> ExecResult {
    st.user_idx(user).filter(|&u| st.user(u).is_active()).ok_or(ApiError::unauthorized("invalid_session"))?;
    let id = tx.new_id();
    tx.push_blob(LogEntry::ProfileImageSet { user, image: Some(id), at: tx.now }, image.clone());
    Ok(Outcome::Id(id))
}

// DELETE /api/me/profile-image — auth user, not pinned. TODO(group A)
pub async fn delete_profile_image(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `LogEntry::ProfileImageSet { image: None, .. }` (exec implemented by core; handler is group A's).
pub fn exec_delete_profile_image(st: &State, tx: &mut Tx, user: Id) -> ExecResult {
    let u = st.user_idx(user).filter(|&u| st.user(u).is_active()).ok_or(ApiError::unauthorized("invalid_session"))?;
    if st.user(u).profile_image.is_none() {
        return Err(ApiError::not_found("account_not_found"));
    }
    tx.push(LogEntry::ProfileImageSet { user, image: None, at: tx.now });
    Ok(Outcome::Unit)
}

// POST /api/me/account-deletion — auth user, pinned. TODO(group A)
pub async fn delete_account(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `LogEntry::AccountDeleted`.
pub fn exec_delete_account(_st: &State, _tx: &mut Tx, _user: Id) -> ExecResult {
    Err(ApiError::not_implemented())
}
