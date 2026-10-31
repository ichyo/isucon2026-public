//! Group A: admin login/logout, certification (§6.11). Admin auth: ctx.auth_admin(&req).

use super::prelude::*;

// POST /api/admin/auth/login — auth none. TODO(group A)
pub async fn admin_login(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `AdminSessionCreated`; Outcome::Login { user: admin id, token }.
pub fn exec_admin_login(_st: &State, _tx: &mut Tx, _username: &str, _password: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// POST /api/admin/auth/logout — admin. TODO(group A)
pub async fn admin_logout(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `AdminSessionEnded`.
pub fn exec_admin_logout(_st: &State, _tx: &mut Tx, _token: Id) -> ExecResult {
    Err(ApiError::not_implemented())
}

// PUT /api/admin/users/{userId}/certification — admin. TODO(group A)
pub async fn certify_user(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}

/// Emits `UserCertified`.
pub fn exec_certify(_st: &State, _tx: &mut Tx, _user: &str) -> ExecResult {
    Err(ApiError::not_implemented())
}

// DELETE /api/admin/users/{userId}/certification — admin; always 409 certification_irreversible. TODO(group A)
pub async fn delete_user_certification(_ctx: Ctx, _req: Req) -> HResult {
    Err(ApiError::not_implemented())
}
