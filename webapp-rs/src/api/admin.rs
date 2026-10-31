//! Group A: admin login/logout, certification (§6.11). Admin auth: ctx.auth_admin(&req).

use sha2::{Digest, Sha256};

use super::prelude::*;

// POST /api/admin/auth/login — auth none. Body {"username","password"}.
// 200 {"token","administratorId"}; other admin sessions stay valid.
pub async fn admin_login(ctx: Ctx, mut req: Req) -> HResult {
    let body = req.bind().await?;
    let username = body.str("username")?.unwrap_or_default();
    let password = body.str("password")?.unwrap_or_default();
    let out = ctx.write(Command::AdminLogin { username, password }).await?;
    let Outcome::Login { user: admin, token } = out else { return Err(ApiError::internal_unknown()) };
    let mut w = ctx.json();
    w.obj().k("token").id(token).k("administratorId").id(admin);
    w.end();
    ctx.finish(w, 200).await
}

/// Emits `AdminSessionCreated`; Outcome::Login { user: admin id, token }. Bad pattern / unknown /
/// wrong password ⇒ 401 invalid_credentials (not counted as a failed user login, §8.1).
pub fn exec_admin_login(st: &State, tx: &mut Tx, username: &str, password: &str) -> ExecResult {
    let fail = || ApiError::unauthorized("invalid_credentials");
    if !text::valid_username(username) {
        return Err(fail());
    }
    let a = st.admin_by_username(username).ok_or_else(fail)?;
    let admin = &st.admins[a as usize];
    let h: [u8; 32] = Sha256::digest(password.as_bytes()).into();
    if !sign::ct_eq(&h, &admin.password_hash) {
        return Err(fail());
    }
    let token = tx.new_id();
    tx.push(LogEntry::AdminSessionCreated { token, admin: admin.id, at: tx.now });
    Ok(Outcome::Login { user: admin.id, token })
}

// POST /api/admin/auth/logout — admin. 204 (invalidates that admin session only).
pub async fn admin_logout(ctx: Ctx, req: Req) -> HResult {
    let admin = ctx.auth_admin(&req)?;
    ctx.write(Command::AdminLogout { token: admin.token }).await?;
    Ok(Resp::no_content())
}

/// Emits `AdminSessionEnded` (session already gone on the leader ⇒ 401).
pub fn exec_admin_logout(st: &State, tx: &mut Tx, token: Id) -> ExecResult {
    if !st.admin_sessions.contains_key(&token) {
        return Err(ApiError::unauthorized("invalid_administrator_session"));
    }
    tx.push(LogEntry::AdminSessionEnded { token });
    Ok(Outcome::Unit)
}

// PUT /api/admin/users/{userId}/certification — admin. 204; missing / deleted / already certified ⇒ 409.
pub async fn certify_user(ctx: Ctx, req: Req) -> HResult {
    ctx.auth_admin(&req)?;
    let user = req.param(0).to_string();
    ctx.write(Command::Certify { user }).await?;
    Ok(Resp::no_content())
}

/// Emits `UserCertified`.
pub fn exec_certify(st: &State, tx: &mut Tx, user: &str) -> ExecResult {
    let u = st
        .active_user_str(user)
        .filter(|&u| st.user(u).account_type == AccountType::Regular)
        .ok_or(ApiError::conflict("certification_conflict"))?;
    tx.push(LogEntry::UserCertified { user: st.user(u).id, at: tx.now });
    Ok(Outcome::Unit)
}

// DELETE /api/admin/users/{userId}/certification — admin; always 409 certification_irreversible.
pub async fn delete_user_certification(ctx: Ctx, req: Req) -> HResult {
    ctx.auth_admin(&req)?;
    Err(ApiError::conflict("certification_irreversible"))
}
