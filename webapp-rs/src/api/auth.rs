//! Group A: POST /api/auth/register, /login (implemented by core), /logout (§6.2).

use sha2::{Digest, Sha256};

use super::prelude::*;

// ---------------------------------------------------------------------------------------------
// POST /api/auth/register — auth none, not pinned
// ---------------------------------------------------------------------------------------------

pub async fn register(ctx: Ctx, mut req: Req) -> HResult {
    let body = req.bind().await?;
    let username = body.str("username")?.unwrap_or_default();
    let display_name = body.str("displayName")?.unwrap_or_default();
    let region = body.str("region")?.unwrap_or_default();
    let out = ctx.write(Command::Register { username, display_name, region }).await?;
    let Outcome::Register { user, secret_key } = out else { return Err(ApiError::internal_unknown()) };
    ctx.respond(201, |st, w| {
        let u = st.user_idx(user).ok_or_else(ApiError::internal_unknown)?;
        w.obj().k("secretKey").s(&secret_key).k("user");
        views::session_user(w, st.user(u));
        w.end();
        Ok(())
    })
    .await
}

pub fn exec_register(st: &State, tx: &mut Tx, username: &str, display_name: &str, region: &str) -> ExecResult {
    let Some(region) = Region::parse(region) else { return Err(ApiError::bad_request("invalid_registration")) };
    if !text::valid_username(username) || !text::len_between(display_name, 1, 100) {
        return Err(ApiError::bad_request("invalid_registration"));
    }
    if st.user_by_username(username).is_some() {
        return Err(ApiError::conflict("username_taken"));
    }
    let id = tx.new_id();
    let secret_key = tx.new_id().to_string();
    let secret_hash: [u8; 32] = Sha256::digest(secret_key.as_bytes()).into();
    tx.push(LogEntry::UserCreated {
        id,
        username: username.into(),
        secret_hash,
        display_name: display_name.into(),
        region,
        at: tx.now,
    });
    Ok(Outcome::Register { user: id, secret_key })
}

// ---------------------------------------------------------------------------------------------
// POST /api/auth/login — auth none, not pinned. 401 counts as a failed login (§8.1).
// ---------------------------------------------------------------------------------------------

pub async fn login(ctx: Ctx, mut req: Req) -> HResult {
    let body = req.bind().await?;
    let username = body.str("username")?.unwrap_or_default();
    let secret_key = body.str("secretKey")?.unwrap_or_default();
    let out = ctx.write(Command::Login { username, secret_key }).await?;
    let Outcome::Login { user, token } = out else { return Err(ApiError::internal_unknown()) };
    ctx.respond(200, |st, w| {
        let u = st.user_idx(user).ok_or_else(ApiError::internal_unknown)?;
        w.obj().k("token").id(token).k("user");
        views::session_user(w, st.user(u));
        w.end();
        Ok(())
    })
    .await
}

pub fn exec_login(st: &State, tx: &mut Tx, username: &str, secret_key: &str) -> ExecResult {
    let fail = || ApiError::unauthorized("invalid_credentials").abuse(Abuse::FailedLogin);
    if !text::valid_username(username) {
        return Err(fail());
    }
    let u = st.user_by_username(username).ok_or_else(fail)?;
    let usr = st.user(u);
    if !usr.is_active() {
        return Err(fail());
    }
    let h: [u8; 32] = Sha256::digest(secret_key.as_bytes()).into();
    if !sign::ct_eq(&h, &usr.secret_hash) {
        return Err(fail());
    }
    let token = tx.new_id();
    tx.push(LogEntry::SessionCreated { token, user: usr.id, at: tx.now });
    Ok(Outcome::Login { user: usr.id, token })
}

// ---------------------------------------------------------------------------------------------
// POST /api/auth/logout — auth user, not pinned. Invalidates the presented session only.
// ---------------------------------------------------------------------------------------------

pub async fn logout(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_user(&req)?;
    ctx.write(Command::Logout { token: me.token }).await?;
    Ok(Resp::no_content())
}

/// Emits `LogEntry::SessionEnded { token }`. The session must still be active when the leader executes it
/// (a concurrent re-login / account deletion already ended it ⇒ 401, as if the request came after it).
pub fn exec_logout(st: &State, tx: &mut Tx, token: Id) -> ExecResult {
    st.session_user(token).ok_or(ApiError::unauthorized("invalid_session"))?;
    tx.push(LogEntry::SessionEnded { token });
    Ok(Outcome::Unit)
}

/// The acting user of a write must still be active when the leader executes it (it may have been deleted
/// between the handler's auth check and the exec) ⇒ else 401 invalid_session.
pub fn active_actor(st: &State, actor: Id) -> Result<UIdx, ApiError> {
    st.user_idx(actor).filter(|&u| st.user(u).is_active()).ok_or(ApiError::unauthorized("invalid_session"))
}
