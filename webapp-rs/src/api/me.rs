//! Group A: /api/me (GET implemented by core), PATCH, profile image, account deletion (§6.3).

use super::auth::active_actor;
use super::prelude::*;

/// Profile images: width and height ≤ 512 (§5.1).
const PROFILE_IMAGE_MAX: u16 = 512;

// GET /api/me — auth user, pinned
pub async fn get_me(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    ctx.respond(200, |st, w| {
        views::me(w, st.user(me.idx));
        Ok(())
    })
    .await
}

// PATCH /api/me — auth user, NOT pinned. Body JSON {displayName?, bio?, region?}; 200 Me (after update).
pub async fn update_me(ctx: Ctx, mut req: Req) -> HResult {
    let me = ctx.auth_user(&req).await?;
    let body = req.bind().await?;
    // binding first (a wrong JSON type in any field ⇒ 400 bind error), then validation
    let display_name = body.str("displayName")?;
    let bio = body.str("bio")?;
    let region = body.str("region")?;
    let invalid = || ApiError::bad_request("invalid_profile");
    if display_name.is_none() && bio.is_none() && region.is_none() {
        return Err(invalid());
    }
    if display_name.as_deref().is_some_and(|d| !text::len_between(d, 1, 100)) {
        return Err(invalid());
    }
    if bio.as_deref().is_some_and(|b| !text::len_between(b, 0, 500)) {
        return Err(invalid());
    }
    let region = match region {
        None => None,
        Some(r) => Some(Region::parse(&r).ok_or_else(invalid)?),
    };
    ctx.write(Command::UpdateProfile { user: me.id, display_name, bio, region }).await?;
    ctx.respond(200, |st, w| {
        views::me(w, st.user(me.idx));
        Ok(())
    })
    .await
}

/// Emits `LogEntry::ProfileUpdated` (apply closes SSE streams on region change). Inputs are validated by
/// the handler; a region change takes effect for the very next request (pinning reads `User.region`).
pub fn exec_update_profile(st: &State, tx: &mut Tx, user: Id, display_name: Option<&str>, bio: Option<&str>, region: Option<Region>) -> ExecResult {
    active_actor(st, user)?;
    tx.push(LogEntry::ProfileUpdated { user, display_name: display_name.map(Into::into), bio: bio.map(Into::into), region, at: tx.now });
    Ok(Outcome::Unit)
}

/// Max buffered profile-image body.
pub const PROFILE_IMAGE_BODY_CAP: usize = 4 * 1024 * 1024;

// PUT /api/me/profile-image — auth user, not pinned. Content-Type must be exactly `image/jpeg` (415), raw
// body = JPEG: undecodable ⇒ 400 + abuse InvalidImage; > 512 px ⇒ 400 (not abuse). 204.
pub async fn put_profile_image(ctx: Ctx, mut req: Req) -> HResult {
    let me = ctx.auth_user(&req).await?;
    if req.header("content-type") != Some("image/jpeg") {
        return Err(ApiError::new(415, "unsupported_media_type"));
    }
    let invalid = || ApiError::bad_request("invalid_profile_image");
    // bounded buffering: not starting with a JPEG SOI + marker (FF D8 FF), or over 4 MiB (a ≤ 512 px JPEG is far
    // smaller) ⇒ undecodable: 400 + abuse, the rest of the body is drained without being stored
    let image = match req.body_capped(PROFILE_IMAGE_BODY_CAP, Some(&[0xFF, 0xD8, 0xFF])).await? {
        crate::http::req::Capped::Body(b) => b,
        crate::http::req::Capped::Rejected => return Err(invalid().abuse(Abuse::InvalidImage)),
    };
    // full decode in the blocking pool, before (and outside) the write path
    let dims = jpeg::decode(image.clone()).await.ok_or_else(|| invalid().abuse(Abuse::InvalidImage))?;
    if dims.width > PROFILE_IMAGE_MAX || dims.height > PROFILE_IMAGE_MAX {
        return Err(invalid());
    }
    ctx.write(Command::SetProfileImage { user: me.id, image: image.into() }).await?;
    Ok(Resp::no_content())
}

/// Emits `tx.push_blob(LogEntry::ProfileImageSet { image: Some(new_id), .. }, image.clone())`; apply
/// hard-deletes the previous profile image.
pub fn exec_set_profile_image(st: &State, tx: &mut Tx, user: Id, image: &Blob) -> ExecResult {
    active_actor(st, user)?;
    let id = tx.new_id();
    tx.push_blob(LogEntry::ProfileImageSet { user, image: Some(id), at: tx.now }, image.clone());
    Ok(Outcome::Id(id))
}

// DELETE /api/me/profile-image — auth user, not pinned. No profile image ⇒ 404 account_not_found. 204.
pub async fn delete_profile_image(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_user(&req).await?;
    ctx.write(Command::DeleteProfileImage { user: me.id }).await?;
    Ok(Resp::no_content())
}

/// Emits `LogEntry::ProfileImageSet { image: None, .. }` (apply hard-deletes the image).
pub fn exec_delete_profile_image(st: &State, tx: &mut Tx, user: Id) -> ExecResult {
    let u = active_actor(st, user)?;
    if st.user(u).profile_image.is_none() {
        return Err(ApiError::not_found("account_not_found"));
    }
    tx.push(LogEntry::ProfileImageSet { user, image: None, at: tx.now });
    Ok(Outcome::Unit)
}

// POST /api/me/account-deletion — auth user, pinned. 204 (§4.7; all sessions dropped by apply).
pub async fn delete_account(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    ctx.write(Command::DeleteAccount { user: me.id }).await?;
    Ok(Resp::no_content())
}

/// Emits `LogEntry::AccountDeleted`.
pub fn exec_delete_account(st: &State, tx: &mut Tx, user: Id) -> ExecResult {
    active_actor(st, user)?;
    tx.push(LogEntry::AccountDeleted { user, at: tx.now });
    Ok(Outcome::Unit)
}
