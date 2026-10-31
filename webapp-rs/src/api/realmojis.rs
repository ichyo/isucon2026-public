//! Group C: RealMojis (§6.9). Sending needs V ≠ author, author active and canView (§4.5); listing needs canView.

use std::cmp::Ordering;

use super::comments::{active_actor, bind_cursor};
use super::prelude::*;

/// RealMoji images: width and height ≤ 512 (§5.1).
const MAX_DIM: u16 = 512;

fn forbidden() -> ApiError {
    ApiError::forbidden("interaction_forbidden")
}

fn invalid() -> ApiError {
    ApiError::bad_request("invalid_realmoji")
}

/// Max buffered multipart body for a RealMoji (≤ 512 px image ≤ 4 MiB + form fields).
pub const REALMOJI_BODY_CAP: usize = 4 * 1024 * 1024 + 64 * 1024;

// POST /api/posts/{postId}/realmojis — auth user, pinned, multipart (`image` file, `reactionType`).
// Input checks (400) all come before any state check; the JPEG is fully decoded off-lock before ctx.write.
pub async fn replace_real_moji(ctx: Ctx, mut req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let post = req.param(0).to_string();
    // bounded buffering: over REALMOJI_BODY_CAP ⇒ the same 400 invalid_realmoji the decoder would give
    let Some(body) = req.bind_capped(REALMOJI_BODY_CAP).await? else { return Err(invalid()) };
    let reaction = body.str("reactionType")?; // binding error (wrong JSON type) ⇒ 400 first
    let image = body.file("image").ok_or_else(invalid)?.data.clone();
    drop(body);
    let reaction = reaction.as_deref().and_then(Reaction::parse).ok_or_else(invalid)?;
    let dims = jpeg::decode(image.clone()).await.ok_or_else(invalid)?;
    if dims.width > MAX_DIM || dims.height > MAX_DIM {
        return Err(invalid());
    }
    let out = ctx.write(Command::PutRealMoji { actor: me.id, post, reaction, image: image.into() }).await?;
    ctx.respond(201, |st, w| {
        let r = st.realmoji_idx(out.id()).ok_or_else(ApiError::internal_unknown)?;
        views::realmoji(w, st, r);
        Ok(())
    })
    .await
}

/// Emits `tx.push_blob(LogEntry::RealMojiCreated { .. }, image.clone())` (apply marks the previous one replaced).
/// Outcome::Id(realmoji id).
pub fn exec_put_realmoji(st: &State, tx: &mut Tx, actor: Id, post: &str, reaction: Reaction, image: &Blob) -> ExecResult {
    let a = active_actor(st, actor)?;
    // 2. post missing/deleted ⇒ 403; actor is the author ⇒ 403
    let p = st.post_idx_str(post).filter(|&p| st.post(p).deleted_at.is_none()).ok_or_else(forbidden)?;
    let pp = st.post(p);
    if pp.author == a {
        return Err(forbidden());
    }
    // 3. author deleted ⇒ 404
    if !st.user(pp.author).is_active() {
        return Err(ApiError::not_found("relationship_not_found"));
    }
    // 4. !canView ⇒ 403 (window ended, viewer has not posted today, blocked, visibility)
    if !rules::can_view(st, &rules::viewer(st, a), p) {
        return Err(forbidden());
    }
    // 5. replace: apply marks the actor's current RealMoji on this post replaced (its image goes dead)
    let id = tx.new_id();
    let img = tx.new_id();
    tx.push_blob(LogEntry::RealMojiCreated { id, post: pp.id, reactor: actor, image: img, reaction, at: tx.now }, image.clone());
    Ok(Outcome::Id(id))
}

// GET /api/posts/{postId}/realmojis[?beforeCreatedAt=&beforeId=] — auth user, pinned.
// Order: createdAt DESC, id DESC; cursor (createdAt, id) < cursor; current RealMojis by active reactors.
pub async fn get_real_mojis(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let cursor = bind_cursor(&req, "beforeCreatedAt", "beforeId")?;
    let post = req.param(0);
    ctx.respond(200, |st, w| {
        let p = st.post_idx_str(post).ok_or_else(forbidden)?;
        if !rules::can_view(st, &rules::viewer(st, me.idx), p) {
            return Err(forbidden());
        }
        let list = st.post_realmojis.get(p); // sorted (created_at, id) asc
        let end = match &cursor {
            None => list.len(),
            Some(c) => list.partition_point(|&i| {
                let rm = st.realmoji(i);
                c.cmp_key(rm.created_at, rm.id) == Ordering::Less
            }),
        };
        let live = list[..end].iter().rev().copied().filter(|&i| {
            let rm = st.realmoji(i);
            rm.is_current() && st.user(rm.reactor).is_active()
        });
        let (items, more) = paginate(live, PAGE_SIZE);
        views::page(w, &items, more, |w, &r| views::realmoji(w, st, r));
        Ok(())
    })
    .await
}

// DELETE /api/realmojis/{realMojiId} — auth user, pinned.
pub async fn delete_real_moji(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let realmoji = req.param(0).to_string();
    ctx.write(Command::DeleteRealMoji { actor: me.id, realmoji }).await?;
    Ok(Resp::no_content())
}

/// Emits `RealMojiDeleted`. A replaced (but not deleted) RealMoji can still be deleted.
pub fn exec_delete_realmoji(st: &State, tx: &mut Tx, actor: Id, realmoji: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let r = st
        .realmoji_idx_str(realmoji)
        .filter(|&r| st.realmoji(r).deleted_at.is_none())
        .ok_or_else(|| ApiError::not_found("interaction_not_found"))?;
    let rm = st.realmoji(r);
    if rm.reactor != a && st.post(rm.post).author != a {
        return Err(forbidden());
    }
    tx.push(LogEntry::RealMojiDeleted { id: rm.id, at: tx.now });
    Ok(Outcome::Unit)
}
