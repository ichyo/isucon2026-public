//! Group C: comments (§6.9). Permission = `rules::can_comment` (§4.4), which gates both creating and listing.
//! Also hosts the `(timestamp, id)` cursor helper shared with `realmojis.rs`.

use std::cmp::Ordering;

use super::prelude::*;

fn forbidden() -> ApiError {
    ApiError::forbidden("interaction_forbidden")
}

/// The actor of a command must still be an active user when the leader executes it (the session was checked
/// by the handler; a concurrent account deletion invalidated it in between).
pub(crate) fn active_actor(st: &State, actor: Id) -> Result<UIdx, ApiError> {
    st.user_idx(actor).filter(|&u| st.user(u).is_active()).ok_or_else(|| ApiError::unauthorized("invalid_session"))
}

/// The id half of a list cursor (`afterId` / `beforeId`). IDs compare as the byte strings of their 36-char
/// lowercase form (§2.7) — which is exactly `Id`'s `Ord`; a cursor that is not a UUID is compared bytewise.
pub(crate) enum IdCursor {
    Id(Id),
    Raw(Box<[u8]>),
}

impl IdCursor {
    fn parse(s: &str) -> IdCursor {
        match Id::parse(s) {
            Some(id) => IdCursor::Id(id),
            None => IdCursor::Raw(s.as_bytes().into()),
        }
    }
    /// `id` compared to the cursor id.
    #[inline]
    fn cmp_id(&self, id: Id) -> Ordering {
        match self {
            IdCursor::Id(c) => id.cmp(c),
            IdCursor::Raw(c) => id.to_buf()[..].cmp(&c[..]),
        }
    }
}

/// A `(createdAt, id)` cursor.
pub(crate) struct Cursor {
    pub ts: Ts,
    pub id: IdCursor,
}

impl Cursor {
    /// Sort key `(ts, id)` of an item compared to the cursor.
    #[inline]
    pub fn cmp_key(&self, ts: Ts, id: Id) -> Ordering {
        ts.cmp(&self.ts).then_with(|| self.id.cmp_id(id))
    }
}

/// Binds a `(timestamp, id)` cursor from the query (§2.5/§2.7): the timestamp binds first (empty or garbage ⇒
/// 400 bind error), an empty id counts as absent; exactly one of the two given ⇒ 400 invalid_pagination.
pub(crate) fn bind_cursor(req: &Req, ts_key: &str, id_key: &str) -> Result<Option<Cursor>, ApiError> {
    let ts = req.query_time(ts_key)?;
    let id = req.query_nonempty(id_key);
    match (ts, id) {
        (None, None) => Ok(None),
        (Some(ts), Some(id)) => Ok(Some(Cursor { ts, id: IdCursor::parse(&id) })),
        _ => Err(ApiError::bad_request("invalid_pagination")),
    }
}

// POST /api/posts/{postId}/comments — auth user, pinned.
pub async fn create_comment(ctx: Ctx, mut req: Req) -> HResult {
    let me = ctx.auth_pinned(&req)?;
    let post = req.param(0).to_string();
    let body = req.bind().await?;
    let text = body.str("body")?.unwrap_or_default();
    if !text::len_between(&text, 1, 1000) {
        return Err(ApiError::bad_request("invalid_interaction"));
    }
    let out = ctx.write(Command::CreateComment { actor: me.id, post, body: text }).await?;
    ctx.respond(201, |st, w| {
        let c = st.comment_idx(out.id()).ok_or_else(ApiError::internal_unknown)?;
        views::comment(w, st, c);
        Ok(())
    })
    .await
}

/// Emits `CommentCreated`. Outcome::Id(comment id).
pub fn exec_create_comment(st: &State, tx: &mut Tx, actor: Id, post: &str, body: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    if !text::len_between(body, 1, 1000) {
        return Err(ApiError::bad_request("invalid_interaction"));
    }
    // missing / deleted post, author deleted, !canView, window ended, … ⇒ 403 (all inside can_comment)
    let p = st.post_idx_str(post).ok_or_else(forbidden)?;
    if !rules::can_comment(st, &rules::viewer(st, a), p) {
        return Err(forbidden());
    }
    let id = tx.new_id();
    tx.push(LogEntry::CommentCreated { id, post: st.post(p).id, author: actor, body: body.into(), at: tx.now });
    Ok(Outcome::Id(id))
}

// GET /api/posts/{postId}/comments[?afterCreatedAt=&afterId=] — auth user, pinned.
// Order: createdAt ASC, id ASC; cursor (createdAt, id) > cursor; non-deleted comments by active authors.
pub async fn get_comments(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req)?;
    let cursor = bind_cursor(&req, "afterCreatedAt", "afterId")?;
    let post = req.param(0);
    ctx.respond(200, |st, w| {
        let p = st.post_idx_str(post).ok_or_else(forbidden)?;
        if !rules::can_comment(st, &rules::viewer(st, me.idx), p) {
            return Err(forbidden());
        }
        let list = st.post_comments.get(p); // sorted (created_at, id) asc
        let start = match &cursor {
            None => 0,
            Some(c) => list.partition_point(|&i| {
                let cm = st.comment(i);
                c.cmp_key(cm.created_at, cm.id) != Ordering::Greater
            }),
        };
        let live = list[start..].iter().copied().filter(|&i| {
            let cm = st.comment(i);
            cm.deleted_at.is_none() && st.user(cm.author).is_active()
        });
        let (items, more) = paginate(live, PAGE_SIZE);
        views::page(w, &items, more, |w, &c| views::comment(w, st, c));
        Ok(())
    })
    .await
}

// DELETE /api/comments/{commentId} — auth user, pinned.
pub async fn delete_comment(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req)?;
    let comment = req.param(0).to_string();
    ctx.write(Command::DeleteComment { actor: me.id, comment }).await?;
    Ok(Resp::no_content())
}

/// Emits `CommentDeleted`. No visibility/window checks (works on deleted posts too).
pub fn exec_delete_comment(st: &State, tx: &mut Tx, actor: Id, comment: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let c = st
        .comment_idx_str(comment)
        .filter(|&c| st.comment(c).deleted_at.is_none())
        .ok_or_else(|| ApiError::not_found("interaction_not_found"))?;
    let cm = st.comment(c);
    if cm.author != a && st.post(cm.post).author != a {
        return Err(forbidden());
    }
    tx.push(LogEntry::CommentDeleted { id: cm.id, at: tx.now });
    Ok(Outcome::Unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_order() {
        let a = Id::parse("0a000000-0000-4000-8000-000000000000").unwrap();
        let b = Id::parse("a0000000-0000-4000-8000-000000000000").unwrap();
        let c = Cursor { ts: 10, id: IdCursor::parse("0A000000-0000-4000-8000-000000000000") };
        assert_eq!(c.cmp_key(10, a), Ordering::Equal);
        assert_eq!(c.cmp_key(10, b), Ordering::Greater);
        assert_eq!(c.cmp_key(9, b), Ordering::Less);
        assert_eq!(c.cmp_key(11, a), Ordering::Greater);
        // non-UUID cursors compare bytewise against the lowercase id string
        let r = Cursor { ts: 10, id: IdCursor::parse("1") };
        assert_eq!(r.cmp_key(10, a), Ordering::Less); // "0a…" < "1"
        assert_eq!(r.cmp_key(10, b), Ordering::Greater); // "a0…" > "1"
        let r = Cursor { ts: 10, id: IdCursor::parse("0a000000-0000-4000-8000-000000000000x") };
        assert_eq!(r.cmp_key(10, a), Ordering::Less); // prefix
    }
}
