//! Group A: GET /api/users?q=, GET /api/users/{userId} (§6.4). Read-only.

use super::prelude::*;

#[inline]
fn certified(u: &User) -> bool {
    u.account_type == AccountType::CertifiedPublic
}

// GET /api/users?q=&beforeUsername=&beforeId= — auth user, pinned.
// Matching: N(username) or N(displayName) contains N(q) (N = NFC∘casefold∘NFC, pre-folded per user).
// Candidates: active users except the viewer and anyone blocked either way, (username, id) > cursor,
// ordered by (username, id) asc. Bio only for certified accounts.
pub async fn search_users(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req)?;
    let q = req.query("q").map(|v| v.into_owned()).unwrap_or_default();
    if !text::len_between(&q, 1, 100) {
        return Err(ApiError::bad_request("invalid_relationship"));
    }
    let before_name = req.query_nonempty("beforeUsername");
    let before_id = req.query_nonempty("beforeId");
    if before_name.is_some() != before_id.is_some() {
        return Err(ApiError::bad_request("invalid_pagination"));
    }
    let needle = text::fold(&q); // off-lock
    ctx.respond(200, |st, w| {
        // first position strictly after the cursor (users_sorted is ordered by (username, id); Id order ==
        // byte order of the lowercase string, so comparing against the raw cursor string is monotone)
        let start = match (&before_name, &before_id) {
            (Some(bn), Some(bi)) => st.users_sorted.partition_point(|&x| {
                let u = st.user(x);
                let id = u.id.to_buf();
                (u.username.as_bytes(), &id[..]) <= (bn.as_bytes(), bi.as_bytes())
            }),
            _ => 0,
        };
        let viewer = st.user(me.idx);
        let no_blocks = viewer.blocking.is_empty() && viewer.blocked_by.is_empty();
        let hits = st.users_sorted[start..].iter().copied().filter(|&x| {
            let u = st.user(x);
            u.is_active()
                && (u.fold_username.contains(needle.as_str()) || u.fold_display.contains(needle.as_str()))
                && x != me.idx
                && (no_blocks || !rules::blocked(st, me.idx, x))
        });
        let (items, more) = paginate(hits, PAGE_SIZE);
        views::page(w, &items, more, |w, &x| {
            let u = st.user(x);
            views::user_summary(w, u, certified(u));
        });
        Ok(())
    })
    .await
}

// GET /api/users/{userId} — auth user, pinned. Missing / deleted / blocked either way ⇒ 404
// relationship_not_found. Bio iff the target is certified or a friend of the viewer (§4.6).
pub async fn get_user(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req)?;
    let target = req.param(0);
    ctx.respond(200, |st, w| {
        let t = st
            .active_user_str(target)
            .filter(|&t| !rules::blocked(st, me.idx, t))
            .ok_or(ApiError::not_found("relationship_not_found"))?;
        let u = st.user(t);
        views::user_summary(w, u, certified(u) || rules::is_friend(st, me.idx, t));
        Ok(())
    })
    .await
}
