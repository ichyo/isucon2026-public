//! Group B: GET /api/memories?month=, GET /api/memories/{date} (§6.8).

use super::feeds::{before_cursor, IdCursor};
use super::prelude::*;

/// Max embedded RealMojis / comments per memory item.
const EMBED_LIMIT: usize = 50;

/// `[from, until)` local-date range.
type Period = (Date, Date);
type Cursor = Option<(Ts, IdCursor)>;

/// Binds `beforePostedAt` / `beforeId` (binding errors first), then validates the period, then the cursor rule.
fn cursor_and_period(req: &Req, period: impl FnOnce() -> Option<Period>) -> Result<(Period, Cursor), ApiError> {
    let before_posted_at = req.query_time("beforePostedAt")?;
    let before_id = req.query_nonempty("beforeId");
    let range = period().ok_or(ApiError::bad_request("invalid_memory_period"))?;
    let cursor = match (before_posted_at, before_id) {
        (None, None) => None,
        (Some(t), Some(id)) => Some((t, IdCursor::parse(&id))),
        _ => return Err(ApiError::bad_request("invalid_pagination")),
    };
    Ok((range, cursor))
}

// GET /api/memories?month=YYYY-MM — auth user, pinned.
pub async fn get_memories(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let month = req.query("month").map(|m| m.into_owned());
    let (range, cursor) = cursor_and_period(&req, || timefmt::parse_month(month.as_deref()?))?;
    respond(ctx, me.idx, range, cursor).await
}

// GET /api/memories/{date} — auth user, pinned.
pub async fn get_memories_by_date(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let date = req.param(0).to_string();
    let (range, cursor) = cursor_and_period(&req, || {
        let d = timefmt::parse_date(&date)?;
        Some((d, Date(d.0 + 1)))
    })?;
    respond(ctx, me.idx, range, cursor).await
}

async fn respond(ctx: Ctx, me: UIdx, (from, until): Period, cursor: Cursor) -> HResult {
    ctx.respond(200, |st, w| {
        let posts = st.user(me).posts.iter().rev().copied().filter(|&p| {
            let post = st.post(p);
            let d = st.post_date(p);
            post.deleted_at.is_none() && d >= from && d < until && before_cursor(&cursor, post.posted_at, post.id)
        });
        let (items, more) = paginate(posts, PAGE_SIZE);
        views::page(w, &items, more, |w, &p| memory_item(w, st, p));
        Ok(())
    })
    .await
}

/// MemoryItem = Post + `realmojis` (current, active reactors, createdAt DESC, ≤50) + `comments` (live, active
/// authors, createdAt ASC, ≤50). No block filtering.
pub fn memory_item(w: &mut JsonOut, st: &State, p: PIdx) {
    views::post(w, st, p, |w| {
        w.k("realmojis").arr();
        let rms = st.post_realmojis.get(p).iter().rev().copied().filter(|&r| {
            let rm = st.realmoji(r);
            rm.is_current() && st.user(rm.reactor).is_active()
        });
        for r in rms.take(EMBED_LIMIT) {
            views::realmoji(w, st, r);
        }
        w.end();
        w.k("comments").arr();
        let cms = st.post_comments.get(p).iter().copied().filter(|&c| {
            let cm = st.comment(c);
            cm.deleted_at.is_none() && st.user(cm.author).is_active()
        });
        for c in cms.take(EMBED_LIMIT) {
            views::comment(w, st, c);
        }
        w.end();
    });
}
