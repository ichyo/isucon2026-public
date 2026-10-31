//! Group B: GET /api/posting-window/current, POST /api/admin/posting-window (§6.5, §6.11).

use super::prelude::*;

// GET /api/posting-window/current — auth user, pinned.
pub async fn get_current_posting_window(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req)?;
    let now = now_us();
    ctx.respond(200, |st, w| {
        let region = st.user(me.idx).region;
        let Some(c) = st.open_window_of(region) else {
            w.obj().k("window").null().k("hasPosted").b(false).k("canPost").b(false).k("deletionCount").i(0).end();
            return Ok(());
        };
        let win = st.window(c);
        let (active, deleted) = rules::slot_counts(st, me.idx, win.local_date);
        w.obj().k("window");
        views::posting_window(w, win);
        w.k("hasPosted").b(active > 0);
        w.k("canPost").b(active == 0 && deleted < 2 && now >= win.starts_at);
        w.k("deletionCount").i(deleted as i64);
        w.end();
        Ok(())
    })
    .await
}

// POST /api/admin/posting-window — admin.
pub async fn open_posting_window(ctx: Ctx, mut req: Req) -> HResult {
    ctx.auth_admin(&req)?;
    let body = req.bind().await?;
    // binding (wrong JSON types ⇒ 400 bind error) before validation
    let region = body.str("region")?;
    let local_date = body.str("localDate")?;
    let on_time_seconds = body.int("onTimeSeconds")?.unwrap_or(86400);
    let invalid = || ApiError::bad_request("invalid_posting_window");
    let region = region.as_deref().and_then(Region::parse).ok_or_else(invalid)?;
    let local_date = local_date.as_deref().and_then(timefmt::parse_date).ok_or_else(invalid)?;
    let out = ctx.write(Command::OpenWindow { region, local_date, on_time_seconds }).await?;
    ctx.respond(201, |st, w| {
        let win = st.window_idx(out.id()).ok_or_else(ApiError::internal_unknown)?;
        views::posting_window(w, st.window(win));
        Ok(())
    })
    .await
}

/// Emits `WindowOpened { starts_at: tx.now, on_time_deadline: tx.now + secs·1e6 (saturating) }` (apply ends the
/// previous window; SSE fan-out via effects). Conflict check: `st.last_date[region]`. Outcome::Id(window id).
pub fn exec_open_window(st: &State, tx: &mut Tx, region: Region, local_date: Date, on_time_seconds: i64) -> ExecResult {
    if st.last_date[region.idx()].is_some_and(|d| d >= local_date) {
        return Err(ApiError::conflict("posting_window_conflict"));
    }
    let id = tx.new_id();
    let starts_at = tx.now;
    let on_time_deadline = starts_at.saturating_add(on_time_seconds.saturating_mul(1_000_000));
    tx.push(LogEntry::WindowOpened { id, region, local_date, starts_at, on_time_deadline });
    Ok(Outcome::Id(id))
}
