//! Group B: posts (§6.6).

use super::prelude::*;

/// The acting user, re-checked on the leader (deleted between auth and exec ⇒ 401).
fn actor_idx(st: &State, actor: Id) -> Result<UIdx, ApiError> {
    st.user_idx(actor).filter(|&u| st.user(u).is_active()).ok_or(ApiError::unauthorized("invalid_session"))
}

/// Validates the location fields of POST /api/posts (§6.6 step 4).
fn parse_location(kind: Option<&str>, lat: Option<f64>, lng: Option<f64>, locality: Option<String>) -> Option<Location> {
    match kind? {
        "none" if lat.is_none() && lng.is_none() && locality.is_none() => Some(Location::None),
        "exact" => {
            let (lat, lng) = (lat?, lng?);
            if locality.is_some() || !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lng) {
                return None;
            }
            Some(Location::Exact { lat, lng })
        }
        "approximate" if lat.is_none() && lng.is_none() => {
            let l = locality?;
            text::len_between(&l, 1, 100).then(|| Location::Approximate { locality: l.into() })
        }
        _ => None,
    }
}

/// Max buffered multipart body for a post (image part ≤ 16 MiB + form fields).
pub const POST_BODY_CAP: usize = 16 * 1024 * 1024 + 64 * 1024;

// POST /api/posts — auth user, pinned, multipart. The JPEG is decoded BEFORE ctx.write.
pub async fn create_post(ctx: Ctx, mut req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    // bounded buffering: a body over POST_BODY_CAP cannot hold a ≤ 4096 px JPEG we accept ⇒ invalid_post
    // (every check that could come first answers the same 400 invalid_post); the rest is drained unstored
    let Some(body) = req.bind_capped(POST_BODY_CAP).await? else { return Err(ApiError::bad_request("invalid_post")) };
    // 1. binding errors (wrong JSON types, unparsable floats)
    let caption = body.str("caption")?.unwrap_or_default();
    let visibility = body.str("visibility")?;
    let kind = body.str("location_kind")?;
    let lat = body.float("latitude")?;
    let lng = body.float("longitude")?;
    let locality = body.str("locality")?;
    let invalid = || ApiError::bad_request("invalid_post");
    // 2. image part + caption
    let image = body.file("image").map(|f| f.data.clone()).ok_or_else(invalid)?;
    if !text::len_between(&caption, 0, 1000) {
        return Err(invalid());
    }
    // 3. visibility
    let visibility = visibility.as_deref().and_then(Visibility::parse).ok_or_else(invalid)?;
    // 4. location
    let location = parse_location(kind.as_deref(), lat, lng, locality).ok_or_else(invalid)?;
    drop(body);
    // 5. full JPEG decode (blocking pool), ≤ 4096×4096
    match jpeg::decode(image.clone(), jpeg::Lane::Post).await {
        Some(d) if d.width <= 4096 && d.height <= 4096 => {}
        _ => return Err(invalid()),
    }
    // 6.–8. window / slot rules + creation (atomic on the leader)
    let out = ctx.write(Command::CreatePost { actor: me.id, image: image.into(), caption, visibility, location }).await?;
    ctx.respond(201, |st, w| {
        let p = st.post_idx(out.id()).ok_or_else(ApiError::internal_unknown)?;
        views::post(w, st, p, |_| {});
        Ok(())
    })
    .await
}

/// Emits `tx.push_blob(LogEntry::PostCreated { .. }, image.clone())`. Outcome::Id(post id).
pub fn exec_create_post(st: &State, tx: &mut Tx, actor: Id, image: &Blob, caption: &str, visibility: Visibility, location: &Location) -> ExecResult {
    let u = actor_idx(st, actor)?;
    let unavailable = || ApiError::conflict("post_slot_unavailable");
    let w = st.open_window_of(st.user(u).region).ok_or_else(unavailable)?;
    let win = st.window(w);
    let (active, deleted) = rules::slot_counts(st, u, win.local_date);
    if active > 0 || deleted >= 2 || tx.now < win.starts_at {
        return Err(unavailable());
    }
    let id = tx.new_id();
    let image_id = tx.new_id();
    tx.push_blob(
        LogEntry::PostCreated {
            id,
            author: actor,
            window: win.id,
            image: image_id,
            caption: caption.into(),
            visibility,
            location: location.clone(),
            at: tx.now,
        },
        image.clone(),
    );
    Ok(Outcome::Id(id))
}

// GET /api/posts/me/current — auth user, pinned.
pub async fn get_my_current_post(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    ctx.respond(200, |st, w| {
        let not_found = || ApiError::not_found("post_not_found");
        let c = st.open_window_of(st.user(me.idx).region).ok_or_else(not_found)?;
        let date = st.window(c).local_date;
        let p = st
            .user(me.idx)
            .posts
            .iter()
            .rev()
            .copied()
            .find(|&p| st.post(p).deleted_at.is_none() && st.post_date(p) == date)
            .ok_or_else(not_found)?;
        views::post(w, st, p, |_| {});
        Ok(())
    })
    .await
}

// GET /api/posts/{postId} — auth user, pinned.
pub async fn get_post(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let post_id = req.param(0);
    ctx.respond(200, |st, w| {
        let v = rules::viewer(st, me.idx);
        let p = st.post_idx_str(post_id).filter(|&p| rules::can_view(st, &v, p)).ok_or(ApiError::not_found("post_not_found"))?;
        views::post(w, st, p, |_| {});
        Ok(())
    })
    .await
}

// PATCH /api/posts/{postId} — auth user, pinned.
pub async fn update_post(ctx: Ctx, mut req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let post = req.param(0).to_string();
    let body = req.bind().await?;
    let caption = body.str("caption")?;
    let visibility = body.str("visibility")?;
    let invalid = || ApiError::bad_request("invalid_post");
    if caption.is_none() && visibility.is_none() {
        return Err(invalid());
    }
    if caption.as_deref().is_some_and(|c| !text::len_between(c, 0, 1000)) {
        return Err(invalid());
    }
    let visibility = match visibility.as_deref() {
        None => None,
        Some(v) => Some(Visibility::parse(v).ok_or_else(invalid)?),
    };
    let out = ctx.write(Command::UpdatePost { actor: me.id, post, caption, visibility }).await?;
    ctx.respond(200, |st, w| {
        let p = st.post_idx(out.id()).ok_or_else(ApiError::internal_unknown)?;
        views::post(w, st, p, |_| {});
        Ok(())
    })
    .await
}

/// Emits `PostUpdated`. Outcome::Id(post id).
pub fn exec_update_post(st: &State, tx: &mut Tx, actor: Id, post: &str, caption: Option<&str>, visibility: Option<Visibility>) -> ExecResult {
    let u = actor_idx(st, actor)?;
    let p = st.post_idx_str(post).filter(|&p| st.post(p).deleted_at.is_none()).ok_or(ApiError::not_found("post_not_found"))?;
    let pp = st.post(p);
    if pp.author != u || !rules::window_open(st, pp.window) {
        return Err(ApiError::forbidden("post_not_editable"));
    }
    tx.push(LogEntry::PostUpdated { id: pp.id, caption: caption.map(Into::into), visibility, at: tx.now });
    Ok(Outcome::Id(pp.id))
}

// DELETE /api/posts/{postId} — auth user, pinned.
pub async fn delete_post(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let post = req.param(0).to_string();
    ctx.write(Command::DeletePost { actor: me.id, post }).await?;
    Ok(Resp::no_content())
}

/// Emits `PostDeleted`.
pub fn exec_delete_post(st: &State, tx: &mut Tx, actor: Id, post: &str) -> ExecResult {
    let u = actor_idx(st, actor)?;
    let p = st.post_idx_str(post).filter(|&p| st.post(p).deleted_at.is_none()).ok_or(ApiError::not_found("post_not_found"))?;
    let pp = st.post(p);
    if pp.author != u {
        return Err(ApiError::forbidden("post_not_editable"));
    }
    let (_, deleted) = rules::slot_counts(st, u, st.post_date(p));
    if deleted >= 2 {
        return Err(ApiError::conflict("post_slot_unavailable"));
    }
    tx.push(LogEntry::PostDeleted { id: pp.id, at: tx.now });
    Ok(Outcome::Unit)
}

#[cfg(test)]
mod tests {
    use super::super::feeds::testutil::*;
    use super::*;

    const T0: Ts = 1_793_000_000_000_000;

    fn code<T: std::fmt::Debug>(r: Result<T, ApiError>) -> (u16, String) {
        let e = r.expect_err("expected an error");
        (e.status, e.code.to_string())
    }

    fn slot() -> (u16, String) {
        (409, "post_slot_unavailable".into())
    }

    #[test]
    fn slot_rules_per_author_and_date_across_regions() {
        let mut st = state();
        let alice = user(&st, "alice");
        let aid = st.user(alice).id;
        assert_eq!(code(post(&mut st, T0, alice, Visibility::Public)), slot()); // no open window
        open(&mut st, T0, Region::Asia, "2026-10-31");
        open(&mut st, T0, Region::Europe, "2026-10-31");
        let p1 = post(&mut st, T0 + 1, alice, Visibility::Public).unwrap();
        assert_eq!(code(post(&mut st, T0 + 2, alice, Visibility::Public)), slot());
        // alice moves to europe: the same localDate in another region's window still counts
        apply_entry(&mut st, LogEntry::ProfileUpdated { user: aid, display_name: None, bio: None, region: Some(Region::Europe), at: T0 + 3 });
        assert!(rules::has_posted_today(&st, alice));
        assert_eq!(code(post(&mut st, T0 + 4, alice, Visibility::Public)), slot());
        // delete #1 ⇒ may post again (now in the europe window)
        let p1id = st.post(p1).id.to_string();
        run(&mut st, T0 + 5, Command::DeletePost { actor: aid, post: p1id.clone() }).unwrap();
        assert_eq!(code(run(&mut st, T0 + 6, Command::DeletePost { actor: aid, post: p1id })), (404, "post_not_found".into()));
        let p2 = post(&mut st, T0 + 7, alice, Visibility::Public).unwrap();
        assert_eq!(st.window(st.post(p2).window).region, Region::Europe);
        // delete #2 ⇒ no more posts for that date
        let id = st.post(p2).id.to_string();
        run(&mut st, T0 + 8, Command::DeletePost { actor: aid, post: id }).unwrap();
        assert_eq!(rules::slot_counts(&st, alice, st.post_date(p2)), (0, 2));
        assert_eq!(code(post(&mut st, T0 + 9, alice, Visibility::Public)), slot());
        // before the window start ⇒ refused
        let bob = user(&st, "bob");
        assert_eq!(code(post(&mut st, T0 - 1, bob, Visibility::Public)), slot());
        assert!(post(&mut st, T0, bob, Visibility::Public).is_ok());
    }

    #[test]
    fn delete_rules() {
        let mut st = state();
        let u = user(&st, "carol");
        let uid = st.user(u).id;
        let other = st.user(user(&st, "bob")).id;
        open(&mut st, T0, Region::Asia, "2026-10-31");
        let p = post(&mut st, T0 + 1, u, Visibility::Public).unwrap();
        let pid = st.post(p).id.to_string();
        assert_eq!(code(run(&mut st, T0 + 2, Command::DeletePost { actor: other, post: pid.clone() })), (403, "post_not_editable".into()));
        run(&mut st, T0 + 3, Command::DeletePost { actor: uid, post: pid }).unwrap();
        let p = post(&mut st, T0 + 4, u, Visibility::Public).unwrap();
        let id = st.post(p).id.to_string();
        run(&mut st, T0 + 5, Command::DeletePost { actor: uid, post: id }).unwrap();
        // deleting an ended-window post is allowed (its own date has < 2 deleted posts)
        let old = st.user(u).posts.iter().copied().find(|&p| st.post(p).deleted_at.is_none() && !rules::window_open(&st, st.post(p).window)).unwrap();
        let id = st.post(old).id.to_string();
        run(&mut st, T0 + 6, Command::DeletePost { actor: uid, post: id }).unwrap();
        assert_eq!(code(run(&mut st, T0 + 7, Command::DeletePost { actor: uid, post: "nope".into() })), (404, "post_not_found".into()));
    }

    #[test]
    fn update_rules() {
        let mut st = state();
        let alice = user(&st, "alice");
        let bob = user(&st, "bob");
        let (aid, bid) = (st.user(alice).id, st.user(bob).id);
        open(&mut st, T0, Region::Asia, "2026-10-31");
        let p = post(&mut st, T0 + 1, alice, Visibility::Public).unwrap();
        let pid = st.post(p).id.to_string();
        let upd = |actor, post: &str| Command::UpdatePost { actor, post: post.into(), caption: Some("c".into()), visibility: Some(Visibility::Friends) };
        assert_eq!(code(run(&mut st, T0 + 2, upd(bid, &pid))), (403, "post_not_editable".into()));
        assert_eq!(code(run(&mut st, T0 + 2, upd(aid, &Id::new_v4().to_string()))), (404, "post_not_found".into()));
        run(&mut st, T0 + 2, upd(aid, &pid)).unwrap();
        assert_eq!((&*st.post(p).caption, st.post(p).visibility), ("c", Visibility::Friends));
        open(&mut st, T0 + 3, Region::Asia, "2026-11-01");
        assert_eq!(code(run(&mut st, T0 + 4, upd(aid, &pid))), (403, "post_not_editable".into()));
        run(&mut st, T0 + 5, Command::DeletePost { actor: aid, post: pid.clone() }).unwrap();
        assert_eq!(code(run(&mut st, T0 + 6, upd(aid, &pid))), (404, "post_not_found".into()));
    }

    #[test]
    fn open_window_conflicts_and_deadline() {
        let mut st = state();
        let d = |s: &str| timefmt::parse_date(s).unwrap();
        let ow = |r, date, secs| Command::OpenWindow { region: r, local_date: d(date), on_time_seconds: secs };
        // initial windows go up to 2026-10-27
        assert_eq!(code(run(&mut st, T0, ow(Region::Asia, "2026-10-27", 1))), (409, "posting_window_conflict".into()));
        let w = run(&mut st, T0, ow(Region::Asia, "2026-10-28", -5)).unwrap().id();
        let win = st.window(st.window_idx(w).unwrap()).clone();
        assert_eq!((win.starts_at, win.on_time_deadline, win.is_open()), (T0, T0 - 5_000_000, true));
        assert_eq!(code(run(&mut st, T0, ow(Region::Asia, "2026-10-28", 1))), (409, "posting_window_conflict".into()));
        // other regions are independent; the deadline saturates
        run(&mut st, T0, ow(Region::Europe, "2026-10-28", i64::MAX)).unwrap();
        assert_eq!(st.window(st.open_window_of(Region::Europe).unwrap()).on_time_deadline, i64::MAX);
        // the next asia window ends the previous one at its start
        let w2 = run(&mut st, T0 + 7, ow(Region::Asia, "2026-10-30", 0)).unwrap().id();
        assert_eq!(st.window(st.window_idx(w).unwrap()).ended_at.get(), Some(T0 + 7));
        assert_eq!(st.open_window_of(Region::Asia), st.window_idx(w2));
    }
}
