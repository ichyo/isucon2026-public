//! Group B: GET /api/feeds/{feed} (§6.7). Candidates: only posts of open windows (st.open_window[*]).

use rustc_hash::FxHashMap;

use super::prelude::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feed {
    Friends,
    FriendsOfFriends,
    Public,
    Following,
}

impl Feed {
    pub fn parse(s: &str) -> Option<Feed> {
        match s {
            "friends" => Some(Feed::Friends),
            "friends-of-friends" => Some(Feed::FriendsOfFriends),
            "public" => Some(Feed::Public),
            "following" => Some(Feed::Following),
            _ => None,
        }
    }
}

// GET /api/feeds/{feed} — auth user, pinned.
pub async fn get_feed(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    // binding (unparsable values ⇒ 400 bind error) before validation
    let before_posted_at = req.query_time("beforePostedAt")?;
    let before_score = req.query_int("beforeScore")?;
    let before_id = req.query_nonempty("beforeId");
    let feed = Feed::parse(req.param(0)).ok_or(ApiError::bad_request("invalid_post"))?;
    let pagination = || ApiError::bad_request("invalid_pagination");
    if before_posted_at.is_some() != before_id.is_some() {
        return Err(pagination());
    }
    if feed == Feed::FriendsOfFriends && before_score.is_some() != before_id.is_some() {
        return Err(pagination());
    }
    let before = match (before_posted_at, before_id.as_deref()) {
        (Some(t), Some(id)) => Some((t, IdCursor::parse(id))),
        _ => None,
    };
    let score = if feed == Feed::FriendsOfFriends { before_score } else { None };
    ctx.respond(200, |st, w| {
        let items = collect(st, me.idx, feed, before, score);
        let (items, more) = paginate(items.into_iter(), PAGE_SIZE);
        views::page(w, &items, more, |w, &(p, s)| {
            views::post(w, st, p, |w| {
                if feed == Feed::FriendsOfFriends {
                    w.k("score").i(s);
                }
            });
        });
        Ok(())
    })
    .await
}

/// `beforeId` as given: a parsed id, or (non-UUID) its raw lowercase-comparable string.
#[derive(Clone, Debug)]
pub enum IdCursor {
    Id(Id),
    Raw(String),
}

impl IdCursor {
    pub fn parse(s: &str) -> IdCursor {
        // ids are printed lowercase; an uppercase UUID string compares differently byte-wise
        if s.bytes().all(|c| !c.is_ascii_uppercase()) {
            if let Some(id) = Id::parse(s) {
                return IdCursor::Id(id);
            }
        }
        IdCursor::Raw(s.to_string())
    }
    /// `id < cursor` in byte order of the 36-char lowercase string.
    pub fn is_after(&self, id: Id) -> bool {
        match self {
            IdCursor::Id(c) => id < *c,
            IdCursor::Raw(s) => id.to_buf().as_slice() < s.as_bytes(),
        }
    }
}

/// (postedAt, id) < cursor
pub fn before_cursor(c: &Option<(Ts, IdCursor)>, posted_at: Ts, id: Id) -> bool {
    match c {
        None => true,
        Some((t, cid)) => posted_at < *t || (posted_at == *t && cid.is_after(id)),
    }
}

/// All matching posts of the feed, sorted (score DESC,) postedAt DESC, id DESC, cursor applied.
/// Returns (post, score) pairs; score is 0 for non-ranked feeds.
pub fn collect(st: &State, v: UIdx, feed: Feed, before: Option<(Ts, IdCursor)>, before_score: Option<i64>) -> Vec<(PIdx, i64)> {
    let viewer = rules::viewer(st, v);
    if !viewer.posted_today {
        return Vec::new();
    }
    let vu = st.user(v);
    let mutual: Option<FxHashMap<UIdx, u32>> = (feed == Feed::FriendsOfFriends).then(|| rules::mutual_counts(st, v));
    let mut out: Vec<(PIdx, i64, Ts, Id)> = Vec::new();
    for r in Region::ALL {
        let Some(w) = st.open_window_of(r) else { continue };
        for &p in &st.window(w).posts {
            let post = st.post(p);
            let a = post.author;
            if a == v {
                continue;
            }
            let pred = match feed {
                Feed::Friends => vu.friends.contains_key(&a),
                Feed::FriendsOfFriends => {
                    !vu.friends.contains_key(&a) && mutual.as_ref().is_some_and(|m| m.get(&a).is_some_and(|&n| n > 0))
                }
                Feed::Public => post.visibility == Visibility::Public,
                Feed::Following => vu.following.contains_key(&a),
            };
            if !pred || !rules::can_view(st, &viewer, p) {
                continue;
            }
            let score = match &mutual {
                Some(m) => fof_score(st, v, m, p),
                None => 0,
            };
            out.push((p, score, post.posted_at, post.id));
        }
    }
    let keep = |&(_, s, t, id): &(PIdx, i64, Ts, Id)| -> bool {
        match before_score {
            Some(bs) if feed == Feed::FriendsOfFriends => s < bs || (s == bs && before_cursor(&before, t, id)),
            _ => before_cursor(&before, t, id),
        }
    };
    out.retain(keep);
    out.sort_unstable_by_key(|a| std::cmp::Reverse((a.1, a.2, a.3)));
    out.into_iter().map(|(p, s, _, _)| (p, s)).collect()
}

/// score(P) = 10 × FE(P) + 3 × Mutual(V, author) + min(Conn(author), 30)   (§6.7)
pub fn fof_score(st: &State, v: UIdx, mutual: &FxHashMap<UIdx, u32>, p: PIdx) -> i64 {
    let vf = &st.user(v).friends;
    let author = st.post(p).author;
    // FE: distinct active friends of V with a current RealMoji or a live comment on P
    let mut engaged: Vec<UIdx> = Vec::new();
    for &r in st.post_realmojis.get(p) {
        let rm = st.realmoji(r);
        if rm.is_current() && vf.contains_key(&rm.reactor) && st.user(rm.reactor).is_active() {
            engaged.push(rm.reactor);
        }
    }
    for &c in st.post_comments.get(p) {
        let cm = st.comment(c);
        if cm.deleted_at.is_none() && vf.contains_key(&cm.author) && st.user(cm.author).is_active() {
            engaged.push(cm.author);
        }
    }
    engaged.sort_unstable();
    engaged.dedup();
    let fe = engaged.len() as i64;
    let m = |x: UIdx| -> i64 { if x == v { 0 } else { mutual.get(&x).copied().unwrap_or(0) as i64 } };
    let conn: i64 = st.user(author).friends.keys().map(|&y| m(y)).sum();
    10 * fe + 3 * m(author) + conn.min(30)
}

/// Test helpers shared by the group B unit tests: the small synthetic dataset and a leader-like
/// exec + apply loop (no WAL, no replication).
#[cfg(test)]
pub(crate) mod testutil {
    use std::sync::OnceLock;

    use crate::apply::apply;
    use crate::command;

    use super::*;

    pub fn state() -> State {
        static PRISTINE: OnceLock<State> = OnceLock::new();
        PRISTINE
            .get_or_init(|| {
                let dir = std::env::temp_dir().join(format!("isureal-groupb-unit-{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&dir);
                gen_testdata::generate(&dir, gen_testdata::Scale::Small).expect("gen testdata");
                let st = crate::loader::load(&dir).expect("load testdata");
                let _ = std::fs::remove_dir_all(&dir);
                st
            })
            .clone()
    }

    pub fn apply_entry(st: &mut State, e: LogEntry) {
        let mut fx = Vec::new();
        let blob = matches!(e, LogEntry::PostCreated { .. } | LogEntry::RealMojiCreated { .. }).then_some(ImageLoc::Wal { off: 0, len: 1 });
        apply(st, &e, blob, &mut fx);
    }

    /// exec on `st` at time `now`, then apply the produced entries.
    pub fn run(st: &mut State, now: Ts, cmd: Command) -> Result<Outcome, ApiError> {
        let mut tx = Tx::new(now);
        let out = command::exec(st, &mut tx, &cmd)?;
        let mut fx = Vec::new();
        for (e, blob) in tx.entries.drain(..) {
            let loc = blob.map(|b| ImageLoc::Wal { off: 0, len: b.len() as u32 });
            apply(st, &e, loc, &mut fx);
        }
        Ok(out)
    }

    pub fn user(st: &State, name: &str) -> UIdx {
        st.user_by_username(name).unwrap_or_else(|| panic!("no user {name}"))
    }

    pub fn open(st: &mut State, now: Ts, r: Region, date: &str) -> WIdx {
        let d = crate::timefmt::parse_date(date).unwrap();
        let out = run(st, now, Command::OpenWindow { region: r, local_date: d, on_time_seconds: 86400 }).unwrap();
        st.window_idx(out.id()).unwrap()
    }

    pub fn post(st: &mut State, now: Ts, u: UIdx, vis: Visibility) -> Result<PIdx, ApiError> {
        let actor = st.user(u).id;
        let out = run(st, now, Command::CreatePost { actor, image: Bytes::from_static(b"jpeg").into(), caption: String::new(), visibility: vis, location: Location::None })?;
        Ok(st.post_idx(out.id()).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::*;
    use super::*;

    const T0: Ts = 1_793_000_000_000_000;

    fn brute_mutual(st: &State, v: UIdx, x: UIdx) -> i64 {
        if v == x {
            return 0;
        }
        st.user(v).friends.keys().filter(|f| st.user(x).friends.contains_key(f)).count() as i64
    }

    /// (viewer, author): author ∈ FoF(viewer) \ Friends(viewer), both asia + active, not blocked.
    fn pick(st: &State) -> (UIdx, UIdx) {
        for v in 0..st.users.len() as UIdx {
            let uv = st.user(v);
            if !uv.is_active() || uv.region != Region::Asia {
                continue;
            }
            for a in 0..st.users.len() as UIdx {
                let ua = st.user(a);
                if a != v && ua.is_active() && ua.region == Region::Asia && !uv.friends.contains_key(&a) && brute_mutual(st, v, a) > 0 && !rules::blocked(st, v, a) {
                    return (v, a);
                }
            }
        }
        panic!("no FoF pair in the test data");
    }

    #[test]
    fn fof_score_matches_definition() {
        let mut st = state();
        let (v, a) = pick(&st);
        open(&mut st, T0, Region::Asia, "2026-10-31");
        // the viewer has not posted yet ⇒ empty
        let pa = post(&mut st, T0 + 1, a, Visibility::FriendsOfFriends).unwrap();
        assert!(collect(&st, v, Feed::FriendsOfFriends, None, None).is_empty());
        post(&mut st, T0 + 2, v, Visibility::Friends).unwrap();

        let friends: Vec<UIdx> = {
            let mut f: Vec<UIdx> = st.user(v).friends.keys().copied().filter(|&f| st.user(f).is_active()).collect();
            f.sort_unstable();
            f
        };
        let pid = st.post(pa).id;
        let mut t = T0 + 10;
        let mut next = || {
            t += 1;
            t
        };
        // friend 0: a realmoji AND a comment ⇒ counts once
        let f0 = st.user(friends[0]).id;
        apply_entry(&mut st, LogEntry::RealMojiCreated { id: Id::new_v4(), post: pid, reactor: f0, image: Id::new_v4(), reaction: Reaction::Like, at: next() });
        apply_entry(&mut st, LogEntry::CommentCreated { id: Id::new_v4(), post: pid, author: f0, body: "hi".into(), at: next() });
        // a non-friend (the author) comments ⇒ not counted
        let aid = st.user(a).id;
        apply_entry(&mut st, LogEntry::CommentCreated { id: Id::new_v4(), post: pid, author: aid, body: "self".into(), at: next() });
        let mut expected_fe = 1;
        if friends.len() > 1 {
            // friend 1: comment then deleted, realmoji then deleted ⇒ not counted
            let f1 = st.user(friends[1]).id;
            let c = Id::new_v4();
            apply_entry(&mut st, LogEntry::CommentCreated { id: c, post: pid, author: f1, body: "x".into(), at: next() });
            apply_entry(&mut st, LogEntry::CommentDeleted { id: c, at: next() });
            let r = Id::new_v4();
            apply_entry(&mut st, LogEntry::RealMojiCreated { id: r, post: pid, reactor: f1, image: Id::new_v4(), reaction: Reaction::Heart, at: next() });
            apply_entry(&mut st, LogEntry::RealMojiDeleted { id: r, at: next() });
        }
        if friends.len() > 2 {
            // friend 2: realmoji replaced by a new one ⇒ the current one counts (once)
            let f2 = st.user(friends[2]).id;
            apply_entry(&mut st, LogEntry::RealMojiCreated { id: Id::new_v4(), post: pid, reactor: f2, image: Id::new_v4(), reaction: Reaction::Smile, at: next() });
            apply_entry(&mut st, LogEntry::RealMojiCreated { id: Id::new_v4(), post: pid, reactor: f2, image: Id::new_v4(), reaction: Reaction::Laugh, at: next() });
            expected_fe += 1;
        }
        let expect = |st: &State, fe: i64| -> i64 {
            let conn: i64 = st.user(a).friends.keys().map(|&y| brute_mutual(st, v, y)).sum();
            10 * fe + 3 * brute_mutual(st, v, a) + conn.min(30)
        };
        let items = collect(&st, v, Feed::FriendsOfFriends, None, None);
        assert_eq!(items.iter().find(|x| x.0 == pa).map(|x| x.1), Some(expect(&st, expected_fe)));

        // friend 0 deleted: no longer engaged, but still counts in Mutual / Conn (deleted friends are bridges)
        apply_entry(&mut st, LogEntry::AccountDeleted { user: f0, at: next() });
        let items = collect(&st, v, Feed::FriendsOfFriends, None, None);
        assert_eq!(items.iter().find(|x| x.0 == pa).map(|x| x.1), Some(expect(&st, expected_fe - 1)));

        // cursor (score, postedAt, id) is strict: the item itself is excluded, a higher score keeps it
        let s = items.iter().find(|x| x.0 == pa).unwrap().1;
        let cur = || Some((st.post(pa).posted_at, IdCursor::Id(st.post(pa).id)));
        assert!(collect(&st, v, Feed::FriendsOfFriends, cur(), Some(s)).iter().all(|x| x.0 != pa));
        assert!(collect(&st, v, Feed::FriendsOfFriends, cur(), Some(s + 1)).iter().any(|x| x.0 == pa));

        // friends feed only has friends; public feed only public posts
        assert!(collect(&st, v, Feed::Friends, None, None).iter().all(|x| st.user(v).friends.contains_key(&st.post(x.0).author)));
        assert!(collect(&st, v, Feed::Public, None, None).iter().all(|x| st.post(x.0).visibility == Visibility::Public));
    }

    #[test]
    fn feeds_only_open_windows_and_not_blocked() {
        let mut st = state();
        let dave = user(&st, "dave");
        let frank = user(&st, "frank");
        assert!(rules::blocked(&st, dave, frank));
        open(&mut st, T0, Region::Asia, "2026-10-31");
        let pf = post(&mut st, T0 + 1, frank, Visibility::Public).unwrap();
        post(&mut st, T0 + 2, dave, Visibility::Public).unwrap();
        let alice = user(&st, "alice");
        let pa = post(&mut st, T0 + 3, alice, Visibility::Public).unwrap();
        let public = collect(&st, dave, Feed::Public, None, None);
        assert!(public.iter().all(|x| x.0 != pf), "blocked author's post in feed");
        assert!(public.iter().any(|x| x.0 == pa));
        assert!(public.windows(2).all(|w| (st.post(w[0].0).posted_at, st.post(w[0].0).id) > (st.post(w[1].0).posted_at, st.post(w[1].0).id)));
        // the next asia window ends the old one ⇒ its posts leave the feeds; dave has not posted "today" anymore
        open(&mut st, T0 + 10, Region::Asia, "2026-11-01");
        assert!(collect(&st, dave, Feed::Public, None, None).is_empty());
        post(&mut st, T0 + 11, dave, Visibility::Public).unwrap();
        assert!(collect(&st, dave, Feed::Public, None, None).is_empty());
        // a deleted author's posts disappear immediately
        post(&mut st, T0 + 12, alice, Visibility::Public).unwrap();
        assert_eq!(collect(&st, dave, Feed::Public, None, None).len(), 1);
        let alice_id = st.user(alice).id;
        apply_entry(&mut st, LogEntry::AccountDeleted { user: alice_id, at: T0 + 13 });
        assert!(collect(&st, dave, Feed::Public, None, None).is_empty());
    }

    #[test]
    fn raw_id_cursor_compares_bytes() {
        let id = Id::parse("7c3d3da7-30e9-4d2b-bc66-74d09f2b33fc").unwrap();
        assert!(IdCursor::parse("zzz").is_after(id));
        assert!(!IdCursor::parse("0").is_after(id));
        assert!(!IdCursor::parse("7C3D3DA7-30E9-4D2B-BC66-74D09F2B33FD").is_after(id)); // uppercase sorts first
        assert!(IdCursor::parse("7c3d3da7-30e9-4d2b-bc66-74d09f2b33fd").is_after(id));
        assert!(!IdCursor::parse("7c3d3da7-30e9-4d2b-bc66-74d09f2b33fc").is_after(id));
    }
}
