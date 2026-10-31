//! Business rules of spec §4 over `State`. Shared by groups B and C (and A for blocks/friends).

use rustc_hash::FxHashMap;

use crate::model::*;
use crate::state::State;

#[inline]
pub fn is_active(st: &State, u: UIdx) -> bool {
    st.user(u).is_active()
}

/// {a, b} is a friendship (deleted users included, §4.1).
#[inline]
pub fn is_friend(st: &State, a: UIdx, b: UIdx) -> bool {
    st.user(a).friends.contains_key(&b)
}

/// A block a→b or b→a exists.
#[inline]
pub fn blocked(st: &State, a: UIdx, b: UIdx) -> bool {
    let ua = st.user(a);
    ua.blocking.contains_key(&b) || ua.blocked_by.contains_key(&b)
}

/// Mutual(V, X): number of F ∈ Friends(V) with {F, X} a friendship. Mutual(V, V) = 0.
pub fn mutual(st: &State, v: UIdx, x: UIdx) -> u32 {
    if v == x {
        return 0;
    }
    let (fv, fx) = (&st.user(v).friends, &st.user(x).friends);
    let (small, big) = if fv.len() <= fx.len() { (fv, fx) } else { (fx, fv) };
    small.keys().filter(|k| big.contains_key(k)).count() as u32
}

/// A ∈ FoF(V) (= some friend of V is a friend of A).
pub fn in_fof(st: &State, v: UIdx, a: UIdx) -> bool {
    let (fv, fa) = (&st.user(v).friends, &st.user(a).friends);
    let (small, big) = if fv.len() <= fa.len() { (fv, fa) } else { (fa, fv) };
    small.keys().any(|k| big.contains_key(k))
}

/// Mutual(V, Y) for every Y with Mutual > 0 (Y ≠ V). Cost: Σ_{F∈Friends(V)} |Friends(F)|.
pub fn mutual_counts(st: &State, v: UIdx) -> FxHashMap<UIdx, u32> {
    let mut m: FxHashMap<UIdx, u32> = FxHashMap::default();
    for &f in st.user(v).friends.keys() {
        for &y in st.user(f).friends.keys() {
            if y != v {
                *m.entry(y).or_insert(0) += 1;
            }
        }
    }
    m
}

/// (active, deleted) post counts of user `u` over all windows (any region) with local date `date`.
pub fn slot_counts(st: &State, u: UIdx, date: Date) -> (u32, u32) {
    let (mut active, mut deleted) = (0, 0);
    for &p in st.user(u).posts.iter().rev() {
        let post = st.post(p);
        if st.window(post.window).local_date == date {
            if post.deleted_at.is_some() {
                deleted += 1;
            } else {
                active += 1;
            }
        }
    }
    (active, deleted)
}

/// HasPostedToday(V) (§4.2).
pub fn has_posted_today(st: &State, v: UIdx) -> bool {
    let Some(w) = st.open_window_of(st.user(v).region) else { return false };
    let date = st.window(w).local_date;
    st.user(v).posts.iter().rev().any(|&p| {
        let post = st.post(p);
        post.deleted_at.is_none() && st.window(post.window).local_date == date
    })
}

/// Post exists for the API: not deleted and author active.
#[inline]
pub fn post_alive(st: &State, p: PIdx) -> bool {
    let post = st.post(p);
    post.deleted_at.is_none() && st.user(post.author).is_active()
}

#[inline]
pub fn window_open(st: &State, w: WIdx) -> bool {
    st.window(w).is_open()
}

/// Per-request viewer context (compute once, reuse for many posts).
#[derive(Clone, Copy, Debug)]
pub struct Viewer {
    pub idx: UIdx,
    pub posted_today: bool,
}

pub fn viewer(st: &State, v: UIdx) -> Viewer {
    Viewer { idx: v, posted_today: has_posted_today(st, v) }
}

/// canView(V, P) (§4.3), including the existence precondition.
pub fn can_view(st: &State, v: &Viewer, p: PIdx) -> bool {
    if !post_alive(st, p) {
        return false;
    }
    let post = st.post(p);
    if post.author == v.idx {
        return true;
    }
    if !window_open(st, post.window) || !v.posted_today || blocked(st, v.idx, post.author) {
        return false;
    }
    match post.visibility {
        Visibility::Public => true,
        Visibility::Friends => is_friend(st, v.idx, post.author),
        Visibility::FriendsOfFriends => is_friend(st, v.idx, post.author) || in_fof(st, v.idx, post.author),
    }
}

/// canComment(V, P) (§4.4) — also gates listing comments.
pub fn can_comment(st: &State, v: &Viewer, p: PIdx) -> bool {
    if !is_active(st, v.idx) || !can_view(st, v, p) {
        return false;
    }
    let post = st.post(p);
    if !window_open(st, post.window) {
        return false;
    }
    if post.author == v.idx {
        return v.posted_today;
    }
    post.visibility == Visibility::Public || is_friend(st, v.idx, post.author)
}

/// §5.3 step 6: image liveness.
pub fn image_live(st: &State, img: Id, meta: &ImageMeta) -> bool {
    let owner = st.user(meta.owner);
    if !owner.is_active() {
        return false;
    }
    match meta.kind {
        ImageKind::Profile => owner.profile_image == Some(img),
        ImageKind::Post => {
            let p = st.post(meta.ref_idx);
            p.deleted_at.is_none() && p.author == meta.owner && p.image == img
        }
        ImageKind::RealMoji => {
            let r = st.realmoji(meta.ref_idx);
            if !(r.is_current() && r.reactor == meta.owner && r.image == img) {
                return false;
            }
            let p = st.post(r.post);
            p.deleted_at.is_none() && st.user(p.author).is_active()
        }
    }
}

/// `(onTime, lateSeconds)` of a post.
pub fn lateness(st: &State, p: PIdx) -> (bool, i64) {
    let post = st.post(p);
    let deadline = st.window(post.window).on_time_deadline;
    if post.posted_at <= deadline {
        (true, 0)
    } else {
        (false, (post.posted_at - deadline) / 1_000_000)
    }
}
