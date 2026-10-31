//! JSON views of the common objects (spec §6 "Common JSON objects"), in documented key order.

use crate::json::JsonOut;
use crate::model::*;
use crate::rules;
use crate::state::State;

/// `{"id","username","displayName","bio","region","accountType","status"}` (no closing brace when `open`)
fn session_user_fields(w: &mut JsonOut, u: &User) {
    w.k("id").id(u.id);
    w.k("username").s(&u.username);
    w.k("displayName").s(&u.display_name);
    w.k("bio").s(&u.bio);
    w.k("region").s(u.region.as_str());
    w.k("accountType").s(u.account_type.as_str());
    w.k("status").s(u.status_str());
}

/// SessionUser
pub fn session_user(w: &mut JsonOut, u: &User) {
    w.obj();
    session_user_fields(w, u);
    w.end();
}

/// Me = SessionUser + profileImageUrl
pub fn me(w: &mut JsonOut, u: &User) {
    w.obj();
    session_user_fields(w, u);
    w.k("profileImageUrl");
    profile_image_url(w, u);
    w.end();
}

/// Signed profile image URL or null.
pub fn profile_image_url(w: &mut JsonOut, u: &User) {
    match u.profile_image {
        Some(img) => w.image_url(img, &u.secret_hash),
        None => w.null(),
    };
}

/// UserSummary `{"id","username","displayName","profileImageUrl","bio","accountType"}`; bio shown iff `show_bio`.
pub fn user_summary(w: &mut JsonOut, u: &User, show_bio: bool) {
    w.obj();
    w.k("id").id(u.id);
    w.k("username").s(&u.username);
    w.k("displayName").s(&u.display_name);
    w.k("profileImageUrl");
    profile_image_url(w, u);
    w.k("bio").s(if show_bio { &u.bio } else { "" });
    w.k("accountType").s(u.account_type.as_str());
    w.end();
}

pub fn location(w: &mut JsonOut, l: &Location) {
    w.obj();
    match l {
        Location::None => {
            w.k("kind").s("none");
        }
        Location::Exact { lat, lng } => {
            w.k("kind").s("exact").k("latitude").f64(*lat).k("longitude").f64(*lng);
        }
        Location::Approximate { locality } => {
            w.k("kind").s("approximate").k("locality").s(locality);
        }
    }
    w.end();
}

/// Post object; `extra` appends keys after `lateSeconds` (e.g. `score`, `imagePath`, `realmojis`, `comments`).
pub fn post(w: &mut JsonOut, st: &State, p: PIdx, extra: impl FnOnce(&mut JsonOut)) {
    let post = st.post(p);
    let author = st.user(post.author);
    let (on_time, late) = rules::lateness(st, p);
    w.obj();
    w.k("id").id(post.id);
    w.k("author");
    user_summary(w, author, true);
    w.k("postingWindowId").id(st.window(post.window).id);
    w.k("imageUrl").image_url(post.image, &author.secret_hash);
    w.k("caption").s(&post.caption);
    w.k("visibility").s(post.visibility.as_str());
    w.k("location");
    location(w, &post.location);
    w.k("postedAt").ts(post.posted_at);
    w.k("onTime").b(on_time);
    w.k("lateSeconds").i(late);
    extra(w);
    w.end();
}

/// RealMoji `{"id","reactor","reactionType","imageUrl","createdAt"}`
pub fn realmoji(w: &mut JsonOut, st: &State, r: RmIdx) {
    let rm = st.realmoji(r);
    let reactor = st.user(rm.reactor);
    w.obj();
    w.k("id").id(rm.id);
    w.k("reactor");
    user_summary(w, reactor, true);
    w.k("reactionType").s(rm.reaction.as_str());
    w.k("imageUrl").image_url(rm.image, &reactor.secret_hash);
    w.k("createdAt").ts(rm.created_at);
    w.end();
}

/// Comment `{"id","author","body","createdAt"}`
pub fn comment(w: &mut JsonOut, st: &State, c: CIdx) {
    let cm = st.comment(c);
    w.obj();
    w.k("id").id(cm.id);
    w.k("author");
    user_summary(w, st.user(cm.author), true);
    w.k("body").s(&cm.body);
    w.k("createdAt").ts(cm.created_at);
    w.end();
}

/// PostingWindow `{"id","region","localDate","startsAt","onTimeDeadline"}`
pub fn posting_window(w: &mut JsonOut, win: &Window) {
    w.obj();
    w.k("id").id(win.id);
    w.k("region").s(win.region.as_str());
    w.k("localDate").date(win.local_date);
    w.k("startsAt").ts(win.starts_at);
    w.k("onTimeDeadline").ts(win.on_time_deadline);
    w.end();
}

/// FriendRequestState `{"relationId","status","updatedAt"}`
pub fn friend_request_state(w: &mut JsonOut, fr: &FriendRequest) {
    w.obj();
    w.k("relationId").id(fr.id);
    w.k("status").s(fr.status.as_str());
    w.k("updatedAt").ts(fr.updated_at);
    w.end();
}

/// Relationship `{"relationId","user","status","updatedAt"}`
pub fn relationship(w: &mut JsonOut, rel_id: Id, other: &User, show_bio: bool, status: &str, updated_at: Ts) {
    w.obj();
    w.k("relationId").id(rel_id);
    w.k("user");
    user_summary(w, other, show_bio);
    w.k("status").s(status);
    w.k("updatedAt").ts(updated_at);
    w.end();
}

/// `{"items":[...],"hasMore":bool}` — `items` writes each element as a value.
pub fn page<T>(w: &mut JsonOut, items: &[T], has_more: bool, mut item: impl FnMut(&mut JsonOut, &T)) {
    w.obj();
    w.k("items").arr();
    for it in items {
        item(w, it);
    }
    w.end();
    w.k("hasMore").b(has_more);
    w.end();
}

/// Takes up to `limit` items, returning (items, has_more) — fetch limit+1 semantics (§2.7).
pub fn paginate<T>(it: impl Iterator<Item = T>, limit: usize) -> (Vec<T>, bool) {
    let mut v: Vec<T> = it.take(limit + 1).collect();
    let more = v.len() > limit;
    v.truncate(limit);
    (v, more)
}

pub const PAGE_SIZE: usize = 50;
