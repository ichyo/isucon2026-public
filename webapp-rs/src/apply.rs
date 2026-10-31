//! `apply`: the only place (besides the loader) that mutates `State`. Deterministic and total:
//! no clock, no RNG, no dependence on hash-map iteration order. Every node applies the same entries in
//! the same order and ends up with the same state.

use std::cmp::Ordering;

use crate::entry::LogEntry;
use crate::model::*;
use crate::state::State;
use crate::text;

/// Side effects for the caller (handled while the write lock is still held, see App::handle_effects).
#[derive(Debug, Clone)]
pub enum Effect {
    ImageCreated(Id, UIdx),
    /// a posting window opened: notify SSE subscribers of its region
    WindowOpened(WIdx),
    /// user deleted or region changed: close their SSE streams
    UserGone(UIdx),
    /// IP ban (stored in App::bans, outside State)
    Ban { ip: String, until: Ts },
}

macro_rules! warn_skip {
    ($($t:tt)*) => {{
        eprintln!("[apply] WARN inconsistent entry skipped: {}", format!($($t)*));
        return;
    }};
}

fn user_of(st: &State, id: Id) -> Option<UIdx> {
    st.user_by_id.get(&id).copied()
}

pub fn apply(st: &mut State, e: &LogEntry, blob: Option<ImageLoc>, fx: &mut Vec<Effect>) {
    match e {
        LogEntry::UserCreated { id, username, secret_hash, display_name, region, at } => {
            if st.user_by_name.contains_key(username) || st.user_by_id.contains_key(id) {
                warn_skip!("duplicate user {username}");
            }
            let idx = st.users.len() as UIdx;
            st.users.push(User {
                id: *id,
                username: username.clone(),
                secret_hash: *secret_hash,
                display_name: display_name.clone(),
                bio: "".into(),
                profile_image: None,
                region: *region,
                account_type: AccountType::Regular,
                deleted: false,
                created_at: *at,
                updated_at: *at,
                deleted_at: OptTs::NONE,
                fold_username: text::fold(username).into(),
                fold_display: text::fold(display_name).into(),
                posts: Vec::new(),
                friends: Default::default(),
                following: Default::default(),
                followers: Default::default(),
                blocking: Default::default(),
                blocked_by: Default::default(),
                incoming: Default::default(),
                outgoing: Default::default(),
                session: None,
            });
            st.user_by_id.insert(*id, idx);
            st.user_by_name.insert(username.clone(), idx);
            let users = &st.users;
            let key = |u: UIdx| (&*users[u as usize].username, users[u as usize].id);
            let pos = st.users_sorted.partition_point(|&x| key(x) < key(idx));
            st.users_sorted.insert(pos, idx);
        }
        LogEntry::SessionCreated { token, user, .. } => {
            let Some(u) = user_of(st, *user) else { warn_skip!("session for unknown user {user}") };
            if let Some(old) = st.users[u as usize].session.take() {
                st.sessions.remove(&old);
            }
            st.sessions.insert(*token, u);
            st.users[u as usize].session = Some(*token);
        }
        LogEntry::SessionEnded { token } => {
            if let Some(u) = st.sessions.remove(token) {
                let usr = &mut st.users[u as usize];
                if usr.session == Some(*token) {
                    usr.session = None;
                }
            }
        }
        LogEntry::ProfileUpdated { user, display_name, bio, region, at } => {
            let Some(u) = user_of(st, *user) else { warn_skip!("profile of unknown user {user}") };
            let usr = &mut st.users[u as usize];
            if let Some(d) = display_name {
                usr.display_name = d.clone();
                usr.fold_display = text::fold(d).into();
            }
            if let Some(b) = bio {
                usr.bio = b.clone();
            }
            if let Some(r) = region {
                if usr.region != *r {
                    usr.region = *r;
                    fx.push(Effect::UserGone(u));
                }
            }
            usr.updated_at = *at;
        }
        LogEntry::ProfileImageSet { user, image, at } => {
            let Some(u) = user_of(st, *user) else { warn_skip!("profile image of unknown user {user}") };
            if let Some(old) = st.users[u as usize].profile_image.take() {
                st.images.remove(&old);
            }
            if let Some(img) = image {
                let Some(loc) = blob else { warn_skip!("profile image {img} without blob") };
                st.images.insert(*img, ImageMeta { kind: ImageKind::Profile, owner: u, ref_idx: u, loc });
                fx.push(Effect::ImageCreated(*img, u));
            }
            let usr = &mut st.users[u as usize];
            usr.profile_image = *image;
            usr.updated_at = *at;
        }
        LogEntry::AccountDeleted { user, at } => {
            let Some(u) = user_of(st, *user) else { warn_skip!("deletion of unknown user {user}") };
            let usr = &mut st.users[u as usize];
            usr.deleted = true;
            usr.deleted_at = OptTs::some(*at);
            usr.updated_at = *at;
            if let Some(tok) = usr.session.take() {
                st.sessions.remove(&tok);
            }
            fx.push(Effect::UserGone(u));
        }
        LogEntry::FriendRequestCreated { id, requester, recipient, at } => {
            let (Some(a), Some(b)) = (user_of(st, *requester), user_of(st, *recipient)) else {
                warn_skip!("friend request with unknown users")
            };
            let idx = st.friend_requests.len() as FrIdx;
            st.friend_requests.push(FriendRequest {
                id: *id,
                requester: a,
                recipient: b,
                status: FrStatus::Pending,
                created_at: *at,
                updated_at: *at,
            });
            st.fr_by_id.insert(*id, idx);
            st.users[a as usize].outgoing.insert(b, idx);
            st.users[b as usize].incoming.insert(a, idx);
        }
        LogEntry::FriendRequestUpdated { id, status, at } => {
            let Some(r) = st.fr_idx(*id) else { warn_skip!("unknown friend request {id}") };
            set_request_status(st, r, *status, *at);
        }
        LogEntry::FriendshipCreated { id, a, b, at, .. } => {
            let (Some(a), Some(b)) = (user_of(st, *a), user_of(st, *b)) else { warn_skip!("friendship with unknown users") };
            let rel = Rel { id: *id, at: *at };
            st.users[a as usize].friends.insert(b, rel);
            st.users[b as usize].friends.insert(a, rel);
        }
        LogEntry::FriendshipRemoved { a, b } => {
            let (Some(a), Some(b)) = (user_of(st, *a), user_of(st, *b)) else { warn_skip!("unfriend with unknown users") };
            st.users[a as usize].friends.remove(&b);
            st.users[b as usize].friends.remove(&a);
        }
        LogEntry::FollowCreated { id, follower, followee, at } => {
            let (Some(a), Some(b)) = (user_of(st, *follower), user_of(st, *followee)) else { warn_skip!("follow with unknown users") };
            let rel = Rel { id: *id, at: *at };
            st.users[a as usize].following.insert(b, rel);
            st.users[b as usize].followers.insert(a, rel);
        }
        LogEntry::FollowRemoved { follower, followee } => {
            let (Some(a), Some(b)) = (user_of(st, *follower), user_of(st, *followee)) else { warn_skip!("unfollow with unknown users") };
            st.users[a as usize].following.remove(&b);
            st.users[b as usize].followers.remove(&a);
        }
        LogEntry::BlockCreated { id, blocker, blocked, at } => {
            let (Some(a), Some(b)) = (user_of(st, *blocker), user_of(st, *blocked)) else { warn_skip!("block with unknown users") };
            let rel = Rel { id: *id, at: *at };
            st.users[a as usize].blocking.insert(b, rel);
            st.users[b as usize].blocked_by.insert(a, rel);
            // cascade: friendship, follows both directions, pending requests either direction
            st.users[a as usize].friends.remove(&b);
            st.users[b as usize].friends.remove(&a);
            st.users[a as usize].following.remove(&b);
            st.users[b as usize].followers.remove(&a);
            st.users[b as usize].following.remove(&a);
            st.users[a as usize].followers.remove(&b);
            let mut pend: Vec<FrIdx> = Vec::with_capacity(2);
            if let Some(&r) = st.users[a as usize].outgoing.get(&b) {
                pend.push(r);
            }
            if let Some(&r) = st.users[a as usize].incoming.get(&b) {
                pend.push(r);
            }
            pend.sort_unstable();
            for r in pend {
                set_request_status(st, r, FrStatus::Cancelled, *at);
            }
        }
        LogEntry::BlockRemoved { blocker, blocked } => {
            let (Some(a), Some(b)) = (user_of(st, *blocker), user_of(st, *blocked)) else { warn_skip!("unblock with unknown users") };
            st.users[a as usize].blocking.remove(&b);
            st.users[b as usize].blocked_by.remove(&a);
        }
        LogEntry::AdminSessionCreated { token, admin, .. } => {
            let Some(a) = st.admins.iter().position(|x| x.id == *admin) else { warn_skip!("unknown admin {admin}") };
            st.admin_sessions.insert(*token, a as u32);
        }
        LogEntry::AdminSessionEnded { token } => {
            st.admin_sessions.remove(token);
        }
        LogEntry::UserCertified { user, at } => {
            let Some(u) = user_of(st, *user) else { warn_skip!("certify unknown user {user}") };
            let usr = &mut st.users[u as usize];
            usr.account_type = AccountType::CertifiedPublic;
            usr.updated_at = *at;
        }
        LogEntry::WindowOpened { id, region, local_date, starts_at, on_time_deadline } => {
            let r = region.idx();
            if let Some(prev) = st.open_window[r] {
                st.windows[prev as usize].ended_at = OptTs::some(*starts_at);
            }
            let idx = st.windows.len() as WIdx;
            st.windows.push(Window {
                id: *id,
                region: *region,
                local_date: *local_date,
                starts_at: *starts_at,
                on_time_deadline: *on_time_deadline,
                ended_at: OptTs::NONE,
                created_at: *starts_at,
                posts: Vec::new(),
            });
            st.window_by_id.insert(*id, idx);
            st.open_window[r] = Some(idx);
            st.last_date[r] = Some(st.last_date[r].map_or(*local_date, |d| d.max(*local_date)));
            fx.push(Effect::WindowOpened(idx));
        }
        LogEntry::PostCreated { id, author, window, image, caption, visibility, location, at } => {
            let Some(a) = user_of(st, *author) else { warn_skip!("post by unknown user {author}") };
            let Some(w) = st.window_idx(*window) else { warn_skip!("post in unknown window {window}") };
            let Some(loc) = blob else { warn_skip!("post {id} without blob") };
            let idx = st.posts.len() as PIdx;
            st.posts.push(Post {
                id: *id,
                author: a,
                window: w,
                image: *image,
                caption: caption.clone(),
                visibility: *visibility,
                location: location.clone(),
                posted_at: *at,
                deleted_at: OptTs::NONE,
            });
            st.post_by_id.insert(*id, idx);
            let posts = &st.posts;
            let key = |p: PIdx| (posts[p as usize].posted_at, posts[p as usize].id);
            let up = &mut st.users[a as usize].posts;
            let pos = up.partition_point(|&x| key(x) <= key(idx));
            up.insert(pos, idx);
            st.windows[w as usize].posts.push(idx);
            st.images.insert(*image, ImageMeta { kind: ImageKind::Post, owner: a, ref_idx: idx, loc });
            fx.push(Effect::ImageCreated(*image, a));
        }
        LogEntry::PostUpdated { id, caption, visibility, .. } => {
            let Some(p) = st.post_idx(*id) else { warn_skip!("update of unknown post {id}") };
            let post = &mut st.posts[p as usize];
            if let Some(c) = caption {
                post.caption = c.clone();
            }
            if let Some(v) = visibility {
                post.visibility = *v;
            }
        }
        LogEntry::PostDeleted { id, at } => {
            let Some(p) = st.post_idx(*id) else { warn_skip!("delete of unknown post {id}") };
            let post = &mut st.posts[p as usize];
            if post.deleted_at.is_none() {
                post.deleted_at = OptTs::some(*at);
            }
        }
        LogEntry::CommentCreated { id, post, author, body, at } => {
            let Some(p) = st.post_idx(*post) else { warn_skip!("comment on unknown post {post}") };
            let Some(a) = user_of(st, *author) else { warn_skip!("comment by unknown user {author}") };
            let idx = st.comments.len() as CIdx;
            st.comments.push(Comment { id: *id, post: p, author: a, body: body.clone(), created_at: *at, deleted_at: OptTs::NONE });
            st.comment_by_id.insert(*id, idx);
            let cs = &st.comments;
            st.post_comments.insert_sorted(p, idx, |x, y| cmp_ts_id((cs[x as usize].created_at, cs[x as usize].id), (cs[y as usize].created_at, cs[y as usize].id)));
        }
        LogEntry::CommentDeleted { id, at } => {
            let Some(c) = st.comment_idx(*id) else { warn_skip!("delete of unknown comment {id}") };
            let cm = &mut st.comments[c as usize];
            if cm.deleted_at.is_none() {
                cm.deleted_at = OptTs::some(*at);
            }
        }
        LogEntry::RealMojiCreated { id, post, reactor, image, reaction, at } => {
            let Some(p) = st.post_idx(*post) else { warn_skip!("realmoji on unknown post {post}") };
            let Some(a) = user_of(st, *reactor) else { warn_skip!("realmoji by unknown user {reactor}") };
            let Some(loc) = blob else { warn_skip!("realmoji {id} without blob") };
            let prev: Vec<RmIdx> = st
                .post_realmojis
                .get(p)
                .iter()
                .copied()
                .filter(|&r| {
                    let rm = &st.realmojis[r as usize];
                    rm.reactor == a && rm.is_current()
                })
                .collect();
            for r in prev {
                st.realmojis[r as usize].replaced_at = OptTs::some(*at);
            }
            let idx = st.realmojis.len() as RmIdx;
            st.realmojis.push(RealMoji {
                id: *id,
                post: p,
                reactor: a,
                image: *image,
                reaction: *reaction,
                created_at: *at,
                replaced_at: OptTs::NONE,
                deleted_at: OptTs::NONE,
            });
            st.realmoji_by_id.insert(*id, idx);
            let rs = &st.realmojis;
            st.post_realmojis.insert_sorted(p, idx, |x, y| cmp_ts_id((rs[x as usize].created_at, rs[x as usize].id), (rs[y as usize].created_at, rs[y as usize].id)));
            st.images.insert(*image, ImageMeta { kind: ImageKind::RealMoji, owner: a, ref_idx: idx, loc });
            fx.push(Effect::ImageCreated(*image, a));
        }
        LogEntry::RealMojiDeleted { id, at } => {
            let Some(r) = st.realmoji_idx(*id) else { warn_skip!("delete of unknown realmoji {id}") };
            let rm = &mut st.realmojis[r as usize];
            if rm.deleted_at.is_none() {
                rm.deleted_at = OptTs::some(*at);
            }
        }
        LogEntry::IpBanned { ip, until } => {
            fx.push(Effect::Ban { ip: ip.clone(), until: *until });
        }
    }
}

fn cmp_ts_id(a: (Ts, Id), b: (Ts, Id)) -> Ordering {
    a.cmp(&b)
}

fn set_request_status(st: &mut State, r: FrIdx, status: FrStatus, at: Ts) {
    let fr = st.friend_requests[r as usize];
    if fr.status == FrStatus::Pending && status != FrStatus::Pending {
        let (a, b) = (fr.requester, fr.recipient);
        if st.users[a as usize].outgoing.get(&b) == Some(&r) {
            st.users[a as usize].outgoing.remove(&b);
        }
        if st.users[b as usize].incoming.get(&a) == Some(&r) {
            st.users[b as usize].incoming.remove(&a);
        }
    }
    let m = &mut st.friend_requests[r as usize];
    m.status = status;
    m.updated_at = at;
}
