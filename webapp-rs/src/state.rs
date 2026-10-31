//! The replicated in-memory state and its indexes. Mutated only by `apply.rs` (and the loader).
//! See docs/design.md §2 for which index serves which endpoint.

use std::cmp::Ordering;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::model::*;

/// Id -> dense index, with an immutable shared base (initial data) and a small mutable overlay.
#[derive(Clone, Default)]
pub struct IdMap {
    base: Arc<FxHashMap<Id, u32>>,
    extra: FxHashMap<Id, u32>,
}
impl IdMap {
    pub fn from_base(base: FxHashMap<Id, u32>) -> IdMap {
        IdMap { base: Arc::new(base), extra: FxHashMap::default() }
    }
    #[inline]
    pub fn get(&self, id: &Id) -> Option<u32> {
        if let Some(&v) = self.extra.get(id) {
            return Some(v);
        }
        self.base.get(id).copied()
    }
    pub fn insert(&mut self, id: Id, idx: u32) {
        self.extra.insert(id, idx);
    }
    pub fn len(&self) -> usize {
        self.base.len() + self.extra.len()
    }
}

/// Per-post item lists (RealMoji / comment indices) sorted ascending by (created_at, id).
/// Base = CSR arrays shared with the pristine copy; posts touched after load live in `extra`.
#[derive(Clone, Default)]
pub struct PostLists {
    base_off: Arc<Vec<u32>>,
    base_items: Arc<Vec<u32>>,
    extra: FxHashMap<PIdx, Vec<u32>>,
}
impl PostLists {
    pub fn from_base(offsets: Vec<u32>, items: Vec<u32>) -> PostLists {
        PostLists { base_off: Arc::new(offsets), base_items: Arc::new(items), extra: FxHashMap::default() }
    }
    #[inline]
    pub fn get(&self, p: PIdx) -> &[u32] {
        if let Some(v) = self.extra.get(&p) {
            return v;
        }
        let p = p as usize;
        if p + 1 < self.base_off.len() {
            &self.base_items[self.base_off[p] as usize..self.base_off[p + 1] as usize]
        } else {
            &[]
        }
    }
    /// Inserts keeping the order defined by `cmp` (ascending).
    pub fn insert_sorted(&mut self, p: PIdx, item: u32, cmp: impl Fn(u32, u32) -> Ordering) {
        if !self.extra.contains_key(&p) {
            let base = self.get(p).to_vec();
            self.extra.insert(p, base);
        }
        let v = self.extra.get_mut(&p).unwrap();
        let pos = v.partition_point(|&x| cmp(x, item) != Ordering::Greater);
        v.insert(pos, item);
    }
}

/// Image id -> metadata. Base (initial images, sorted by id) is shared and immutable.
#[derive(Clone, Default)]
pub struct ImageIndex {
    base: Arc<Vec<(Id, ImageMeta)>>,
    extra: FxHashMap<Id, ImageMeta>,
    /// initial images that were hard-deleted (replaced/deleted profile images)
    deleted_base: FxHashSet<Id>,
}
impl ImageIndex {
    pub fn from_base(sorted: Vec<(Id, ImageMeta)>) -> ImageIndex {
        ImageIndex { base: Arc::new(sorted), extra: FxHashMap::default(), deleted_base: FxHashSet::default() }
    }
    pub fn get(&self, id: &Id) -> Option<ImageMeta> {
        if let Some(m) = self.extra.get(id) {
            return Some(*m);
        }
        let i = self.base.binary_search_by(|(k, _)| k.cmp(id)).ok()?;
        if self.deleted_base.contains(id) {
            return None;
        }
        Some(self.base[i].1)
    }
    pub fn insert(&mut self, id: Id, meta: ImageMeta) {
        self.extra.insert(id, meta);
    }
    /// Permanently deletes an image (GET ⇒ "image must exist" fails).
    pub fn remove(&mut self, id: &Id) {
        if self.extra.remove(id).is_none() && self.base.binary_search_by(|(k, _)| k.cmp(id)).is_ok() {
            self.deleted_base.insert(*id);
        }
    }
    pub fn len(&self) -> usize {
        self.base.len() + self.extra.len() - self.deleted_base.len()
    }
}

#[derive(Clone, Default)]
pub struct State {
    pub users: Vec<User>,
    pub user_by_id: FxHashMap<Id, UIdx>,
    pub user_by_name: FxHashMap<Arc<str>, UIdx>,
    /// all users (incl. deleted) sorted by (username, id) ascending — search order
    pub users_sorted: Vec<UIdx>,
    /// active user sessions: token -> user
    pub sessions: FxHashMap<Id, UIdx>,

    pub admins: Vec<Admin>,
    pub admin_sessions: FxHashMap<Id, u32>,

    pub windows: Vec<Window>,
    pub window_by_id: FxHashMap<Id, WIdx>,
    /// the open window per region (index = Region::idx())
    pub open_window: [Option<WIdx>; 3],
    /// max local_date of any window per region
    pub last_date: [Option<Date>; 3],

    pub posts: Vec<Post>,
    pub post_by_id: IdMap,
    pub post_realmojis: PostLists,
    pub post_comments: PostLists,

    pub realmojis: Vec<RealMoji>,
    pub realmoji_by_id: IdMap,

    pub comments: Vec<Comment>,
    pub comment_by_id: IdMap,

    pub friend_requests: Vec<FriendRequest>,
    pub fr_by_id: FxHashMap<Id, FrIdx>,

    pub images: ImageIndex,
}

impl State {
    // ---- users ----
    #[inline]
    pub fn user(&self, u: UIdx) -> &User {
        &self.users[u as usize]
    }
    pub fn user_idx(&self, id: Id) -> Option<UIdx> {
        self.user_by_id.get(&id).copied()
    }
    /// Lookup by the raw id string (invalid id ⇒ None).
    pub fn user_idx_str(&self, id: &str) -> Option<UIdx> {
        self.user_idx(Id::parse(id)?)
    }
    /// Active (not deleted) user by id string.
    pub fn active_user_str(&self, id: &str) -> Option<UIdx> {
        self.user_idx_str(id).filter(|&u| self.user(u).is_active())
    }
    pub fn user_by_username(&self, name: &str) -> Option<UIdx> {
        self.user_by_name.get(name).copied()
    }
    /// Session token -> active user.
    pub fn session_user(&self, token: Id) -> Option<UIdx> {
        let u = *self.sessions.get(&token)?;
        self.user(u).is_active().then_some(u)
    }

    // ---- windows ----
    #[inline]
    pub fn window(&self, w: WIdx) -> &Window {
        &self.windows[w as usize]
    }
    pub fn window_idx(&self, id: Id) -> Option<WIdx> {
        self.window_by_id.get(&id).copied()
    }
    pub fn open_window_of(&self, r: Region) -> Option<WIdx> {
        self.open_window[r.idx()]
    }

    // ---- posts ----
    #[inline]
    pub fn post(&self, p: PIdx) -> &Post {
        &self.posts[p as usize]
    }
    pub fn post_idx(&self, id: Id) -> Option<PIdx> {
        self.post_by_id.get(&id)
    }
    pub fn post_idx_str(&self, id: &str) -> Option<PIdx> {
        self.post_idx(Id::parse(id)?)
    }
    /// local date of a post = its window's localDate
    pub fn post_date(&self, p: PIdx) -> Date {
        self.window(self.post(p).window).local_date
    }

    // ---- realmojis / comments ----
    #[inline]
    pub fn realmoji(&self, r: RmIdx) -> &RealMoji {
        &self.realmojis[r as usize]
    }
    pub fn realmoji_idx(&self, id: Id) -> Option<RmIdx> {
        self.realmoji_by_id.get(&id)
    }
    pub fn realmoji_idx_str(&self, id: &str) -> Option<RmIdx> {
        self.realmoji_idx(Id::parse(id)?)
    }
    #[inline]
    pub fn comment(&self, c: CIdx) -> &Comment {
        &self.comments[c as usize]
    }
    pub fn comment_idx(&self, id: Id) -> Option<CIdx> {
        self.comment_by_id.get(&id)
    }
    pub fn comment_idx_str(&self, id: &str) -> Option<CIdx> {
        self.comment_idx(Id::parse(id)?)
    }

    // ---- friend requests ----
    #[inline]
    pub fn friend_request(&self, r: FrIdx) -> &FriendRequest {
        &self.friend_requests[r as usize]
    }
    pub fn fr_idx(&self, id: Id) -> Option<FrIdx> {
        self.fr_by_id.get(&id).copied()
    }
    pub fn fr_idx_str(&self, id: &str) -> Option<FrIdx> {
        self.fr_idx(Id::parse(id)?)
    }
    /// The pending request between a and b (either direction), if any.
    pub fn pending_between(&self, a: UIdx, b: UIdx) -> Option<FrIdx> {
        let ua = self.user(a);
        ua.outgoing.get(&b).or_else(|| ua.incoming.get(&b)).copied()
    }

    // ---- admins ----
    pub fn admin_by_username(&self, name: &str) -> Option<u32> {
        self.admins.iter().position(|a| &*a.username == name).map(|i| i as u32)
    }

    /// Short summary for logs/status.
    pub fn summary(&self) -> String {
        format!(
            "users={} posts={} realmojis={} comments={} windows={} friend_requests={} images={} sessions={}",
            self.users.len(),
            self.posts.len(),
            self.realmojis.len(),
            self.comments.len(),
            self.windows.len(),
            self.friend_requests.len(),
            self.images.len(),
            self.sessions.len()
        )
    }
}
