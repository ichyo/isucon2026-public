//! Commands (one per state-changing endpoint), their outcome, and the leader-side execution context.
//!
//! `exec_*` functions live in the group files (`api/*.rs`). They are PURE: read `&State`, validate in spec
//! order, generate ids via `tx.new_id()`, use `tx.now`, push `LogEntry`s, return an `Outcome`. They run on the
//! leader under the state write lock, and as a dry run on followers (results discarded) to fail fast.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::api;
use crate::entry::LogEntry;
use crate::http::ApiError;
use crate::model::*;
use crate::state::State;

/// An uploaded image carried by a command: small ones inline (replicated in the log), large ones already
/// stored durably on the origin node (only the reference is replicated). Handlers pass `bytes.into()`;
/// `App::write` converts large inline blobs into `Stored` before the command leaves the origin.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Blob {
    Inline(Bytes),
    Stored(BlobRef),
}

impl From<Bytes> for Blob {
    fn from(b: Bytes) -> Blob {
        Blob::Inline(b)
    }
}

impl Blob {
    pub fn len(&self) -> usize {
        match self {
            Blob::Inline(b) => b.len(),
            Blob::Stored(r) => r.len as usize,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    /// POST /api/initialize (handled by the replication layer, not by exec)
    Initialize,
    /// IP ban detected on some node
    Ban { ip: String, until: Ts },
    /// control-port debug hook: create a session for a user (like a login without the secret)
    DebugSession { user: Id },

    // ---- group A ----
    Register { username: String, display_name: String, region: String },
    Login { username: String, secret_key: String },
    Logout { token: Id },
    UpdateProfile { user: Id, display_name: Option<String>, bio: Option<String>, region: Option<Region> },
    /// validated JPEG (≤512×512)
    SetProfileImage { user: Id, image: Blob },
    DeleteProfileImage { user: Id },
    DeleteAccount { user: Id },
    CreateFriendRequest { actor: Id, recipient: String },
    TransitionFriendRequest { actor: Id, request: String, action: String },
    DeleteFriendship { actor: Id, target: String },
    Follow { actor: Id, target: String },
    Unfollow { actor: Id, target: String },
    Block { actor: Id, target: String },
    Unblock { actor: Id, target: String },
    AdminLogin { username: String, password: String },
    AdminLogout { token: Id },
    Certify { user: String },

    // ---- group B ----
    OpenWindow { region: Region, local_date: Date, on_time_seconds: i64 },
    /// validated JPEG (≤4096×4096) + validated fields
    CreatePost { actor: Id, image: Blob, caption: String, visibility: Visibility, location: Location },
    UpdatePost { actor: Id, post: String, caption: Option<String>, visibility: Option<Visibility> },
    DeletePost { actor: Id, post: String },

    // ---- group C ----
    CreateComment { actor: Id, post: String, body: String },
    DeleteComment { actor: Id, comment: String },
    /// validated JPEG (≤512×512)
    PutRealMoji { actor: Id, post: String, reaction: Reaction, image: Blob },
    DeleteRealMoji { actor: Id, realmoji: String },
}

impl Command {
    /// The image blob carried by this command (at most one).
    pub fn blob(&self) -> Option<&Blob> {
        match self {
            Command::SetProfileImage { image, .. } | Command::CreatePost { image, .. } | Command::PutRealMoji { image, .. } => Some(image),
            _ => None,
        }
    }
    pub fn blob_mut(&mut self) -> Option<&mut Blob> {
        match self {
            Command::SetProfileImage { image, .. } | Command::CreatePost { image, .. } | Command::PutRealMoji { image, .. } => Some(image),
            _ => None,
        }
    }
    pub fn name(&self) -> &'static str {
        match self {
            Command::Initialize => "initialize",
            Command::Ban { .. } => "ban",
            Command::DebugSession { .. } => "debug_session",
            Command::Register { .. } => "register",
            Command::Login { .. } => "login",
            Command::Logout { .. } => "logout",
            Command::UpdateProfile { .. } => "update_me",
            Command::SetProfileImage { .. } => "put_profile_image",
            Command::DeleteProfileImage { .. } => "delete_profile_image",
            Command::DeleteAccount { .. } => "delete_account",
            Command::CreateFriendRequest { .. } => "create_friend_request",
            Command::TransitionFriendRequest { .. } => "transition_friend_request",
            Command::DeleteFriendship { .. } => "delete_friendship",
            Command::Follow { .. } => "follow",
            Command::Unfollow { .. } => "unfollow",
            Command::Block { .. } => "block",
            Command::Unblock { .. } => "unblock",
            Command::AdminLogin { .. } => "admin_login",
            Command::AdminLogout { .. } => "admin_logout",
            Command::Certify { .. } => "certify_user",
            Command::OpenWindow { .. } => "open_posting_window",
            Command::CreatePost { .. } => "create_post",
            Command::UpdatePost { .. } => "update_post",
            Command::DeletePost { .. } => "delete_post",
            Command::CreateComment { .. } => "create_comment",
            Command::DeleteComment { .. } => "delete_comment",
            Command::PutRealMoji { .. } => "replace_real_moji",
            Command::DeleteRealMoji { .. } => "delete_real_moji",
        }
    }
}

/// What the leader tells the origin about a successful command. Everything else needed for the
/// response is read from the (already updated) local state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Outcome {
    Unit,
    Id(Id),
    Register { user: Id, secret_key: String },
    Login { user: Id, token: Id },
}

impl Outcome {
    /// The id carried by `Outcome::Id` (NIL otherwise).
    pub fn id(&self) -> Id {
        match self {
            Outcome::Id(i) => *i,
            Outcome::Register { user, .. } => *user,
            Outcome::Login { token, .. } => *token,
            Outcome::Unit => Id::NIL,
        }
    }
}

/// Leader-side execution context of one command.
pub struct Tx {
    /// leader wall clock at execution (µs) — use for every timestamp of this command
    pub now: Ts,
    pub(crate) entries: Vec<(LogEntry, Option<Blob>)>,
    pub dry_run: bool,
}

impl Tx {
    pub fn new(now: Ts) -> Tx {
        Tx { now, entries: Vec::with_capacity(4), dry_run: false }
    }
    pub fn dry(now: Ts) -> Tx {
        Tx { now, entries: Vec::new(), dry_run: true }
    }
    /// New random UUID v4.
    pub fn new_id(&mut self) -> Id {
        Id::new_v4()
    }
    pub fn push(&mut self, e: LogEntry) {
        self.entries.push((e, None));
    }
    /// Entry whose record carries the command's image blob (PostCreated / RealMojiCreated / ProfileImageSet).
    pub fn push_blob(&mut self, e: LogEntry, blob: Blob) {
        self.entries.push((e, Some(blob)));
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub type ExecResult = Result<Outcome, ApiError>;

/// Dispatches a command to its exec function.
pub fn exec(st: &State, tx: &mut Tx, cmd: &Command) -> ExecResult {
    use crate::api::*;
    match cmd {
        Command::Initialize => Err(ApiError::internal("initialize is not an exec command")),
        Command::Ban { ip, until } => {
            tx.push(LogEntry::IpBanned { ip: ip.clone(), until: *until });
            Ok(Outcome::Unit)
        }
        Command::DebugSession { user } => {
            let u = st.user_idx(*user).filter(|&u| st.user(u).is_active()).ok_or(ApiError::not_found("account_not_found"))?;
            let token = tx.new_id();
            tx.push(LogEntry::SessionCreated { token, user: st.user(u).id, at: tx.now });
            Ok(Outcome::Login { user: *user, token })
        }
        Command::Register { username, display_name, region } => auth::exec_register(st, tx, username, display_name, region),
        Command::Login { username, secret_key } => auth::exec_login(st, tx, username, secret_key),
        Command::Logout { token } => auth::exec_logout(st, tx, *token),
        Command::UpdateProfile { user, display_name, bio, region } => {
            me::exec_update_profile(st, tx, *user, display_name.as_deref(), bio.as_deref(), *region)
        }
        Command::SetProfileImage { user, image } => me::exec_set_profile_image(st, tx, *user, image),
        Command::DeleteProfileImage { user } => me::exec_delete_profile_image(st, tx, *user),
        Command::DeleteAccount { user } => me::exec_delete_account(st, tx, *user),
        Command::CreateFriendRequest { actor, recipient } => relationships::exec_create_friend_request(st, tx, *actor, recipient),
        Command::TransitionFriendRequest { actor, request, action } => {
            relationships::exec_transition_friend_request(st, tx, *actor, request, action)
        }
        Command::DeleteFriendship { actor, target } => relationships::exec_delete_friendship(st, tx, *actor, target),
        Command::Follow { actor, target } => relationships::exec_follow(st, tx, *actor, target),
        Command::Unfollow { actor, target } => relationships::exec_unfollow(st, tx, *actor, target),
        Command::Block { actor, target } => relationships::exec_block(st, tx, *actor, target),
        Command::Unblock { actor, target } => relationships::exec_unblock(st, tx, *actor, target),
        Command::AdminLogin { username, password } => admin::exec_admin_login(st, tx, username, password),
        Command::AdminLogout { token } => admin::exec_admin_logout(st, tx, *token),
        Command::Certify { user } => admin::exec_certify(st, tx, user),
        Command::OpenWindow { region, local_date, on_time_seconds } => windows::exec_open_window(st, tx, *region, *local_date, *on_time_seconds),
        Command::CreatePost { actor, image, caption, visibility, location } => {
            posts::exec_create_post(st, tx, *actor, image, caption, *visibility, location)
        }
        Command::UpdatePost { actor, post, caption, visibility } => posts::exec_update_post(st, tx, *actor, post, caption.as_deref(), *visibility),
        Command::DeletePost { actor, post } => posts::exec_delete_post(st, tx, *actor, post),
        Command::CreateComment { actor, post, body } => comments::exec_create_comment(st, tx, *actor, post, body),
        Command::DeleteComment { actor, comment } => comments::exec_delete_comment(st, tx, *actor, comment),
        Command::PutRealMoji { actor, post, reaction, image } => realmojis::exec_put_realmoji(st, tx, *actor, post, *reaction, image),
        Command::DeleteRealMoji { actor, realmoji } => realmojis::exec_delete_realmoji(st, tx, *actor, realmoji),
    }
}

#[allow(unused_imports)]
use api as _;
