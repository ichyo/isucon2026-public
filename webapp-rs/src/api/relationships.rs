//! Group A: friend requests, friendships, follows, blocks, relationship lists (§6.10).

use super::auth::active_actor;
use super::prelude::*;

fn conflict() -> ApiError {
    ApiError::conflict("relationship_conflict")
}
fn not_found() -> ApiError {
    ApiError::not_found("relationship_not_found")
}
fn action_forbidden() -> ApiError {
    ApiError::forbidden("relationship_action_forbidden")
}

/// Common "relationship target" precondition: empty or the actor itself ⇒ 400 invalid_relationship;
/// missing (incl. not a UUID) or deleted ⇒ 404 relationship_not_found.
fn target_user(st: &State, actor: UIdx, target: &str) -> Result<UIdx, ApiError> {
    if target.is_empty() {
        return Err(ApiError::bad_request("invalid_relationship"));
    }
    let id = Id::parse(target);
    if id == Some(st.user(actor).id) {
        return Err(ApiError::bad_request("invalid_relationship"));
    }
    id.and_then(|i| st.user_idx(i)).filter(|&t| st.user(t).is_active()).ok_or_else(not_found)
}

/// Friendship rows are stored as (low id, high id).
fn ordered(a: Id, b: Id) -> (Id, Id) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// Handler shape shared by the 204 relationship writes on `/{userId}`: auth (pinned), path param, write.
async fn target_write(ctx: Ctx, req: Req, cmd: impl FnOnce(Id, String) -> Command) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let target = req.param(0).to_string();
    ctx.write(cmd(me.id, target)).await?;
    Ok(Resp::no_content())
}

/// Renders `FriendRequestState` of the request written by the command (from the local, updated state).
async fn friend_request_response(ctx: Ctx, status: u16, request: Id) -> HResult {
    ctx.respond(status, |st, w| {
        let r = st.fr_idx(request).ok_or_else(ApiError::internal_unknown)?;
        views::friend_request_state(w, st.friend_request(r));
        Ok(())
    })
    .await
}

// POST /api/friend-requests — auth user, pinned. Body {"recipientId"}. 201 FriendRequestState.
pub async fn create_friend_request(ctx: Ctx, mut req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let body = req.bind().await?;
    let recipient = body.str("recipientId")?.unwrap_or_default();
    let out = ctx.write(Command::CreateFriendRequest { actor: me.id, recipient }).await?;
    friend_request_response(ctx, 201, out.id()).await
}

/// Emits `FriendRequestCreated`, or (reverse pending request) `FriendRequestUpdated{Accepted}` + `FriendshipCreated`.
/// Outcome::Id(request id).
pub fn exec_create_friend_request(st: &State, tx: &mut Tx, actor: Id, recipient: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let b = target_user(st, a, recipient)?;
    if rules::blocked(st, a, b) || rules::is_friend(st, a, b) {
        return Err(conflict());
    }
    let b_id = st.user(b).id;
    if let Some(r) = st.pending_between(a, b) {
        let fr = st.friend_request(r);
        if fr.requester == a {
            return Err(conflict());
        }
        // the recipient already asked the actor: accept that request
        tx.push(LogEntry::FriendRequestUpdated { id: fr.id, status: FrStatus::Accepted, at: tx.now });
        let id = tx.new_id();
        let (lo, hi) = ordered(actor, b_id);
        tx.push(LogEntry::FriendshipCreated { id, a: lo, b: hi, source_request: Some(fr.id), at: tx.now });
        return Ok(Outcome::Id(fr.id));
    }
    let id = tx.new_id();
    tx.push(LogEntry::FriendRequestCreated { id, requester: actor, recipient: b_id, at: tx.now });
    Ok(Outcome::Id(id))
}

// POST /api/friend-requests/{requestId}/{action} — auth user, pinned. 200 FriendRequestState.
pub async fn transition_friend_request(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let request = req.param(0).to_string();
    let action = req.param(1).to_string();
    let out = ctx.write(Command::TransitionFriendRequest { actor: me.id, request, action }).await?;
    friend_request_response(ctx, 200, out.id()).await
}

/// Emits `FriendRequestUpdated` (+ `FriendshipCreated` on accept). Checks in spec order: action, request
/// exists, actor's role, both parties active, still pending, (accept) not blocked.
pub fn exec_transition_friend_request(st: &State, tx: &mut Tx, actor: Id, request: &str, action: &str) -> ExecResult {
    let status = match action {
        "accept" => FrStatus::Accepted,
        "reject" => FrStatus::Rejected,
        "cancel" => FrStatus::Cancelled,
        _ => return Err(action_forbidden()),
    };
    let a = active_actor(st, actor)?;
    let r = st.fr_idx_str(request).ok_or_else(not_found)?;
    let fr = *st.friend_request(r);
    let allowed = if status == FrStatus::Cancelled { fr.requester == a } else { fr.recipient == a };
    if !allowed {
        return Err(action_forbidden());
    }
    if !st.user(fr.requester).is_active() || !st.user(fr.recipient).is_active() {
        return Err(not_found());
    }
    if fr.status != FrStatus::Pending {
        return Err(action_forbidden());
    }
    if status == FrStatus::Accepted && rules::blocked(st, fr.requester, fr.recipient) {
        return Err(conflict());
    }
    tx.push(LogEntry::FriendRequestUpdated { id: fr.id, status, at: tx.now });
    // (a pending request never coexists with a friendship; keep an existing one untouched just in case)
    if status == FrStatus::Accepted && !rules::is_friend(st, fr.requester, fr.recipient) {
        let id = tx.new_id();
        let (lo, hi) = ordered(st.user(fr.requester).id, st.user(fr.recipient).id);
        tx.push(LogEntry::FriendshipCreated { id, a: lo, b: hi, source_request: Some(fr.id), at: tx.now });
    }
    Ok(Outcome::Id(fr.id))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Friends,
    Following,
    Followers,
    Incoming,
    Outgoing,
}

impl Kind {
    fn parse(s: &str) -> Option<Kind> {
        match s {
            "friends" => Some(Kind::Friends),
            "following" => Some(Kind::Following),
            "followers" => Some(Kind::Followers),
            "incoming-requests" => Some(Kind::Incoming),
            "outgoing-requests" => Some(Kind::Outgoing),
            _ => None,
        }
    }
    fn status(self) -> &'static str {
        match self {
            Kind::Friends => "friends",
            Kind::Following => "following",
            Kind::Followers => "followers",
            Kind::Incoming | Kind::Outgoing => "pending",
        }
    }
}

/// `(updatedAt, relationId) < (c_at, c_id)`; ids compare as byte strings (lowercase form).
#[inline]
fn before_cursor(at: Ts, id: Id, c_at: Ts, c_id: &str) -> bool {
    at < c_at || (at == c_at && id.to_buf()[..] < *c_id.as_bytes())
}

// GET /api/relationships/{kind}[?beforeUpdatedAt=&beforeId=] — auth user, pinned.
// Rows whose other user is active, ordered by (updatedAt, relationId) desc.
pub async fn get_relationships(ctx: Ctx, req: Req) -> HResult {
    let me = ctx.auth_pinned(&req).await?;
    let before_at = req.query_time("beforeUpdatedAt")?; // empty / garbage ⇒ 400 bind
    let before_id = req.query_nonempty("beforeId");
    if before_at.is_some() != before_id.is_some() {
        return Err(ApiError::bad_request("invalid_pagination"));
    }
    let kind = Kind::parse(req.param(0)).ok_or(ApiError::bad_request("invalid_relationship"))?;
    let cursor = before_at.zip(before_id);
    ctx.respond(200, |st, w| {
        let u = st.user(me.idx);
        let mut rows: Vec<(Ts, Id, UIdx)> = match kind {
            Kind::Friends => u.friends.iter().map(|(&o, r)| (r.at, r.id, o)).collect(),
            Kind::Following => u.following.iter().map(|(&o, r)| (r.at, r.id, o)).collect(),
            Kind::Followers => u.followers.iter().map(|(&o, r)| (r.at, r.id, o)).collect(),
            Kind::Incoming | Kind::Outgoing => {
                let m = if kind == Kind::Incoming { &u.incoming } else { &u.outgoing };
                m.iter()
                    .map(|(&o, &r)| {
                        let fr = st.friend_request(r);
                        (fr.updated_at, fr.id, o)
                    })
                    .collect()
            }
        };
        rows.retain(|&(at, id, o)| st.user(o).is_active() && cursor.as_ref().is_none_or(|(c_at, c_id)| before_cursor(at, id, *c_at, c_id)));
        let desc = |x: &(Ts, Id, UIdx), y: &(Ts, Id, UIdx)| (y.0, y.1).cmp(&(x.0, x.1));
        if rows.len() > PAGE_SIZE + 1 {
            rows.select_nth_unstable_by(PAGE_SIZE, desc);
            rows.truncate(PAGE_SIZE + 1);
        }
        rows.sort_unstable_by(desc);
        let (items, more) = paginate(rows.into_iter(), PAGE_SIZE);
        let status = kind.status();
        views::page(w, &items, more, |w, &(at, id, o)| {
            let other = st.user(o);
            let show_bio = kind == Kind::Friends || other.account_type == AccountType::CertifiedPublic;
            views::relationship(w, id, other, show_bio, status, at);
        });
        Ok(())
    })
    .await
}

// DELETE /api/friendships/{userId} — auth user, pinned. 204.
pub async fn delete_friendship(ctx: Ctx, req: Req) -> HResult {
    target_write(ctx, req, |actor, target| Command::DeleteFriendship { actor, target }).await
}

/// Emits `FriendshipRemoved`.
pub fn exec_delete_friendship(st: &State, tx: &mut Tx, actor: Id, target: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let t = target_user(st, a, target)?;
    if !rules::is_friend(st, a, t) {
        return Err(conflict());
    }
    let (lo, hi) = ordered(actor, st.user(t).id);
    tx.push(LogEntry::FriendshipRemoved { a: lo, b: hi });
    Ok(Outcome::Unit)
}

// PUT /api/follows/{userId} — auth user, pinned. 204.
pub async fn follow(ctx: Ctx, req: Req) -> HResult {
    target_write(ctx, req, |actor, target| Command::Follow { actor, target }).await
}

/// Emits `FollowCreated`. Target must be certified, not blocked either way, not yet followed (409 each).
pub fn exec_follow(st: &State, tx: &mut Tx, actor: Id, target: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let t = target_user(st, a, target)?;
    if st.user(t).account_type != AccountType::CertifiedPublic || rules::blocked(st, a, t) || st.user(a).following.contains_key(&t) {
        return Err(conflict());
    }
    let id = tx.new_id();
    tx.push(LogEntry::FollowCreated { id, follower: actor, followee: st.user(t).id, at: tx.now });
    Ok(Outcome::Unit)
}

// DELETE /api/follows/{userId} — auth user, pinned. 204.
pub async fn unfollow(ctx: Ctx, req: Req) -> HResult {
    target_write(ctx, req, |actor, target| Command::Unfollow { actor, target }).await
}

/// Emits `FollowRemoved`.
pub fn exec_unfollow(st: &State, tx: &mut Tx, actor: Id, target: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let t = target_user(st, a, target)?;
    if !st.user(a).following.contains_key(&t) {
        return Err(conflict());
    }
    tx.push(LogEntry::FollowRemoved { follower: actor, followee: st.user(t).id });
    Ok(Outcome::Unit)
}

// PUT /api/blocks/{userId} — auth user, pinned. 204.
pub async fn block(ctx: Ctx, req: Req) -> HResult {
    target_write(ctx, req, |actor, target| Command::Block { actor, target }).await
}

/// Emits `BlockCreated` (apply performs the cascade: friendship, follows both ways, pending requests
/// either way ⇒ cancelled). A reverse block does not prevent it.
pub fn exec_block(st: &State, tx: &mut Tx, actor: Id, target: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let t = target_user(st, a, target)?;
    if st.user(a).blocking.contains_key(&t) {
        return Err(conflict());
    }
    let id = tx.new_id();
    tx.push(LogEntry::BlockCreated { id, blocker: actor, blocked: st.user(t).id, at: tx.now });
    Ok(Outcome::Unit)
}

// DELETE /api/blocks/{userId} — auth user, pinned. 204.
pub async fn unblock(ctx: Ctx, req: Req) -> HResult {
    target_write(ctx, req, |actor, target| Command::Unblock { actor, target }).await
}

/// Emits `BlockRemoved`. Not blocking ⇒ 403 relationship_action_forbidden.
pub fn exec_unblock(st: &State, tx: &mut Tx, actor: Id, target: &str) -> ExecResult {
    let a = active_actor(st, actor)?;
    let t = target_user(st, a, target)?;
    if !st.user(a).blocking.contains_key(&t) {
        return Err(action_forbidden());
    }
    tx.push(LogEntry::BlockRemoved { blocker: actor, blocked: st.user(t).id });
    Ok(Outcome::Unit)
}
