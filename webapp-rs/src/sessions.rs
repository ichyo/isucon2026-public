//! Sessions have one authority: the primary node of the user's current region. Ordinary issue/revoke
//! operations only fsync that node's journal. Migration durably freezes the old generation, then
//! transfers its token through the global synchronous WAL. There is no positive authentication cache.
use crate::app::{App, AuthUser, WaitPolicy};
use crate::command::{Command, Outcome, Tx};
use crate::http::ApiError;
use crate::model::{now_us, Id, NodeId, Region, User};
use crate::repl::Repl;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Default)]
pub struct UserLocks(Mutex<FxHashMap<Id, Arc<tokio::sync::Mutex<()>>>>);
impl UserLocks {
    fn get(&self, user: Id) -> Arc<tokio::sync::Mutex<()>> {
        self.0.lock().entry(user).or_default().clone()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub epoch: u64,
    pub user: Option<Id>,
    pub op: Operation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Login { username: String, secret_key: String },
    Debug,
    Validate { token: Id },
    Logout { token: Id },
    Profile { region: Region, display_name: Option<String>, bio: Option<String> },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Reply {
    Redirect { node: NodeId, user: Id },
    Auth { user: Id, region: Region },
    Outcome(Outcome),
}
fn invalid() -> ApiError {
    ApiError::unauthorized("invalid_session")
}
fn changed() -> ApiError {
    ApiError::unavailable("session ownership changed")
}
fn ready(app: &App) -> Result<(), ApiError> {
    if !app.session_store.healthy() {
        return Err(ApiError::unavailable("session journal unavailable"));
    }
    if matches!(&app.repl, Repl::Follower(f) if !f.in_sync()) {
        return Err(ApiError::unavailable("session owner not in sync"));
    }
    Ok(())
}
/// Two routing bits, with 120 random bits remaining. The journal decides validity.
fn new_token(region: Region) -> Id {
    let mut token = Id::new_v4();
    token.lo = (token.lo & !3) | region.idx() as u64;
    token
}
fn issuer(token: Id) -> Result<NodeId, ApiError> {
    match token.lo & 3 {
        0 => Ok(NodeId::ASIA1),
        1 => Ok(NodeId::EUROPE1),
        2 => Ok(NodeId::AMERICA1),
        _ => Err(invalid()),
    }
}
fn current(app: &App, epoch: u64, user: &User) -> Option<Id> {
    app.session_store.get(epoch, user.id, user.session_generation).unwrap_or(user.session)
}
/// These are only identity/routing hints, never evidence that a token is active.
fn identify(app: &App, st: &crate::state::State, token: Id) -> Option<Id> {
    st.sessions.get(&token).map(|&u| st.user(u).id).or_else(|| app.session_store.identify(st.epoch, token))
}
pub fn is_regional(cmd: &Command) -> bool {
    matches!(cmd, Command::Login { .. } | Command::Logout { .. } | Command::DebugSession { .. } | Command::UpdateProfile { region: Some(_), .. })
}
pub async fn write(app: &'static App, cmd: Command) -> Result<Outcome, ApiError> {
    ready(app)?;
    let (request, node) = {
        let st = app.read();
        let (user, op) = match cmd {
            Command::Login { username, secret_key } => {
                // Preserve abuse/credential checks and reject oversized invalid secrets before RPC.
                let Outcome::Login { user, .. } = crate::api::auth::exec_login(&st, &mut Tx::dry(now_us()), &username, &secret_key)? else {
                    return Err(ApiError::internal_unknown());
                };
                (Some(user), Operation::Login { username, secret_key })
            }
            Command::DebugSession { user } => (Some(user), Operation::Debug),
            Command::Logout { token } => (identify(app, &st, token), Operation::Logout { token }),
            Command::UpdateProfile { user, display_name, bio, region: Some(region) } => (Some(user), Operation::Profile { region, display_name, bio }),
            _ => return Err(ApiError::internal("not a regional session operation")),
        };
        let node = if let Some(user) = user {
            let u = st.user_idx(user).ok_or_else(invalid)?;
            NodeId::primary(st.user(u).region)
        } else if let Operation::Logout { token } = op {
            issuer(token)?
        } else {
            return Err(invalid());
        };
        (Request { epoch: st.epoch, user, op }, node)
    };
    match route(app, node, request).await? {
        Reply::Outcome(out) => Ok(out),
        _ => Err(ApiError::internal_unknown()),
    }
}
pub async fn authenticate(app: &'static App, token: Id) -> Result<AuthUser, ApiError> {
    ready(app)?;
    let (request, node) = {
        let st = app.read();
        let user = identify(app, &st, token);
        let node = match user.and_then(|id| st.user_idx(id)) {
            Some(u) => NodeId::primary(st.user(u).region),
            None => issuer(token)?,
        };
        (Request { epoch: st.epoch, user, op: Operation::Validate { token } }, node)
    };
    let epoch = request.epoch;
    let Reply::Auth { user, region } = route(app, node, request).await? else {
        return Err(ApiError::internal_unknown());
    };
    let st = app.read();
    if st.epoch != epoch {
        return Err(changed());
    }
    let idx = st.user_idx(user).filter(|&u| st.user(u).is_active()).ok_or_else(invalid)?;
    if st.user(idx).region != region {
        return Err(changed());
    }
    Ok(AuthUser { idx, id: user, region, token })
}
async fn route(app: &'static App, mut node: NodeId, mut request: Request) -> Result<Reply, ApiError> {
    for _ in 0..8 {
        let reply = if node == app.cfg.node {
            // Caller cancellation must not drop an in-flight migration's owner lock.
            let request = request.clone();
            tokio::spawn(async move { handle(app, request).await }).await.map_err(ApiError::internal)??
        } else {
            app.session_net.call(node, request.clone()).await?
        };
        match reply {
            Reply::Redirect { node: next, user } => {
                node = next;
                request.user = Some(user);
            }
            reply => return Ok(reply),
        }
    }
    Err(changed())
}
/// Internal RPC execution also passes the follower freshness gate.
pub async fn handle(app: &'static App, request: Request) -> Result<Reply, ApiError> {
    ready(app)?;
    let user = {
        let st = app.read();
        if st.epoch != request.epoch {
            return Err(changed());
        }
        request
            .user
            .or_else(|| match request.op {
                Operation::Validate { token } | Operation::Logout { token } => identify(app, &st, token),
                _ => None,
            })
            .ok_or_else(invalid)?
    };
    let lock = app.session_locks.get(user);
    let _guard = lock.lock().await;
    ready(app)?;
    // Resume durable migration intent after a lost acknowledgement/restart. Only one copy of its
    // fenced command can advance the generation. Login/logout cannot pass this freeze.
    let pending = {
        let st = app.read();
        if st.epoch != request.epoch {
            return Err(changed());
        }
        let u = st.user_idx(user).filter(|&u| st.user(u).is_active()).ok_or_else(invalid)?;
        let usr = st.user(u);
        if NodeId::primary(usr.region) != app.cfg.node {
            return Ok(Reply::Redirect { node: NodeId::primary(usr.region), user });
        }
        app.session_store.pending(st.epoch, user, usr.session_generation)
    };
    if let Some(cmd) = pending {
        // Resubmit only on the global business-data path.
        if let Err(e) = app.write_replicated(cmd, WaitPolicy::AllReplicas).await {
            let st = app.read();
            let still_pending = st.epoch == request.epoch
                && st.user_idx(user).is_some_and(|u| app.session_store.pending(st.epoch, user, st.user(u).session_generation).is_some());
            if still_pending {
                return Err(e);
            }
        }
    }
    // Normal authentication is an in-memory read at the authority, with no blocking worker or fsync.
    if let Operation::Validate { token } = &request.op {
        ready(app)?;
        let st = app.read();
        if st.epoch != request.epoch {
            return Err(changed());
        }
        let u = st.user_idx(user).filter(|&u| st.user(u).is_active()).ok_or_else(invalid)?;
        let usr = st.user(u);
        let owner = NodeId::primary(usr.region);
        if owner != app.cfg.node {
            return Ok(Reply::Redirect { node: owner, user });
        }
        if current(app, st.epoch, usr) != Some(*token) {
            return Err(invalid());
        }
        return Ok(Reply::Auth { user, region: usr.region });
    }
    let op = request.op.clone();
    // Hold the state read guard until the journal is durable. Initialize/deletion/transfer cannot
    // interleave validation and publication; disk I/O stays off the async executor.
    let result = tokio::task::spawn_blocking(move || -> Result<Local, ApiError> {
        ready(app)?;
        let st = app.read();
        if st.epoch != request.epoch {
            return Err(changed());
        }
        let u = st.user_idx(user).filter(|&u| st.user(u).is_active()).ok_or_else(invalid)?;
        let usr = st.user(u);
        let owner = NodeId::primary(usr.region);
        if owner != app.cfg.node {
            return Ok(Local::Reply(Reply::Redirect { node: owner, user }));
        }
        let token = current(app, st.epoch, usr);
        let out = match op {
            Operation::Validate { token: presented } => {
                if token != Some(presented) {
                    return Err(invalid());
                }
                return Ok(Local::Reply(Reply::Auth { user, region: usr.region }));
            }
            Operation::Logout { token: presented } => {
                if token != Some(presented) {
                    return Err(invalid());
                }
                app.session_store.set(st.epoch, user, usr.session_generation, None).map_err(ApiError::internal)?;
                Outcome::Unit
            }
            Operation::Login { username, secret_key } => {
                let Outcome::Login { user: found, .. } = crate::api::auth::exec_login(&st, &mut Tx::dry(now_us()), &username, &secret_key)? else {
                    return Err(ApiError::internal_unknown());
                };
                if found != user {
                    return Err(invalid());
                }
                let token = new_token(usr.region);
                app.session_store.set(st.epoch, user, usr.session_generation, Some(token)).map_err(ApiError::internal)?;
                Outcome::Login { user, token }
            }
            Operation::Debug => {
                let token = new_token(usr.region);
                app.session_store.set(st.epoch, user, usr.session_generation, Some(token)).map_err(ApiError::internal)?;
                Outcome::Login { user, token }
            }
            Operation::Profile { region, display_name, bio } => {
                if region == usr.region {
                    return Ok(Local::Global(Command::UpdateProfile { user, display_name, bio, region: None }));
                }
                let cmd =
                    Command::MoveSession { epoch: st.epoch, user, from: usr.region, generation: usr.session_generation, token, region, display_name, bio };
                app.session_store.freeze(st.epoch, user, usr.session_generation, cmd.clone()).map_err(ApiError::internal)?;
                return Ok(Local::Global(cmd));
            }
        };
        // Creation/handoff metadata may have been applied before its fsync completed. A successful
        // session mutation must survive together with the metadata that gives it meaning.
        let lsn = app.wal.inner.lock().lsn;
        Ok(Local::Durable(Reply::Outcome(out), lsn))
    })
    .await
    .map_err(ApiError::internal)??;
    match result {
        Local::Reply(reply) => Ok(reply),
        Local::Durable(reply, lsn) => {
            app.wal.wait_synced(lsn).await;
            Ok(reply)
        }
        Local::Global(cmd) => Ok(Reply::Outcome(app.write_replicated(cmd, WaitPolicy::AllReplicas).await?)),
    }
}
enum Local {
    Reply(Reply),
    Durable(Reply, u64),
    Global(Command),
}
