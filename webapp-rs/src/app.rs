//! `App` = everything a handler needs (`Ctx = &'static App`): state, auth helpers, the write path,
//! JSON response helpers.

use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};

use crate::apply::Effect;
use crate::bans::Bans;
use crate::command::{self, Blob, Command, Outcome, Tx};
use crate::config::Config;
use crate::http::router::Router;
use crate::http::{Abuse, ApiError, HResult, Req, Resp};
use crate::images::ImageStore;
use crate::json::JsonOut;
use crate::model::*;
use crate::repl::Repl;
use crate::sign::Signer;
use crate::sse::SseHub;
use crate::state::State;
use crate::static_files::StaticFiles;
use crate::throttle::Throttle;
use crate::wal::Wal;

pub type Ctx = &'static App;

pub struct App {
    pub cfg: Config,
    pub state: RwLock<State>,
    /// the initial dataset, never mutated (initialize clones it)
    pub pristine: Arc<State>,
    pub wal: Wal,
    pub images: ImageStore,
    pub signer: Signer,
    pub sse: SseHub,
    pub bans: Bans,
    pub throttle: Throttle,
    pub router: Router,
    pub statics: StaticFiles,
    pub repl: Repl,
}

/// The authenticated session user (copied out of the state; safe to keep across awaits).
#[derive(Clone, Copy, Debug)]
pub struct AuthUser {
    pub idx: UIdx,
    pub id: Id,
    pub region: Region,
    pub token: Id,
}

#[derive(Clone, Copy, Debug)]
pub struct AuthAdmin {
    pub idx: u32,
    pub id: Id,
    pub token: Id,
}

/// How long a write waits before it is acknowledged to the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitPolicy {
    /// leader WAL durable + every in-sync follower applied it (strict read-your-writes) — default
    AllReplicas,
    /// leader applied + durable only (reserved for future async additive writes)
    LeaderOnly,
}

fn bearer(req: &Req) -> Option<&str> {
    req.header("authorization")?.strip_prefix("Bearer ")
}

impl App {
    /// Read guard on the state. Never hold it across `.await` (the compiler rejects it: the guard is !Send —
    /// and `drop(guard)` before an await is NOT enough for the checker; use a block scope or `respond`).
    /// Do not take a second guard (or call `write`) while holding one.
    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, State> {
        self.state.read()
    }

    /// This server's fixed region.
    pub fn region(&self) -> Region {
        self.cfg.region
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.repl, Repl::Leader(_))
    }

    /// `Authorization: Bearer <token>` of an active session of an active user, else 401 invalid_session.
    pub fn auth_user(&self, req: &Req) -> Result<AuthUser, ApiError> {
        let err = || ApiError::unauthorized("invalid_session");
        let token = Id::parse(bearer(req).ok_or_else(err)?).ok_or_else(err)?;
        let st = self.state.read_recursive();
        let u = st.session_user(token).ok_or_else(err)?;
        let usr = st.user(u);
        Ok(AuthUser { idx: u, id: usr.id, region: usr.region, token })
    }

    /// `auth_user` + region pinning (403 region_mismatch).
    pub fn auth_pinned(&self, req: &Req) -> Result<AuthUser, ApiError> {
        let a = self.auth_user(req)?;
        if a.region != self.cfg.region {
            return Err(ApiError::forbidden("region_mismatch"));
        }
        Ok(a)
    }

    /// Admin session (401 invalid_administrator_session).
    pub fn auth_admin(&self, req: &Req) -> Result<AuthAdmin, ApiError> {
        let err = || ApiError::unauthorized("invalid_administrator_session");
        let token = Id::parse(bearer(req).ok_or_else(err)?).ok_or_else(err)?;
        let st = self.state.read_recursive();
        let a = *st.admin_sessions.get(&token).ok_or_else(err)?;
        Ok(AuthAdmin { idx: a, id: st.admins[a as usize].id, token })
    }

    /// A JSON writer whose image URLs use this response's `exp`.
    pub fn json(&'static self) -> JsonOut {
        let exp = self.signer.exp_for(now_us().div_euclid(1_000_000));
        JsonOut::new(exp, &self.signer)
    }

    /// Builds JSON under the read lock (the guard never crosses an await). See `respond`.
    pub fn render(&'static self, f: impl FnOnce(&State, &mut JsonOut) -> Result<(), ApiError>) -> Result<JsonOut, ApiError> {
        let mut w = self.json();
        let st = self.read();
        f(&st, &mut w)?;
        Ok(w)
    }

    /// `render` + `finish`: the usual way to answer a JSON endpoint.
    /// ```ignore
    /// ctx.respond(200, |st, w| { views::me(w, st.user(me.idx)); Ok(()) }).await
    /// ```
    pub async fn respond(&'static self, status: u16, f: impl FnOnce(&State, &mut JsonOut) -> Result<(), ApiError>) -> HResult {
        let w = self.render(f)?;
        self.finish(w, status).await
    }

    /// Finishes a JSON response: computes missing image signatures in the blocking pool (off-lock),
    /// appends `\n`, `Content-Type: application/json`.
    pub async fn finish(&'static self, w: JsonOut, status: u16) -> HResult {
        let body = if w.pending_signatures() == 0 {
            w.finish()
        } else {
            tokio::task::spawn_blocking(move || w.finish()).await.map_err(ApiError::internal)?
        };
        Ok(Resp::json(status, body))
    }

    /// The write path (docs/design.md §3): leader executes + replicates; followers dry-run locally and
    /// forward to the leader. Returns after the entries are applied locally (and on all in-sync replicas).
    pub async fn write(&'static self, cmd: Command) -> Result<Outcome, ApiError> {
        self.write_with(cmd, WaitPolicy::AllReplicas).await
    }

    pub async fn write_with(&'static self, mut cmd: Command, policy: WaitPolicy) -> Result<Outcome, ApiError> {
        let big = matches!(cmd.blob(), Some(Blob::Inline(b)) if b.len() > self.cfg.blob_inline_max);
        let dry = !matches!(cmd, Command::Initialize | Command::Ban { .. }) && (!self.is_leader() || big);
        if dry {
            // fail fast (no RTT, no blob write) on errors the local replica can already see
            let st = self.read();
            let mut tx = Tx::dry(now_us());
            command::exec(&st, &mut tx, &cmd)?;
        }
        if big {
            // large uploads stay on this (origin) node: durable before the write is submitted; the log only
            // carries the reference, other nodes fetch the bytes on demand (blobs.rs)
            if let Some(slot) = cmd.blob_mut() {
                if let Blob::Inline(b) = slot {
                    let r = self.images.blobs.put(b.clone(), self.cfg.region).await.map_err(|e| ApiError::internal(format!("blob store: {e}")))?;
                    *slot = Blob::Stored(r);
                }
            }
        }
        match &self.repl {
            Repl::Leader(l) => l.submit(self, cmd, crate::repl::Origin::Local, policy).await,
            Repl::Follower(f) => f.forward(cmd).await,
        }
    }

    /// Effects of applied entries; called with the write lock held (`st` = the new state).
    pub fn handle_effects(&self, st: &State, fx: &[Effect]) {
        for e in fx {
            match e {
                Effect::WindowOpened(w) => self.sse.on_window_opened(st, *w),
                Effect::UserGone(u) => self.sse.on_user_gone(*u),
                Effect::Ban { ip, until } => self.bans.set(ip, *until),
            }
        }
    }

    /// Feeds the ban detector; on a ban, enforces it locally at once and replicates it asynchronously.
    pub fn on_abuse(&'static self, ip: &str, a: Abuse) {
        if let Some(until) = self.bans.record(ip, a, now_us()) {
            eprintln!("[ban] {ip} banned until {} ({a:?})", crate::timefmt::format_ts(until));
            self.bans.set(ip, until);
            let ip = ip.to_string();
            tokio::spawn(async move {
                if let Err(e) = self.write(Command::Ban { ip, until }).await {
                    eprintln!("[ban] replicate failed: {} {}", e.status, e.code);
                }
            });
        }
    }

    /// Resets the local state to the pristine dataset (used by initialize on every node).
    /// Must be called with the clone already prepared; returns the old state (drop it off-thread).
    pub fn swap_state(&self, st: &mut State, fresh: State) -> State {
        self.bans.clear();
        self.signer.clear();
        self.sse.close_all();
        std::mem::replace(st, fresh)
    }

    pub fn status_json(&self) -> String {
        let (epoch, seq, end) = {
            let w = self.wal.inner.lock();
            (w.epoch, w.last_seq, w.end)
        };
        let summary = self.read().summary();
        let (ms, fms) = self.throttle.get();
        format!(
            "{{\"region\":\"{}\",\"leader_region\":\"{}\",\"role\":\"{}\",\"epoch\":{},\"seq\":{},\"wal_bytes\":{},\"repl\":{},\"blobs\":{},\"state\":\"{}\",\"throttle_ms\":{},\"throttle_feed_ms\":{},\"bans\":{},\"sse_subscribers\":{},\"sig_cache\":{}}}",
            self.cfg.region.as_str(),
            self.cfg.leader_region.as_str(),
            if self.is_leader() { "leader" } else { "follower" },
            epoch,
            seq,
            end,
            self.repl.status_json(),
            self.images.net.status_json(&self.images.blobs, &self.images.cache),
            summary,
            ms,
            fms,
            self.bans.active(now_us()),
            self.sse.len(),
            self.signer.len()
        )
    }
}
