//! Replication (docs/design.md §4): the leader sequences every write; followers forward writes and apply the
//! leader's log in order.

pub mod follower;
pub mod leader;
pub mod proto;

pub use follower::Follower;
pub use leader::Leader;

pub enum Repl {
    Leader(Leader),
    Follower(Follower),
}

/// Where a command came from (for blob dedup and responses).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Local,
    Link { link: u64, req_id: u64 },
}

impl Repl {
    pub fn status_json(&self) -> String {
        match self {
            Repl::Leader(l) => l.status_json(),
            Repl::Follower(f) => f.status_json(),
        }
    }
}

/// Clones the pristine state in the blocking pool.
pub(crate) async fn clone_pristine(app: &'static crate::app::App) -> crate::state::State {
    tokio::task::spawn_blocking(move || (*app.pristine).clone()).await.expect("clone pristine")
}

/// Drops a (big) old state off the async workers.
pub(crate) fn drop_in_background(st: crate::state::State) {
    tokio::task::spawn_blocking(move || drop(st));
}

/// Clones a pristine state in the blocking pool (startup, before the App exists).
pub(crate) async fn clone_pristine_arc(p: &std::sync::Arc<crate::state::State>) -> crate::state::State {
    let p = p.clone();
    tokio::task::spawn_blocking(move || (*p).clone()).await.expect("clone pristine")
}
