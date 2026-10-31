//! Endpoint handlers. File ownership: docs/design.md §1 / §15.

pub mod admin;
pub mod auth;
pub mod comments;
pub mod export;
pub mod feeds;
pub mod images;
pub mod initialize;
pub mod me;
pub mod memories;
pub mod notifications;
pub mod posts;
pub mod realmojis;
pub mod relationships;
pub mod users;
pub mod windows;

/// Everything a handler file usually needs: `use crate::api::prelude::*;`
pub mod prelude {
    pub use crate::app::{AuthAdmin, AuthUser, Ctx};
    pub use crate::command::{Command, ExecResult, Outcome, Tx};
    pub use crate::entry::LogEntry;
    pub use crate::http::{Abuse, ApiError, Body, HResult, Req, Resp};
    pub use crate::json::JsonOut;
    pub use crate::model::*;
    pub use crate::state::State;
    pub use crate::views::{paginate, PAGE_SIZE};
    pub use crate::{jpeg, rules, sign, text, timefmt, views};
    pub use bytes::Bytes;
}
