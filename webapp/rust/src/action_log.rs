use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;
use std::sync::{Arc, LazyLock, Mutex};

use axum::extract::{FromRequestParts, MatchedPath, Request, State};
use axum::http::Method;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::Response;
use serde_json::{Map, Value};

use crate::api_error::AbuseEvent;
use crate::ip_ban::extract_client_ip;
use crate::session::Session;
use crate::user::USERNAME_PATTERN;

static ACTION_NAMES: LazyLock<HashMap<&str, &str>> = LazyLock::new(|| {
    HashMap::from([
        ("POST /api/initialize", "initialize"),
        ("POST /api/auth/register", "register"),
        ("POST /api/auth/login", "login"),
        ("POST /api/auth/logout", "logout"),
        ("GET /api/users", "search_users"),
        ("GET /api/users/{userId}", "get_user"),
        ("GET /api/me/export", "export_posts"),
        ("GET /api/me", "get_me"),
        ("PATCH /api/me", "update_me"),
        ("PUT /api/me/profile-image", "put_profile_image"),
        ("DELETE /api/me/profile-image", "delete_profile_image"),
        ("POST /api/me/account-deletion", "delete_account"),
        ("POST /api/posts", "create_post"),
        (
            "GET /api/posting-window/current",
            "get_current_posting_window",
        ),
        ("GET /api/posts/me/current", "get_my_current_post"),
        ("GET /api/posts/{postId}", "get_post"),
        ("PATCH /api/posts/{postId}", "update_post"),
        ("DELETE /api/posts/{postId}", "delete_post"),
        ("GET /api/feeds/{feed}", "get_feed"),
        ("GET /api/memories", "get_memories"),
        ("GET /api/memories/{date}", "get_memories_by_date"),
        ("POST /api/posts/{postId}/comments", "create_comment"),
        ("GET /api/posts/{postId}/comments", "get_comments"),
        ("DELETE /api/comments/{commentId}", "delete_comment"),
        ("POST /api/posts/{postId}/realmojis", "replace_real_moji"),
        ("GET /api/posts/{postId}/realmojis", "get_real_mojis"),
        ("DELETE /api/realmojis/{realMojiId}", "delete_real_moji"),
        ("GET /api/images/{imageId}", "get_image"),
        ("GET /api/notifications", "get_notifications"),
        ("POST /api/friend-requests", "create_friend_request"),
        (
            "POST /api/friend-requests/{requestId}/{action}",
            "transition_friend_request",
        ),
        ("GET /api/relationships/{kind}", "get_relationships"),
        ("DELETE /api/friendships/{userId}", "delete_friendship"),
        ("PUT /api/follows/{userId}", "follow"),
        ("DELETE /api/follows/{userId}", "unfollow"),
        ("PUT /api/blocks/{userId}", "block"),
        ("DELETE /api/blocks/{userId}", "unblock"),
        ("POST /api/admin/auth/login", "admin_login"),
        ("POST /api/admin/auth/logout", "admin_logout"),
        (
            "PUT /api/admin/users/{userId}/certification",
            "certify_user",
        ),
        (
            "DELETE /api/admin/users/{userId}/certification",
            "delete_user_certification",
        ),
        ("POST /api/admin/posting-window", "open_posting_window"),
    ])
});

static LOG_LEVEL_RANKS: LazyLock<HashMap<&str, i32>> =
    LazyLock::new(|| HashMap::from([("debug", 0), ("info", 1), ("error", 2)]));

#[derive(Clone)]
pub struct ActionLog {
    min_level: String,
    output: Arc<Mutex<dyn Write + Send>>,
}

impl ActionLog {
    pub fn new(min_level: &str, output: impl Write + Send + 'static) -> ActionLog {
        ActionLog {
            min_level: min_level.to_owned(),
            output: Arc::new(Mutex::new(output)),
        }
    }
}

#[derive(Clone, Default)]
pub struct Username(Arc<Mutex<Option<String>>>);

impl Username {
    pub fn set(&self, username: &str) {
        *self.0.lock().unwrap() = Some(username.to_owned());
    }

    fn get(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
}

impl<S: Send + Sync> FromRequestParts<S> for Username {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<Username>()
            .cloned()
            .unwrap_or_default())
    }
}

pub async fn action_log_middleware(
    State(action_log): State<ActionLog>,
    mut req: Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned());
    let ip = extract_client_ip(&req);
    let username = Username::default();
    req.extensions_mut().insert(username.clone());

    let response = next.run(req).await;
    let status = response.status().as_u16();

    let level = if status >= 500 {
        "error"
    } else if method == Method::GET
        && !(status == 403 && response.extensions().get::<AbuseEvent>().is_some())
    {
        "debug"
    } else {
        "info"
    };
    let rank = |level: &str| LOG_LEVEL_RANKS.get(level).copied().unwrap_or_default();
    if rank(level) < rank(&action_log.min_level) {
        return response;
    }

    let mut entry = Map::new();
    entry.insert("level".to_owned(), level.into());
    entry.insert("action".to_owned(), Value::Null);
    entry.insert("status".to_owned(), status.into());
    entry.insert("ip".to_owned(), ip.into());
    entry.insert("user_id".to_owned(), Value::Null);
    let action = route.and_then(|path| {
        ACTION_NAMES
            .get(format!("{method} {path}").as_str())
            .copied()
    });
    if let Some(action) = action {
        entry.insert("action".to_owned(), action.into());
    }
    if let Some(session) = response.extensions().get::<Session>() {
        entry.insert("user_id".to_owned(), session.user_id.clone().into());
    }
    if let Some(abuse) = response.extensions().get::<AbuseEvent>() {
        entry.insert("abuse".to_owned(), abuse.0.into());
    }
    if action == Some("login") || action == Some("register") {
        entry.insert("username".to_owned(), Value::Null);
        if let Some(username) = username.get()
            && USERNAME_PATTERN.is_match(&username)
        {
            entry.insert("username".to_owned(), username.into());
        }
    }
    if let Ok(mut line) = serde_json::to_vec(&entry) {
        line.push(b'\n');
        let _ = action_log.output.lock().unwrap().write_all(&line);
    }
    response
}
