use std::any::Any;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{Next, from_fn, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::serve::ListenerExt;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;

use crate::account_deletion_api::delete_account_handler;
use crate::action_log::{ActionLog, action_log_middleware};
use crate::admin_api::{admin_login_handler, admin_logout_handler};
use crate::admin_posting_window_api::open_posting_window_handler;
use crate::admin_session::admin_session_middleware;
use crate::admin_user_api::{certify_user_handler, delete_user_certification_handler};
use crate::api_error::{Error, HttpError};
use crate::comment_api::{create_comment_handler, delete_comment_handler, get_comments_handler};
use crate::config::Config;
use crate::export_api::export_posts_handler;
use crate::feed_api::get_feed_handler;
use crate::friend_request_api::{create_friend_request_handler, transition_friend_request_handler};
use crate::image_api::get_image_handler;
use crate::initialize::initialize_handler;
use crate::ip_ban::{IpBanList, ip_ban_middleware};
use crate::memory_api::{get_memories_by_date_handler, get_memories_handler};
use crate::notification_api::get_notifications_handler;
use crate::post_api::{
    create_post_handler, delete_post_handler, get_my_current_post_handler, get_post_handler,
    update_post_handler,
};
use crate::posting_window_api::get_current_posting_window_handler;
use crate::profile_image_api::{delete_profile_image_handler, put_profile_image_handler};
use crate::realmoji_api::{
    delete_real_moji_handler, get_real_mojis_handler, replace_real_moji_handler,
};
use crate::region::region_middleware;
use crate::relationship_api::{
    block_handler, delete_friendship_handler, follow_handler, unblock_handler, unfollow_handler,
};
use crate::relationship_list_api::get_relationships_handler;
use crate::session::session_middleware;
use crate::user::valid_region;
use crate::user_api::{
    get_me_handler, login_handler, logout_handler, register_handler, update_me_handler,
};
use crate::user_read_api::{get_user_handler, search_users_handler};

pub struct HandlerContext {
    pub db: PgPool,
    pub config: Config,
    pub ip_bans: IpBanList,
}

pub async fn new_server(config: Config) -> Router {
    if !valid_region(&config.region) {
        panic!("invalid server region: {}", config.region);
    }

    let options = match PgConnectOptions::from_str(&config.database_dsn()) {
        Ok(options) => options.statement_cache_capacity(512),
        Err(err) => panic!("failed to open database: {err}"),
    };
    let db = match PgPoolOptions::new()
        .max_connections(30)
        .connect_with(options)
        .await
    {
        Ok(db) => db,
        Err(err) => panic!("failed to ping database: {err}"),
    };

    let action_log = ActionLog::new(&config.log_level, io::stdout());
    let handler_context = Arc::new(HandlerContext {
        db,
        config,
        ip_bans: IpBanList::default(),
    });
    let session = |auth_required| {
        from_fn_with_state((handler_context.clone(), auth_required), session_middleware)
    };
    let region = || from_fn_with_state(handler_context.clone(), region_middleware);
    let admin_session = |auth_required| {
        from_fn_with_state(
            (handler_context.clone(), auth_required),
            admin_session_middleware,
        )
    };

    #[rustfmt::skip]
    let server = Router::new()
        .route("/api/initialize", post(initialize_handler))
        .route("/api/auth/register", post(register_handler))
        .route("/api/auth/login", post(login_handler))
        .route("/api/auth/logout", post(logout_handler).route_layer(session(true)))
        .route("/api/users", get(search_users_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/users/{userId}", get(get_user_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/me/export", get(export_posts_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/me", get(get_me_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/me", patch(update_me_handler).route_layer(session(true)))
        .route("/api/me/profile-image", put(put_profile_image_handler).route_layer(session(true)))
        .route("/api/me/profile-image", delete(delete_profile_image_handler).route_layer(session(true)))
        .route("/api/me/account-deletion", post(delete_account_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts", post(create_post_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posting-window/current", get(get_current_posting_window_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/me/current", get(get_my_current_post_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/{postId}", get(get_post_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/{postId}", patch(update_post_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/{postId}", delete(delete_post_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/feeds/{feed}", get(get_feed_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/memories", get(get_memories_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/memories/{date}", get(get_memories_by_date_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/{postId}/comments", post(create_comment_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/{postId}/comments", get(get_comments_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/comments/{commentId}", delete(delete_comment_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/{postId}/realmojis", post(replace_real_moji_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/posts/{postId}/realmojis", get(get_real_mojis_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/realmojis/{realMojiId}", delete(delete_real_moji_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/images/{imageId}", get(get_image_handler))
        .route("/api/notifications", get(get_notifications_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/friend-requests", post(create_friend_request_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/friend-requests/{requestId}/{action}", post(transition_friend_request_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/relationships/{kind}", get(get_relationships_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/friendships/{userId}", delete(delete_friendship_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/follows/{userId}", put(follow_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/follows/{userId}", delete(unfollow_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/blocks/{userId}", put(block_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/blocks/{userId}", delete(unblock_handler).route_layer(region()).route_layer(session(true)))
        .route("/api/admin/auth/login", post(admin_login_handler))
        .route("/api/admin/auth/logout", post(admin_logout_handler).route_layer(admin_session(true)))
        .route("/api/admin/users/{userId}/certification", put(certify_user_handler).route_layer(admin_session(true)))
        .route("/api/admin/users/{userId}/certification", delete(delete_user_certification_handler).route_layer(admin_session(true)))
        .route("/api/admin/posting-window", post(open_posting_window_handler).route_layer(admin_session(true)))
        .fallback(not_found_handler)
        .method_not_allowed_fallback(method_not_allowed_handler)
        .layer(
            ServiceBuilder::new()
                .layer(from_fn(run_to_completion))
                .layer(from_fn_with_state(action_log, action_log_middleware))
                .layer(CatchPanicLayer::custom(recover))
                .layer(from_fn_with_state(handler_context.clone(), ip_ban_middleware)),
        )
        .with_state(handler_context);

    server
}

pub async fn start(server: Router, address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    axum::serve(
        listener,
        server.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}

pub async fn run_to_completion(req: Request, next: Next) -> Response {
    match tokio::spawn(next.run(req)).await {
        Ok(response) => response,
        Err(err) => match err.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
    }
}

async fn not_found_handler() -> Error {
    Error::Http(HttpError::new(StatusCode::NOT_FOUND))
}

async fn method_not_allowed_handler() -> Error {
    Error::Http(HttpError::new(StatusCode::METHOD_NOT_ALLOWED))
}

fn recover(err: Box<dyn Any + Send + 'static>) -> Response {
    let message = if let Some(message) = err.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = err.downcast_ref::<&str>() {
        message.to_string()
    } else {
        "panic".to_owned()
    };
    Error::Internal(anyhow::anyhow!("[PANIC RECOVER] {message}")).into_response()
}
