use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::api_error::Error;
use crate::posting_window::PostingWindow;
use crate::response::json;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::rfc3339_nano;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentPostingWindowResponse {
    pub id: String,
    pub region: String,
    pub local_date: String,
    #[serde(serialize_with = "rfc3339_nano")]
    pub starts_at: DateTime<Utc>,
    #[serde(serialize_with = "rfc3339_nano")]
    pub on_time_deadline: DateTime<Utc>,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetCurrentPostingWindowResponse {
    pub window: Option<CurrentPostingWindowResponse>,
    pub has_posted: bool,
    pub can_post: bool,
    pub deletion_count: i64,
}

pub async fn get_current_posting_window_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
) -> Result<Response, Error> {
    #[derive(sqlx::FromRow)]
    struct PostingWindowState {
        #[sqlx(flatten)]
        posting_window: PostingWindow,
        active: i64,
        deleted: i64,
    }
    let state: Option<PostingWindowState> = sqlx::query_as(
        r#"
   SELECT w.id, w.region, w.local_date, w.starts_at, w.on_time_deadline, w.ended_at,
          count(p.id) FILTER (WHERE p.deleted_at IS NULL) AS active,
          count(p.id) FILTER (WHERE p.deleted_at IS NOT NULL) AS deleted
   FROM users u JOIN posting_windows w ON w.region = u.region AND w.ended_at IS NULL
   LEFT JOIN posting_windows posted_window ON posted_window.local_date = w.local_date
   LEFT JOIN posts p ON p.author_id = u.id AND p.posting_window_id = posted_window.id
   WHERE u.id = $1
   GROUP BY w.id
  "#,
    )
    .bind(&session.user_id)
    .fetch_optional(&h.db)
    .await?;
    let Some(state) = state else {
        return json(StatusCode::OK, GetCurrentPostingWindowResponse::default());
    };
    let window = &state.posting_window;
    json(
        StatusCode::OK,
        GetCurrentPostingWindowResponse {
            window: Some(CurrentPostingWindowResponse {
                id: window.id.clone(),
                region: window.region.clone(),
                local_date: window.local_date.format("%Y-%m-%d").to_string(),
                starts_at: window.starts_at,
                on_time_deadline: window.on_time_deadline,
            }),
            has_posted: state.active > 0,
            can_post: state.active == 0 && state.deleted < 2 && Utc::now() >= window.starts_at,
            deletion_count: state.deleted,
        },
    )
}
