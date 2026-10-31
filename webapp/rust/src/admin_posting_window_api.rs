use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable};
use crate::posting_window::get_posting_window;
use crate::response::json;
use crate::server::HandlerContext;
use crate::timefmt::{parse_date_only, rfc3339_nano};
use crate::user::valid_region;

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct OpenPostingWindowRequest {
    pub region: String,
    pub local_date: String,
    pub on_time_seconds: Option<i64>,
}

impl Bindable for OpenPostingWindowRequest {}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenPostingWindowResponse {
    pub id: String,
    pub region: String,
    pub local_date: String,
    #[serde(serialize_with = "rfc3339_nano")]
    pub starts_at: DateTime<Utc>,
    #[serde(serialize_with = "rfc3339_nano")]
    pub on_time_deadline: DateTime<Utc>,
}

pub async fn open_posting_window_handler(
    State(h): State<Arc<HandlerContext>>,
    Bind(input): Bind<OpenPostingWindowRequest>,
) -> Result<Response, Error> {
    let region = &input.region;
    let local_date = match (valid_region(region), parse_date_only(&input.local_date)) {
        (true, Some(local_date)) => local_date,
        _ => {
            return Err(new_api_error(
                StatusCode::BAD_REQUEST,
                "invalid_posting_window",
            ));
        }
    };
    let seconds = input.on_time_seconds.unwrap_or(24 * 60 * 60);
    let mut tx = h.db.begin().await?;
    sqlx::query("LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    let conflict: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM posting_windows WHERE region = $1 AND local_date >= $2)",
    )
    .bind(region)
    .bind(local_date)
    .fetch_one(&mut *tx)
    .await?;
    if conflict {
        return Err(new_api_error(
            StatusCode::CONFLICT,
            "posting_window_conflict",
        ));
    }
    let starts_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("UPDATE posting_windows SET ended_at = $2 WHERE region = $1 AND ended_at IS NULL")
        .bind(region)
        .bind(starts_at)
        .execute(&mut *tx)
        .await?;
    let window_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
   INSERT INTO posting_windows (id, region, local_date, starts_at, on_time_deadline)
   VALUES ($1, $2, $3, $4, $5)
  "#,
    )
    .bind(&window_id)
    .bind(region)
    .bind(local_date)
    .bind(starts_at)
    .bind(starts_at + TimeDelta::nanoseconds(seconds.wrapping_mul(1_000_000_000)))
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
   INSERT INTO notifications (id, user_id, posting_window_id, created_at)
   SELECT gen_random_uuid()::text, id, $2, $3 FROM users WHERE region = $1 AND status = 'active'
  "#,
    )
    .bind(region)
    .bind(&window_id)
    .bind(starts_at)
    .execute(&mut *tx)
    .await?;
    let window = get_posting_window(&mut *tx, &window_id).await?;
    tx.commit().await?;
    json(
        StatusCode::CREATED,
        OpenPostingWindowResponse {
            id: window.id,
            region: window.region,
            local_date: window.local_date.format("%Y-%m-%d").to_string(),
            starts_at: window.starts_at,
            on_time_deadline: window.on_time_deadline,
        },
    )
}
