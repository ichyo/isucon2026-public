use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Extension;
use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::Response;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::Serialize;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use crate::api_error::Error;
use crate::logger;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::rfc3339_nano;
use crate::user::get_user;

#[derive(Serialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct GetNotificationsResponse {
    pub id: String,
    pub posting_window_id: String,
    pub region: String,
    #[serde(serialize_with = "rfc3339_nano")]
    pub created_at: DateTime<Utc>,
}

pub async fn get_notifications_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
) -> Result<Response, Error> {
    let user_id = session.user_id.clone();
    let connected_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&h.db)
        .await?;
    let event = async move {
        let period = Duration::from_millis(500);
        let mut ticker = interval_at(Instant::now() + period, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let user = match get_user(&h.db, &user_id).await {
                Ok(user) => user,
                Err(err) => {
                    if !err.is_no_rows() {
                        logger::error("failed to get notification recipient", &err);
                    }
                    return None;
                }
            };
            if user.region != session.region {
                return None;
            }
            let notification: Result<Option<GetNotificationsResponse>, sqlx::Error> =
                sqlx::query_as(
                    r#"
    SELECT n.id, n.posting_window_id, w.region, n.created_at
    FROM notifications n JOIN posting_windows w ON w.id = n.posting_window_id
    WHERE n.user_id = $1 AND n.created_at > $2 AND w.ended_at IS NULL
    ORDER BY n.created_at, n.id LIMIT 1
   "#,
                )
                .bind(&user_id)
                .bind(connected_at)
                .fetch_optional(&h.db)
                .await;
            match notification {
                Ok(Some(notification)) => {
                    let data = match serde_json::to_string(&notification) {
                        Ok(data) => data,
                        Err(err) => {
                            logger::error("failed to encode notification", &err);
                            return None;
                        }
                    };
                    return Some(Ok::<_, Infallible>(format!("data: {data}\n\n")));
                }
                Ok(None) => {}
                Err(err) => {
                    logger::error("failed to get notification", &err);
                    return None;
                }
            }
            ticker.tick().await;
        }
    };
    let stream = futures_util::stream::once(event).filter_map(|event| async move { event });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .map_err(anyhow::Error::from)?)
}
