use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{Executor, Postgres};

use crate::api_error::Error;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PostingWindow {
    pub id: String,
    pub region: String,
    pub local_date: NaiveDate,
    pub starts_at: DateTime<Utc>,
    pub on_time_deadline: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

pub async fn get_posting_window<'c>(
    db: impl Executor<'c, Database = Postgres>,
    window_id: &str,
) -> Result<PostingWindow, Error> {
    let window = sqlx::query_as(
        "SELECT id, region, local_date, starts_at, on_time_deadline, ended_at FROM posting_windows WHERE id = $1",
    )
    .bind(window_id)
    .fetch_one(db)
    .await?;
    Ok(window)
}
