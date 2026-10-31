use std::collections::HashMap;
use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Days, Months, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, PgConnection};

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder};
use crate::comment::select_comments_with_author;
use crate::comment_api::{CreateCommentResponse, new_comment_response};
use crate::db::{args, in_placeholders};
use crate::post::select_posts_with_author;
use crate::post_api::{CreatePostResponse, new_post_response};
use crate::realmoji::select_current_real_mojis;
use crate::realmoji_api::{ReplaceRealMojiResponse, new_real_moji_response};
use crate::response::json;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::{parse_date_only, parse_year_month};

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetMemoriesRequest {
    #[serde(skip)]
    pub month: String,
    #[serde(skip)]
    pub before_posted_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub before_id: String,
}

impl Bindable for GetMemoriesRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.query("month", &mut self.month)?;
        b.query("beforePostedAt", &mut self.before_posted_at)?;
        b.query("beforeId", &mut self.before_id)
    }
}

#[derive(Serialize)]
pub struct MemoryPostResponse {
    #[serde(flatten)]
    pub create_post_response: CreatePostResponse,
    pub realmojis: Vec<ReplaceRealMojiResponse>,
    pub comments: Vec<CreateCommentResponse>,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetMemoriesResponse {
    pub items: Vec<MemoryPostResponse>,
    pub has_more: bool,
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetMemoriesByDateRequest {
    #[serde(skip)]
    pub date: String,
    #[serde(skip)]
    pub before_posted_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub before_id: String,
}

impl Bindable for GetMemoriesByDateRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("date", &mut self.date)?;
        b.query("beforePostedAt", &mut self.before_posted_at)?;
        b.query("beforeId", &mut self.before_id)
    }
}

pub type GetMemoriesByDateResponse = GetMemoriesResponse;

pub async fn get_memories_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetMemoriesRequest>,
) -> Result<Response, Error> {
    let Some(start) = parse_year_month(&input.month) else {
        return Err(new_api_error(
            StatusCode::BAD_REQUEST,
            "invalid_memory_period",
        ));
    };
    let response = list_memories(
        &mut *h.db.acquire().await?,
        &session.user_id,
        start,
        start + Months::new(1),
        input.before_posted_at,
        &input.before_id,
    )
    .await?;
    json(StatusCode::OK, response)
}

pub async fn get_memories_by_date_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetMemoriesByDateRequest>,
) -> Result<Response, Error> {
    let Some(start) = parse_date_only(&input.date) else {
        return Err(new_api_error(
            StatusCode::BAD_REQUEST,
            "invalid_memory_period",
        ));
    };
    let response: GetMemoriesByDateResponse = list_memories(
        &mut *h.db.acquire().await?,
        &session.user_id,
        start,
        start + Days::new(1),
        input.before_posted_at,
        &input.before_id,
    )
    .await?;
    json(StatusCode::OK, response)
}

pub async fn list_memories(
    db: &mut PgConnection,
    user_id: &str,
    start: NaiveDate,
    end: NaiveDate,
    before_at: Option<DateTime<Utc>>,
    before_id: &str,
) -> Result<GetMemoriesResponse, Error> {
    let mut response = GetMemoriesResponse::default();
    if before_at.is_none() != before_id.is_empty() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_pagination"));
    }
    let posts = select_posts_with_author(
        &mut *db,
        r#"
  AND p.author_id = $1 AND w.local_date >= $2 AND w.local_date < $3
  AND ($4::timestamptz IS NULL OR (p.posted_at, p.id) < ($4, $5))
  ORDER BY p.posted_at DESC, p.id DESC LIMIT 51
 "#,
        args![user_id, start, end, before_at, before_id],
    )
    .await?;
    let mut items = Vec::with_capacity(posts.len());
    for post in &posts {
        items.push(MemoryPostResponse {
            create_post_response: new_post_response(&post.post, &post.author),
            realmojis: Vec::new(),
            comments: Vec::new(),
        });
    }
    response.has_more = items.len() > 50;
    if response.has_more {
        items.truncate(50);
    }
    load_memory_interactions(db, &mut items).await?;
    response.items = items;
    Ok(response)
}

async fn load_memory_interactions(
    db: &mut PgConnection,
    items: &mut [MemoryPostResponse],
) -> Result<(), Error> {
    if items.is_empty() {
        return Ok(());
    }
    let post_ids: Vec<String> = items
        .iter()
        .map(|item| item.create_post_response.id.clone())
        .collect();
    let item_by_post_id: HashMap<String, usize> = post_ids
        .iter()
        .enumerate()
        .map(|(i, post_id)| (post_id.clone(), i))
        .collect();

    let mut args = PgArguments::default();
    for post_id in &post_ids {
        args.add(post_id).map_err(|err| anyhow::anyhow!(err))?;
    }
    let reactions = select_current_real_mojis(
        &mut *db,
        &format!(
            "AND r.post_id IN ({}) ORDER BY r.created_at DESC, r.id DESC",
            in_placeholders(1, post_ids.len())
        ),
        args,
    )
    .await?;
    for reaction in &reactions {
        let item = &mut items[item_by_post_id[&reaction.real_moji.post_id]];
        if item.realmojis.len() < 50 {
            item.realmojis.push(new_real_moji_response(
                &reaction.real_moji,
                &reaction.reactor,
            ));
        }
    }

    let mut args = PgArguments::default();
    for post_id in &post_ids {
        args.add(post_id).map_err(|err| anyhow::anyhow!(err))?;
    }
    let comments = select_comments_with_author(
        &mut *db,
        &format!(
            "AND c.post_id IN ({}) ORDER BY c.created_at, c.id",
            in_placeholders(1, post_ids.len())
        ),
        args,
    )
    .await?;
    for comment in &comments {
        let item = &mut items[item_by_post_id[&comment.comment.post_id]];
        if item.comments.len() < 50 {
            item.comments
                .push(new_comment_response(&comment.comment, &comment.author));
        }
    }
    Ok(())
}
