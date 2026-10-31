use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, PgPool};

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder};
use crate::db::{args, in_placeholders};
use crate::post::{
    Friendship, Post, PostWithAuthor, ViewerContext, load_viewer_context, select_posts_with_author,
};
use crate::post_api::{CreatePostResponse, new_post_response};
use crate::response::json;
use crate::server::HandlerContext;
use crate::session::Session;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetFeedRequest {
    #[serde(skip)]
    pub feed: String,
    #[serde(skip)]
    pub before_score: Option<i64>,
    #[serde(skip)]
    pub before_posted_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub before_id: String,
}

impl Bindable for GetFeedRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("feed", &mut self.feed)?;
        b.query("beforeScore", &mut self.before_score)?;
        b.query("beforePostedAt", &mut self.before_posted_at)?;
        b.query("beforeId", &mut self.before_id)
    }
}

#[derive(Serialize)]
pub struct FeedItemResponse {
    #[serde(flatten)]
    pub create_post_response: CreatePostResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetFeedResponse {
    pub items: Vec<FeedItemResponse>,
    pub has_more: bool,
}

pub async fn get_feed_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetFeedRequest>,
) -> Result<Response, Error> {
    match input.feed.as_str() {
        "friends" | "friends-of-friends" | "public" | "following" => {}
        _ => return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post")),
    }
    if input.before_posted_at.is_none() != input.before_id.is_empty() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_pagination"));
    }
    let ranked = input.feed == "friends-of-friends";
    if ranked && input.before_score.is_none() != input.before_id.is_empty() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_pagination"));
    }
    let viewer = match load_viewer_context(&mut *h.db.acquire().await?, &session.user_id).await {
        Err(err) if err.is_no_rows() => {
            return json(
                StatusCode::OK,
                GetFeedResponse {
                    items: Vec::new(),
                    has_more: false,
                },
            );
        }
        result => result?,
    };
    let mut following = HashSet::new();
    if input.feed == "following" {
        let followee_ids: Vec<String> =
            sqlx::query_scalar("SELECT followee_id FROM follows WHERE follower_id = $1")
                .bind(&viewer.user.id)
                .fetch_all(&h.db)
                .await?;
        following.extend(followee_ids);
    }
    let (before_posted_at, before_id) = if ranked {
        (None, "")
    } else {
        (input.before_posted_at, input.before_id.as_str())
    };
    let posts = select_posts_with_author(
        &h.db,
        r#"
   AND p.author_id <> $1 AND w.ended_at IS NULL
   AND ($2::timestamptz IS NULL OR (p.posted_at, p.id) < ($2, $3))
   ORDER BY p.posted_at DESC, p.id DESC
  "#,
        args![&viewer.user.id, before_posted_at, before_id],
    )
    .await?;
    let mut candidates = Vec::new();
    for post in &posts {
        if viewer.can_view(&post.post) && matches_feed(&viewer, &following, &post.post, &input.feed)
        {
            candidates.push(post);
        }
    }
    let mut scores = HashMap::new();
    if ranked {
        scores = recommendation_scores(&h.db, &viewer, &candidates).await?;
        candidates.sort_by(|a, b| scores[&b.post.id].cmp(&scores[&a.post.id]));
    }
    let mut items = Vec::new();
    for post in candidates {
        let score = scores.get(&post.post.id).copied().unwrap_or_default();
        if ranked
            && let (Some(before_score), Some(before_posted_at)) =
                (input.before_score, input.before_posted_at)
            && !ranked_before(
                score,
                &post.post,
                before_score,
                before_posted_at,
                &input.before_id,
            )
        {
            continue;
        }
        items.push(FeedItemResponse {
            create_post_response: new_post_response(&post.post, &post.author),
            score: ranked.then_some(score),
        });
        if items.len() == 51 {
            break;
        }
    }
    let has_more = items.len() > 50;
    if has_more {
        items.truncate(50);
    }
    json(StatusCode::OK, GetFeedResponse { items, has_more })
}

fn ranked_before(
    score: i64,
    post: &Post,
    before_score: i64,
    before_posted_at: DateTime<Utc>,
    before_id: &str,
) -> bool {
    if score != before_score {
        return score < before_score;
    }
    if post.posted_at != before_posted_at {
        return post.posted_at < before_posted_at;
    }
    post.id.as_str() < before_id
}

async fn recommendation_scores(
    db: &PgPool,
    viewer: &ViewerContext,
    posts: &[&PostWithAuthor],
) -> Result<HashMap<String, i64>, Error> {
    let mut scores = HashMap::with_capacity(posts.len());
    if posts.is_empty() {
        return Ok(scores);
    }
    let author_ids: Vec<&String> = posts.iter().map(|post| &post.post.author_id).collect();
    let post_ids: Vec<&String> = posts.iter().map(|post| &post.post.id).collect();

    let query = format!(
        "SELECT user_low_id, user_high_id FROM friendships WHERE user_low_id IN ({}) OR user_high_id IN ({})",
        in_placeholders(1, author_ids.len()),
        in_placeholders(author_ids.len() + 1, author_ids.len())
    );
    let mut args = PgArguments::default();
    for id in author_ids.iter().chain(&author_ids) {
        args.add(*id).map_err(|err| anyhow::anyhow!(err))?;
    }
    let friendships: Vec<Friendship> = sqlx::query_as_with(&query, args).fetch_all(db).await?;
    let mut connections: HashMap<&str, i64> = HashMap::new();
    for friendship in &friendships {
        *connections.entry(&friendship.user_low_id).or_default() += viewer
            .mutual_friends
            .get(&friendship.user_high_id)
            .copied()
            .unwrap_or_default();
        *connections.entry(&friendship.user_high_id).or_default() += viewer
            .mutual_friends
            .get(&friendship.user_low_id)
            .copied()
            .unwrap_or_default();
    }

    let query = format!(
        r#"
  SELECT r.post_id, r.reactor_id AS user_id FROM realmojis r
  JOIN users u ON u.id = r.reactor_id AND u.status = 'active'
  WHERE r.post_id IN ({}) AND r.replaced_at IS NULL AND r.deleted_at IS NULL
  UNION
  SELECT c.post_id, c.author_id AS user_id FROM comments c
  JOIN users u ON u.id = c.author_id AND u.status = 'active'
  WHERE c.post_id IN ({}) AND c.deleted_at IS NULL
 "#,
        in_placeholders(1, post_ids.len()),
        in_placeholders(post_ids.len() + 1, post_ids.len())
    );
    let mut args = PgArguments::default();
    for id in post_ids.iter().chain(&post_ids) {
        args.add(*id).map_err(|err| anyhow::anyhow!(err))?;
    }
    let engagements: Vec<(String, String)> =
        sqlx::query_as_with(&query, args).fetch_all(db).await?;
    let mut friend_engagements: HashMap<&str, i64> = HashMap::new();
    for (post_id, user_id) in &engagements {
        if viewer.friends.contains(user_id) {
            *friend_engagements.entry(post_id).or_default() += 1;
        }
    }

    for post in posts {
        let engagement = friend_engagements
            .get(post.post.id.as_str())
            .copied()
            .unwrap_or_default();
        let mutual_friends = viewer
            .mutual_friends
            .get(&post.post.author_id)
            .copied()
            .unwrap_or_default();
        let connection = connections
            .get(post.post.author_id.as_str())
            .copied()
            .unwrap_or_default();
        scores.insert(
            post.post.id.clone(),
            10 * engagement + 3 * mutual_friends + connection.min(30),
        );
    }
    Ok(scores)
}

fn matches_feed(
    viewer: &ViewerContext,
    following: &HashSet<String>,
    post: &Post,
    feed: &str,
) -> bool {
    match feed {
        "public" => post.visibility == "public",
        "following" => following.contains(&post.author_id),
        "friends" => viewer.friends.contains(&post.author_id),
        "friends-of-friends" => {
            !viewer.friends.contains(&post.author_id)
                && viewer.friends_of_friends.contains(&post.author_id)
        }
        _ => false,
    }
}
