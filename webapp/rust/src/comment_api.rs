use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder};
use crate::comment::{Comment, can_comment, get_comment, select_comments_with_author};
use crate::db::args;
use crate::post::get_post;
use crate::relationship_list_api::RelationshipUserResponse;
use crate::response::{json, no_content};
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::rfc3339_nano;
use crate::user::{User, get_user, valid_user_text};
use crate::user_read_api::new_relationship_user_response;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct CreateCommentRequest {
    #[serde(skip)]
    pub post_id: String,
    pub body: String,
}

impl Bindable for CreateCommentRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("postId", &mut self.post_id)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateCommentResponse {
    pub id: String,
    pub author: RelationshipUserResponse,
    pub body: String,
    #[serde(serialize_with = "rfc3339_nano")]
    pub created_at: DateTime<Utc>,
}

pub fn new_comment_response(comment: &Comment, user: &User) -> CreateCommentResponse {
    CreateCommentResponse {
        id: comment.id.clone(),
        author: new_relationship_user_response(user),
        body: comment.body.clone(),
        created_at: comment.created_at,
    }
}

pub async fn create_comment_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<CreateCommentRequest>,
) -> Result<Response, Error> {
    if !valid_user_text(&input.body, 1, 1000) {
        return Err(new_api_error(
            StatusCode::BAD_REQUEST,
            "invalid_interaction",
        ));
    }
    let actor = &session.user_id;
    let post = match get_post(&h.db, &input.post_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(
                StatusCode::FORBIDDEN,
                "interaction_forbidden",
            ));
        }
        result => result?,
    };
    let mut tx = h.db.begin().await?;
    let mut ids = vec![actor.min(&post.author_id), actor.max(&post.author_id)];
    ids.dedup();
    for id in ids {
        let _locked_id: String =
            sqlx::query_scalar("SELECT id FROM users WHERE id=$1 FOR NO KEY UPDATE")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
    }
    let allowed = can_comment(&mut tx, actor, &post.id).await?;
    if !allowed {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "interaction_forbidden",
        ));
    }
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO comments(id,post_id,author_id,body,created_at) VALUES($1,$2,$3,$4,clock_timestamp())")
        .bind(&id)
        .bind(&post.id)
        .bind(actor)
        .bind(&input.body)
        .execute(&mut *tx)
        .await?;
    let comment = get_comment(&mut *tx, &id).await?;
    let user = get_user(&mut *tx, actor).await?;
    tx.commit().await?;
    json(StatusCode::CREATED, new_comment_response(&comment, &user))
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetCommentsRequest {
    #[serde(skip)]
    pub post_id: String,
    #[serde(skip)]
    pub after_created_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub after_id: String,
}

impl Bindable for GetCommentsRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("postId", &mut self.post_id)?;
        b.query("afterCreatedAt", &mut self.after_created_at)?;
        b.query("afterId", &mut self.after_id)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetCommentsResponse {
    pub items: Vec<CreateCommentResponse>,
    pub has_more: bool,
}

pub async fn get_comments_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetCommentsRequest>,
) -> Result<Response, Error> {
    if input.after_created_at.is_none() != input.after_id.is_empty() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_pagination"));
    }
    let allowed = can_comment(
        &mut *h.db.acquire().await?,
        &session.user_id,
        &input.post_id,
    )
    .await?;
    if !allowed {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "interaction_forbidden",
        ));
    }
    let comments = select_comments_with_author(
        &h.db,
        "AND c.post_id=$1 AND ($2::timestamptz IS NULL OR (c.created_at,c.id)>($2,$3)) ORDER BY c.created_at,c.id LIMIT 51",
        args![&input.post_id, input.after_created_at, &input.after_id],
    )
    .await?;
    let mut items = Vec::with_capacity(comments.len());
    for comment in &comments {
        items.push(new_comment_response(&comment.comment, &comment.author));
    }
    let has_more = items.len() > 50;
    if has_more {
        items.truncate(50);
    }
    json(StatusCode::OK, GetCommentsResponse { items, has_more })
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct DeleteCommentRequest {
    #[serde(skip)]
    pub comment_id: String,
}

impl Bindable for DeleteCommentRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("commentId", &mut self.comment_id)
    }
}

pub async fn delete_comment_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<DeleteCommentRequest>,
) -> Result<Response, Error> {
    let actor = &session.user_id;
    let mut tx = h.db.begin().await?;
    let _locked_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE id=$1 FOR NO KEY UPDATE")
            .bind(actor)
            .fetch_one(&mut *tx)
            .await?;
    #[derive(sqlx::FromRow)]
    struct Record {
        author_id: String,
        post_author_id: String,
    }
    let record: Option<Record> = sqlx::query_as("SELECT c.author_id,p.author_id AS post_author_id FROM comments c JOIN posts p ON p.id=c.post_id WHERE c.id=$1 AND c.deleted_at IS NULL FOR UPDATE OF c")
        .bind(&input.comment_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(record) = record else {
        return Err(new_api_error(
            StatusCode::NOT_FOUND,
            "interaction_not_found",
        ));
    };
    if *actor != record.author_id && *actor != record.post_author_id {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "interaction_forbidden",
        ));
    }
    sqlx::query("UPDATE comments SET deleted_at=clock_timestamp() WHERE id=$1")
        .bind(&input.comment_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}
