use chrono::{DateTime, Utc};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Executor, FromRow, PgConnection, Postgres};

use crate::api_error::Error;
use crate::post::{get_post_with_author, load_viewer_context};
use crate::user::{User, user_columns_as};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Comment {
    pub id: String,
    pub post_id: String,
    pub author_id: String,
    pub body: String,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

pub async fn get_comment<'c>(
    db: impl Executor<'c, Database = Postgres>,
    id: &str,
) -> Result<Comment, Error> {
    let comment = sqlx::query_as(
        "SELECT id,post_id,author_id,body,created_at,deleted_at FROM comments WHERE id=$1",
    )
    .bind(id)
    .fetch_one(db)
    .await?;
    Ok(comment)
}

#[derive(Debug, Clone)]
pub struct CommentWithAuthor {
    pub comment: Comment,
    pub author: User,
}

impl FromRow<'_, PgRow> for CommentWithAuthor {
    fn from_row(row: &PgRow) -> sqlx::Result<CommentWithAuthor> {
        Ok(CommentWithAuthor {
            comment: Comment::from_row(row)?,
            author: User::from_row_with_prefix(row, "author")?,
        })
    }
}

pub async fn select_comments_with_author<'c>(
    db: impl Executor<'c, Database = Postgres>,
    conditions: &str,
    args: PgArguments,
) -> Result<Vec<CommentWithAuthor>, Error> {
    let mut comments: Vec<CommentWithAuthor> = sqlx::query_as_with(
        &format!(
            r#"
  SELECT c.id, c.post_id, c.author_id, c.body, c.created_at, c.deleted_at, {}
  FROM comments c JOIN users u ON u.id = c.author_id AND u.status = 'active'
  WHERE c.deleted_at IS NULL {conditions}"#,
            user_columns_as("u", "author")
        ),
        args,
    )
    .fetch_all(db)
    .await?;
    for comment in &mut comments {
        comment.author.set_image_key()?;
    }
    Ok(comments)
}

pub async fn can_comment(
    db: &mut PgConnection,
    viewer_id: &str,
    post_id: &str,
) -> Result<bool, Error> {
    let post = match get_post_with_author(&mut *db, post_id).await {
        Err(err) if err.is_no_rows() => return Ok(false),
        result => result?,
    };
    let viewer = match load_viewer_context(db, viewer_id).await {
        Err(err) if err.is_no_rows() => return Ok(false),
        result => result?,
    };
    if !viewer.can_view(&post.post) || post.post.window_ended_at.is_some() {
        return Ok(false);
    }
    if viewer.user.id == post.post.author_id {
        return Ok(viewer.has_posted_today);
    }
    if post.post.visibility == "public" {
        return Ok(true);
    }
    Ok(viewer.friends.contains(&post.post.author_id))
}
