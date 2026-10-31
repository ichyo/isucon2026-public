use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Arguments, Executor, FromRow, PgConnection, Postgres, Row};

use crate::api_error::Error;
use crate::db::{args, in_placeholders, numeric_to_f64};
use crate::user::{User, get_user, user_columns_as, valid_user_text};

#[derive(Debug, Clone, Default, Serialize)]
pub struct PostLocation {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<String>,
}

impl PostLocation {
    pub fn valid(&self) -> bool {
        match self.kind.as_str() {
            "none" => {
                self.latitude.is_none() && self.longitude.is_none() && self.locality.is_none()
            }
            "exact" => match (self.latitude, self.longitude) {
                (Some(latitude), Some(longitude)) => {
                    self.locality.is_none()
                        && (-90.0..=90.0).contains(&latitude)
                        && (-180.0..=180.0).contains(&longitude)
                }
                _ => false,
            },
            "approximate" => {
                self.latitude.is_none()
                    && self.longitude.is_none()
                    && self
                        .locality
                        .as_ref()
                        .is_some_and(|locality| valid_user_text(locality, 1, 100))
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Post {
    pub id: String,
    pub author_id: String,
    pub posting_window_id: String,
    pub image_id: String,
    pub caption: String,
    pub visibility: String,
    pub post_location: PostLocation,
    pub posted_at: DateTime<Utc>,
    pub on_time_deadline: DateTime<Utc>,
    pub window_ended_at: Option<DateTime<Utc>>,
}

impl FromRow<'_, PgRow> for Post {
    fn from_row(row: &PgRow) -> sqlx::Result<Post> {
        Ok(Post {
            id: row.try_get("id")?,
            author_id: row.try_get("author_id")?,
            posting_window_id: row.try_get("posting_window_id")?,
            image_id: row.try_get("image_id")?,
            caption: row.try_get("caption")?,
            visibility: row.try_get("visibility")?,
            post_location: PostLocation {
                kind: row.try_get("location_kind")?,
                latitude: numeric_to_f64(row.try_get("latitude")?),
                longitude: numeric_to_f64(row.try_get("longitude")?),
                locality: row.try_get("locality")?,
            },
            posted_at: row.try_get("posted_at")?,
            on_time_deadline: row.try_get("on_time_deadline")?,
            window_ended_at: row.try_get("window_ended_at")?,
        })
    }
}

pub const POST_COLUMNS: &str = r#"p.id, p.author_id, p.posting_window_id, p.image_id, p.caption, p.visibility,
  p.location_kind, p.latitude, p.longitude, p.locality, p.posted_at,
  w.on_time_deadline, w.ended_at AS window_ended_at"#;

pub async fn get_post<'c>(
    db: impl Executor<'c, Database = Postgres>,
    post_id: &str,
) -> Result<Post, Error> {
    let post = sqlx::query_as(&format!(
        r#"
  SELECT {POST_COLUMNS}
  FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
  WHERE p.id = $1 AND p.deleted_at IS NULL
 "#
    ))
    .bind(post_id)
    .fetch_one(db)
    .await?;
    Ok(post)
}

#[derive(Debug, Clone)]
pub struct PostWithAuthor {
    pub post: Post,
    pub author: User,
}

impl FromRow<'_, PgRow> for PostWithAuthor {
    fn from_row(row: &PgRow) -> sqlx::Result<PostWithAuthor> {
        Ok(PostWithAuthor {
            post: Post::from_row(row)?,
            author: User::from_row_with_prefix(row, "author")?,
        })
    }
}

pub async fn select_posts_with_author<'c>(
    db: impl Executor<'c, Database = Postgres>,
    conditions: &str,
    args: PgArguments,
) -> Result<Vec<PostWithAuthor>, Error> {
    let mut posts: Vec<PostWithAuthor> = sqlx::query_as_with(
        &format!(
            r#"
  SELECT {POST_COLUMNS}, {}
  FROM posts p
  JOIN posting_windows w ON w.id = p.posting_window_id
  JOIN users u ON u.id = p.author_id AND u.status = 'active'
  WHERE p.deleted_at IS NULL {conditions}"#,
            user_columns_as("u", "author")
        ),
        args,
    )
    .fetch_all(db)
    .await?;
    for post in &mut posts {
        post.author.set_image_key()?;
    }
    Ok(posts)
}

pub async fn get_post_with_author<'c>(
    db: impl Executor<'c, Database = Postgres>,
    post_id: &str,
) -> Result<PostWithAuthor, Error> {
    let posts = select_posts_with_author(db, "AND p.id = $1", args![post_id]).await?;
    match posts.into_iter().next() {
        Some(post) => Ok(post),
        None => Err(sqlx::Error::RowNotFound.into()),
    }
}

pub struct ViewerContext {
    pub user: User,
    pub has_posted_today: bool,
    pub friends: HashSet<String>,
    pub friends_of_friends: HashSet<String>,
    pub mutual_friends: HashMap<String, i64>,
    pub blocked: HashSet<String>,
}

#[derive(sqlx::FromRow)]
pub struct Friendship {
    pub user_low_id: String,
    pub user_high_id: String,
}

pub async fn load_viewer_context(
    db: &mut PgConnection,
    viewer_id: &str,
) -> Result<ViewerContext, Error> {
    let mut viewer = ViewerContext {
        user: get_user(&mut *db, viewer_id).await?,
        has_posted_today: false,
        friends: HashSet::new(),
        friends_of_friends: HashSet::new(),
        mutual_friends: HashMap::new(),
        blocked: HashSet::new(),
    };
    viewer.has_posted_today = sqlx::query_scalar(
        r#"
  SELECT EXISTS(
    SELECT 1 FROM posts p
    JOIN posting_windows posted ON posted.id = p.posting_window_id
    JOIN posting_windows current ON current.region = $2 AND current.ended_at IS NULL
                                AND current.local_date = posted.local_date
    WHERE p.author_id = $1 AND p.deleted_at IS NULL
  )
 "#,
    )
    .bind(&viewer.user.id)
    .bind(&viewer.user.region)
    .fetch_one(&mut *db)
    .await?;
    viewer.blocked = get_blocked_user_ids(&mut *db, &viewer.user.id).await?;
    let friends = get_friend_ids(&mut *db, &viewer.user.id).await?;
    if friends.is_empty() {
        return Ok(viewer);
    }
    for id in &friends {
        viewer.friends.insert(id.clone());
    }
    let query = format!(
        "SELECT user_low_id, user_high_id FROM friendships WHERE user_low_id IN ({}) OR user_high_id IN ({})",
        in_placeholders(1, friends.len()),
        in_placeholders(friends.len() + 1, friends.len())
    );
    let mut args = PgArguments::default();
    for id in friends.iter().chain(&friends) {
        args.add(id).map_err(|err| anyhow::anyhow!(err))?;
    }
    let friendships: Vec<Friendship> = sqlx::query_as_with(&query, args)
        .fetch_all(&mut *db)
        .await?;
    for friendship in friendships {
        if viewer.friends.contains(&friendship.user_low_id) {
            viewer
                .friends_of_friends
                .insert(friendship.user_high_id.clone());
            if friendship.user_high_id != viewer.user.id {
                *viewer
                    .mutual_friends
                    .entry(friendship.user_high_id.clone())
                    .or_default() += 1;
            }
        }
        if viewer.friends.contains(&friendship.user_high_id) {
            viewer
                .friends_of_friends
                .insert(friendship.user_low_id.clone());
            if friendship.user_low_id != viewer.user.id {
                *viewer
                    .mutual_friends
                    .entry(friendship.user_low_id.clone())
                    .or_default() += 1;
            }
        }
    }
    Ok(viewer)
}

impl ViewerContext {
    pub fn can_view(&self, post: &Post) -> bool {
        if post.author_id == self.user.id {
            return true;
        }
        if post.window_ended_at.is_some()
            || !self.has_posted_today
            || self.blocked.contains(&post.author_id)
        {
            return false;
        }
        match post.visibility.as_str() {
            "public" => true,
            "friends" => self.friends.contains(&post.author_id),
            "friends_of_friends" => {
                self.friends.contains(&post.author_id)
                    || self.friends_of_friends.contains(&post.author_id)
            }
            _ => false,
        }
    }
}

pub async fn can_view_post(
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
    Ok(viewer.can_view(&post.post))
}

pub async fn get_friend_ids<'c>(
    db: impl Executor<'c, Database = Postgres>,
    user_id: &str,
) -> Result<Vec<String>, Error> {
    let friendships: Vec<Friendship> = sqlx::query_as(
        "SELECT user_low_id, user_high_id FROM friendships WHERE user_low_id = $1 OR user_high_id = $1",
    )
    .bind(user_id)
    .fetch_all(db)
    .await?;
    let mut ids = Vec::new();
    for friendship in friendships {
        if friendship.user_low_id == user_id {
            ids.push(friendship.user_high_id);
        } else {
            ids.push(friendship.user_low_id);
        }
    }
    Ok(ids)
}

pub async fn get_blocked_user_ids<'c>(
    db: impl Executor<'c, Database = Postgres>,
    user_id: &str,
) -> Result<HashSet<String>, Error> {
    let ids: Vec<String> = sqlx::query_scalar(
        r#"
  SELECT CASE WHEN blocker_id = $1 THEN blocked_id ELSE blocker_id END
  FROM block_relations WHERE blocker_id = $1 OR blocked_id = $1
 "#,
    )
    .bind(user_id)
    .fetch_all(db)
    .await?;
    Ok(ids.into_iter().collect())
}
