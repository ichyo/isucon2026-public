use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder, FileHeader};
use crate::db::f64_to_numeric;
use crate::image_signature::signed_image_url;
use crate::jpeg;
use crate::post::{Post, PostLocation, get_post, get_post_with_author, load_viewer_context};
use crate::posting_window::get_posting_window;
use crate::response::{json, no_content};
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::rfc3339_nano;
use crate::user::{User, get_user, valid_user_text};
use crate::user_api::new_get_me_response;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct CreatePostRequest {
    #[serde(skip)]
    pub image: Option<FileHeader>,
    #[serde(skip)]
    pub caption: String,
    #[serde(skip)]
    pub visibility: String,
    #[serde(skip)]
    pub location_kind: String,
    #[serde(skip)]
    pub latitude: Option<f64>,
    #[serde(skip)]
    pub longitude: Option<f64>,
    #[serde(skip)]
    pub locality: Option<String>,
}

impl Bindable for CreatePostRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.form_file("image", &mut self.image)?;
        b.form("caption", &mut self.caption)?;
        b.form("visibility", &mut self.visibility)?;
        b.form("location_kind", &mut self.location_kind)?;
        b.form("latitude", &mut self.latitude)?;
        b.form("longitude", &mut self.longitude)?;
        b.form("locality", &mut self.locality)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostAuthorResponse {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub profile_image_url: Option<String>,
    pub bio: String,
    pub account_type: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePostResponse {
    pub id: String,
    pub author: PostAuthorResponse,
    pub posting_window_id: String,
    pub image_url: String,
    pub caption: String,
    pub visibility: String,
    pub location: PostLocation,
    #[serde(serialize_with = "rfc3339_nano")]
    pub posted_at: DateTime<Utc>,
    pub on_time: bool,
    pub late_seconds: i64,
}

pub async fn create_post_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<CreatePostRequest>,
) -> Result<Response, Error> {
    let Some(image) = &input.image else {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post"));
    };
    if !valid_user_text(&input.caption, 0, 1000) {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post"));
    }
    match input.visibility.as_str() {
        "friends" | "friends_of_friends" | "public" => {}
        _ => return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post")),
    }
    let location = PostLocation {
        kind: input.location_kind.clone(),
        latitude: input.latitude,
        longitude: input.longitude,
        locality: input.locality.clone(),
    };
    if !location.valid() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post"));
    }
    let image_data = image.open();
    let img = match jpeg::decode(image_data.clone()).await {
        Ok(img) => img,
        Err(_) => return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post")),
    };
    if img.width > 4096 || img.height > 4096 {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post"));
    }

    let user_id = &session.user_id;
    let mut tx = h.db.begin().await?;
    let _locked_user_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE")
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?;
    let user = get_user(&mut *tx, user_id).await?;
    #[derive(sqlx::FromRow)]
    struct Window {
        id: String,
        local_date: NaiveDate,
        starts_at: DateTime<Utc>,
    }
    let mut window: Option<Window> = sqlx::query_as(
        "SELECT id, local_date, starts_at FROM posting_windows WHERE region = $1 AND ended_at IS NULL FOR UPDATE",
    )
    .bind(&user.region)
    .fetch_optional(&mut *tx)
    .await?;
    if window.is_none() {
        window = sqlx::query_as(
            "SELECT id, local_date, starts_at FROM posting_windows WHERE region = $1 AND ended_at IS NULL FOR UPDATE",
        )
        .bind(&user.region)
        .fetch_optional(&mut *tx)
        .await?;
    }
    let Some(window) = window else {
        return Err(new_api_error(StatusCode::CONFLICT, "post_slot_unavailable"));
    };
    #[derive(sqlx::FromRow)]
    struct Counts {
        active: i64,
        deleted: i64,
    }
    let counts: Counts = sqlx::query_as(
        r#"
   SELECT count(*) FILTER (WHERE deleted_at IS NULL) AS active,
          count(*) FILTER (WHERE deleted_at IS NOT NULL) AS deleted
   FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE p.author_id = $1 AND w.local_date = $2
  "#,
    )
    .bind(user_id)
    .bind(window.local_date)
    .fetch_one(&mut *tx)
    .await?;
    let posted_at = Utc::now();
    if counts.active != 0 || counts.deleted >= 2 || posted_at < window.starts_at {
        return Err(new_api_error(StatusCode::CONFLICT, "post_slot_unavailable"));
    }
    let image_id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO image_objects (id, owner_user_id, data, media_type, created_at) VALUES ($1, $2, $3, 'image/jpeg', $4)")
        .bind(&image_id)
        .bind(user_id)
        .bind(&image_data[..])
        .bind(posted_at)
        .execute(&mut *tx)
        .await?;
    let post_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
   INSERT INTO posts (id, author_id, posting_window_id, image_id, caption, visibility,
                      location_kind, latitude, longitude, locality, posted_at)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
  "#,
    )
    .bind(&post_id)
    .bind(user_id)
    .bind(&window.id)
    .bind(&image_id)
    .bind(&input.caption)
    .bind(&input.visibility)
    .bind(&location.kind)
    .bind(f64_to_numeric(location.latitude))
    .bind(f64_to_numeric(location.longitude))
    .bind(&location.locality)
    .bind(posted_at)
    .execute(&mut *tx)
    .await?;
    let post = get_post(&mut *tx, &post_id).await?;
    tx.commit().await?;
    json(StatusCode::CREATED, new_post_response(&post, &user))
}

pub fn new_post_response(post: &Post, user: &User) -> CreatePostResponse {
    let mut response = CreatePostResponse {
        id: post.id.clone(),
        author: PostAuthorResponse {
            id: user.id.clone(),
            username: user.username.clone(),
            display_name: user.display_name.clone(),
            profile_image_url: new_get_me_response(user).profile_image_url,
            bio: user.bio.clone(),
            account_type: user.account_type.clone(),
        },
        posting_window_id: post.posting_window_id.clone(),
        image_url: signed_image_url(user, &post.image_id),
        caption: post.caption.clone(),
        visibility: post.visibility.clone(),
        location: post.post_location.clone(),
        posted_at: post.posted_at,
        on_time: post.posted_at <= post.on_time_deadline,
        late_seconds: 0,
    };
    if !response.on_time {
        response.late_seconds = (post.posted_at - post.on_time_deadline).num_seconds();
    }
    response
}

pub type GetMyCurrentPostResponse = CreatePostResponse;

pub async fn get_my_current_post_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
) -> Result<Response, Error> {
    let user_id = &session.user_id;
    let post_id: Option<String> = sqlx::query_scalar(
        r#"
   SELECT p.id FROM posts p
   JOIN posting_windows w ON w.id = p.posting_window_id
   JOIN users u ON u.id = p.author_id
   JOIN posting_windows current ON current.region = u.region AND current.ended_at IS NULL
                              AND current.local_date = w.local_date
   WHERE p.author_id = $1 AND p.deleted_at IS NULL
  "#,
    )
    .bind(user_id)
    .fetch_optional(&h.db)
    .await?;
    let Some(post_id) = post_id else {
        return Err(new_api_error(StatusCode::NOT_FOUND, "post_not_found"));
    };
    let post = match get_post(&h.db, &post_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(StatusCode::NOT_FOUND, "post_not_found"));
        }
        result => result?,
    };
    let user = get_user(&h.db, user_id).await?;
    let response: GetMyCurrentPostResponse = new_post_response(&post, &user);
    json(StatusCode::OK, response)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetPostRequest {
    #[serde(skip)]
    pub post_id: String,
}

impl Bindable for GetPostRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("postId", &mut self.post_id)
    }
}

pub type GetPostResponse = CreatePostResponse;

pub async fn get_post_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetPostRequest>,
) -> Result<Response, Error> {
    let post = match get_post_with_author(&h.db, &input.post_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(StatusCode::NOT_FOUND, "post_not_found"));
        }
        result => result?,
    };
    let viewer = match load_viewer_context(&mut *h.db.acquire().await?, &session.user_id).await {
        Err(err) if err.is_no_rows() => None,
        result => Some(result?),
    };
    if !viewer.is_some_and(|viewer| viewer.can_view(&post.post)) {
        return Err(new_api_error(StatusCode::NOT_FOUND, "post_not_found"));
    }
    let response: GetPostResponse = new_post_response(&post.post, &post.author);
    json(StatusCode::OK, response)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct UpdatePostRequest {
    #[serde(skip)]
    pub post_id: String,
    pub caption: Option<String>,
    pub visibility: Option<String>,
}

impl Bindable for UpdatePostRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("postId", &mut self.post_id)
    }
}

pub type UpdatePostResponse = CreatePostResponse;

pub async fn update_post_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<UpdatePostRequest>,
) -> Result<Response, Error> {
    if input.caption.is_none() && input.visibility.is_none() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post"));
    }
    if input
        .caption
        .as_ref()
        .is_some_and(|caption| !valid_user_text(caption, 0, 1000))
    {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post"));
    }
    if let Some(visibility) = &input.visibility {
        match visibility.as_str() {
            "friends" | "friends_of_friends" | "public" => {}
            _ => return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_post")),
        }
    }
    let user_id = &session.user_id;
    let mut tx = h.db.begin().await?;
    let _locked_user_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE")
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?;
    let post = match get_post(&mut *tx, &input.post_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(StatusCode::NOT_FOUND, "post_not_found"));
        }
        result => result?,
    };
    if post.author_id != *user_id {
        return Err(new_api_error(StatusCode::FORBIDDEN, "post_not_editable"));
    }
    let window = get_posting_window(&mut *tx, &post.posting_window_id).await?;
    if window.ended_at.is_some() {
        return Err(new_api_error(StatusCode::FORBIDDEN, "post_not_editable"));
    }
    if let Some(caption) = &input.caption {
        sqlx::query("UPDATE posts SET caption = $2 WHERE id = $1")
            .bind(&post.id)
            .bind(caption)
            .execute(&mut *tx)
            .await?;
    }
    if let Some(visibility) = &input.visibility {
        sqlx::query("UPDATE posts SET visibility = $2 WHERE id = $1")
            .bind(&post.id)
            .bind(visibility)
            .execute(&mut *tx)
            .await?;
    }
    let post = get_post(&mut *tx, &post.id).await?;
    let user = get_user(&mut *tx, user_id).await?;
    tx.commit().await?;
    let response: UpdatePostResponse = new_post_response(&post, &user);
    json(StatusCode::OK, response)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct DeletePostRequest {
    #[serde(skip)]
    pub post_id: String,
}

impl Bindable for DeletePostRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("postId", &mut self.post_id)
    }
}

pub async fn delete_post_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<DeletePostRequest>,
) -> Result<Response, Error> {
    let user_id = &session.user_id;
    let mut tx = h.db.begin().await?;
    let _locked_user_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE")
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?;
    let post = match get_post(&mut *tx, &input.post_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(StatusCode::NOT_FOUND, "post_not_found"));
        }
        result => result?,
    };
    if post.author_id != *user_id {
        return Err(new_api_error(StatusCode::FORBIDDEN, "post_not_editable"));
    }
    let window = get_posting_window(&mut *tx, &post.posting_window_id).await?;
    let deletions: i64 = sqlx::query_scalar(
        r#"
   SELECT count(*) FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE p.author_id = $1 AND w.local_date = $2 AND p.deleted_at IS NOT NULL
  "#,
    )
    .bind(user_id)
    .bind(window.local_date)
    .fetch_one(&mut *tx)
    .await?;
    if deletions >= 2 {
        return Err(new_api_error(StatusCode::CONFLICT, "post_slot_unavailable"));
    }
    sqlx::query("UPDATE posts SET deleted_at = clock_timestamp() WHERE id = $1")
        .bind(&post.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}
