use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::action_log::Username;
use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable};
use crate::image_signature::signed_image_url;
use crate::response::{json, no_content};
use crate::server::HandlerContext;
use crate::session::{Session, create_login_session_tx, invalidate_login_session};
use crate::user::{USERNAME_PATTERN, User, get_user, valid_region, valid_user_text};

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RegisterRequest {
    pub username: String,
    pub display_name: String,
    pub region: String,
}

impl Bindable for RegisterRequest {}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterResponse {
    pub secret_key: String,
    pub user: SessionUserResponse,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LoginRequest {
    pub username: String,
    pub secret_key: String,
}

impl Bindable for LoginRequest {}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    pub token: String,
    pub user: SessionUserResponse,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionUserResponse {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub bio: String,
    pub region: String,
    pub account_type: String,
    pub status: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetMeResponse {
    #[serde(flatten)]
    pub session_user_response: SessionUserResponse,
    pub profile_image_url: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UpdateMeRequest {
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub region: Option<String>,
}

impl Bindable for UpdateMeRequest {}

pub type UpdateMeResponse = GetMeResponse;

pub fn new_session_user_response(user: &User) -> SessionUserResponse {
    SessionUserResponse {
        id: user.id.clone(),
        username: user.username.clone(),
        display_name: user.display_name.clone(),
        bio: user.bio.clone(),
        region: user.region.clone(),
        account_type: user.account_type.clone(),
        status: user.status.clone(),
    }
}

pub fn new_get_me_response(user: &User) -> GetMeResponse {
    let mut response = GetMeResponse {
        session_user_response: new_session_user_response(user),
        profile_image_url: None,
    };
    if let Some(profile_image_id) = &user.profile_image_id {
        let url = signed_image_url(user, profile_image_id);
        response.profile_image_url = Some(url);
    }
    response
}

pub async fn register_handler(
    State(h): State<Arc<HandlerContext>>,
    username: Username,
    Bind(input): Bind<RegisterRequest>,
) -> Result<Response, Error> {
    username.set(&input.username);
    if !USERNAME_PATTERN.is_match(&input.username)
        || !valid_user_text(&input.display_name, 1, 100)
        || !valid_region(&input.region)
    {
        return Err(new_api_error(
            StatusCode::BAD_REQUEST,
            "invalid_registration",
        ));
    }
    let secret_key = Uuid::new_v4().to_string();
    let hash = Sha256::digest(secret_key.as_bytes());
    let mut tx = h.db.begin().await?;
    // ユーザー名が一意である必要があります。
    sqlx::query("LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)")
        .bind(&input.username)
        .fetch_one(&mut *tx)
        .await?;
    if exists {
        return Err(new_api_error(StatusCode::CONFLICT, "username_taken"));
    }
    let user_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO users (id, username, secret_key_hash, display_name, region) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&user_id)
    .bind(&input.username)
    .bind(hex::encode(hash))
    .bind(&input.display_name)
    .bind(&input.region)
    .execute(&mut *tx)
    .await?;
    let user = get_user(&mut *tx, &user_id).await?;
    tx.commit().await?;
    json(
        StatusCode::CREATED,
        RegisterResponse {
            secret_key,
            user: new_session_user_response(&user),
        },
    )
}

pub async fn login_handler(
    State(h): State<Arc<HandlerContext>>,
    username: Username,
    Bind(input): Bind<LoginRequest>,
) -> Result<Response, Error> {
    username.set(&input.username);
    if !USERNAME_PATTERN.is_match(&input.username) {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
        ));
    }
    let mut tx = h.db.begin().await?;
    let user: Option<User> = sqlx::query_as("SELECT id, username, secret_key_hash, display_name, bio, profile_image_id, region, account_type, status FROM users WHERE username = $1 FOR NO KEY UPDATE")
        .bind(&input.username)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(user) = user else {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
        ));
    };
    let hash = Sha256::digest(input.secret_key.as_bytes());
    if user.status != "active" || user.secret_key_hash != hex::encode(hash) {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
        ));
    }
    let session = create_login_session_tx(&mut tx, &user.id).await?;
    tx.commit().await?;
    json(
        StatusCode::OK,
        LoginResponse {
            token: session.token,
            user: new_session_user_response(&user),
        },
    )
}

pub async fn logout_handler(
    State(h): State<Arc<HandlerContext>>,
    session: Option<Extension<Session>>,
) -> Result<Response, Error> {
    let Some(Extension(session)) = session else {
        return Err(new_api_error(StatusCode::UNAUTHORIZED, "invalid_session"));
    };
    invalidate_login_session(&h.db, &session.user_id, &session.token_hash).await?;
    no_content(StatusCode::NO_CONTENT)
}

pub async fn get_me_handler(
    State(h): State<Arc<HandlerContext>>,
    session: Option<Extension<Session>>,
) -> Result<Response, Error> {
    let Some(Extension(session)) = session else {
        return Err(new_api_error(StatusCode::UNAUTHORIZED, "invalid_session"));
    };
    let user = match get_user(&h.db, &session.user_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(StatusCode::NOT_FOUND, "account_not_found"));
        }
        result => result?,
    };
    json(StatusCode::OK, new_get_me_response(&user))
}

pub async fn update_me_handler(
    State(h): State<Arc<HandlerContext>>,
    session: Option<Extension<Session>>,
    Bind(input): Bind<UpdateMeRequest>,
) -> Result<Response, Error> {
    let Some(Extension(session)) = session else {
        return Err(new_api_error(StatusCode::UNAUTHORIZED, "invalid_session"));
    };
    if (input.display_name.is_none() && input.bio.is_none() && input.region.is_none())
        || input
            .display_name
            .as_ref()
            .is_some_and(|display_name| !valid_user_text(display_name, 1, 100))
        || input
            .bio
            .as_ref()
            .is_some_and(|bio| !valid_user_text(bio, 0, 500))
        || input
            .region
            .as_ref()
            .is_some_and(|region| !valid_region(region))
    {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_profile"));
    }
    let mut tx = h.db.begin().await?;
    if let Some(display_name) = &input.display_name {
        sqlx::query(
            "UPDATE users SET display_name = $2, updated_at = clock_timestamp() WHERE id = $1",
        )
        .bind(&session.user_id)
        .bind(display_name)
        .execute(&mut *tx)
        .await?;
    }
    if let Some(bio) = &input.bio {
        sqlx::query("UPDATE users SET bio = $2, updated_at = clock_timestamp() WHERE id = $1")
            .bind(&session.user_id)
            .bind(bio)
            .execute(&mut *tx)
            .await?;
    }
    if let Some(region) = &input.region {
        sqlx::query("UPDATE users SET region = $2, updated_at = clock_timestamp() WHERE id = $1")
            .bind(&session.user_id)
            .bind(region)
            .execute(&mut *tx)
            .await?;
    }
    let user = match get_user(&mut *tx, &session.user_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(StatusCode::NOT_FOUND, "account_not_found"));
        }
        result => result?,
    };
    tx.commit().await?;
    let response: UpdateMeResponse = new_get_me_response(&user);
    json(StatusCode::OK, response)
}
