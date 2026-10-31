use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::admin_session::{AdminSession, create_admin_session, invalidate_admin_session};
use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable};
use crate::response::{json, no_content};
use crate::server::HandlerContext;
use crate::user::USERNAME_PATTERN;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct AdminLoginRequest {
    pub username: String,
    pub password: String,
}

impl Bindable for AdminLoginRequest {}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminLoginResponse {
    pub token: String,
    pub administrator_id: String,
}

pub async fn admin_login_handler(
    State(h): State<Arc<HandlerContext>>,
    Bind(input): Bind<AdminLoginRequest>,
) -> Result<Response, Error> {
    if !USERNAME_PATTERN.is_match(&input.username) {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
        ));
    }
    #[derive(sqlx::FromRow)]
    struct Administrator {
        id: String,
        password_hash: String,
    }
    let administrator: Option<Administrator> =
        sqlx::query_as("SELECT id, password_hash FROM administrators WHERE username = $1")
            .bind(&input.username)
            .fetch_optional(&h.db)
            .await?;
    let Some(administrator) = administrator else {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
        ));
    };
    let hash = Sha256::digest(input.password.as_bytes());
    if administrator.password_hash != hex::encode(hash) {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
        ));
    }
    let session = create_admin_session(&h.db, &administrator.id).await?;
    json(
        StatusCode::OK,
        AdminLoginResponse {
            token: session.token,
            administrator_id: session.administrator_id,
        },
    )
}

pub async fn admin_logout_handler(
    State(h): State<Arc<HandlerContext>>,
    session: Option<Extension<AdminSession>>,
) -> Result<Response, Error> {
    let Some(Extension(session)) = session else {
        return Err(new_api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_administrator_session",
        ));
    };
    invalidate_admin_session(&h.db, &session.administrator_id, &session.token_hash).await?;
    no_content(StatusCode::NO_CONTENT)
}
