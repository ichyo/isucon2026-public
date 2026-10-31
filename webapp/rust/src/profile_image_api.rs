use std::sync::Arc;

use anyhow::Context as _;
use axum::Extension;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::response::Response;
use uuid::Uuid;

use crate::api_error::{Error, new_abuse_error, new_api_error};
use crate::jpeg;
use crate::response::no_content;
use crate::server::HandlerContext;
use crate::session::Session;

pub async fn put_profile_image_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    req: Request,
) -> Result<Response, Error> {
    if req
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|value| value.as_bytes())
        != Some(b"image/jpeg")
    {
        return Err(new_api_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        ));
    }
    let data = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .context("failed to read request body")?;
    let img = match jpeg::decode(data.clone()).await {
        Ok(img) => img,
        Err(_) => {
            return Err(new_abuse_error(
                StatusCode::BAD_REQUEST,
                "invalid_profile_image",
                "invalid_image",
            ));
        }
    };
    if img.width > 512 || img.height > 512 {
        return Err(new_api_error(
            StatusCode::BAD_REQUEST,
            "invalid_profile_image",
        ));
    }

    let user_id = &session.user_id;
    let mut tx = h.db.begin().await?;
    let old_id: Option<Option<String>> =
        sqlx::query_scalar("SELECT profile_image_id FROM users WHERE id=$1 FOR NO KEY UPDATE")
            .bind(user_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(old_id) = old_id else {
        return Err(new_api_error(StatusCode::NOT_FOUND, "account_not_found"));
    };
    let image_id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO image_objects(id,owner_user_id,data,media_type) VALUES ($1,$2,$3,'image/jpeg')")
        .bind(&image_id)
        .bind(user_id)
        .bind(&data[..])
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE users SET profile_image_id=$2,updated_at=clock_timestamp() WHERE id=$1")
        .bind(user_id)
        .bind(&image_id)
        .execute(&mut *tx)
        .await?;
    if let Some(old_id) = old_id {
        sqlx::query("DELETE FROM image_objects WHERE id=$1")
            .bind(old_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}

pub async fn delete_profile_image_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
) -> Result<Response, Error> {
    let user_id = &session.user_id;
    let mut tx = h.db.begin().await?;
    let image_id: Option<Option<String>> =
        sqlx::query_scalar("SELECT profile_image_id FROM users WHERE id=$1 FOR NO KEY UPDATE")
            .bind(user_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(image_id) = image_id else {
        return Err(new_api_error(StatusCode::NOT_FOUND, "account_not_found"));
    };
    let Some(image_id) = image_id else {
        return Err(new_api_error(StatusCode::NOT_FOUND, "account_not_found"));
    };
    sqlx::query("UPDATE users SET profile_image_id=NULL,updated_at=clock_timestamp() WHERE id=$1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM image_objects WHERE id=$1")
        .bind(&image_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}
