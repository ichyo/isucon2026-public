use std::sync::Arc;

use axum::extract::{RawQuery, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use serde::Deserialize;
use subtle::ConstantTimeEq;

use crate::api_error::{Error, new_abuse_error, new_api_error};
use crate::bind::{Bind, Bindable, Binder, parse_query};
use crate::image_signature::{image_signature, image_signing_key};
use crate::response::blob;
use crate::server::HandlerContext;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetImageRequest {
    #[serde(skip)]
    pub image_id: String,
    #[serde(skip)]
    pub exp: String,
    #[serde(skip)]
    pub sig: String,
}

impl Bindable for GetImageRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("imageId", &mut self.image_id)?;
        b.query("exp", &mut self.exp)?;
        b.query("sig", &mut self.sig)
    }
}

pub async fn get_image_handler(
    State(h): State<Arc<HandlerContext>>,
    RawQuery(raw_query): RawQuery,
    input: Result<Bind<GetImageRequest>, Error>,
) -> Response {
    let mut response = get_image(&h, raw_query.unwrap_or_default(), input)
        .await
        .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn get_image(
    h: &HandlerContext,
    raw_query: String,
    input: Result<Bind<GetImageRequest>, Error>,
) -> Result<Response, Error> {
    let Ok(Bind(input)) = input else {
        return Err(new_api_error(StatusCode::FORBIDDEN, "image_forbidden"));
    };
    let query = parse_query(&raw_query);
    if !query.is_ok_and(|query| {
        query.get("exp").map(Vec::len) == Some(1) && query.get("sig").map(Vec::len) == Some(1)
    }) {
        return Err(new_api_error(StatusCode::FORBIDDEN, "image_forbidden"));
    }
    let exp = match input.exp.parse::<i64>() {
        Ok(exp) if exp.to_string() == input.exp && exp > Utc::now().timestamp() => exp,
        _ => return Err(new_api_error(StatusCode::FORBIDDEN, "image_forbidden")),
    };
    let sig = match URL_SAFE_NO_PAD.decode(&input.sig) {
        Ok(sig) if sig.len() == 32 && URL_SAFE_NO_PAD.encode(&sig) == input.sig => sig,
        _ => return Err(new_api_error(StatusCode::FORBIDDEN, "image_forbidden")),
    };

    let secret_key_hash: Option<String> = sqlx::query_scalar(
        r#"
   SELECT u.secret_key_hash
   FROM image_objects i JOIN users u ON u.id = i.owner_user_id
   WHERE i.id = $1 AND u.status = 'active'
  "#,
    )
    .bind(&input.image_id)
    .fetch_optional(&h.db)
    .await?;
    let Some(secret_key_hash) = secret_key_hash else {
        return Err(new_api_error(StatusCode::FORBIDDEN, "image_forbidden"));
    };
    let key = image_signing_key(&secret_key_hash)?;
    if !bool::from(sig.ct_eq(&image_signature(&key, &input.image_id, exp))) {
        return Err(new_abuse_error(
            StatusCode::FORBIDDEN,
            "image_forbidden",
            "invalid_signature",
        ));
    }

    // 削除状態を毎回確認する。即時反映も5秒以内の許容範囲に含まれる。
    #[derive(sqlx::FromRow)]
    struct ImageObject {
        data: Vec<u8>,
        media_type: String,
    }
    let object: Option<ImageObject> = sqlx::query_as(
        r#"
   SELECT i.data, i.media_type
   FROM image_objects i JOIN users u ON u.id = i.owner_user_id AND u.status = 'active'
   WHERE i.id = $1 AND (
     u.profile_image_id = i.id
     OR EXISTS (
       SELECT 1 FROM posts p
       WHERE p.image_id = i.id AND p.author_id = u.id AND p.deleted_at IS NULL
     )
     OR EXISTS (
       SELECT 1 FROM realmojis r
       JOIN posts p ON p.id = r.post_id AND p.deleted_at IS NULL
       JOIN users author ON author.id = p.author_id AND author.status = 'active'
       WHERE r.image_id = i.id AND r.reactor_id = u.id AND r.replaced_at IS NULL AND r.deleted_at IS NULL
     )
   )
  "#,
    )
    .bind(&input.image_id)
    .fetch_optional(&h.db)
    .await?;
    let Some(object) = object else {
        return Err(new_api_error(StatusCode::FORBIDDEN, "image_forbidden"));
    };
    if exp <= Utc::now().timestamp() {
        return Err(new_api_error(StatusCode::FORBIDDEN, "image_forbidden"));
    }
    blob(StatusCode::OK, &object.media_type, object.data)
}
