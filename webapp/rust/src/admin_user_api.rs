use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder};
use crate::response::no_content;
use crate::server::HandlerContext;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct CertifyUserRequest {
    #[serde(skip)]
    pub user_id: String,
}

impl Bindable for CertifyUserRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("userId", &mut self.user_id)
    }
}

pub async fn certify_user_handler(
    State(h): State<Arc<HandlerContext>>,
    Bind(input): Bind<CertifyUserRequest>,
) -> Result<Response, Error> {
    let result = sqlx::query("UPDATE users SET account_type='certified_public', updated_at=clock_timestamp() WHERE id=$1 AND status='active' AND account_type='regular'")
        .bind(&input.user_id)
        .execute(&h.db)
        .await?;
    let count = result.rows_affected();
    if count == 0 {
        return Err(new_api_error(
            StatusCode::CONFLICT,
            "certification_conflict",
        ));
    }
    no_content(StatusCode::NO_CONTENT)
}

// 2026-08-26: 認定されたユーザーを戻すことは禁止する
pub async fn delete_user_certification_handler() -> Result<Response, Error> {
    Err(new_api_error(
        StatusCode::CONFLICT,
        "certification_irreversible",
    ))
}
