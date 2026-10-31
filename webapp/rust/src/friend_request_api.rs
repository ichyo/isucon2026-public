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
use crate::post::get_friend_ids;
use crate::relationship::{
    begin_relationship_tx, get_friend_request, insert_friendship, is_blocked,
};
use crate::response::json;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::rfc3339_nano;

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CreateFriendRequestRequest {
    pub recipient_id: String,
}

impl Bindable for CreateFriendRequestRequest {}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateFriendRequestResponse {
    pub relation_id: String,
    pub status: String,
    #[serde(serialize_with = "rfc3339_nano")]
    pub updated_at: DateTime<Utc>,
}

pub async fn create_friend_request_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<CreateFriendRequestRequest>,
) -> Result<Response, Error> {
    let actor = &session.user_id;
    let mut tx = begin_relationship_tx(&h.db, actor, &input.recipient_id).await?;
    let blocked = is_blocked(&mut *tx, actor, &input.recipient_id).await?;
    if blocked {
        return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
    }
    let friends = get_friend_ids(&mut *tx, actor).await?;
    for id in &friends {
        if *id == input.recipient_id {
            return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
        }
    }
    let existing_id: Option<String> = sqlx::query_scalar(
        r#"
   SELECT id FROM friend_requests WHERE status='pending' AND
    ((requester_id=$1 AND recipient_id=$2) OR (requester_id=$2 AND recipient_id=$1))
  "#,
    )
    .bind(actor)
    .bind(&input.recipient_id)
    .fetch_optional(&mut *tx)
    .await?;
    let request_id = if let Some(existing_id) = existing_id {
        let request = get_friend_request(&mut *tx, &existing_id).await?;
        if request.requester_id == *actor {
            return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
        }
        sqlx::query("UPDATE friend_requests SET status='accepted', updated_at=clock_timestamp() WHERE id=$1")
            .bind(&request.id)
            .execute(&mut *tx)
            .await?;
        insert_friendship(&mut tx, &request).await?;
        request.id
    } else {
        let request_id = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO friend_requests (id, requester_id, recipient_id, status, created_at, updated_at) VALUES ($1,$2,$3,'pending',clock_timestamp(),clock_timestamp())")
            .bind(&request_id)
            .bind(actor)
            .bind(&input.recipient_id)
            .execute(&mut *tx)
            .await?;
        request_id
    };
    let request = get_friend_request(&mut *tx, &request_id).await?;
    tx.commit().await?;
    json(
        StatusCode::CREATED,
        CreateFriendRequestResponse {
            relation_id: request.id,
            status: request.status,
            updated_at: request.updated_at,
        },
    )
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct TransitionFriendRequestRequest {
    #[serde(skip)]
    pub request_id: String,
    #[serde(skip)]
    pub action: String,
}

impl Bindable for TransitionFriendRequestRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("requestId", &mut self.request_id)?;
        b.param("action", &mut self.action)
    }
}

pub type TransitionFriendRequestResponse = CreateFriendRequestResponse;

pub async fn transition_friend_request_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<TransitionFriendRequestRequest>,
) -> Result<Response, Error> {
    let status = match input.action.as_str() {
        "accept" => "accepted",
        "reject" => "rejected",
        "cancel" => "cancelled",
        _ => {
            return Err(new_api_error(
                StatusCode::FORBIDDEN,
                "relationship_action_forbidden",
            ));
        }
    };
    let actor = &session.user_id;
    let request = match get_friend_request(&h.db, &input.request_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(
                StatusCode::NOT_FOUND,
                "relationship_not_found",
            ));
        }
        result => result?,
    };
    if (status == "cancelled" && *actor != request.requester_id)
        || (status != "cancelled" && *actor != request.recipient_id)
    {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "relationship_action_forbidden",
        ));
    }
    let mut tx = begin_relationship_tx(&h.db, &request.requester_id, &request.recipient_id).await?;
    let request = get_friend_request(&mut *tx, &input.request_id).await?;
    if request.status != "pending" {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "relationship_action_forbidden",
        ));
    }
    if status == "accepted" {
        let blocked = is_blocked(&mut *tx, &request.requester_id, &request.recipient_id).await?;
        if blocked {
            return Err(new_api_error(StatusCode::CONFLICT, "relationship_conflict"));
        }
        insert_friendship(&mut tx, &request).await?;
    }
    sqlx::query("UPDATE friend_requests SET status=$2, updated_at=clock_timestamp() WHERE id=$1")
        .bind(&request.id)
        .bind(status)
        .execute(&mut *tx)
        .await?;
    let request = get_friend_request(&mut *tx, &request.id).await?;
    tx.commit().await?;
    let response: TransitionFriendRequestResponse = CreateFriendRequestResponse {
        relation_id: request.id,
        status: request.status,
        updated_at: request.updated_at,
    };
    json(StatusCode::OK, response)
}
