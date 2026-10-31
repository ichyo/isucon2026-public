use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder};
use crate::response::json;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::rfc3339_nano;
use crate::user::{User, user_columns_as};
use crate::user_read_api::new_relationship_user_response;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetRelationshipsRequest {
    #[serde(skip)]
    pub kind: String,
    #[serde(skip)]
    pub before_updated_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub before_id: String,
}

impl Bindable for GetRelationshipsRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("kind", &mut self.kind)?;
        b.query("beforeUpdatedAt", &mut self.before_updated_at)?;
        b.query("beforeId", &mut self.before_id)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipUserResponse {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub profile_image_url: Option<String>,
    pub bio: String,
    pub account_type: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipResponse {
    pub relation_id: String,
    pub user: RelationshipUserResponse,
    pub status: String,
    #[serde(serialize_with = "rfc3339_nano")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRelationshipsResponse {
    pub items: Vec<RelationshipResponse>,
    pub has_more: bool,
}

pub async fn get_relationships_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetRelationshipsRequest>,
) -> Result<Response, Error> {
    if input.before_updated_at.is_none() != input.before_id.is_empty() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_pagination"));
    }
    let query = match input.kind.as_str() {
        "friends" => {
            "SELECT id, user_low_id AS first_id, user_high_id AS second_id, updated_at FROM friendships WHERE user_low_id=$1 OR user_high_id=$1"
        }
        "following" => {
            "SELECT id, follower_id AS first_id, followee_id AS second_id, updated_at FROM follows WHERE follower_id=$1"
        }
        "followers" => {
            "SELECT id, follower_id AS first_id, followee_id AS second_id, updated_at FROM follows WHERE followee_id=$1"
        }
        "incoming-requests" => {
            "SELECT id, requester_id AS first_id, recipient_id AS second_id, updated_at FROM friend_requests WHERE recipient_id=$1 AND status='pending'"
        }
        "outgoing-requests" => {
            "SELECT id, requester_id AS first_id, recipient_id AS second_id, updated_at FROM friend_requests WHERE requester_id=$1 AND status='pending'"
        }
        _ => {
            return Err(new_api_error(
                StatusCode::BAD_REQUEST,
                "invalid_relationship",
            ));
        }
    };
    let actor = &session.user_id;
    struct Record {
        id: String,
        updated_at: DateTime<Utc>,
        user: User,
    }
    impl FromRow<'_, PgRow> for Record {
        fn from_row(row: &PgRow) -> sqlx::Result<Record> {
            Ok(Record {
                id: row.try_get("id")?,
                updated_at: row.try_get("updated_at")?,
                user: User::from_row_with_prefix(row, "user")?,
            })
        }
    }
    let records: Vec<Record> = sqlx::query_as(&format!(
        r#"
   SELECT relations.id, relations.updated_at, {}
   FROM ({query}) relations
   JOIN users u ON u.status = 'active'
               AND u.id = CASE WHEN relations.first_id = $1 THEN relations.second_id ELSE relations.first_id END
   WHERE ($2::timestamptz IS NULL OR (relations.updated_at, relations.id) < ($2, $3))
   ORDER BY relations.updated_at DESC, relations.id DESC LIMIT 51
  "#,
        user_columns_as("u", "user")
    ))
    .bind(actor)
    .bind(input.before_updated_at)
    .bind(&input.before_id)
    .fetch_all(&h.db)
    .await?;
    let mut items = Vec::with_capacity(records.len());
    for record in records {
        let mut user = record.user;
        user.set_image_key()?;
        if user.account_type != "certified_public" && input.kind != "friends" {
            user.bio = String::new();
        }
        let mut status = input.kind.clone();
        if input.kind == "incoming-requests" || input.kind == "outgoing-requests" {
            status = "pending".to_owned();
        }
        items.push(RelationshipResponse {
            relation_id: record.id,
            status,
            updated_at: record.updated_at,
            user: new_relationship_user_response(&user),
        });
    }
    let has_more = items.len() > 50;
    if has_more {
        items.truncate(50);
    }
    json(StatusCode::OK, GetRelationshipsResponse { items, has_more })
}
