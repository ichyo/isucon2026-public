use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder, FileHeader};
use crate::db::args;
use crate::image_signature::signed_image_url;
use crate::jpeg;
use crate::post::{can_view_post, get_post};
use crate::realmoji::{RealMoji, get_real_moji, select_current_real_mojis};
use crate::relationship::begin_relationship_tx;
use crate::relationship_list_api::RelationshipUserResponse;
use crate::response::{json, no_content};
use crate::server::HandlerContext;
use crate::session::Session;
use crate::timefmt::rfc3339_nano;
use crate::user::{User, get_user};
use crate::user_read_api::new_relationship_user_response;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct ReplaceRealMojiRequest {
    #[serde(skip)]
    pub post_id: String,
    #[serde(skip)]
    pub image: Option<FileHeader>,
    #[serde(skip)]
    pub reaction_type: String,
}

impl Bindable for ReplaceRealMojiRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("postId", &mut self.post_id)?;
        b.form_file("image", &mut self.image)?;
        b.form("reactionType", &mut self.reaction_type)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplaceRealMojiResponse {
    pub id: String,
    pub reactor: RelationshipUserResponse,
    pub reaction_type: String,
    pub image_url: String,
    #[serde(serialize_with = "rfc3339_nano")]
    pub created_at: DateTime<Utc>,
}

pub fn new_real_moji_response(reaction: &RealMoji, user: &User) -> ReplaceRealMojiResponse {
    ReplaceRealMojiResponse {
        id: reaction.id.clone(),
        reactor: new_relationship_user_response(user),
        reaction_type: reaction.reaction_type.clone(),
        image_url: signed_image_url(user, &reaction.image_id),
        created_at: reaction.created_at,
    }
}

pub async fn replace_real_moji_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<ReplaceRealMojiRequest>,
) -> Result<Response, Error> {
    let Some(image) = &input.image else {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_realmoji"));
    };
    match input.reaction_type.as_str() {
        "like" | "smile" | "surprise" | "heart" | "laugh" | "instant" => {}
        _ => return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_realmoji")),
    }
    let data = image.open();
    let img = match jpeg::decode(data.clone()).await {
        Ok(img) => img,
        Err(_) => return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_realmoji")),
    };
    if img.width > 512 || img.height > 512 {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_realmoji"));
    }

    let actor = &session.user_id;
    let post = match get_post(&h.db, &input.post_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(
                StatusCode::FORBIDDEN,
                "interaction_forbidden",
            ));
        }
        result => result?,
    };
    if *actor == post.author_id {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "interaction_forbidden",
        ));
    }
    let mut tx = begin_relationship_tx(&h.db, actor, &post.author_id).await?;
    let allowed = can_view_post(&mut tx, actor, &post.id).await?;
    if !allowed {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "interaction_forbidden",
        ));
    }
    sqlx::query("UPDATE realmojis SET replaced_at=clock_timestamp() WHERE post_id=$1 AND reactor_id=$2 AND replaced_at IS NULL AND deleted_at IS NULL")
        .bind(&post.id)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
    let image_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO image_objects(id,owner_user_id,data,media_type) VALUES($1,$2,$3,'image/jpeg')",
    )
    .bind(&image_id)
    .bind(actor)
    .bind(&data[..])
    .execute(&mut *tx)
    .await?;
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO realmojis(id,post_id,reactor_id,image_id,reaction_type,created_at) VALUES($1,$2,$3,$4,$5,clock_timestamp())")
        .bind(&id)
        .bind(&post.id)
        .bind(actor)
        .bind(&image_id)
        .bind(&input.reaction_type)
        .execute(&mut *tx)
        .await?;
    let reaction = get_real_moji(&mut *tx, &id).await?;
    let user = get_user(&mut *tx, actor).await?;
    tx.commit().await?;
    json(
        StatusCode::CREATED,
        new_real_moji_response(&reaction, &user),
    )
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetRealMojisRequest {
    #[serde(skip)]
    pub post_id: String,
    #[serde(skip)]
    pub before_created_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub before_id: String,
}

impl Bindable for GetRealMojisRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("postId", &mut self.post_id)?;
        b.query("beforeCreatedAt", &mut self.before_created_at)?;
        b.query("beforeId", &mut self.before_id)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRealMojisResponse {
    pub items: Vec<ReplaceRealMojiResponse>,
    pub has_more: bool,
}

pub async fn get_real_mojis_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetRealMojisRequest>,
) -> Result<Response, Error> {
    if input.before_created_at.is_none() != input.before_id.is_empty() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_pagination"));
    }
    let allowed = can_view_post(
        &mut *h.db.acquire().await?,
        &session.user_id,
        &input.post_id,
    )
    .await?;
    if !allowed {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "interaction_forbidden",
        ));
    }
    let reactions = select_current_real_mojis(
        &h.db,
        "AND r.post_id=$1 AND ($2::timestamptz IS NULL OR (r.created_at,r.id)<($2,$3)) ORDER BY r.created_at DESC,r.id DESC LIMIT 51",
        args![&input.post_id, input.before_created_at, &input.before_id],
    )
    .await?;
    let mut items = Vec::with_capacity(reactions.len());
    for reaction in &reactions {
        items.push(new_real_moji_response(
            &reaction.real_moji,
            &reaction.reactor,
        ));
    }
    let has_more = items.len() > 50;
    if has_more {
        items.truncate(50);
    }
    json(StatusCode::OK, GetRealMojisResponse { items, has_more })
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct DeleteRealMojiRequest {
    #[serde(skip)]
    pub real_moji_id: String,
}

impl Bindable for DeleteRealMojiRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("realMojiId", &mut self.real_moji_id)
    }
}

pub async fn delete_real_moji_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<DeleteRealMojiRequest>,
) -> Result<Response, Error> {
    let actor = &session.user_id;
    let mut tx = h.db.begin().await?;
    let _locked_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE id=$1 FOR NO KEY UPDATE")
            .bind(actor)
            .fetch_one(&mut *tx)
            .await?;
    #[derive(sqlx::FromRow)]
    struct Record {
        reactor_id: String,
        author_id: String,
    }
    let record: Option<Record> = sqlx::query_as("SELECT r.reactor_id,p.author_id FROM realmojis r JOIN posts p ON p.id=r.post_id WHERE r.id=$1 AND r.deleted_at IS NULL FOR UPDATE OF r")
        .bind(&input.real_moji_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(record) = record else {
        return Err(new_api_error(
            StatusCode::NOT_FOUND,
            "interaction_not_found",
        ));
    };
    if *actor != record.reactor_id && *actor != record.author_id {
        return Err(new_api_error(
            StatusCode::FORBIDDEN,
            "interaction_forbidden",
        ));
    }
    sqlx::query("UPDATE realmojis SET deleted_at=clock_timestamp() WHERE id=$1")
        .bind(&input.real_moji_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    no_content(StatusCode::NO_CONTENT)
}
