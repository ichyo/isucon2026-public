use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use crate::api_error::{Error, new_api_error};
use crate::bind::{Bind, Bindable, Binder};
use crate::post::{get_blocked_user_ids, get_friend_ids};
use crate::relationship::is_blocked;
use crate::relationship_list_api::RelationshipUserResponse;
use crate::response::json;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::user::{USER_COLUMNS, User, get_user, valid_user_text};
use crate::user_api::new_get_me_response;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct SearchUsersRequest {
    #[serde(skip)]
    pub q: String,
    #[serde(skip)]
    pub before_username: String,
    #[serde(skip)]
    pub before_id: String,
}

impl Bindable for SearchUsersRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.query("q", &mut self.q)?;
        b.query("beforeUsername", &mut self.before_username)?;
        b.query("beforeId", &mut self.before_id)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchUsersResponse {
    pub items: Vec<RelationshipUserResponse>,
    pub has_more: bool,
}

fn normalize_search_text(value: &str) -> String {
    caseless::default_case_fold_str(&value.nfc().collect::<String>())
        .nfc()
        .collect()
}

pub async fn search_users_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<SearchUsersRequest>,
) -> Result<Response, Error> {
    if !valid_user_text(&input.q, 1, 100) {
        return Err(new_api_error(
            StatusCode::BAD_REQUEST,
            "invalid_relationship",
        ));
    }
    if input.before_username.is_empty() != input.before_id.is_empty() {
        return Err(new_api_error(StatusCode::BAD_REQUEST, "invalid_pagination"));
    }
    let viewer = &session.user_id;
    let users: Vec<User> = sqlx::query_as(&format!("SELECT {} FROM users WHERE status='active' AND id<>$1 AND ($2='' OR (username,id)>($2,$3)) ORDER BY username, id", USER_COLUMNS.join(", ")))
        .bind(viewer)
        .bind(&input.before_username)
        .bind(&input.before_id)
        .fetch_all(&h.db)
        .await?;
    let blocked = get_blocked_user_ids(&h.db, viewer).await?;
    let query = normalize_search_text(&input.q);
    let mut items = Vec::new();
    for mut user in users {
        if !normalize_search_text(&user.username).contains(&query)
            && !normalize_search_text(&user.display_name).contains(&query)
        {
            continue;
        }
        if blocked.contains(&user.id) {
            continue;
        }
        user.set_image_key()?;
        if user.account_type != "certified_public" {
            user.bio = String::new();
        }
        items.push(new_relationship_user_response(&user));
        if items.len() == 51 {
            break;
        }
    }
    let has_more = items.len() > 50;
    if has_more {
        items.truncate(50);
    }
    json(StatusCode::OK, SearchUsersResponse { items, has_more })
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GetUserRequest {
    #[serde(skip)]
    pub user_id: String,
}

impl Bindable for GetUserRequest {
    fn bind(&mut self, b: &Binder) -> Result<(), String> {
        b.param("userId", &mut self.user_id)
    }
}

pub type GetUserResponse = RelationshipUserResponse;

pub async fn get_user_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
    Bind(input): Bind<GetUserRequest>,
) -> Result<Response, Error> {
    let viewer = &session.user_id;
    let mut user = match get_user(&h.db, &input.user_id).await {
        Err(err) if err.is_no_rows() => {
            return Err(new_api_error(
                StatusCode::NOT_FOUND,
                "relationship_not_found",
            ));
        }
        result => result?,
    };
    let blocked = is_blocked(&h.db, viewer, &user.id).await?;
    if blocked {
        return Err(new_api_error(
            StatusCode::NOT_FOUND,
            "relationship_not_found",
        ));
    }
    let friends = get_friend_ids(&h.db, viewer).await?;
    if user.account_type != "certified_public" && !friends.contains(&user.id) {
        user.bio = String::new();
    }
    let response: GetUserResponse = new_relationship_user_response(&user);
    json(StatusCode::OK, response)
}

pub fn new_relationship_user_response(user: &User) -> RelationshipUserResponse {
    RelationshipUserResponse {
        id: user.id.clone(),
        username: user.username.clone(),
        display_name: user.display_name.clone(),
        profile_image_url: new_get_me_response(user).profile_image_url,
        bio: user.bio.clone(),
        account_type: user.account_type.clone(),
    }
}
