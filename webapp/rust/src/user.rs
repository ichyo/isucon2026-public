use std::sync::LazyLock;

use regex::Regex;
use sqlx::postgres::PgRow;
use sqlx::{Executor, Postgres, Row};

use crate::api_error::Error;
use crate::image_signature::image_signing_key;

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct User {
    pub id: String,
    pub username: String,
    pub secret_key_hash: String,
    pub display_name: String,
    pub bio: String,
    pub profile_image_id: Option<String>,
    pub region: String,
    pub account_type: String,
    pub status: String,
    #[sqlx(skip)]
    pub image_key: Vec<u8>,
}

pub static USERNAME_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9_.-]{1,64}$").unwrap());

pub fn valid_user_text(value: &str, minimum: usize, maximum: usize) -> bool {
    let length = value.chars().count();
    length >= minimum && length <= maximum
}

pub fn valid_region(region: &str) -> bool {
    matches!(region, "america" | "europe" | "asia")
}

pub const USER_COLUMNS: [&str; 9] = [
    "id",
    "username",
    "secret_key_hash",
    "display_name",
    "bio",
    "profile_image_id",
    "region",
    "account_type",
    "status",
];

pub fn user_columns_as(alias: &str, prefix: &str) -> String {
    USER_COLUMNS
        .iter()
        .map(|column| format!(r#"{alias}.{column} AS "{prefix}.{column}""#))
        .collect::<Vec<_>>()
        .join(", ")
}

impl User {
    pub fn set_image_key(&mut self) -> Result<(), Error> {
        self.image_key = image_signing_key(&self.secret_key_hash)?;
        Ok(())
    }

    pub fn from_row_with_prefix(row: &PgRow, prefix: &str) -> sqlx::Result<User> {
        let column = |name: &str| format!("{prefix}.{name}");
        Ok(User {
            id: row.try_get(column("id").as_str())?,
            username: row.try_get(column("username").as_str())?,
            secret_key_hash: row.try_get(column("secret_key_hash").as_str())?,
            display_name: row.try_get(column("display_name").as_str())?,
            bio: row.try_get(column("bio").as_str())?,
            profile_image_id: row.try_get(column("profile_image_id").as_str())?,
            region: row.try_get(column("region").as_str())?,
            account_type: row.try_get(column("account_type").as_str())?,
            status: row.try_get(column("status").as_str())?,
            image_key: Vec::new(),
        })
    }
}

pub async fn get_user<'c>(
    db: impl Executor<'c, Database = Postgres>,
    user_id: &str,
) -> Result<User, Error> {
    let mut user: User = sqlx::query_as(&format!(
        "SELECT {} FROM users WHERE id = $1 AND status = 'active'",
        USER_COLUMNS.join(", ")
    ))
    .bind(user_id)
    .fetch_one(db)
    .await?;
    user.set_image_key()?;
    Ok(user)
}
