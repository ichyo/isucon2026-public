use chrono::{DateTime, Utc};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Executor, FromRow, Postgres};

use crate::api_error::Error;
use crate::user::{User, user_columns_as};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RealMoji {
    pub id: String,
    pub post_id: String,
    pub reactor_id: String,
    pub image_id: String,
    pub reaction_type: String,
    pub created_at: DateTime<Utc>,
    pub replaced_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
}

pub async fn get_real_moji<'c>(
    db: impl Executor<'c, Database = Postgres>,
    id: &str,
) -> Result<RealMoji, Error> {
    let reaction = sqlx::query_as("SELECT id, post_id, reactor_id, image_id, reaction_type, created_at, replaced_at, deleted_at FROM realmojis WHERE id=$1")
        .bind(id)
        .fetch_one(db)
        .await?;
    Ok(reaction)
}

#[derive(Debug, Clone)]
pub struct RealMojiWithReactor {
    pub real_moji: RealMoji,
    pub reactor: User,
}

impl FromRow<'_, PgRow> for RealMojiWithReactor {
    fn from_row(row: &PgRow) -> sqlx::Result<RealMojiWithReactor> {
        Ok(RealMojiWithReactor {
            real_moji: RealMoji::from_row(row)?,
            reactor: User::from_row_with_prefix(row, "reactor")?,
        })
    }
}

pub async fn select_current_real_mojis<'c>(
    db: impl Executor<'c, Database = Postgres>,
    conditions: &str,
    args: PgArguments,
) -> Result<Vec<RealMojiWithReactor>, Error> {
    let mut reactions: Vec<RealMojiWithReactor> = sqlx::query_as_with(
        &format!(
            r#"
  SELECT r.id, r.post_id, r.reactor_id, r.image_id, r.reaction_type, r.created_at, r.replaced_at, r.deleted_at,
         {}
  FROM realmojis r JOIN users u ON u.id = r.reactor_id AND u.status = 'active'
  WHERE r.replaced_at IS NULL AND r.deleted_at IS NULL {conditions}"#,
            user_columns_as("u", "reactor")
        ),
        args,
    )
    .fetch_all(db)
    .await?;
    for reaction in &mut reactions {
        reaction.reactor.set_image_key()?;
    }
    Ok(reactions)
}
