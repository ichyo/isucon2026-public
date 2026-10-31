use std::io::{Cursor, Write};
use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use crate::api_error::Error;
use crate::post::{POST_COLUMNS, Post};
use crate::post_api::{CreatePostResponse, new_post_response};
use crate::response::blob;
use crate::server::HandlerContext;
use crate::session::Session;
use crate::user::get_user;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportPostResponse {
    #[serde(flatten)]
    pub create_post_response: CreatePostResponse,
    pub image_path: String,
}

pub async fn export_posts_handler(
    State(h): State<Arc<HandlerContext>>,
    Extension(session): Extension<Session>,
) -> Result<Response, Error> {
    let user_id = &session.user_id;
    let author = get_user(&h.db, user_id).await?;
    struct ExportPost {
        post: Post,
        image_data: Vec<u8>,
    }
    impl FromRow<'_, PgRow> for ExportPost {
        fn from_row(row: &PgRow) -> sqlx::Result<ExportPost> {
            Ok(ExportPost {
                post: Post::from_row(row)?,
                image_data: row.try_get("image_data")?,
            })
        }
    }
    let posts: Vec<ExportPost> = sqlx::query_as(&format!(
        r#"
   SELECT {POST_COLUMNS}, i.data AS image_data
   FROM posts p
   JOIN posting_windows w ON w.id = p.posting_window_id
   JOIN image_objects i ON i.id = p.image_id
   WHERE p.author_id = $1 AND p.deleted_at IS NULL
   ORDER BY p.posted_at, p.id
  "#
    ))
    .bind(user_id)
    .fetch_all(&h.db)
    .await?;

    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let mut items = Vec::with_capacity(posts.len());
    for post in &posts {
        let image_path = format!("images/{:06}.jpg", items.len() + 1);
        writer.start_file(
            image_path.as_str(),
            SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
        )?;
        writer.write_all(&post.image_data)?;
        items.push(ExportPostResponse {
            create_post_response: new_post_response(&post.post, &author),
            image_path,
        });
    }
    writer.start_file("posts.json", SimpleFileOptions::default())?;
    let mut manifest = serde_json::to_vec(&items)?;
    manifest.push(b'\n');
    writer.write_all(&manifest)?;
    let body = writer.finish()?.into_inner();
    let mut response = blob(StatusCode::OK, "application/zip", body)?;
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static(r#"attachment; filename="isureal-posts.zip""#),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}
