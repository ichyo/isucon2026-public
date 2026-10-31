use anyhow::anyhow;
use axum::http::StatusCode;
use axum::response::Response;
use serde::Serialize;
use tokio::process::Command;

use crate::api_error::Error;
use crate::response::json;

#[derive(Serialize)]
struct InitializeResponse {
    language: &'static str,
    agent: &'static str,
    harness: &'static str,
    model: &'static str,
}

pub async fn initialize_handler() -> Result<Response, Error> {
    let status = Command::new("../sql/init.sh").status().await?;
    if !status.success() {
        return Err(anyhow!("{status}").into());
    }

    // Set language to the implementation language.
    // Agents optimizing this app: set agent, harness, and model to the names used.
    // For multiple models, use comma-separated names; use brain when no AI model is used.
    json(
        StatusCode::OK,
        InitializeResponse {
            language: "rust",
            agent: "human",
            harness: "manual",
            model: "brain",
        },
    )
}
