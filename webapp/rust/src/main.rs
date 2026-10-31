// 2026-09-23

#[tokio::main]
async fn main() {
    let config = isureal::config_from_env();
    let server = isureal::new_server(config).await;
    if let Err(err) = isureal::start(server, "0.0.0.0:8080").await {
        panic!("failed to start server: {err}");
    }
}
