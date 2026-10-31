use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::api_error::{Error, new_api_error};
use crate::server::HandlerContext;

const IP_BAN_RELOAD_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Default)]
pub struct IpBanList {
    reloading: tokio::sync::Mutex<()>,
    snapshot: RwLock<Option<Arc<IpBanSnapshot>>>,
}

struct IpBanSnapshot {
    loaded_at: Instant,
    bans: HashMap<String, DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct IpBan {
    ip: String,
    banned_until: DateTime<Utc>,
}

impl IpBanList {
    pub async fn banned(&self, db: &PgPool, ip: &str) -> Result<bool, Error> {
        let mut snapshot = self.snapshot.read().unwrap().clone();
        if snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.loaded_at.elapsed() >= IP_BAN_RELOAD_INTERVAL)
            && let Ok(_reloading) = self.reloading.try_lock()
        {
            let loaded = Arc::new(load_ip_bans(db).await?);
            *self.snapshot.write().unwrap() = Some(loaded.clone());
            snapshot = Some(loaded);
        }
        let Some(snapshot) = snapshot else {
            return Ok(false);
        };
        Ok(snapshot
            .bans
            .get(ip)
            .is_some_and(|until| Utc::now() < *until))
    }
}

async fn load_ip_bans(db: &PgPool) -> Result<IpBanSnapshot, Error> {
    let rows: Vec<IpBan> = sqlx::query_as(
        "SELECT ip, banned_until FROM ip_bans WHERE banned_until > clock_timestamp()",
    )
    .fetch_all(db)
    .await?;
    let bans = rows
        .into_iter()
        .map(|row| (row.ip, row.banned_until))
        .collect();
    Ok(IpBanSnapshot {
        loaded_at: Instant::now(),
        bans,
    })
}

pub fn extract_client_ip(req: &Request) -> String {
    if let Some(ip) = req.headers().get("X-Client-IP")
        && !ip.is_empty()
    {
        return String::from_utf8_lossy(ip.as_bytes()).into_owned();
    }
    match req.extensions().get::<ConnectInfo<SocketAddr>>() {
        Some(ConnectInfo(addr)) => addr.ip().to_string(),
        None => String::new(),
    }
}

pub async fn ip_ban_middleware(
    State(h): State<Arc<HandlerContext>>,
    req: Request,
    next: Next,
) -> Result<Response, Error> {
    let banned = h.ip_bans.banned(&h.db, &extract_client_ip(&req)).await?;
    if banned {
        return Err(new_api_error(StatusCode::FORBIDDEN, "ip_banned"));
    }
    Ok(next.run(req).await)
}
