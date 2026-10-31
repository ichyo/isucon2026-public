//! Configuration from the environment (docs/design.md §12).

use std::path::PathBuf;

use anyhow::{bail, Context};

use crate::model::Region;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub region: Region,
    pub leader_region: Region,
    pub role: Role,
    pub listen: String,
    pub repl_listen: String,
    pub leader_addr: String,
    pub control_listen: String,
    pub data_dir: PathBuf,
    pub initial_dir: PathBuf,
    pub public_dir: PathBuf,
    pub throttle_ms: u64,
    pub throttle_feed_ms: u64,
    pub fsync: bool,
    pub exp_quantum: i64,
    pub log_debug: bool,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

pub fn default_leader_addr(r: Region) -> &'static str {
    match r {
        Region::Asia => "192.168.1.10:7000",
        Region::Europe => "192.168.2.10:7000",
        Region::America => "192.168.3.10:7000",
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Config> {
        let region_s = env("ISUREAL_REGION").context("ISUREAL_REGION is required")?;
        let Some(region) = Region::parse(&region_s) else { bail!("invalid ISUREAL_REGION {region_s:?}") };
        let lr_s = env("ISUREAL_LEADER_REGION").unwrap_or_else(|| "asia".into());
        let Some(leader_region) = Region::parse(&lr_s) else { bail!("invalid ISUREAL_LEADER_REGION {lr_s:?}") };
        let role = match env("ISUREAL_ROLE").as_deref() {
            Some("leader") => Role::Leader,
            Some("follower") => Role::Follower,
            Some(o) => bail!("invalid ISUREAL_ROLE {o:?}"),
            None if region == leader_region => Role::Leader,
            None => Role::Follower,
        };
        let num = |name: &str, d: u64| -> anyhow::Result<u64> {
            match env(name) {
                Some(v) => v.parse().with_context(|| format!("invalid {name}")),
                None => Ok(d),
            }
        };
        Ok(Config {
            region,
            leader_region,
            role,
            listen: env("ISUREAL_LISTEN").unwrap_or_else(|| "0.0.0.0:80".into()),
            repl_listen: env("ISUREAL_REPL_LISTEN").unwrap_or_else(|| "0.0.0.0:7000".into()),
            leader_addr: env("ISUREAL_LEADER_ADDR").unwrap_or_else(|| default_leader_addr(leader_region).into()),
            control_listen: env("ISUREAL_CONTROL_LISTEN").unwrap_or_else(|| "127.0.0.1:9100".into()),
            data_dir: env("ISUREAL_DATA_DIR").unwrap_or_else(|| "/home/isucon/data".into()).into(),
            initial_dir: env("ISUREAL_INITIAL_DIR").unwrap_or_else(|| "/home/isucon/initial".into()).into(),
            public_dir: env("ISUREAL_PUBLIC_DIR").unwrap_or_else(|| "/home/isucon/webapp/public".into()).into(),
            throttle_ms: num("ISUREAL_THROTTLE_MS", 0)?,
            throttle_feed_ms: num("ISUREAL_THROTTLE_FEED_MS", 0)?,
            fsync: env("ISUREAL_FSYNC").map(|v| v != "0").unwrap_or(true),
            exp_quantum: num("ISUREAL_EXP_QUANTUM", 1)? as i64,
            log_debug: env("ISUREAL_LOG").as_deref() == Some("debug"),
        })
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
}
