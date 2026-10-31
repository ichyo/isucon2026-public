//! Configuration from the environment (docs/design.md §12).

use std::path::PathBuf;

use anyhow::{bail, Context};

use crate::model::{NodeId, Region};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

/// How a follower-originated write is confirmed to its client (docs/design.md §4.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckMode {
    /// the leader responds after every connected follower acked (2 RTT for a forwarded write)
    Leader,
    /// the origin follower completes as soon as it applied the records and every other node the leader waits
    /// for acked them directly to it (≈1.5 RTT); falls back to the leader's response
    Direct,
}

/// Actions that may use `WaitPolicy::LeaderOnly` (ISUREAL_ASYNC_ACTIONS): purely additive writes only.
pub const ASYNC_CAPABLE: [&str; 8] =
    ["create_post", "create_comment", "replace_real_moji", "create_friend_request", "follow", "register", "login", "certify_user"];

#[derive(Clone, Debug)]
pub struct Config {
    /// this server's fixed region (region pinning)
    pub region: Region,
    /// this node's identity (several nodes may share a region: asia1 + asia2)
    pub node: NodeId,
    /// the sequencer node; all nodes must agree
    pub leader_node: NodeId,
    pub role: Role,
    pub listen: String,
    pub repl_listen: String,
    pub leader_addr: String,
    pub control_listen: String,
    /// internal blob service (every node)
    pub blob_listen: String,
    /// blob service address of every node (index = NodeId::idx())
    pub blob_peers: [Option<String>; NodeId::COUNT],
    /// uploads larger than this are stored on the origin node only (not replicated in the log)
    pub blob_inline_max: usize,
    pub data_dir: PathBuf,
    pub initial_dir: PathBuf,
    pub public_dir: PathBuf,
    pub throttle_ms: u64,
    pub throttle_feed_ms: u64,
    pub fsync: bool,
    pub exp_quantum: i64,
    pub log_debug: bool,
    /// ISUREAL_ACK_MODE=leader|direct (default leader)
    pub ack_mode: AckMode,
    /// ISUREAL_ASYNC_ACTIONS: actions answered after the leader's own apply + WAL only (default none)
    pub async_actions: Vec<&'static str>,
}

fn parse_async_actions(s: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    for a in s.split(',').map(str::trim).filter(|a| !a.is_empty()) {
        match ASYNC_CAPABLE.iter().find(|&&c| c == a) {
            Some(&c) => out.push(c),
            None => eprintln!("[config] !!!!! ISUREAL_ASYNC_ACTIONS: {a:?} is not an additive action (allowed: {ASYNC_CAPABLE:?}); it stays synchronous !!!!!"),
        }
    }
    out
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

pub const DEFAULT_BLOB_PEERS: &str = "asia1=192.168.1.10:7001,asia2=192.168.4.10:7001,europe1=192.168.2.10:7001,america1=192.168.3.10:7001";
pub const DEFAULT_BLOB_INLINE_MAX: usize = 64 * 1024;

/// Replication address of a node (port 7000 on its private IP).
pub fn default_leader_addr(n: NodeId) -> String {
    format!("{}:7000", n.private_ip())
}

/// Node from the contest hostname (`ip-192-168-<k>-10`), if recognisable.
fn node_from_hostname() -> Option<NodeId> {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes a NUL-terminated name into the buffer.
    if unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) } != 0 {
        return None;
    }
    let end = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    let h = std::str::from_utf8(&buf[..end]).ok()?;
    match h.split('.').next()? {
        "ip-192-168-1-10" => Some(NodeId::ASIA1),
        "ip-192-168-2-10" => Some(NodeId::EUROPE1),
        "ip-192-168-3-10" => Some(NodeId::AMERICA1),
        "ip-192-168-4-10" => Some(NodeId::ASIA2),
        _ => None,
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Config> {
        let region_s = env("ISUREAL_REGION").context("ISUREAL_REGION is required")?;
        let Some(region) = Region::parse(&region_s) else { bail!("invalid ISUREAL_REGION {region_s:?}") };
        // node identity: ISUREAL_NODE, else the contest hostname, else the region's primary node
        let node = match env("ISUREAL_NODE") {
            Some(n) => NodeId::parse(&n).with_context(|| format!("invalid ISUREAL_NODE {n:?}"))?,
            None => node_from_hostname().filter(|n| n.region() == region).unwrap_or(NodeId::primary(region)),
        };
        if node.region() != region {
            bail!("ISUREAL_NODE {} belongs to region {}, but ISUREAL_REGION is {}", node.name(), node.region().as_str(), region.as_str());
        }
        // leader: ISUREAL_LEADER_NODE, else (legacy) ISUREAL_LEADER_REGION's primary node, else asia1
        let leader_node = match (env("ISUREAL_LEADER_NODE"), env("ISUREAL_LEADER_REGION")) {
            (Some(n), _) => NodeId::parse(&n).with_context(|| format!("invalid ISUREAL_LEADER_NODE {n:?}"))?,
            (None, Some(r)) => NodeId::primary(Region::parse(&r).with_context(|| format!("invalid ISUREAL_LEADER_REGION {r:?}"))?),
            (None, None) => NodeId::ASIA1,
        };
        let role = match env("ISUREAL_ROLE").as_deref() {
            Some("leader") => Role::Leader,
            Some("follower") => Role::Follower,
            Some(o) => bail!("invalid ISUREAL_ROLE {o:?}"),
            None if node == leader_node => Role::Leader,
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
            node,
            leader_node,
            role,
            listen: env("ISUREAL_LISTEN").unwrap_or_else(|| "0.0.0.0:80".into()),
            repl_listen: env("ISUREAL_REPL_LISTEN").unwrap_or_else(|| "0.0.0.0:7000".into()),
            leader_addr: env("ISUREAL_LEADER_ADDR").unwrap_or_else(|| default_leader_addr(leader_node)),
            control_listen: env("ISUREAL_CONTROL_LISTEN").unwrap_or_else(|| "127.0.0.1:9100".into()),
            blob_listen: env("ISUREAL_BLOB_LISTEN").unwrap_or_else(|| "0.0.0.0:7001".into()),
            blob_peers: crate::blobs::parse_peers(&env("ISUREAL_BLOB_PEERS").unwrap_or_else(|| DEFAULT_BLOB_PEERS.into()))?,
            blob_inline_max: num("ISUREAL_BLOB_INLINE_MAX", DEFAULT_BLOB_INLINE_MAX as u64)? as usize,
            data_dir: env("ISUREAL_DATA_DIR").unwrap_or_else(|| "/home/isucon/data".into()).into(),
            initial_dir: env("ISUREAL_INITIAL_DIR").unwrap_or_else(|| "/home/isucon/initial".into()).into(),
            public_dir: env("ISUREAL_PUBLIC_DIR").unwrap_or_else(|| "/home/isucon/webapp/public".into()).into(),
            throttle_ms: num("ISUREAL_THROTTLE_MS", 0)?,
            throttle_feed_ms: num("ISUREAL_THROTTLE_FEED_MS", 0)?,
            fsync: env("ISUREAL_FSYNC").map(|v| v != "0").unwrap_or(true),
            exp_quantum: num("ISUREAL_EXP_QUANTUM", 1)? as i64,
            log_debug: env("ISUREAL_LOG").as_deref() == Some("debug"),
            ack_mode: match env("ISUREAL_ACK_MODE").as_deref() {
                None | Some("leader") => AckMode::Leader,
                Some("direct") => AckMode::Direct,
                Some(o) => bail!("invalid ISUREAL_ACK_MODE {o:?} (leader|direct)"),
            },
            async_actions: parse_async_actions(&env("ISUREAL_ASYNC_ACTIONS").unwrap_or_default()),
        })
    }

    /// Config for in-process tests: everything on 127.0.0.1 ephemeral ports, fsync off, leader = asia1.
    pub fn for_tests(node: NodeId, role: Role, data: &std::path::Path, initial: &std::path::Path, leader_addr: &str) -> Config {
        Config {
            region: node.region(),
            node,
            leader_node: NodeId::ASIA1,
            role,
            listen: "127.0.0.1:0".into(),
            repl_listen: "127.0.0.1:0".into(),
            leader_addr: leader_addr.into(),
            control_listen: "127.0.0.1:0".into(),
            blob_listen: "127.0.0.1:0".into(),
            blob_peers: Default::default(),
            blob_inline_max: DEFAULT_BLOB_INLINE_MAX,
            data_dir: data.into(),
            initial_dir: initial.into(),
            public_dir: std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../webapp/public"),
            throttle_ms: 0,
            throttle_feed_ms: 0,
            fsync: false,
            exp_quantum: 1,
            log_debug: false,
            ack_mode: AckMode::Leader,
            async_actions: Vec::new(),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
}
