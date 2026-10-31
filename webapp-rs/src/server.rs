//! Node startup: load initial data -> pristine state -> replay WAL -> replication -> HTTP.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use parking_lot::RwLock;
use tokio::net::TcpListener;

use crate::app::App;
use crate::apply::apply;
use crate::bans::Bans;
use crate::blobs::{BlobCache, BlobNet, BlobStore};
use crate::config::Config;
use crate::entry::BlobIn;
use crate::http::router::Router;
use crate::images::ImageStore;
use crate::model::{now_us, ImageLoc};
use crate::repl::{self, Repl};
use crate::sign::Signer;
use crate::sse::SseHub;
use crate::static_files::StaticFiles;
use crate::throttle::Throttle;
use crate::wal::Wal;

pub struct Node {
    pub app: &'static App,
    pub http_addr: SocketAddr,
    pub repl_addr: Option<SocketAddr>,
    pub control_addr: Option<SocketAddr>,
    pub blob_addr: Option<SocketAddr>,
}

/// Points every node's blob client at the others' actual blob-service addresses (tests / ephemeral ports).
pub fn wire_blob_peers(nodes: &[&Node]) {
    for n in nodes {
        for m in nodes {
            if let Some(a) = m.blob_addr {
                n.app.images.net.set_peer(m.app.cfg.region, a.to_string());
            }
        }
    }
}

pub async fn start(cfg: Config) -> anyhow::Result<Node> {
    let t0 = Instant::now();
    eprintln!(
        "[start] region={} leader_region={} role={:?} listen={} repl_listen={} leader_addr={} data={} initial={} public={} fsync={} exp_quantum={}",
        cfg.region.as_str(),
        cfg.leader_region.as_str(),
        cfg.role,
        cfg.listen,
        cfg.repl_listen,
        cfg.leader_addr,
        cfg.data_dir.display(),
        cfg.initial_dir.display(),
        cfg.public_dir.display(),
        cfg.fsync,
        cfg.exp_quantum
    );
    eprintln!(
        "[start] blobs: listen={} peers={:?} inline_max={} bytes (larger uploads stay on their origin node)",
        cfg.blob_listen, cfg.blob_peers, cfg.blob_inline_max
    );
    eprintln!("[start] throttle: ms={} feed_ms={} (change: curl -XPOST '{}/throttle?ms=N&feed_ms=M')", cfg.throttle_ms, cfg.throttle_feed_ms, cfg.control_listen);

    let dir = cfg.initial_dir.clone();
    let pristine = tokio::task::spawn_blocking(move || crate::loader::load(&dir)).await?.context("loading initial data")?;
    let pristine = Arc::new(pristine);
    let live = repl::clone_pristine_arc(&pristine).await;

    let wal = Wal::open(&cfg.data_dir, cfg.fsync).context("opening WAL")?;
    let blobs = BlobStore::open(&cfg.data_dir, cfg.fsync).context("opening blob store")?;
    let cache = BlobCache::open(&cfg.data_dir).context("opening blob cache")?;
    let net = BlobNet::new(cfg.blob_peers.clone());
    let images = ImageStore::open(&cfg.initial_dir, wal.file(), blobs, cache, net).context("opening images.pack")?;
    let statics = StaticFiles::load(&cfg.public_dir);
    let repl = if cfg.is_leader() { Repl::Leader(repl::Leader::new()) } else { Repl::Follower(repl::Follower::new()) };
    let app: &'static App = Box::leak(Box::new(App {
        throttle: Throttle::new(cfg.throttle_ms, cfg.throttle_feed_ms),
        signer: Signer::new(cfg.exp_quantum),
        cfg,
        state: RwLock::new(live),
        pristine,
        wal,
        images,
        sse: SseHub::default(),
        bans: Bans::default(),
        metrics: crate::metrics::Metrics::new(&Router::new().action_names()),
        router: Router::new(),
        statics,
        repl,
    }));

    // replay the local WAL
    {
        let region = app.cfg.region;
        let t = Instant::now();
        let mut st = app.state.write();
        let mut fx = Vec::new();
        let n = app
            .wal
            .replay(|d, _rec, off| {
                let loc = match d.blob {
                    BlobIn::Inline { off: o, len } => Some(ImageLoc::Wal { off: off + o as u64, len: len as u32 }),
                    BlobIn::Ref(r) => Some(r.loc_on(region)),
                    _ => None,
                };
                apply(&mut st, &d.entry, loc, &mut fx);
            })
            .context("replaying WAL")?;
        app.handle_effects(&st, &fx);
        let w = app.wal.inner.lock();
        eprintln!("[start] replayed {n} records (epoch {:016x}, seq {}) in {:.1?}; {}", w.epoch, w.last_seq, t.elapsed(), st.summary());
    }

    // replication
    let mut repl_addr = None;
    match &app.repl {
        Repl::Leader(_) => {
            let l = TcpListener::bind(&app.cfg.repl_listen).await.with_context(|| format!("bind repl {}", app.cfg.repl_listen))?;
            repl_addr = Some(l.local_addr()?);
            tokio::spawn(repl::leader::serve(app, l));
        }
        Repl::Follower(_) => {
            tokio::spawn(repl::follower::run(app));
        }
    }

    // internal blob service (every node serves its own blobs.dat)
    let mut blob_addr = None;
    match TcpListener::bind(&app.cfg.blob_listen).await {
        Ok(l) => {
            blob_addr = Some(l.local_addr()?);
            tokio::spawn(crate::blobs::serve(&app.images.blobs, &app.images.net, l));
        }
        Err(e) => eprintln!("[start] WARN cannot bind blob service {}: {e} (other nodes cannot fetch this node's uploads!)", app.cfg.blob_listen),
    }

    // control server
    let mut control_addr = None;
    match TcpListener::bind(&app.cfg.control_listen).await {
        Ok(l) => {
            control_addr = Some(l.local_addr()?);
            tokio::spawn(crate::throttle::serve_control(app, l));
        }
        Err(e) => eprintln!("[start] WARN cannot bind control {}: {e}", app.cfg.control_listen),
    }

    // per-second metrics timeline (process CPU%)
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_secs(1));
        loop {
            iv.tick().await;
            app.metrics.tick();
        }
    });

    // housekeeping
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_secs(30));
        loop {
            iv.tick().await;
            let now = now_us();
            app.signer.purge(now / 1_000_000);
            app.bans.purge(now);
        }
    });

    let l = TcpListener::bind(&app.cfg.listen).await.with_context(|| format!("bind http {}", app.cfg.listen))?;
    let http_addr = l.local_addr()?;
    tokio::spawn(crate::http::serve(app, l));
    eprintln!("[start] ready: http={http_addr} repl={repl_addr:?} blob={blob_addr:?} control={control_addr:?} in {:.1?}", t0.elapsed());
    Ok(Node { app, http_addr, repl_addr, control_addr, blob_addr })
}
