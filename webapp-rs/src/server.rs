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
                n.app.images.net.set_peer(m.app.cfg.node, a.to_string());
            }
        }
    }
}

/// TcpListener with a larger accept backlog than std's default 128 (bursts of new connections).
/// `rcvbuf` > 0 fixes SO_RCVBUF of the listener (accepted connections inherit it; no autotuning): a request whose
/// body we do not read yet then holds only that much in flight (upload gate, ISUREAL_RCVBUF).
async fn bind_backlog(addr: &str, backlog: u32, rcvbuf: u32) -> std::io::Result<TcpListener> {
    let sa = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| std::io::Error::other(format!("cannot resolve {addr}")))?;
    let sock = if sa.is_ipv4() { tokio::net::TcpSocket::new_v4()? } else { tokio::net::TcpSocket::new_v6()? };
    if rcvbuf > 0 {
        sock.set_recv_buffer_size(rcvbuf)?;
    }
    sock.set_reuseaddr(true)?;
    sock.set_nodelay(true)?;
    sock.bind(sa)?;
    sock.listen(backlog)
}

/// Logs panics (with location) once per process; the default hook output is kept too.
fn install_panic_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let thread = std::thread::current();
            eprintln!("[panic] thread {:?}: {info}", thread.name().unwrap_or("?"));
            prev(info);
        }));
    });
}

pub async fn start(cfg: Config) -> anyhow::Result<Node> {
    install_panic_hook();
    let t0 = Instant::now();
    eprintln!(
        "[start] node={} region={} leader_node={} role={:?} listen={} repl_listen={} leader_addr={} data={} initial={} public={} fsync={} exp_quantum={}",
        cfg.node.name(),
        cfg.region.as_str(),
        cfg.leader_node.name(),
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
    eprintln!(
        "[start] replication: ack_mode={:?} async_actions={:?} gate_silence_ms={} (runtime: {} threads at nice {}); budgets: jpeg={} sign={} (0 = CPUs)",
        cfg.ack_mode, cfg.async_actions, cfg.gate_silence_ms, 2, crate::rt::REPL_NICE, cfg.jpeg_concurrency, cfg.sign_concurrency
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
    crate::rt::init_budgets(cfg.jpeg_concurrency, cfg.sign_concurrency);
    crate::http::req::init_body_budget(cfg.body_budget_mb);
    let repl = if cfg.is_leader() { Repl::Leader(repl::Leader::new()) } else { Repl::Follower(repl::Follower::new(cfg.gate_silence_ms)) };
    let app: &'static App = Box::leak(Box::new(App {
        throttle: Throttle::new(cfg.throttle_ms, cfg.throttle_feed_ms),
        signer: Signer::new(cfg.exp_quantum),
        sigbatch: crate::sigshare::VerifyBatcher::default(),
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

    app.sigbatch.start(&app.signer);
    crate::sigshare::spawn_sharing(app);

    // replay the local WAL
    {
        let node = app.cfg.node;
        let t = Instant::now();
        let mut st = app.state.write();
        let mut fx = Vec::new();
        let n = app
            .wal
            .replay(|d, _rec, off| {
                let loc = match d.blob {
                    BlobIn::Inline { off: o, len } => Some(ImageLoc::Wal { off: off + o as u64, len: len as u32 }),
                    BlobIn::Ref(r) => Some(r.loc_on(node)),
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
            // bound on the replication runtime so its driver (not the request runtime's) serves the socket
            let l = crate::rt::spawn(async move { TcpListener::bind(&app.cfg.repl_listen).await })
                .await?
                .with_context(|| format!("bind repl {}", app.cfg.repl_listen))?;
            repl_addr = Some(l.local_addr()?);
            crate::rt::spawn(repl::leader::serve(app, l));
            if app.cfg.ack_mode == crate::config::AckMode::Direct {
                crate::rt::spawn(repl::leader::run_synced_broadcast(app));
            }
        }
        Repl::Follower(_) => {
            crate::rt::spawn(repl::follower::run(app));
            if app.cfg.ack_mode == crate::config::AckMode::Direct {
                // direct acks go to every other follower (the leader gets its ACK over the link)
                let peers: Vec<_> = crate::model::NodeId::ALL.into_iter().filter(|&n| n != app.cfg.node && n != app.cfg.leader_node).collect();
                app.images.net.spawn_ack_broadcast(app.cfg.node, peers);
            }
        }
    }

    // internal blob service (every node serves its own blobs.dat)
    let mut blob_addr = None;
    match crate::rt::spawn(async move { TcpListener::bind(&app.cfg.blob_listen).await }).await? {
        Ok(l) => {
            blob_addr = Some(l.local_addr()?);
            crate::rt::spawn(crate::blobs::serve(&app.images.blobs, &app.images.net, &app.signer, l));
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

    let rcvbuf: u32 = std::env::var("ISUREAL_RCVBUF").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    crate::http::req::set_gate_slots(std::env::var("ISUREAL_UPLOAD_SLOTS").ok().and_then(|v| v.parse().ok()).unwrap_or(0));
    eprintln!("[start] upload gate: slots={} rcvbuf={rcvbuf} (change slots: curl -XPOST '{}/uploadgate?slots=N')", crate::http::req::gate_slots(), app.cfg.control_listen);
    let l = bind_backlog(&app.cfg.listen, 4096, rcvbuf).await.with_context(|| format!("bind http {}", app.cfg.listen))?;
    let http_addr = l.local_addr()?;
    tokio::spawn(crate::http::serve(app, l));
    eprintln!("[start] ready: http={http_addr} repl={repl_addr:?} blob={blob_addr:?} control={control_addr:?} in {:.1?}", t0.elapsed());
    Ok(Node { app, http_addr, repl_addr, control_addr, blob_addr })
}
