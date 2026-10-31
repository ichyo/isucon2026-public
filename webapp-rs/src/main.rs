use isureal::config::Config;

// mimalloc keeps freed memory for reuse: large upload bodies (0.5-32 MB) no longer mmap/zero fresh pages each time.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[fatal] configuration error: {e:#}");
            std::process::exit(2);
        }
    };
    isureal::accesslog::init();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    rt.block_on(async move {
        if let Err(e) = isureal::server::start(cfg).await {
            eprintln!("[fatal] startup failed: {e:#}");
            std::process::exit(1);
        }
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("sigterm");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        eprintln!("[main] shutting down");
    });
}
