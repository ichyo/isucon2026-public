//! Deterministic session durability races: the journal worker is paused while global state changes.

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use crate::command::{Command, Outcome};
use crate::config::{Config, Role};
use crate::model::{Id, NodeId, Region};
use crate::server::{self, Node};
use crate::sessions;

async fn node(name: &str) -> (Node, PathBuf) {
    let root = std::env::temp_dir().join(format!("isureal-session-race-{name}-{}", Id::new_v4()));
    gen_testdata::generate(&root.join("initial"), gen_testdata::Scale::Small).unwrap();
    let mut cfg = Config::for_tests(
        NodeId::ASIA1,
        Role::Leader,
        &root.join("data"),
        &root.join("initial"),
        "",
    );
    cfg.fsync = true;
    let node = server::start(cfg).await.unwrap();
    server::wire_blob_peers(&[&node]);
    (node, root)
}

fn login_command(root: &std::path::Path, name: &str) -> Command {
    let csv = std::fs::read_to_string(root.join("initial/secrets.csv")).unwrap();
    let secret = csv
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name},")))
        .unwrap()
        .split(',')
        .next()
        .unwrap();
    Command::Login {
        username: name.into(),
        secret_key: secret.into(),
    }
}

fn token(outcome: Outcome) -> Id {
    match outcome {
        Outcome::Login { token, .. } => token,
        _ => panic!("unexpected login outcome"),
    }
}

async fn paused(entered: mpsc::Receiver<()>) {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || entered.recv().unwrap()),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initialize_and_other_users_authenticate_while_session_sync_is_stalled() {
    let (node, root) = node("initialize").await;
    let app = node.app;
    let previous = token(app.write(login_command(&root, "alice")).await.unwrap());
    let bob = token(app.write(login_command(&root, "bob")).await.unwrap());
    let alice = sessions::authenticate(app, previous).await.unwrap().id;
    let epoch = app.read().epoch;
    let command = login_command(&root, "alice");
    let (entered, release) = app.session_store.pause_next_sync();
    let pending = tokio::spawn(async move { app.write(command).await });
    paused(entered).await;

    // Authentication for another user must not wait for the journal's write or sync mutex.
    tokio::time::timeout(Duration::from_secs(2), sessions::authenticate(app, bob))
        .await
        .unwrap()
        .unwrap();
    // Initialize needs the global state write lock; it must finish before session fsync is released.
    tokio::time::timeout(Duration::from_secs(2), app.write(Command::Initialize))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(app.read().epoch, epoch);
    assert!(!pending.is_finished(), "session reply preceded durability");

    release.send(()).unwrap();
    assert_eq!(pending.await.unwrap().unwrap_err().status, 503);
    let new_epoch = app.read().epoch;
    assert_eq!(app.session_store.get(new_epoch, alice, 0), None);
    let stale = app.session_store.get(epoch, alice, 0).unwrap().unwrap();
    assert_ne!(stale, previous);
    assert_eq!(
        sessions::authenticate(app, stale).await.unwrap_err().status,
        401
    );
    assert_eq!(
        sessions::authenticate(app, previous)
            .await
            .unwrap_err()
            .status,
        401
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_during_session_sync_rejects_the_persisted_login() {
    let (node, root) = node("deletion").await;
    let app = node.app;
    let previous = token(app.write(login_command(&root, "alice")).await.unwrap());
    let alice = sessions::authenticate(app, previous).await.unwrap().id;
    let epoch = app.read().epoch;
    let command = login_command(&root, "alice");
    let (entered, release) = app.session_store.pause_next_sync();
    let pending = tokio::spawn(async move { app.write(command).await });
    paused(entered).await;

    tokio::time::timeout(
        Duration::from_secs(2),
        app.write(Command::DeleteAccount { user: alice }),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!pending.is_finished(), "session reply preceded durability");
    release.send(()).unwrap();
    assert_eq!(pending.await.unwrap().unwrap_err().status, 401);
    let stale = app.session_store.get(epoch, alice, 0).unwrap().unwrap();
    assert_ne!(stale, previous);
    assert_eq!(
        sessions::authenticate(app, stale).await.unwrap_err().status,
        401
    );
    assert_eq!(
        sessions::authenticate(app, previous)
            .await
            .unwrap_err()
            .status,
        401
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_away_and_back_during_sync_cannot_publish_an_old_generation() {
    let (node, root) = node("generation").await;
    let app = node.app;
    let previous = token(app.write(login_command(&root, "alice")).await.unwrap());
    let alice = sessions::authenticate(app, previous).await.unwrap().id;
    let epoch = app.read().epoch;
    let command = login_command(&root, "alice");
    let (entered, release) = app.session_store.pause_next_sync();
    let pending = tokio::spawn(async move { app.write(command).await });
    paused(entered).await;

    // Apply fenced handoffs on the replicated path to model ownership metadata arriving while an
    // older local journal operation is outstanding. Returning to Asia must not pass the old fence.
    for (generation, from, region) in [
        (0, Region::Asia, Region::Europe),
        (1, Region::Europe, Region::Asia),
    ] {
        tokio::time::timeout(
            Duration::from_secs(2),
            app.write(Command::MoveSession {
                epoch,
                user: alice,
                from,
                generation,
                token: Some(previous),
                region,
                display_name: None,
                bio: None,
            }),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert!(!pending.is_finished(), "session reply preceded durability");
    release.send(()).unwrap();
    assert_eq!(pending.await.unwrap().unwrap_err().status, 503);
    let stale = app.session_store.get(epoch, alice, 0).unwrap().unwrap();
    assert_ne!(stale, previous);
    assert_eq!(app.session_store.get(epoch, alice, 2), None);
    assert_eq!(
        sessions::authenticate(app, stale).await.unwrap_err().status,
        401
    );
    assert_eq!(
        sessions::authenticate(app, previous).await.unwrap().id,
        alice
    );
}
