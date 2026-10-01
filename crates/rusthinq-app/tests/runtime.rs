use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Runtime},
};
use rusthinq_lifecycle::Action;
use rusthinq_protocol::thinq1;
use rusthinq_server::{Config, Server};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, watch},
    time::timeout,
};

async fn until(
    events: &mut broadcast::Receiver<Event>,
    predicate: impl Fn(&Event) -> bool,
) -> Event {
    timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.unwrap();
            if predicate(&event) {
                return event;
            }
        }
    })
    .await
    .unwrap()
}
async fn identify(server: &mut Server, did: &str) -> tokio::io::DuplexStream {
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    let payload =
        serde_json::json!({"Header":{"x-lgedm-deviceId":did},"Body":{"Cmd":"Mon"}}).to_string();
    peer.write_all(&thinq1::encode(payload.as_bytes(), 8192).unwrap())
        .await
        .unwrap();
    let size = timeout(Duration::from_secs(2), peer.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut ack = vec![0; size as usize];
    peer.read_exact(&mut ack).await.unwrap();
    peer
}

#[tokio::test]
async fn forget_closes_transport_commits_removal_and_allows_new_incarnation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let mut peer = identify(&mut server, "d").await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    let old = handle.snapshot()[0].entry.clone();
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| matches!(event, Event::Lifecycle(Action::Removed {id, incarnation}) if id == "d" && *incarnation == old.incarnation)).await;
    assert!(handle.snapshot().is_empty());
    assert!(server.handle().snapshot().is_empty());
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(1), peer.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert!(saved["entries"].as_array().unwrap().is_empty());
    let _replacement = identify(&mut server, "d").await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    assert!(handle.snapshot()[0].entry.incarnation > old.incarnation);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert!(
        Storage::open(&path, 8).unwrap().state().ledger.entries[0].incarnation > old.incarnation
    );
}

#[tokio::test]
async fn forget_failed_commit_never_emits_removed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let _peer = identify(&mut server, "d").await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::ForgetFailed {
                step: rusthinq_lifecycle::Step::PersistRemoval,
                ..
            })
        )
    })
    .await;
    assert_eq!(handle.snapshot().len(), 1);
    assert!(handle.snapshot()[0].session.is_none());
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(event, Event::Lifecycle(Action::Removed { .. })));
    }
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn real_session_events_persist_replace_and_recover_offline() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    let mut server = Server::new(Config {
        generation_floor: storage.state().generation_floor,
        ..Config::default()
    })
    .unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let _old = identify(&mut server, "d").await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    let first = handle.snapshot()[0].clone();
    assert!(first.online);
    let _new = identify(&mut server, "d").await;
    until(&mut events,|event| matches!(event,Event::Lifecycle(Action::Changed(device)) if device.session.is_some_and(|session| session.generation > first.entry.last_generation))).await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    assert_eq!(
        handle.snapshot()[0].entry.incarnation,
        first.entry.incarnation
    );
    let latest = handle.snapshot()[0].entry.last_generation;
    assert!(latest > first.entry.last_generation);
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let restored = Storage::open(&path, 8).unwrap();
    assert_eq!(restored.state().generation_floor, latest);
    let server = Server::new(Config {
        generation_floor: restored.state().generation_floor,
        ..Config::default()
    })
    .unwrap();
    let runtime = Runtime::new(restored, server.handle(), Duration::ZERO, 8).unwrap();
    assert!(!runtime.handle().snapshot()[0].online);
    assert!(runtime.handle().snapshot()[0].session.is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn lost_broadcast_events_reconcile_authoritative_sessions() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let mut server = Server::new(Config {
        event_capacity: 1,
        ..Config::default()
    })
    .unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let _a = identify(&mut server, "a").await;
    let _b = identify(&mut server, "b").await;
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    until(
        &mut events,
        |event| matches!(event,Event::Lost {transport_events} if *transport_events > 0),
    )
    .await;
    assert_eq!(handle.snapshot().len(), 2);
    assert!(handle.snapshot().iter().all(|device| device.online));
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn grace_deadline_runs_and_shutdown_joins_pending_storage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::from_millis(30), 128).unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let peer = identify(&mut server, "d").await;
    until(&mut events, |event| {
        matches!(event, Event::Lifecycle(Action::Online { .. }))
    })
    .await;
    drop(peer);
    until(&mut events, |event| {
        matches!(event, Event::Lifecycle(Action::Offline { .. }))
    })
    .await;
    assert!(!handle.snapshot()[0].online);
    assert!(handle.snapshot()[0].session.is_none());
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        Storage::open(&path, 8)
            .unwrap()
            .state()
            .ledger
            .entries
            .len(),
        1
    );
}

#[tokio::test]
async fn persistence_failure_is_reported_and_shutdown_releases_storage_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    std::fs::create_dir(&path).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let mut events = runtime.handle().subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let _peer = identify(&mut server, "d").await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Err(_), .. })
        )
    })
    .await;
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    std::fs::remove_dir(&path).unwrap();
    assert!(
        Storage::open(&path, 8)
            .unwrap()
            .state()
            .ledger
            .entries
            .is_empty()
    );
}
