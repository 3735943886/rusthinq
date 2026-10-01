use rusthinq_app::lifecycle_storage::Storage;
use rusthinq_app::runtime::{Event, Runtime};
use rusthinq_server::{Config, Reject, Server};
use std::time::Duration;
use tokio::{
    sync::{broadcast, watch},
    time::timeout,
};

async fn until(events: &mut broadcast::Receiver<Event>, predicate: impl Fn(&Event) -> bool) {
    timeout(Duration::from_secs(3), async {
        loop {
            if predicate(&events.recv().await.unwrap()) {
                return;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn runtime_refills_committed_budget_without_closing_existing_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let block = storage.reserve_generations(2).unwrap();
    let mut server = Server::new(Config {
        generation_floor: block.floor,
        generation_ceiling: block.ceiling,
        ..Config::default()
    })
    .unwrap();
    let server_handle = server.handle();
    assert_eq!(
        server_handle.extend_generations(1, 4),
        Err(Reject::InvalidConfig)
    );
    let runtime = Runtime::new(storage, server_handle.clone(), Duration::ZERO, 128)
        .unwrap()
        .with_generation_refill(2, 1)
        .unwrap();
    let mut events = runtime.handle().subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    assert_eq!(server.admit(stream).unwrap(), 1);
    let payload = br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#;
    peer.write_all(&rusthinq_protocol::thinq1::encode(payload, 8192).unwrap())
        .await
        .unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut ack = vec![0; length as usize];
    peer.read_exact(&mut ack).await.unwrap();
    until(&mut events, |event| {
        matches!(event, Event::GenerationExtended { ceiling: 4 })
    })
    .await;
    let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved["generation_floor"], 4);
    assert_eq!(server_handle.generation_budget(), (1, 4));
    assert_eq!(server_handle.snapshot().len(), 1);
    let (stream, _peer) = tokio::io::duplex(64);
    assert_eq!(server.admit(stream).unwrap(), 2);
    let (stream, _peer) = tokio::io::duplex(64);
    assert_eq!(server.admit(stream).unwrap(), 3);
    until(&mut events, |event| {
        matches!(event, Event::GenerationExtended { ceiling: 6 })
    })
    .await;
    assert_eq!(server_handle.snapshot().len(), 1);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert_eq!(server_handle.extend_generations(6, 8), Err(Reject::Stopped));
    assert_eq!(Storage::open(&path, 8).unwrap().state().generation_floor, 6);
}

#[tokio::test]
async fn refill_commit_failure_keeps_transport_ceiling_and_disables_retry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let block = storage.reserve_generations(2).unwrap();
    let mut server = Server::new(Config {
        generation_floor: block.floor,
        generation_ceiling: block.ceiling,
        ..Config::default()
    })
    .unwrap();
    let server_handle = server.handle();
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let runtime = Runtime::new(storage, server_handle.clone(), Duration::ZERO, 128)
        .unwrap()
        .with_generation_refill(2, 1)
        .unwrap();
    let mut events = runtime.handle().subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let (stream, _peer) = tokio::io::duplex(64);
    server.admit(stream).unwrap();
    until(&mut events, |event| {
        matches!(event, Event::GenerationRefillFailed { .. })
    })
    .await;
    assert_eq!(server_handle.generation_budget(), (1, 2));
    let (stream, _peer) = tokio::io::duplex(64);
    assert_eq!(server.admit(stream).unwrap(), 2);
    let (stream, _peer) = tokio::io::duplex(64);
    assert_eq!(server.admit(stream), Err(Reject::GenerationExhausted));
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(event, Event::GenerationExtended { .. }));
    }
}

#[tokio::test]
async fn restart_skips_entire_reservation_without_session_ledger_save() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let first = storage.reserve_generations(2).unwrap();
    assert_eq!((first.floor, first.ceiling), (0, 2));
    let mut server = Server::new(Config {
        generation_floor: first.floor,
        generation_ceiling: first.ceiling,
        ..Config::default()
    })
    .unwrap();
    let (stream, _peer) = tokio::io::duplex(64);
    assert_eq!(server.admit(stream).unwrap(), 1);
    server.shutdown().await;
    drop(storage);
    let mut storage = Storage::open(&path, 8).unwrap();
    assert!(storage.state().ledger.entries.is_empty());
    let second = storage.reserve_generations(2).unwrap();
    assert_eq!((second.floor, second.ceiling), (2, 4));
    let mut server = Server::new(Config {
        generation_floor: second.floor,
        generation_ceiling: second.ceiling,
        ..Config::default()
    })
    .unwrap();
    for expected in [3, 4] {
        let (stream, _peer) = tokio::io::duplex(64);
        assert_eq!(server.admit(stream).unwrap(), expected);
    }
    let (stream, _peer) = tokio::io::duplex(64);
    assert_eq!(server.admit(stream), Err(Reject::GenerationExhausted));
    server.shutdown().await;
}

#[test]
fn reservation_preserves_ledger_and_rejects_zero_overflow_and_invalid_config() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let ledger = storage.state().ledger.clone();
    assert!(storage.reserve_generations(0).is_err());
    assert!(!path.exists());
    storage.reserve_generations(u64::MAX).unwrap();
    assert!(storage.reserve_generations(1).is_err());
    assert!(!storage.requires_reopen());
    storage.save(&ledger).unwrap();
    drop(storage);
    let storage = Storage::open(&path, 8).unwrap();
    assert_eq!(storage.state().ledger, ledger);
    assert_eq!(storage.state().generation_floor, u64::MAX);
    assert!(matches!(
        Server::new(Config {
            generation_floor: 2,
            generation_ceiling: 1,
            ..Config::default()
        }),
        Err(Reject::InvalidConfig)
    ));
}

#[test]
fn failed_commit_returns_no_reservation_and_requires_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(storage.reserve_generations(2).is_err());
    assert_eq!(storage.state().generation_floor, 0);
    assert!(storage.requires_reopen());
    assert!(storage.reserve_generations(2).is_err());
}
