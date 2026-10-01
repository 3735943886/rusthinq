use rusthinq_app::{
    cleanup_mqtt::Session,
    lifecycle_cleanup::device_owner,
    lifecycle_storage::Storage,
    retained_cleanup::Ledger,
    runtime::{CleanupStatus, Event, Runtime},
};
use rusthinq_lifecycle::{Action, Entry, Ledger as Devices};
use rusthinq_server::{Config, Server, retained::Tombstone};
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, watch},
    time::timeout,
};

async fn packet(peer: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let first = peer.read_u8().await.unwrap();
    let length = peer.read_u8().await.unwrap();
    assert!(length < 128);
    let mut bytes = vec![first, length];
    bytes.resize(2 + length as usize, 0);
    peer.read_exact(&mut bytes[2..]).await.unwrap();
    bytes
}
async fn session(
    confirm: bool,
) -> (
    Session<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<()>,
) {
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        assert_eq!(packet(&mut peer).await[0], 0x10);
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        let publish = packet(&mut peer).await;
        assert_eq!(publish[0], 0x33);
        let length = u16::from_be_bytes([publish[2], publish[3]]) as usize;
        assert_eq!(&publish[4..4 + length], b"old/topic");
        assert_eq!(publish.len(), 6 + length); // empty retained payload
        if confirm {
            peer.write_all(&[0x40, 2, publish[4 + length], publish[5 + length]])
                .await
                .unwrap();
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        }
    });
    (
        Session::connect(stream, "cleanup", Duration::from_secs(1))
            .await
            .unwrap(),
        remote,
    )
}
fn storage(path: &Path, known: bool) -> Storage {
    let mut storage = Storage::open(path, 8).unwrap();
    if known {
        storage
            .save(&Devices {
                revision: 1,
                next_incarnation: 2,
                entries: vec![Entry {
                    id: "d".into(),
                    incarnation: 1,
                    last_generation: 1,
                }],
            })
            .unwrap();
    }
    storage
}
fn inventory(path: &Path) -> Ledger {
    let mut ledger = Ledger::open(path, 8).unwrap();
    ledger
        .enqueue(&[
            Tombstone {
                owner: device_owner("d", 1).unwrap(),
                topic: "old/topic".into(),
            },
            Tombstone {
                owner: "adapter/ha".into(),
                topic: "adapter/topic".into(),
            },
            Tombstone {
                owner: "device/01:d/1".into(),
                topic: "noncanonical/topic".into(),
            },
        ])
        .unwrap();
    ledger
}
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
async fn runtime_removal_cleans_owner_and_joins_cleanup_on_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let devices = directory.path().join("devices.json");
    let retained = directory.path().join("retained.json");
    let storage = storage(&devices, true);
    let server = Server::new(Config {
        generation_floor: 1,
        ..Config::default()
    })
    .unwrap();
    let (session, remote) = session(true).await;
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128)
        .unwrap()
        .with_retained_cleanup(session, inventory(&retained));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    // Known offline devices retain their topics until durable removal.
    assert!(!handle.snapshot()[0].online);
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| {
        matches!(event, Event::Lifecycle(Action::Removed { .. }))
    })
    .await;
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    remote.await.unwrap();
    let pending = Ledger::open(&retained, 8).unwrap().pending();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|item| item.topic != "old/topic"));
    assert!(
        Storage::open(&devices, 8)
            .unwrap()
            .state()
            .ledger
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn failed_remote_cleanup_survives_restart_and_startup_recovers_orphans() {
    let directory = tempfile::tempdir().unwrap();
    let devices = directory.path().join("devices.json");
    let retained = directory.path().join("retained.json");
    let mut initial = Some(inventory(&retained));
    for confirm in [false, true] {
        let ledger = if confirm {
            Ledger::open(&retained, 8).unwrap()
        } else {
            initial.take().unwrap()
        };
        let server = Server::new(Config::default()).unwrap();
        let (session, remote) = session(confirm).await;
        let runtime = Runtime::new(
            storage(&devices, false),
            server.handle(),
            Duration::ZERO,
            128,
        )
        .unwrap()
        .with_retained_cleanup(session, ledger);
        let mut events = runtime.handle().subscribe();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(runtime.run(stopped));
        if !confirm {
            until(&mut events, |event| {
                matches!(event, Event::CleanupFailed { .. })
            })
            .await;
        }
        server.shutdown().await;
        stop.send_replace(true);
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        remote.await.unwrap();
        assert_eq!(
            Ledger::open(&retained, 8).unwrap().pending().len(),
            if confirm { 2 } else { 3 }
        );
    }
}

#[tokio::test]
async fn failed_worker_recovers_in_same_runtime_and_health_survives_late_subscription() {
    let directory = tempfile::tempdir().unwrap();
    let devices = directory.path().join("devices.json");
    let retained = directory.path().join("retained.json");
    let server = Server::new(Config::default()).unwrap();
    let (first, remote) = session(false).await;
    let runtime = Runtime::new(
        storage(&devices, false),
        server.handle(),
        Duration::ZERO,
        128,
    )
    .unwrap()
    .with_retained_cleanup(first, inventory(&retained));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    until(&mut events, |event| {
        matches!(event, Event::CleanupFailed { .. })
    })
    .await;
    remote.await.unwrap();
    assert!(matches!(
        *handle.cleanup_status().borrow(),
        CleanupStatus::Failed(_)
    ));
    let ledger = Ledger::open(&retained, 8).unwrap();
    assert_eq!(ledger.pending().len(), 3);
    let (second, remote) = session(true).await;
    handle
        .attach_retained_cleanup(second, ledger)
        .await
        .unwrap();
    assert_eq!(*handle.cleanup_status().borrow(), CleanupStatus::Running);
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    remote.await.unwrap();
    assert_eq!(Ledger::open(&retained, 8).unwrap().pending().len(), 2);
    assert_eq!(*handle.cleanup_status().borrow(), CleanupStatus::Disabled);
}

async fn idle_session() -> (
    Session<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<()>,
) {
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        assert_eq!(packet(&mut peer).await[0], 0x10);
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        // Rejected attachments and known offline owners send no retained deletes.
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    });
    (
        Session::connect(stream, "idle", Duration::from_secs(1))
            .await
            .unwrap(),
        remote,
    )
}

#[tokio::test]
async fn active_worker_and_stopped_runtime_reject_attachments_and_release_resources() {
    let directory = tempfile::tempdir().unwrap();
    let devices = directory.path().join("devices.json");
    let retained = directory.path().join("retained.json");
    let extra = directory.path().join("extra.json");
    let server = Server::new(Config {
        generation_floor: 1,
        ..Config::default()
    })
    .unwrap();
    let (first, remote) = idle_session().await;
    let runtime = Runtime::new(
        storage(&devices, true),
        server.handle(),
        Duration::ZERO,
        128,
    )
    .unwrap()
    .with_retained_cleanup(first, inventory(&retained));
    let handle = runtime.handle();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let (second, rejected) = idle_session().await;
    let error = handle
        .attach_retained_cleanup(second, Ledger::open(&extra, 8).unwrap())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    timeout(Duration::from_secs(3), rejected)
        .await
        .unwrap()
        .unwrap();
    drop(Ledger::open(&extra, 8).unwrap());
    assert_eq!(*handle.cleanup_status().borrow(), CleanupStatus::Running);
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    remote.await.unwrap();
    assert_eq!(Ledger::open(&retained, 8).unwrap().pending().len(), 3);
    let (third, rejected) = idle_session().await;
    assert_eq!(
        handle
            .attach_retained_cleanup(third, Ledger::open(&extra, 8).unwrap())
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotConnected
    );
    timeout(Duration::from_secs(3), rejected)
        .await
        .unwrap()
        .unwrap();
    drop(Ledger::open(&extra, 8).unwrap());
}
