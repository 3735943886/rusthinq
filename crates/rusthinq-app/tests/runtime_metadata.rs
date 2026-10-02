use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Runtime},
};
use rusthinq_server::{Config, Server, thinq1_http::Metadata};
use std::time::Duration;
use tokio::{
    sync::{broadcast, mpsc, watch},
    time::timeout,
};

fn metadata(id: &str, model: &str) -> Metadata {
    Metadata {
        device_id: id.into(),
        model_name: model.into(),
        device_type: "purifier".into(),
    }
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
async fn metadata_inventory_updates_at_capacity_and_rejects_new_ids_without_online_devices() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 1).unwrap();
    let server = Server::new(Config::default()).unwrap();
    let (sender, receiver) = mpsc::channel(4);
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 16)
        .unwrap()
        .with_metadata(receiver);
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    sender.send(metadata("d", "first")).await.unwrap();
    until(&mut events, |event| matches!(event, Event::Metadata(_))).await;
    sender.send(metadata("d", "updated")).await.unwrap();
    until(
        &mut events,
        |event| matches!(event,Event::Metadata(data) if data.model_name=="updated"),
    )
    .await;
    sender.send(metadata("other", "rejected")).await.unwrap();
    until(
        &mut events,
        |event| matches!(event,Event::Rejected{device,..} if device=="other"),
    )
    .await;
    assert_eq!(handle.metadata_snapshot(), vec![metadata("d", "updated")]);
    assert!(handle.snapshot().is_empty());
    drop(sender);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn stopped_runtime_drains_accepted_metadata_before_releasing_storage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 1).unwrap();
    let server = Server::new(Config::default()).unwrap();
    let (sender, receiver) = mpsc::channel(4);
    sender.send(metadata("d", "first")).await.unwrap();
    sender.send(metadata("d", "latest")).await.unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 16)
        .unwrap()
        .with_metadata(receiver);
    let handle = runtime.handle();
    let (_stop, stopped) = watch::channel(true);
    server.shutdown().await;
    runtime.run(stopped).await.unwrap();
    assert_eq!(handle.metadata_snapshot(), vec![metadata("d", "latest")]);
    assert!(Storage::open(&path, 1).is_ok());
}

#[tokio::test]
async fn model_information_survives_restart_and_is_rebound_only_to_current_incarnation() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sender, receiver) = mpsc::channel(4);
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_metadata(receiver);
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    sender.send(metadata("d", "D140110")).await.unwrap();
    until(&mut events, |event| matches!(event, Event::Metadata(_))).await;
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    peer.write_all(
        &rusthinq_protocol::thinq1::encode(
            br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#,
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut ack = vec![0; length as usize];
    peer.read_exact(&mut ack).await.unwrap();
    until(&mut events,|event|matches!(event,Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.online)).await;
    let original = handle.snapshot()[0].session.unwrap();
    drop(peer);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    let storage = Storage::open(&path, 4).unwrap();
    assert_eq!(storage.state().metadata["d"].model_name, "D140110");
    let mut server = Server::new(Config {
        generation_floor: storage.state().generation_floor,
        ..Default::default()
    })
    .unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32).unwrap();
    let handle = runtime.handle();
    assert_eq!(handle.metadata_snapshot(), vec![metadata("d", "D140110")]);
    assert!(!handle.snapshot()[0].online);
    assert_eq!(handle.driver_models()["d"].1, "D140110");
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    peer.write_all(
        &rusthinq_protocol::thinq1::encode(
            br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#,
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut ack = vec![0; length as usize];
    peer.read_exact(&mut ack).await.unwrap();
    until(&mut events,|event|matches!(event,Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.online)).await;
    let new = handle.snapshot()[0].session.unwrap();
    assert_eq!(new.incarnation, original.incarnation);
    assert!(new.generation > original.generation);
    assert_eq!(handle.driver_models()["d"].0, new);
    handle
        .forget_scoped("d".into(), new.incarnation)
        .await
        .unwrap();
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(rusthinq_lifecycle::Action::Removed { .. })
        )
    })
    .await;
    assert!(handle.driver_models().is_empty());
    assert!(handle.metadata_snapshot().is_empty());
    drop(peer);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}
