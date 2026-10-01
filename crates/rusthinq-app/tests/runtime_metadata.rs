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
