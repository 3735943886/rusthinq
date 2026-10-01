use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Runtime},
};
use rusthinq_lifecycle::Action;
use rusthinq_protocol::mqtt;
use rusthinq_server::{
    Config, Event as TransportEvent, Protocol, Reject, Server,
    mqtt::{Broker, SystemClock},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{broadcast, watch},
    time::timeout,
};

async fn packet(peer: &mut DuplexStream) -> Vec<u8> {
    timeout(Duration::from_secs(3), async {
        let mut bytes = vec![peer.read_u8().await.unwrap()];
        loop {
            if let Some(total) = mqtt::length(&bytes, 8192).unwrap() {
                let offset = bytes.len();
                bytes.resize(total, 0);
                peer.read_exact(&mut bytes[offset..]).await.unwrap();
                return bytes;
            }
            bytes.push(peer.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap()
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
async fn publish(peer: &mut DuplexStream, topic: &str, value: serde_json::Value) {
    peer.write_all(&mqtt::publish(topic, value.to_string().as_bytes(), 8192).unwrap())
        .await
        .unwrap();
}
async fn connect(peer: &mut DuplexStream) {
    peer.write_all(
        &mqtt::frame(
            0x10,
            &[0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'x'],
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(packet(peer).await, [0x20, 2, 0, 0]);
    let topic = b"lime/devices/d";
    let mut body = vec![0, 1, 0, topic.len() as u8];
    body.extend_from_slice(topic);
    body.push(0);
    peer.write_all(&mqtt::frame(0x82, &body, 8192).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(peer).await, [0x90, 3, 0, 1, 0]);
}
async fn deploy(peer: &mut DuplexStream) {
    publish(
        peer,
        "clip/provisioning/devices/d",
        json!({"did":"d","cmd":"deploy","kind":"model","data":{}}),
    )
    .await;
    assert!(matches!(
        mqtt::decode(&packet(peer).await, 8192).unwrap(),
        mqtt::Packet::Publish { .. }
    ));
    publish(
        peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
}

async fn thinq1(server: &mut Server, id: &str) -> DuplexStream {
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    let payload = json!({"Header":{"x-lgedm-deviceId":id},"Body":{"Cmd":"Mon"}}).to_string();
    peer.write_all(&rusthinq_protocol::thinq1::encode(payload.as_bytes(), 8192).unwrap())
        .await
        .unwrap();
    let length = timeout(Duration::from_secs(3), peer.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut ack = vec![0; length as usize];
    peer.read_exact(&mut ack).await.unwrap();
    peer
}

#[tokio::test]
async fn mixed_protocols_share_lifecycle_allocator_and_fence_outbound_codecs() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let block = storage.reserve_generations(4).unwrap();
    let mut server = Server::new(Config {
        generation_floor: block.floor,
        generation_ceiling: block.ceiling,
        ..Config::default()
    })
    .unwrap();
    let server_handle = server.handle();
    let broker = Broker::sharing(server_handle.clone(), Arc::new(SystemClock));
    let runtime = Runtime::new(storage, server_handle.clone(), Duration::ZERO, 128)
        .unwrap()
        .with_generation_refill(4, 1)
        .unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    let mut first = thinq1(&mut server, "d").await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    let incarnation = handle.snapshot()[0].entry.incarnation;
    let (stream, mut peer) = tokio::io::duplex(8192);
    let service = broker.clone();
    let transport = tokio::spawn(async move { service.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    until(&mut events, |event| {
        matches!(event, Event::Transport(TransportEvent::Ready(_, _)))
    })
    .await;
    let mqtt_session = broker.handle().snapshot()[0].clone();
    assert_eq!(mqtt_session.generation, 2);
    assert_eq!(server_handle.protocol(&mqtt_session), Ok(Protocol::ThinQ2));
    assert!(matches!(
        server_handle.send(&mqtt_session, b"{}"),
        Err(Reject::WrongTransport)
    ));
    assert_eq!(handle.snapshot()[0].entry.incarnation, incarnation);
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(3), first.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let _other = thinq1(&mut server, "other").await;
    until(&mut events, |event| {
        matches!(event, Event::GenerationExtended { ceiling: 8 })
    })
    .await;
    let other = server_handle
        .snapshot()
        .into_iter()
        .find(|session| session.device == "other")
        .unwrap();
    assert_eq!(other.generation, 3);
    assert_eq!(server_handle.protocol(&other), Ok(Protocol::ThinQ1));
    assert!(matches!(
        broker.handle().send(&other, b"{}"),
        Err(Reject::WrongTransport)
    ));
    let _replacement = thinq1(&mut server, "d").await;
    until(&mut events,|event|matches!(event,Event::Lifecycle(Action::Changed(device)) if device.entry.id=="d" && device.entry.last_generation==4)).await;
    timeout(Duration::from_secs(3), transport)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        handle
            .snapshot()
            .iter()
            .find(|device| device.entry.id == "d")
            .unwrap()
            .entry
            .incarnation,
        incarnation
    );
    assert!(matches!(
        broker.handle().send(&mqtt_session, b"{}"),
        Err(Reject::StaleSession)
    ));
    handle.forget("d".into()).unwrap();
    until(
        &mut events,
        |event| matches!(event,Event::Lifecycle(Action::Removed{id,..}) if id=="d"),
    )
    .await;
    assert_eq!(handle.snapshot().len(), 1);
    assert_eq!(handle.snapshot()[0].entry.id, "other");
    // One shared stop wakes both protocols; each owner still joins its own tasks.
    server.shutdown().await;
    stop.send_replace(true);
    app.await.unwrap().unwrap();
    assert!(broker.handle().snapshot().is_empty());
    let storage = Storage::open(&path, 8).unwrap();
    assert_eq!(storage.state().generation_floor, 8);
    assert_eq!(storage.state().ledger.entries.len(), 1);
    assert_eq!(storage.state().ledger.entries[0].id, "other");
}

#[tokio::test]
async fn mixed_admission_bound_and_owner_shutdown_apply_to_both_transports() {
    let mut server = Server::new(Config {
        max_connections: 2,
        ..Config::default()
    })
    .unwrap();
    let broker = Broker::sharing(server.handle(), Arc::new(SystemClock));
    let _thin_peer = thinq1(&mut server, "other").await;
    let (stream, mut peer) = tokio::io::duplex(8192);
    let service = broker.clone();
    let task = tokio::spawn(async move { service.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    // CONNECT processing allocated the MQTT slot before acknowledging the peer.
    let (stream, _peer) = tokio::io::duplex(64);
    assert_eq!(server.admit(stream), Err(Reject::Busy));
    let (stream, _peer) = tokio::io::duplex(64);
    assert!(broker.run(stream).await.is_err());
    assert_eq!(server.handle().generation_budget().0, 2);
    server.shutdown().await;
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    assert!(broker.handle().snapshot().is_empty());
    let (stream, _peer) = tokio::io::duplex(64);
    assert!(broker.run(stream).await.is_err());
}

#[tokio::test]
async fn mqtt_lifecycle_redeploy_refill_and_forget_wait_for_transport_completion() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let block = storage.reserve_generations(4).unwrap();
    let broker = Broker::new(
        Config {
            generation_floor: block.floor,
            generation_ceiling: block.ceiling,
            ..Config::default()
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    let runtime = Runtime::new_mqtt(storage, broker.handle(), Duration::ZERO, 128)
        .unwrap()
        .with_generation_refill(4, 2)
        .unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    let service = broker.clone();
    let transport = tokio::spawn(async move { service.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    let first = handle.snapshot()[0].entry.clone();
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"opaque","unknown":42}),
    )
    .await;
    until(&mut events, |event| matches!(event,Event::Transport(TransportEvent::CloudBound(id,_)) if id.generation == first.last_generation)).await;
    deploy(&mut peer).await;
    until(&mut events, |event| {
        matches!(event, Event::GenerationExtended { ceiling: 8 })
    })
    .await;
    if handle.snapshot()[0].entry.last_generation == first.last_generation {
        until(&mut events, |event| matches!(event, Event::Lifecycle(Action::Changed(device)) if device.entry.last_generation > first.last_generation)).await;
    }
    assert_eq!(handle.snapshot()[0].entry.incarnation, first.incarnation);
    assert!(handle.snapshot()[0].entry.last_generation > first.last_generation);
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| {
        matches!(event, Event::Lifecycle(Action::Removed { .. }))
    })
    .await;
    timeout(Duration::from_secs(3), transport)
        .await
        .unwrap()
        .unwrap();
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    assert!(handle.snapshot().is_empty());
    assert!(broker.handle().snapshot().is_empty());
    broker.stop();
    stop.send_replace(true);
    app.await.unwrap().unwrap();
    let storage = Storage::open(&path, 8).unwrap();
    assert!(storage.state().ledger.entries.is_empty());
    assert_eq!(storage.state().generation_floor, 8);
}

#[tokio::test]
async fn mqtt_eof_persists_device_and_restart_restores_offline() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    let runtime =
        Runtime::new_mqtt(storage, broker.handle(), Duration::from_millis(10), 128).unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    let service = broker.clone();
    let transport = tokio::spawn(async move { service.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    drop(peer);
    transport.await.unwrap();
    until(&mut events, |event| {
        matches!(event, Event::Lifecycle(Action::Offline { .. }))
    })
    .await;
    assert!(handle.snapshot()[0].session.is_none());
    broker.stop();
    stop.send_replace(true);
    app.await.unwrap().unwrap();
    let storage = Storage::open(&path, 8).unwrap();
    let broker = Broker::new(
        Config {
            generation_floor: storage.state().generation_floor,
            ..Config::default()
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    let restored = Runtime::new_mqtt(storage, broker.handle(), Duration::ZERO, 128).unwrap();
    assert_eq!(restored.handle().snapshot().len(), 1);
    assert!(!restored.handle().snapshot()[0].online);
}
