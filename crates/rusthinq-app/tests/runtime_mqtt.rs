#![cfg(feature = "scripting")]
use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Runtime},
    scripts::{Callbacks, Context, DataEncoding, Owner, PublishSink},
};
use rusthinq_lifecycle::Action;
use rusthinq_protocol::mqtt;
use rusthinq_scripting::{Compiled, Limits, worker};
use rusthinq_server::{
    Config, Event as TransportEvent, Protocol, Reject, Server,
    mqtt::{Broker, SystemClock},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{broadcast, mpsc, watch},
    time::timeout,
};

struct ScriptSink(mpsc::Sender<(Context, String)>);

#[tokio::test]
async fn completed_provisioning_auto_loads_real_driver_without_external_mqtt() {
    auto_driver(false, false).await;
}
#[tokio::test]
async fn real_driver_external_publications_have_durable_inventory_and_delete_routes() {
    auto_driver(true, false).await;
}
#[tokio::test]
async fn automatic_driver_reload_preserves_scope_on_compile_failure_and_recovers() {
    auto_driver(false, true).await;
}
async fn auto_driver(external: bool, watched: bool) {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    let sources = directory.path().join("drivers");
    std::fs::create_dir(&sources).unwrap();
    for source in std::fs::read_dir(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/drivers"),
    )
    .unwrap()
    {
        let source = source.unwrap().path();
        if source
            .extension()
            .is_some_and(|extension| extension == "rhai")
        {
            std::fs::copy(&source, sources.join(source.file_name().unwrap())).unwrap();
        }
    }
    let driver_config = rusthinq_app::drivers::Config {
        directory: sources.clone(),
        topic_prefix: "rusthinq".into(),
        il_prefix: Some("ildevice".into()),
        bindings: Default::default(),
        watch: watched,
    };
    let mut runtime = Runtime::new_mqtt(storage, broker.handle(), Duration::ZERO, 128)
        .unwrap()
        .with_drivers(driver_config.clone())
        .unwrap();
    let external_stop = watch::channel(false);
    let mut adapter_task = None;
    let mut remote_task = None;
    let mut confirmed = None;
    if external {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let inventory = directory.path().join("retained.json");
        let (sink, adapter) =
            rusthinq_app::external_mqtt::new(rusthinq_app::external_mqtt::Config {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
                tls: false,
                ca: None,
                client: "real-driver".into(),
                username: None,
                password: None,
                inventory: inventory.clone(),
            })
            .unwrap();
        runtime = runtime.with_external_mqtt(sink.clone());
        adapter_task = Some(tokio::spawn(
            adapter.run(runtime.handle(), external_stop.1.clone()),
        ));
        let (sent, received) = tokio::sync::oneshot::channel();
        confirmed = Some((received, sink));
        remote_task = Some(tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.unwrap();
            async fn frame(peer: &mut tokio::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
                let header = peer.read_u8().await?;
                let mut len = 0usize;
                let mut shift = 0;
                loop {
                    let byte = peer.read_u8().await?;
                    len |= usize::from(byte & 127) << shift;
                    if byte & 128 == 0 {
                        break;
                    }
                    shift += 7;
                    assert!(shift <= 21);
                }
                let mut bytes = vec![0; len];
                peer.read_exact(&mut bytes).await?;
                Ok((header, bytes))
            }
            assert_eq!(frame(&mut peer).await.unwrap().0, 0x10);
            peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let mut publications = 0;
            let mut sent = Some(sent);
            while let Ok((header, body)) = frame(&mut peer).await {
                assert_eq!(header, 0x33);
                let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let topic = std::str::from_utf8(&body[2..2 + length]).unwrap();
                let saved: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&inventory).unwrap()).unwrap();
                assert!(
                    saved["pending"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|entry| entry["topic"] == topic)
                );
                let id = &body[2 + length..4 + length];
                peer.write_all(&[0x40, 2, id[0], id[1]]).await.unwrap();
                if body.len() > 4 + length {
                    publications += 1;
                }
                if publications == 2
                    && let Some(sent) = sent.take()
                {
                    let _ = sent.send(());
                }
            }
            publications
        }));
    }
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let watcher = watched.then(|| {
        tokio::spawn(rusthinq_app::driver_watch::run(
            driver_config,
            handle.clone(),
            stopped.clone(),
        ))
    });
    let app = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    let running = broker.clone();
    let device = tokio::spawn(async move { running.run(stream).await.unwrap() });
    connect(&mut peer).await;
    publish(
        &mut peer,
        "clip/provisioning/devices/d",
        json!({"did":"d","cmd":"deploy","kind":"D140110","data":{}}),
    )
    .await;
    packet(&mut peer).await;
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
    until(&mut events,|event|matches!(event,Event::ScriptOutput {payload,context:Context {generation:1,..}} if serde_json::from_str::<serde_json::Value>(payload).unwrap()["topic"]=="ildevice/d")).await;
    let session = handle.snapshot()[0].session.unwrap();
    if !external {
        handle
            .invoke_script(
                "d".into(),
                session,
                1,
                "__command".into(),
                json!({"prop":"unknown","value":"x"}).to_string(),
            )
            .await
            .unwrap();
        until(&mut events,|event|matches!(event,Event::ScriptOutput {payload,..} if serde_json::from_str::<serde_json::Value>(payload).unwrap()["topic"]=="rusthinq/d/reject")).await;
    }
    assert_eq!(handle.driver_models()["d"].1, "D140110");
    assert_eq!(handle.script_states()["d"].1, 1);
    if watched {
        // Poll once with the original prepared driver before editing it.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let path = sources.join("D140110.rhai");
        let original = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, format!("{original}\n// updated fixture\n")).unwrap();
        timeout(Duration::from_secs(3), async {
            while handle.script_states()["d"].1 != 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        handle
            .invoke_script(
                "d".into(),
                session,
                2,
                "__command".into(),
                json!({"prop":"unknown","value":"x"}).to_string(),
            )
            .await
            .unwrap();
        until(&mut events,|event|matches!(event,Event::ScriptOutput{context,payload} if context.generation==2 && {
            let publication:serde_json::Value=serde_json::from_str(payload).unwrap();
            publication["topic"]=="rusthinq/d/reject" && serde_json::from_str::<serde_json::Value>(publication["payload"].as_str().unwrap()).unwrap()["code"]=="unknown_property"
        })).await;
        std::fs::write(&path, "fn invalid(").unwrap();
        until(
            &mut events,
            |event| matches!(event,Event::Rejected{reason,..} if reason.contains("driver reload")),
        )
        .await;
        assert_eq!(handle.script_states()["d"].1, 2);
        std::fs::write(&path, original).unwrap();
        until(&mut events,|event|matches!(event,Event::ScriptExecuted{context,error:None,..} if context.generation==3)).await;
    }
    if let Some((received, sink)) = confirmed {
        timeout(Duration::from_secs(5), received)
            .await
            .unwrap()
            .unwrap();
        drop(peer);
        until(&mut events,|event|matches!(event,Event::ScriptStopped{context,error:None} if context.session==session)).await;
        timeout(Duration::from_secs(5), sink.flush())
            .await
            .unwrap()
            .unwrap();
        let owner =
            rusthinq_app::lifecycle_cleanup::device_owner("d", session.incarnation).unwrap();
        assert_eq!(sink.delete_owner(owner).await.unwrap(), 2);
        external_stop.0.send_replace(true);
        adapter_task.unwrap().await.unwrap().unwrap();
        assert_eq!(remote_task.unwrap().await.unwrap(), 3);
        assert!(
            rusthinq_app::retained_cleanup::Ledger::open(
                &directory.path().join("retained.json"),
                4
            )
            .unwrap()
            .pending()
            .is_empty()
        );
    }
    broker.stop();
    device.await.unwrap();
    stop.send_replace(true);
    app.await.unwrap().unwrap();
    if let Some(watcher) = watcher {
        watcher.await.unwrap().unwrap();
    }
    let stored = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    assert_eq!(stored.state().metadata["d"].model_name, "D140110");
    assert!(stored.state().metadata["d"].thinq2);
}

#[cfg(feature = "bridge")]
#[tokio::test]
async fn completed_local_provisioning_protects_endpoints_from_firmware_learning() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    let relay = rusthinq_bridge::passthrough::Relay::new(
        Default::default(),
        Arc::new(rusthinq_bridge::passthrough::HttpsConnector),
    )
    .unwrap();
    relay.suspect("https://api.example/fw").unwrap();
    let runtime = Runtime::new_mqtt(storage, broker.handle(), Duration::ZERO, 128)
        .unwrap()
        .with_firmware(relay.clone());
    let mut events = runtime.handle().subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let device = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    connect(&mut peer).await;
    publish(&mut peer, "clip/provisioning/devices/d", json!({"did":"d","cmd":"deploy","kind":"model","data":{"api-server":"https://api.example","mqtt-server":"ssl://mqtt.example:8883"}})).await;
    packet(&mut peer).await;
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
    until(&mut events, |event| {
        matches!(event, Event::Transport(TransportEvent::Ready(_, _)))
    })
    .await;
    // A later cloud command cannot turn proven local endpoints into remote hosts.
    relay
        .learn_command(&json!([
            "https://api.example/fw",
            "https://mqtt.example/fw",
            "https://cdn.example/fw"
        ]))
        .unwrap();
    let snapshot = relay.snapshot().unwrap();
    assert_eq!(
        snapshot["api.example"],
        rusthinq_bridge::firmware::Evidence::Local
    );
    assert_eq!(
        snapshot["mqtt.example"],
        rusthinq_bridge::firmware::Evidence::Local
    );
    assert_eq!(
        snapshot["cdn.example"],
        rusthinq_bridge::firmware::Evidence::Command
    );
    broker.stop();
    device.await.unwrap();
    stop.send_replace(true);
    app.await.unwrap().unwrap();
}
impl PublishSink for ScriptSink {
    fn try_publish(&self, context: &Context, payload: String) -> Result<(), String> {
        self.0
            .try_send((context.clone(), payload))
            .map_err(|error| error.to_string())
    }
}
async fn attach_data(handle: &rusthinq_app::runtime::Handle, id: &str, encoding: DataEncoding) {
    let session = handle
        .snapshot()
        .into_iter()
        .find(|d| d.entry.id == id)
        .unwrap()
        .session
        .unwrap();
    handle.attach_script(id.into(), session,
        Compiled::new("let count=0; fn on_data(v){count+=1;publish(v);send(\"{ \\\"cmd\\\": \\\"script\\\" }\");}", Limits::default(), true).unwrap(),
        worker::Config::default(), Callbacks {data: Some("on_data".into()), data_encoding: encoding, ..Callbacks::default()}).await.unwrap();
}
async fn script_publish(publications: &mut mpsc::Receiver<(Context, String)>) -> (Context, String) {
    timeout(Duration::from_secs(3), publications.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn script_packet(peer: &mut DuplexStream) {
    let bytes = packet(peer).await;
    assert_eq!(bytes[0] & 1, 0, "script downlink must not be retained");
    match mqtt::decode(&bytes, 8192).unwrap() {
        mqtt::Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "lime/devices/d");
            assert_eq!(payload, br#"{ "cmd": "script" }"#);
        }
        other => panic!("expected script publish, got {other:?}"),
    }
}

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

#[tokio::test]
async fn mqtt_binary_script_data_and_redeploy_are_fenced_with_nonretained_downlinks() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    let (sink, mut publications) = mpsc::channel(8);
    let runtime = Runtime::new_mqtt(storage, broker.handle(), Duration::ZERO, 128)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_script_sink(Arc::new(ScriptSink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    let service = broker.clone();
    let transport = tokio::spawn(async move { service.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    until(&mut events, |e| {
        matches!(e, Event::Transport(TransportEvent::Ready(..)))
    })
    .await;
    attach_data(&handle, "d", DataEncoding::Hex).await;
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"device_packet","data":"00ff80"}),
    )
    .await;
    let (first, bytes) = script_publish(&mut publications).await;
    assert_eq!(bytes, "00ff80");
    script_packet(&mut peer).await;
    until(&mut events, |e| {
        matches!(
            e,
            Event::ScriptDelivery {
                delivery: rusthinq_server::Delivery::Sent,
                ..
            }
        )
    })
    .await;
    deploy(&mut peer).await;
    until(&mut events, |e| matches!(e, Event::Transport(TransportEvent::Ready(id,_)) if id.generation > first.session.generation)).await;
    attach_data(&handle, "d", DataEncoding::Hex).await;
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"device_packet","data":"fe01"}),
    )
    .await;
    let (second, bytes) = script_publish(&mut publications).await;
    assert_eq!(bytes, "fe01");
    assert!(second.session.generation > first.session.generation);
    assert_eq!(second.session.incarnation, first.session.incarnation);
    script_packet(&mut peer).await;
    broker.stop();
    transport.await.unwrap();
    stop.send_replace(true);
    app.await.unwrap().unwrap();
}

#[tokio::test]
async fn mixed_script_downlinks_choose_protocol_from_captured_session() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let broker = Broker::sharing(server.handle(), Arc::new(SystemClock));
    let (sink, mut publications) = mpsc::channel(8);
    let runtime = Runtime::new_mixed(
        storage,
        server.handle(),
        broker.handle(),
        Duration::ZERO,
        128,
    )
    .unwrap()
    .with_scripts(Owner::new(2).unwrap())
    .with_script_sink(Arc::new(ScriptSink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    let mut thin = thinq1(&mut server, "other").await;
    until(
        &mut events,
        |e| matches!(e, Event::Lifecycle(Action::Online {id,..}) if id == "other"),
    )
    .await;
    attach_data(&handle, "other", DataEncoding::Utf8).await;
    let body =
        json!({"Header":{"x-lgedm-deviceId":"other"},"Body":{"ReturnCode":"OK"}}).to_string();
    thin.write_all(&rusthinq_protocol::thinq1::encode(body.as_bytes(), 8192).unwrap())
        .await
        .unwrap();
    let (context, payload) = script_publish(&mut publications).await;
    assert_eq!(context.device, "other");
    assert_eq!(payload, body);
    let size = timeout(Duration::from_secs(3), thin.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut frame = vec![0; size as usize];
    thin.read_exact(&mut frame).await.unwrap();
    assert_eq!(frame, br#"{ "cmd": "script" }"#);
    let (stream, mut peer) = tokio::io::duplex(8192);
    let service = broker.clone();
    let transport = tokio::spawn(async move { service.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    until(&mut events, |e| {
        matches!(e, Event::Transport(TransportEvent::Ready(..)))
    })
    .await;
    attach_data(&handle, "d", DataEncoding::Hex).await;
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"device_packet","data":"ff00"}),
    )
    .await;
    let (context, payload) = script_publish(&mut publications).await;
    assert_eq!(context.device, "d");
    assert_eq!(payload, "ff00");
    script_packet(&mut peer).await;
    assert_eq!(handle.snapshot().len(), 2);
    server.shutdown().await;
    transport.await.unwrap();
    stop.send_replace(true);
    app.await.unwrap().unwrap();
}

#[tokio::test]
async fn mixed_runtime_rejects_unrelated_registries_before_starting() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let server = Server::new(Config::default()).unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    assert!(!broker.handle().shares_registry(&server.handle()));
    let result = Runtime::new_mixed(
        storage,
        server.handle(),
        broker.handle(),
        Duration::ZERO,
        32,
    );
    assert!(matches!(result, Err(error) if error.kind() == std::io::ErrorKind::InvalidInput));
    server.shutdown().await;
    broker.stop();
}

#[tokio::test]
async fn mqtt_utf8_admission_error_does_not_fault_script_or_change_its_scope() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    let (sink, mut publications) = mpsc::channel(4);
    let runtime = Runtime::new_mqtt(storage, broker.handle(), Duration::ZERO, 128)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_script_sink(Arc::new(ScriptSink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    let service = broker.clone();
    let transport = tokio::spawn(async move { service.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    until(&mut events, |e| {
        matches!(e, Event::Transport(TransportEvent::Ready(..)))
    })
    .await;
    let session = handle.snapshot()[0].session.unwrap();
    handle
        .attach_script(
            "d".into(),
            session,
            Compiled::new(
                "let count=0; fn on_data(v){count+=1;publish(v+count.to_string());}",
                Limits::default(),
                true,
            )
            .unwrap(),
            worker::Config::default(),
            Callbacks {
                data: Some("on_data".into()),
                ..Callbacks::default()
            },
        )
        .await
        .unwrap();
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"device_packet","data":"ff"}),
    )
    .await;
    until(
        &mut events,
        |e| matches!(e,Event::Rejected {reason,..} if reason.contains("invalid utf-8")),
    )
    .await;
    assert!(publications.try_recv().is_err());
    publish(
        &mut peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"device_packet","data":"6869"}),
    )
    .await;
    assert_eq!(script_publish(&mut publications).await.1, "hi1");
    broker.stop();
    transport.await.unwrap();
    stop.send_replace(true);
    app.await.unwrap().unwrap();
}
