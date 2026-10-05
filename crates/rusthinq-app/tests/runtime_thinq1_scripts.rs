#![cfg(feature = "scripting")]

use base64::{Engine, engine::general_purpose::STANDARD};
use rusthinq_app::drivers::Config;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};

#[tokio::test]
async fn thinq1_status_automatically_attaches_and_publishes_without_broker() {
    automatic_driver(false).await;
}

#[tokio::test]
async fn thinq1_mqtt_outputs_keep_restart_safe_exact_and_owner_deletion() {
    automatic_driver(true).await;
}

async fn automatic_driver(external: bool) {
    use rusthinq_app::{
        lifecycle_storage::Storage,
        runtime::{Event, Runtime},
    };
    use rusthinq_server::{Server, thinq1_http::Metadata};
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{mpsc, watch},
        time::timeout,
    };
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Default::default()).unwrap();
    let (metadata, receiver) = mpsc::channel(4);
    let mut runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 64)
        .unwrap()
        .with_metadata(receiver)
        .with_drivers(Config {
            directory: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/host-drivers"),
            topic_prefix: "rusthinq".into(),
            bindings: BTreeMap::new(),
            watch: false,
        })
        .unwrap();
    let inventory = directory.path().join("retained.json");
    let (adapter_stop, adapter_stopped) = watch::channel(false);
    let mut mqtt = None;
    if external {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = rusthinq_app::external_mqtt::Config {
            host: "127.0.0.1".into(),
            port: listener.local_addr().unwrap().port(),
            tls: false,
            ca: None,
            client: "thinq1-replay".into(),
            username: None,
            password: None,
            inventory: inventory.clone(),
        };
        let (sink, adapter) = rusthinq_app::external_mqtt::new(config.clone()).unwrap();
        runtime = runtime.with_external_mqtt(sink.clone());
        let adapter = tokio::spawn(adapter.run(runtime.handle(), adapter_stopped));
        let (reports, received) = mpsc::channel(128);
        let broker = tokio::spawn(reference_broker(listener, inventory.clone(), reports));
        mqtt = Some((sink, adapter, broker, received, config));
    }
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    metadata
        .send(Metadata {
            device_id: "dishwasher".into(),
            model_name: "Echo".into(),
            device_type: "dishwasher".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while !matches!(events.recv().await.unwrap(), Event::Metadata(_)) {}
    })
    .await
    .unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    let envelope = json!({
        "Header":{"x-lgedm-deviceId":"dishwasher"},
        "Body":{"Cmd":"Mon","Format":"B64","Data":STANDARD.encode(b"running")}
    });
    peer.write_all(
        &rusthinq_protocol::thinq1::encode(envelope.to_string().as_bytes(), 8192).unwrap(),
    )
    .await
    .unwrap();
    let ack = timeout(Duration::from_secs(3), async {
        let size = peer.read_u32().await.unwrap();
        let mut bytes = vec![0; size as usize];
        peer.read_exact(&mut bytes).await.unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    })
    .await
    .unwrap();
    assert_eq!(ack["Header"], envelope["Header"]);
    timeout(Duration::from_secs(3), async {
        let mut status = false;
        while !status {
            match events.recv().await.unwrap() {
                Event::ScriptOutput { payload, .. } => {
                    let message: Value = serde_json::from_str(&payload).unwrap();
                    status |= message["topic"] == "rusthinq/dishwasher/status"
                        && message["payload"] == "running";
                }
                Event::ScriptStopped { error, .. } => panic!("driver stopped: {error:?}"),
                Event::ScriptExecuted {
                    error: Some(error), ..
                } => panic!("driver failed: {error}"),
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(handle.snapshot().len(), 1);
    assert!(handle.snapshot()[0].online);
    let (session, generation, _) = handle.script_states()["dishwasher"];
    let presentation = handle.publications("dishwasher", session, generation);
    assert!(
        presentation
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value["topic"] == "rusthinq/dishwasher/status"
                && value["payload"] == "running")
    );
    assert_eq!(
        handle.publications("dishwasher", session, generation + 1),
        json!([])
    );
    assert_eq!(handle.external_mqtt().is_some(), external);
    let owner = rusthinq_app::lifecycle_cleanup::device_owner(
        "dishwasher",
        handle.snapshot()[0].entry.incarnation,
    )
    .unwrap();
    if let Some((sink, _, _, received, _)) = mqtt.as_mut() {
        timeout(Duration::from_secs(3), sink.flush())
            .await
            .unwrap()
            .unwrap();
        let mut retained = BTreeMap::new();
        while let Ok((topic, value)) = received.try_recv() {
            retained.insert(topic, value);
        }
        assert_eq!(retained.len(), 2);
        assert_eq!(retained["rusthinq/dishwasher/status"], "running");
        assert_eq!(retained["rusthinq/dishwasher/available"], "true");
    }
    drop(peer);
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if let Some((sink, adapter, broker, mut received, config)) = mqtt {
        timeout(Duration::from_secs(3), sink.flush())
            .await
            .unwrap()
            .unwrap();
        let terminal = timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            terminal,
            ("rusthinq/dishwasher/available".into(), "false".into())
        );
        adapter_stop.send_replace(true);
        timeout(Duration::from_secs(3), adapter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let ledger = rusthinq_app::retained_cleanup::Ledger::open(&inventory, 128).unwrap();
        assert_eq!(ledger.pending().len(), 2);
        assert!(ledger.pending().iter().all(|topic| topic.owner == owner));
        drop(ledger);
        // Reopen with no in-memory publication cache: deletion must use the ledger.
        let (sink, adapter) = rusthinq_app::external_mqtt::new(config).unwrap();
        let (stop, stopped) = watch::channel(false);
        let adapter = tokio::spawn(adapter.run(handle, stopped));
        assert_eq!(
            timeout(
                Duration::from_secs(3),
                sink.delete_topic(owner.clone(), "rusthinq/dishwasher/status".into(),)
            )
            .await
            .unwrap()
            .unwrap(),
            1
        );
        assert_eq!(
            timeout(Duration::from_secs(3), sink.delete_owner(owner))
                .await
                .unwrap()
                .unwrap(),
            1
        );
        stop.send_replace(true);
        timeout(Duration::from_secs(3), adapter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            timeout(Duration::from_secs(3), broker)
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
        let ledger = rusthinq_app::retained_cleanup::Ledger::open(&inventory, 128).unwrap();
        assert!(ledger.pending().is_empty());
        assert!(ledger.requested().is_empty());
    }
    assert!(Storage::open(&directory.path().join("devices.json"), 4).is_ok());
}

// A bounded MQTT 3.1.1 peer, retaining topic bytes across two client connections.
// Checkpoint ownership must exist before *every* retained publish, including clears.
async fn reference_broker(
    listener: tokio::net::TcpListener,
    inventory: PathBuf,
    reports: tokio::sync::mpsc::Sender<(String, String)>,
) -> BTreeMap<String, String> {
    use tokio::io::AsyncWriteExt;
    let mut retained = BTreeMap::new();
    for _ in 0..2 {
        let (mut peer, _) = listener.accept().await.unwrap();
        let (header, _) = broker_frame(&mut peer).await.unwrap();
        assert_eq!(header, 0x10);
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        while let Ok((header, body)) = broker_frame(&mut peer).await {
            if header == 0xe0 {
                break;
            }
            if header == 0xc0 {
                peer.write_all(&[0xd0, 0]).await.unwrap();
                continue;
            }
            assert_eq!(header, 0x33);
            let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
            let topic = std::str::from_utf8(&body[2..2 + length])
                .unwrap()
                .to_string();
            let value = std::str::from_utf8(&body[4 + length..])
                .unwrap()
                .to_string();
            let saved: Value = serde_json::from_slice(&std::fs::read(&inventory).unwrap()).unwrap();
            assert!(
                saved["pending"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["topic"] == topic)
            );
            if value.is_empty() {
                retained.remove(&topic);
            } else {
                retained.insert(topic.clone(), value.clone());
            }
            reports.try_send((topic, value)).unwrap();
            let id = &body[2 + length..4 + length];
            peer.write_all(&[0x40, 2, id[0], id[1]]).await.unwrap();
        }
    }
    retained
}
async fn broker_frame(peer: &mut tokio::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let header = peer.read_u8().await?;
    let mut length = 0usize;
    for shift in [0, 7, 14, 21] {
        let byte = peer.read_u8().await?;
        length |= usize::from(byte & 127) << shift;
        assert!(length <= 131072);
        if byte & 128 == 0 {
            let mut bytes = vec![0; length];
            peer.read_exact(&mut bytes).await?;
            return Ok((header, bytes));
        }
    }
    Err(std::io::Error::other("invalid MQTT length"))
}

/// Broker outage: device traffic and the driver keep running, non-retained output is dropped
/// and counted rather than failing the script, and reconnect republishes the latest retained state.
#[tokio::test]
async fn broker_outage_keeps_driver_running_and_reconnect_resyncs_retained_state() {
    use rusthinq_app::{
        lifecycle_storage::Storage,
        runtime::{Event, Runtime},
    };
    use rusthinq_server::{Server, thinq1_http::Metadata};
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{mpsc, oneshot, watch},
        time::timeout,
    };
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Default::default()).unwrap();
    let (metadata, receiver) = mpsc::channel(4);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sink, adapter) = rusthinq_app::external_mqtt::new(rusthinq_app::external_mqtt::Config {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
        tls: false,
        ca: None,
        client: "outage".into(),
        username: None,
        password: None,
        inventory: directory.path().join("retained.json"),
    })
    .unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 64)
        .unwrap()
        .with_metadata(receiver)
        .with_drivers(Config {
            directory: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/host-drivers"),
            topic_prefix: "rusthinq".into(),
            bindings: BTreeMap::new(),
            watch: false,
        })
        .unwrap()
        .with_external_mqtt(sink.clone());
    let handle = runtime.handle();
    let (adapter_stop, adapter_stopped) = watch::channel(false);
    let adapter = tokio::spawn(adapter.run(handle.clone(), adapter_stopped));
    let (first_done, first) = oneshot::channel();
    let (second_done, second) = oneshot::channel::<BTreeMap<String, String>>();
    let broker = tokio::spawn(async move {
        let mut first_done = Some(first_done);
        let mut second_done = Some(second_done);
        for connection in 0..2 {
            let (mut peer, _) = listener.accept().await.unwrap();
            assert_eq!(broker_frame(&mut peer).await.unwrap().0, 0x10);
            peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let mut retained = BTreeMap::new();
            while let Ok((header, body)) = broker_frame(&mut peer).await {
                match header {
                    0xe0 => break,
                    0xc0 => {
                        peer.write_all(&[0xd0, 0]).await.unwrap();
                        continue;
                    }
                    _ => {}
                }
                assert_eq!(header, 0x33);
                let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let topic = std::str::from_utf8(&body[2..2 + length]).unwrap();
                let value = std::str::from_utf8(&body[4 + length..]).unwrap();
                retained.insert(topic.to_string(), value.to_string());
                let id = &body[2 + length..4 + length];
                peer.write_all(&[0x40, 2, id[0], id[1]]).await.unwrap();
                if connection == 0 && retained.len() == 2 {
                    let _ = first_done.take().unwrap().send(());
                    break; // drop the connection: the outage
                }
                // A reconnect must never clear the device's state (an empty descriptor is a removal).
                assert!(
                    connection == 0 || !value.is_empty(),
                    "reconnect cleared {topic}"
                );
                if connection == 1
                    && retained.len() == 2
                    && retained
                        .get("rusthinq/dishwasher/status")
                        .map(String::as_str)
                        == Some("rinsing")
                {
                    let _ = second_done.take().unwrap().send(retained.clone());
                }
            }
        }
    });
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    metadata
        .send(Metadata {
            device_id: "dishwasher".into(),
            model_name: "Echo".into(),
            device_type: "dishwasher".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    async fn status(peer: &mut tokio::io::DuplexStream, frame: &str) {
        let envelope = json!({
            "Header":{"x-lgedm-deviceId":"dishwasher"},
            "Body":{"Cmd":"Mon","Format":"B64","Data":STANDARD.encode(frame.as_bytes())}
        });
        peer.write_all(
            &rusthinq_protocol::thinq1::encode(envelope.to_string().as_bytes(), 8192).unwrap(),
        )
        .await
        .unwrap();
        let ack = timeout(Duration::from_secs(3), async {
            let size = peer.read_u32().await.unwrap();
            let mut bytes = vec![0; size as usize];
            peer.read_exact(&mut bytes).await.unwrap();
            serde_json::from_slice::<Value>(&bytes).unwrap()
        })
        .await
        .unwrap();
        assert_eq!(ack["Header"]["x-lgedm-deviceId"], "dishwasher");
    }
    status(&mut peer, "running").await;
    timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap();
    // During the outage the device is still acknowledged and the driver still runs.
    timeout(Duration::from_secs(3), async {
        while !matches!(
            *sink.status().borrow_and_update(),
            rusthinq_app::external_mqtt::Status::Failed(_)
        ) {
            sink.status().changed().await.unwrap();
        }
    })
    .await
    .ok();
    let dropped = sink.dropped_transient();
    status(&mut peer, "rinsing").await;
    timeout(Duration::from_secs(3), async {
        loop {
            match events.recv().await.unwrap() {
                Event::ScriptOutput { payload, .. } => {
                    let message: Value = serde_json::from_str(&payload).unwrap();
                    if message["topic"] == "rusthinq/dishwasher/status"
                        && message["payload"] == "rinsing"
                    {
                        break;
                    }
                }
                Event::ScriptStopped { error, .. } => panic!("driver stopped: {error:?}"),
                Event::ScriptExecuted {
                    error: Some(error), ..
                } => panic!("driver failed: {error}"),
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(sink.dropped_transient() >= dropped);
    let resynced = timeout(Duration::from_secs(10), second)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resynced["rusthinq/dishwasher/status"], "rinsing");
    assert!(handle.snapshot()[0].online);
    drop(peer);
    server.shutdown().await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    adapter_stop.send_replace(true);
    timeout(Duration::from_secs(3), adapter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    broker.abort();
}
