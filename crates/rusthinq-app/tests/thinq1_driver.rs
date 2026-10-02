#![cfg(feature = "scripting")]

use base64::{Engine, engine::general_purpose::STANDARD};
use rusthinq_app::drivers::Config;
use rusthinq_scripting::{Host, Output};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};

// Frames and expected states originate in the pinned upstream driver suite.
// The Mon envelope is constructed: this is replay, not a captured TLS session.
fn captured_frame(name: &str) -> Vec<u8> {
    let source = include_str!("fixtures/drivers/D140110.test.rhai");
    let prefix = format!("fn {name}() {{ \"");
    let frame = source
        .lines()
        .find_map(|line| {
            line.strip_prefix(&prefix)
                .and_then(|rest| rest.split_once('"').map(|pair| pair.0))
        })
        .unwrap();
    rusthinq_protocol::hex::decode(frame).unwrap()
}

fn feed(host: &mut Host, frame: &[u8]) -> Vec<Value> {
    let envelope = json!({
        "Header": {"x-lgedm-deviceId": "dishwasher"},
        "Body": {"Cmd": "Mon", "Format": "B64", "Data": STANDARD.encode(frame)}
    });
    let result = host.invoke(
        1,
        "__data",
        &rusthinq_protocol::hex::encode(envelope.to_string()),
    );
    assert_eq!(result.error, None);
    publications(result.outputs)
}

fn publications(outputs: Vec<Output>) -> Vec<Value> {
    outputs
        .into_iter()
        .map(|output| {
            let Output::Publish(text) = output else {
                panic!("read-only driver emitted a device effect")
            };
            serde_json::from_str(&text).unwrap()
        })
        .collect()
}

#[test]
fn real_thinq1_b64_driver_replays_nine_captured_cycle_states() {
    let config = Config {
        directory: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/drivers"),
        topic_prefix: "rusthinq".into(),
        il_prefix: Some("ildevice".into()),
        bindings: BTreeMap::new(),
        watch: false,
    };
    let mut host = Host::new(
        config
            .prepare("dishwasher", "D140110", false, true)
            .unwrap(),
    );
    let initial = host.invoke(1, "__init", "");
    assert_eq!(initial.error, None);
    let descriptor = publications(initial.outputs);
    assert_eq!(descriptor.len(), 2);
    assert_eq!(descriptor[0]["topic"], "ildevice/dishwasher");
    assert_eq!(descriptor[0]["retain"], true);
    let descriptor: Value =
        serde_json::from_str(descriptor[0]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(descriptor["class"], "dishwasher");
    assert_eq!(descriptor["model"], "D140110");

    for (frame, status, process, remaining, door, child_lock) in [
        ("ready", "ready", "none", "93", "false", "false"),
        ("running", "running", "washing", "93", "false", "false"),
        ("child_lock", "running", "washing", "89", "false", "true"),
        ("rinsing", "running", "rinsing", "9", "false", "false"),
        ("drying", "running", "drying", "9", "false", "false"),
        ("door_open", "running", "drying", "2", "true", "false"),
        ("finished", "finished", "finished", "1", "true", "false"),
        ("standby", "standby", "none", "1", "true", "false"),
        ("off", "off", "none", "1", "true", "false"),
    ] {
        let outputs = feed(&mut host, &captured_frame(frame));
        assert_eq!(outputs.len(), 16, "{frame}");
        for (property, expected) in [
            ("available", "true"),
            ("status", status),
            ("process", process),
            ("remaining_time", remaining),
            ("door", door),
            ("child_lock", child_lock),
            ("total_time", "93"),
            ("error", "none"),
        ] {
            let topic = format!("rusthinq/dishwasher/{property}");
            let output = outputs
                .iter()
                .find(|output| output["topic"] == topic)
                .unwrap();
            assert_eq!(output["payload"], expected, "{frame}/{property}");
            assert_eq!(output["retain"], true);
        }
    }
    // Unsupported/short records must not invent values or fault the worker.
    for frame in ["001122", "AA0632EC0000BB"] {
        assert!(feed(&mut host, &rusthinq_protocol::hex::decode(frame).unwrap()).is_empty());
    }
    let command = host.invoke(
        1,
        "__command",
        &json!({"prop":"status","value":"off"}).to_string(),
    );
    assert_eq!(command.error, None);
    let rejected = publications(command.outputs);
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0]["retain"], false);
    let event: Value = serde_json::from_str(rejected[0]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(event["code"], "read_only");
    let dropped = host.invoke(1, "__drop", "");
    assert_eq!(dropped.error, None);
    let dropped = publications(dropped.outputs);
    assert_eq!(dropped[0]["topic"], "rusthinq/dishwasher/available");
    assert_eq!(dropped[0]["payload"], "false");
}

#[tokio::test]
async fn captured_thinq1_status_automatically_attaches_and_publishes_without_broker() {
    automatic_driver(false).await;
}

#[tokio::test]
async fn captured_thinq1_mqtt_outputs_keep_restart_safe_exact_and_owner_deletion() {
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
            directory: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/drivers"),
            topic_prefix: "rusthinq".into(),
            il_prefix: Some("ildevice".into()),
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
            model_name: "D140110".into(),
            device_type: "dishwasher".into(),
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
        "Body":{"Cmd":"Mon","Format":"B64","Data":STANDARD.encode(captured_frame("running"))}
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
        let mut descriptor = false;
        let mut status = false;
        while !descriptor || !status {
            match events.recv().await.unwrap() {
                Event::ScriptOutput { payload, .. } => {
                    let message: Value = serde_json::from_str(&payload).unwrap();
                    descriptor |= message["topic"] == "ildevice/dishwasher";
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
        assert_eq!(retained.len(), 17);
        assert_eq!(retained["rusthinq/dishwasher/status"], "running");
        assert_eq!(retained["rusthinq/dishwasher/process"], "washing");
        assert_eq!(retained["rusthinq/dishwasher/total_time"], "93");
        assert_eq!(retained["rusthinq/dishwasher/available"], "true");
        let descriptor: Value = serde_json::from_str(&retained["ildevice/dishwasher"]).unwrap();
        assert_eq!(descriptor["model"], "D140110");
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
        assert_eq!(ledger.pending().len(), 17);
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
            16
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
