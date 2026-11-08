#![cfg(feature = "scripting")]
//! `on_drop` output is published only when L2 reports the device offline after grace,
//! never for a session that a successor replaced within grace (2026-11-08 dryer report).
use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Handle, Runtime},
};
use rusthinq_lifecycle::Action;
use rusthinq_protocol::mqtt;
use rusthinq_server::{
    Config,
    mqtt::{Broker, SystemClock},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{broadcast, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

const AVAILABLE: &str = "rusthinq/d/available";

struct Rig {
    _directory: tempfile::TempDir,
    broker: Broker,
    handle: Handle,
    events: broadcast::Receiver<Event>,
    stop: watch::Sender<bool>,
    app: JoinHandle<std::io::Result<()>>,
}

fn rig(grace: Duration) -> Rig {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    let sources = directory.path().join("drivers");
    std::fs::create_dir(&sources).unwrap();
    std::fs::copy(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/host-drivers/DropGrace.rhai"),
        sources.join("DropGrace.rhai"),
    )
    .unwrap();
    let runtime = Runtime::new_mqtt(storage, broker.handle(), grace, 256)
        .unwrap()
        .with_drivers(rusthinq_app::drivers::Config {
            directory: sources,
            topic_prefix: "rusthinq".into(),
            limits: Default::default(),
            bindings: Default::default(),
            watch: false,
        })
        .unwrap();
    let handle = runtime.handle();
    let events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = tokio::spawn(runtime.run(stopped));
    Rig {
        _directory: directory,
        broker,
        handle,
        events,
        stop,
        app,
    }
}

/// The availability value of a script publication, if it is one.
fn available(event: &Event) -> Option<String> {
    let Event::ScriptOutput { payload, .. } = event else {
        return None;
    };
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    (value["topic"] == AVAILABLE && value["retain"] == true)
        .then(|| value["payload"].as_str().unwrap_or_default().to_string())
}

impl Rig {
    /// Receive events until `predicate` matches; returns the availability values seen.
    async fn until(&mut self, predicate: impl Fn(&Event) -> bool) -> Vec<String> {
        let mut seen = Vec::new();
        timeout(Duration::from_secs(5), async {
            loop {
                let event = self.events.recv().await.unwrap();
                seen.extend(available(&event));
                if predicate(&event) {
                    return;
                }
            }
        })
        .await
        .unwrap();
        seen
    }
    /// Availability values published during `period`.
    async fn during(&mut self, period: Duration) -> Vec<String> {
        let mut seen = Vec::new();
        let _ = timeout(period, async {
            loop {
                seen.extend(available(&self.events.recv().await.unwrap()));
            }
        })
        .await;
        seen
    }
    async fn attach(&mut self) -> (DuplexStream, JoinHandle<()>, Vec<String>) {
        let (stream, mut peer) = tokio::io::duplex(8192);
        let running = self.broker.clone();
        let device = tokio::spawn(async move { running.run(stream).await.unwrap() });
        connect(&mut peer).await;
        publish(
            &mut peer,
            "clip/provisioning/devices/d",
            json!({"did":"d","cmd":"deploy","kind":"DropGrace","data":{}}),
        )
        .await;
        packet(&mut peer).await;
        publish(
            &mut peer,
            "clip/message/devices/d",
            json!({"did":"d","cmd":"completeProvisioning_ack"}),
        )
        .await;
        let seen = self
            .until(|event| available(event).as_deref() == Some("true"))
            .await;
        (peer, device, seen)
    }
    /// Close the TCP session and wait until L2 has taken it down.
    async fn detach(&mut self, peer: DuplexStream, device: JoinHandle<()>) -> Vec<String> {
        drop(peer);
        let seen = self
            .until(|event| {
                matches!(event, Event::Lifecycle(Action::Changed(d)) if d.session.is_none())
            })
            .await;
        device.await.unwrap();
        seen
    }
    async fn shutdown(mut self) -> Vec<String> {
        self.broker.stop();
        self.stop.send_replace(true);
        self.app.await.unwrap().unwrap();
        let mut seen = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            seen.extend(available(&event));
        }
        seen
    }
}

#[tokio::test]
async fn reconnect_within_grace_never_publishes_on_drop_output() {
    let grace = Duration::from_millis(600);
    let mut rig = rig(grace);
    let (peer, device, _) = rig.attach().await;
    let first = rig.handle.snapshot()[0].session.unwrap();
    let mut seen = rig.detach(peer, device).await;
    // The 2026-11-08 sequence: the old session ends and a new one is up 73 ms later.
    sleep(Duration::from_millis(73)).await;
    let (peer, device, attached) = rig.attach().await;
    seen.extend(attached);
    assert_ne!(rig.handle.snapshot()[0].session.unwrap(), first);
    seen.extend(rig.during(grace * 2).await);
    assert_eq!(
        seen,
        ["true"],
        "a replaced session must not mark its successor offline"
    );
    assert!(rig.handle.snapshot()[0].online);

    // Staying away past grace publishes on_drop output exactly once, after offline.
    let mut seen = rig.detach(peer, device).await;
    assert!(seen.is_empty(), "{seen:?}");
    seen.extend(
        rig.until(|event| matches!(event, Event::Lifecycle(Action::Offline { .. })))
            .await,
    );
    assert!(
        seen.is_empty(),
        "on_drop output waits for the offline report: {seen:?}"
    );
    seen.extend(rig.during(grace * 2).await);
    assert_eq!(seen, ["false"]);

    // Reconnecting restores availability through the driver.
    let (peer, device, seen) = rig.attach().await;
    assert_eq!(seen, ["true"]);
    let seen = rig.detach(peer, device).await;
    assert!(seen.is_empty(), "{seen:?}");
    let seen = rig.shutdown().await;
    // Stopping inside grace: the device will be offline, so held output is published.
    assert_eq!(seen, ["false"]);
}

#[tokio::test]
async fn forget_and_repairing_discard_held_on_drop_output() {
    let grace = Duration::from_millis(400);
    let mut rig = rig(grace);
    let (peer, device, _) = rig.attach().await;
    let old = rig.handle.snapshot()[0].entry.incarnation;
    let mut seen = rig.detach(peer, device).await;
    rig.handle.forget("d".into()).unwrap();
    seen.extend(
        rig.until(|event| matches!(event, Event::Lifecycle(Action::Removed { .. })))
            .await,
    );
    seen.extend(rig.during(grace * 2).await);
    assert!(
        seen.is_empty(),
        "a forgotten device's held output is discarded: {seen:?}"
    );

    // Pairing again is a new incarnation; only its own on_drop output can be published.
    let (peer, device, seen) = rig.attach().await;
    assert_eq!(seen, ["true"]);
    let new = rig.handle.snapshot()[0].entry.incarnation;
    assert_ne!(new, old);
    let mut seen = rig.detach(peer, device).await;
    let mut offline = None;
    timeout(Duration::from_secs(5), async {
        loop {
            let event = rig.events.recv().await.unwrap();
            seen.extend(available(&event));
            if let Event::Lifecycle(Action::Offline { incarnation, .. }) = event {
                offline = Some(incarnation);
                return;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(offline, Some(new));
    seen.extend(rig.during(grace * 2).await);
    assert_eq!(seen, ["false"]);
    assert!(rig.shutdown().await.is_empty());
}

#[tokio::test]
async fn zero_grace_publishes_on_drop_output_at_once() {
    let mut rig = rig(Duration::ZERO);
    let (peer, device, _) = rig.attach().await;
    let mut seen = rig.detach(peer, device).await;
    seen.extend(rig.during(Duration::from_millis(500)).await);
    assert_eq!(seen, ["false"]);
    assert!(rig.shutdown().await.is_empty());
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
