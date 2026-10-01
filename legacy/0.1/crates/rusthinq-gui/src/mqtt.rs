//! rusthinq-gui's own MQTT client -- deliberately independent of
//! `rusthinq_core::mqtt::MqttSink`, which only ever talks to code in the same
//! process as the real `mqtt_client.rs` connection. This dials the same external
//! broker (`config.mqtt.mqtt_url`) as its own connection instead, so the dashboard
//! sees rusthinq-cloud exactly the way any other MQTT client would -- it is bundled
//! into the same binary for deploy convenience only, not wired into `DeviceBridge`/
//! `Bridge` directly.
//!
//! Subscribes for the whole run to `<rusthinq_prefix>/devices` (retained snapshot,
//! see `devlist.rs`) and `<rusthinq_prefix>/bridge/#` (`bridge/status`,
//! `bridge/login-url`, see `bridge_control.rs`). Per-device raw traffic
//! (`<raw_prefix>/<id>/raw/rx`/`tx`/`clip/tx`, see `raw_bus.rs`) is subscribed on demand by
//! `http.rs`'s `/device` handler and restored after each broker reconnect.

use crate::state::Shared;
use anyhow::Result;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS, Transport};
use rusthinq_core::config::MqttConfig;
use rusthinq_core::mqtt::parse_mqtt_url;
use rusthinq_util::backoff::ExponentialBackoff;
use rusthinq_util::sync::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast;

pub type Publish = rumqttc::Publish;

/// Cheap-clone handle for `http.rs`'s handlers: publish commands, and listen for
/// whatever's already flowing through the shared event loop -- rather than each
/// axum handler owning its own MQTT connection.
#[derive(Clone)]
pub struct Handle {
    pub client: AsyncClient,
    events: broadcast::Sender<Publish>,
    pub prefix: String,
    pub raw_prefix: Option<String>,
    raw_topics: Arc<Mutex<HashSet<String>>>,
}

impl Handle {
    /// Remember monitor topics across clean-session reconnects.
    pub async fn subscribe_raw(&self, topic: &str) {
        self.raw_topics.lock().insert(topic.to_string());
        let _ = self.client.subscribe(topic, QoS::AtMostOnce).await;
    }

    /// Every incoming publish from here on. Bounded and lossy under backpressure --
    /// a slow/gone receiver (a closed browser tab) just misses old messages rather
    /// than blocking the MQTT event loop for everyone else.
    pub fn subscribe_events(&self) -> broadcast::Receiver<Publish> {
        self.events.subscribe()
    }

    pub fn bridge_status_topic(&self, id: &str) -> String {
        format!("{}/{}/bridge/status", self.prefix, id)
    }

    pub fn forget_status_topic(&self, id: &str) -> String {
        format!("{}/{}/forget/status", self.prefix, id)
    }

    pub fn account_topic(&self, suffix: &str) -> String {
        format!("{}/bridge/{}", self.prefix, suffix)
    }
}

/// Starts the connection in the background and returns a `Handle` immediately --
/// callers don't need to wait for the first `ConnAck` before publishing; rumqttc
/// queues outbound work until the connection is up.
pub fn start(cfg: MqttConfig, state: Arc<Shared>) -> Result<Handle> {
    let (host, port, use_tls) = parse_mqtt_url(&cfg.mqtt_url)?;
    let mut opts = MqttOptions::new("rusthinq-gui", (host, port));
    opts.set_keep_alive(rusthinq_util::MQTT_KEEP_ALIVE.as_secs() as u16);
    if !cfg.mqtt_user.is_empty() {
        opts.set_credentials(cfg.mqtt_user.clone(), cfg.mqtt_pass.clone());
    }
    if use_tls {
        opts.set_transport(Transport::tls_with_default_config());
    }
    let (client, eventloop) = AsyncClient::builder(opts).capacity(64).build();
    let (events_tx, _) = broadcast::channel(256);

    let raw_topics = Arc::new(Mutex::new(HashSet::new()));
    let handle = Handle {
        client: client.clone(),
        events: events_tx.clone(),
        prefix: cfg.rusthinq_prefix.clone(),
        raw_prefix: cfg.raw_prefix.clone(),
        raw_topics: raw_topics.clone(),
    };

    tokio::spawn(run_event_loop(
        eventloop,
        client,
        cfg.rusthinq_prefix,
        events_tx,
        state,
        raw_topics,
    ));

    Ok(handle)
}

async fn run_event_loop(
    mut eventloop: rumqttc::EventLoop,
    client: AsyncClient,
    prefix: String,
    events_tx: broadcast::Sender<Publish>,
    state: Arc<Shared>,
    raw_topics: Arc<Mutex<HashSet<String>>>,
) {
    let devices_topic = format!("{prefix}/devices");
    let mut backoff = ExponentialBackoff::for_local_control_plane();
    let mut subscriptions: Option<tokio::task::JoinHandle<()>> = None;
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                backoff.reset();
                tracing::info!("rusthinq-gui MQTT connection established");
                state.set_gui_mqtt_connected(true);
                if let Some(task) = subscriptions.take() {
                    task.abort();
                }
                let client = client.clone();
                let topics = vec![
                    devices_topic.clone(),
                    format!("{prefix}/bridge/#"),
                    format!("{prefix}/+/bridge/status"),
                    format!("{prefix}/+/forget/status"),
                ];
                let raw: Vec<_> = raw_topics.lock().iter().cloned().collect();
                subscriptions = Some(tokio::spawn(async move {
                    for topic in topics {
                        let _ = client.subscribe(topic, QoS::AtLeastOnce).await;
                    }
                    for topic in raw {
                        let _ = client.subscribe(topic, QoS::AtMostOnce).await;
                    }
                }));
            }
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                if p.topic == devices_topic {
                    state.set_snapshot(&p.payload);
                }
                let _ = events_tx.send(p);
            }
            Ok(_) => {}
            Err(e) => {
                state.set_gui_mqtt_connected(false);
                let delay = backoff.next_delay();
                tracing::warn!("rusthinq-gui MQTT error: {e} (retrying in {delay:?})");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mqtt_plain() {
        let (host, port, tls) = parse_mqtt_url("mqtt://broker.local:1883").unwrap();
        assert_eq!(host, "broker.local");
        assert_eq!(port, 1883);
        assert!(!tls);
    }

    #[test]
    fn parse_mqtts_defaults_port_and_tls() {
        let (host, port, tls) = parse_mqtt_url("mqtts://broker.example").unwrap();
        assert_eq!(host, "broker.example");
        assert_eq!(port, 8883);
        assert!(tls);
    }
}

#[cfg(test)]
mod reconnect_tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let kind = stream.read_u8().await.unwrap();
        let mut length = 0;
        let mut multiplier = 1;
        loop {
            let byte = stream.read_u8().await.unwrap();
            length += (byte & 127) as usize * multiplier;
            if byte & 128 == 0 {
                break;
            }
            multiplier *= 128;
        }
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        (kind, body)
    }

    async fn accept(listener: &TcpListener) -> TcpStream {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(packet(&mut stream).await.0 >> 4, 1);
        stream.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        stream
    }

    async fn until_subscribed(stream: &mut TcpStream, topic: &str) {
        loop {
            let (kind, body) = packet(stream).await;
            if kind >> 4 == 8 {
                let length = u16::from_be_bytes([body[2], body[3]]) as usize;
                stream
                    .write_all(&[0x90, 3, body[0], body[1], 0])
                    .await
                    .unwrap();
                if &body[4..4 + length] == topic.as_bytes() {
                    return;
                }
            }
        }
    }

    #[tokio::test]
    async fn monitor_subscription_is_restored_after_clean_session_reconnect() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let cfg = MqttConfig {
                mqtt_url: format!("mqtt://{}", listener.local_addr().unwrap()),
                rusthinq_prefix: "r".into(),
                raw_prefix: Some("raw".into()),
                mqtt_user: String::new(),
                mqtt_pass: String::new(),
                raw: Default::default(),
                state_file: None,
            };
            let handle = start(cfg, Shared::new()).unwrap();
            let mut first = accept(&listener).await;
            let topic = "raw/device/raw/rx";
            handle.subscribe_raw(topic).await;
            until_subscribed(&mut first, topic).await;
            // Let the client consume SUBACK, so success requires restoring an
            // acknowledged subscription rather than replaying an in-flight one.
            let mut events = handle.subscribe_events();
            first.write_all(&[0x30, 4, 0, 1, b'x', b'1']).await.unwrap();
            events.recv().await.unwrap();
            drop(first);
            let mut second = accept(&listener).await;
            until_subscribed(&mut second, topic).await;
        })
        .await
        .unwrap();
    }
}
