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
//! `http.rs`'s `/device` handler for as long as a monitor page is open.

use crate::state::Shared;
use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS, Transport};
use rusthinq_core::config::MqttConfig;
use rusthinq_util::backoff::ExponentialBackoff;
use std::sync::Arc;
use tokio::sync::broadcast;
use url::Url;

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
}

impl Handle {
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

    let handle = Handle {
        client: client.clone(),
        events: events_tx.clone(),
        prefix: cfg.rusthinq_prefix.clone(),
        raw_prefix: cfg.raw_prefix.clone(),
    };

    tokio::spawn(run_event_loop(
        eventloop,
        client,
        cfg.rusthinq_prefix,
        events_tx,
        state,
    ));

    Ok(handle)
}

async fn run_event_loop(
    mut eventloop: rumqttc::EventLoop,
    client: AsyncClient,
    prefix: String,
    events_tx: broadcast::Sender<Publish>,
    state: Arc<Shared>,
) {
    let devices_topic = format!("{prefix}/devices");
    let mut backoff = ExponentialBackoff::for_local_control_plane();
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                backoff.reset();
                tracing::info!("rusthinq-gui MQTT connection established");
                state.set_gui_mqtt_connected(true);
                let _ = client.subscribe(&devices_topic, QoS::AtLeastOnce).await;
                // Account-level topics (`<prefix>/bridge/status`, `.../login-url`)...
                let _ = client
                    .subscribe(format!("{prefix}/bridge/#"), QoS::AtLeastOnce)
                    .await;
                // ...and per-device ones (`<prefix>/<id>/bridge/status`) -- a
                // different shape, see bridge_control.rs's doc comment.
                let _ = client
                    .subscribe(format!("{prefix}/+/bridge/status"), QoS::AtLeastOnce)
                    .await;
                // `<prefix>/<id>/forget/status` -- see device_control.rs.
                let _ = client
                    .subscribe(format!("{prefix}/+/forget/status"), QoS::AtLeastOnce)
                    .await;
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

/// Mirrors `rusthinq-cloud`'s `mqtt_client.rs::parse_mqtt_url` -- same small parse,
/// kept local since it's the only bit of that file this crate needs.
fn parse_mqtt_url(url: &str) -> Result<(String, u16, bool)> {
    let u = Url::parse(url).with_context(|| format!("parse mqtt_url {url}"))?;
    let host = u.host_str().unwrap_or("127.0.0.1").to_string();
    let use_tls = u.scheme() == "mqtts" || u.scheme() == "ssl";
    let port = u.port().unwrap_or(if use_tls { 8883 } else { 1883 });
    Ok((host, port, use_tls))
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
