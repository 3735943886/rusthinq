//! Real MQTT control-plane client via rumqttc, wired to MqttSink.
//!
//! Consumer-neutral, same as `MqttSink`/`MqttConnection` (see `rusthinq_core::mqtt`) —
//! nothing here assumes Home Assistant, or any other specific integration, is on the
//! other end. `emit_discovery()` (a generic "resync everything" hook) fires on every
//! `ConnAck`, i.e. whenever *this* connection comes up; nothing here also watches for
//! some downstream integration's own restart signal (HA's birth message, or anything
//! else) — a script that wants that can subscribe to whatever topic it needs and
//! trigger its own resync.

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, Incoming, LastWill, MqttOptions, QoS, Transport};
use rusthinq_core::mqtt::MqttSink;
use rusthinq_util::backoff::ExponentialBackoff;
use std::sync::Arc;
use tokio::sync::mpsc;
use url::Url;

pub async fn start_mqtt_client(sink: Arc<MqttSink>) -> Result<()> {
    let cfg = sink.config.clone();
    let (host, port, use_tls) = parse_mqtt_url(&cfg.mqtt_url)?;
    let mut opts = MqttOptions::new("rusthinq-cloud", host, port);
    opts.set_keep_alive(rusthinq_util::MQTT_KEEP_ALIVE);
    if !cfg.mqtt_user.is_empty() {
        opts.set_credentials(&cfg.mqtt_user, &cfg.mqtt_pass);
    }
    let will = LastWill::new(
        format!("{}/availability", cfg.rusthinq_prefix),
        b"offline".to_vec(),
        QoS::AtLeastOnce,
        true,
    );
    opts.set_last_will(will);
    if use_tls {
        // System CA roots, no client cert — same transport path as thinq2_conn.
        opts.set_transport(Transport::tls_with_default_config());
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    // Single ordered publisher task: preserves retain order for discovery bursts
    // and surfaces publish errors instead of fire-and-forget spawns.
    let (pub_tx, mut pub_rx) = mpsc::unbounded_channel::<(String, Vec<u8>, bool)>();
    let client_pub = client.clone();
    tokio::spawn(async move {
        while let Some((topic, payload, retain)) = pub_rx.recv().await {
            if let Err(e) = client_pub
                .publish(&topic, QoS::AtLeastOnce, retain, payload)
                .await
            {
                tracing::warn!(
                    target: "rusthinq_mqtt",
                    %topic,
                    error = %e,
                    "MQTT publish failed"
                );
            }
        }
    });

    sink.set_publish_fn(move |topic, payload, retain| {
        if pub_tx
            .send((topic.to_string(), payload.to_vec(), retain))
            .is_err()
        {
            tracing::warn!(
                target: "rusthinq_mqtt",
                %topic,
                "MQTT publisher channel closed; dropping publish"
            );
        }
    });

    let prefix = cfg.rusthinq_prefix.clone();
    let raw_prefix = cfg.raw_prefix.clone();
    let sink2 = sink.clone();

    tokio::spawn(async move {
        let mut backoff = ExponentialBackoff::for_local_control_plane();
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    backoff.reset();
                    *sink2.connected.lock() = true;
                    rusthinq_core::logging::log("status", &["MQTT connection established"]);
                    sink2.published_availability.lock().clear();
                    let _ = client
                        .subscribe(format!("{prefix}/+/+/set"), QoS::AtLeastOnce)
                        .await;
                    let _ = client
                        .subscribe(format!("{prefix}/+/+/+/set"), QoS::AtLeastOnce)
                        .await;
                    let _ = client
                        .subscribe(format!("{prefix}/+/availability"), QoS::AtLeastOnce)
                        .await;
                    // raw_bus's inject/emit topics (<raw_prefix>/<id>/raw/inject|emit/set) live
                    // under a separate prefix from the rest of the control plane - see
                    // raw_bus.rs's doc comment on register_inject. Without this, publishing to
                    // raw_prefix is a silent no-op: nothing here is subscribed to receive it, so
                    // the broker just drops it - which is exactly what made the rethink-TS-adapter
                    // setup's capability queries time out forever while rx (a publish, not a
                    // subscription) kept working fine.
                    if let Some(raw_prefix) = &raw_prefix {
                        if raw_prefix != &prefix {
                            let _ = client
                                .subscribe(format!("{raw_prefix}/+/+/set"), QoS::AtLeastOnce)
                                .await;
                            let _ = client
                                .subscribe(format!("{raw_prefix}/+/+/+/set"), QoS::AtLeastOnce)
                                .await;
                        }
                    }
                    let _ = client
                        .publish(
                            format!("{prefix}/availability"),
                            QoS::AtLeastOnce,
                            true,
                            b"online".to_vec(),
                        )
                        .await;
                    sink2.emit_discovery();
                }
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    sink2.handle_message(&p.topic, &p.payload, p.retain);
                }
                Ok(Event::Incoming(Incoming::Disconnect)) => {
                    *sink2.connected.lock() = false;
                    tracing::warn!("MQTT connection lost");
                }
                Ok(_) => {}
                Err(e) => {
                    *sink2.connected.lock() = false;
                    let delay = backoff.next_delay();
                    tracing::error!("MQTT error: {e} (retrying in {delay:?})");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    });

    Ok(())
}

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
        let (host, port, tls) = parse_mqtt_url("mqtt://ha.local:1883").unwrap();
        assert_eq!(host, "ha.local");
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

    #[test]
    fn parse_ssl_scheme() {
        let (_, port, tls) = parse_mqtt_url("ssl://10.0.0.1:8884").unwrap();
        assert_eq!(port, 8884);
        assert!(tls);
    }
}
