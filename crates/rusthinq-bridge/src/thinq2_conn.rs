//! ThinQ2 upstream MQTT connection to LG cloud (port of bridge/thinq2connection.ts).

use crate::pair::{
    Thinq2DeviceState, format_device_packet, format_pre_deploy, format_relayed_clip,
    is_relayable_cmd, parse_lg_packet_payload,
};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS, TlsConfiguration, Transport};
use rusthinq_util::backoff::ExponentialBackoff;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::sync::mpsc;

/// Cloneable handle: local→LG send + stop.
#[derive(Clone)]
pub struct Thinq2Handle {
    client: AsyncClient,
    mid: Arc<AtomicU32>,
    device_id: String,
    model_name: String,
    pub_topic: String,
    stopped: Arc<AtomicBool>,
}

impl Thinq2Handle {
    pub async fn send_from_local(&self, data: &[u8]) -> anyhow::Result<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return Ok(());
        }
        let hex_data = rusthinq_util::hex::encode_upper(data);
        rusthinq_core::logging::log("bridge", &[&format!("{} -> {hex_data}", self.device_id)]);
        let m = self.mid.fetch_add(1, Ordering::SeqCst) + 1;
        let payload = format_device_packet(m, &self.device_id, &self.model_name, &hex_data);
        self.client
            .publish(&self.pub_topic, QoS::AtMostOnce, false, payload)
            .await?;
        Ok(())
    }

    /// Put a message from the appliance in front of the real cloud as it stands —
    /// e.g. its `respUniversalCtrl` answer to a liveness check the cloud needs before
    /// it will offer a firmware update. The appliance's own fields are kept (an
    /// answer carries the cloud's `messageId`/`reqType` back, which is what the cloud
    /// correlates the answer on) and only the envelope this connection owns
    /// (mid/did/kind) is rewritten. Matches thinq2connection.ts's `Connection.sendClip()`.
    pub async fn send_clip(&self, payload: serde_json::Value) -> anyhow::Result<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return Ok(());
        }
        let cmd = payload
            .get("cmd")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !is_relayable_cmd(&cmd) {
            rusthinq_core::logging::log(
                "bridge",
                &[&format!("{} -> refused {cmd}", self.device_id)],
            );
            return Ok(());
        }
        rusthinq_core::logging::log("bridge", &[&format!("{} -> relay {cmd}", self.device_id)]);
        let m = self.mid.fetch_add(1, Ordering::SeqCst) + 1;
        let payload = format_relayed_clip(payload, m, &self.device_id, &self.model_name);
        self.client
            .publish(&self.pub_topic, QoS::AtLeastOnce, false, payload)
            .await?;
        Ok(())
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let c = self.client.clone();
        tokio::spawn(async move {
            let _ = c.disconnect().await;
        });
    }
}

/// Open LG MQTT; returns handle + channel of LG→local CLIP messages, each forwarded
/// exactly as the cloud sent it (see `is_relayable_cmd`).
///
/// `live_app_info`/`live_platform_info` are the physical appliance's own deploy
/// message, when the caller has one on hand — see `format_pre_deploy`.
pub async fn connect_thinq2(
    state: &Thinq2DeviceState,
    device_id: &str,
    model_name: &str,
    live_app_info: Option<serde_json::Value>,
    live_platform_info: Option<serde_json::Value>,
) -> anyhow::Result<(Thinq2Handle, mpsc::UnboundedReceiver<serde_json::Value>)> {
    let mqtt_url = state.mqtt_server.replace("ssl://", "mqtts://");
    let url = url::Url::parse(&mqtt_url)
        .or_else(|_| url::Url::parse(&format!("mqtts://{}", state.mqtt_server)))?;
    let host = url.host_str().unwrap_or("localhost").to_string();
    let port = url.port().unwrap_or(8883);

    rusthinq_core::logging::log(
        "bridge",
        &[&format!("{device_id} connecting to {}", state.mqtt_server)],
    );

    let mut opts = MqttOptions::new(device_id, host, port);
    opts.set_keep_alive(rusthinq_util::MQTT_KEEP_ALIVE);
    opts.set_transport(Transport::tls_with_config(TlsConfiguration::Simple {
        ca: state.ca_certificate.as_bytes().to_vec(),
        alpn: None,
        client_auth: Some((
            state.certificate.as_bytes().to_vec(),
            state.private_key.as_bytes().to_vec(),
        )),
    }));

    let (client, mut eventloop) = AsyncClient::new(opts, 32);
    let (tx, rx) = mpsc::unbounded_channel();
    let mid = Arc::new(AtomicU32::new(10000));
    let stopped = Arc::new(AtomicBool::new(false));

    let sub_topic = state.sub_topic.clone();
    let prov_topic = state.prov_topic.clone();
    let pub_topic = state.pub_topic.clone();
    let did = device_id.to_string();
    let model = model_name.to_string();
    let country = state.country_code.clone();
    let mid_c = mid.clone();
    let client_c = client.clone();
    let stopped_c = stopped.clone();

    let live_app_info_c = live_app_info.clone();
    let live_platform_info_c = live_platform_info.clone();

    tokio::spawn(async move {
        let mut backoff = ExponentialBackoff::for_external_upstream();
        loop {
            if stopped_c.load(Ordering::SeqCst) {
                break;
            }
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    backoff.reset();
                    rusthinq_core::logging::log("bridge", &[&format!("{did} connected")]);
                    // A subscribe failure here (AWS IoT refusing a topic the policy
                    // doesn't cover has happened in practice) must not leave this
                    // session silently half-open with no downlink and no retry.
                    // Disconnecting routes it through the same Err/Disconnect branches
                    // below, which already back off and reconnect.
                    if let Err(e) = client_c.subscribe(&sub_topic, QoS::AtLeastOnce).await {
                        rusthinq_core::logging::log(
                            "bridge",
                            &[&format!("{did} could not subscribe to {sub_topic}: {e}")],
                        );
                        let _ = client_c.disconnect().await;
                        continue;
                    }
                    let m = mid_c.fetch_add(1, Ordering::SeqCst) + 1;
                    let pre = format_pre_deploy(
                        m,
                        &did,
                        &model,
                        &country,
                        live_app_info_c.as_ref(),
                        live_platform_info_c.as_ref(),
                    );
                    // Same reasoning as the subscribe above: a publish failure here
                    // must not leave the session silently stuck never having
                    // introduced itself upstream, with nothing to retry it.
                    if let Err(e) = client_c
                        .publish(&prov_topic, QoS::AtLeastOnce, false, pre)
                        .await
                    {
                        rusthinq_core::logging::log(
                            "bridge",
                            &[&format!("{did} could not publish pre-deploy to {prov_topic}: {e}")],
                        );
                        let _ = client_c.disconnect().await;
                        continue;
                    }
                }
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic != sub_topic {
                        continue;
                    }
                    let Ok(text) = String::from_utf8(p.payload.to_vec()) else {
                        continue;
                    };
                    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&text) else {
                        continue;
                    };
                    let cmd = payload.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
                    if cmd == "completeProvisioning" {
                        let m = mid_c.fetch_add(1, Ordering::SeqCst) + 1;
                        let ack = serde_json::json!({
                            "mid": m, "did": did, "kind": model,
                            "cmd": "completeProvisioning_ack",
                            "rssi": -48, "fs": "idle", "data": null, "type": 1,
                        });
                        let _ = client_c
                            .publish(&pub_topic, QoS::AtMostOnce, false, ack.to_string())
                            .await;
                        continue;
                    }
                    // Everything else the cloud sends is carried to the appliance
                    // exactly as it arrived — including its own `mid`. Rebuilding the
                    // message here (as the local device's send() does for its own,
                    // fresh commands) would defeat an acknowledgement: the appliance
                    // matches an ack to its own pending message by mid, so an ack
                    // carrying a mid it never sent is worth no more than no ack at
                    // all — it just repeats the report until the cloud counts a real
                    // appliance resending an identical frame up to fifteen times.
                    if !is_relayable_cmd(cmd) {
                        rusthinq_core::logging::log(
                            "bridge",
                            &[&format!("{did} <- refused {cmd}")],
                        );
                        continue;
                    }
                    if let Some(buf) = parse_lg_packet_payload(&payload) {
                        rusthinq_core::logging::log(
                            "bridge",
                            &[&format!("{did} <- {}", rusthinq_util::hex::encode(&buf))],
                        );
                    } else {
                        rusthinq_core::logging::log("bridge", &[&format!("{did} <- relay {cmd}")]);
                    }
                    if tx.send(payload).is_err() {
                        break;
                    }
                }
                Ok(Event::Incoming(Incoming::Disconnect)) => {
                    // The broker closing the connection is not fatal — keep
                    // polling so rumqttc's own reconnect (same as the Err
                    // branch below) re-establishes it; ConnAck re-subscribes
                    // and re-sends pre-deploy once it does. A hard stop() is
                    // the only thing that should end this loop.
                    let delay = backoff.next_delay();
                    tracing::warn!("{did} disconnected, reconnecting in {delay:?}");
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    let delay = backoff.next_delay();
                    tracing::error!("{did} mqtt error: {e} (retrying in {delay:?})");
                    tokio::time::sleep(delay).await;
                }
                _ => {}
            }
        }
    });

    let handle = Thinq2Handle {
        client,
        mid,
        device_id: device_id.to_string(),
        model_name: model_name.to_string(),
        pub_topic: state.pub_topic.clone(),
        stopped,
    };
    Ok((handle, rx))
}
