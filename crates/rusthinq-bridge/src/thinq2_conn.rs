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
    /// Local→cloud uplink writes go through this instead of a direct
    /// `client.publish().await`. A per-message `tokio::spawn` around that await
    /// gives no ordering guarantee between messages sent close together (task B
    /// can complete its publish before task A if A's await point yields first);
    /// funneling every write through one channel drained by one task, mirroring
    /// ThinQ1's `Thinq1Handle` mpsc pattern, preserves call order on the wire.
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl Thinq2Handle {
    /// Queues `data` for upstream send and returns immediately (non-async, like
    /// `Thinq1Handle::send_from_local`) so a caller invoking this synchronously
    /// from within a local `on_data` callback preserves call order on the wire —
    /// the previous `tokio::spawn`-per-call version did not.
    pub fn send_from_local(&self, data: &[u8]) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        let hex_data = rusthinq_util::hex::encode_upper(data);
        rusthinq_core::logging::log("bridge", &[&format!("{} -> {hex_data}", self.device_id)]);
        let m = self.mid.fetch_add(1, Ordering::SeqCst) + 1;
        let payload = format_device_packet(m, &self.device_id, &self.model_name, &hex_data);
        let _ = self.write_tx.send(payload.into_bytes());
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
                            &[&format!(
                                "{did} could not publish pre-deploy to {prov_topic}: {e}"
                            )],
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

    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let client_w = client.clone();
    let pub_topic_w = state.pub_topic.clone();
    let stopped_w = stopped.clone();
    let did_w = device_id.to_string();
    tokio::spawn(async move {
        while let Some(payload) = write_rx.recv().await {
            if stopped_w.load(Ordering::SeqCst) {
                break;
            }
            if let Err(e) = client_w
                .publish(&pub_topic_w, QoS::AtMostOnce, false, payload)
                .await
            {
                rusthinq_core::logging::log(
                    "bridge",
                    &[&format!(
                        "{did_w} could not publish local data to {pub_topic_w}: {e}"
                    )],
                );
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
        write_tx,
    };
    Ok((handle, rx))
}

#[cfg(test)]
mod ordering_tests {
    use super::*;
    use crate::pair::Thinq2DeviceState;
    use bytes::BytesMut;
    use rcgen::{CertificateParams, KeyPair};
    use rumqttc::mqttbytes::Error as MqttError;
    use rumqttc::mqttbytes::v4::{ConnAck, Packet, SubAck};
    use rumqttc::{ConnectReturnCode, SubscribeReasonCode};
    use rustls::ServerConfig;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    /// A real self-signed cert/key pair (PEM), usable either as the fake
    /// broker's own identity or as a throwaway client identity — our fake
    /// broker never requests client auth, so the latter just needs to parse.
    fn self_signed_pem() -> (String, String) {
        let params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let key_pair = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        (cert.pem(), key_pair.serialize_pem())
    }

    fn test_tls_acceptor(cert_pem: &str, key_pem: &str) -> TlsAcceptor {
        let mut cert_reader = std::io::Cursor::new(cert_pem.as_bytes());
        let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut key_reader = std::io::Cursor::new(key_pem.as_bytes());
        let key = rustls_pemfile::private_key(&mut key_reader)
            .unwrap()
            .unwrap();
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
        TlsAcceptor::from(Arc::new(config))
    }

    /// A minimal MQTT 3.1.1-over-TLS broker: ConnAck/SubAck any Connect/Subscribe,
    /// and forward every Publish on `pub_topic` to `pub_tx` in the order received.
    async fn run_fake_broker(
        listener: TcpListener,
        acceptor: TlsAcceptor,
        pub_topic: String,
        pub_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut tls) = acceptor.accept(tcp).await else {
            return;
        };
        let mut buf = BytesMut::with_capacity(8192);
        let mut chunk = [0u8; 4096];
        loop {
            match tls.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
            loop {
                match Packet::read(&mut buf, 1024 * 1024) {
                    Ok(Packet::Connect(_)) => {
                        let mut out = BytesMut::new();
                        ConnAck::new(ConnectReturnCode::Success, false)
                            .write(&mut out)
                            .unwrap();
                        if tls.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                    Ok(Packet::Subscribe(sub)) => {
                        let mut out = BytesMut::new();
                        SubAck::new(
                            sub.pkid,
                            vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                        )
                        .write(&mut out)
                        .unwrap();
                        if tls.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                    Ok(Packet::Publish(p)) => {
                        if p.topic == pub_topic && pub_tx.send(p.payload.to_vec()).is_err() {
                            return;
                        }
                    }
                    Ok(Packet::PingReq) => {
                        if tls.write_all(&[0xD0, 0x00]).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(MqttError::InsufficientBytes(_)) => break,
                    Err(_) => return,
                }
            }
        }
    }

    /// Reproduces #24: `Thinq2Handle::send_from_local` used to be async and every
    /// caller (`lib.rs`'s ThinQ2 `on_data` handler) wrapped it in its own
    /// `tokio::spawn`, so nothing guaranteed messages reached the wire in the
    /// order the local device sent them. `send_from_local` is now synchronous and
    /// funnels writes through one channel drained by one task (mirroring
    /// `Thinq1Handle`), so calling it back-to-back — exactly how the local
    /// `on_data` callback invokes it — must publish in that same order.
    #[tokio::test]
    async fn local_uplink_writes_preserve_call_order() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (server_cert_pem, server_key_pem) = self_signed_pem();
        let (client_cert_pem, client_key_pem) = self_signed_pem();
        let acceptor = test_tls_acceptor(&server_cert_pem, &server_key_pem);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let pub_topic = "app/uplink".to_string();
        let (pub_tx, mut pub_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(run_fake_broker(
            listener,
            acceptor,
            pub_topic.clone(),
            pub_tx,
        ));

        let state = Thinq2DeviceState {
            country_code: "KR".to_string(),
            api_server: String::new(),
            mqtt_server: format!("mqtts://localhost:{port}"),
            ca_certificate: server_cert_pem,
            private_key: client_key_pem,
            certificate: client_cert_pem,
            pub_topic: pub_topic.clone(),
            prov_topic: "app/provisioning".to_string(),
            sub_topic: "app/downlink".to_string(),
            deploy_app_info: None,
            deploy_platform_info: None,
        };

        let (handle, _from_lg) = connect_thinq2(&state, "dev-order-test", "MODEL", None, None)
            .await
            .expect("connect must succeed");

        // Same call pattern as lib.rs's ThinQ2 on_data handler post-fix: plain,
        // synchronous, back-to-back calls with no spawn in between.
        const N: u8 = 30;
        for i in 0..N {
            handle.send_from_local(&[i]);
        }

        let mut received = Vec::new();
        for _ in 0..N {
            let payload = tokio::time::timeout(Duration::from_secs(5), pub_rx.recv())
                .await
                .expect("timed out waiting for publish")
                .expect("broker channel closed early");
            let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
            let data_hex = json["data"].as_str().unwrap();
            received.push(rusthinq_util::hex::decode(data_hex).unwrap()[0]);
        }

        let expected: Vec<u8> = (0..N).collect();
        assert_eq!(
            received, expected,
            "uplink publishes must reach the broker in the same order send_from_local was called"
        );
    }
}
