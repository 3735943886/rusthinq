//! ThinQ2 device acceptor — listens on the internal MQTT broker for CLIP traffic.

use super::provisioning::generate_deploy_response;
use crate::devmgr::{ConnectedDevice, DeviceManager, Platform, SendToDevice};
use crate::mqtt_broker::{Broker, PublishPacket};
use base64::Engine;
use chrono::{Datelike, Timelike};
use rusthinq_core::metadata::Metadata;
use rusthinq_util::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub struct DeviceAcceptor {
    broker: Arc<Broker>,
    manager: Arc<DeviceManager>,
    clients_by_id: Mutex<HashMap<String, u64>>,
    devices: Mutex<HashMap<u64, Arc<ConnectedDevice>>>,
}

impl DeviceAcceptor {
    pub fn new(broker: Arc<Broker>, manager: Arc<DeviceManager>) -> Arc<Self> {
        let acceptor = Arc::new(Self {
            broker: broker.clone(),
            manager,
            clients_by_id: Mutex::new(HashMap::new()),
            devices: Mutex::new(HashMap::new()),
        });

        let acc = acceptor.clone();
        broker.on_publish(Arc::new(move |packet, client| {
            let Some(client_id) = client else {
                rusthinq_core::logging::log(
                    "outgoing",
                    &[
                        &packet.topic,
                        &String::from_utf8_lossy(&packet.payload),
                        "retain:",
                        &packet.retain.to_string(),
                    ],
                );
                return;
            };
            rusthinq_core::logging::log(
                "incoming",
                &[
                    &packet.topic,
                    &String::from_utf8_lossy(&packet.payload),
                    "retain:",
                    &packet.retain.to_string(),
                ],
            );
            if !packet.topic.contains("clip/") {
                return;
            }
            let mut payload_bytes = packet.payload.clone();
            if payload_bytes.last() == Some(&0) {
                payload_bytes.pop();
            }
            let text = match String::from_utf8(payload_bytes) {
                Ok(t) => t,
                Err(_) => return,
            };
            let payload: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("clip parse error: {e}");
                    return;
                }
            };
            acc.handle_mqtt(&packet.topic, &payload, client_id);
        }));

        let acc2 = acceptor.clone();
        broker.on_disconnect(Arc::new(move |client_id| {
            acc2.disconnected(client_id);
        }));

        acceptor
    }

    /// `pub(crate)` so `sim_device.rs` can feed it simulated CLIP traffic directly,
    /// bypassing the broker/wire-protocol layer entirely — see its module doc.
    pub(crate) fn handle_mqtt(&self, topic: &str, payload: &serde_json::Value, client_id: u64) {
        let topic = if let Some(idx) = topic.find("/clip") {
            format!("clip{}", &topic[idx + 5..])
        } else if let Some(idx) = topic.find("clip/") {
            topic[idx..].to_string()
        } else {
            topic.to_string()
        };

        let did = payload
            .get("did")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let cmd = payload
            .get("cmd")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if topic == format!("clip/message/devices/{did}") {
            if cmd == "completeProvisioning_ack" {
                self.complete_provisioning(&did, payload, client_id);
            }
            if cmd == "device_packet" {
                let meta = self.broker.client_meta(client_id);
                if meta
                    .deploy_msg
                    .as_ref()
                    .and_then(|d| d.get("did"))
                    .and_then(|v| v.as_str())
                    == Some(did.as_str())
                    && let Some(dev) = self.devices.lock().get(&client_id).cloned()
                {
                    // Heal: reconnect race may have removed this id from DeviceManager
                    // while the MQTT session + acceptor still hold the device.
                    if self.manager.get(&did).is_none() {
                        rusthinq_core::logging::log(
                            "status",
                            &[&format!(
                                "device {did}: re-accepting into manager (was orphaned)"
                            )],
                        );
                        self.manager.accept(dev.clone());
                    }
                    if let Some(data) = payload.get("data").and_then(|d| d.as_str())
                        && let Ok(buf) = rusthinq_util::hex::decode(data)
                    {
                        dev.notify_data(&buf);
                    }
                }
            }
            if cmd == "req_timesync" {
                self.time_sync_request(client_id);
            }
        }

        if topic == format!("clip/provisioning/devices/{did}")
            && (cmd == "preDeploy" || cmd == "deploy")
        {
            let mut meta = self.broker.client_meta(client_id);
            meta.deploy_msg = Some(payload.clone());
            self.broker.set_client_meta(client_id, meta);
            let resp = generate_deploy_response(payload);
            self.broker.publish(
                PublishPacket {
                    topic: format!("lime/devices/{did}"),
                    payload: serde_json::to_vec(&resp).unwrap_or_default(),
                    retain: false,
                    qos: 0,
                    dup: false,
                },
                None,
            );
        }
    }

    fn complete_provisioning(&self, device_id: &str, _payload: &serde_json::Value, client_id: u64) {
        let meta_c = self.broker.client_meta(client_id);
        let Some(deploy) = meta_c.deploy_msg.clone() else {
            tracing::warn!("completeProvisioning_ack received without deploy/preDeploy");
            return;
        };
        // Same MQTT client already provisioned — only re-bind if manager lost the device
        // (e.g. pre-fix reconnect race). Otherwise ignore duplicate ack.
        if meta_c.has_device {
            if self.manager.get(device_id).is_some() && self.devices.lock().contains_key(&client_id)
            {
                return;
            }
            rusthinq_core::logging::log(
                "status",
                &[&format!(
                    "device {device_id}: completeProvisioning_ack on already-flagged client but missing from manager — re-registering"
                )],
            );
        }

        if let Some(old) = self.clients_by_id.lock().get(device_id).copied()
            && old != client_id
        {
            tracing::debug!("device {device_id} already connected, dropping the old one");
            self.broker.destroy_client(old);
        }
        self.clients_by_id
            .lock()
            .insert(device_id.to_string(), client_id);

        let model_id = deploy
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let model_name = deploy
            .pointer("/data/appInfo/modelName")
            .and_then(|v| v.as_str())
            .unwrap_or(&model_id)
            .to_string();
        let sw_version = deploy
            .pointer("/data/appInfo/softVer")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let device_type = deploy
            .pointer("/data/appInfo/DeviceType")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let meta = Metadata {
            model_id,
            model_name,
            device_type,
            sw_version,
        };

        let device_slot: Arc<Mutex<Option<Arc<ConnectedDevice>>>> = Arc::new(Mutex::new(None));
        let slot_emit = device_slot.clone();
        let emit = Arc::new(move |buf: Vec<u8>| {
            if let Some(dev) = slot_emit.lock().as_ref() {
                dev.notify_data(&buf);
            }
        });

        let broker = self.broker.clone();
        let did = device_id.to_string();
        let slot_send = device_slot.clone();
        let send_to = Arc::new(move |msg: SendToDevice| {
            if let Some(d) = slot_send.lock().as_ref() {
                d.notify_send(msg.clone());
            }
            let mid = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let messagestr = match msg {
                SendToDevice::T2Packet(buf) => serde_json::json!({
                    "did": did,
                    "mid": mid,
                    "cmd": "packet",
                    "type": 1,
                    "data": rusthinq_util::hex::encode(&buf),
                }),
                SendToDevice::T2Clip {
                    cmd,
                    msg_type,
                    data,
                } => serde_json::json!({
                    "did": did,
                    "mid": mid,
                    "cmd": cmd,
                    "type": msg_type,
                    "data": data,
                }),
                // Forwarded exactly as received — mid included, nothing rebuilt. Used
                // to relay a bridged cloud message (e.g. an ack) to the local device;
                // rebuilding it here the way the other branches do would strip the
                // mid the appliance needs to match it to its own pending message.
                SendToDevice::T2Raw(payload) => payload,
                SendToDevice::T1Json(_) => return,
            };
            broker.publish(
                PublishPacket {
                    topic: format!("lime/devices/{did}"),
                    payload: serde_json::to_vec(&messagestr).unwrap_or_default(),
                    retain: false,
                    qos: 0,
                    dup: false,
                },
                None,
            );
        });

        let dev =
            ConnectedDevice::new(device_id.to_string(), Platform::Thinq2, meta, emit, send_to);
        // The appliance's own deploy message — the LG bridge prefers this over its
        // placeholder appInfo/platformInfo when introducing the appliance upstream
        // (see rusthinq_bridge::pair::format_pre_deploy).
        if let (Some(app_info), Some(platform_info)) = (
            deploy.pointer("/data/appInfo"),
            deploy.pointer("/data/platformInfo"),
        ) {
            dev.set_deploy_info(app_info.clone(), platform_info.clone());
        }
        *device_slot.lock() = Some(dev.clone());

        let mut cm = self.broker.client_meta(client_id);
        cm.has_device = true;
        cm.device_id = Some(device_id.to_string());
        self.broker.set_client_meta(client_id, cm);

        self.devices.lock().insert(client_id, dev.clone());
        self.manager.accept(dev);
    }

    fn time_sync_request(&self, client_id: u64) {
        let meta = self.broker.client_meta(client_id);
        let Some(device_id) = meta
            .deploy_msg
            .as_ref()
            .and_then(|d| d.get("did"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        else {
            return;
        };
        let now = chrono::Utc::now();
        let mut buf = [0u8; 7];
        buf[0] = (now.year() % 100) as u8;
        buf[1] = now.month0() as u8;
        buf[2] = now.day() as u8;
        buf[3] = now.hour() as u8;
        buf[4] = now.minute() as u8;
        buf[5] = now.second() as u8;
        buf[6] = now.weekday().num_days_from_sunday() as u8;
        let mid = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let payload = serde_json::json!({
            "did": device_id,
            "mid": mid,
            "cmd": "resp_timesync",
            "type": 1,
            "data": base64::engine::general_purpose::STANDARD.encode(buf),
        });
        self.broker.publish(
            PublishPacket {
                topic: format!("lime/devices/{device_id}"),
                payload: serde_json::to_vec(&payload).unwrap_or_default(),
                retain: false,
                qos: 0,
                dup: false,
            },
            None,
        );
    }

    fn disconnected(&self, client_id: u64) {
        if let Some(dev) = self.devices.lock().remove(&client_id) {
            self.clients_by_id.lock().remove(&dev.id);
            dev.notify_close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
    use tokio::net::TcpListener;

    /// Real (not mocked) local-device flow: a real `rumqttc` client speaks the same
    /// wire protocol a physical appliance would to the real `Broker`/`DeviceAcceptor`,
    /// over a real loopback TCP socket.
    async fn connect_fake_device(port: u16, did: &str) -> AsyncClient {
        let mut opts = MqttOptions::new(format!("fake-{did}"), "127.0.0.1", port);
        opts.set_keep_alive(std::time::Duration::from_secs(30));
        let (client, mut eventloop) = AsyncClient::new(opts, 32);
        client
            .subscribe(format!("lime/devices/{did}"), QoS::AtMostOnce)
            .await
            .unwrap();
        tokio::spawn(async move {
            loop {
                if eventloop.poll().await.is_err() {
                    break;
                }
            }
        });
        client
    }

    /// `deploy`'s real appInfo/platformInfo must reach `ConnectedDevice::deploy_info`
    /// once provisioning completes — this is what lets the LG bridge introduce the
    /// appliance upstream as what it actually is instead of a fixed placeholder (see
    /// rusthinq_bridge::pair::format_pre_deploy).
    #[tokio::test]
    async fn complete_provisioning_captures_the_appliances_own_deploy_info() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = Arc::new(Broker::new());
        let manager = DeviceManager::new();
        let _acceptor = DeviceAcceptor::new(broker.clone(), manager.clone());
        {
            let broker = broker.clone();
            tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let broker = broker.clone();
                    tokio::spawn(async move { broker.accept_tcp(stream).await });
                }
            });
        }

        let did = "dev-deploy-1";
        let client = connect_fake_device(port, did).await;

        let app_info = serde_json::json!({
            "modelName": "RAC_056905_WW", "protocolVer": "7", "softVer": "1.2.3",
        });
        let platform_info = serde_json::json!({"provisioningKey": "RAC_056905_WW"});
        let deploy = serde_json::json!({
            "did": did, "mid": 1, "kind": "RAC_056905_WW", "cmd": "deploy", "type": 0,
            "data": { "appInfo": app_info, "platformInfo": platform_info },
        });
        client
            .publish(
                format!("clip/provisioning/devices/{did}"),
                QoS::AtMostOnce,
                false,
                deploy.to_string(),
            )
            .await
            .unwrap();

        let ack = serde_json::json!({
            "did": did, "mid": 2, "cmd": "completeProvisioning_ack", "type": 1,
        });
        client
            .publish(
                format!("clip/message/devices/{did}"),
                QoS::AtMostOnce,
                false,
                ack.to_string(),
            )
            .await
            .unwrap();

        let dev = wait_for(|| manager.get(did)).await;
        assert_eq!(
            dev.deploy_info.lock().clone(),
            Some((app_info, platform_info)),
        );
    }

    /// `SendToDevice::T2Raw` must reach the appliance's own MQTT topic byte-for-byte —
    /// this is the path the LG bridge uses to relay a cloud message (e.g. an ack) to
    /// the local device without rebuilding it and losing the cloud's own `mid`.
    #[tokio::test]
    async fn t2_raw_reaches_the_device_topic_unchanged() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = Arc::new(Broker::new());
        let manager = DeviceManager::new();
        let _acceptor = DeviceAcceptor::new(broker.clone(), manager.clone());
        {
            let broker = broker.clone();
            tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let broker = broker.clone();
                    tokio::spawn(async move { broker.accept_tcp(stream).await });
                }
            });
        }

        let did = "dev-raw-1";
        // `client` is the device's own connection (kept alive so DeviceAcceptor still
        // considers it connected); a second, independent client subscribes to the same
        // topic to observe what the device receives.
        let client = connect_fake_device(port, did).await;

        let mut opts = MqttOptions::new(format!("fake-{did}-reader"), "127.0.0.1", port);
        opts.set_keep_alive(std::time::Duration::from_secs(30));
        let (reader_client, mut reader_eventloop) = AsyncClient::new(opts, 32);
        let (publish_tx, mut publish_rx) = tokio::sync::mpsc::unbounded_channel();
        let (subacked_tx, subacked_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut subacked_tx = Some(subacked_tx);
            loop {
                match reader_eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::SubAck(_))) => {
                        if let Some(tx) = subacked_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let _ = publish_tx.send(p);
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });
        // Subscribed and confirmed live before anything is published — otherwise the
        // publish below can race ahead of the SUBSCRIBE this client hasn't sent yet
        // (rumqttc only sends what its own eventloop.poll() drives).
        reader_client
            .subscribe(format!("lime/devices/{did}"), QoS::AtMostOnce)
            .await
            .unwrap();
        subacked_rx.await.unwrap();

        client
            .publish(
                format!("clip/provisioning/devices/{did}"),
                QoS::AtMostOnce,
                false,
                serde_json::json!({
                    "did": did, "mid": 1, "kind": "RAC", "cmd": "deploy", "type": 0,
                    "data": {},
                })
                .to_string(),
            )
            .await
            .unwrap();
        client
            .publish(
                format!("clip/message/devices/{did}"),
                QoS::AtMostOnce,
                false,
                serde_json::json!({"did": did, "mid": 2, "cmd": "completeProvisioning_ack", "type": 1})
                    .to_string(),
            )
            .await
            .unwrap();

        let dev = wait_for(|| manager.get(did)).await;
        let ack = serde_json::json!({"did": did, "mid": 1785283454163_u64, "cmd": "ack", "type": 1, "data": "AA08F000C5043EBB"});
        (dev.send_to_device)(SendToDevice::T2Raw(ack.clone()));

        let received = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(p) = publish_rx.recv().await {
                if p.topic != format!("lime/devices/{did}") {
                    continue;
                }
                let v: serde_json::Value = serde_json::from_slice(&p.payload).unwrap();
                if v.get("cmd").and_then(|c| c.as_str()) == Some("ack") {
                    return v;
                }
            }
            panic!("publish channel closed before the ack arrived");
        })
        .await
        .expect("timed out waiting for the T2Raw publish");

        assert_eq!(
            received, ack,
            "the appliance must see the cloud's mid unchanged"
        );
    }

    async fn wait_for<T>(mut f: impl FnMut() -> Option<T>) -> T {
        for _ in 0..200 {
            if let Some(v) = f() {
                return v;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("condition never became true");
    }
}
