//! ThinQ2 device acceptor — listens on the internal MQTT broker for CLIP traffic.

use super::firmware::FirmwareHosts;
use super::now_millis;
use super::provisioning::generate_deploy_response;
use crate::devmgr::{ConnectedDevice, DeferredSlot, DeviceManager, Platform, SendToDevice};
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
    firmware_hosts: Arc<FirmwareHosts>,
    clients_by_id: Mutex<HashMap<String, u64>>,
    devices: Mutex<HashMap<u64, Arc<ConnectedDevice>>>,
}

impl DeviceAcceptor {
    pub fn new(
        broker: Arc<Broker>,
        manager: Arc<DeviceManager>,
        firmware_hosts: Arc<FirmwareHosts>,
    ) -> Arc<Self> {
        let acceptor = Arc::new(Self {
            broker: broker.clone(),
            manager,
            firmware_hosts,
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
        // rethink's TS counterpart does this with `topic.replace(/^.*\/clip/, 'clip')` —
        // a greedy regex, so it pivots on the *last* "/clip". A topic like AWS IoT's rule
        // republish path `$aws/rules/clip_provisioning_rule/clip/provisioning/devices/<id>`
        // contains "/clip" twice (once inside the rule name itself); pivoting on the first
        // one leaves the rule-name fragment attached and the result never matches
        // `clip/provisioning/devices/<id>` below, so devices retrying over that AWS-style
        // topic never get a response and loop forever. `rfind` matches the greedy regex.
        let topic = if let Some(idx) = topic.rfind("/clip") {
            format!("clip{}", &topic[idx + 5..])
        } else if let Some(idx) = topic.rfind("clip/") {
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
                let deploy_did_matches = meta
                    .deploy_msg
                    .as_ref()
                    .and_then(|d| d.get("did"))
                    .and_then(|v| v.as_str())
                    == Some(did.as_str());
                // Some RTL CLIP firmware (e.g. T17A1EFHU_F) never sends
                // completeProvisioning_ack after the completeProvisioning response —
                // it just starts publishing device_packets. Register on the first
                // packet instead of waiting for an ack that will never arrive.
                if deploy_did_matches && self.devices.lock().get(&client_id).is_none() {
                    self.complete_provisioning(&did, payload, client_id);
                }
                if deploy_did_matches
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
            // The appliance's side of the same gap this topic used to have: anything
            // it publishes that isn't one of the three cmds above (e.g. its
            // respUniversalCtrl answer to a reqUniversalCtrl liveness check, which
            // gates whether the cloud will even offer a firmware update) used to be
            // dropped here without a trace. Hand it to whoever's listening — the LG
            // bridge, so it can carry the answer upstream — instead.
            if !matches!(
                cmd.as_str(),
                "completeProvisioning_ack" | "device_packet" | "req_timesync"
            ) && let Some(dev) = self.devices.lock().get(&client_id).cloned()
                && dev.id == did
            {
                dev.notify_unhandled_clip(payload.clone());
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
                PublishPacket::new(
                    format!("lime/devices/{did}"),
                    serde_json::to_vec(&resp).unwrap_or_default(),
                ),
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

        let device_slot: DeferredSlot<Arc<ConnectedDevice>> = DeferredSlot::new();
        let slot_emit = device_slot.clone();
        let emit = Arc::new(move |buf: Vec<u8>| {
            if let Some(dev) = slot_emit.get() {
                dev.notify_data(&buf);
            }
        });

        let broker = self.broker.clone();
        let did = device_id.to_string();
        let slot_send = device_slot.clone();
        let send_to = Arc::new(move |msg: SendToDevice| {
            if let Some(d) = slot_send.get() {
                d.notify_send(msg.clone());
            }
            let mid = now_millis();
            let messagestr = match msg {
                SendToDevice::T2Packet(buf) => serde_json::json!({
                    "did": did,
                    "mid": mid,
                    "cmd": "packet",
                    "type": 1,
                    // Some firmware (e.g. F_C__Y___W.A__QEUK) only accepts uppercase hex
                    // in the packet payload and silently ignores lowercase.
                    "data": rusthinq_util::hex::encode_upper(&buf),
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
                PublishPacket::new(
                    format!("lime/devices/{did}"),
                    serde_json::to_vec(&messagestr).unwrap_or_default(),
                ),
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
            // This device just completed a full local TLS+CLIP handshake, proving it
            // trusts rusthinq's CA — immunize the core cloud endpoints it reports for
            // itself against ever being misclassified as a firmware/SOTA host by an
            // unrelated caller's rejected handshake. Walk the whole `data` object, not
            // just `appInfo`: api-server/mqtt-server/https-server actually live under
            // the sibling `extraProfile` (confirmed against a live capture), and
            // nothing here should have to assume which sub-object a future field like
            // this lands under either. See
            // thinq2::firmware::FirmwareHosts::confirm_local_urls_in.
            if let Some(data) = deploy.pointer("/data") {
                self.firmware_hosts.confirm_local_urls_in(data);
            }
        }
        device_slot.set(dev.clone());

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
        let payload = serde_json::json!({
            "did": device_id,
            "mid": now_millis(),
            "cmd": "resp_timesync",
            "type": 1,
            "data": base64::engine::general_purpose::STANDARD.encode(buf),
        });
        self.broker.publish(
            PublishPacket::new(
                format!("lime/devices/{device_id}"),
                serde_json::to_vec(&payload).unwrap_or_default(),
            ),
            None,
        );
    }

    fn disconnected(&self, client_id: u64) {
        if let Some(dev) = self.devices.lock().remove(&client_id) {
            // Only remove the id -> client mapping if it still points at *this*
            // client: a fast reconnect for the same device id can already have
            // rebound it to the replacement client via complete_provisioning
            // before this stale disconnect fires (async, after TCP teardown
            // propagates) -- removing unconditionally would delete the
            // *replacement's* live entry instead, leaking it (nothing would then
            // recognize a third reconnect needs to destroy_client() that
            // still-open replacement). Same identity-before-removing guard as
            // DeviceManager::accept's close handler uses for `devices`.
            let mut clients_by_id = self.clients_by_id.lock();
            if clients_by_id.get(&dev.id) == Some(&client_id) {
                clients_by_id.remove(&dev.id);
            }
            drop(clients_by_id);
            dev.notify_close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::wait_for;
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

    /// A second `rumqttc` client subscribed to `topic`, confirmed live (SUBACK seen)
    /// before returning -- a publish made right after this call would otherwise be
    /// able to race ahead of the SUBSCRIBE this client hasn't sent yet (rumqttc only
    /// sends what its own eventloop.poll() drives).
    async fn subscribed_reader(
        port: u16,
        reader_id: &str,
        topic: impl Into<String>,
    ) -> (AsyncClient, tokio::sync::mpsc::UnboundedReceiver<rumqttc::Publish>) {
        let mut opts = MqttOptions::new(format!("fake-{reader_id}-reader"), "127.0.0.1", port);
        opts.set_keep_alive(std::time::Duration::from_secs(30));
        let (reader_client, mut reader_eventloop) = AsyncClient::new(opts, 32);
        let (publish_tx, publish_rx) = tokio::sync::mpsc::unbounded_channel();
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
        reader_client
            .subscribe(topic, QoS::AtMostOnce)
            .await
            .unwrap();
        subacked_rx.await.unwrap();
        (reader_client, publish_rx)
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
        let _acceptor = DeviceAcceptor::new(broker.clone(), manager.clone(), Arc::new(FirmwareHosts::new()));
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

    /// The regression this test exists for: a bridged appliance could never take a
    /// firmware update because its answer to the cloud's liveness check
    /// (`respUniversalCtrl`, answering `reqUniversalCtrl`) was silently dropped here —
    /// only `completeProvisioning_ack`/`device_packet`/`req_timesync` had a handler,
    /// so from the cloud's side the appliance never answered and looked offline.
    #[tokio::test]
    async fn unhandled_clip_cmds_reach_the_devices_handler() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = Arc::new(Broker::new());
        let manager = DeviceManager::new();
        let _acceptor =
            DeviceAcceptor::new(broker.clone(), manager.clone(), Arc::new(FirmwareHosts::new()));
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

        let did = "dev-unhandled-clip-1";
        let client = connect_fake_device(port, did).await;

        let deploy = serde_json::json!({
            "did": did, "mid": 1, "kind": "RAC_056905_WW", "cmd": "deploy", "type": 0,
            "data": { "appInfo": {"modelName": "RAC_056905_WW"}, "platformInfo": {} },
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
        let ack = serde_json::json!({"did": did, "mid": 2, "cmd": "completeProvisioning_ack", "type": 1});
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

        let received: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let r = received.clone();
        dev.add_unhandled_clip_handler(move |payload| r.lock().push(payload));

        // The three cmds this topic already has a handler for must NOT also reach
        // on_unhandled_clip.
        for handled in [
            serde_json::json!({"did": did, "mid": 3, "cmd": "req_timesync", "type": 1}),
            serde_json::json!({"did": did, "mid": 4, "cmd": "device_packet", "type": 1, "data": "aa"}),
        ] {
            client
                .publish(
                    format!("clip/message/devices/{did}"),
                    QoS::AtMostOnce,
                    false,
                    handled.to_string(),
                )
                .await
                .unwrap();
        }

        // An unhandled cmd — the appliance's answer to a relayed liveness check — must.
        let resp = serde_json::json!({
            "did": did, "mid": 5, "cmd": "respUniversalCtrl", "type": 1,
            "data": {"reqType": "online_check", "messageId": "m-1", "responseCode": "0000"},
        });
        client
            .publish(
                format!("clip/message/devices/{did}"),
                QoS::AtMostOnce,
                false,
                resp.to_string(),
            )
            .await
            .unwrap();

        wait_for(|| (!received.lock().is_empty()).then_some(())).await;
        let got = received.lock().clone();
        assert_eq!(got.len(), 1, "only the unhandled cmd should have reached the handler");
        assert_eq!(got[0]["cmd"], "respUniversalCtrl");
    }

    /// The regression this test exists for: some RTL CLIP firmware (e.g.
    /// T17A1EFHU_F) never sends `completeProvisioning_ack` after the
    /// `completeProvisioning` response — it just starts publishing
    /// `device_packet` messages. Without a fallback, those packets were dropped
    /// forever (nothing was registered in `self.devices` to receive them) and the
    /// appliance eventually got undeployed.
    #[tokio::test]
    async fn device_packet_registers_a_device_that_never_acked_provisioning() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = Arc::new(Broker::new());
        let manager = DeviceManager::new();
        let _acceptor = DeviceAcceptor::new(broker.clone(), manager.clone(), Arc::new(FirmwareHosts::new()));
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

        let did = "dev-no-ack-1";
        let client = connect_fake_device(port, did).await;

        let deploy = serde_json::json!({
            "did": did, "mid": 1, "kind": "T17A1EFHU_F", "cmd": "deploy", "type": 0,
            "data": { "appInfo": {"modelName": "T17A1EFHU_F"}, "platformInfo": {} },
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

        // No completeProvisioning_ack here — go straight to a device_packet, like
        // the buggy firmware does.
        let packet = serde_json::json!({
            "did": did, "mid": 2, "cmd": "device_packet", "type": 1,
            "data": rusthinq_util::hex::encode([0xAA, 0xBB]),
        });
        client
            .publish(
                format!("clip/message/devices/{did}"),
                QoS::AtMostOnce,
                false,
                packet.to_string(),
            )
            .await
            .unwrap();

        let dev = wait_for(|| manager.get(did)).await;
        assert_eq!(dev.meta.model_id, "T17A1EFHU_F");

        let received: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let received_c = received.clone();
        dev.add_data_handler(move |buf| {
            *received_c.lock() = Some(buf.to_vec());
        });
        let packet2 = serde_json::json!({
            "did": did, "mid": 3, "cmd": "device_packet", "type": 1,
            "data": rusthinq_util::hex::encode([0xCC, 0xDD]),
        });
        client
            .publish(
                format!("clip/message/devices/{did}"),
                QoS::AtMostOnce,
                false,
                packet2.to_string(),
            )
            .await
            .unwrap();
        wait_for(|| received.lock().clone()).await;
        assert_eq!(received.lock().clone(), Some(vec![0xCC, 0xDD]));
    }

    /// The regression this test exists for: a real appliance's `api-server`/
    /// `mqtt-server`/`https-server` fields live under `data.extraProfile`, a sibling
    /// of `appInfo` — not inside `appInfo` itself. An earlier version of this wiring
    /// only walked `appInfo`, so `confirm_local_urls_in` silently never found them:
    /// 658 successful provisionings in a row never immunized kic-common.lgthinq.com,
    /// and one unrelated rejected handshake was free to misroute it for everyone.
    /// Shaped after a live capture, not a synthetic minimal payload, specifically so
    /// this would have caught that.
    #[tokio::test]
    async fn complete_provisioning_immunizes_the_appliances_extra_profile_hosts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = Arc::new(Broker::new());
        let manager = DeviceManager::new();
        let firmware_hosts = Arc::new(FirmwareHosts::new());
        let _acceptor = DeviceAcceptor::new(broker.clone(), manager.clone(), firmware_hosts.clone());
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

        let did = "dev-deploy-2";
        let client = connect_fake_device(port, did).await;

        let deploy = serde_json::json!({
            "did": did, "mid": 1, "kind": "2RSFL2DBN3K_Z", "cmd": "deploy", "type": 0,
            "data": {
                "appInfo": { "modelName": "2RSFL2DBN3K_Z", "protocolVer": "7" },
                "platformInfo": { "provisioningKey": "2RSFL2DBN3K_Z" },
                "extraProfile": {
                    "bootId": "123",
                    "api-server": "https://kic-common.lgthinq.com:443",
                    "mqtt-server": "ssl://common.iot.kic.lgthinq.com:8883",
                    "https-server": "https://kic-mclip.lgthinq.com:443",
                },
            },
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

        wait_for(|| manager.get(did)).await;

        // Immunized means never routed to passthrough in the first place — `has`
        // (which answers "should this be routed away?") is false for all three right
        // now, same as before any of this ran.
        assert!(!firmware_hosts.has("kic-common.lgthinq.com"));
        assert!(!firmware_hosts.has("common.iot.kic.lgthinq.com"));
        assert!(!firmware_hosts.has("kic-mclip.lgthinq.com"));

        // The real proof: an unrelated caller's rejected handshake (exactly what
        // happened in production) must not be able to touch it after this.
        firmware_hosts.note("https://kic-common.lgthinq.com/route");
        assert!(!firmware_hosts.has("kic-common.lgthinq.com"));
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
        let _acceptor = DeviceAcceptor::new(broker.clone(), manager.clone(), Arc::new(FirmwareHosts::new()));
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

        let (_reader_client, mut publish_rx) =
            subscribed_reader(port, did, format!("lime/devices/{did}")).await;

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

    /// A real appliance retrying `deploy` sometimes publishes on the AWS IoT rule
    /// republish path (`$aws/rules/<rule-name>/clip/...`) rather than the plain
    /// `clip/...` topic — seen on a real WBEY3GT cooktop and a real 2RSFL2DBN3K_Z
    /// sensor, both looping forever because the old topic normalization (pivoting on
    /// the *first* "/clip") mis-parsed the rule-name's own embedded "/clip" and never
    /// matched `clip/provisioning/devices/<id>`, so no `completeProvisioning` response
    /// was ever sent. Must still get answered.
    #[tokio::test]
    async fn deploy_on_an_aws_rule_republish_topic_still_gets_a_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = Arc::new(Broker::new());
        let manager = DeviceManager::new();
        let _acceptor = DeviceAcceptor::new(broker.clone(), manager.clone(), Arc::new(FirmwareHosts::new()));
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

        let did = "dev-aws-rule-1";
        let client = connect_fake_device(port, did).await;

        let (_reader_client, mut publish_rx) =
            subscribed_reader(port, did, format!("lime/devices/{did}")).await;

        client
            .publish(
                format!("$aws/rules/clip_provisioning_rule/clip/provisioning/devices/{did}"),
                QoS::AtMostOnce,
                false,
                serde_json::json!({
                    "did": did, "mid": 1, "kind": "WBEY3GT", "cmd": "deploy", "type": 0,
                    "data": {},
                })
                .to_string(),
            )
            .await
            .unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(p) = publish_rx.recv().await {
                if p.topic != format!("lime/devices/{did}") {
                    continue;
                }
                let v: serde_json::Value = serde_json::from_slice(&p.payload).unwrap();
                if v.get("cmd").and_then(|c| c.as_str()) == Some("completeProvisioning") {
                    return v;
                }
            }
            panic!("publish channel closed before the completeProvisioning response arrived");
        })
        .await
        .expect(
            "timed out waiting for a completeProvisioning response to a deploy \
             published on an AWS rule republish topic",
        );

        assert_eq!(
            received.get("did").and_then(|v| v.as_str()),
            Some(did)
        );
    }

    async fn complete_provisioning_for(port: u16, did: &str) {
        let client = connect_fake_device(port, did).await;
        let deploy = serde_json::json!({
            "did": did, "mid": 1, "kind": "MODEL", "cmd": "deploy", "type": 0,
            "data": { "appInfo": {}, "platformInfo": {} },
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
    }

    /// The exact race #23 was filed for: a fast reconnect for the same device id
    /// correctly rebinds `clients_by_id[did]` to the new client via
    /// `complete_provisioning` (which also `destroy_client()`s the old one), but the
    /// *old* client's disconnect event fires asynchronously, after its socket is
    /// actually torn down -- arriving *after* the rebind. `disconnected` used to
    /// delete `clients_by_id[did]` unconditionally on any client's disconnect,
    /// wiping the replacement's brand new live entry even though the replacement
    /// never disconnected. Nothing then recognizes a third reconnect needs to
    /// `destroy_client()` that still-open replacement -- a leaked session.
    #[tokio::test]
    async fn a_superseded_clients_stale_disconnect_does_not_evict_the_replacement() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = Arc::new(Broker::new());
        let manager = DeviceManager::new();
        let acceptor =
            DeviceAcceptor::new(broker.clone(), manager.clone(), Arc::new(FirmwareHosts::new()));
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

        let did = "dev-reconnect-race";

        complete_provisioning_for(port, did).await;
        let client_id_a = wait_for(|| acceptor.clients_by_id.lock().get(did).copied()).await;

        // A fast reconnect: a second client for the same device id completes
        // provisioning (rebinding clients_by_id[did] and destroy_client()-ing A)
        // before A's own disconnect has necessarily been processed yet.
        complete_provisioning_for(port, did).await;
        let client_id_b = wait_for(|| {
            acceptor
                .clients_by_id
                .lock()
                .get(did)
                .copied()
                .filter(|id| *id != client_id_a)
        })
        .await;
        assert_ne!(client_id_a, client_id_b);

        // A's socket was closed by the destroy_client(old) call inside the second
        // complete_provisioning above; give its disconnect event, which fires
        // asynchronously, every chance to arrive and be processed.
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert_eq!(
            acceptor.clients_by_id.lock().get(did).copied(),
            Some(client_id_b),
            "the replacement client's live entry must survive the superseded \
             client's stale disconnect"
        );
    }
}
