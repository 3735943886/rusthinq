//! Raw wire-frame MQTT bus — the "observer" pattern shared with zigbang/smartrelay: a
//! live rx/tx frame tap plus injection, over the same MQTT connection rusthinq already
//! holds open. Not retained — it's a live tail, not a history log. Meant for local
//! trusted tooling (rusthinq-mcp among them), not for driving any specific downstream
//! integration's entities: only wired up at all
//! when `config.mqtt.raw_prefix` is set — see its doc comment — and even then, run it
//! only where that connection is otherwise firewalled/trusted, the same assumption
//! smartrelay's own `observer` listener makes.
//!
//! Topics (`<prefix>` = `config.mqtt.raw_prefix`):
//!   - `<prefix>/<id>/raw/rx` (published) — hex of each frame received from the device
//!   - `<prefix>/<id>/raw/tx` (published) — hex (or JSON for CLIP commands) of each frame sent to it
//!   - `<prefix>/<id>/raw/inject/set` (subscribed) — hex frame to send to the device
//!   - `<prefix>/<id>/raw/emit/set` (subscribed) — hex frame to inject as if received from the device
//!
//! `attach()` runs unconditionally for every connected device once `raw_prefix` is set,
//! with zero awareness of whether `registry.rs` also gave that device's model a
//! script/native handler — there is no priority between the two, both just run in
//! parallel on every frame. Deliberate coexistence (debugging a scripted device's wire
//! traffic, testing a command via inject before adding it to the script) is exactly
//! what this bus is for and is fine. What isn't fine, and isn't checked anywhere in
//! code, is a model where this bus is another process's *only* data source (see
//! config.toml's `[devices]` comment — the rethink-TS-adapter setup) also getting a
//! `<modelId>.rhai`: that's two independent full drivers for one device, and nothing
//! here will warn you.

use crate::devmgr::{ConnectedDevice, DeviceManager, SendToDevice};
use rusthinq_core::mqtt::{MqttConnection, MqttSink};
use serde_json::json;
use std::sync::Arc;

fn send_to_device_payload(msg: &SendToDevice) -> String {
    match msg {
        SendToDevice::T2Packet(b) => rusthinq_util::hex::encode(b),
        SendToDevice::T2Clip {
            cmd,
            msg_type,
            data,
        } => json!({ "cmd": cmd, "type": msg_type, "data": data }).to_string(),
        SendToDevice::T2Raw(v) => v.to_string(),
        SendToDevice::T1Json(v) => v.to_string(),
    }
}

/// Publish every rx/tx frame for `dev` onto the raw bus. Call once per connected
/// device, e.g. from [`DeviceManager::on_new_device`] — only when `raw_prefix` is set;
/// there is nothing to attach when it isn't.
pub fn attach(mqtt: &Arc<dyn MqttConnection>, dev: &ConnectedDevice, raw_prefix: &str) {
    let id = dev.id.clone();
    let mqtt_rx = mqtt.clone();
    let prefix_rx = raw_prefix.to_string();
    dev.add_data_handler(move |buf| {
        mqtt_rx.publish_raw(
            &format!("{prefix_rx}/{id}/raw/rx"),
            rusthinq_util::hex::encode(buf).as_bytes(),
            false,
        );
    });

    let id2 = dev.id.clone();
    let mqtt_tx = mqtt.clone();
    let prefix_tx = raw_prefix.to_string();
    dev.add_send_handler(move |msg| {
        mqtt_tx.publish_raw(
            &format!("{prefix_tx}/{id2}/raw/tx"),
            send_to_device_payload(&msg).as_bytes(),
            false,
        );
    });
}

/// Register the inject/emit handlers once, globally, on `sink` — routes
/// `<raw_prefix>/<id>/raw/inject/set` and `<raw_prefix>/<id>/raw/emit/set` to the
/// matching connected device via `manager`. `MqttSink::handle_message` already tries
/// both `rusthinq_prefix` and `raw_prefix`, so nothing here needs to know which one
/// matched. Silently ignored for an unknown id or bad hex: this is a debugging tap,
/// not a control plane that reports errors back to the publisher. Only call this when
/// `config.mqtt.raw_prefix` is set.
///
/// `raw/inject-clip/set` is the named-CLIP-command counterpart to `raw/inject` (raw
/// bytes only, per `SendToDevice::T2Packet`) — some commands, like the
/// rethink-TS-adapter's one-shot `setMaskingInfo`, aren't a TLV/AABB packet at all,
/// they're a `{cmd, type, data}` JSON CLIP envelope (`SendToDevice::T2Clip`), which
/// `raw/inject` has no way to carry. Payload is JSON, not hex; malformed JSON or a
/// missing `cmd` is silently ignored, same policy as bad hex above.
pub fn register_inject(sink: &MqttSink, manager: Arc<DeviceManager>) {
    sink.on_set_property(move |id, prop, value| {
        let Some(dev) = manager.get(id) else { return };
        match prop {
            "raw/inject" => {
                let Ok(buf) = rusthinq_util::hex::decode(value) else {
                    return;
                };
                (dev.send_to_device)(SendToDevice::T2Packet(buf));
            }
            "raw/emit" => {
                let Ok(buf) = rusthinq_util::hex::decode(value) else {
                    return;
                };
                (dev.emit_data)(buf);
            }
            "raw/inject-clip" => {
                let Ok(parsed) = serde_json::from_str::<serde_json::Value>(value) else {
                    return;
                };
                let Some(cmd) = parsed.get("cmd").and_then(|v| v.as_str()) else {
                    return;
                };
                let msg_type = parsed.get("type").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let data = parsed
                    .get("data")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                (dev.send_to_device)(SendToDevice::T2Clip {
                    cmd: cmd.to_string(),
                    msg_type,
                    data,
                });
            }
            _ => {}
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::devmgr::Platform;
    use rusthinq_core::MockMqttConnection;
    use rusthinq_core::metadata::Metadata;

    fn dummy_dev(id: &str) -> Arc<ConnectedDevice> {
        ConnectedDevice::new(
            id.into(),
            Platform::Thinq2,
            Metadata {
                model_id: "RAC_056905_WW".into(),
                model_name: "RAC".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(|_m| {}),
        )
    }

    fn test_mqtt_config() -> rusthinq_core::config::MqttConfig {
        rusthinq_core::config::MqttConfig {
            mqtt_url: "mqtt://127.0.0.1:1883".into(),
            rusthinq_prefix: "rusthinq".into(),
            mqtt_user: String::new(),
            mqtt_pass: String::new(),
            raw_prefix: Some("rusthinq-raw".into()),
            state_file: None,
        }
    }

    #[test]
    fn attach_publishes_rx_frames_as_hex_under_the_raw_prefix() {
        let mqtt = MockMqttConnection::new();
        let dev = dummy_dev("dev-1");
        attach(
            &(mqtt.clone() as Arc<dyn MqttConnection>),
            &dev,
            "rusthinq-raw",
        );

        dev.notify_data(&[0xaa, 0x01, 0x02, 0xbb]);

        let raw = mqtt.raw_publishes();
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0].0, "rusthinq-raw/dev-1/raw/rx");
        assert_eq!(raw[0].1, b"aa0102bb");
        assert!(!raw[0].2, "raw/rx must not be retained");
    }

    #[test]
    fn attach_publishes_tx_packet_as_hex_and_clip_as_json() {
        let mqtt = MockMqttConnection::new();
        let dev = dummy_dev("dev-2");
        attach(
            &(mqtt.clone() as Arc<dyn MqttConnection>),
            &dev,
            "rusthinq-raw",
        );

        dev.notify_send(SendToDevice::T2Packet(vec![0xde, 0xad]));
        dev.notify_send(SendToDevice::T2Clip {
            cmd: "setMaskingInfo".into(),
            msg_type: 1,
            data: json!({"x": 1}),
        });

        let raw = mqtt.raw_publishes();
        assert!(
            raw.iter()
                .any(|(t, p, _)| t == "rusthinq-raw/dev-2/raw/tx" && p == b"dead")
        );
        assert!(raw.iter().any(|(t, p, _)| {
            t == "rusthinq-raw/dev-2/raw/tx"
                && String::from_utf8_lossy(p).contains("setMaskingInfo")
                && String::from_utf8_lossy(p).contains("\"x\":1")
        }));
    }

    #[test]
    fn inject_set_sends_hex_packet_to_matching_device() {
        let sink = MqttSink::new(test_mqtt_config());
        let manager = DeviceManager::new();
        let sent = Arc::new(rusthinq_util::sync::Mutex::new(Vec::<SendToDevice>::new()));
        let sent2 = sent.clone();
        let dev = ConnectedDevice::new(
            "dev-3".into(),
            Platform::Thinq2,
            Metadata {
                model_id: "RAC_056905_WW".into(),
                model_name: "RAC".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(move |msg| sent2.lock().push(msg)),
        );
        manager.accept(dev);
        register_inject(&sink, manager);

        sink.handle_message("rusthinq-raw/dev-3/raw/inject/set", b"aa0102bb");

        let sent = sent.lock();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            SendToDevice::T2Packet(b) => assert_eq!(b, &vec![0xaa, 0x01, 0x02, 0xbb]),
            other => panic!("expected T2Packet, got {other:?}"),
        }
    }

    #[test]
    fn inject_clip_set_sends_named_clip_command_to_matching_device() {
        let sink = MqttSink::new(test_mqtt_config());
        let manager = DeviceManager::new();
        let sent = Arc::new(rusthinq_util::sync::Mutex::new(Vec::<SendToDevice>::new()));
        let sent2 = sent.clone();
        let dev = ConnectedDevice::new(
            "dev-3".into(),
            Platform::Thinq2,
            Metadata {
                model_id: "RAC_056905_WW".into(),
                model_name: "RAC".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(move |msg| sent2.lock().push(msg)),
        );
        manager.accept(dev);
        register_inject(&sink, manager);

        sink.handle_message(
            "rusthinq-raw/dev-3/raw/inject-clip/set",
            br#"{"cmd":"setMaskingInfo","type":0,"data":{"blacklist_tlv":"1200"}}"#,
        );

        let sent = sent.lock();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            SendToDevice::T2Clip {
                cmd,
                msg_type,
                data,
            } => {
                assert_eq!(cmd, "setMaskingInfo");
                assert_eq!(*msg_type, 0);
                assert_eq!(data, &json!({"blacklist_tlv": "1200"}));
            }
            other => panic!("expected T2Clip, got {other:?}"),
        }
    }

    #[test]
    fn inject_clip_set_ignores_malformed_json_and_missing_cmd() {
        let sink = MqttSink::new(test_mqtt_config());
        let manager = DeviceManager::new();
        let sent = Arc::new(rusthinq_util::sync::Mutex::new(Vec::<SendToDevice>::new()));
        let sent2 = sent.clone();
        let dev = ConnectedDevice::new(
            "dev-4".into(),
            Platform::Thinq2,
            Metadata {
                model_id: "RAC_056905_WW".into(),
                model_name: "RAC".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(move |msg| sent2.lock().push(msg)),
        );
        manager.accept(dev);
        register_inject(&sink, manager);

        sink.handle_message("rusthinq-raw/dev-4/raw/inject-clip/set", b"not json");
        sink.handle_message(
            "rusthinq-raw/dev-4/raw/inject-clip/set",
            br#"{"type":0,"data":{}}"#,
        );

        assert!(sent.lock().is_empty());
    }

    #[test]
    fn emit_set_injects_as_if_from_device() {
        let sink = MqttSink::new(test_mqtt_config());
        let manager = DeviceManager::new();
        let emitted = Arc::new(rusthinq_util::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        let emitted2 = emitted.clone();
        let dev = ConnectedDevice::new(
            "dev-4".into(),
            Platform::Thinq2,
            Metadata {
                model_id: "RAC_056905_WW".into(),
                model_name: "RAC".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(move |b| emitted2.lock().push(b)),
            Arc::new(|_m| {}),
        );
        manager.accept(dev);
        register_inject(&sink, manager);

        sink.handle_message("rusthinq-raw/dev-4/raw/emit/set", b"deadbeef");

        assert_eq!(emitted.lock().clone(), vec![vec![0xde, 0xad, 0xbe, 0xef]]);
    }

    #[test]
    fn inject_is_unreachable_when_raw_prefix_is_unset() {
        let mut cfg = test_mqtt_config();
        cfg.raw_prefix = None;
        let sink = MqttSink::new(cfg);
        let manager = DeviceManager::new();
        let sent = Arc::new(rusthinq_util::sync::Mutex::new(Vec::<SendToDevice>::new()));
        let sent2 = sent.clone();
        let dev = ConnectedDevice::new(
            "dev-5".into(),
            Platform::Thinq2,
            Metadata {
                model_id: "RAC_056905_WW".into(),
                model_name: "RAC".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(move |msg| sent2.lock().push(msg)),
        );
        manager.accept(dev);
        register_inject(&sink, manager);

        // Without a configured raw_prefix, handle_message only ever tries
        // rusthinq_prefix — a publish to what would have been the raw topic must be a
        // no-op, not accidentally routed through rusthinq_prefix too.
        sink.handle_message("rusthinq-raw/dev-5/raw/inject/set", b"aa0102bb");
        assert!(sent.lock().is_empty());
    }

    #[test]
    fn inject_ignores_unknown_device_and_bad_hex() {
        let sink = MqttSink::new(test_mqtt_config());
        let manager = DeviceManager::new();
        register_inject(&sink, manager);

        // Neither call should panic: unknown device id, then bad hex on a real one.
        sink.handle_message("rusthinq-raw/no-such-device/raw/inject/set", b"aabb");
    }
}
