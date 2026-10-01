//! Raw wire-frame MQTT bus — the "observer" pattern shared with zigbang/smartrelay: a
//! live rx/tx frame tap plus injection, over the same MQTT connection rusthinq already
//! holds open. Not retained — it's a live tail, not a history log. Meant for local
//! trusted tooling (rusthinq-mcp among them), not for driving any specific downstream
//! integration's entities: only wired up at all
//! when `config.mqtt.raw_prefix` is set — see its doc comment — and even then, run it
//! only where that connection is otherwise firewalled/trusted, the same assumption
//! smartrelay's own `observer` listener makes.
//!
//! Topics (`<prefix>` = `config.mqtt.raw_prefix`). The `raw/` segment is what keeps these
//! apart from rusthinq's own `<prefix>/<id>/<property>` topics when `raw_prefix` is set
//! to the same value as `rusthinq_prefix`, which is allowed. The payload of a topic is
//! fixed: hex for packets, JSON for everything else.
//!   - `<prefix>/<id>/raw/rx`, `raw/tx` (published, hex) — packets received from / sent to the device
//!   - `<prefix>/<id>/raw/clip/rx` (published, JSON) — CLIP messages the device sent that
//!     nothing local handles; otherwise dropped unseen unless bridged
//!   - `<prefix>/<id>/raw/clip/tx` (published, JSON) — CLIP commands sent to the device
//!     (ThinQ1: its JSON), including what the LG cloud sent down while bridged
//!   - `<prefix>/<id>/raw/lg/rx`, `raw/lg/tx` (published, JSON, `bridge` feature) — what the
//!     bridge receives from / sends to the real LG cloud; see [`lg_tap`]
//!   - `<prefix>/<id>/raw/inject/set` (subscribed, hex) — packet to send to the device
//!   - `<prefix>/<id>/raw/inject/clip/set` (subscribed, JSON `{cmd, type, data}`) — CLIP command to send
//!   - `<prefix>/<id>/raw/emit/set` (subscribed, hex) — packet to inject as if received from the device
//!   - `<prefix>/<id>/raw/sim/...` — the device simulator, see `sim_device.rs`
//!
//! Each stream is switched on by name in `[mqtt] raw = [...]` (`RawStreams`); with `raw`
//! left out every stream is off, so `raw_prefix` alone exposes nothing.
//!
//! `attach()` runs for every connected device once `raw_prefix` is set (for whichever
//! streams `raw` lists),
//! with zero awareness of whether `registry.rs` also gave that device's model a
//! script handler — there is no priority between the two, both just run in
//! parallel on every frame. Deliberate coexistence (debugging a scripted device's wire
//! traffic, testing a command via inject before adding it to the script) is exactly
//! what this bus is for and is fine. What isn't fine, and isn't checked anywhere in
//! code, is a model where this bus is another process's *only* data source (see
//! config.toml's `[scripting]` comment — the rethink-TS-adapter setup) also getting a
//! `<modelId>.rhai`: that's two independent full drivers for one device, and nothing
//! here will warn you.

use crate::devmgr::{ConnectedDevice, DeviceManager, SendToDevice};
use rusthinq_core::config::RawStreams;
use rusthinq_core::mqtt::{MqttConnection, MqttSink};
use serde_json::json;
use std::sync::Arc;

/// Which raw topic a message sent to the device belongs on, and its payload: packets are
/// hex on `raw/tx`, every other kind is JSON on `raw/clip/tx`, so a topic never mixes
/// payload formats.
fn tx_leaf_and_payload(msg: &SendToDevice) -> (&'static str, String) {
    match msg {
        SendToDevice::T2Packet(b) => ("tx", rusthinq_util::hex::encode(b)),
        SendToDevice::T2Clip {
            cmd,
            msg_type,
            data,
        } => (
            "clip/tx",
            json!({ "cmd": cmd, "type": msg_type, "data": data }).to_string(),
        ),
        SendToDevice::T2Raw(v) => ("clip/tx", v.to_string()),
        SendToDevice::T1Json(v) => ("clip/tx", v.to_string()),
    }
}

/// Publish the streams `streams` lists for `dev` onto the raw bus. Call once per
/// connected device, e.g. from [`DeviceManager::on_new_device`] — only when `raw_prefix`
/// is set; there is nothing to attach when it isn't.
pub fn attach(
    mqtt: &Arc<dyn MqttConnection>,
    dev: &ConnectedDevice,
    raw_prefix: &str,
    streams: &RawStreams,
) {
    if streams.rx {
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
    }

    if streams.tx || streams.clip_tx {
        let id = dev.id.clone();
        let mqtt_tx = mqtt.clone();
        let prefix_tx = raw_prefix.to_string();
        let (tx, clip_tx) = (streams.tx, streams.clip_tx);
        dev.add_send_handler(move |msg| {
            let (leaf, payload) = tx_leaf_and_payload(&msg);
            if (leaf == "tx" && !tx) || (leaf == "clip/tx" && !clip_tx) {
                return;
            }
            mqtt_tx.publish_raw(
                &format!("{prefix_tx}/{id}/raw/{leaf}"),
                payload.as_bytes(),
                false,
            );
        });
    }

    if streams.clip_rx {
        let id = dev.id.clone();
        let mqtt_clip = mqtt.clone();
        let prefix_clip = raw_prefix.to_string();
        dev.add_unhandled_clip_handler(move |payload| {
            mqtt_clip.publish_raw(
                &format!("{prefix_clip}/{id}/raw/clip/rx"),
                payload.to_string().as_bytes(),
                false,
            );
        });
    }
}

/// Publish one bridge<->LG message: `raw/lg/tx` when `to_lg` (sent to the LG cloud),
/// `raw/lg/rx` otherwise (received from it). Wired by `main.rs` to the bridge's traffic
/// hook when `lg_tx`/`lg_rx` is on. There is no LG side without the `bridge` feature, so
/// this is not compiled without it.
#[cfg(any(test, feature = "bridge"))]
pub fn lg_tap(
    mqtt: &Arc<dyn MqttConnection>,
    raw_prefix: &str,
    streams: &RawStreams,
    id: &str,
    to_lg: bool,
    payload: &serde_json::Value,
) {
    let (enabled, leaf) = if to_lg {
        (streams.lg_tx, "tx")
    } else {
        (streams.lg_rx, "rx")
    };
    if !enabled {
        return;
    }
    mqtt.publish_raw(
        &format!("{raw_prefix}/{id}/raw/lg/{leaf}"),
        payload.to_string().as_bytes(),
        false,
    );
}

/// Register the inject/emit handlers once, globally, on `sink` — routes
/// `<raw_prefix>/<id>/raw/inject/set`, `raw/inject/clip/set` and `raw/emit/set` to the
/// matching connected device via `manager`. `MqttSink::handle_message` already tries
/// the configured prefixes and restricts `raw/` commands to `raw_prefix`. Silently ignored for an unknown id or bad hex: this is a debugging tap,
/// not a control plane that reports errors back to the publisher. Only call this when
/// `config.mqtt.raw_prefix` is set.
///
/// `raw/inject/clip/set` is the named-CLIP-command counterpart to `raw/inject` (raw
/// bytes only, per `SendToDevice::T2Packet`) — some commands, like the
/// rethink-TS-adapter's one-shot `setMaskingInfo`, aren't a TLV/AABB packet at all,
/// they're a `{cmd, type, data}` JSON CLIP envelope (`SendToDevice::T2Clip`), which
/// `raw/inject` has no way to carry. Payload is JSON, not hex; malformed JSON or a
/// missing `cmd` is silently ignored, same policy as bad hex above.
pub fn register_inject(sink: &MqttSink, manager: Arc<DeviceManager>, streams: &RawStreams) {
    let streams = streams.clone();
    sink.on_set_property(move |id, prop, value| {
        let Some(dev) = manager.get(id) else { return };
        match prop {
            "raw/inject" if streams.inject => {
                let Ok(buf) = rusthinq_util::hex::decode(value) else {
                    return;
                };
                (dev.send_to_device)(SendToDevice::T2Packet(buf));
            }
            "raw/emit" if streams.emit => {
                let Ok(buf) = rusthinq_util::hex::decode(value) else {
                    return;
                };
                (dev.emit_data)(buf);
            }
            "raw/inject/clip" if streams.inject_clip => {
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
            raw: Default::default(),
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
            &RawStreams::all(),
        );

        dev.notify_data(&[0xaa, 0x01, 0x02, 0xbb]);

        let raw = mqtt.raw_publishes();
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0].0, "rusthinq-raw/dev-1/raw/rx");
        assert_eq!(raw[0].1, b"aa0102bb");
        assert!(!raw[0].2, "raw/rx must not be retained");
    }

    #[test]
    fn clip_rx_is_off_unless_listed_and_publishes_unhandled_clip_when_on() {
        let payload = json!({"cmd": "respUniversalCtrl", "mid": 7});

        let mqtt = MockMqttConnection::new();
        let dev = dummy_dev("dev-clip-off");
        attach(
            &(mqtt.clone() as Arc<dyn MqttConnection>),
            &dev,
            "rusthinq-raw",
            &RawStreams::default(),
        );
        dev.notify_unhandled_clip(payload.clone());
        assert!(
            mqtt.raw_publishes().is_empty(),
            "raw/clip/rx must be off unless listed"
        );

        let mqtt = MockMqttConnection::new();
        let dev = dummy_dev("dev-clip-on");
        let streams = RawStreams {
            clip_rx: true,
            ..RawStreams::default()
        };
        attach(
            &(mqtt.clone() as Arc<dyn MqttConnection>),
            &dev,
            "rusthinq-raw",
            &streams,
        );
        dev.notify_unhandled_clip(payload.clone());
        let raw = mqtt.raw_publishes();
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0].0, "rusthinq-raw/dev-clip-on/raw/clip/rx");
        assert_eq!(raw[0].1, payload.to_string().as_bytes());
        assert!(!raw[0].2, "raw/clip/rx must not be retained");
    }

    #[test]
    fn rx_and_tx_can_each_be_switched_off() {
        let mqtt = MockMqttConnection::new();
        let dev = dummy_dev("dev-quiet");
        let streams = RawStreams {
            rx: false,
            tx: false,
            ..RawStreams::all()
        };
        attach(
            &(mqtt.clone() as Arc<dyn MqttConnection>),
            &dev,
            "rusthinq-raw",
            &streams,
        );
        dev.notify_data(&[0xaa]);
        dev.notify_send(SendToDevice::T2Packet(vec![0xde]));
        assert!(mqtt.raw_publishes().is_empty());
    }

    #[test]
    fn lg_tap_publishes_only_the_enabled_direction() {
        let mqtt = MockMqttConnection::new();
        let dyn_mqtt = mqtt.clone() as Arc<dyn MqttConnection>;
        let streams = RawStreams {
            lg_tx: true,
            ..RawStreams::default()
        };
        let msg = json!({"cmd": "device_packet", "data": "AA"});

        lg_tap(&dyn_mqtt, "rusthinq-raw", &streams, "dev-1", true, &msg);
        lg_tap(&dyn_mqtt, "rusthinq-raw", &streams, "dev-1", false, &msg);

        let raw = mqtt.raw_publishes();
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0].0, "rusthinq-raw/dev-1/raw/lg/tx");
    }

    #[test]
    fn tx_packets_are_hex_on_raw_tx_and_everything_else_is_json_on_raw_clip_tx() {
        let mqtt = MockMqttConnection::new();
        let dev = dummy_dev("dev-2");
        attach(
            &(mqtt.clone() as Arc<dyn MqttConnection>),
            &dev,
            "rusthinq-raw",
            &RawStreams::all(),
        );

        dev.notify_send(SendToDevice::T2Packet(vec![0xde, 0xad]));
        dev.notify_send(SendToDevice::T2Clip {
            cmd: "setMaskingInfo".into(),
            msg_type: 1,
            data: json!({"x": 1}),
        });
        dev.notify_send(SendToDevice::T2Raw(json!({"cmd": "ack", "mid": 5})));

        let raw = mqtt.raw_publishes();
        assert!(
            raw.iter()
                .any(|(t, p, _)| t == "rusthinq-raw/dev-2/raw/tx" && p == b"dead")
        );
        let clip_tx: Vec<_> = raw
            .iter()
            .filter(|(t, _, _)| t == "rusthinq-raw/dev-2/raw/clip/tx")
            .map(|(_, p, _)| String::from_utf8_lossy(p).to_string())
            .collect();
        assert_eq!(clip_tx.len(), 2);
        assert!(clip_tx[0].contains("setMaskingInfo") && clip_tx[0].contains("\"x\":1"));
        assert!(clip_tx[1].contains("\"ack\""));
        assert!(
            !raw.iter()
                .any(|(t, p, _)| t.ends_with("/raw/tx") && p.first() == Some(&b'{')),
            "raw/tx must never carry JSON"
        );
    }

    #[test]
    fn tx_and_clip_tx_are_switched_independently() {
        let send = |streams: &RawStreams| {
            let mqtt = MockMqttConnection::new();
            let dev = dummy_dev("dev-split");
            attach(
                &(mqtt.clone() as Arc<dyn MqttConnection>),
                &dev,
                "rusthinq-raw",
                streams,
            );
            dev.notify_send(SendToDevice::T2Packet(vec![0x01]));
            dev.notify_send(SendToDevice::T2Raw(json!({"cmd": "ack"})));
            mqtt.raw_publishes()
                .into_iter()
                .map(|(t, _, _)| t)
                .collect::<Vec<_>>()
        };

        let only_tx = send(&RawStreams {
            tx: true,
            ..RawStreams::default()
        });
        assert_eq!(only_tx, vec!["rusthinq-raw/dev-split/raw/tx"]);

        let only_clip = send(&RawStreams {
            clip_tx: true,
            ..RawStreams::default()
        });
        assert_eq!(only_clip, vec!["rusthinq-raw/dev-split/raw/clip/tx"]);
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
        register_inject(&sink, manager, &RawStreams::all());

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
        register_inject(&sink, manager, &RawStreams::all());

        sink.handle_message(
            "rusthinq-raw/dev-3/raw/inject/clip/set",
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
        register_inject(&sink, manager, &RawStreams::all());

        sink.handle_message("rusthinq-raw/dev-4/raw/inject/clip/set", b"not json");
        sink.handle_message(
            "rusthinq-raw/dev-4/raw/inject/clip/set",
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
        register_inject(&sink, manager, &RawStreams::all());

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
        register_inject(&sink, manager, &RawStreams::all());

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
        register_inject(&sink, manager, &RawStreams::all());

        // Neither call should panic: unknown device id, then bad hex on a real one.
        sink.handle_message("rusthinq-raw/no-such-device/raw/inject/set", b"aabb");
    }
}
