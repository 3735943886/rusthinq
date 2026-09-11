//! "Forget device": erases everything rusthinq persists for a device id, whether or
//! not it's currently connected — issue #9 (no way to see or clean up per-device
//! state for a device that's gone for good). `bridge_control.rs`'s enable/disable
//! only ever touch a *live* session; this is the counterpart for a device that will
//! never reconnect at all, e.g. one `devlist.rs`'s snapshot now lists as
//! `online: false` with a stale `lastSeenUnix`.
//!
//! `<prefix>/<id>/forget/set` (subscribed, payload ignored):
//!   - clears every retained `<prefix>/<id>/<property>` topic
//!     (`MqttConnection::clear_retained`) — this also drops `id` from
//!     `MqttSink::known_devices`, so it stops appearing in `<prefix>/devices` at all
//!   - with the `bridge` feature and a live LG-cloud bridge, also clears the
//!     bridge's saved pairing state for `id` (`Bridge::disable`), same as a manual
//!     `bridge/disable` would
//!   - republishes `<prefix>/devices` and answers on `<prefix>/<id>/forget/status`
//!
//! Always registered (unlike `bridge_control.rs`, which only exists with the
//! `bridge` feature) — clearing MQTT-side state doesn't need a bridge at all.

use crate::bridge_handle::Bridge;
use crate::devlist::DeviceListPublisher;
use rusthinq_core::mqtt::{MqttConnection, MqttSink};
use std::sync::Arc;

pub fn register(
    sink: &MqttSink,
    mqtt: Arc<dyn MqttConnection>,
    bridge: Option<Arc<Bridge>>,
    device_list: Arc<DeviceListPublisher>,
) {
    sink.on_set_property(move |id, prop, _value| {
        if prop != "forget" {
            return;
        }
        let id = id.to_string();
        let mqtt = mqtt.clone();
        let bridge = bridge.clone();
        let device_list = device_list.clone();
        tokio::spawn(async move {
            disable_bridge(&bridge, &id).await;
            mqtt.clear_retained(&id);
            device_list.publish();
            mqtt.publish_event(&id, "forget/status", "forgotten");
        });
    });
}

#[cfg(feature = "bridge")]
async fn disable_bridge(bridge: &Option<Arc<Bridge>>, id: &str) {
    if let Some(br) = bridge {
        let _ = br.disable(id).await;
    }
}

#[cfg(not(feature = "bridge"))]
async fn disable_bridge(_bridge: &Option<Arc<Bridge>>, _id: &str) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device_bridge::DeviceBridge;
    use crate::devmgr::DeviceManager;
    use rusthinq_core::MockMqttConnection;

    #[tokio::test]
    async fn forget_clears_retained_state_and_republishes_the_device_list() {
        let mqtt = MockMqttConnection::new();
        mqtt.publish_property("dev-gone", "power", "ON");
        assert_eq!(mqtt.known_devices().len(), 1);

        let manager = DeviceManager::new();
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let device_list = DeviceListPublisher::new(
            mqtt.clone() as Arc<dyn MqttConnection>,
            manager,
            device_bridge,
            None,
        );
        let sink_cfg = rusthinq_core::config::MqttConfig {
            mqtt_url: "mqtt://127.0.0.1:1883".into(),
            rusthinq_prefix: "rusthinq".into(),
            mqtt_user: String::new(),
            mqtt_pass: String::new(),
            raw_prefix: None,
            state_file: None,
        };
        let sink = MqttSink::new(sink_cfg);

        register(
            &sink,
            mqtt.clone() as Arc<dyn MqttConnection>,
            None,
            device_list,
        );
        sink.handle_message("rusthinq/dev-gone/forget/set", b"");

        // The handler is spawned onto its own task -- poll until it's had a chance
        // to run rather than assuming it beat this assertion.
        for _ in 0..50 {
            if mqtt.known_devices().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            mqtt.known_devices().is_empty(),
            "forget must clear_retained the id out of known_devices"
        );
        let raw = mqtt.retained("devices").expect("devices must republish");
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(v["devices"].get("dev-gone").is_none());
    }
}
