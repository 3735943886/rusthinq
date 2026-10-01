//! "Forget device": erases everything rusthinq persists for a device id, whether or
//! not it's currently connected — issue #9 (no way to see or clean up per-device
//! state for a device that's gone for good). `bridge_control.rs`'s enable/disable
//! only ever touch a *live* session; this is the counterpart for a device that will
//! never reconnect at all, e.g. one `devlist.rs`'s snapshot now lists as
//! `online: false` with a stale `lastSeenUnix`.
//!
//! `<prefix>/<id>/forget/set` (subscribed, payload ignored):
//!   - clears every retained `<prefix>/<id>/<property>` topic
//!     (`MqttConnection::clear_retained`) -- only relevant if something drove this
//!     device's properties via `publish_property` in the first place (a `.rhai`
//!     handler); a no-op otherwise, which is most of the time for a
//!     raw-bus-driven device
//!   - with `[scripting] il_prefix` set, also clears the retained IL descriptor at
//!     `<il_prefix>/<id>` (an empty retained payload; a consumer takes that as "this
//!     device is gone")
//!   - removes the id from `known_devices.rs`'s connection-level ledger, which is
//!     what actually makes it stop appearing in `<prefix>/devices` at all
//!   - with the `bridge` feature and a live LG-cloud bridge, also clears the
//!     bridge's saved pairing state for `id` (`Bridge::disable`), same as a manual
//!     `bridge/disable` would
//!   - republishes `<prefix>/devices` and answers on `<prefix>/<id>/forget/status`
//!
//! Always registered (unlike `bridge_control.rs`, which only exists with the
//! `bridge` feature) — clearing this state doesn't need a bridge at all.

use crate::bridge_handle::Bridge;
use crate::devlist::DeviceListPublisher;
use crate::known_devices::KnownDevices;
use rusthinq_core::mqtt::{MqttConnection, MqttSink};
use std::sync::Arc;

pub fn register(
    sink: &MqttSink,
    mqtt: Arc<dyn MqttConnection>,
    bridge: Option<Arc<Bridge>>,
    device_list: Arc<DeviceListPublisher>,
    known_devices: Arc<KnownDevices>,
) {
    sink.on_set_property(move |id, prop, _value| {
        if prop != "forget" {
            return;
        }
        let id = id.to_string();
        let mqtt = mqtt.clone();
        let bridge = bridge.clone();
        let device_list = device_list.clone();
        let known_devices = known_devices.clone();
        tokio::spawn(async move {
            disable_bridge(&bridge, &id).await;
            mqtt.clear_retained(&id);
            #[cfg(feature = "scripting")]
            if let Some(prefix) = rusthinq_devices::scripting::il_prefix() {
                mqtt.publish_raw(&format!("{prefix}/{id}"), b"", true);
            }
            known_devices.forget(&id);
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
        let known_devices = KnownDevices::new(None);
        known_devices.note_connected(
            "dev-gone",
            &rusthinq_core::metadata::Metadata::new("MODEL", "MODEL", "1.0"),
            crate::devmgr::Platform::Thinq2,
        );
        known_devices.note_disconnected("dev-gone");
        assert_eq!(known_devices.all().len(), 1);

        let manager = DeviceManager::new();
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let device_list = DeviceListPublisher::new(
            mqtt.clone() as Arc<dyn MqttConnection>,
            manager,
            device_bridge,
            None,
            known_devices.clone(),
        );
        let sink_cfg = rusthinq_core::config::MqttConfig {
            mqtt_url: "mqtt://127.0.0.1:1883".into(),
            rusthinq_prefix: "rusthinq".into(),
            mqtt_user: String::new(),
            mqtt_pass: String::new(),
            raw_prefix: None,
            raw: Default::default(),
            state_file: None,
        };
        let sink = MqttSink::new(sink_cfg);
        #[cfg(feature = "scripting")]
        rusthinq_devices::scripting::set_il_prefix(Some("il".into()));

        register(
            &sink,
            mqtt.clone() as Arc<dyn MqttConnection>,
            None,
            device_list,
            known_devices.clone(),
        );
        sink.handle_message("rusthinq/dev-gone/forget/set", b"");

        // The handler is spawned onto its own task -- poll until it's had a chance
        // to run rather than assuming it beat this assertion.
        for _ in 0..50 {
            if known_devices.all().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            known_devices.all().is_empty(),
            "forget must remove the id from known_devices"
        );
        assert!(
            mqtt.known_devices().is_empty(),
            "forget must also clear_retained any properties that were published"
        );
        #[cfg(feature = "scripting")]
        {
            rusthinq_devices::scripting::set_il_prefix(None);
            assert!(
                mqtt.raw_publishes()
                    .iter()
                    .any(|(t, p, retain)| t == "il/dev-gone" && p.is_empty() && *retain),
                "forget must clear the retained IL descriptor"
            );
        }
        let raw = mqtt.retained("devices").expect("devices must republish");
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(v["devices"].get("dev-gone").is_none());
    }
}
