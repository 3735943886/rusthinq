//! Retained `<rusthinq_prefix>/devices` topic: a JSON snapshot of MQTT control-plane
//! connection state, LG-bridge login state, and every connected device's
//! handler-mapping/bridge status.
//! Republished whenever any of those can have changed. Retained so a client that
//! (re)subscribes gets current state immediately, without needing a request/response
//! round trip — this is what the old management HTTP/WS surface's `/ws` hello and
//! `/api/devices/{id}` gave (both removed in favor of MQTT-only control; the optional
//! `rusthinq-gui` dashboard's `/ws` route rebuilds its panel from this same topic
//! rather than reviving that surface).

use crate::bridge_handle::Bridge;
use crate::device_bridge::DeviceBridge;
use crate::devmgr::DeviceManager;
use crate::known_devices::KnownDevices;
use rusthinq_core::mqtt::MqttConnection;
use serde_json::{Value, json};
use std::sync::Arc;

pub struct DeviceListPublisher {
    mqtt: Arc<dyn MqttConnection>,
    manager: Arc<DeviceManager>,
    device_bridge: Arc<DeviceBridge>,
    bridge: Option<Arc<Bridge>>,
    known_devices: Arc<KnownDevices>,
}

impl DeviceListPublisher {
    pub fn new(
        mqtt: Arc<dyn MqttConnection>,
        manager: Arc<DeviceManager>,
        device_bridge: Arc<DeviceBridge>,
        bridge: Option<Arc<Bridge>>,
        known_devices: Arc<KnownDevices>,
    ) -> Arc<Self> {
        Arc::new(Self {
            mqtt,
            manager,
            device_bridge,
            bridge,
            known_devices,
        })
    }

    fn snapshot(&self) -> Value {
        let mut all = serde_json::Map::new();
        let live = self.manager.all();
        for (id, dev) in &live {
            all.insert(
                id.clone(),
                json!({
                    "name": self.bridge.as_ref().and_then(|b| b.name(id)),
                    "model": dev.meta.model_id,
                    "modelName": dev.meta.model_name,
                    "deviceType": dev.meta.device_type,
                    "swVersion": dev.meta.sw_version,
                    "platform": dev.platform.as_str(),
                    "mapped": self.device_bridge.has_device(id),
                    "bridged": self.bridge.as_ref().map(|b| b.status_for(id)).unwrap_or(false),
                    // Distinct from "bridged": stays true across a disconnect, or a
                    // live session dying while still connected -- see
                    // Bridge::is_paired's doc comment.
                    "bridgePaired": self.bridge.as_ref().map(|b| b.is_paired(id)).unwrap_or(false),
                    "online": true,
                }),
            );
        }
        // A device rusthinq has connected before but isn't currently connected is
        // otherwise invisible everywhere — `DeviceManager::all()` above is
        // live-connections-only, so a disconnected id (even one that's gone for
        // good) would simply vanish from this snapshot with nothing left to notice
        // or clean it up. List it too, from `known_devices` -- tracked at the
        // connection level (`DeviceManager::on_new_device`/`on_drop_device`) rather
        // than from MQTT property publishes, since a device with no script
        // handler (driven purely over the raw bus, or not driven by anything at
        // all) never publishes a single property and would otherwise never appear
        // here even while genuinely connected, let alone after. See
        // `device_control.rs` for the matching `forget` command that removes one of
        // these.
        for (id, known) in self.known_devices.all() {
            if live.contains_key(&id) {
                continue;
            }
            all.insert(
                id.clone(),
                json!({
                    "name": self.bridge.as_ref().and_then(|b| b.name(&id)),
                    "model": known.meta.model_id,
                    "modelName": known.meta.model_name,
                    "deviceType": known.meta.device_type,
                    "swVersion": known.meta.sw_version,
                    "platform": known.platform,
                    // Whether LG-cloud pairing state still exists for a device that's
                    // currently unreachable locally -- see Bridge::is_paired's doc
                    // comment. True here means the bridge will auto-resume
                    // (`Bridge::on_local_device`) the moment this device reconnects;
                    // false means it was disabled/forgotten while offline (or never
                    // bridged), and reconnecting brings it back as local-only.
                    "bridgePaired": self.bridge.as_ref().map(|b| b.is_paired(&id)).unwrap_or(false),
                    "online": false,
                    "lastSeenUnix": known.last_seen_unix,
                }),
            );
        }
        json!({
            "mqtt": self.mqtt.is_connected(),
            "bridgeLoggedIn": self.bridge.as_ref().map(|b| b.is_logged_in()),
            "devices": Value::Object(all),
            // Which of this *binary's* optional features were actually compiled in --
            // `rusthinq-gui` talks to rusthinq-cloud only over MQTT and has no other way
            // to know. Used for two things on the dashboard: showing the running
            // build's features next to its version, and deciding whether a device's "no
            // script handler" warning means anything -- with `scripting` off, *every*
            // device would show it regardless of the device itself, which is not a
            // per-device signal at that point.
            "features": {
                "bridge": cfg!(feature = "bridge"),
                "scripting": cfg!(feature = "scripting"),
            },
        })
    }

    /// Publish the current snapshot. Call after any device connect/disconnect, and
    /// after anything that can change `mapped`/`bridged`/`bridgeLoggedIn` status
    /// (bridge enable/disable).
    pub fn publish(&self) {
        self.mqtt
            .publish_retained("devices", &self.snapshot().to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::devmgr::{ConnectedDevice, Platform};
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

    fn empty_known_devices() -> Arc<KnownDevices> {
        KnownDevices::new(None)
    }

    #[test]
    fn publishes_retained_snapshot_with_platform_and_model() {
        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        manager.accept(dummy_dev("dev-1"));
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            None,
            empty_known_devices(),
        );

        publisher.publish();

        let raw = mqtt
            .retained("devices")
            .expect("devices topic must be retained");
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"]["dev-1"]["model"], "RAC_056905_WW");
        assert_eq!(v["devices"]["dev-1"]["platform"], "thinq2");
        assert_eq!(v["devices"]["dev-1"]["mapped"], false);
        assert_eq!(v["devices"]["dev-1"]["bridged"], false);
        // No bridge configured at all -- can't have a ThinQ-account name for anything.
        assert_eq!(v["devices"]["dev-1"]["name"], Value::Null);
        assert_eq!(v["bridgeLoggedIn"], Value::Null);
    }

    /// `rusthinq-gui` has no other way to learn which of this binary's optional
    /// features are actually compiled in -- it talks to rusthinq-cloud only over
    /// MQTT. Checked against the same `cfg!` the snapshot itself uses (not a
    /// hardcoded value) so this catches a typo'd key name or a swapped value, not
    /// just a change in which features happen to be on for this test run.
    #[test]
    fn snapshot_reports_compiled_in_features() {
        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            None,
            empty_known_devices(),
        );

        publisher.publish();

        let raw = mqtt
            .retained("devices")
            .expect("devices topic must be retained");
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["features"]["bridge"], cfg!(feature = "bridge"));
        assert_eq!(v["features"]["scripting"], cfg!(feature = "scripting"));
    }

    #[test]
    fn mapped_reflects_device_bridge_has_device() {
        // This test's modelId has no matching `.rhai`
        // script, so device_bridge.new_device() can't actually map it; "mapped" must
        // stay false rather than assume success. This still exercises the real
        // new_device() path so the snapshot logic is checked against DeviceBridge,
        // not a hand-set flag.
        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        let dev = dummy_dev("dev-2");
        manager.accept(dev.clone());
        let device_bridge = DeviceBridge::new(mqtt.clone());
        device_bridge.new_device(dev);
        assert!(!device_bridge.has_device("dev-2"));
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            None,
            empty_known_devices(),
        );

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"]["dev-2"]["mapped"], false);
    }

    #[test]
    #[cfg(feature = "bridge")]
    fn bridge_logged_in_reflects_stored_credentials() {
        use rusthinq_bridge::JsonStorage;
        use rusthinq_bridge::state::{BridgeState, Credentials, Environment};

        let dir = std::env::temp_dir().join("rusthinq-devlist-test-bridge-logged-in");
        let storage = Arc::new(JsonStorage::new(&dir));
        storage.set_credentials(Some(Credentials {
            refresh_token: "tok".into(),
            env: Environment {
                country_code: "US".into(),
                language_code: None,
            },
        }));
        let bridge = Bridge::new(storage);

        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            Some(bridge),
            empty_known_devices(),
        );

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["bridgeLoggedIn"], true);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_manager_publishes_empty_devices_map() {
        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            None,
            empty_known_devices(),
        );

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"], json!({}));
    }

    /// Issue #9: a device rusthinq has connected before but isn't connected right
    /// now must still show up in the snapshot (as `online: false`), not vanish the
    /// moment it disconnects -- tracked via `known_devices` (connection-level),
    /// not `mqtt.known_devices()` (property-publish-level, blind to a raw-bus-only
    /// device with no script handler).
    #[test]
    fn known_but_offline_devices_are_listed_alongside_live_ones() {
        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        manager.accept(dummy_dev("dev-1"));
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let known_devices = empty_known_devices();
        let gone_meta = Metadata {
            model_id: "WBEY3GT".into(),
            model_name: "WBEY3GT".into(),
            device_type: Some("303".into()),
            sw_version: None,
        };
        known_devices.note_connected("dev-gone", &gone_meta, Platform::Thinq2);
        known_devices.note_disconnected("dev-gone"); // no live connection at all
        let publisher =
            DeviceListPublisher::new(mqtt.clone(), manager, device_bridge, None, known_devices);

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"]["dev-1"]["online"], true);
        assert_eq!(v["devices"]["dev-gone"]["online"], false);
        assert_eq!(v["devices"]["dev-gone"]["model"], "WBEY3GT");
        assert!(v["devices"]["dev-gone"]["lastSeenUnix"].as_i64().unwrap() > 0);
    }

    /// A live connection always wins over stale `known_devices` bookkeeping for the
    /// same id -- e.g. right after a reconnect, before the old entry's ever cleared.
    #[test]
    fn a_live_connection_is_not_shadowed_by_its_own_known_devices_entry() {
        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        let dev = dummy_dev("dev-1");
        manager.accept(dev.clone());
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let known_devices = empty_known_devices();
        known_devices.note_connected("dev-1", &dev.meta, Platform::Thinq2);
        let publisher =
            DeviceListPublisher::new(mqtt.clone(), manager, device_bridge, None, known_devices);

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"]["dev-1"]["online"], true);
        assert_eq!(v["devices"]["dev-1"]["model"], "RAC_056905_WW");
    }

    /// `bridgePaired` distinguishes "has saved LG-cloud pairing state" from
    /// `bridged` ("has a live relay session right now") -- a device can be online
    /// with saved state but no live session (its bridge connection died, or never
    /// came back up on reconnect), which is a different, worse situation than never
    /// having been bridged at all.
    #[test]
    #[cfg(feature = "bridge")]
    fn bridge_paired_is_true_for_an_online_device_with_saved_state_but_no_live_session() {
        use rusthinq_bridge::JsonStorage;
        use rusthinq_bridge::state::BridgeState;

        let dir = std::env::temp_dir().join("rusthinq-devlist-test-bridge-paired-online");
        let storage = Arc::new(JsonStorage::new(&dir));
        storage.set_device_state_json("dev-1", Some(json!({"some": "state"})));
        let bridge = Bridge::new(storage);

        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        manager.accept(dummy_dev("dev-1"));
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            Some(bridge),
            empty_known_devices(),
        );

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"]["dev-1"]["bridged"], false);
        assert_eq!(v["devices"]["dev-1"]["bridgePaired"], true);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same distinction while the device is offline: `bridgePaired` says whether
    /// reconnecting will auto-resume the bridge (`Bridge::on_local_device`) or bring
    /// it back as local-only (disabled/forgotten while offline, or never bridged).
    #[test]
    #[cfg(feature = "bridge")]
    fn bridge_paired_is_true_for_an_offline_device_with_saved_state() {
        use rusthinq_bridge::JsonStorage;
        use rusthinq_bridge::state::BridgeState;

        let dir = std::env::temp_dir().join("rusthinq-devlist-test-bridge-paired-offline");
        let storage = Arc::new(JsonStorage::new(&dir));
        storage.set_device_state_json("dev-gone", Some(json!({"some": "state"})));
        let bridge = Bridge::new(storage);

        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let known_devices = empty_known_devices();
        known_devices.note_connected(
            "dev-gone",
            &Metadata::new("MODEL", "MODEL", "1.0"),
            Platform::Thinq2,
        );
        known_devices.note_disconnected("dev-gone");
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            Some(bridge),
            known_devices,
        );

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"]["dev-gone"]["online"], false);
        assert_eq!(v["devices"]["dev-gone"]["bridgePaired"], true);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
