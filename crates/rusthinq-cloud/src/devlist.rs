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
        // than from MQTT property publishes, since a device with no native/script
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
                    "online": false,
                    "lastSeenUnix": known.last_seen_unix,
                }),
            );
        }
        json!({
            "mqtt": self.mqtt.is_connected(),
            "bridgeLoggedIn": self.bridge.as_ref().map(|b| b.is_logged_in()),
            "devices": Value::Object(all),
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

    #[test]
    fn mapped_reflects_device_bridge_has_device() {
        // This test's modelId has neither a native factory nor a matching `.rhai`
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
    /// device with no native/script handler).
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
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            None,
            known_devices,
        );

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
        let publisher = DeviceListPublisher::new(
            mqtt.clone(),
            manager,
            device_bridge,
            None,
            known_devices,
        );

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"]["dev-1"]["online"], true);
        assert_eq!(v["devices"]["dev-1"]["model"], "RAC_056905_WW");
    }
}
