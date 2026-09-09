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
use rusthinq_core::mqtt::MqttConnection;
use serde_json::{Value, json};
use std::sync::Arc;

pub struct DeviceListPublisher {
    mqtt: Arc<dyn MqttConnection>,
    manager: Arc<DeviceManager>,
    device_bridge: Arc<DeviceBridge>,
    bridge: Option<Arc<Bridge>>,
}

impl DeviceListPublisher {
    pub fn new(
        mqtt: Arc<dyn MqttConnection>,
        manager: Arc<DeviceManager>,
        device_bridge: Arc<DeviceBridge>,
        bridge: Option<Arc<Bridge>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            mqtt,
            manager,
            device_bridge,
            bridge,
        })
    }

    fn snapshot(&self) -> Value {
        let mut all = serde_json::Map::new();
        for (id, dev) in self.manager.all() {
            all.insert(
                id.clone(),
                json!({
                    "name": self.bridge.as_ref().and_then(|b| b.name(&id)),
                    "model": dev.meta.model_id,
                    "modelName": dev.meta.model_name,
                    "deviceType": dev.meta.device_type,
                    "swVersion": dev.meta.sw_version,
                    "platform": dev.platform.as_str(),
                    "mapped": self.device_bridge.has_device(&id),
                    "bridged": self.bridge.as_ref().map(|b| b.status_for(&id)).unwrap_or(false),
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

    #[test]
    fn publishes_retained_snapshot_with_platform_and_model() {
        let mqtt = MockMqttConnection::new();
        let manager = DeviceManager::new();
        manager.accept(dummy_dev("dev-1"));
        let device_bridge = DeviceBridge::new(mqtt.clone());
        let publisher = DeviceListPublisher::new(mqtt.clone(), manager, device_bridge, None);

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
        let publisher = DeviceListPublisher::new(mqtt.clone(), manager, device_bridge, None);

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
        let publisher =
            DeviceListPublisher::new(mqtt.clone(), manager, device_bridge, Some(bridge));

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
        let publisher = DeviceListPublisher::new(mqtt.clone(), manager, device_bridge, None);

        publisher.publish();

        let raw = mqtt.retained("devices").unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["devices"], json!({}));
    }
}
