//! Bridge enable/disable, and LG account login/logout, over MQTT — these topics are
//! the real implementation; the optional `rusthinq-gui` dashboard's HTTP routes of
//! the same name (`/bridge/{id}/enable`/`/disable`, `/thinq_login*`) are just a thin
//! frontend that publishes here and waits for the reply. Enable/disable needs the
//! connected device's own local socket to start/stop the upstream LG session, so it
//! has to stay in the daemon; login/logout don't touch a live device at all
//! (`Bridge` only needs its credential storage for those), but there's no reason to
//! keep them a separate one-shot CLI process either — they're reachable over the
//! exact same MQTT connection as everything else here.
//!
//! Per-device topics (`<prefix>` = `config.mqtt.rusthinq_prefix`, `<id>` = a real
//! connected device's id):
//!   - `<prefix>/<id>/bridge/enable/set` (subscribed) — payload: LG deviceType,
//!     or empty to use the value the device itself reported
//!   - `<prefix>/<id>/bridge/disable/set` (subscribed) — payload ignored
//!   - `<prefix>/<id>/bridge/status` (published, non-retained) — progress
//!     messages during enable, then a final "enabled"/"disabled"/error string
//!
//! Account-level topics (not scoped to any device — `bridge` here is a fixed literal
//! standing in for the id slot, never a real device id):
//!   - `<prefix>/bridge/login/set` (subscribed) — payload: LG country code (e.g.
//!     "US"), empty defaults to "US". Publishes the LG sign-in URL to
//!     `<prefix>/bridge/login-url`; open it in a browser, log in, and copy the final
//!     redirected URL.
//!   - `<prefix>/bridge/login/complete/set` (subscribed) — payload: that redirected
//!     URL, in full (its `code` query parameter is what's actually used). Uses
//!     whichever country code the most recent `login/set` requested.
//!   - `<prefix>/bridge/logout/set` (subscribed) — payload ignored, clears stored
//!     credentials and detaches every live bridge session.
//!   - `<prefix>/bridge/login-url` (published, non-retained) — the URL from `login/set`
//!   - `<prefix>/bridge/status` (published, non-retained) — also carries
//!     login/logout outcomes ("logged in", "logged out", or an error string);
//!     current logged-in state is also always visible in `<prefix>/devices`
//!     (`devlist.rs`), so there's no dedicated login-status topic to poll.

use crate::bridge_adapter::ConnectedAsLocal;
use crate::devlist::DeviceListPublisher;
use crate::devmgr::DeviceManager;
use rusthinq_bridge::{Bridge, LocalDevice};
use rusthinq_core::mqtt::{MqttConnection, MqttSink};
use rusthinq_util::sync::Mutex;
use std::sync::Arc;

/// Not a real device id — the id slot every other command here uses to scope a
/// topic to one connected device. Login/logout are account-level, not
/// device-level, so they borrow the same `<prefix>/<id>/.../set` wire shape with
/// this fixed literal instead, rather than teaching `MqttSink::handle_message` a
/// second topic shape with no id segment at all. A real ThinQ device id is never
/// this string, so there's no risk of the two colliding.
const ACCOUNT_ID: &str = "bridge";

pub fn register(
    sink: &MqttSink,
    mqtt: Arc<dyn MqttConnection>,
    manager: Arc<DeviceManager>,
    bridge: Arc<Bridge>,
    device_list: Arc<DeviceListPublisher>,
) {
    // Remembers the country code `login/set` most recently requested, so
    // `login/complete/set` (a separate MQTT message, arbitrarily later once the
    // human's done with the browser) knows which one to use — mirroring the local
    // variable a synchronous CLI login flow would hold between its own two steps.
    let pending_country_code = Arc::new(Mutex::new(String::new()));

    sink.on_set_property(move |id, prop, value| {
        let mqtt = mqtt.clone();
        match (id, prop) {
            (_, "bridge/enable") => {
                let id = id.to_string();
                let value = value.to_string();
                let manager = manager.clone();
                let bridge = bridge.clone();
                let device_list = device_list.clone();
                tokio::spawn(async move {
                    do_enable(&mqtt, &manager, &bridge, &device_list, &id, &value).await
                });
            }
            (_, "bridge/disable") => {
                let id = id.to_string();
                let bridge = bridge.clone();
                let device_list = device_list.clone();
                tokio::spawn(async move { do_disable(&mqtt, &bridge, &device_list, &id).await });
            }
            (ACCOUNT_ID, "login") => {
                let country_code = if value.is_empty() { "US" } else { value }.to_string();
                *pending_country_code.lock() = country_code.clone();
                let bridge = bridge.clone();
                tokio::spawn(async move { do_login(&mqtt, &bridge, &country_code).await });
            }
            (ACCOUNT_ID, "login/complete") => {
                let callback_url = value.to_string();
                let country_code = pending_country_code.lock().clone();
                let country_code = if country_code.is_empty() {
                    "US"
                } else {
                    &country_code
                }
                .to_string();
                let bridge = bridge.clone();
                let device_list = device_list.clone();
                tokio::spawn(async move {
                    do_complete_login(&mqtt, &bridge, &device_list, &country_code, &callback_url)
                        .await
                });
            }
            (ACCOUNT_ID, "logout") => {
                let bridge = bridge.clone();
                let device_list = device_list.clone();
                tokio::spawn(async move { do_logout(&mqtt, &bridge, &device_list).await });
            }
            _ => {}
        }
    });
}

async fn do_enable(
    mqtt: &Arc<dyn MqttConnection>,
    manager: &Arc<DeviceManager>,
    bridge: &Arc<Bridge>,
    device_list: &Arc<DeviceListPublisher>,
    id: &str,
    device_type: &str,
) {
    let Some(dev) = manager.get(id) else {
        mqtt.publish_event(id, "bridge/status", "device not connected");
        return;
    };
    let device: Arc<dyn LocalDevice> = Arc::new(ConnectedAsLocal(dev));
    let dt = if device_type.is_empty() {
        None
    } else {
        Some(device_type)
    };

    let mqtt_report = mqtt.clone();
    let id_report = id.to_string();
    let report: Box<dyn FnMut(&str) + Send> = Box::new(move |s: &str| {
        mqtt_report.publish_event(&id_report, "bridge/status", s);
    });

    match bridge.enable(device, dt, Some(report)).await {
        Ok(true) => {
            mqtt.publish_event(id, "bridge/status", "enabled");
            device_list.publish();
        }
        Ok(false) => {
            tracing::warn!("bridge enable failed for {id}");
            mqtt.publish_event(id, "bridge/status", "enable failed");
        }
        Err(e) => {
            tracing::warn!("bridge enable error for {id}: {e:#}");
            mqtt.publish_event(id, "bridge/status", &format!("enable error: {e:#}"));
        }
    }
}

async fn do_disable(
    mqtt: &Arc<dyn MqttConnection>,
    bridge: &Arc<Bridge>,
    device_list: &Arc<DeviceListPublisher>,
    id: &str,
) {
    let _ = bridge.disable(id).await;
    mqtt.publish_event(id, "bridge/status", "disabled");
    device_list.publish();
}

async fn do_login(mqtt: &Arc<dyn MqttConnection>, bridge: &Arc<Bridge>, country_code: &str) {
    match bridge.begin_login(country_code).await {
        Ok(url) => {
            // Also on stdout/journal (not just MQTT) — logging in wouldn't otherwise
            // need an MQTT client at hand, just whatever's watching the daemon's own
            // output.
            rusthinq_core::logging::log("bridge", &[&format!("LG sign-in URL: {url}")]);
            mqtt.publish_event(ACCOUNT_ID, "login-url", &url);
        }
        Err(e) => {
            let msg = format!("login error: {e:#}");
            tracing::warn!("{msg}");
            mqtt.publish_event(ACCOUNT_ID, "status", &msg);
        }
    }
}

async fn do_complete_login(
    mqtt: &Arc<dyn MqttConnection>,
    bridge: &Arc<Bridge>,
    device_list: &Arc<DeviceListPublisher>,
    country_code: &str,
    callback_url: &str,
) {
    let (msg, ok) = match bridge.complete_login(country_code, callback_url).await {
        Ok(true) => ("logged in".to_string(), true),
        Ok(false) => (
            "login failed: no auth code in the callback URL".to_string(),
            false,
        ),
        Err(e) => (format!("login error: {e:#}"), false),
    };
    if ok {
        rusthinq_core::logging::log("bridge", &[&msg]);
        device_list.publish();
    } else {
        tracing::warn!("{msg}");
    }
    mqtt.publish_event(ACCOUNT_ID, "status", &msg);
}

async fn do_logout(
    mqtt: &Arc<dyn MqttConnection>,
    bridge: &Arc<Bridge>,
    device_list: &Arc<DeviceListPublisher>,
) {
    let (msg, ok) = match bridge.logout().await {
        Ok(()) => ("logged out".to_string(), true),
        Err(e) => (format!("logout error: {e:#}"), false),
    };
    if ok {
        rusthinq_core::logging::log("bridge", &[&msg]);
        device_list.publish();
    } else {
        tracing::warn!("{msg}");
    }
    mqtt.publish_event(ACCOUNT_ID, "status", &msg);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device_bridge::DeviceBridge;
    use crate::devmgr::{ConnectedDevice, Platform};
    use crate::known_devices::KnownDevices;
    use rusthinq_bridge::JsonStorage;
    use rusthinq_core::config::MqttConfig;
    use rusthinq_core::metadata::Metadata;

    fn test_sink() -> Arc<MqttSink> {
        MqttSink::new(MqttConfig {
            mqtt_url: "mqtt://127.0.0.1:1883".into(),
            rusthinq_prefix: "rusthinq".into(),
            mqtt_user: String::new(),
            mqtt_pass: String::new(),
            raw_prefix: None,
            raw: Default::default(),
            state_file: None,
        })
    }

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

    #[tokio::test]
    async fn enable_topic_routes_through_mqtt_to_status_publish() {
        let sink = test_sink();
        let mqtt: Arc<dyn MqttConnection> = sink.clone();
        let manager = DeviceManager::new();
        let storage = Arc::new(JsonStorage::new(
            std::env::temp_dir().join("rusthinq-bridge-control-test-empty"),
        ));
        let bridge = Bridge::new(storage);
        let device_bridge = DeviceBridge::new(sink.clone());
        let device_list = DeviceListPublisher::new(
            mqtt.clone(),
            manager.clone(),
            device_bridge,
            None,
            KnownDevices::new(None),
        );

        let published: Arc<rusthinq_util::sync::Mutex<Vec<(String, String)>>> =
            Arc::new(rusthinq_util::sync::Mutex::new(Vec::new()));
        let published2 = published.clone();
        sink.set_publish_fn(move |topic, payload, _retain| {
            published2.lock().push((
                topic.to_string(),
                String::from_utf8_lossy(payload).to_string(),
            ));
        });

        register(&sink, mqtt, manager, bridge, device_list);
        sink.handle_message("rusthinq/no-such-device/bridge/enable/set", b"");

        // The handler is dispatched via tokio::spawn; wait for it to publish.
        for _ in 0..50 {
            if !published.lock().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let events = published.lock();
        assert!(events.iter().any(|(topic, payload)| {
            topic == "rusthinq/no-such-device/bridge/status" && payload == "device not connected"
        }));
    }

    #[tokio::test]
    async fn do_enable_reports_not_connected_for_unknown_device() {
        let mqtt = rusthinq_core::MockMqttConnection::new();
        let mqtt_dyn: Arc<dyn MqttConnection> = mqtt.clone();
        let manager = DeviceManager::new();
        let storage = Arc::new(JsonStorage::new(
            std::env::temp_dir().join("rusthinq-bridge-control-test-2"),
        ));
        let bridge = Bridge::new(storage);
        let device_bridge = DeviceBridge::new(test_sink());
        let device_list = DeviceListPublisher::new(
            mqtt_dyn.clone(),
            manager.clone(),
            device_bridge,
            None,
            KnownDevices::new(None),
        );

        do_enable(&mqtt_dyn, &manager, &bridge, &device_list, "missing", "").await;

        let events = mqtt.device("missing").unwrap().events;
        assert!(events.contains(&(
            "bridge/status".to_string(),
            "device not connected".to_string()
        )));
    }

    #[tokio::test]
    async fn do_enable_fails_without_login_when_device_present() {
        let mqtt = rusthinq_core::MockMqttConnection::new();
        let mqtt_dyn: Arc<dyn MqttConnection> = mqtt.clone();
        let manager = DeviceManager::new();
        manager.accept(dummy_dev("dev-1"));
        let storage = Arc::new(JsonStorage::new(
            std::env::temp_dir().join("rusthinq-bridge-control-test-3"),
        ));
        let bridge = Bridge::new(storage);
        let device_bridge = DeviceBridge::new(test_sink());
        let device_list = DeviceListPublisher::new(
            mqtt_dyn.clone(),
            manager.clone(),
            device_bridge,
            None,
            KnownDevices::new(None),
        );

        do_enable(&mqtt_dyn, &manager, &bridge, &device_list, "dev-1", "401").await;

        let events = mqtt.device("dev-1").unwrap().events;
        assert!(
            events
                .iter()
                .any(|(topic, payload)| topic == "bridge/status" && payload == "not logged in")
        );
    }

    #[tokio::test]
    async fn do_disable_publishes_status_and_refreshes_device_list() {
        let mqtt = rusthinq_core::MockMqttConnection::new();
        let mqtt_dyn: Arc<dyn MqttConnection> = mqtt.clone();
        let manager = DeviceManager::new();
        manager.accept(dummy_dev("dev-2"));
        let storage = Arc::new(JsonStorage::new(
            std::env::temp_dir().join("rusthinq-bridge-control-test-4"),
        ));
        let bridge = Bridge::new(storage);
        let device_bridge = DeviceBridge::new(test_sink());
        let device_list = DeviceListPublisher::new(
            mqtt_dyn.clone(),
            manager.clone(),
            device_bridge,
            None,
            KnownDevices::new(None),
        );

        do_disable(&mqtt_dyn, &bridge, &device_list, "dev-2").await;

        let events = mqtt.device("dev-2").unwrap().events;
        assert!(events.contains(&("bridge/status".to_string(), "disabled".to_string())));
        assert!(mqtt.retained("devices").is_some());
    }

    // `do_login`/`do_complete_login` always make a real network call to LG's gateway
    // (there's no local-only fast path the way `enable` has "not logged in" —
    // see `Bridge::begin_login`/`complete_login`), so — matching `rusthinq-bridge`'s
    // own test suite, which doesn't exercise those two methods either — they're not
    // covered here. `do_logout` needs no network at all (just storage + live
    // sessions), so it's fully testable.

    #[tokio::test]
    async fn do_logout_publishes_status_and_refreshes_device_list() {
        let mqtt = rusthinq_core::MockMqttConnection::new();
        let mqtt_dyn: Arc<dyn MqttConnection> = mqtt.clone();
        let manager = DeviceManager::new();
        let storage = Arc::new(JsonStorage::new(
            std::env::temp_dir().join("rusthinq-bridge-control-test-logout"),
        ));
        let bridge = Bridge::new(storage);
        let device_bridge = DeviceBridge::new(test_sink());
        let device_list = DeviceListPublisher::new(
            mqtt_dyn.clone(),
            manager.clone(),
            device_bridge,
            None,
            KnownDevices::new(None),
        );

        do_logout(&mqtt_dyn, &bridge, &device_list).await;

        let events = mqtt.device(ACCOUNT_ID).unwrap().events;
        assert!(events.contains(&("status".to_string(), "logged out".to_string())));
        assert!(mqtt.retained("devices").is_some());
        assert!(!bridge.is_logged_in());
    }

    #[tokio::test]
    async fn logout_topic_routes_through_mqtt_to_the_account_status_topic() {
        let sink = test_sink();
        let mqtt: Arc<dyn MqttConnection> = sink.clone();
        let manager = DeviceManager::new();
        let storage = Arc::new(JsonStorage::new(
            std::env::temp_dir().join("rusthinq-bridge-control-test-logout-topic"),
        ));
        let bridge = Bridge::new(storage);
        let device_bridge = DeviceBridge::new(sink.clone());
        let device_list = DeviceListPublisher::new(
            mqtt.clone(),
            manager.clone(),
            device_bridge,
            None,
            KnownDevices::new(None),
        );

        let published: Arc<rusthinq_util::sync::Mutex<Vec<(String, String)>>> =
            Arc::new(rusthinq_util::sync::Mutex::new(Vec::new()));
        let published2 = published.clone();
        sink.set_publish_fn(move |topic, payload, _retain| {
            published2.lock().push((
                topic.to_string(),
                String::from_utf8_lossy(payload).to_string(),
            ));
        });

        register(&sink, mqtt, manager, bridge, device_list);
        // The account-level id ("bridge") must never be confused with a real
        // per-device enable/disable topic, and this exercises that exact routing
        // path end to end.
        sink.handle_message("rusthinq/bridge/logout/set", b"");

        for _ in 0..50 {
            if !published.lock().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let events = published.lock();
        assert!(
            events.iter().any(
                |(topic, payload)| topic == "rusthinq/bridge/status" && payload == "logged out"
            )
        );
    }
}
