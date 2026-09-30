//! Latest devices-panel snapshot, translated from `devlist.rs`'s retained
//! `<rusthinq_prefix>/devices` payload into the shape `assets/panel.js` expects, and
//! broadcast to every `/ws` connection via a `watch` channel (each new subscriber
//! gets the current value immediately, then every update after).

use rusthinq_util::sync::Mutex;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;

pub struct Shared {
    tx: watch::Sender<Value>,
    /// Last raw payload from devlist.rs's retained `<prefix>/devices` topic (or the
    /// empty default before the first one arrives), kept separately from `tx`'s
    /// already-translated value so `recompute` can rebuild the full snapshot when
    /// *either* a new payload arrives *or* `gui_mqtt_connected` changes on its own --
    /// the latter has nothing to do with devlist.rs and would otherwise have no way
    /// to reach the broadcast snapshot without re-deriving everything else in it.
    last_raw: Mutex<Value>,
    /// Whether rusthinq-gui's *own* MQTT client (`mqtt.rs`, a separate connection to
    /// the same broker from devlist.rs's `self.mqtt` on the rusthinq-cloud side) is
    /// currently connected. Set by `mqtt.rs`'s event loop directly -- this is the
    /// one piece of "Connectivity" state that never comes from a devlist.rs payload,
    /// since a payload can only arrive at all while this connection is already up.
    gui_mqtt_connected: AtomicBool,
}

impl Shared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tx: watch::channel(default_snapshot()).0,
            last_raw: Mutex::new(json!({})),
            gui_mqtt_connected: AtomicBool::new(false),
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<Value> {
        self.tx.subscribe()
    }

    pub fn current(&self) -> Value {
        self.tx.borrow().clone()
    }

    /// `payload` is `devlist.rs`'s retained snapshot:
    /// `{mqtt, bridgeLoggedIn, devices}`. Ignored if it isn't valid JSON -- a
    /// malformed retained message shouldn't crash the dashboard, just leave the
    /// last-known-good snapshot in place.
    pub fn set_snapshot(&self, payload: &[u8]) {
        let Ok(raw) = serde_json::from_slice::<Value>(payload) else {
            return;
        };
        *self.last_raw.lock() = raw;
        self.recompute();
    }

    /// Called by `mqtt.rs`'s event loop on every connect/disconnect of rusthinq-gui's
    /// own MQTT client -- see `gui_mqtt_connected`'s doc comment.
    pub fn set_gui_mqtt_connected(&self, connected: bool) {
        self.gui_mqtt_connected.store(connected, Ordering::SeqCst);
        self.recompute();
    }

    /// Rebuilds the broadcast snapshot from `last_raw` + `gui_mqtt_connected` and
    /// sends it to every `/ws` subscriber.
    fn recompute(&self) {
        let raw = self.last_raw.lock().clone();
        // Present (even if `null`) means devlist.rs actually reported bridge state --
        // `null` there means rusthinq-cloud's own `[bridge]` config section is absent
        // (disabled by configuration, not merely "not logged in yet"), which
        // `panel.js` needs to tell apart from `loggedIn: false`. Only the *key*'s
        // absence entirely (before the first snapshot ever arrives) collapses to no
        // `bridge` object at all.
        let bridge = raw
            .get("bridgeLoggedIn")
            .cloned()
            .map(|logged_in| json!({ "loggedIn": logged_in }));
        let translated = json!({
            "mqtt": raw.get("mqtt").cloned().unwrap_or(Value::Bool(false)),
            "guiMqtt": self.gui_mqtt_connected.load(Ordering::SeqCst),
            "bridge": bridge,
            "devices": raw.get("devices").cloned().unwrap_or_else(|| json!({})),
            "version": rusthinq_core::version::VERSION,
            // Passed through verbatim from devlist.rs's snapshot -- which of
            // rusthinq-cloud's optional features are actually compiled into *this*
            // running binary. rusthinq-gui talks to it only over MQTT and has no
            // other way to know.
            "features": raw.get("features").cloned().unwrap_or_else(|| json!({})),
        });
        // `send` (unlike `send_replace`) is a no-op when there are no receivers
        // yet -- e.g. the very first `<prefix>/devices` message arriving before any
        // browser has opened `/ws` -- which would otherwise lose that snapshot.
        self.tx.send_replace(translated);
    }

    /// Whether `id` is in the current devices snapshot, and its `model` (used as
    /// `assets/monitor.js`'s `meta.modelId`) if so -- drives `/device`'s
    /// online/offline status without the GUI needing its own device-manager view.
    pub fn device_online(&self, id: &str) -> Option<String> {
        let current = self.current();
        let device = current.get("devices")?.get(id)?;
        if device.get("online").and_then(Value::as_bool) != Some(true) {
            return None;
        }
        device.get("model")?.as_str().map(str::to_string)
    }
}

fn default_snapshot() -> Value {
    json!({
        "mqtt": false,
        "guiMqtt": false,
        "bridge": Value::Null,
        "devices": {},
        "version": rusthinq_core::version::VERSION,
        "features": {},
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_snapshot_wraps_bridge_logged_in_and_passes_through_the_rest() {
        let shared = Shared::new();
        shared.set_snapshot(
            br#"{"mqtt":true,"bridgeLoggedIn":true,"devices":{"d1":{"model":"RAC"}}}"#,
        );
        let got = shared.current();
        assert_eq!(got["mqtt"], json!(true));
        assert_eq!(got["bridge"], json!({"loggedIn": true}));
        assert_eq!(got["devices"]["d1"]["model"], json!("RAC"));
    }

    /// `assets/panel.js` uses `features.scripting` to decide
    /// whether a device's "no script handler" warning means anything --
    /// this must survive the translation from devlist.rs's raw shape unchanged.
    #[test]
    fn set_snapshot_passes_through_features() {
        let shared = Shared::new();
        shared.set_snapshot(
            br#"{"mqtt":true,"bridgeLoggedIn":null,"devices":{},"features":{"bridge":true,"scripting":true}}"#,
        );
        let got = shared.current();
        assert_eq!(got["features"]["bridge"], json!(true));
        assert_eq!(got["features"]["scripting"], json!(true));
    }

    /// A raw payload from before this field existed (or one that simply omits it)
    /// must not crash the dashboard -- fall back to an empty object rather than
    /// `null`, so the frontend's `features.scripting` reads `undefined` instead of
    /// throwing on `null.scripting`.
    #[test]
    fn set_snapshot_defaults_features_to_an_empty_object_when_absent() {
        let shared = Shared::new();
        shared.set_snapshot(br#"{"mqtt":true,"bridgeLoggedIn":null,"devices":{}}"#);
        assert_eq!(shared.current()["features"], json!({}));
    }

    /// `guiMqtt` reflects rusthinq-gui's own MQTT connection (set directly by
    /// `mqtt.rs`, never part of a devlist.rs payload) -- distinct from `mqtt`,
    /// which is rusthinq-cloud's own connection as reported by devlist.rs. Setting
    /// it must broadcast on its own, without needing a `set_snapshot` call, and
    /// must survive one.
    #[test]
    fn set_gui_mqtt_connected_updates_independently_of_devlist_payloads() {
        let shared = Shared::new();
        assert_eq!(shared.current()["guiMqtt"], json!(false));

        shared.set_gui_mqtt_connected(true);
        assert_eq!(shared.current()["guiMqtt"], json!(true));

        // A devlist.rs payload (which knows nothing about guiMqtt) must not reset it.
        shared.set_snapshot(br#"{"mqtt":true,"bridgeLoggedIn":null,"devices":{}}"#);
        assert_eq!(shared.current()["guiMqtt"], json!(true));
        assert_eq!(shared.current()["mqtt"], json!(true));

        shared.set_gui_mqtt_connected(false);
        assert_eq!(shared.current()["guiMqtt"], json!(false));
        // Unrelated fields from the last devlist.rs payload must still be there.
        assert_eq!(shared.current()["mqtt"], json!(true));
    }

    /// `bridgeLoggedIn: null` means rusthinq-cloud's own `[bridge]` config section is
    /// absent -- disabled by configuration, not merely "not logged in yet" -- and
    /// `panel.js` shows a distinct "Disabled" status for it (`bridge.loggedIn ===
    /// null`, vs. `false` for "configured but not logged in"). It must still come
    /// through as `{loggedIn: null}`, not collapse to a top-level `null` the way
    /// "no snapshot has arrived yet" does (see `default_snapshot`).
    #[test]
    fn set_snapshot_reports_bridge_logged_in_null_as_disabled_not_absent() {
        let shared = Shared::new();
        shared.set_snapshot(br#"{"mqtt":false,"bridgeLoggedIn":null,"devices":{}}"#);
        assert_eq!(shared.current()["bridge"], json!({"loggedIn": null}));
    }

    /// Before the first devlist.rs payload ever arrives, there's no `bridgeLoggedIn`
    /// key at all (`last_raw` starts as `{}`) -- that's the one case that still
    /// collapses to a top-level `null` `bridge`, matching `default_snapshot`.
    #[test]
    fn bridge_is_null_before_any_snapshot_has_arrived() {
        let shared = Shared::new();
        assert_eq!(shared.current()["bridge"], Value::Null);
    }

    #[test]
    fn set_snapshot_ignores_malformed_json() {
        let shared = Shared::new();
        shared.set_snapshot(br#"{"mqtt":true,"#);
        assert_eq!(shared.current(), default_snapshot());
    }

    #[test]
    fn device_online_reads_model_from_the_current_snapshot() {
        let shared = Shared::new();
        assert_eq!(shared.device_online("d1"), None);
        shared.set_snapshot(
            br#"{"mqtt":true,"bridgeLoggedIn":null,"devices":{"d1":{"model":"RAC_056905_WW","online":true}}}"#,
        );
        assert_eq!(
            shared.device_online("d1"),
            Some("RAC_056905_WW".to_string())
        );
        assert_eq!(shared.device_online("missing"), None);
    }
}

#[cfg(test)]
mod regression_regression {
    #[test]
    fn regression_offline_device_is_not_online() {
        let state = super::Shared::new();
        state.set_snapshot(br#"{"mqtt":true,"devices":{"d":{"model":"M","online":false}}}"#);
        assert_eq!(state.device_online("d"), None);
    }
}
