//! Latest devices-panel snapshot, translated from `devlist.rs`'s retained
//! `<rusthinq_prefix>/devices` payload into the shape `assets/panel.js` expects, and
//! broadcast to every `/ws` connection via a `watch` channel (each new subscriber
//! gets the current value immediately, then every update after).

use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::watch;

pub struct Shared {
    tx: watch::Sender<Value>,
}

impl Shared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tx: watch::channel(default_snapshot()).0,
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
        let bridge = raw
            .get("bridgeLoggedIn")
            .filter(|v| !v.is_null())
            .map(|logged_in| json!({ "loggedIn": logged_in }));
        let translated = json!({
            "mqtt": raw.get("mqtt").cloned().unwrap_or(Value::Bool(false)),
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
        current
            .get("devices")?
            .get(id)?
            .get("model")?
            .as_str()
            .map(str::to_string)
    }
}

fn default_snapshot() -> Value {
    json!({
        "mqtt": false,
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

    /// `assets/panel.js` uses `features.native`/`features.scripting` to decide
    /// whether a device's "no native or script handler" warning means anything --
    /// this must survive the translation from devlist.rs's raw shape unchanged.
    #[test]
    fn set_snapshot_passes_through_features() {
        let shared = Shared::new();
        shared.set_snapshot(
            br#"{"mqtt":true,"bridgeLoggedIn":null,"devices":{},"features":{"bridge":true,"native":false,"scripting":true}}"#,
        );
        let got = shared.current();
        assert_eq!(got["features"]["bridge"], json!(true));
        assert_eq!(got["features"]["native"], json!(false));
        assert_eq!(got["features"]["scripting"], json!(true));
    }

    /// A raw payload from before this field existed (or one that simply omits it)
    /// must not crash the dashboard -- fall back to an empty object rather than
    /// `null`, so the frontend's `features.native` reads `undefined` instead of
    /// throwing on `null.native`.
    #[test]
    fn set_snapshot_defaults_features_to_an_empty_object_when_absent() {
        let shared = Shared::new();
        shared.set_snapshot(br#"{"mqtt":true,"bridgeLoggedIn":null,"devices":{}}"#);
        assert_eq!(shared.current()["features"], json!({}));
    }

    #[test]
    fn set_snapshot_leaves_bridge_null_when_bridge_logged_in_is_null() {
        let shared = Shared::new();
        shared.set_snapshot(br#"{"mqtt":false,"bridgeLoggedIn":null,"devices":{}}"#);
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
            br#"{"mqtt":true,"bridgeLoggedIn":null,"devices":{"d1":{"model":"RAC_056905_WW"}}}"#,
        );
        assert_eq!(
            shared.device_online("d1"),
            Some("RAC_056905_WW".to_string())
        );
        assert_eq!(shared.device_online("missing"), None);
    }
}
