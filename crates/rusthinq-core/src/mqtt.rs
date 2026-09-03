//! MQTT control-plane connection trait — transport primitives only, consumer-neutral.
//!
//! No device-modeling schema and no downstream-integration topic convention (Home
//! Assistant MQTT discovery or anything else) lives here.
//! A device publishes named properties at `<rusthinq_prefix>/<id>/<property>`
//! (`publish_property`) or one-shot events (`publish_event`); a caller with its own
//! topic scheme entirely (an HA-discovery renderer, eventually written in Rhai, or
//! anything else) uses `publish_raw`, which has no opinion on topic or payload shape.
//! Reconnect resync (`on_discovery`/`emit_discovery`) is generic too — it is up to
//! whatever wires a real connection (see rusthinq-cloud's `mqtt_client.rs`) to decide what
//! external signal (an HA birth message, or nothing at all) should trigger it.

use crate::config::MqttConfig;
use rusthinq_util::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Trait for publishing to the MQTT control plane (real connection or mock).
pub trait MqttConnection: Send + Sync {
    fn publish_property(&self, id: &str, property: &str, value: &str);
    /// Fire a one-shot event payload. Not retained.
    fn publish_event(&self, id: &str, topic_suffix: &str, payload: &str);
    /// Publish a retained payload not scoped to any device, at
    /// `<rusthinq_prefix>/<topic_suffix>` — e.g. a management snapshot meant to be
    /// available immediately to a client that subscribes after the fact.
    fn publish_retained(&self, topic_suffix: &str, payload: &str);
    /// Publish to an exact topic string, bypassing the `<rusthinq_prefix>/<id>/...`
    /// scoping every other method here applies — for callers with their own prefix
    /// or topic convention entirely (the raw wire-frame observer/inject bus and device
    /// simulator use `MqttConfig::raw_prefix`; a future HA-discovery renderer would use
    /// this too, publishing to HA's own `homeassistant/...` topic space).
    fn publish_raw(&self, topic: &str, payload: &[u8], retain: bool);
    /// Clear every retained `<rusthinq_prefix>/<id>/<property>` topic ever published
    /// for `id` via `publish_property` (publishes an empty retained payload to each,
    /// the standard MQTT way to clear a retained message) — call this once a device is
    /// permanently gone, so its last-known state doesn't sit in the broker forever.
    /// Does not touch `publish_event`/`publish_retained`/`publish_raw` topics (never
    /// retained, or not scoped to one device).
    fn clear_retained(&self, id: &str);
    fn is_connected(&self) -> bool;
    /// The `<rusthinq_prefix>/<id>` base topic `publish_property`/`publish_event`
    /// scope under — for a caller that needs to *reference* one of those topics from
    /// outside them (e.g. a Home Assistant discovery config's `state_topic`/
    /// `command_topic`, built via `publish_raw` since discovery itself lives outside
    /// `rusthinq_prefix`), without hand-duplicating the prefix.
    fn device_topic(&self, id: &str) -> String;
}

/// `(topic, payload, retain)` publish callback, or a `set`-message handler keyed
/// `(device_id, property, value)` depending on which field uses it.
type SetHandler = Box<dyn Fn(&str, &str, &str) + Send + Sync>;
type PublishFn = Box<dyn Fn(&str, &[u8], bool) + Send + Sync>;

/// Mock MQTT control-plane connection for unit tests.
#[derive(Default)]
pub struct MockMqttConnection {
    inner: Mutex<MockMqttInner>,
    set_handlers: Mutex<Vec<SetHandler>>,
}

#[derive(Default)]
struct MockMqttInner {
    devices: HashMap<String, MockDeviceInfo>,
    retained: HashMap<String, String>,
    raw: Vec<(String, Vec<u8>, bool)>,
}

#[derive(Debug, Clone, Default)]
pub struct MockDeviceInfo {
    pub availability: Option<String>,
    pub properties: HashMap<String, String>,
    /// (topic_suffix, payload) non-retained events
    pub events: Vec<(String, String)>,
}

impl MockMqttConnection {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn on_set_property<F>(&self, f: F)
    where
        F: Fn(&str, &str, &str) + Send + Sync + 'static,
    {
        self.set_handlers.lock().push(Box::new(f));
    }

    pub fn devices(&self) -> HashMap<String, MockDeviceInfo> {
        self.inner.lock().devices.clone()
    }

    pub fn device(&self, id: &str) -> Option<MockDeviceInfo> {
        self.inner.lock().devices.get(id).cloned()
    }

    pub fn retained(&self, topic_suffix: &str) -> Option<String> {
        self.inner.lock().retained.get(topic_suffix).cloned()
    }

    pub fn raw_publishes(&self) -> Vec<(String, Vec<u8>, bool)> {
        self.inner.lock().raw.clone()
    }

    pub fn emit_set_property(&self, id: &str, prop: &str, value: &str) {
        for h in self.set_handlers.lock().iter() {
            h(id, prop, value);
        }
    }
}

impl MqttConnection for MockMqttConnection {
    fn publish_property(&self, id: &str, property: &str, value: &str) {
        let mut inner = self.inner.lock();
        let entry = inner.devices.entry(id.to_string()).or_default();
        if property == "availability" {
            entry.availability = Some(value.to_string());
        } else {
            entry
                .properties
                .insert(property.to_string(), value.to_string());
        }
    }

    fn publish_event(&self, id: &str, topic_suffix: &str, payload: &str) {
        let mut inner = self.inner.lock();
        let entry = inner.devices.entry(id.to_string()).or_default();
        entry
            .events
            .push((topic_suffix.to_string(), payload.to_string()));
    }

    fn publish_retained(&self, topic_suffix: &str, payload: &str) {
        self.inner
            .lock()
            .retained
            .insert(topic_suffix.to_string(), payload.to_string());
    }

    fn publish_raw(&self, topic: &str, payload: &[u8], retain: bool) {
        self.inner
            .lock()
            .raw
            .push((topic.to_string(), payload.to_vec(), retain));
    }

    fn clear_retained(&self, id: &str) {
        self.inner.lock().devices.remove(id);
    }

    fn is_connected(&self) -> bool {
        true
    }

    fn device_topic(&self, id: &str) -> String {
        format!("rusthinq/{id}")
    }
}

/// Reads the persisted `id -> property names` map from `path`, if any (missing or
/// unreadable/corrupt file just means "nothing known yet" — never fatal).
fn load_published_topics(path: &std::path::Path) -> HashMap<String, HashSet<String>> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_published_topics(path: &std::path::Path, topics: &HashMap<String, HashSet<String>>) {
    match serde_json::to_string(topics) {
        Ok(json) => {
            if let Err(e) = std::fs::write(path, json) {
                tracing::warn!(
                    target: "rusthinq_mqtt",
                    error = %e,
                    path = %path.display(),
                    "failed to persist mqtt published-topics state"
                );
            }
        }
        Err(e) => {
            tracing::warn!(target: "rusthinq_mqtt", error = %e, "failed to serialize mqtt published-topics state");
        }
    }
}

/// Shared sink for the real MQTT control-plane connection (filled by rusthinq-cloud).
pub struct MqttSink {
    pub config: MqttConfig,
    pub published_availability: Mutex<HashSet<String>>,
    /// `id -> property names ever published via publish_property`, so a permanently
    /// gone device's retained topics can be cleared later (`clear_retained`) even
    /// though property names are otherwise unknown to this crate. Persisted to
    /// `config.state_file` (if set) so a daemon restart doesn't forget what an
    /// already-connected device published in a previous run.
    published_topics: Mutex<HashMap<String, HashSet<String>>>,
    state_file: Option<std::path::PathBuf>,
    /// Callback to publish raw MQTT: (topic, payload, retain)
    pub publish_fn: Mutex<Option<PublishFn>>,
    pub set_handlers: Mutex<Vec<SetHandler>>,
    pub discovery_handlers: Mutex<Vec<Box<dyn Fn() + Send + Sync>>>,
    pub connected: Mutex<bool>,
}

impl MqttSink {
    pub fn new(config: MqttConfig) -> Arc<Self> {
        let state_file = config.state_file.as_ref().map(std::path::PathBuf::from);
        let published_topics = state_file
            .as_deref()
            .map(load_published_topics)
            .unwrap_or_default();
        Arc::new(Self {
            config,
            published_availability: Mutex::new(HashSet::new()),
            published_topics: Mutex::new(published_topics),
            state_file,
            publish_fn: Mutex::new(None),
            set_handlers: Mutex::new(Vec::new()),
            discovery_handlers: Mutex::new(Vec::new()),
            connected: Mutex::new(false),
        })
    }

    pub fn set_publish_fn<F>(&self, f: F)
    where
        F: Fn(&str, &[u8], bool) + Send + Sync + 'static,
    {
        *self.publish_fn.lock() = Some(Box::new(f));
    }

    fn do_publish(&self, topic: &str, payload: &[u8], retain: bool) {
        if let Some(f) = self.publish_fn.lock().as_ref() {
            f(topic, payload, retain);
        } else {
            // Once is enough — startup race if devices connect before the MQTT client installs publish_fn.
            static WARNED: AtomicBool = AtomicBool::new(false);
            if !WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    target: "rusthinq_mqtt",
                    %topic,
                    retain,
                    "MQTT publish_fn not set; dropping publish (will not warn again)"
                );
            }
        }
    }

    pub fn on_set_property<F>(&self, f: F)
    where
        F: Fn(&str, &str, &str) + Send + Sync + 'static,
    {
        self.set_handlers.lock().push(Box::new(f));
    }

    /// Register a callback to run on `emit_discovery()` — e.g. republish every
    /// device's last known config. What (if anything) triggers `emit_discovery` is up
    /// to whoever owns the real connection (see rusthinq-cloud's `mqtt_client.rs`); this
    /// sink has no opinion on it.
    pub fn on_discovery<F>(&self, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.discovery_handlers.lock().push(Box::new(f));
    }

    pub fn emit_discovery(&self) {
        for (i, h) in self.discovery_handlers.lock().iter().enumerate() {
            crate::panic_guard::guard(&format!("discovery handler #{i}"), h);
        }
    }

    pub fn handle_message(&self, topic: &str, message: &[u8], retain: bool) {
        let msg = String::from_utf8_lossy(message);
        // Two independent topic namespaces can carry `<id>/.../set` commands: the core
        // control plane (rusthinq_prefix, always on) and the raw/debug surface
        // (raw_prefix, only when configured — see MqttConfig::raw_prefix). Try both;
        // a topic only ever matches one of them in practice; there is no risk of a raw
        // topic on raw_prefix reaching the `<id>/availability` handling below, since
        // raw_bus/sim_device topics are never exactly two segments long.
        let prefixes = std::iter::once(self.config.rusthinq_prefix.as_str())
            .chain(self.config.raw_prefix.as_deref())
            .filter(|p| !p.is_empty());
        for p in prefixes {
            let prefix = format!("{p}/");
            let Some(rest) = topic.strip_prefix(&prefix) else {
                continue;
            };
            let parts: Vec<&str> = rest.split('/').collect();
            if parts.len() >= 3 && parts[parts.len() - 1] == "set" {
                let id = parts[0];
                let prop = parts[1..parts.len() - 1].join("/");
                // This runs on the single shared MQTT event loop task — every
                // device's set_property calls funnel through here. Guard each handler
                // so one device's bad `msg` can't take the whole MQTT connection down
                // with it (see panic_guard.rs).
                for h in self.set_handlers.lock().iter() {
                    crate::panic_guard::guard(
                        &format!("set_property handler for {id}/{prop}"),
                        || h(id, &prop, &msg),
                    );
                }
            }
            if parts.len() == 2
                && parts[1] == "availability"
                && msg == "online"
                && retain
                && !self.published_availability.lock().contains(parts[0])
            {
                self.do_publish(topic, b"offline", true);
            }
        }
    }
}

impl MqttConnection for MqttSink {
    fn publish_property(&self, id: &str, property: &str, value: &str) {
        if property == "availability" {
            self.published_availability.lock().insert(id.to_string());
        }
        let is_new_property = self
            .published_topics
            .lock()
            .entry(id.to_string())
            .or_default()
            .insert(property.to_string());
        if is_new_property && let Some(path) = &self.state_file {
            save_published_topics(path, &self.published_topics.lock());
        }
        let topic = format!("{}/{}/{}", self.config.rusthinq_prefix, id, property);
        self.do_publish(&topic, value.as_bytes(), true);
    }

    fn publish_event(&self, id: &str, topic_suffix: &str, payload: &str) {
        let topic = format!("{}/{}/{}", self.config.rusthinq_prefix, id, topic_suffix);
        self.do_publish(&topic, payload.as_bytes(), false);
    }

    fn publish_retained(&self, topic_suffix: &str, payload: &str) {
        let topic = format!("{}/{}", self.config.rusthinq_prefix, topic_suffix);
        self.do_publish(&topic, payload.as_bytes(), true);
    }

    fn publish_raw(&self, topic: &str, payload: &[u8], retain: bool) {
        self.do_publish(topic, payload, retain);
    }

    fn clear_retained(&self, id: &str) {
        let properties = self.published_topics.lock().remove(id);
        self.published_availability.lock().remove(id);
        if let Some(path) = &self.state_file {
            save_published_topics(path, &self.published_topics.lock());
        }
        if let Some(properties) = properties {
            for property in properties {
                let topic = format!("{}/{}/{}", self.config.rusthinq_prefix, id, property);
                self.do_publish(&topic, b"", true);
            }
        }
    }

    fn is_connected(&self) -> bool {
        *self.connected.lock()
    }

    fn device_topic(&self, id: &str) -> String {
        format!("{}/{id}", self.config.rusthinq_prefix)
    }
}

#[cfg(test)]
mod ha_mqtt_sink_tests {
    use super::*;
    use crate::config::MqttConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_cfg() -> MqttConfig {
        MqttConfig {
            mqtt_url: "mqtt://127.0.0.1:1883".into(),
            rusthinq_prefix: "rusthinq".into(),
            mqtt_user: String::new(),
            mqtt_pass: String::new(),
            raw_prefix: None,
            state_file: None,
        }
    }

    #[test]
    fn set_property_handlers_fire_on_handle_message() {
        let sink = MqttSink::new(test_cfg());
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        sink.on_set_property(move |id, prop, val| {
            assert_eq!(id, "dev1");
            assert_eq!(prop, "climate-mode");
            assert_eq!(val, "heat");
            h.fetch_add(1, Ordering::SeqCst);
        });
        sink.handle_message("rusthinq/dev1/climate-mode/set", b"heat", false);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    /// The failure-isolation guarantee this crate depends on: `handle_message` runs
    /// on a single task shared by every connected device (the MQTT event loop).
    /// A panic in one device's set_property handler (a bad packet, a future Rhai
    /// script bug) must not stop *other* handlers registered on the same sink, and
    /// must not stop this sink from processing the *next* message either — i.e. it
    /// cannot take the whole MQTT connection down with it.
    #[test]
    fn one_panicking_set_property_handler_does_not_break_others_or_future_messages() {
        let sink = MqttSink::new(test_cfg());
        let healthy_hits = Arc::new(AtomicUsize::new(0));

        sink.on_set_property(|_id, _prop, _val| {
            panic!("simulated bug in one device's handler");
        });
        let h = healthy_hits.clone();
        sink.on_set_property(move |_id, _prop, _val| {
            h.fetch_add(1, Ordering::SeqCst);
        });

        // First message: the panicking handler runs and is contained, but the
        // healthy handler registered after it must still fire for this same message.
        sink.handle_message("rusthinq/dev1/power/set", b"ON", false);
        assert_eq!(
            healthy_hits.load(Ordering::SeqCst),
            1,
            "a sibling handler must still run when an earlier handler panics"
        );

        // Second message, different device: the sink/task must still be alive and
        // dispatching normally — nothing about the earlier panic should have broken
        // handle_message itself.
        sink.handle_message("rusthinq/dev2/power/set", b"OFF", false);
        assert_eq!(
            healthy_hits.load(Ordering::SeqCst),
            2,
            "the shared dispatch path must keep working for other devices after a panic"
        );
    }

    #[test]
    fn publish_fn_receives_publish_property() {
        let sink = MqttSink::new(test_cfg());
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        sink.set_publish_fn(move |topic, payload, retain| {
            assert!(topic.contains("rusthinq/dev1/power"));
            assert_eq!(payload, b"ON");
            assert!(retain);
            h.fetch_add(1, Ordering::SeqCst);
        });
        use crate::mqtt::MqttConnection;
        sink.publish_property("dev1", "power", "ON");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn publish_raw_bypasses_rusthinq_prefix_scoping() {
        use crate::mqtt::MqttConnection;
        let sink = MqttSink::new(test_cfg());
        #[allow(clippy::type_complexity)]
        let pubs: Arc<Mutex<Vec<(String, Vec<u8>, bool)>>> = Arc::new(Mutex::new(Vec::new()));
        let p = pubs.clone();
        sink.set_publish_fn(move |topic, payload, retain| {
            p.lock().push((topic.to_string(), payload.to_vec(), retain));
        });

        // Consumer-neutral escape hatch: any topic, any shape, no rusthinq_prefix
        // scoping applied — this is what a future HA-discovery renderer (or anything
        // else with its own topic convention) would use.
        sink.publish_raw("homeassistant/device/some-node/config", b"{}", true);

        let logged = pubs.lock().clone();
        assert_eq!(
            logged,
            vec![(
                "homeassistant/device/some-node/config".to_string(),
                b"{}".to_vec(),
                true
            )]
        );
    }

    #[test]
    fn emit_discovery_runs_every_registered_handler() {
        let sink = MqttSink::new(test_cfg());
        let hits = Arc::new(AtomicUsize::new(0));
        let h1 = hits.clone();
        sink.on_discovery(move || {
            h1.fetch_add(1, Ordering::SeqCst);
        });
        let h2 = hits.clone();
        sink.on_discovery(move || {
            h2.fetch_add(10, Ordering::SeqCst);
        });

        // Nothing in this sink decides *when* this fires — see mqtt_client.rs for the
        // actual trigger (reconnect, or an integration's own birth message).
        sink.emit_discovery();
        assert_eq!(hits.load(Ordering::SeqCst), 11);
    }

    #[test]
    fn clear_retained_republishes_empty_for_every_tracked_property() {
        use crate::mqtt::MqttConnection;
        let sink = MqttSink::new(test_cfg());
        #[allow(clippy::type_complexity)]
        let pubs: Arc<Mutex<Vec<(String, Vec<u8>, bool)>>> = Arc::new(Mutex::new(Vec::new()));
        let p = pubs.clone();
        sink.set_publish_fn(move |topic, payload, retain| {
            p.lock().push((topic.to_string(), payload.to_vec(), retain));
        });

        sink.publish_property("dev1", "power", "ON");
        sink.publish_property("dev1", "temperature", "23.5");
        pubs.lock().clear(); // only care about what clear_retained itself publishes

        sink.clear_retained("dev1");

        let mut cleared = pubs.lock().clone();
        cleared.sort();
        let mut expected = vec![
            ("rusthinq/dev1/power".to_string(), Vec::new(), true),
            ("rusthinq/dev1/temperature".to_string(), Vec::new(), true),
        ];
        expected.sort();
        assert_eq!(
            cleared, expected,
            "every property ever published for the id must be cleared with an empty retained payload"
        );
    }

    #[test]
    fn published_topics_survive_a_restart_via_state_file() {
        use crate::mqtt::MqttConnection;
        let path = std::env::temp_dir().join(format!(
            "rusthinq-mqtt-state-test-{}-{}.json",
            std::process::id(),
            line!()
        ));
        let _ = std::fs::remove_file(&path);

        let mut cfg = test_cfg();
        cfg.state_file = Some(path.to_string_lossy().into());
        let sink = MqttSink::new(cfg.clone());
        sink.publish_property("dev1", "power", "ON");

        // Simulate a daemon restart: a brand new MqttSink reading the same state
        // file, with no publish_property calls yet in this "session".
        let restarted = MqttSink::new(cfg);
        #[allow(clippy::type_complexity)]
        let pubs: Arc<Mutex<Vec<(String, Vec<u8>, bool)>>> = Arc::new(Mutex::new(Vec::new()));
        let p = pubs.clone();
        restarted.set_publish_fn(move |topic, payload, retain| {
            p.lock().push((topic.to_string(), payload.to_vec(), retain));
        });
        restarted.clear_retained("dev1");

        assert_eq!(
            pubs.lock().clone(),
            vec![("rusthinq/dev1/power".to_string(), Vec::new(), true)],
            "a restarted sink must still know what a previous run published, via the state file"
        );

        let _ = std::fs::remove_file(&path);
    }
}
