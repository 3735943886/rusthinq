//! `DeviceCtx` — the object a `.rhai` script's `ctx` parameter is bound to.
//!
//! Deliberately narrow: a script can only publish/send for *its own* device (the one
//! `ScriptedDevice` was built for), scoped through `MqttConnection::publish_property`/
//! `publish_event` exactly like a native handler would. It does not expose
//! `publish_retained`/`publish_raw` — those are for callers with no per-device scope
//! (devlist snapshots, the raw wire-frame bus), not a single device's script.

use rhai::EvalAltResult;
use rusthinq_core::metadata::Metadata;
use rusthinq_core::mqtt::MqttConnection;
use rusthinq_core::thinq::{Thinq1Device, Thinq2Device};
use std::sync::Arc;

/// The two wire protocols a script's device might be running on. `Thinq1Device` has no
/// raw-bytes send (ThinQ1 is JSON-over-length-prefixed-frame) and `Thinq2Device` has no
/// JSON send — `DeviceCtx::send_raw`/`send_json` are each a no-op on the platform that
/// doesn't support them, rather than being unavailable at script-compile time.
#[derive(Clone)]
pub enum DeviceHandle {
    T1(Arc<dyn Thinq1Device>),
    T2(Arc<dyn Thinq2Device>),
}

#[derive(Clone)]
pub struct DeviceCtx {
    id: Arc<str>,
    meta: Metadata,
    mqtt: Arc<dyn MqttConnection>,
    device: DeviceHandle,
    /// A `rhai::Dynamic::into_shared()` map holding this device's persistent state
    /// (accumulated TLV values, "have we published discovery yet") — the same value
    /// (not a snapshot) on every future call for this device, which is how it
    /// survives across separate `on_data`/`on_set_property`/... calls the way a plain
    /// top-level `let` cannot (every call re-runs the script's top-level statements,
    /// `eval_ast: true`, needed so a top-level `import` keeps working call after
    /// call — which would just as readily reset a `let raw_clip_state = #{};` back to
    /// empty on every single call).
    ///
    /// Exposed only through `state_get`/`state_set`/`state_has`, never as a `Dynamic`
    /// a script could index itself: returning an already-shared `Dynamic` from a
    /// registered function and then index-assigning into it from script code silently
    /// *un-shares* it in rhai 1.26.0 (verified — see
    /// `engine.rs::tests::a_native_setter_method_mutates_the_original_shared_value_where_returning_it_does_not`),
    /// so the mutation is lost. Doing the get/set natively, locking this field
    /// directly, sidesteps that entirely.
    state: rhai::Dynamic,
}

impl DeviceCtx {
    pub fn new(
        id: Arc<str>,
        meta: Metadata,
        mqtt: Arc<dyn MqttConnection>,
        device: DeviceHandle,
        state: rhai::Dynamic,
    ) -> Self {
        Self {
            id,
            meta,
            mqtt,
            device,
            state,
        }
    }

    /// Read one key of this device's persistent state (`()` if unset — the same
    /// convention as indexing a rhai map with a missing key).
    pub fn state_get(&mut self, key: String) -> rhai::Dynamic {
        self.state
            .read_lock::<rhai::Map>()
            .and_then(|map| map.get(key.as_str()).cloned())
            .unwrap_or(rhai::Dynamic::UNIT)
    }

    /// Write one key of this device's persistent state.
    pub fn state_set(&mut self, key: String, value: rhai::Dynamic) {
        if let Some(mut map) = self.state.write_lock::<rhai::Map>() {
            map.insert(key.into(), value);
        }
    }

    /// True if `key` is currently set in this device's persistent state.
    pub fn state_has(&mut self, key: String) -> bool {
        self.state
            .read_lock::<rhai::Map>()
            .is_some_and(|map| map.contains_key(key.as_str()))
    }

    pub fn id(&mut self) -> String {
        self.id.to_string()
    }

    pub fn model_id(&mut self) -> String {
        self.meta.model_id.clone()
    }

    pub fn model_name(&mut self) -> String {
        self.meta.model_name.clone()
    }

    pub fn sw_version(&mut self) -> String {
        self.meta.sw_version.clone().unwrap_or_default()
    }

    /// The `<rusthinq_prefix>/<id>` base topic `publish_property`/`publish_event`
    /// scope under — for building a `state_topic`/`command_topic` string to embed in
    /// a discovery config published via `publish_raw` (which is outside that scoping
    /// itself, so can't just reuse it implicitly).
    pub fn device_topic(&mut self) -> String {
        self.mqtt.device_topic(&self.id)
    }

    pub fn publish_property(&mut self, property: String, value: String) {
        self.mqtt.publish_property(&self.id, &property, &value);
    }

    pub fn publish_event(&mut self, topic_suffix: String, payload: String) {
        self.mqtt.publish_event(&self.id, &topic_suffix, &payload);
    }

    /// Publish to an exact topic, bypassing `rusthinq_prefix` scoping entirely —
    /// for a script that needs to speak some other convention's topic space (a
    /// downstream integration's own discovery-config topic, say). Delegates
    /// straight to `MqttConnection::publish_raw`, the same primitive
    /// `raw_bus.rs`/`sim_device.rs` already use for their own prefix. Unlike
    /// `publish_property`, retain is the caller's choice — a discovery config
    /// published with `retain: true` is what makes it survive a downstream
    /// consumer's own restart with no extra resync mechanism needed on either side.
    pub fn publish_raw(&mut self, topic: String, payload: String, retain: bool) {
        self.mqtt.publish_raw(&topic, payload.as_bytes(), retain);
    }

    /// Send raw bytes to the appliance. T2 only — a no-op (logged) on T1.
    pub fn send_raw(&mut self, data: Vec<u8>) {
        match &self.device {
            DeviceHandle::T2(dev) => dev.send_packet(&data),
            DeviceHandle::T1(_) => {
                tracing::warn!(
                    target: "rusthinq_scripting",
                    id = %self.id,
                    "ctx.send_raw() called on a ThinQ1 device (no raw send on this platform) — ignored"
                );
            }
        }
    }

    /// Send a JSON control body to the appliance. T1 only — a no-op (logged) on T2.
    pub fn send_json(&mut self, text: String) -> Result<(), Box<EvalAltResult>> {
        match &self.device {
            DeviceHandle::T1(dev) => {
                let body: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| format!("ctx.send_json: invalid JSON: {e}"))?;
                dev.send(body);
                Ok(())
            }
            DeviceHandle::T2(_) => {
                tracing::warn!(
                    target: "rusthinq_scripting",
                    id = %self.id,
                    "ctx.send_json() called on a ThinQ2 device (no JSON send on this platform) — ignored"
                );
                Ok(())
            }
        }
    }
}
