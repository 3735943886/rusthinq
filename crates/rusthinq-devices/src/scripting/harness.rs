//! Test harness for `.rhai` device scripts: load a script against mock MQTT/device
//! connections, feed it wire frames (including from a plain hex fixture file), and
//! assert on what it published or sent back.
//!
//! Not gated behind `#[cfg(test)]` — `script_test` (the runner for driver tests written in
//! Rhai, which is how a device script kept in its own separate repo is tested) drives it from
//! a normal (non-test) build, the same way `rusthinq-core`'s `Mock*` types are reachable.

use crate::device_trait::DeviceHandler;
use crate::scripting::scripted_device::{ScriptedDevice, build_t1_scripted, build_t2_scripted};
use rusthinq_core::{
    Metadata, MockMqttConnection, MockThinq1Device, MockThinq2Device, MqttConnection, Thinq1Device,
    Thinq2Device,
};
use std::path::Path;
use std::sync::Arc;

const HARNESS_DEVICE_ID: &str = "harness";

enum HarnessDevice {
    T1(Arc<MockThinq1Device>),
    T2(Arc<MockThinq2Device>),
}

fn split(
    built: Result<Arc<ScriptedDevice>, Arc<dyn DeviceHandler>>,
) -> (Arc<dyn DeviceHandler>, Option<Arc<ScriptedDevice>>) {
    match built {
        Ok(dev) => (dev.clone() as Arc<dyn DeviceHandler>, Some(dev)),
        Err(broken) => (broken, None),
    }
}

/// Drives one `.rhai` script end-to-end against mock connections. Construct with
/// [`ScriptHarness::t2`]/[`ScriptHarness::t1`], feed it input, then read back what it
/// published/sent.
pub struct ScriptHarness {
    mqtt: Arc<MockMqttConnection>,
    handler: Arc<dyn DeviceHandler>,
    /// `None` when the script failed to compile (`handler` is then the broken stub).
    scripted: Option<Arc<ScriptedDevice>>,
    device: HarnessDevice,
}

impl ScriptHarness {
    /// Compile `script_path` and run it as a ThinQ2 device's script.
    pub fn t2(script_path: impl AsRef<Path>, model_id: &str) -> Self {
        let mqtt = MockMqttConnection::new();
        let meta = Metadata::new(model_id, model_id, "1.0");
        let thinq = MockThinq2Device::new(HARNESS_DEVICE_ID, meta.clone());
        let built = build_t2_scripted(
            mqtt.clone() as Arc<dyn MqttConnection>,
            thinq.clone() as Arc<dyn Thinq2Device>,
            meta,
            script_path.as_ref(),
        );
        let (handler, scripted) = split(built);
        Self {
            mqtt,
            handler,
            scripted,
            device: HarnessDevice::T2(thinq),
        }
    }

    /// Compile `script_path` and run it as a ThinQ1 device's script.
    pub fn t1(script_path: impl AsRef<Path>, model_id: &str) -> Self {
        let mqtt = MockMqttConnection::new();
        let meta = Metadata::new(model_id, model_id, "1.0");
        let thinq = MockThinq1Device::new(HARNESS_DEVICE_ID, meta.clone());
        let built = build_t1_scripted(
            mqtt.clone() as Arc<dyn MqttConnection>,
            thinq.clone() as Arc<dyn Thinq1Device>,
            meta,
            script_path.as_ref(),
        );
        let (handler, scripted) = split(built);
        Self {
            mqtt,
            handler,
            scripted,
            device: HarnessDevice::T1(thinq),
        }
    }

    /// Run the script's `start(ctx)`.
    pub fn start(&self) -> &Self {
        self.handler.start();
        self
    }

    /// Run the script's `on_drop(ctx)`.
    pub fn drop_device(&self) -> &Self {
        self.handler.drop_device();
        self
    }

    /// Run the script's `on_set_property(ctx, prop, value)`.
    pub fn set_property(&self, prop: &str, value: &str) -> &Self {
        self.handler.set_property(prop, value);
        self
    }

    /// Structured (CLIP) messages the script sent via `ctx.send_clip`, as `(cmd, type, data)`.
    /// T2 only; empty on a T1 harness.
    pub fn sent_clip(&self) -> Vec<(String, i32, serde_json::Value)> {
        match &self.device {
            HarnessDevice::T2(dev) => dev
                .sent()
                .into_iter()
                .map(|m| (m.cmd, m.msg_type, m.data))
                .collect(),
            HarnessDevice::T1(_) => Vec::new(),
        }
    }

    /// The IL descriptor the script published with `ctx.publish_il` (`None` if it has not).
    pub fn descriptor(&self) -> Option<serde_json::Value> {
        self.scripted.as_ref().and_then(|d| d.il_descriptor())
    }

    /// Timers the script has armed and not yet fired, as `(name, delay_ms)`, sorted by name.
    pub fn pending_timers(&self) -> Vec<(String, u64)> {
        self.scripted
            .as_ref()
            .map(|d| d.pending_timers())
            .unwrap_or_default()
    }

    /// Fire the armed timer `name` right now (its `on_timer(ctx, name)` runs), instead of
    /// waiting out its delay. Does nothing if `name` is not armed.
    pub fn fire_timer(&self, name: &str) -> &Self {
        if let Some(d) = &self.scripted {
            d.fire_timer(name);
        }
        self
    }

    /// Feed one raw wire frame (hex string; whitespace ignored) through `on_data`.
    pub fn feed_hex(&self, hex: &str) -> &Self {
        let cleaned: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        let bytes = rusthinq_util::hex::decode(&cleaned)
            .unwrap_or_else(|e| panic!("ScriptHarness::feed_hex({hex:?}): {e}"));
        match &self.device {
            HarnessDevice::T1(dev) => dev.emit_data(&bytes),
            HarnessDevice::T2(dev) => dev.emit_data(&bytes),
        }
        self
    }

    /// Feed a JSON response envelope through `on_response`. T1 only.
    pub fn feed_response(&self, body: serde_json::Value) -> &Self {
        match &self.device {
            HarnessDevice::T1(dev) => dev.emit_response(&body),
            HarnessDevice::T2(_) => panic!("ScriptHarness::feed_response is T1-only"),
        }
        self
    }

    /// Feed every non-blank, non-`#`-comment line of `fixture_path` as one hex frame,
    /// in file order.
    pub fn feed_hex_fixture(&self, fixture_path: impl AsRef<Path>) -> &Self {
        let path = fixture_path.as_ref();
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("ScriptHarness::feed_hex_fixture({}): {e}", path.display()));
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            self.feed_hex(line);
        }
        self
    }

    /// The value currently published for `property` via `ctx.publish_property`.
    pub fn property(&self, property: &str) -> Option<String> {
        self.mqtt
            .device(HARNESS_DEVICE_ID)?
            .properties
            .get(property)
            .cloned()
    }

    /// The most recent payload published to `.../<topic_suffix>` via
    /// `ctx.publish_event`.
    pub fn event(&self, topic_suffix: &str) -> Option<String> {
        let dev = self.mqtt.device(HARNESS_DEVICE_ID)?;
        dev.events
            .iter()
            .rev()
            .find(|(t, _)| t == topic_suffix)
            .map(|(_, payload)| payload.clone())
    }

    /// Every `(topic, payload, retain)` published via `ctx.publish_raw`, in order.
    pub fn raw_publishes(&self) -> Vec<(String, String, bool)> {
        self.mqtt
            .raw_publishes()
            .into_iter()
            .map(|(t, payload, retain)| (t, String::from_utf8_lossy(&payload).into_owned(), retain))
            .collect()
    }

    /// The most recent `(payload, retain)` published to an exact topic via
    /// `ctx.publish_raw`.
    pub fn raw_publish(&self, topic: &str) -> Option<(String, bool)> {
        self.mqtt
            .raw_publishes()
            .into_iter()
            .rev()
            .find(|(t, ..)| t == topic)
            .map(|(_, payload, retain)| (String::from_utf8_lossy(&payload).into_owned(), retain))
    }

    /// The most recent runtime error a hook raised, as `<hook>: <error>` (`None` if none).
    pub fn script_error(&self) -> Option<String> {
        self.event("script_error")
    }

    /// Raw byte frames sent to the appliance via `ctx.send_raw`, in send order. Empty
    /// for a T1 harness (no raw send on that platform).
    pub fn sent_raw(&self) -> Vec<Vec<u8>> {
        match &self.device {
            HarnessDevice::T2(dev) => dev.outbox(),
            HarnessDevice::T1(_) => Vec::new(),
        }
    }

    /// JSON bodies sent to the appliance via `ctx.send_json`, in send order. Empty for
    /// a T2 harness (no JSON send on that platform).
    pub fn sent_json(&self) -> Vec<serde_json::Value> {
        match &self.device {
            HarnessDevice::T1(dev) => dev.sent(),
            HarnessDevice::T2(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn a_script_can_import_a_common_module_next_to_it() {
        // Shared logic across device scripts needs no dedicated support — it's just a
        // plain .rhai file in the same rhai_dir, imported the normal rhai way. This
        // only works because compile_file (not read_to_string + compile) records the
        // script's own path as its AST source, which is what rhai's default
        // FileModuleResolver uses to resolve a relative import.
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "common.rhai",
            r#"fn scale_temp(raw) { raw / 2.0 }"#,
        );
        let script = write(
            dir.path(),
            "model.rhai",
            r#"
                import "common" as common;
                fn on_data(ctx, data) {
                    ctx.publish_property("temp", common::scale_temp(data[0]).to_string());
                }
            "#,
        );

        let harness = ScriptHarness::t2(&script, "HARNESS_MODEL_IMPORT");
        harness.feed_hex("2A"); // 0x2a = 42 -> 21.0

        assert_eq!(harness.property("temp").as_deref(), Some("21.0"));
    }

    #[test]
    fn feed_hex_fixture_drives_on_data_line_by_line() {
        let dir = tempfile::tempdir().unwrap();
        let script = write(
            dir.path(),
            "model.rhai",
            r#"
                fn start(ctx) { ctx.publish_property("frames", "0"); }
                fn on_data(ctx, data) {
                    ctx.publish_property("last_hex", hex_encode(data));
                }
            "#,
        );
        let fixture = write(
            dir.path(),
            "frames.hex",
            "# a comment line, and a blank line below\n\nDEADBEEF\ncafe0001\n",
        );

        let harness = ScriptHarness::t2(&script, "HARNESS_MODEL");
        harness.start().feed_hex_fixture(&fixture);

        assert_eq!(harness.property("last_hex").as_deref(), Some("cafe0001"));
    }

    #[test]
    fn start_publishes_config_without_the_script_calling_it_itself() {
        // A newly-connected device must not depend on DeviceBridge::republish_all's
        // resync (only fired on this daemon's own MQTT reconnect) to get its first
        // config out — start() calls publish_config(ctx) on its own, so a script that
        // defines both independently (never calling one from the other) still works.
        let dir = tempfile::tempdir().unwrap();
        let script = write(
            dir.path(),
            "model.rhai",
            r#"
                fn start(ctx) { ctx.publish_property("booted", "true"); }
                fn publish_config(ctx) { ctx.publish_property("config", "{}"); }
            "#,
        );

        let harness = ScriptHarness::t2(&script, "HARNESS_MODEL_CONFIG");
        harness.start();

        assert_eq!(harness.property("booted").as_deref(), Some("true"));
        assert_eq!(harness.property("config").as_deref(), Some("{}"));
    }

    #[test]
    fn ctx_state_persists_across_calls_where_a_top_level_let_does_not() {
        // Real devices need to accumulate TLV state across separate on_data calls
        // (e.g. "have we seen the caps response yet"). A top-level `let` can't do
        // this — every call re-runs the script's top-level statements (needed so a
        // top-level `import` keeps resolving call after call), resetting it right
        // back to its initial value — so `ctx.state_set`/`state_get`/`state_has` is
        // the supported way (see `ctx.rs`'s doc comment on `DeviceCtx`'s `state` field).
        let dir = tempfile::tempdir().unwrap();
        let script = write(
            dir.path(),
            "model.rhai",
            r#"
                let seen = #{};
                fn on_data(ctx, data) {
                    let key = data[0].to_string();

                    // The top-level `let` above: always empty here, every call.
                    seen[key] = true;
                    ctx.publish_property("seen_count_via_let", seen.len().to_string());

                    // ctx.state_*: genuinely accumulates across calls.
                    ctx.state_set(key, true);
                    ctx.publish_property("state_has_1", ctx.state_has("1").to_string());
                    ctx.publish_property("state_has_2", ctx.state_has("2").to_string());
                }
            "#,
        );

        let harness = ScriptHarness::t2(&script, "HARNESS_MODEL_STATE");
        harness.feed_hex("01").feed_hex("02").feed_hex("01");

        // A top-level `let` never accumulates: every call sees it freshly re-run, so
        // it always has exactly the one entry from *this* call.
        assert_eq!(harness.property("seen_count_via_let").as_deref(), Some("1"));

        // ctx.state_has confirms both distinct byte values 0x01 and 0x02 (keyed as
        // "1"/"2" — data[0].to_string() on a decoded byte value) stuck via
        // state_set, seen across all three calls by the time the last one runs.
        assert_eq!(harness.property("state_has_1").as_deref(), Some("true"));
        assert_eq!(harness.property("state_has_2").as_deref(), Some("true"));
    }

    #[test]
    fn publish_raw_reaches_a_topic_outside_rusthinq_prefix() {
        // e.g. some downstream integration's own discovery-config topic space, which
        // is outside rusthinq_prefix scoping entirely and must be retainable.
        let dir = tempfile::tempdir().unwrap();
        let script = write(
            dir.path(),
            "model.rhai",
            r#"
                fn publish_config(ctx) {
                    let topic = `external/device/rusthinq/${ctx.id()}/config`;
                    ctx.publish_raw(topic, "{}", true);
                }
            "#,
        );

        let harness = ScriptHarness::t2(&script, "HARNESS_MODEL_DISCOVERY");
        harness.start();

        let (payload, retain) = harness
            .raw_publish("external/device/rusthinq/harness/config")
            .expect("discovery config published");
        assert_eq!(payload, "{}");
        assert!(retain);
    }

    #[test]
    fn set_property_and_publish_event_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let script = write(
            dir.path(),
            "model.rhai",
            r#"
                fn on_set_property(ctx, prop, value) {
                    ctx.publish_event("changed", `${prop}=${value}`);
                }
            "#,
        );

        let harness = ScriptHarness::t2(&script, "HARNESS_MODEL_2");
        harness.set_property("power", "ON");

        assert_eq!(harness.event("changed").as_deref(), Some("power=ON"));
    }

    #[test]
    fn t1_send_json_and_on_response_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let script = write(
            dir.path(),
            "model.rhai",
            r#"
                fn start(ctx) { ctx.send_json("{\"cmd\":\"poll\"}"); }
                fn on_response(ctx, body) { ctx.publish_property("last_response", body); }
            "#,
        );

        let harness = ScriptHarness::t1(&script, "HARNESS_MODEL_T1");
        harness.start();
        assert_eq!(harness.sent_json().len(), 1);

        harness.feed_response(serde_json::json!({"ReturnCode": "0000"}));
        assert_eq!(
            harness.property("last_response").as_deref(),
            Some(r#"{"ReturnCode":"0000"}"#)
        );
    }
}
