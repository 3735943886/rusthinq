//! `ScriptedDevice`: a `DeviceHandler` whose behavior is entirely defined by a `.rhai`
//! script instead of Rust code — see `registry.rs`'s lookup.

use crate::device_trait::DeviceHandler;
use crate::scripting::cache::{self, AstSlot};
use crate::scripting::ctx::{DeviceCtx, DeviceHandle, HostServices, TimerHost};
use crate::scripting::engine::engine;
use rhai::{AST, CallFnOptions, Dynamic, EvalAltResult, Scope};
use rusthinq_core::metadata::Metadata;
use rusthinq_core::mqtt::MqttConnection;
use rusthinq_core::panic_guard;
use rusthinq_core::thinq::{Thinq1Device, Thinq2Device};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

/// Call `fn_name(ctx, ..extra)` on `ast` if the script defines it, against a fresh
/// `Scope` — deliberately *not* reused across calls. `call_fn`'s default
/// `eval_ast: true` re-runs the script's top-level statements before every call,
/// which a top-level `import "common" as c;` needs in order for `c::...` to resolve
/// on every single call (rhai does not let that binding survive from a previous
/// call) — so a fresh scope each time is actually required, not just simpler. Any
/// state a script wants to keep across calls belongs in `ctx.state_get`/`state_set`
/// (see `ctx.rs`) instead of a top-level `let`, which this re-execution would
/// otherwise reset right back to its initial value on every call.
///
/// `rewind_scope: false` (rhai's default is `true`) is required even though the
/// scope is thrown away right after this call regardless — verified against 1.26.0
/// that with the default `true`, a script's own top-level `let` is not just reset
/// every call but flat out invisible (`Variable not found`) to a function it calls
/// that tries to read or mutate it, even within one single call (see
/// `engine.rs::tests::does_rewind_scope_false_matter_for_single_call_visibility`).
///
/// A missing function is not an error (most of the script API is optional) — only a
/// genuine script runtime error is logged.
///
/// A genuine runtime error is returned (as text) so the caller can also surface it.
fn call_optional(
    ast: &AST,
    label: &str,
    fn_name: &str,
    ctx: DeviceCtx,
    extra: Vec<Dynamic>,
) -> Option<String> {
    let mut scope = Scope::new();
    let mut args: Vec<Dynamic> = vec![Dynamic::from(ctx)];
    args.extend(extra);
    let mut options = CallFnOptions::default();
    options.rewind_scope = false;
    let result: Result<(), Box<EvalAltResult>> =
        engine().call_fn_with_options(options, &mut scope, ast, fn_name, args);
    if let Err(err) = result {
        if matches!(*err, EvalAltResult::ErrorFunctionNotFound(..)) {
            return None;
        }
        tracing::warn!(
            target: "rusthinq_scripting",
            label,
            fn_name,
            error = %err,
            "rhai script error"
        );
        return Some(format!("{fn_name}: {err}"));
    }
    None
}

/// "Broken script" stub: the script file existed but failed to compile (or vanished
/// between `has_script_for`'s check and the factory running). Never panics the
/// factory — one model's bad script must not take down `new_device()` for every
/// model. Publishes `script_error` once, at `start()`, and is otherwise a no-op.
struct BrokenScript {
    id: String,
    mqtt: Arc<dyn MqttConnection>,
    error: String,
}

impl DeviceHandler for BrokenScript {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&self) {
        self.mqtt
            .publish_property(&self.id, "script_error", &self.error);
    }

    fn drop_device(&self) {}
    fn set_property(&self, _prop: &str, _value: &str) {}
    fn publish_config(&self) {}
}

/// A device's named one-shot timers. Each `set` starts a sleeping thread holding only a
/// `Weak` to the device and a generation number: when it wakes it fires `on_timer` only if
/// its generation is still the current one for that name (re-arming or cancelling bumps or
/// removes it), so a replaced timer never fires. Nothing here keeps the device alive.
#[derive(Default)]
struct TimerTable {
    /// name -> (generation, delay in ms) of the armed timer.
    armed: rusthinq_util::sync::Mutex<HashMap<String, (u64, u64)>>,
    next_gen: AtomicU64,
    owner: OnceLock<Weak<ScriptedDevice>>,
}

impl TimerTable {
    fn fire(&self, name: &str) {
        self.armed.lock().remove(name);
        if let Some(dev) = self.owner.get().and_then(Weak::upgrade) {
            dev.call("on_timer", vec![name.into()]);
        }
    }
}

impl TimerHost for Arc<TimerTable> {
    fn set(&self, name: &str, after_ms: u64) {
        let generation = self.next_gen.fetch_add(1, Ordering::Relaxed);
        self.armed
            .lock()
            .insert(name.to_string(), (generation, after_ms));
        let table = self.clone();
        let name = name.to_string();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(after_ms));
            let current = table.armed.lock().get(&name).map(|(g, _)| *g);
            if current == Some(generation) {
                table.fire(&name);
            }
        });
    }

    fn cancel(&self, name: &str) {
        self.armed.lock().remove(name);
    }
}

pub struct ScriptedDevice {
    id: String,
    meta: Metadata,
    mqtt: Arc<dyn MqttConnection>,
    device: DeviceHandle,
    ast_slot: AstSlot,
    /// This device's persistent state, handed to the script as `ctx.state()` — see
    /// `DeviceCtx::state`'s doc comment for why this (not a top-level `let`) is what
    /// survives across calls.
    state: Dynamic,
    timers: Arc<TimerTable>,
    /// The descriptor the script published and the values it reported, which a command is
    /// validated against before the script sees it (see `il.rs`).
    il_state: Arc<crate::scripting::il::IlState>,
}

impl ScriptedDevice {
    /// The IL descriptor the script last published, if any — a test hook (`ScriptHarness`).
    pub(crate) fn il_descriptor(&self) -> Option<serde_json::Value> {
        self.il_state.descriptor()
    }

    /// Timers currently armed, as `(name, delay_ms)` — a test hook (`ScriptHarness`).
    pub(crate) fn pending_timers(&self) -> Vec<(String, u64)> {
        let mut v: Vec<_> = self
            .timers
            .armed
            .lock()
            .iter()
            .map(|(n, (_, ms))| (n.clone(), *ms))
            .collect();
        v.sort();
        v
    }

    /// Fire `name` now, if armed — a test hook so a test need not wait out real time.
    pub(crate) fn fire_timer(&self, name: &str) {
        if self.timers.armed.lock().contains_key(name) {
            self.timers.fire(name);
        }
    }

    fn call(&self, fn_name: &str, extra: Vec<Dynamic>) {
        let ast = self.ast_slot.read().clone();
        let ctx = DeviceCtx::new(
            Arc::from(self.id.as_str()),
            self.meta.clone(),
            self.mqtt.clone(),
            self.device.clone(),
            self.state.clone(),
            HostServices {
                timers: Arc::new(self.timers.clone()),
                il_state: self.il_state.clone(),
                il_prefix: crate::scripting::il_prefix(),
            },
        );
        let id = self.id.clone();
        let fn_name_owned = fn_name.to_string();
        let mqtt = self.mqtt.clone();
        panic_guard::guard(&format!("rhai:{id}:{fn_name_owned}"), move || {
            // A runtime error in a hook is also published as a `script_error` event, so it
            // can be seen without the log (and asserted on in a test).
            if let Some(error) = call_optional(&ast, &id, &fn_name_owned, ctx, extra) {
                mqtt.publish_event(&id, "script_error", &error);
            }
        });
    }
}

impl DeviceHandler for ScriptedDevice {
    fn id(&self) -> &str {
        &self.id
    }

    /// Runs the script's `start(ctx)`, then `publish_config(ctx)` — a newly-connected
    /// device must not depend on a resync (`DeviceBridge::republish_all`, which only
    /// fires on *this daemon's own* MQTT reconnect, not on every new device) to get
    /// its first config out. `publish_config(ctx)` stays idempotent/safe to call twice
    /// in a row: whatever it publishes should be exactly what a later resync would
    /// republish, so calling it here doesn't duplicate meaning, just timing.
    fn start(&self) {
        self.call("start", vec![]);
        self.call("publish_config", vec![]);
    }

    fn drop_device(&self) {
        self.cancel_pending_work();
        self.call("on_drop", vec![]);
        // The link is down, so no value is current any more: publish them as absent
        // (il.md R-4); `available`, which the script has just set to false, stays.
        for prop in self.il_state.take_stale_props() {
            self.mqtt.publish_property(&self.id, &prop, "");
        }
    }

    fn cancel_pending_work(&self) {
        self.timers.armed.lock().clear();
    }

    /// A command is checked against the IL descriptor the script published (writable,
    /// `requires` satisfied, the type, options, range and step) and reaches the script
    /// already in canonical form. One that fails is reported as a `reject` event and never
    /// reaches the script. A script that publishes no descriptor is not checked.
    fn set_property(&self, prop: &str, value: &str) {
        match self.il_state.validate(prop, value) {
            Ok(Some(canonical)) => {
                self.call("on_set_property", vec![prop.into(), canonical.into()])
            }
            Ok(None) => self.call("on_set_property", vec![prop.into(), value.into()]),
            Err(reject) => {
                self.mqtt
                    .publish_event(&self.id, "reject", &reject.body(prop).to_string())
            }
        }
    }

    fn publish_config(&self) {
        self.call("publish_config", vec![]);
    }
}

fn script_path(model_id: &str) -> Option<PathBuf> {
    let dir = crate::scripting::rhai_dir()?;
    let path = dir.join(format!("{model_id}.rhai"));
    path.exists().then_some(path)
}

/// True if a `.rhai` script exists for `model_id` — what `registry.rs` checks.
pub fn has_script_for(model_id: &str) -> bool {
    script_path(model_id).is_some()
}

pub fn scripted_t2_factory(
    mqtt: Arc<dyn MqttConnection>,
    thinq: Arc<dyn Thinq2Device>,
    meta: Metadata,
) -> Arc<dyn DeviceHandler> {
    let error = format!("no .rhai script found for model {}", meta.model_id);
    match script_path(&meta.model_id) {
        Some(path) => build_t2(mqtt, thinq, meta, &path),
        None => Arc::new(BrokenScript {
            id: thinq.id().to_string(),
            mqtt,
            error,
        }),
    }
}

pub fn scripted_t1_factory(
    mqtt: Arc<dyn MqttConnection>,
    thinq: Arc<dyn Thinq1Device>,
    meta: Metadata,
) -> Arc<dyn DeviceHandler> {
    let error = format!("no .rhai script found for model {}", meta.model_id);
    match script_path(&meta.model_id) {
        Some(path) => build_t1(mqtt, thinq, meta, &path),
        None => Arc::new(BrokenScript {
            id: thinq.id().to_string(),
            mqtt,
            error,
        }),
    }
}

/// Shared by `build_t1`/`build_t2`: compile-or-broken-stub, then construct the
/// `ScriptedDevice`. `wire` hooks up whatever local-device callbacks the caller's
/// concrete `thinq` type offers (`on_data` for both, plus T1's `on_response`) —
/// it's the only part that actually differs between the two.
fn build_scripted_device(
    mqtt: Arc<dyn MqttConnection>,
    id: String,
    meta: Metadata,
    path: &std::path::Path,
    device: DeviceHandle,
    wire: impl FnOnce(&Arc<ScriptedDevice>),
) -> Result<Arc<ScriptedDevice>, Arc<dyn DeviceHandler>> {
    match cache::get_or_compile(path) {
        Ok(ast_slot) => {
            let handler = Arc::new(ScriptedDevice {
                id,
                meta,
                mqtt,
                device,
                ast_slot,
                state: Dynamic::from_map(rhai::Map::new()).into_shared(),
                timers: Arc::new(TimerTable::default()),
                il_state: Arc::new(crate::scripting::il::IlState::default()),
            });
            let _ = handler.timers.owner.set(Arc::downgrade(&handler));
            wire(&handler);
            Ok(handler)
        }
        Err(error) => Err(Arc::new(BrokenScript { id, mqtt, error })),
    }
}

/// Build a `ScriptedDevice` from an already-resolved `.rhai` path, bypassing
/// `rhai_dir`/model_id lookup entirely — used by the factories above (which resolve
/// the path from the configured `rhai_dir` first) and by `harness.rs` (which lets a
/// script's own test point at any path directly, independent of process-global
/// scripting config).
pub(crate) fn build_t2(
    mqtt: Arc<dyn MqttConnection>,
    thinq: Arc<dyn Thinq2Device>,
    meta: Metadata,
    path: &std::path::Path,
) -> Arc<dyn DeviceHandler> {
    match build_t2_scripted(mqtt, thinq, meta, path) {
        Ok(dev) => dev,
        Err(broken) => broken,
    }
}

/// Like `build_t2`, but hands back the concrete `ScriptedDevice` (for `harness.rs`'s
/// timer hooks); `Err` is the broken-script stub.
pub(crate) fn build_t2_scripted(
    mqtt: Arc<dyn MqttConnection>,
    thinq: Arc<dyn Thinq2Device>,
    meta: Metadata,
    path: &std::path::Path,
) -> Result<Arc<ScriptedDevice>, Arc<dyn DeviceHandler>> {
    let id = thinq.id().to_string();
    build_scripted_device(
        mqtt,
        id,
        meta,
        path,
        DeviceHandle::T2(thinq.clone()),
        |handler| {
            let for_data = handler.clone();
            thinq.on_data(Box::new(move |data: &[u8]| {
                for_data.call("on_data", vec![Dynamic::from_blob(data.to_vec())]);
            }));
        },
    )
}

pub(crate) fn build_t1(
    mqtt: Arc<dyn MqttConnection>,
    thinq: Arc<dyn Thinq1Device>,
    meta: Metadata,
    path: &std::path::Path,
) -> Arc<dyn DeviceHandler> {
    match build_t1_scripted(mqtt, thinq, meta, path) {
        Ok(dev) => dev,
        Err(broken) => broken,
    }
}

pub(crate) fn build_t1_scripted(
    mqtt: Arc<dyn MqttConnection>,
    thinq: Arc<dyn Thinq1Device>,
    meta: Metadata,
    path: &std::path::Path,
) -> Result<Arc<ScriptedDevice>, Arc<dyn DeviceHandler>> {
    let id = thinq.id().to_string();
    build_scripted_device(
        mqtt,
        id,
        meta,
        path,
        DeviceHandle::T1(thinq.clone()),
        |handler| {
            let for_data = handler.clone();
            thinq.on_data(Box::new(move |data: &[u8]| {
                for_data.call("on_data", vec![Dynamic::from_blob(data.to_vec())]);
            }));
            let for_response = handler.clone();
            thinq.on_response(Box::new(move |body: &serde_json::Value| {
                for_response.call("on_response", vec![Dynamic::from(body.to_string())]);
            }));
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusthinq_core::{MockMqttConnection, MockThinq1Device, MockThinq2Device};

    fn meta(model_id: &str) -> Metadata {
        Metadata::new(model_id, model_id, "1.0")
    }

    fn with_script_dir(body: &str, model_id: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(format!("{model_id}.rhai")), body).unwrap();
        crate::scripting::init(dir.path().to_path_buf(), false);
        dir
    }

    #[test]
    fn t2_on_data_reaches_the_script_and_publishes_via_ctx() {
        let _dir = with_script_dir(
            r#"
                fn on_data(ctx, data) {
                    ctx.publish_property("last_hex", hex_encode(data));
                }
            "#,
            "T2_SCRIPT_TEST",
        );
        let mqtt = MockMqttConnection::new();
        let thinq = MockThinq2Device::new("dev-1", meta("T2_SCRIPT_TEST"));
        let handler = scripted_t2_factory(
            mqtt.clone() as Arc<dyn MqttConnection>,
            thinq.clone() as Arc<dyn Thinq2Device>,
            meta("T2_SCRIPT_TEST"),
        );
        handler.start();

        thinq.emit_data(&[0xde, 0xad, 0xbe, 0xef]);

        let dev = mqtt.device("dev-1").expect("device published to");
        assert_eq!(
            dev.properties.get("last_hex").map(String::as_str),
            Some("deadbeef")
        );
    }

    #[test]
    fn a_compile_error_yields_a_broken_script_stub_not_a_panic() {
        let _dir = with_script_dir("fn on_data( {{{ not valid rhai", "T2_BROKEN");
        let mqtt = MockMqttConnection::new();
        let thinq = MockThinq2Device::new("dev-2", meta("T2_BROKEN"));
        let handler = scripted_t2_factory(
            mqtt.clone() as Arc<dyn MqttConnection>,
            thinq as Arc<dyn Thinq2Device>,
            meta("T2_BROKEN"),
        );
        handler.start();

        let dev = mqtt.device("dev-2").expect("device published to");
        assert!(dev.properties.contains_key("script_error"));
    }

    #[test]
    fn missing_script_file_also_yields_a_broken_script_stub() {
        let dir = tempfile::tempdir().unwrap();
        crate::scripting::init(dir.path().to_path_buf(), false);

        let mqtt = MockMqttConnection::new();
        let thinq = MockThinq2Device::new("dev-3", meta("NO_SUCH_MODEL"));
        assert!(!has_script_for("NO_SUCH_MODEL"));
        let handler = scripted_t2_factory(
            mqtt.clone() as Arc<dyn MqttConnection>,
            thinq as Arc<dyn Thinq2Device>,
            meta("NO_SUCH_MODEL"),
        );
        handler.start();

        let dev = mqtt.device("dev-3").expect("device published to");
        assert!(dev.properties.contains_key("script_error"));
    }

    #[test]
    fn t1_send_json_reaches_the_mock_device() {
        let _dir = with_script_dir(
            r#"
                fn start(ctx) {
                    ctx.send_json("{\"cmd\":\"hello\"}");
                }
            "#,
            "T1_SCRIPT_TEST",
        );
        let mqtt = MockMqttConnection::new();
        let thinq = MockThinq1Device::new("dev-4", meta("T1_SCRIPT_TEST"));
        let handler = scripted_t1_factory(
            mqtt as Arc<dyn MqttConnection>,
            thinq.clone() as Arc<dyn Thinq1Device>,
            meta("T1_SCRIPT_TEST"),
        );
        handler.start();

        let sent = thinq.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["cmd"], "hello");
    }
}
