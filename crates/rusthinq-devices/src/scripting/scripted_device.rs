//! `ScriptedDevice`: a `DeviceHandler` whose behavior is entirely defined by a `.rhai`
//! script instead of Rust code — see `registry.rs`'s two-stage lookup (native first,
//! this as the fallback).

use crate::device_trait::DeviceHandler;
use crate::scripting::cache::{self, AstSlot};
use crate::scripting::ctx::{DeviceCtx, DeviceHandle};
use crate::scripting::engine::engine;
use rhai::{AST, CallFnOptions, Dynamic, EvalAltResult, Scope};
use rusthinq_core::metadata::Metadata;
use rusthinq_core::mqtt::MqttConnection;
use rusthinq_core::panic_guard;
use rusthinq_core::thinq::{Thinq1Device, Thinq2Device};
use std::path::PathBuf;
use std::sync::Arc;

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
fn call_optional(ast: &AST, label: &str, fn_name: &str, ctx: DeviceCtx, extra: Vec<Dynamic>) {
    let mut scope = Scope::new();
    let mut args: Vec<Dynamic> = vec![Dynamic::from(ctx)];
    args.extend(extra);
    let mut options = CallFnOptions::default();
    options.rewind_scope = false;
    let result: Result<(), Box<EvalAltResult>> =
        engine().call_fn_with_options(options, &mut scope, ast, fn_name, args);
    if let Err(err) = result {
        if matches!(*err, EvalAltResult::ErrorFunctionNotFound(..)) {
            return;
        }
        tracing::warn!(
            target: "rusthinq_scripting",
            label,
            fn_name,
            error = %err,
            "rhai script error"
        );
    }
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
}

impl ScriptedDevice {
    fn call(&self, fn_name: &str, extra: Vec<Dynamic>) {
        let ast = self.ast_slot.read().clone();
        let ctx = DeviceCtx::new(
            Arc::from(self.id.as_str()),
            self.meta.clone(),
            self.mqtt.clone(),
            self.device.clone(),
            self.state.clone(),
        );
        let id = self.id.clone();
        let fn_name_owned = fn_name.to_string();
        panic_guard::guard(&format!("rhai:{id}:{fn_name_owned}"), move || {
            call_optional(&ast, &id, &fn_name_owned, ctx, extra);
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
        self.call("on_drop", vec![]);
    }

    fn set_property(&self, prop: &str, value: &str) {
        self.call("on_set_property", vec![prop.into(), value.into()]);
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

/// True if a `.rhai` script exists for `model_id` — the fallback `registry.rs` checks
/// once no native factory matches.
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
) -> Arc<dyn DeviceHandler> {
    match cache::get_or_compile(path) {
        Ok(ast_slot) => {
            let handler = Arc::new(ScriptedDevice {
                id,
                meta,
                mqtt,
                device,
                ast_slot,
                state: Dynamic::from_map(rhai::Map::new()).into_shared(),
            });
            wire(&handler);
            handler
        }
        Err(error) => Arc::new(BrokenScript { id, mqtt, error }),
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
