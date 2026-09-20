//! The one `rhai::Engine`, shared by every `ScriptedDevice` regardless of model —
//! registration (types/functions) is static, so there is no reason to build more than
//! one. `Engine`/`AST` are `Send + Sync` because the `sync` cargo feature is enabled
//! (see `rusthinq-devices/Cargo.toml`) — required since `DeviceHandler: Send + Sync`.

use crate::scripting::ctx::DeviceCtx;
use rhai::module_resolvers::FileModuleResolver;
use rhai::{Dynamic, Engine, EvalAltResult, Module, ModuleResolver, Position, Shared};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;

static ENGINE: LazyLock<Engine> = LazyLock::new(build_engine);

pub fn engine() -> &'static Engine {
    &ENGINE
}

/// Wraps rhai's own `FileModuleResolver` (which every `Engine::new()` already sets up
/// for plain relative `import "x" as x;` resolution) behind a `Mutex` purely so a
/// changed *imported* `.rhai` module — shared logic multiple device scripts `import`,
/// as opposed to a top-level device script itself — can be evicted from its compiled-
/// module cache without a process restart. `FileModuleResolver::clear_cache_for_path`
/// needs `&mut self`; the `Engine` (and thus its resolver) is shared behind `&'static`
/// for every device, so this lock is what makes that reachable from `invalidate_module`.
///
/// It is a `RwLock` used so that resolving never blocks: `resolve` takes a read lock, which
/// many threads (and the same thread, recursively) may hold at once. Compiling a module
/// resolves that module's own `import`s through this same resolver while the outer read lock
/// is still held, so a plain `Mutex` here deadlocked the first time a module imported
/// another module. Invalidation is the only writer and never *queues* for the write lock
/// (`try_write` in a loop), because a queued writer would block that nested read.
///
/// It also fixes how a module's own `import`s are found. rhai's `FileModuleResolver` names a
/// module's source by its import name (`"tlv_common"`), not by its file path, so an `import`
/// inside a module has no directory to be relative to and falls back to the process's working
/// directory. This resolver remembers the directory each module was found in and hands it
/// back as the source of that module's nested imports, so they resolve beside it, exactly as
/// a top-level script's do.
struct HotReloadResolver {
    inner: RwLock<FileModuleResolver>,
    /// import name -> directory it was resolved from.
    module_dirs: Mutex<HashMap<String, PathBuf>>,
}

impl HotReloadResolver {
    fn new() -> Self {
        Self {
            inner: RwLock::new(FileModuleResolver::new()),
            module_dirs: Mutex::new(HashMap::new()),
        }
    }

    /// The source to resolve `path` against: the importer's own path if it has one, else the
    /// directory its module was found in (a nested import).
    fn effective_source(&self, source: Option<&str>) -> Option<String> {
        let source = source?;
        if Path::new(source)
            .parent()
            .is_some_and(|p| p != Path::new(""))
        {
            return Some(source.to_string());
        }
        let dirs = self.module_dirs.lock().unwrap_or_else(|e| e.into_inner());
        Some(match dirs.get(source) {
            Some(dir) => dir.join(source).to_string_lossy().into_owned(),
            None => source.to_string(),
        })
    }

    fn invalidate(&self, path: &Path) {
        loop {
            match self.inner.try_write() {
                Ok(mut resolver) => {
                    let _ = resolver.clear_cache_for_path(path);
                    return;
                }
                Err(std::sync::TryLockError::Poisoned(e)) => {
                    let _ = e.into_inner().clear_cache_for_path(path);
                    return;
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }
}

impl ModuleResolver for HotReloadResolver {
    fn resolve(
        &self,
        engine: &Engine,
        source: Option<&str>,
        path: &str,
        pos: Position,
    ) -> Result<Shared<Module>, Box<EvalAltResult>> {
        let effective = self.effective_source(source);
        // Remember where `path` is being looked for, so its own imports can find their way.
        if let Some(dir) = effective
            .as_deref()
            .and_then(|s| Path::new(s).parent())
            .filter(|p| *p != Path::new(""))
        {
            self.module_dirs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(path.to_string(), dir.to_path_buf());
        }
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .resolve(engine, effective.as_deref(), path, pos)
    }
}

static MODULE_RESOLVER: LazyLock<Arc<HotReloadResolver>> =
    LazyLock::new(|| Arc::new(HotReloadResolver::new()));

/// `engine.set_module_resolver` takes ownership, so the `Engine` needs its own local
/// handle onto the shared resolver rather than the `Arc` itself (implementing a
/// foreign trait for `Arc<HotReloadResolver>` directly would violate the orphan
/// rule — neither `ModuleResolver` nor `Arc` are local to this crate).
struct SharedResolverHandle(Arc<HotReloadResolver>);

impl ModuleResolver for SharedResolverHandle {
    fn resolve(
        &self,
        engine: &Engine,
        source: Option<&str>,
        path: &str,
        pos: Position,
    ) -> Result<Shared<Module>, Box<EvalAltResult>> {
        self.0.resolve(engine, source, path, pos)
    }
}

/// Evict `path` from the shared module-import cache. Called by the hot-reload watcher
/// for every changed `.rhai` file, regardless of whether it turns out to be a
/// top-level device script (which `cache::reload` handles separately) or a module
/// some script `import`s — harmless no-op if `path` was never resolved as a module.
pub(crate) fn invalidate_module(path: &Path) {
    MODULE_RESOLVER.invalidate(path);
}

/// Op-count/collection-size sandboxing — a buggy device script (infinite loop, a huge
/// `Array`/`Map`/`String` built from a malformed packet) must degrade to a logged
/// script error, never hang or balloon memory in the host process. Not a security
/// boundary against a hostile script (these are the operator's own device scripts),
/// just a safety net; a limit hit surfaces as an ordinary `EvalAltResult::Err` through
/// `call_fn`, which `scripted_device::call_optional` already logs and moves on from —
/// no extra handling needed at the call site.
const MAX_OPERATIONS: u64 = 5_000_000;
const MAX_STRING_SIZE: usize = 64 * 1024;
const MAX_ARRAY_SIZE: usize = 10_000;
const MAX_MAP_SIZE: usize = 1_000;
/// rhai's own defaults for these (32/16 in debug builds, 64/32 in release) are tuned
/// for `no_std`/embedded use and are too shallow for an ordinary discovery-document
/// literal (a nested map of maps of arrays) once expression nesting is counted the
/// way rhai's parser counts it — and differing by build profile means a script that
/// compiles for a developer's `cargo test` could fail once really deployed, or vice
/// versa. Set explicitly, the same in every build, generously above what a real
/// device script needs.
const MAX_EXPR_DEPTH: usize = 200;
const MAX_FUNCTION_EXPR_DEPTH: usize = 100;

fn build_engine() -> Engine {
    let mut engine = Engine::new();

    engine
        .set_max_operations(MAX_OPERATIONS)
        .set_max_string_size(MAX_STRING_SIZE)
        .set_max_array_size(MAX_ARRAY_SIZE)
        .set_max_map_size(MAX_MAP_SIZE)
        .set_max_expr_depths(MAX_EXPR_DEPTH, MAX_FUNCTION_EXPR_DEPTH)
        .set_module_resolver(SharedResolverHandle(MODULE_RESOLVER.clone()));

    engine
        .register_type_with_name::<DeviceCtx>("DeviceCtx")
        .register_fn("id", DeviceCtx::id)
        .register_fn("model_id", DeviceCtx::model_id)
        .register_fn("model_name", DeviceCtx::model_name)
        .register_fn("sw_version", DeviceCtx::sw_version)
        .register_fn("device_topic", DeviceCtx::device_topic)
        .register_fn("state_get", DeviceCtx::state_get)
        .register_fn("state_set", DeviceCtx::state_set)
        .register_fn("state_has", DeviceCtx::state_has)
        .register_fn("publish_property", DeviceCtx::publish_property)
        .register_fn("publish_event", DeviceCtx::publish_event)
        .register_fn("publish_raw", DeviceCtx::publish_raw)
        .register_fn("send_raw", DeviceCtx::send_raw)
        .register_fn("send_json", DeviceCtx::send_json)
        .register_fn("send_clip", DeviceCtx::send_clip)
        .register_fn("set_timer", DeviceCtx::set_timer)
        .register_fn("cancel_timer", DeviceCtx::cancel_timer)
        .register_fn("publish_il", DeviceCtx::publish_il);

    register_codec_helpers(&mut engine);

    // `json_stringify(#{...})` — a script builds a discovery/config document as an
    // ordinary rhai map (nested maps/arrays and all) and turns it into the JSON text
    // `publish_property`/`publish_raw` actually send, without hand-escaping strings.
    // `rhai::format_map_as_json` already walks nested Array/Map/Dynamic values.
    engine.register_fn("json_stringify", |value: rhai::Map| {
        rhai::format_map_as_json(&value)
    });

    engine
}

/// Pure `rusthinq-util` codec helpers, opt-in from a script — no `ctx` needed.
fn register_codec_helpers(engine: &mut Engine) {
    engine
        .register_fn("crc16", |data: Vec<u8>| {
            i64::from(rusthinq_util::crc16::crc16(&data))
        })
        .register_fn("hex_encode", |data: Vec<u8>| {
            rusthinq_util::hex::encode(&data)
        })
        .register_fn("hex_encode_upper", |data: Vec<u8>| {
            rusthinq_util::hex::encode_upper(&data)
        })
        .register_fn(
            "hex_decode",
            |s: String| -> Result<Vec<u8>, Box<EvalAltResult>> {
                rusthinq_util::hex::decode(&s).map_err(|e| e.to_string().into())
            },
        )
        .register_fn("tlv_parse", tlv_parse)
        .register_fn(
            "tlv_build",
            |items: rhai::Array| -> Result<Vec<u8>, Box<EvalAltResult>> {
                let tlvs = array_to_tlvs(items).map_err(|e| format!("tlv_build: {e}"))?;
                Ok(rusthinq_util::tlv::build(&tlvs))
            },
        )
        .register_fn("tlv_frame_parse", |data: Vec<u8>| -> rhai::Dynamic {
            match rusthinq_util::tlv::frame_parse(&data) {
                Some(tlvs) => tlvs_to_array(tlvs).into(),
                None => rhai::Dynamic::UNIT,
            }
        })
        .register_fn(
            "tlv_frame_build",
            |header: rhai::Array, items: rhai::Array| -> Result<Vec<u8>, Box<EvalAltResult>> {
                // A header is written `[1, 1, 2, 2, 1]`: an array of small integers.
                let header: Vec<u8> = header
                    .iter()
                    .map(|d| d.as_int().ok().and_then(|n| u8::try_from(n).ok()))
                    .collect::<Option<_>>()
                    .ok_or("tlv_frame_build: header must be an array of byte values")?;
                let tlvs = array_to_tlvs(items).map_err(|e| format!("tlv_frame_build: {e}"))?;
                rusthinq_util::tlv::frame_build(&header, &tlvs)
                    .ok_or_else(|| "tlv_frame_build: bad header or too many elements".into())
            },
        )
        .register_fn("aabb_wrap", |inner: Vec<u8>| {
            crate::device_base::wrap_aabb(&inner)
        })
        .register_fn(
            "aabb_wrap",
            |inner: rhai::Array| -> Result<Vec<u8>, Box<EvalAltResult>> {
                let bytes: Vec<u8> = inner
                    .iter()
                    .map(|d| d.as_int().ok().and_then(|n| u8::try_from(n).ok()))
                    .collect::<Option<_>>()
                    .ok_or("aabb_wrap: expected an array of byte values")?;
                Ok(crate::device_base::wrap_aabb(&bytes))
            },
        )
        .register_fn("aabb_unwrap", |data: Vec<u8>| -> rhai::Dynamic {
            match crate::device_base::unwrap_aabb(&data) {
                Some(inner) => rhai::Dynamic::from_blob(inner),
                None => rhai::Dynamic::UNIT,
            }
        })
        .register_fn("known_tag_name", |id: i64| {
            rusthinq_util::tlv_catalog::known_tag_name(id as u16)
                .unwrap_or("")
                .to_string()
        })
        .register_fn("is_known_tag", |id: i64| {
            rusthinq_util::tlv_catalog::is_known_tag(id as u16)
        })
        .register_fn("length_prefixed_make", |data: Vec<u8>| {
            rusthinq_util::length_prefixed_frame::make(&data)
        })
        .register_fn("length_prefixed_make_str", |s: String| {
            rusthinq_util::length_prefixed_frame::make_str(&s)
        });
}

/// `Tlv { t, v }` -> `#{t: .., v: ..}`, as an array in wire order.
fn tlv_parse(data: Vec<u8>) -> rhai::Array {
    tlvs_to_array(rusthinq_util::tlv::parse(&data))
}

fn array_to_tlvs(items: rhai::Array) -> Result<Vec<rusthinq_util::tlv::Tlv>, Box<EvalAltResult>> {
    let mut tlvs = Vec::with_capacity(items.len());
    for item in items {
        let map = item
            .try_cast::<rhai::Map>()
            .ok_or("expected an array of #{t: .., v: ..} maps")?;
        let t = map
            .get("t")
            .and_then(|d| d.as_int().ok())
            .ok_or("map missing integer field \"t\"")?;
        let v = map
            .get("v")
            .and_then(|d| d.as_int().ok())
            .ok_or("map missing integer field \"v\"")?;
        tlvs.push(rusthinq_util::tlv::Tlv::new(t as u16, v as u32));
    }
    Ok(tlvs)
}

fn tlvs_to_array(tlvs: Vec<rusthinq_util::tlv::Tlv>) -> rhai::Array {
    tlvs.into_iter()
        .map(|tlv| {
            let mut map = rhai::Map::new();
            map.insert("t".into(), Dynamic::from_int(i64::from(tlv.t)));
            map.insert("v".into(), Dynamic::from_int(i64::from(tlv.v)));
            Dynamic::from_map(map)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rhai::Scope;

    fn run(script: &str) -> Result<(), Box<EvalAltResult>> {
        let ast = engine().compile(script).unwrap();
        let mut scope = Scope::new();
        engine().call_fn(&mut scope, &ast, "run_script", ())
    }

    /// A module that itself imports another module, with the working directory somewhere
    /// else entirely. Two things went wrong here before: the resolver held its (then
    /// non-reentrant) lock while compiling a module and deadlocked on the nested import, and
    /// rhai names a module's source by import name, so the nested import was looked up
    /// relative to the working directory instead of beside the module.
    #[test]
    fn a_module_can_import_another_module_from_its_own_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("inner_mod.rhai"), "fn base() { 40 }").unwrap();
        std::fs::write(
            dir.path().join("outer_mod.rhai"),
            "import \"inner_mod\" as i;\nfn answer() { i::base() + 2 }",
        )
        .unwrap();
        let script = dir.path().join("script.rhai");
        std::fs::write(
            &script,
            "import \"outer_mod\" as o;\nfn run_script() { o::answer() }",
        )
        .unwrap();
        let ast = engine().compile_file(script).unwrap();
        let mut scope = Scope::new();
        let answer: i64 = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        assert_eq!(answer, 42);
    }

    #[test]
    fn aabb_wrap_and_unwrap_round_trip_and_reject_unframed_input() {
        let ast = engine()
            .compile(
                r#"
                fn run_script() {
                    let inner = [0x12, 0xec, 0x01];
                    let framed = aabb_wrap(inner);
                    if framed[0] != 0xaa || framed[framed.len() - 1] != 0xbb { return "bad frame"; }
                    let back = aabb_unwrap(framed);
                    if back.len() != 3 || back[1] != 0xec { return "bad inner"; }
                    if aabb_unwrap(blob(4, 1)) != () { return "accepted unframed"; }
                    "ok"
                }
                "#,
            )
            .unwrap();
        let mut scope = Scope::new();
        let out: String = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        assert_eq!(out, "ok");
    }

    #[test]
    fn json_stringify_handles_nested_maps_and_arrays() {
        let ast = engine()
            .compile(
                r#"
                fn run_script() {
                    json_stringify(#{
                        name: "climate",
                        modes: ["cool", "fan_only"],
                        nested: #{ min: 16, max: 30 },
                    })
                }
                "#,
            )
            .unwrap();
        let mut scope = Scope::new();
        let json: String = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["name"], "climate");
        assert_eq!(parsed["modes"][1], "fan_only");
        assert_eq!(parsed["nested"]["max"], 30);
    }

    #[test]
    fn max_operations_stops_an_infinite_loop() {
        let err = run("fn run_script() { loop {} }").unwrap_err();
        assert!(matches!(*err, EvalAltResult::ErrorTooManyOperations(..)));
    }

    #[test]
    fn max_array_size_rejects_a_runaway_array() {
        let err = run("fn run_script() { let a = []; loop { a.push(1); } }").unwrap_err();
        assert!(matches!(
            *err,
            EvalAltResult::ErrorDataTooLarge(..) | EvalAltResult::ErrorTooManyOperations(..)
        ));
    }

    #[test]
    fn max_string_size_rejects_a_runaway_string() {
        let err =
            run(r#"fn run_script() { let s = ""; loop { s += "xxxxxxxxxx"; } }"#).unwrap_err();
        assert!(matches!(
            *err,
            EvalAltResult::ErrorDataTooLarge(..) | EvalAltResult::ErrorTooManyOperations(..)
        ));
    }

    #[test]
    fn invalidate_module_picks_up_a_changed_common_module() {
        let dir = tempfile::tempdir().unwrap();
        let common_path = dir.path().join("common.rhai");
        let main_path = dir.path().join("main.rhai");
        std::fs::write(&common_path, "fn value() { 1 }").unwrap();
        std::fs::write(
            &main_path,
            r#"import "common" as common; fn run_script() { common::value() }"#,
        )
        .unwrap();

        let ast = engine().compile_file(main_path).unwrap();
        let mut scope = Scope::new();
        let before: i64 = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        assert_eq!(before, 1);

        std::fs::write(&common_path, "fn value() { 2 }").unwrap();

        // Without invalidation, the resolver's own compiled-module cache still holds
        // the old version — this is the gap `invalidate_module` closes.
        let still_stale: i64 = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        assert_eq!(still_stale, 1);

        invalidate_module(&common_path);

        let after: i64 = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        assert_eq!(after, 2);
    }

    /// **Known rhai constraint** (verified against 1.26.0, not documented anywhere
    /// obvious): a module's own top-level `const`/`let` is *not* visible from that
    /// module's own functions, and an importer cannot read an imported module's
    /// exported `const`/`let` via `module::NAME` from within one of *its* functions
    /// either — only from top-level statements. Cross-module *function* calls
    /// (`module::some_fn()`) work fine regardless (`FileModuleResolver`'s own doc
    /// comment says as much for same-module calls). This is why
    /// `ac_common.rhai` exposes its shared tag numbers as zero-arg functions
    /// (`ac::tag_power()`) rather than `const TAG_POWER`, and why `CST_570004_WW.rhai`
    /// declares its own tag constants directly instead of reading them off `ac::`.
    #[test]
    fn module_const_visibility_constraints() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("common.rhai"),
            "const MY_CONST = 42; fn get_const() { MY_CONST }",
        )
        .unwrap();

        // A module's own function can't see the module's own top-level const.
        std::fs::write(
            dir.path().join("main_a.rhai"),
            r#"import "common" as c; fn run_script() { c::get_const() }"#,
        )
        .unwrap();
        let ast_a = engine()
            .compile_file(dir.path().join("main_a.rhai"))
            .unwrap();
        let err = engine()
            .call_fn::<i64>(&mut Scope::new(), &ast_a, "run_script", ())
            .unwrap_err();
        assert!(matches!(*err, EvalAltResult::ErrorInFunctionCall(..)));

        // Nor can the importer read it via `c::MY_CONST` from within a function.
        std::fs::write(
            dir.path().join("main_b.rhai"),
            r#"import "common" as c; fn run_script() { c::MY_CONST }"#,
        )
        .unwrap();
        let ast_b = engine()
            .compile_file(dir.path().join("main_b.rhai"))
            .unwrap();
        let err = engine()
            .call_fn::<i64>(&mut Scope::new(), &ast_b, "run_script", ())
            .unwrap_err();
        assert!(matches!(*err, EvalAltResult::ErrorVariableNotFound(..)));
    }

    #[test]
    fn top_level_script_const_is_visible_to_its_own_function_with_persistent_scope() {
        // Contrast with module_function_sees_its_own_module_top_level_const: this AST
        // is called directly (not imported as a module), with a persistent Scope and
        // eval_ast:true on the first call, exactly like ScriptedDevice::call does —
        // that combination is what makes the difference.
        let ast = engine()
            .compile("const MY_CONST = 42; fn run_script() { MY_CONST }")
            .unwrap();
        let mut scope = Scope::new();
        let mut options = rhai::CallFnOptions::default();
        options.rewind_scope = false;
        let result: Result<i64, Box<EvalAltResult>> =
            engine().call_fn_with_options(options, &mut scope, &ast, "run_script", ());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn shared_dynamic_map_mutations_are_visible_through_every_clone() {
        // The basis for ctx.state(): a Dynamic::from_map(...).into_shared() clone
        // handed to the script each call must let mutations made in one call show up
        // in the next, without relying on rhai's own scope-persistence machinery
        // (which conflicts with `import` needing eval_ast:true on every call).
        let shared = Dynamic::from_map(rhai::Map::new()).into_shared();
        let clone_a = shared.clone();
        let clone_b = shared.clone();

        let ast = engine()
            .compile(r#"fn run_script(state, key, value) { state[key] = value; }"#)
            .unwrap();
        let mut scope = Scope::new();
        let _: () = engine()
            .call_fn(
                &mut scope,
                &ast,
                "run_script",
                (clone_a, "k".to_string(), 42_i64),
            )
            .unwrap();

        let map = clone_b.read_lock::<rhai::Map>().unwrap();
        assert_eq!(map.get("k").and_then(|d| d.as_int().ok()), Some(42));
    }

    /// **Known rhai constraint**: a registered native function that *returns* an
    /// already-shared `Dynamic` silently loses the sharing once the script
    /// index-assigns into the returned value — the mutation lands on a private copy,
    /// never the original. This is why `ctx.rs` exposes `state_get`/`state_set`/
    /// `state_has` (each doing its own get/set natively, locking the field directly)
    /// instead of a single `state()` a script would index itself.
    #[test]
    fn returning_a_shared_dynamic_from_a_registered_function_does_not_persist_a_script_side_mutation()
     {
        #[derive(Clone)]
        struct Holder {
            state: Dynamic,
        }
        impl Holder {
            fn state(&mut self) -> Dynamic {
                self.state.clone()
            }
        }

        let mut eng = Engine::new();
        eng.register_type_with_name::<Holder>("Holder")
            .register_fn("state", Holder::state);

        let ast = eng
            .compile(
                r#"
                fn run_script(holder, key, value) {
                    let s = holder.state();
                    s[key] = value;
                }
                "#,
            )
            .unwrap();

        let holder = Holder {
            state: Dynamic::from_map(rhai::Map::new()).into_shared(),
        };
        let probe = holder.state.clone();

        let mut scope = Scope::new();
        let _: () = eng
            .call_fn(
                &mut scope,
                &ast,
                "run_script",
                (holder, "k".to_string(), 42_i64),
            )
            .unwrap();

        let map = probe.read_lock::<rhai::Map>().unwrap();
        assert_eq!(map.get("k").and_then(|d| d.as_int().ok()), None);
    }

    #[test]
    fn a_native_setter_method_mutates_the_original_shared_value_where_returning_it_does_not() {
        // Contrast with the previous test: rather than handing the script a live
        // Dynamic to index-assign into (which rhai silently unshares on return from a
        // registered function — confirmed broken above), do the mutation natively via
        // a registered *setter* method that locks the original Dynamic directly. This
        // is why ctx.rs exposes `state_get`/`state_set`/`state_has` instead of a
        // single `state()` a script would index itself.
        #[derive(Clone)]
        struct Holder {
            state: Dynamic,
        }
        impl Holder {
            fn state_set(&mut self, key: String, value: Dynamic) {
                let mut map = self.state.write_lock::<rhai::Map>().unwrap();
                map.insert(key.into(), value);
            }
        }

        let mut eng = Engine::new();
        eng.register_type_with_name::<Holder>("Holder")
            .register_fn("state_set", Holder::state_set);

        let ast = eng
            .compile("fn run_script(holder, key, value) { holder.state_set(key, value); }")
            .unwrap();

        let holder = Holder {
            state: Dynamic::from_map(rhai::Map::new()).into_shared(),
        };
        let probe = holder.state.clone();

        let mut scope = Scope::new();
        let _: () = eng
            .call_fn(
                &mut scope,
                &ast,
                "run_script",
                (holder, "k".to_string(), 42_i64),
            )
            .unwrap();

        let map = probe.read_lock::<rhai::Map>().unwrap();
        assert_eq!(map.get("k").and_then(|d| d.as_int().ok()), Some(42));
    }

    /// **Known rhai constraint**: script-defined function arguments are passed *by
    /// value* — a plain `fn add_key(m, key, value) { m[key] = value; }` mutates only
    /// its own local copy of `m`, never the caller's variable, even though `m` looks
    /// like an ordinary mutable map from inside the function. Contrast with a
    /// *native* method called via `x.method(...)` syntax (`Array`/`Blob`'s built-in
    /// `.push`/`.append`, used throughout `ac_common.rhai`'s `send_tlv`) — those
    /// really do mutate the caller's variable, because native-method dispatch is a
    /// different code path from a plain script-function call. This is why
    /// `ac_common.rhai::add_field_topics` returns the modified `config` instead of
    /// mutating its `config` parameter in place, and every call site reassigns
    /// `config = ac::add_field_topics(ctx, config, ...);`.
    #[test]
    fn script_function_arguments_are_passed_by_value_not_by_reference() {
        let ast = engine()
            .compile(
                r#"
                fn add_key(m, key, value) {
                    m[key] = value;
                }
                fn run_script() {
                    let config = #{};
                    add_key(config, "x", 1);
                    config["x"]
                }
                "#,
            )
            .unwrap();
        let mut scope = Scope::new();
        let x = engine().call_fn::<i64>(&mut scope, &ast, "run_script", ());
        assert!(x.is_err(), "the mutation must not have reached `config`");
    }

    #[test]
    fn returning_the_mutated_argument_is_the_correct_fix_for_by_value_arguments() {
        // The pattern add_field_topics actually uses: return the modified value and
        // have the caller reassign, rather than relying on in-place mutation.
        let ast = engine()
            .compile(
                r#"
                fn add_key(m, key, value) {
                    m[key] = value;
                    m
                }
                fn run_script() {
                    let config = #{ components: #{} };
                    config.components["climate"] = #{ platform: "climate" };
                    config.components["climate"] = add_key(config.components["climate"], "state_topic", "x/y");
                    config.components["climate"]["state_topic"]
                }
                "#,
            )
            .unwrap();
        let mut scope = Scope::new();
        let topic: String = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        assert_eq!(topic, "x/y");
    }

    /// **Known rhai constraint**: with the default `rewind_scope: true`, a script's
    /// own top-level `let` is not merely reset every call — it's flat out invisible
    /// (`ErrorVariableNotFound`) to a function that reads or mutates it, even within
    /// one single call. `rewind_scope: false` fixes this even though nothing here
    /// relies on the scope surviving *across* calls (`ScriptedDevice` uses a fresh
    /// `Scope` every call regardless — see `scripted_device.rs::call_optional`).
    #[test]
    fn rewind_scope_false_is_required_for_a_function_to_see_its_own_scripts_top_level_let() {
        let ast = engine()
            .compile(
                r#"
                let seen = #{};
                fn run_script(key) {
                    seen[key] = true;
                    seen.len()
                }
                "#,
            )
            .unwrap();

        let mut scope = Scope::new();
        let with_default_rewind: Result<i64, Box<EvalAltResult>> =
            engine().call_fn(&mut scope, &ast, "run_script", ("k".to_string(),));
        assert!(matches!(
            *with_default_rewind.unwrap_err(),
            EvalAltResult::ErrorVariableNotFound(..)
        ));

        let mut scope = Scope::new();
        let mut options = rhai::CallFnOptions::default();
        options.rewind_scope = false;
        let len: i64 = engine()
            .call_fn_with_options(options, &mut scope, &ast, "run_script", ("k".to_string(),))
            .unwrap();
        assert_eq!(len, 1);
    }

    #[test]
    fn ordinary_scripts_are_unaffected_by_the_limits() {
        let ast = engine()
            .compile("fn run_script() { let a = []; for i in 0..10 { a.push(i); } a.len() }")
            .unwrap();
        let mut scope = Scope::new();
        let len: i64 = engine()
            .call_fn(&mut scope, &ast, "run_script", ())
            .unwrap();
        assert_eq!(len, 10);
    }
}
