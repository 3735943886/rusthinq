//! Driver tests written in Rhai, next to the driver they test.
//!
//! `<scripts>/tests/<Model>.test.rhai` tests `<scripts>/<Model>.rhai`. Every zero-argument
//! function named `test_*` in it is one test; it runs against a fresh device and fails on the
//! first error (a failed `expect*`, or any runtime error). Test files can `import` helper
//! modules that sit in the same `tests/` directory.
//!
//! What a test sees, beyond everything a driver script sees (`hex_decode`, `tlv_frame_build`,
//! `aabb_wrap`, …):
//!
//! - `device()` (or `device_t1()`): a fresh device running the driver. On it:
//!   `start()`, `drop_device()`, `feed(hex or blob)`, `set(prop, value)`, `fire(timer)`,
//!   `property(name)`, `event(name)`, `script_error()`, `sent()` (the frames the driver sent,
//!   as blobs), `sent_tlvs(i)` (frame `i` as `[[tag, value], ..]`), `timers()`
//!   (`[[name, delay_ms], ..]`), `clips()` (`[#{cmd, type, data}, ..]`), `descriptor()` (the
//!   IL descriptor it published, a map, or `()`).
//! - `expect(cond, msg)`, `expect_eq(actual, expected, msg)`, `expect_props(dev, #{prop: "value"})`.
//!
//! Rhai functions cannot see script-level constants, so a test file keeps its captured frames
//! in functions (`fn caps() { "0000…" }`).

use crate::scripting::engine::new_engine;
use crate::scripting::harness::ScriptHarness;
use rhai::{Array, CallFnOptions, Dynamic, Engine, EvalAltResult, Map, Scope};
use serde_json::Value;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The outcome of one `test_*` function.
pub struct Outcome {
    pub name: String,
    /// `None` when it passed.
    pub error: Option<String>,
}

/// The outcomes of one `.test.rhai` file.
pub struct FileReport {
    pub file: PathBuf,
    pub outcomes: Vec<Outcome>,
}

#[derive(Clone)]
struct Dev(Arc<ScriptHarness>);

type Res<T> = Result<T, Box<EvalAltResult>>;

fn fail<T>(msg: String) -> Res<T> {
    Err(msg.into())
}

fn hex_bytes(text: &str) -> Res<Vec<u8>> {
    let cleaned: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    rusthinq_util::hex::decode(&cleaned).or_else(|e| fail(format!("bad hex ({e}): {text}")))
}

/// A Rhai value as JSON, for comparing and printing. A blob is a `"blob:<hex>"` string.
fn to_json(d: &Dynamic) -> Res<Value> {
    if d.is_unit() {
        Ok(Value::Null)
    } else if let Ok(b) = d.as_bool() {
        Ok(Value::Bool(b))
    } else if let Ok(n) = d.as_int() {
        Ok(Value::from(n))
    } else if let Ok(f) = d.as_float() {
        Ok(Value::from(f))
    } else if d.is_blob() {
        let bytes = d.clone().into_blob().unwrap_or_default();
        Ok(Value::String(format!(
            "blob:{}",
            rusthinq_util::hex::encode(&bytes)
        )))
    } else if d.is_array() {
        let items = d.clone().into_array().unwrap_or_default();
        Ok(Value::Array(items.iter().map(to_json).collect::<Res<_>>()?))
    } else if d.is_map() {
        let map = d.clone().try_cast::<Map>().unwrap_or_default();
        let mut out = serde_json::Map::new();
        for (k, v) in &map {
            out.insert(k.to_string(), to_json(v)?);
        }
        Ok(Value::Object(out))
    } else if let Ok(s) = d.clone().into_string() {
        Ok(Value::String(s))
    } else {
        fail(format!("cannot compare a value of type {}", d.type_name()))
    }
}

fn from_json(v: &Value) -> Dynamic {
    match v {
        Value::Null => Dynamic::UNIT,
        Value::Bool(b) => (*b).into(),
        Value::Number(n) => match n.as_i64() {
            Some(i) => i.into(),
            None => n.as_f64().unwrap_or(0.0).into(),
        },
        Value::String(s) => s.clone().into(),
        Value::Array(a) => a.iter().map(from_json).collect::<Array>().into(),
        Value::Object(o) => o
            .iter()
            .map(|(k, v)| (k.as_str().into(), from_json(v)))
            .collect::<Map>()
            .into(),
    }
}

/// Equality that treats `1` and `1.0` as the same number.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

fn expect_eq(actual: Dynamic, expected: Dynamic, msg: &str) -> Res<()> {
    let (a, e) = (to_json(&actual)?, to_json(&expected)?);
    if same(&a, &e) {
        Ok(())
    } else {
        fail(format!("{msg}\n  actual:   {a}\n  expected: {e}"))
    }
}

fn register(engine: &mut Engine, driver: &Path, model: &str) {
    let (d1, m1) = (driver.to_path_buf(), model.to_string());
    let (d2, m2) = (driver.to_path_buf(), model.to_string());
    engine
        .register_type_with_name::<Dev>("Device")
        .register_fn("device", move || Dev(Arc::new(ScriptHarness::t2(&d1, &m1))))
        .register_fn("device_t1", move || {
            Dev(Arc::new(ScriptHarness::t1(&d2, &m2)))
        })
        .register_fn("start", |d: &mut Dev| {
            d.0.start();
        })
        .register_fn("drop_device", |d: &mut Dev| {
            d.0.drop_device();
        })
        .register_fn("feed", |d: &mut Dev, hex: &str| -> Res<()> {
            hex_bytes(hex)?;
            d.0.feed_hex(hex);
            Ok(())
        })
        .register_fn("feed", |d: &mut Dev, bytes: Vec<u8>| {
            d.0.feed_hex(&rusthinq_util::hex::encode(&bytes));
        })
        .register_fn("set", |d: &mut Dev, prop: &str, value: &str| {
            d.0.set_property(prop, value);
        })
        .register_fn("fire", |d: &mut Dev, name: &str| {
            d.0.fire_timer(name);
        })
        .register_fn("property", |d: &mut Dev, name: &str| -> Dynamic {
            d.0.property(name).map_or(Dynamic::UNIT, Dynamic::from)
        })
        .register_fn("event", |d: &mut Dev, name: &str| -> Dynamic {
            d.0.event(name).map_or(Dynamic::UNIT, Dynamic::from)
        })
        .register_fn("script_error", |d: &mut Dev| -> Dynamic {
            d.0.script_error().map_or(Dynamic::UNIT, Dynamic::from)
        })
        .register_fn("sent", |d: &mut Dev| -> Array {
            d.0.sent_raw().into_iter().map(Dynamic::from_blob).collect()
        })
        .register_fn("sent_tlvs", |d: &mut Dev, i: i64| -> Res<Array> {
            let sent = d.0.sent_raw();
            let frame = usize::try_from(i)
                .ok()
                .and_then(|i| sent.get(i))
                .ok_or_else(|| format!("sent_tlvs({i}): only {} frame(s) were sent", sent.len()))?;
            let Some(tlv_bytes) = frame
                .get(11..frame.len().saturating_sub(2))
                .filter(|_| frame.len() >= 13)
            else {
                return fail(format!("sent_tlvs({i}): not a TLV frame"));
            };
            Ok(rusthinq_util::tlv::parse(tlv_bytes)
                .into_iter()
                .map(|t| {
                    Dynamic::from(vec![
                        Dynamic::from(i64::from(t.t)),
                        Dynamic::from(i64::from(t.v)),
                    ])
                })
                .collect())
        })
        .register_fn("timers", |d: &mut Dev| -> Array {
            d.0.pending_timers()
                .into_iter()
                .map(|(n, ms)| Dynamic::from(vec![Dynamic::from(n), Dynamic::from(ms as i64)]))
                .collect()
        })
        .register_fn("clips", |d: &mut Dev| -> Array {
            d.0.sent_clip()
                .into_iter()
                .map(|(cmd, ty, data)| {
                    let mut m = Map::new();
                    m.insert("cmd".into(), cmd.into());
                    m.insert("type".into(), i64::from(ty).into());
                    m.insert("data".into(), from_json(&data));
                    Dynamic::from(m)
                })
                .collect()
        })
        .register_fn("descriptor", |d: &mut Dev| -> Dynamic {
            d.0.descriptor().map_or(Dynamic::UNIT, |v| from_json(&v))
        })
        .register_fn("expect", |cond: bool, msg: &str| -> Res<()> {
            if cond { Ok(()) } else { fail(msg.to_string()) }
        })
        .register_fn("expect_eq", |a: Dynamic, e: Dynamic, msg: &str| {
            expect_eq(a, e, msg)
        })
        .register_fn("expect_eq", |a: Dynamic, e: Dynamic| {
            expect_eq(a, e, "expect_eq")
        })
        .register_fn("expect_props", |d: &mut Dev, want: Map| -> Res<()> {
            for (prop, value) in &want {
                expect_eq(
                    d.0.property(prop).map_or(Dynamic::UNIT, Dynamic::from),
                    value.clone(),
                    &format!("property {prop}"),
                )?;
            }
            Ok(())
        });
}

/// Run every `test_*` of one `<scripts>/tests/<Model>.test.rhai`.
pub fn run_test_file(test_path: &Path) -> Result<FileReport, String> {
    let file_name = test_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("bad test path")?;
    let model = file_name
        .strip_suffix(".test.rhai")
        .ok_or_else(|| format!("{file_name}: a driver test is named <Model>.test.rhai"))?;
    let scripts_dir = test_path
        .parent()
        .and_then(Path::parent)
        .ok_or("a test file belongs in <scripts>/tests/")?;
    let driver = scripts_dir.join(format!("{model}.rhai"));
    if !driver.is_file() {
        return Err(format!(
            "{}: no driver {}",
            test_path.display(),
            driver.display()
        ));
    }

    let mut engine = new_engine();
    register(&mut engine, &driver, model);
    let ast = engine
        .compile_file(test_path.to_path_buf())
        .map_err(|e| format!("{}: {e}", test_path.display()))?;

    let mut names: Vec<String> = ast
        .iter_functions()
        .filter(|f| f.name.starts_with("test_") && f.params.is_empty())
        .map(|f| f.name.to_string())
        .collect();
    names.sort();

    let outcomes = names
        .into_iter()
        .map(|name| {
            let run = catch_unwind(AssertUnwindSafe(|| {
                engine.call_fn_with_options::<Dynamic>(
                    CallFnOptions::new().rewind_scope(false),
                    &mut Scope::new(),
                    &ast,
                    &name,
                    (),
                )
            }));
            let error = match run {
                Ok(Ok(_)) => None,
                Ok(Err(e)) => Some(e.to_string()),
                Err(_) => Some("panicked".to_string()),
            };
            Outcome { name, error }
        })
        .collect();
    Ok(FileReport {
        file: test_path.to_path_buf(),
        outcomes,
    })
}

/// The `<scripts>/tests/*.test.rhai` files, sorted.
pub fn test_files(scripts_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let dir = scripts_dir.join("tests");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".test.rhai"))
        .collect();
    files.sort();
    Ok(files)
}

/// Run every driver test under `scripts_dir`.
pub fn run_dir(scripts_dir: &Path) -> Result<Vec<FileReport>, String> {
    test_files(scripts_dir)?
        .iter()
        .map(|f| run_test_file(f))
        .collect()
}

/// Print a report, one line per test and the reason under each failure. Returns the number
/// of failures.
pub fn print_reports(reports: &[FileReport]) -> usize {
    let mut failed = 0;
    for r in reports {
        println!("{}", r.file.display());
        for o in &r.outcomes {
            match &o.error {
                None => println!("  ok    {}", o.name),
                Some(e) => {
                    failed += 1;
                    println!(
                        "  FAIL  {}\n        {}",
                        o.name,
                        e.replace('\n', "\n        ")
                    );
                }
            }
        }
    }
    let total: usize = reports.iter().map(|r| r.outcomes.len()).sum();
    println!("{} passed, {failed} failed", total - failed);
    failed
}
