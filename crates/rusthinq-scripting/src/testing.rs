//! Offline driver test adapter. Uses the production bounded host and opaque effects.
use crate::{Host, Output, drivers::Config};
use rhai::{
    Array, Dynamic, Engine, EvalAltResult, Map, Scope, module_resolvers::FileModuleResolver,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io,
    path::Path,
    sync::{Arc, Mutex},
};
type Result<T> = std::result::Result<T, Box<EvalAltResult>>;
#[derive(Clone)]
struct Device(Arc<Mutex<State>>);
struct State {
    host: Host,
    thinq2: bool,
    properties: BTreeMap<String, String>,
    events: BTreeMap<String, String>,
    descriptor: Option<Value>,
    sent: Vec<Vec<u8>>,
    clips: Vec<Value>,
    timers: BTreeMap<String, u64>,
    error: Option<String>,
}
impl State {
    fn invoke(&mut self, function: &str, input: &str) {
        let result = self.host.invoke(1, function, input);
        self.error = result.error.map(|e| format!("{e:?}"));
        for output in result.outputs {
            match output {
                Output::Publish(text) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        let topic = value["topic"].as_str().unwrap_or_default();
                        let payload = value["payload"].as_str().unwrap_or_default();
                        if topic == "ildevice/test-device" {
                            self.descriptor = serde_json::from_str(payload).ok();
                        } else if let Some(name) = topic.strip_prefix("rusthinq/test-device/event/")
                        {
                            self.events.insert(name.into(), payload.into());
                        } else if let Some(name) = topic.strip_prefix("rusthinq/test-device/") {
                            if value["retain"] == false {
                                self.events.insert(name.into(), payload.into());
                            } else {
                                self.properties.insert(name.into(), payload.into());
                            }
                        }
                    }
                }
                Output::Send(text) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        if value["cmd"] == "packet"
                            && let Some(text) = value["data"].as_str()
                            && let Ok(data) = rusthinq_protocol::hex::decode(text)
                        {
                            self.sent.push(data);
                        } else {
                            self.clips.push(value);
                        }
                    }
                }
                Output::Timer {
                    name,
                    after_ms: Some(delay),
                } => {
                    self.timers.insert(name, delay);
                }
                Output::Timer {
                    name,
                    after_ms: None,
                } => {
                    self.timers.remove(&name);
                }
            }
        }
        if self.sent.len() > 1024
            || self.clips.len() > 1024
            || self.properties.len() > 1024
            || self.events.len() > 128
            || self.timers.len() > 256
        {
            self.error = Some("test effect budget exceeded".into());
            self.sent.truncate(1024);
            self.clips.truncate(1024);
        }
    }
    fn feed(&mut self, bytes: &[u8]) {
        let input = if self.thinq2 {
            rusthinq_protocol::hex::encode(bytes)
        } else {
            use base64::Engine;
            rusthinq_protocol::hex::encode(serde_json::json!({"Header":{"x-lgedm-deviceId":"test-device"},"Body":{"Cmd":"Mon","Format":"B64","Data":base64::engine::general_purpose::STANDARD.encode(bytes)}}).to_string())
        };
        self.invoke("__data", &input);
    }
}
fn device(config: &Config, model: &str, thinq2: bool) -> Result<Device> {
    let compiled = config
        .prepare("test-device", model, thinq2, true)
        .map_err(|e| Box::<EvalAltResult>::from(format!("{e:?}")))?;
    let raw = if compiled.has_function("on_set_property", 3) {
        "fn __test_command(ctx,text){let cmd=json_parse(text);on_set_property(ctx,cmd.prop,cmd.value);}"
    } else {
        "fn __test_command(ctx,text){}"
    };
    let compiled = compiled
        .with_entry(raw)
        .map_err(|e| Box::<EvalAltResult>::from(format!("{e:?}")))?;
    Ok(Device(Arc::new(Mutex::new(State {
        host: Host::new(compiled),
        thinq2,
        properties: Default::default(),
        events: Default::default(),
        descriptor: None,
        sent: Vec::new(),
        clips: Vec::new(),
        timers: Default::default(),
        error: None,
    }))))
}

fn state(device: &Device) -> std::sync::MutexGuard<'_, State> {
    device.0.lock().unwrap_or_else(|e| e.into_inner())
}
fn dynamic(value: Option<String>) -> Dynamic {
    value.map(Dynamic::from).unwrap_or(Dynamic::UNIT)
}
fn json(value: &Dynamic) -> Result<Value> {
    if value.is_blob() {
        return Ok(Value::String(format!(
            "blob:{}",
            rusthinq_protocol::hex::encode(value.clone().cast::<Vec<u8>>())
        )));
    }
    if value.is_array() {
        let array = value.clone().cast::<Array>();
        return array
            .iter()
            .map(json)
            .collect::<Result<Vec<_>>>()
            .map(Value::Array);
    }
    rhai::serde::from_dynamic(value).map_err(|e| e.to_string().into())
}
fn equal(a: Dynamic, b: Dynamic, reason: &str) -> Result<()> {
    let a = json(&a)?;
    let b = json(&b)?;
    fn same(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b))
            }
            (Value::Object(a), Value::Object(b)) => {
                a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|b| same(v, b)))
            }
            _ => a == b,
        }
    }
    if same(&a, &b) {
        Ok(())
    } else {
        Err(format!("{reason}: actual={a}, expected={b}").into())
    }
}
fn engine(config: Config, model: String) -> Engine {
    let mut engine = Engine::new();
    engine
        .set_max_operations(10_000_000)
        .set_max_string_size(524288)
        .set_max_array_size(65536)
        .set_max_map_size(1024)
        .set_max_modules(16)
        .set_max_call_levels(64);
    engine.set_module_resolver(FileModuleResolver::new_with_path(
        config.directory.join("tests"),
    ));
    crate::codecs::install(&mut engine);
    let other = config.clone();
    let other_model = model.clone();
    engine
        .register_type_with_name::<Device>("Device")
        .register_fn("device", move || device(&config, &model, true))
        .register_fn("device_t1", move || device(&other, &other_model, false))
        .register_fn("start", |d: &mut Device| state(d).invoke("__init", ""))
        .register_fn("drop_device", |d: &mut Device| {
            state(d).invoke("__drop", "")
        })
        .register_fn("feed", |d: &mut Device, text: &str| -> Result<()> {
            let text = text
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>();
            let bytes = rusthinq_protocol::hex::decode(text)
                .map_err(|e| Box::<EvalAltResult>::from(e.to_string()))?;
            state(d).feed(&bytes);
            Ok(())
        })
        .register_fn("feed", |d: &mut Device, bytes: Vec<u8>| {
            state(d).feed(&bytes)
        })
        .register_fn("set", |d: &mut Device, prop: &str, value: &str| {
            let mut d = state(d);
            let command = if d.descriptor.is_some() {
                "__command"
            } else {
                "__test_command"
            };
            d.invoke(
                command,
                &serde_json::json!({"prop":prop,"value":value}).to_string(),
            );
        })
        .register_fn("fire", |d: &mut Device, name: &str| {
            let mut d = state(d);
            if d.timers.remove(name).is_some() {
                d.invoke("__timer", name);
            }
        })
        .register_fn("property", |d: &mut Device, name: &str| {
            dynamic(state(d).properties.get(name).cloned())
        })
        .register_fn("event", |d: &mut Device, name: &str| {
            dynamic(state(d).events.get(name).cloned())
        })
        .register_fn("script_error", |d: &mut Device| {
            dynamic(state(d).error.clone())
        })
        .register_fn("descriptor", |d: &mut Device| {
            let d = state(d);
            d.descriptor
                .as_ref()
                .map(|v| rhai::serde::to_dynamic(v).unwrap_or(Dynamic::UNIT))
                .unwrap_or(Dynamic::UNIT)
        })
        .register_fn("sent", |d: &mut Device| -> Array {
            state(d)
                .sent
                .iter()
                .cloned()
                .map(Dynamic::from_blob)
                .collect()
        })
        .register_fn("clips", |d: &mut Device| -> Array {
            state(d)
                .clips
                .iter()
                .map(|v| rhai::serde::to_dynamic(v).unwrap_or(Dynamic::UNIT))
                .collect()
        })
        .register_fn("timers", |d: &mut Device| -> Array {
            state(d)
                .timers
                .iter()
                .map(|(n, ms)| {
                    Dynamic::from(vec![Dynamic::from(n.clone()), Dynamic::from(*ms as i64)])
                })
                .collect()
        })
        .register_fn("sent_tlvs", |d: &mut Device, index: i64| -> Result<Array> {
            let d = state(d);
            let frame = usize::try_from(index)
                .ok()
                .and_then(|i| d.sent.get(i))
                .ok_or_else(|| Box::<EvalAltResult>::from("invalid sent index"))?;
            let bytes = frame
                .get(11..frame.len().saturating_sub(2))
                .ok_or_else(|| Box::<EvalAltResult>::from("not a TLV frame"))?;
            Ok(rusthinq_protocol::tlv::parse(bytes)
                .iter()
                .map(|v| Dynamic::from(vec![Dynamic::from(v.t as i64), Dynamic::from(v.v as i64)]))
                .collect())
        })
        .register_fn("expect", |condition: bool, text: &str| -> Result<()> {
            if condition { Ok(()) } else { Err(text.into()) }
        })
        .register_fn("expect_eq", |a: Dynamic, b: Dynamic| {
            equal(a, b, "expect_eq")
        })
        .register_fn("expect_eq", |a: Dynamic, b: Dynamic, text: &str| {
            equal(a, b, text)
        })
        .register_fn(
            "expect_props",
            |d: &mut Device, properties: Map| -> Result<()> {
                for (prop, value) in properties {
                    equal(
                        dynamic(state(d).properties.get(prop.as_str()).cloned()),
                        value,
                        &prop,
                    )?;
                }
                Ok(())
            },
        );
    engine
}
#[derive(Debug)]
pub struct Report {
    pub model: String,
    pub test: String,
    pub error: Option<String>,
}
/// Runs the repository's own assertions; IL conformance remains owned by that repository.
pub fn run(directory: &Path) -> io::Result<Vec<Report>> {
    let config = Config {
        directory: directory.canonicalize()?,
        topic_prefix: "rusthinq".into(),
        il_prefix: Some("ildevice".into()),
        bindings: Default::default(),
        watch: false,
    };
    config.validate()?;
    let mut reports = Vec::new();
    let mut paths = std::fs::read_dir(&config.directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    paths.sort();
    for path in paths {
        let Some(model) = path
            .file_stem()
            .and_then(|n| n.to_str())
            .filter(|n| !n.ends_with("_common"))
        else {
            continue;
        };
        if path.extension().is_none_or(|e| e != "rhai") {
            continue;
        }
        let test = config
            .directory
            .join("tests")
            .join(format!("{model}.test.rhai"));
        if !test.is_file() {
            reports.push(Report {
                model: model.into(),
                test: "test file".into(),
                error: Some("missing test file".into()),
            });
            continue;
        }
        let bytes = std::fs::read(&test)?;
        if bytes.len() > 524288 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "test source exceeded",
            ));
        }
        let text = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
        let engine = engine(config.clone(), model.into());
        let ast = engine
            .compile(text)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut names = ast
            .iter_functions()
            .filter(|f| f.name.starts_with("test_") && f.params.is_empty())
            .map(|f| f.name.to_string())
            .collect::<Vec<_>>();
        names.sort();
        if names.is_empty() {
            reports.push(Report {
                model: model.into(),
                test: "test functions".into(),
                error: Some("no tests".into()),
            });
        }
        for name in names {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                engine.call_fn::<Dynamic>(&mut Scope::new(), &ast, &name, ())
            }));
            let error = match result {
                Ok(Ok(_)) => None,
                Ok(Err(e)) => Some(e.to_string()),
                Err(_) => Some("test panicked".into()),
            };
            reports.push(Report {
                model: model.into(),
                test: name,
                error,
            });
        }
    }
    if reports.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no driver tests",
        ));
    }
    Ok(reports)
}
