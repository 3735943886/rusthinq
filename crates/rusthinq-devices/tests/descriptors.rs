//! Every driver's descriptor is well formed against the IL: known types and roles
//! (with the type each role requires), coherent `class` / `series` / `category`, and
//! `requires` naming a real binary.
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;
use serde_json::Value;

/// role -> the type the registry gives it (il.md section 9)
const ROLES: &[(&str, &str)] = &[
    ("available", "binary"),
    ("on", "binary"),
    ("mode", "select"),
    ("fan_speed", "select"),
    ("target_humidity", "number"),
    ("current_humidity", "number"),
    ("current_temperature", "number"),
    ("target_temperature", "number"),
    ("swing_vertical", "binary"),
    ("swing_horizontal", "binary"),
    ("action", "select"),
    ("brightness", "number"),
    ("color_temperature", "number"),
    ("color", "text"),
    ("color_mode", "select"),
    ("position", "number"),
    ("tilt", "number"),
    ("motion", "select"),
    ("open", "trigger"),
    ("close", "trigger"),
    ("stop", "trigger"),
    ("locked", "binary"),
    ("unlatch", "trigger"),
    ("opened", "binary"),
    ("alarm_state", "select"),
    ("arm_home", "trigger"),
    ("arm_away", "trigger"),
    ("arm_night", "trigger"),
    ("disarm", "trigger"),
    ("vacuum_state", "select"),
    ("start", "trigger"),
    ("pause", "trigger"),
    ("return_home", "trigger"),
    ("locate", "trigger"),
    ("battery", "number"),
];

/// Roles a producer must not declare writable (il.md O-6).
const READ_ONLY_ROLES: &[&str] = &[
    "available",
    "current_humidity",
    "current_temperature",
    "action",
    "color_mode",
    "motion",
    "alarm_state",
    "vacuum_state",
    "battery",
];

/// Every driver in `scripts/`: each `.rhai` that is not a shared module.
fn models() -> Vec<String> {
    let dir = format!("{}/../../scripts", env!("CARGO_MANIFEST_DIR"));
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|n| n.strip_suffix(".rhai").map(str::to_string))
        .filter(|n| !n.ends_with("_common"))
        .collect();
    names.sort();
    names
}

fn descriptor(model: &str) -> Value {
    rusthinq_devices::scripting::set_il_prefix(Some("il".to_string()));
    let path = format!("{}/../../scripts/{model}.rhai", env!("CARGO_MANIFEST_DIR"));
    let h = ScriptHarness::t2(&path, model);
    h.start();
    let (_, payload, _) = h
        .raw_publishes()
        .into_iter()
        .find(|(t, _, _)| t.starts_with("il/"))
        .unwrap_or_else(|| panic!("{model} published no descriptor"));
    serde_json::from_str(&payload).unwrap()
}

#[test]
fn every_driver_publishes_a_well_formed_descriptor() {
    for model in &models() {
        let d = descriptor(model);
        assert_eq!(d["il"], 0, "{model}");
        let props = d["props"]
            .as_object()
            .unwrap_or_else(|| panic!("{model}: no props"));
        assert!(props.contains_key("available"), "{model}: no availability");
        for (name, p) in props {
            let n = name.clone();
            let at = format!("{model}.{name}");
            let ty = p["type"]
                .as_str()
                .unwrap_or_else(|| panic!("{at}: no type"));
            assert!(
                ["binary", "number", "select", "text", "trigger", "event"].contains(&ty),
                "{at}: type {ty}"
            );
            let rw = p["rw"].as_bool().unwrap_or(false);
            if let Some(role) = p.get("role") {
                let role = role.as_str().unwrap();
                let want = ROLES
                    .iter()
                    .find(|(r, _)| *r == role)
                    .unwrap_or_else(|| panic!("{at}: unknown role {role}"))
                    .1;
                assert_eq!(ty, want, "{at}: role {role} has type {want}");
                assert!(
                    !(rw && READ_ONLY_ROLES.contains(&role)),
                    "{at}: role {role} is read only"
                );
            }
            if let Some(series) = p.get("series") {
                assert_eq!(ty, "number", "{at}: series only on numbers");
                assert!(
                    ["gauge", "counter"].contains(&series.as_str().unwrap()),
                    "{at}"
                );
            }
            if let Some(cat) = p.get("category") {
                let cat = cat.as_str().unwrap();
                assert!(
                    ["diagnostic", "config"].contains(&cat),
                    "{at}: category {cat}"
                );
                assert!(
                    cat != "config" || rw,
                    "{at}: a config control must be writable"
                );
                assert!(
                    cat != "diagnostic" || !rw,
                    "{at}: a diagnostic is not a control"
                );
            }
            if let Some(class) = p.get("class") {
                assert!(class.as_str().is_some_and(|c| !c.is_empty()), "{at}");
            }
            if ty == "trigger" {
                assert!(
                    p.get("rw").is_none_or(|v| v == true),
                    "{at}: a trigger is never rw:false"
                );
            }
            if ty == "event" {
                assert!(!rw, "{at}: an event is never writable");
            }
            if ty == "select" || ty == "event" {
                assert!(
                    !p["options"].as_array().unwrap_or(&vec![]).is_empty(),
                    "{at}: no options"
                );
            }
            if ty == "number" && p.get("min").is_some() && p.get("max").is_some() {
                assert!(p["min"].as_f64() <= p["max"].as_f64(), "{at}: min > max");
            }
            if let Some(req) = p.get("requires") {
                match req {
                    Value::String(name) => {
                        assert_ne!(name, &n, "{at}: requires itself");
                        assert_eq!(props[name]["type"], "binary", "{at}: requires {name}");
                    }
                    Value::Object(cond) => {
                        let name = cond["prop"].as_str().unwrap();
                        assert_ne!(name, n, "{at}: requires itself");
                        assert_eq!(props[name]["type"], "select", "{at}: requires {name}");
                        let options = props[name]["options"].as_array().unwrap();
                        let list = cond["in"].as_array().unwrap();
                        assert!(!list.is_empty(), "{at}: empty `in`");
                        assert!(
                            list.iter().all(|o| options.contains(o)),
                            "{at}: `in` not in options"
                        );
                    }
                    _ => panic!("{at}: requires is neither a string nor an object"),
                }
            }
        }
    }
}
