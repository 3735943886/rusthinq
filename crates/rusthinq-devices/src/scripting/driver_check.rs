//! Checks on a directory of drivers beyond what their own tests assert, so a driver
//! repository can gate itself with one command (`rusthinq-script-test <dir>`):
//!
//! - every driver has a `tests/<Model>.test.rhai` with at least one `test_*` function;
//! - every driver publishes a descriptor that is well formed against the IL: known types
//!   and roles (with the type each role requires), coherent `class` / `series` /
//!   `category`, and `requires` naming a real binary or select.

use crate::scripting::{ScriptHarness, set_il_prefix};
use serde_json::{Map, Value};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;

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

const TYPES: &[&str] = &["binary", "number", "select", "text", "trigger", "event"];

/// What `check_dir` looked at and what it found wrong.
pub struct CheckReport {
    pub drivers: usize,
    /// One line per problem, each starting with the driver (or `driver.prop`) it is about.
    pub problems: Vec<String>,
}

/// The drivers in `scripts_dir`: every `.rhai` that is not a shared `*_common` module.
pub fn drivers(scripts_dir: &Path) -> Result<Vec<String>, String> {
    let mut names: Vec<String> = std::fs::read_dir(scripts_dir)
        .map_err(|e| format!("{}: {e}", scripts_dir.display()))?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|n| n.strip_suffix(".rhai").map(str::to_string))
        .filter(|n| !n.ends_with("_common"))
        .collect();
    names.sort();
    Ok(names)
}

/// Run every check on every driver in `scripts_dir`.
pub fn check_dir(scripts_dir: &Path) -> Result<CheckReport, String> {
    let models = drivers(scripts_dir)?;
    let mut problems = Vec::new();
    for model in &models {
        check_has_tests(scripts_dir, model, &mut problems);
        match descriptor(scripts_dir, model) {
            Ok(d) => check_descriptor(model, &d, &mut problems),
            Err(e) => problems.push(format!("{model}: {e}")),
        }
    }
    Ok(CheckReport {
        drivers: models.len(),
        problems,
    })
}

fn check_has_tests(scripts_dir: &Path, model: &str, out: &mut Vec<String>) {
    let file = scripts_dir.join("tests").join(format!("{model}.test.rhai"));
    match std::fs::read_to_string(&file) {
        Err(_) => out.push(format!("{model}: no {}", file.display())),
        Ok(text) if !text.contains("fn test_") => {
            out.push(format!("{model}: {} has no test_ function", file.display()));
        }
        Ok(_) => {}
    }
}

/// The descriptor a driver publishes from `start`, run against a harness device.
fn descriptor(scripts_dir: &Path, model: &str) -> Result<Value, String> {
    let path = scripts_dir.join(format!("{model}.rhai"));
    set_il_prefix(Some("il".to_string()));
    let published = catch_unwind(AssertUnwindSafe(|| {
        let h = ScriptHarness::t2(&path, model);
        h.start();
        h.raw_publishes()
    }))
    .map_err(|_| "could not start the driver".to_string())?;
    let (_, payload, _) = published
        .into_iter()
        .find(|(t, _, _)| t.starts_with("il/"))
        .ok_or_else(|| "published no descriptor".to_string())?;
    serde_json::from_str(&payload).map_err(|e| format!("descriptor is not JSON: {e}"))
}

fn check_descriptor(model: &str, d: &Value, out: &mut Vec<String>) {
    if d["il"] != 0 {
        out.push(format!("{model}: il is not 0"));
    }
    let Some(props) = d["props"].as_object() else {
        out.push(format!("{model}: no props"));
        return;
    };
    if !props.contains_key("available") {
        out.push(format!("{model}: no availability"));
    }
    for (name, p) in props {
        check_prop(model, name, p, props, out);
    }
}

fn check_prop(
    model: &str,
    name: &str,
    p: &Value,
    props: &Map<String, Value>,
    out: &mut Vec<String>,
) {
    let mut bad = |msg: &str| out.push(format!("{model}.{name}: {msg}"));

    let Some(ty) = p["type"].as_str() else {
        bad("no type");
        return;
    };
    if !TYPES.contains(&ty) {
        bad(&format!("type {ty}"));
    }
    let rw = p["rw"].as_bool().unwrap_or(false);

    if let Some(role) = p.get("role") {
        match role
            .as_str()
            .and_then(|r| ROLES.iter().find(|(known, _)| *known == r))
        {
            None => bad(&format!("unknown role {role}")),
            Some((role, want)) => {
                if ty != *want {
                    bad(&format!("role {role} is type {want}, not {ty}"));
                }
                if rw && READ_ONLY_ROLES.contains(role) {
                    bad(&format!("role {role} is read only"));
                }
            }
        }
    }
    if let Some(series) = p.get("series") {
        if ty != "number" {
            bad("series only on numbers");
        }
        if !series
            .as_str()
            .is_some_and(|s| ["gauge", "counter"].contains(&s))
        {
            bad("series must be gauge or counter");
        }
    }
    if let Some(cat) = p.get("category") {
        match cat.as_str() {
            Some("config") if !rw => bad("a config control must be writable"),
            Some("diagnostic") if rw => bad("a diagnostic is not a control"),
            Some("config" | "diagnostic") => {}
            _ => bad(&format!("category {cat}")),
        }
    }
    if let Some(class) = p.get("class")
        && class.as_str().is_none_or(|c| c.is_empty())
    {
        bad("empty class");
    }
    if ty == "trigger" && p.get("rw").is_some_and(|v| v.as_bool() != Some(true)) {
        bad("a trigger is never rw:false");
    }
    if ty == "event" && rw {
        bad("an event is never writable");
    }
    if (ty == "select" || ty == "event") && p["options"].as_array().is_none_or(Vec::is_empty) {
        bad("no options");
    }
    if ty == "number"
        && let (Some(min), Some(max)) = (p["min"].as_f64(), p["max"].as_f64())
        && min > max
    {
        bad("min > max");
    }
    if let Some(req) = p.get("requires") {
        match req {
            Value::String(other) => {
                if other == name {
                    bad("requires itself");
                } else if props.get(other).and_then(|t| t["type"].as_str()) != Some("binary") {
                    bad(&format!("requires {other}, which is not a binary"));
                }
            }
            Value::Object(cond) => {
                let Some(other) = cond.get("prop").and_then(Value::as_str) else {
                    bad("requires has no prop");
                    return;
                };
                if other == name {
                    bad("requires itself");
                    return;
                }
                let target = props.get(other);
                if target.and_then(|t| t["type"].as_str()) != Some("select") {
                    bad(&format!("requires {other}, which is not a select"));
                    return;
                }
                let options = target
                    .and_then(|t| t["options"].as_array())
                    .map_or(&[][..], Vec::as_slice);
                let list = cond
                    .get("in")
                    .and_then(Value::as_array)
                    .map_or(&[][..], Vec::as_slice);
                if list.is_empty() {
                    bad("empty `in`");
                } else if !list.iter().all(|o| options.contains(o)) {
                    bad("`in` not in options");
                }
            }
            _ => bad("requires is neither a string nor an object"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn problems(d: Value) -> Vec<String> {
        let mut out = Vec::new();
        check_descriptor("m", &d, &mut out);
        out
    }

    fn descriptor_with(props: Value) -> Value {
        let mut all = json!({"available": {"type": "binary", "role": "available"}});
        if let (Some(all), Some(extra)) = (all.as_object_mut(), props.as_object()) {
            all.extend(extra.clone());
        }
        json!({"il": 0, "props": all})
    }

    #[test]
    fn a_plain_descriptor_is_clean() {
        let d = descriptor_with(json!({
            "temp": {"type": "number", "class": "temperature", "min": 1, "max": 9},
            "mode": {"type": "select", "options": ["a", "b"], "rw": true},
            "extra": {"type": "text", "rw": true, "requires": {"prop": "mode", "in": ["a"]}},
        }));
        assert_eq!(problems(d), Vec::<String>::new());
    }

    #[test]
    fn a_missing_availability_and_a_bad_il_are_reported() {
        let found = problems(json!({"il": 1, "props": {"x": {"type": "binary"}}}));
        assert!(found.iter().any(|p| p.contains("il is not 0")));
        assert!(found.iter().any(|p| p.contains("no availability")));
    }

    #[test]
    fn contradictory_props_are_each_reported() {
        let d = descriptor_with(json!({
            "a": {"type": "select"},
            "b": {"type": "number", "min": 5, "max": 1},
            "c": {"type": "binary", "category": "config"},
            "d": {"type": "binary", "category": "diagnostic", "rw": true},
            "e": {"type": "binary", "requires": "a"},
            "f": {"type": "binary", "requires": "f"},
            "g": {"type": "select", "options": ["x"], "requires": {"prop": "a", "in": ["z"]}},
            "h": {"type": "number", "role": "on"},
            "i": {"type": "binary", "role": "battery"},
        }));
        let found = problems(d).join("\n");
        for want in [
            "m.a: no options",
            "m.b: min > max",
            "m.c: a config control must be writable",
            "m.d: a diagnostic is not a control",
            "m.e: requires a, which is not a binary",
            "m.f: requires itself",
            "m.h: role on is type binary, not number",
            "m.i: role battery is type number, not binary",
        ] {
            assert!(found.contains(want), "missing `{want}` in:\n{found}");
        }
    }

    #[test]
    fn an_unusable_driver_directory_is_an_error() {
        assert!(check_dir(Path::new("/nonexistent/rusthinq-drivers")).is_err());
    }
}
