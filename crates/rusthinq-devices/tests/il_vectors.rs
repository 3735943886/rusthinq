//! Runs the IL specification's command vectors (`../ildevice/vectors/commands.json`) through
//! the host's validator. The specification lives in a sibling repository, so the test is
//! skipped when that checkout is not next to this one.
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::il::validate;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

/// The text form a value takes on the wire (a bool as `true`, a trigger's null as empty).
fn wire(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[test]
fn the_host_validator_agrees_with_the_il_command_vectors() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../ildevice/vectors/commands.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        eprintln!("skipped: {} not found", path.display());
        return;
    };
    let doc: Value = serde_json::from_str(&text).unwrap();
    let props = doc["descriptor"]["props"].as_object().unwrap();

    let mut failures = Vec::new();
    for case in doc["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let values: HashMap<String, String> = case["state"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), wire(v)))
            .collect();
        let got = validate(
            props,
            &values,
            case["prop"].as_str().unwrap(),
            &wire(&case["value"]),
        );
        let expect = &case["expect"];
        let ok = match (got, expect.get("reject").and_then(Value::as_str)) {
            (Err(reject), Some(code)) => reject.code == code,
            (Ok(canonical), None) => match &expect["accept"] {
                Value::Null => true, // a trigger: the payload is not used
                Value::Number(n) => canonical.parse::<f64>().ok() == n.as_f64(),
                accepted => canonical == wire(accepted),
            },
            _ => false,
        };
        if !ok {
            failures.push(name.to_string());
        }
    }
    assert!(failures.is_empty(), "vectors that disagree: {failures:?}");
}
