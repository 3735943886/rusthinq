//! Host-side validation of a command against the device's IL descriptor.
//!
//! A driver publishes its descriptor with `ctx.publish_il`; the host keeps the `props` map
//! and the last value the driver published for each property, and checks every `set` against
//! them *before* the script's `on_set_property` runs, so a script only ever sees a value the
//! descriptor allows, already in its canonical form. The descriptor is the single source of
//! truth: writability, `requires`, the type, the options, the range and the step are the IL's
//! rules, not something each script re-implements slightly differently.
//!
//! A device whose script never publishes a descriptor is not validated (its commands go
//! straight to the script, as before).

use rusthinq_util::sync::Mutex;
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(Default)]
struct State {
    /// The descriptor's `props` object, once `publish_il` has been called.
    props: Option<Map<String, Value>>,
    /// The last value published for each property (`ctx.publish_property`).
    values: HashMap<String, String>,
}

/// One device's descriptor and last-published values.
#[derive(Default)]
pub struct IlState(Mutex<State>);

impl IlState {
    /// Remember the descriptor's `props`. A descriptor with no `props` object clears it.
    pub fn set_descriptor(&self, descriptor: &Value) {
        self.0.lock().props = descriptor.get("props").and_then(Value::as_object).cloned();
    }

    /// Record a value the driver just published (for `requires`).
    pub fn record_value(&self, prop: &str, value: &str) {
        self.0
            .lock()
            .values
            .insert(prop.to_string(), value.to_string());
    }

    /// Check a command. `Ok(None)` when there is no descriptor (nothing to check against),
    /// `Ok(Some(canonical))` for a valid one, `Err(reason)` otherwise.
    pub fn validate(&self, prop: &str, value: &str) -> Result<Option<String>, String> {
        let state = self.0.lock();
        match &state.props {
            None => Ok(None),
            Some(props) => validate(props, &state.values, prop, value).map(Some),
        }
    }
}

fn parse_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "on" | "1" => Some(true),
        "false" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// The canonical text of a number: an integer when it is one, else the shortest float.
fn number_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

pub fn validate(
    props: &Map<String, Value>,
    values: &HashMap<String, String>,
    prop: &str,
    value: &str,
) -> Result<String, String> {
    let def = props
        .get(prop)
        .and_then(Value::as_object)
        .ok_or_else(|| "unknown property".to_string())?;
    let ty = def.get("type").and_then(Value::as_str).unwrap_or("");
    // a trigger is a write-only action: always writable, its payload never used
    let writable = ty == "trigger" || def.get("rw").and_then(Value::as_bool) == Some(true);
    if !writable {
        return Err("read-only property".to_string());
    }

    if let Some(required) = def.get("requires").and_then(Value::as_str) {
        let armed = values.get(required).and_then(|v| parse_bool(v)) == Some(true);
        if !armed {
            return Err(format!("requires {required}"));
        }
    }

    match ty {
        "trigger" => Ok(value.to_string()),
        "binary" => parse_bool(value)
            .map(|b| b.to_string())
            .ok_or_else(|| "not a boolean".to_string()),
        "number" => {
            let n: f64 = value
                .trim()
                .parse()
                .ok()
                .filter(|n: &f64| n.is_finite())
                .ok_or_else(|| "not a number".to_string())?;
            let min = def.get("min").and_then(Value::as_f64);
            let max = def.get("max").and_then(Value::as_f64);
            if min.is_some_and(|m| n < m) {
                return Err(format!(
                    "below the minimum of {}",
                    number_text(min.unwrap())
                ));
            }
            if max.is_some_and(|m| n > m) {
                return Err(format!(
                    "above the maximum of {}",
                    number_text(max.unwrap())
                ));
            }
            if let Some(step) = def.get("step").and_then(Value::as_f64).filter(|s| *s > 0.0) {
                let base = min.unwrap_or(0.0);
                let steps = (n - base) / step;
                if (steps - steps.round()).abs() > 1e-9 {
                    return Err(format!("not a multiple of {}", number_text(step)));
                }
            }
            Ok(number_text(n))
        }
        "select" => match def.get("options").and_then(Value::as_array) {
            Some(options) if !options.iter().any(|o| o.as_str() == Some(value)) => {
                Err("not one of the options".to_string())
            }
            _ => Ok(value.to_string()),
        },
        // text, and a type this host does not know: the descriptor says nothing to check
        _ => Ok(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn props() -> Map<String, Value> {
        json!({
            "power": { "type": "binary", "rw": true },
            "mode": { "type": "select", "rw": true, "options": ["smart", "jet"] },
            "target": { "type": "number", "rw": true, "min": 30, "max": 70, "step": 5 },
            "timer": { "type": "number", "rw": true, "min": 0, "max": 540, "step": 60, "unit": "min" },
            "ratio": { "type": "number", "rw": true, "min": 0, "max": 1, "step": 0.1 },
            "humidity": { "type": "number" },
            "note": { "type": "text", "rw": true },
            "go": { "type": "trigger", "requires": "armed" },
            "armed": { "type": "binary" },
            "lit": { "type": "binary", "rw": true, "requires": "armed" }
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn check(prop: &str, value: &str) -> Result<String, String> {
        validate(&props(), &HashMap::new(), prop, value)
    }

    #[test]
    fn a_binary_is_normalised_and_anything_else_is_refused() {
        assert_eq!(check("power", "ON").as_deref(), Ok("true"));
        assert_eq!(check("power", "0").as_deref(), Ok("false"));
        assert_eq!(check("power", "maybe"), Err("not a boolean".into()));
    }

    #[test]
    fn a_select_must_be_one_of_its_options() {
        assert_eq!(check("mode", "jet").as_deref(), Ok("jet"));
        assert_eq!(check("mode", "turbo"), Err("not one of the options".into()));
    }

    #[test]
    fn a_number_is_checked_against_min_max_and_step_and_returned_canonical() {
        assert_eq!(check("target", "45").as_deref(), Ok("45"));
        assert_eq!(check("target", "45.0").as_deref(), Ok("45"));
        assert_eq!(check("target", "25"), Err("below the minimum of 30".into()));
        assert_eq!(check("target", "75"), Err("above the maximum of 70".into()));
        assert_eq!(check("target", "42"), Err("not a multiple of 5".into()));
        assert_eq!(check("target", "abc"), Err("not a number".into()));
        assert_eq!(check("target", "NaN"), Err("not a number".into()));
        assert_eq!(check("timer", "120").as_deref(), Ok("120"));
        assert_eq!(check("timer", "90"), Err("not a multiple of 60".into()));
    }

    #[test]
    fn a_fractional_step_tolerates_float_error() {
        assert_eq!(check("ratio", "0.3").as_deref(), Ok("0.3"));
        assert_eq!(check("ratio", "0.35"), Err("not a multiple of 0.1".into()));
    }

    #[test]
    fn a_read_only_or_unknown_property_is_refused() {
        assert_eq!(check("humidity", "50"), Err("read-only property".into()));
        assert_eq!(check("nope", "1"), Err("unknown property".into()));
    }

    #[test]
    fn text_passes_through_and_a_trigger_ignores_its_payload() {
        assert_eq!(check("note", "hello").as_deref(), Ok("hello"));
        let mut values = HashMap::new();
        values.insert("armed".to_string(), "true".to_string());
        assert_eq!(validate(&props(), &values, "go", "").as_deref(), Ok(""));
        assert_eq!(
            validate(&props(), &values, "go", "anything").as_deref(),
            Ok("anything")
        );
    }

    #[test]
    fn requires_names_a_binary_that_must_currently_be_true() {
        assert_eq!(check("go", ""), Err("requires armed".into()));
        assert_eq!(check("lit", "true"), Err("requires armed".into()));
        let mut values = HashMap::new();
        values.insert("armed".to_string(), "false".to_string());
        assert_eq!(
            validate(&props(), &values, "go", ""),
            Err("requires armed".into())
        );
        values.insert("armed".to_string(), "true".to_string());
        assert_eq!(
            validate(&props(), &values, "lit", "on").as_deref(),
            Ok("true")
        );
    }

    #[test]
    fn no_descriptor_means_nothing_to_check() {
        let il = IlState::default();
        assert_eq!(il.validate("anything", "x"), Ok(None));
        il.set_descriptor(&json!({ "props": { "power": { "type": "binary", "rw": true } } }));
        assert_eq!(il.validate("power", "off"), Ok(Some("false".into())));
        assert_eq!(il.validate("other", "1"), Err("unknown property".into()));
        il.record_value("power", "true");
    }
}
