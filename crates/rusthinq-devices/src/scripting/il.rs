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
    /// The whole descriptor, as published.
    descriptor: Option<Value>,
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
        let mut state = self.0.lock();
        state.props = descriptor.get("props").and_then(Value::as_object).cloned();
        state.descriptor = Some(descriptor.clone());
    }

    /// The descriptor the driver last published, if any.
    pub fn descriptor(&self) -> Option<Value> {
        self.0.lock().descriptor.clone()
    }

    /// Record a value the driver just published (for `requires`).
    pub fn record_value(&self, prop: &str, value: &str) {
        self.0
            .lock()
            .values
            .insert(prop.to_string(), value.to_string());
    }

    /// The property with the role `available`, if the descriptor has one.
    fn available_prop(state: &State) -> Option<String> {
        state
            .props
            .as_ref()?
            .iter()
            .find(|(_, def)| def.get("role").and_then(Value::as_str) == Some("available"))
            .map(|(name, _)| name.clone())
    }

    /// The `available` property, if the descriptor has one and nothing has been reported for
    /// it yet: the device is not known to be available (il.md A-2), so the host says `false`.
    pub fn unreported_available(&self) -> Option<String> {
        let state = self.0.lock();
        Self::available_prop(&state).filter(|name| !state.values.contains_key(name))
    }

    /// Forget every reported value (the link is down, so none is current any more) and return
    /// the properties that must now be published as absent — all but `available`, which
    /// carries the down state itself (il.md R-4).
    pub fn take_stale_props(&self) -> Vec<String> {
        let mut state = self.0.lock();
        if state.props.is_none() {
            return Vec::new(); // no descriptor, no IL contract to keep
        }
        let available = Self::available_prop(&state);
        std::mem::take(&mut state.values)
            .into_keys()
            .filter(|name| Some(name) != available.as_ref())
            .collect()
    }

    /// Check a command. `Ok(None)` when there is no descriptor (nothing to check against),
    /// `Ok(Some(canonical))` for a valid one, `Err(reject)` otherwise.
    pub fn validate(&self, prop: &str, value: &str) -> Result<Option<String>, Reject> {
        let state = self.0.lock();
        match &state.props {
            None => Ok(None),
            Some(props) => validate(props, &state.values, prop, value).map(Some),
        }
    }
}

/// Why a command was refused: the IL's `code` (il.md section 5) and free text for people.
#[derive(Debug, PartialEq)]
pub struct Reject {
    pub code: &'static str,
    pub reason: String,
}

impl Reject {
    fn new(code: &'static str, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }

    /// The `reject` event body for `prop`.
    pub fn body(&self, prop: &str) -> Value {
        serde_json::json!({ "prop": prop, "code": self.code, "reason": self.reason })
    }
}

/// A reject a driver reports itself (the appliance answered that it will not do it) has no
/// `code`; the IL's is `refused`. Anything that is not a JSON object is passed through.
pub fn with_reject_code(payload: &str) -> String {
    match serde_json::from_str::<Value>(payload) {
        Ok(Value::Object(mut body)) => {
            body.entry("code").or_insert_with(|| "refused".into());
            Value::Object(body).to_string()
        }
        _ => payload.to_string(),
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

/// Whether `requires` holds now: a string names a binary that must be `true`, an object
/// `{prop, in}` a select whose value must be one of `in`. A value never reported counts as unmet.
fn requires_met(requires: &Value, values: &HashMap<String, String>) -> Result<(), Reject> {
    let (name, met) = match requires {
        Value::String(name) => (
            name.as_str(),
            values.get(name).and_then(|v| parse_bool(v)) == Some(true),
        ),
        Value::Object(cond) => {
            let name = cond.get("prop").and_then(Value::as_str).unwrap_or("");
            let met = values.get(name).is_some_and(|v| {
                cond.get("in")
                    .and_then(Value::as_array)
                    .is_some_and(|list| list.iter().any(|o| o.as_str() == Some(v)))
            });
            (name, met)
        }
        _ => return Ok(()),
    };
    if met {
        Ok(())
    } else {
        Err(Reject::new("requires_unmet", format!("requires {name}")))
    }
}

pub fn validate(
    props: &Map<String, Value>,
    values: &HashMap<String, String>,
    prop: &str,
    value: &str,
) -> Result<String, Reject> {
    let def = props
        .get(prop)
        .and_then(Value::as_object)
        .ok_or_else(|| Reject::new("unknown_property", "unknown property"))?;
    let ty = def.get("type").and_then(Value::as_str).unwrap_or("");
    // a trigger is a write-only action: always writable, its payload never used
    let writable = ty == "trigger" || def.get("rw").and_then(Value::as_bool) == Some(true);
    if !writable {
        return Err(Reject::new("read_only", "read-only property"));
    }

    if let Some(requires) = def.get("requires") {
        requires_met(requires, values)?;
    }

    let invalid = |reason: &str| Reject::new("invalid_value", reason);
    match ty {
        "trigger" => Ok(value.to_string()),
        "binary" => parse_bool(value)
            .map(|b| b.to_string())
            .ok_or_else(|| invalid("not a boolean")),
        "number" => {
            let n: f64 = value
                .trim()
                .parse()
                .ok()
                .filter(|n: &f64| n.is_finite())
                .ok_or_else(|| invalid("not a number"))?;
            let min = def.get("min").and_then(Value::as_f64);
            let max = def.get("max").and_then(Value::as_f64);
            if let Some(m) = min.filter(|m| n < *m) {
                return Err(Reject::new(
                    "out_of_range",
                    format!("below the minimum of {}", number_text(m)),
                ));
            }
            if let Some(m) = max.filter(|m| n > *m) {
                return Err(Reject::new(
                    "out_of_range",
                    format!("above the maximum of {}", number_text(m)),
                ));
            }
            if let Some(step) = def.get("step").and_then(Value::as_f64).filter(|s| *s > 0.0) {
                let base = min.unwrap_or(0.0);
                let steps = (n - base) / step;
                if (steps - steps.round()).abs() > 1e-9 {
                    return Err(Reject::new(
                        "bad_step",
                        format!("not a multiple of {}", number_text(step)),
                    ));
                }
            }
            Ok(number_text(n))
        }
        "select" => match def.get("options").and_then(Value::as_array) {
            Some(options) if !options.iter().any(|o| o.as_str() == Some(value)) => {
                Err(invalid("not one of the options"))
            }
            _ => Ok(value.to_string()),
        },
        "text" if value.is_empty() => Err(invalid("empty text")),
        // text, and a type this host does not know: the descriptor says nothing more to check
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
        validate(&props(), &HashMap::new(), prop, value).map_err(|r| r.reason)
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
            validate(&props(), &values, "go", "").map_err(|r| r.code),
            Err("requires_unmet")
        );
        values.insert("armed".to_string(), "true".to_string());
        assert_eq!(
            validate(&props(), &values, "lit", "on").as_deref(),
            Ok("true")
        );
    }

    #[test]
    fn a_reject_carries_the_il_code_of_the_failed_step() {
        let code = |p: &str, v: &str| validate(&props(), &HashMap::new(), p, v).unwrap_err().code;
        assert_eq!(code("nope", "1"), "unknown_property");
        assert_eq!(code("humidity", "1"), "read_only");
        assert_eq!(code("go", ""), "requires_unmet");
        assert_eq!(code("power", "x"), "invalid_value");
        assert_eq!(code("target", "75"), "out_of_range");
        assert_eq!(code("target", "42"), "bad_step");
        assert_eq!(code("note", ""), "invalid_value");
        let body = validate(&props(), &HashMap::new(), "note", "")
            .unwrap_err()
            .body("note");
        assert_eq!(body["code"], "invalid_value");
        assert_eq!(body["prop"], "note");
    }

    #[test]
    fn an_object_requires_needs_the_select_to_hold_a_listed_value() {
        let props = json!({
            "mode": { "type": "select", "rw": true, "options": ["cool", "heat"] },
            "save": { "type": "binary", "rw": true, "requires": { "prop": "mode", "in": ["cool"] } }
        })
        .as_object()
        .unwrap()
        .clone();
        let mut values = HashMap::new();
        let check =
            |v: &HashMap<String, String>| validate(&props, v, "save", "on").map_err(|r| r.code);
        assert_eq!(check(&values), Err("requires_unmet"));
        values.insert("mode".into(), "heat".into());
        assert_eq!(check(&values), Err("requires_unmet"));
        values.insert("mode".into(), "cool".into());
        assert_eq!(check(&values).as_deref(), Ok("true"));
    }

    #[test]
    fn a_driver_reject_without_a_code_is_refused() {
        let out: Value =
            serde_json::from_str(&with_reject_code(r#"{"prop":"power","reason":"busy"}"#)).unwrap();
        assert_eq!(out["code"], "refused");
        let kept = with_reject_code(r#"{"prop":"p","code":"unavailable"}"#);
        assert!(kept.contains("unavailable") && !kept.contains("refused"));
    }

    #[test]
    fn a_dropped_link_makes_every_value_but_available_stale() {
        let il = IlState::default();
        il.set_descriptor(&json!({ "props": {
            "up": { "type": "binary", "role": "available" },
            "power": { "type": "binary", "rw": true }
        } }));
        assert_eq!(il.unreported_available().as_deref(), Some("up"));
        il.record_value("up", "true");
        il.record_value("power", "true");
        assert_eq!(il.unreported_available(), None);
        assert_eq!(il.take_stale_props(), vec!["power".to_string()]);
        // the values are gone: nothing stale is left for `requires`, and availability is unreported again
        assert!(il.take_stale_props().is_empty());
        assert_eq!(il.unreported_available().as_deref(), Some("up"));
    }

    #[test]
    fn no_descriptor_means_nothing_to_check() {
        let il = IlState::default();
        assert_eq!(il.validate("anything", "x"), Ok(None));
        il.set_descriptor(&json!({ "props": { "power": { "type": "binary", "rw": true } } }));
        assert_eq!(il.validate("power", "off"), Ok(Some("false".into())));
        assert_eq!(
            il.validate("other", "1").map_err(|r| r.code),
            Err("unknown_property")
        );
        il.record_value("power", "true");
    }
}
