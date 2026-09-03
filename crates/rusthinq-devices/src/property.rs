//! A device property's value type.
//!
//! This is the only piece of "device modeling" that survives as a shared Rust type —
//! it is a plain value union (string/int/float), not a schema belonging to any
//! particular downstream integration. Anything that decides what a property *means* to
//! some consumer is that consumer's renderer, built in Rhai on top of the transport
//! primitives in `rusthinq_core::mqtt`.

/// A device property's value, as produced by a device handler. `MqttConnection` itself
/// only ever sees the `.as_string()` form — this type exists for the ergonomic `From`
/// impls device handlers (native or, eventually, Rhai-marshaled) construct values with.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Str(String),
    Num(f64),
    Int(i64),
}

impl PropertyValue {
    pub fn as_string(&self) -> String {
        match self {
            PropertyValue::Str(s) => s.clone(),
            PropertyValue::Num(n) => {
                if *n == (*n as i64) as f64 {
                    format!("{}", *n as i64)
                } else {
                    n.to_string()
                }
            }
            PropertyValue::Int(i) => i.to_string(),
        }
    }

    pub fn from_num(n: impl Into<f64>) -> Self {
        let v = n.into();
        if v == (v as i64) as f64 && v.abs() < 1e15 {
            PropertyValue::Int(v as i64)
        } else {
            PropertyValue::Num(v)
        }
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::Str(s.into())
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::Str(s)
    }
}

impl From<i32> for PropertyValue {
    fn from(n: i32) -> Self {
        PropertyValue::Int(n as i64)
    }
}

impl From<i64> for PropertyValue {
    fn from(n: i64) -> Self {
        PropertyValue::Int(n)
    }
}

impl From<u32> for PropertyValue {
    fn from(n: u32) -> Self {
        PropertyValue::Int(n as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(n: f64) -> Self {
        PropertyValue::from_num(n)
    }
}
