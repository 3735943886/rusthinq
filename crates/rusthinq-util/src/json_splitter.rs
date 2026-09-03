//! Incremental JSON object/array splitter (brace depth tracking with string awareness).

use serde_json::Value;

#[derive(Debug, PartialEq, Eq)]
pub enum JsonSplitError {
    TooManyClosing,
    Parse(String),
}

impl std::fmt::Display for JsonSplitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyClosing => f.write_str("Invalid JSON: too many closing tokens"),
            Self::Parse(s) => write!(f, "JSON parse error: {s}"),
        }
    }
}

impl std::error::Error for JsonSplitError {}

/// Incremental JSON stream splitter — feeds bytes, emits complete top-level values.
pub struct Splitter {
    state: u8,
    depth: i32,
    buf: Vec<u8>,
}

impl Default for Splitter {
    fn default() -> Self {
        Self::new()
    }
}

impl Splitter {
    pub fn new() -> Self {
        Self {
            state: 0,
            depth: 0,
            buf: Vec::new(),
        }
    }

    /// Feed one byte. On complete top-level JSON value, returns Some(Value).
    pub fn feed(&mut self, byte: u8) -> Result<Option<Value>, JsonSplitError> {
        self.buf.push(byte);
        let mut result = None;

        if self.state == 0 {
            if byte == 0x5b || byte == 0x7b {
                // [ {
                self.depth += 1;
            }
            if byte == 0x5d || byte == 0x7d {
                // ] }
                self.depth -= 1;
                if self.depth < 0 {
                    return Err(JsonSplitError::TooManyClosing);
                }
                if self.depth == 0 {
                    let s = String::from_utf8_lossy(&self.buf);
                    let val: Value = serde_json::from_str(&s)
                        .map_err(|e| JsonSplitError::Parse(e.to_string()))?;
                    self.buf.clear();
                    result = Some(val);
                }
            }
            if byte == 0x22 {
                // "
                self.state = 1;
            }
        } else if self.state == 1 {
            if byte == 0x22 {
                self.state = 0;
            }
            if byte == 0x5c {
                // \
                self.state = 2;
            }
        } else if self.state == 2 {
            self.state = 1;
        }
        Ok(result)
    }

    /// Feed a full stream string; return all complete values.
    pub fn feed_str(stream: &str) -> Result<Vec<Value>, JsonSplitError> {
        let mut split = Self::new();
        let mut out = Vec::new();
        for byte in stream.bytes() {
            if let Some(v) = split.feed(byte)? {
                out.push(v);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn splits_two_adjacent_objects() {
        assert_eq!(
            Splitter::feed_str(r#"{"a":1}{"b":2}"#).unwrap(),
            vec![json!({"a": 1}), json!({"b": 2})]
        );
    }

    #[test]
    fn splits_nested_objects() {
        assert_eq!(
            Splitter::feed_str(r#"{"a":{"b":[1,2,{"c":3}]}}{"d":4}"#).unwrap(),
            vec![json!({"a": {"b": [1, 2, {"c": 3}]}}), json!({"d": 4})]
        );
    }

    #[test]
    fn quoted_braces_do_not_affect_depth() {
        assert_eq!(
            Splitter::feed_str(r#"{"s":"}{"}{"x":1}"#).unwrap(),
            vec![json!({"s": "}{"}), json!({"x": 1})]
        );
    }

    #[test]
    fn escaped_quote_inside_string() {
        assert_eq!(
            Splitter::feed_str(r#"{"s":"a\"b"}{"x":1}"#).unwrap(),
            vec![json!({"s": "a\"b"}), json!({"x": 1})]
        );
    }

    #[test]
    fn arrays_at_top_level() {
        assert_eq!(
            Splitter::feed_str("[1,2,3][4]").unwrap(),
            vec![json!([1, 2, 3]), json!([4])]
        );
    }

    #[test]
    fn throws_on_too_many_closing() {
        let mut split = Splitter::new();
        let mut err = None;
        for byte in b"{}}" {
            match split.feed(*byte) {
                Ok(_) => {}
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert_eq!(err, Some(JsonSplitError::TooManyClosing));
    }
}
