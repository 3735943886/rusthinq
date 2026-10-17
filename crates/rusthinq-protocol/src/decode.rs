//! Hex-payload decode/export used by both the cloud daemon's management surface
//! and offline tools (MCP). Pure functions — no device manager, no I/O — so both
//! sides can call them directly instead of one going over a network round trip.
//!
//! Only framing is decoded here: what a byte or a tag means is the driver's, through its
//! optional `describe(data, to_device)` hook, whose answer [`description`] bounds.

use crate::packet_codec::{Decoded, Direction, decode_packet};
use crate::tlv;
use serde_json::{Map, Value, json};

const MAX_FIELDS: usize = 256;
const MAX_NOTES: usize = 32;
const MAX_TEXT: usize = 256;

pub fn strip_hex(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_hexdigit())
        .collect::<String>()
        .to_ascii_lowercase()
}

/// Decode a hex wire frame (TLV/AABB/UART-binary/raw) into its framing: protocol,
/// direction, integrity, TLV elements with their byte spans, and the opaque body.
pub fn decode_hex_payload(hex_in: &str, direction: Option<&str>) -> Result<Value, String> {
    let hex = strip_hex(hex_in);
    if hex.is_empty() {
        return Err("empty hex".into());
    }
    let bytes = crate::hex::decode(&hex).map_err(|e| format!("hex decode: {e}"))?;
    let mut dir_out = match direction.unwrap_or("fromDevice") {
        "toDevice" | "to" | "tx" => "toDevice",
        _ => "fromDevice",
    }
    .to_string();

    let mut protocol = "Unknown";
    // (t, v, byte_start_in_packet, byte_end_in_packet)
    let mut tlv_spans: Vec<(u16, u32, usize, usize)> = Vec::new();
    let mut body: Option<String> = None;
    let mut notes = Vec::new();
    let mut crc_ok: Option<bool> = None;

    match decode_packet(&hex) {
        Decoded::Tlv(t) => {
            protocol = "Tlv";
            dir_out = match t.direction {
                Direction::FromDevice => "fromDevice".into(),
                Direction::ToDevice => "toDevice".into(),
            };
            crc_ok = Some(t.crc_ok);
            // Full UART frame: TLV body starts at byte 11 (after 2 reliability + 9 header).
            let base = 11usize;
            if let Some(tlv_bytes) = bytes.get(base..base + t.frame.len as usize) {
                tlv_spans = spans(tlv_bytes, base);
            } else {
                tlv_spans = t.tlv.iter().map(|e| (e.t, e.v, 0, 0)).collect();
            }
            notes.push(envelope_note(
                t.frame.kind,
                t.frame.byte5,
                t.frame.byte6,
                t.frame.byte7,
                t.frame.len,
            ));
        }
        Decoded::Aabb(a) => {
            protocol = "Aabb";
            // AA FF frames carry a 16-bit checksum whose rule is unknown: integrity stays unknown.
            if a.length == 0xff {
                notes.push(format!(
                    "long frame body_len={} (last byte: 16-bit checksum part)",
                    a.body.len() / 2
                ));
            } else {
                crc_ok = Some(a.checksum_ok);
                notes.push(format!(
                    "length=0x{:02x} body_len={}",
                    a.length,
                    a.body.len() / 2
                ));
            }
            body = Some(a.body);
        }
        Decoded::Unknown(u) if u.reason.starts_with("uart_binary") => {
            // A UART envelope whose body is not TLV: keep the body opaque.
            protocol = "UartBinary";
            if let &[_, _, _, _, _, _, kind, b5, b6, b7, len, ..] = bytes.as_slice() {
                notes.push(envelope_note(kind, b5, b6, b7, len));
                let end = (11 + len as usize).min(bytes.len().saturating_sub(2));
                body = bytes.get(11..end).map(crate::hex::encode);
            }
            crc_ok = if u.reason.contains("crc_ok=true") {
                Some(true)
            } else if u.reason.contains("crc_ok=false") {
                Some(false)
            } else {
                None
            };
        }
        Decoded::Unknown(u) => {
            notes.push(format!("packet_codec: {}", u.reason));
            // Raw TLV body (no UART envelope): whole buffer, then after a 2-byte prefix.
            for base in [0, 2] {
                let found = bytes
                    .get(base..)
                    .map(|b| spans(b, base))
                    .unwrap_or_default();
                if !found.is_empty() {
                    tlv_spans = found;
                    protocol = "TlvRaw";
                    break;
                }
            }
            if protocol == "Unknown" && bytes.len() >= 2 {
                body = Some(hex.clone());
                protocol = "Raw";
            }
        }
    }

    let elements: Vec<Value> = tlv_spans
        .iter()
        .map(|(t, v, b0, b1)| {
            json!({
                "t": t,
                "hex": format!("0x{t:03x}"),
                "v": v,
                // Byte offsets in the full packet; hex char offsets are 2× (no separators).
                "byteStart": b0,
                "byteEnd": b1,
                "hexStart": b0 * 2,
                "hexEnd": b1 * 2,
            })
        })
        .collect();

    Ok(json!({
        "ok": true,
        "protocol": protocol,
        "direction": dir_out,
        "crcOk": crc_ok,
        "elements": elements,
        "body": body,
        "notes": notes,
        "hex": hex,
    }))
}

fn spans(bytes: &[u8], base: usize) -> Vec<(u16, u32, usize, usize)> {
    tlv::parse_with_spans(bytes)
        .into_iter()
        .map(|s| (s.tlv.t, s.tlv.v, base + s.byte_start, base + s.byte_end))
        .collect()
}

fn envelope_note(kind: u8, b5: u8, b6: u8, b7: u8, len: u8) -> String {
    format!("kind=0x{kind:02x} b5=0x{b5:02x} b6=0x{b6:02x} b7=0x{b7:02x} len={len}")
}

fn text(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(s) => s.clone(),
        Value::Number(_) | Value::Bool(_) => value.to_string(),
        _ => return None,
    };
    Some(text.chars().take(MAX_TEXT).collect())
}

/// Bound a driver's `describe` answer to `{summary?, fields: [{name, value, start?, length?}],
/// notes: [..]}`. Anything else in it is dropped; `None` when nothing usable remains.
pub fn description(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let mut out = Map::new();
    if let Some(summary) = object.get("summary").and_then(text) {
        out.insert("summary".into(), summary.into());
    }
    let fields: Vec<Value> = object
        .get("fields")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|field| {
            let name = field.get("name").and_then(text)?;
            let mut entry = Map::new();
            entry.insert("name".into(), name.into());
            entry.insert(
                "value".into(),
                field.get("value").and_then(text).unwrap_or_default().into(),
            );
            for key in ["start", "length"] {
                if let Some(n) = field.get(key).and_then(Value::as_u64) {
                    entry.insert(key.into(), n.into());
                }
            }
            Some(Value::Object(entry))
        })
        .take(MAX_FIELDS)
        .collect();
    let notes: Vec<Value> = object
        .get("notes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(text)
        .take(MAX_NOTES)
        .map(Value::from)
        .collect();
    if out.is_empty() && fields.is_empty() && notes.is_empty() {
        return None;
    }
    out.insert("fields".into(), fields.into());
    out.insert("notes".into(), notes.into());
    Some(Value::Object(out))
}

/// Render a decoded payload (as produced by [`decode_hex_payload`], with the driver's
/// `description` when one was attached) as plain text for notes or an LLM prompt.
pub fn re_export_text(decoded: &Value, model_id: Option<&str>, direction: Option<&str>) -> String {
    let field = |key: &str| decoded.get(key).and_then(Value::as_str).unwrap_or("");
    let mut out = format!("# ThinQ {} frame\n", field("protocol"));
    if let Some(model) = model_id.filter(|m| !m.is_empty()) {
        out.push_str(&format!("modelId: {model}\n"));
    }
    let direction = direction.unwrap_or_else(|| field("direction"));
    if !direction.is_empty() {
        out.push_str(&format!("direction: {direction}\n"));
    }
    out.push_str(&format!("hex: {}\n", field("hex")));
    if let Some(ok) = decoded.get("crcOk").and_then(Value::as_bool) {
        out.push_str(&format!("integrity: {}\n", if ok { "ok" } else { "bad" }));
    }
    for note in decoded
        .get("notes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        out.push_str(&format!("note: {note}\n"));
    }
    if let Some(body) = decoded.get("body").and_then(Value::as_str) {
        out.push_str(&format!("body: {body}\n"));
    }
    let elements = decoded
        .get("elements")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !elements.is_empty() {
        out.push_str("tlv:\n");
        for element in elements {
            out.push_str(&format!(
                "  {} = {}\n",
                element["hex"].as_str().unwrap_or(""),
                element["v"]
            ));
        }
    }
    match decoded.get("description").filter(|d| d.is_object()) {
        Some(description) => {
            match description["source"].as_str() {
                Some(source) => out.push_str(&format!("driver ({source}):\n")),
                None => out.push_str("driver:\n"),
            }
            if let Some(summary) = description["summary"].as_str() {
                out.push_str(&format!("  summary: {summary}\n"));
            }
            for field in description["fields"].as_array().into_iter().flatten() {
                let span = match (field["start"].as_u64(), field["length"].as_u64()) {
                    (Some(start), Some(length)) => format!(" [bytes {start}+{length}]"),
                    (Some(start), None) => format!(" [byte {start}]"),
                    _ => String::new(),
                };
                out.push_str(&format!(
                    "  {} = {}{span}\n",
                    field["name"].as_str().unwrap_or(""),
                    field["value"].as_str().unwrap_or("")
                ));
            }
            for note in description["notes"].as_array().into_iter().flatten() {
                out.push_str(&format!("  note: {}\n", note.as_str().unwrap_or("")));
            }
        }
        None => out
            .push_str("driver: no description (no driver for this model, or it has no describe)\n"),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tlv_elements_carry_tags_values_and_spans_only() {
        let tlv_bytes = tlv::build(&[tlv::Tlv::new(0x1f7, 1), tlv::Tlv::new(0x3fe, 42)]);
        let hex = crate::hex::encode(&tlv_bytes);
        let v = decode_hex_payload(&hex, Some("fromDevice")).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["protocol"], "TlvRaw");
        let els = v["elements"].as_array().unwrap();
        assert_eq!(els.len(), 2);
        assert_eq!(els[1]["t"], 0x3fe);
        assert_eq!(els[1]["v"], 42);
        assert!(els[0].get("name").is_none());
    }

    #[test]
    fn aabb_framing_is_decoded_without_meaning() {
        let v = decode_hex_payload("aa16f0263a03ff040100000000000300000000004fbb", None).unwrap();
        assert_eq!(v["protocol"], "Aabb");
        assert_eq!(v["crcOk"], true);
        assert_eq!(v["body"], "f0263a03ff04010000000000030000000000");
    }

    #[test]
    fn long_aabb_frames_have_unknown_integrity() {
        let v = decode_hex_payload("aaff200a00120010130001007f0000e91dbb", None).unwrap();
        assert_eq!(v["protocol"], "Aabb");
        assert_eq!(v["crcOk"], Value::Null);
    }

    #[test]
    fn description_is_bounded_and_rendered() {
        let raw = json!({"summary":"status","fields":[{"name":"power","value":true,"start":4,"length":1},{"value":"nameless"},{"name":"remaining","value":95}],"notes":["n",{"x":1}],"extra":1});
        let d = description(&raw).unwrap();
        assert_eq!(
            d,
            json!({"summary":"status","fields":[{"name":"power","value":"true","start":4,"length":1},{"name":"remaining","value":"95"}],"notes":["n"]})
        );
        assert_eq!(description(&json!("text")), None);
        assert_eq!(description(&json!({"fields":[]})), None);
        let mut decoded = decode_hex_payload("aa0720d801ffbb", None).unwrap();
        decoded["description"] = d;
        let text = re_export_text(&decoded, Some("MODEL"), None);
        assert!(text.contains("modelId: MODEL"));
        assert!(text.contains("power = true [bytes 4+1]"));
        assert!(text.contains("summary: status"));
    }

    #[test]
    fn export_without_description_says_so() {
        let decoded = decode_hex_payload("aa0720d801ffbb", Some("fromDevice")).unwrap();
        let text = re_export_text(&decoded, None, None);
        assert!(text.contains("# ThinQ Aabb frame"));
        assert!(text.contains("driver: no description"));
    }

    #[test]
    fn empty_hex_is_rejected() {
        assert!(decode_hex_payload("", None).is_err());
        assert!(decode_hex_payload("zz", None).is_err());
    }
}
