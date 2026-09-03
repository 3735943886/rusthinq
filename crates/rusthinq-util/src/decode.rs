//! Hex-payload decode/export used by both the cloud daemon's management surface
//! and offline tools (MCP). Pure functions — no device manager, no I/O — so both
//! sides can call them directly instead of one going over a network round trip.

use crate::aabb_analysis::{aabb_export_text, analyze_aabb_body};
use crate::packet_codec::{Decoded, Direction, decode_packet};
use crate::tlv;
use crate::tlv_catalog::{KNOWN_TAGS, classify_tlvs, llm_export_text};
use crate::uart_binary::{analyze_uart_binary, uart_binary_export_text};
use serde_json::{Value, json};

pub fn strip_hex(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_hexdigit())
        .collect::<String>()
        .to_ascii_lowercase()
}

/// JSON body for the `/api/tlv/catalog` (or equivalent) listing of known TLV tags.
pub fn tlv_catalog_json() -> Value {
    let tags: Vec<Value> = KNOWN_TAGS
        .iter()
        .map(|t| {
            json!({
                "id": t.id,
                "hex": format!("0x{:03x}", t.id),
                "name": t.name,
                "family": t.family,
            })
        })
        .collect();
    json!({ "tags": tags, "count": tags.len() })
}

/// Decode a hex wire frame (TLV/AABB/UART-binary/raw) into the rich JSON shape
/// consumed by the RE tooling: protocol, direction, elements, unknowns, notes, etc.
pub fn decode_hex_payload(hex_in: &str, direction: Option<&str>) -> Result<Value, String> {
    let hex = strip_hex(hex_in);
    if hex.is_empty() {
        return Err("empty hex".into());
    }
    let bytes = crate::hex::decode(&hex).map_err(|e| format!("hex decode: {e}"))?;
    let requested_dir = match direction.unwrap_or("fromDevice") {
        "toDevice" | "to" | "tx" => "toDevice",
        _ => "fromDevice",
    };

    let mut protocol = String::from("Unknown");
    // (t, v, byte_start_in_packet, byte_end_in_packet)
    let mut tlv_spans: Vec<(u16, u32, usize, usize)> = Vec::new();
    let mut aabb_body: Option<String> = None;
    let mut notes = Vec::new();
    let mut dir_out = requested_dir.to_string();
    let mut crc_ok: Option<bool> = None;
    let mut binary_analysis: Option<Value> = None;

    match decode_packet(&hex) {
        Decoded::Tlv(t) => {
            protocol = "Tlv".into();
            dir_out = match t.direction {
                Direction::FromDevice => "fromDevice".into(),
                Direction::ToDevice => "toDevice".into(),
            };
            crc_ok = Some(t.crc_ok);
            // Full UART frame: TLV body starts at byte 11 (after 2 reliability + 9 header).
            let tlv_base = 11usize;
            let len = t.frame.len as usize;
            if bytes.len() >= tlv_base + len {
                let spans = tlv::parse_with_spans(&bytes[tlv_base..tlv_base + len]);
                tlv_spans = spans
                    .into_iter()
                    .map(|s| {
                        (
                            s.tlv.t,
                            s.tlv.v,
                            tlv_base + s.byte_start,
                            tlv_base + s.byte_end,
                        )
                    })
                    .collect();
            } else {
                tlv_spans = t.tlv.iter().map(|e| (e.t, e.v, 0usize, 0usize)).collect();
            }
            notes.push(format!(
                "kind=0x{:02x} b5=0x{:02x} b6=0x{:02x} b7=0x{:02x} len={}",
                t.frame.kind, t.frame.byte5, t.frame.byte6, t.frame.byte7, t.frame.len
            ));
            if t.frame.len == 0 {
                notes.push("empty body (ACK/keepalive-style)".into());
            }
        }
        Decoded::Aabb(a) => {
            protocol = "Aabb".into();
            aabb_body = Some(a.body.clone());
            crc_ok = Some(a.checksum_ok);
            let body_bytes = crate::hex::decode(&a.body).unwrap_or_default();
            let analysis =
                analyze_aabb_body(&body_bytes, bytes.len(), a.length, Some(a.checksum_ok));
            if let Some(k) = analysis.kind {
                notes.push(format!(
                    "kind=0x{k:02x} type={} body_len={}",
                    analysis
                        .frame_type
                        .map(|t| format!("0x{t:02x}"))
                        .unwrap_or_else(|| "—".into()),
                    analysis.body_len
                ));
            } else {
                notes.push(format!("AABB body_len={}", analysis.body_len));
            }
            if let Some(phase) = analysis.fields.iter().find(|f| f.name == "phase") {
                notes.push(format!("phase={}", phase.interpretation));
            }
            binary_analysis = Some(serde_json::to_value(&analysis).unwrap_or(json!({})));
        }
        Decoded::Unknown(u) => {
            notes.push(format!("packet_codec: {}", u.reason));
            // Non-TLV UART envelope: analyze body with heuristics (do NOT invent TLV tags).
            if u.reason.starts_with("uart_binary") {
                protocol = "UartBinary".into();
                if bytes.len() >= 13 {
                    let kind = bytes[6];
                    let b5 = bytes[7];
                    let b6 = bytes[8];
                    let b7 = bytes[9];
                    let len = bytes[10] as usize;
                    let start = 11usize;
                    let end = (start + len).min(bytes.len().saturating_sub(2));
                    if end > start {
                        let body = &bytes[start..end];
                        aabb_body = Some(crate::hex::encode(body));
                        let crc = if u.reason.contains("crc_ok=true") {
                            Some(true)
                        } else if u.reason.contains("crc_ok=false") {
                            Some(false)
                        } else {
                            None
                        };
                        let analysis = analyze_uart_binary(kind, b5, b6, b7, body, crc);
                        notes.push(format!(
                            "binary body {} bytes — heuristic hits={}",
                            body.len(),
                            analysis.heuristics.len()
                        ));
                        binary_analysis =
                            Some(serde_json::to_value(&analysis).unwrap_or(json!({})));
                    }
                }
            } else {
                // Raw TLV body (no UART envelope) — try whole buffer, then after 2-byte prefix
                let candidates: &[(usize, &[u8])] = if bytes.len() > 2 {
                    &[(0, &bytes[..]), (2, &bytes[2..])]
                } else {
                    &[(0, &bytes[..])]
                };
                let mut parsed = false;
                for &(base, slice) in candidates {
                    let spans = tlv::parse_with_spans(slice);
                    if !spans.is_empty() {
                        tlv_spans = spans
                            .into_iter()
                            .map(|s| (s.tlv.t, s.tlv.v, base + s.byte_start, base + s.byte_end))
                            .collect();
                        protocol = "TlvRaw".into();
                        parsed = true;
                        break;
                    }
                }
                if !parsed && bytes.len() >= 2 {
                    aabb_body = Some(hex.clone());
                    protocol = "Raw".into();
                }
            }
        }
    }

    let tlv_items: Vec<(u16, u32)> = tlv_spans.iter().map(|(t, v, _, _)| (*t, *v)).collect();
    let classified = classify_tlvs(&tlv_items);
    let unknowns: Vec<Value> = classified
        .iter()
        .filter(|c| !c.known)
        .map(|c| json!({"t": c.t, "hex": format!("0x{:03x}", c.t), "v": c.v}))
        .collect();
    let elements: Vec<Value> = classified
        .iter()
        .zip(tlv_spans.iter())
        .map(|(c, (_, _, b0, b1))| {
            json!({
                "t": c.t,
                "hex": format!("0x{:03x}", c.t),
                "v": c.v,
                "known": c.known,
                "name": c.name,
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
        "unknowns": unknowns,
        "unknownCount": unknowns.len(),
        "aabbBody": aabb_body,
        "binaryAnalysis": binary_analysis,
        "notes": notes,
        "hex": hex,
    }))
}

/// Render a decoded payload (as produced by [`decode_hex_payload`]) as the
/// human/LLM-readable RE export text, dispatching on its `protocol` field.
pub fn re_export_text(decoded: &Value, model_id: Option<&str>, direction: Option<&str>) -> String {
    let hex = decoded.get("hex").and_then(|h| h.as_str()).unwrap_or("");
    let protocol = decoded
        .get("protocol")
        .and_then(|p| p.as_str())
        .unwrap_or("");

    // AABB fixed-layout frames (dryer/washer/fridge) — not TLV.
    if protocol == "Aabb" {
        return if let Some(ba) = decoded.get("binaryAnalysis").filter(|v| !v.is_null()) {
            // Re-analyze from body for stable text (same pattern as UartBinary)
            if let Some(body_hex) = ba.get("body_hex").and_then(|v| v.as_str()) {
                let raw = crate::hex::decode(body_hex).unwrap_or_default();
                let len_byte = ba.get("length_byte").and_then(|v| v.as_u64()).unwrap_or(0) as u8;
                let packet_len = ba
                    .get("packet_len")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(hex.len() as u64 / 2) as usize;
                let csum = ba.get("checksum_ok").and_then(|v| v.as_bool());
                let analysis = analyze_aabb_body(&raw, packet_len, len_byte, csum);
                aabb_export_text(model_id, direction, hex, &analysis)
            } else if let Some(body_h) = decoded.get("aabbBody").and_then(|v| v.as_str()) {
                let raw = crate::hex::decode(body_h).unwrap_or_default();
                let analysis = analyze_aabb_body(&raw, hex.len() / 2, 0, None);
                aabb_export_text(model_id, direction, hex, &analysis)
            } else {
                format!("# ThinQ AABB frame\nhex: {hex}\n")
            }
        } else if let Some(body_h) = decoded.get("aabbBody").and_then(|v| v.as_str()) {
            let raw = crate::hex::decode(body_h).unwrap_or_default();
            let analysis = analyze_aabb_body(&raw, hex.len() / 2, 0, None);
            aabb_export_text(model_id, direction, hex, &analysis)
        } else {
            format!("# ThinQ AABB frame\nhex: {hex}\n")
        };
    }

    // Binary UART: prefer structured heuristic export over empty TLV export.
    // Note: JSON null for binaryAnalysis is still Some(Value::Null) — filter it out.
    if protocol == "UartBinary" {
        return if let Some(ba) = decoded.get("binaryAnalysis").filter(|v| !v.is_null()) {
            if let (Some(kind), Some(b5), Some(b6), Some(b7), Some(body_hex)) = (
                ba.pointer("/envelope/kind").and_then(|v| v.as_u64()),
                ba.pointer("/envelope/byte5").and_then(|v| v.as_u64()),
                ba.pointer("/envelope/byte6").and_then(|v| v.as_u64()),
                ba.pointer("/envelope/byte7").and_then(|v| v.as_u64()),
                ba.get("body_hex").and_then(|v| v.as_str()),
            ) {
                let raw_body = crate::hex::decode(body_hex).unwrap_or_default();
                let crc = ba.pointer("/envelope/crc_ok").and_then(|v| v.as_bool());
                let analysis =
                    analyze_uart_binary(kind as u8, b5 as u8, b6 as u8, b7 as u8, &raw_body, crc);
                uart_binary_export_text(model_id, direction.or(Some("fromDevice")), hex, &analysis)
            } else {
                // Envelope present but incomplete — compact fallback, never dump JSON "null"
                let notes = decoded
                    .get("notes")
                    .and_then(|n| n.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join("; ")
                    })
                    .unwrap_or_default();
                let mut out = String::from("# ThinQ UART binary\n");
                if let Some(m) = model_id {
                    out.push_str(&format!("modelId: {m}\n"));
                }
                if !notes.is_empty() {
                    out.push_str(&format!("{notes}\n"));
                }
                out.push_str(&format!("hex: {hex}\n"));
                if let Some(body_h) = decoded.get("aabbBody").and_then(|v| v.as_str()) {
                    out.push_str(&format!("body: {body_h}\n"));
                }
                out
            }
        } else {
            // Empty body / no analysis — envelope only from notes
            let notes = decoded
                .get("notes")
                .and_then(|n| n.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .unwrap_or_default();
            let mut out = String::from("# ThinQ UART binary\n");
            if let Some(m) = model_id {
                out.push_str(&format!("modelId: {m}\n"));
            }
            if !notes.is_empty() {
                out.push_str(&format!("{notes}\n"));
            }
            out.push_str(&format!("hex: {hex}\n"));
            out.push_str("body: (empty)\n");
            out
        };
    }

    let items: Vec<(u16, u32)> = decoded
        .get("elements")
        .and_then(|e| e.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|el| {
                    let t = el.get("t")?.as_u64()? as u16;
                    let v = el.get("v")?.as_u64()? as u32;
                    Some((t, v))
                })
                .collect()
        })
        .unwrap_or_default();
    let classified = classify_tlvs(&items);
    llm_export_text(model_id, direction.or(Some("fromDevice")), hex, &classified)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_known_tlv_query_style() {
        // Tags are 10-bit; use 0x3fe as a deliberately uncatalogued tag.
        let tlv_bytes = tlv::build(&[tlv::Tlv::new(0x1f7, 1), tlv::Tlv::new(0x3fe, 42)]);
        let hex = crate::hex::encode(&tlv_bytes);
        let v = decode_hex_payload(&hex, Some("fromDevice")).unwrap();
        assert_eq!(v["ok"], true);
        let els = v["elements"].as_array().unwrap();
        assert!(els.iter().any(|e| e["t"] == 0x1f7 && e["known"] == true));
        assert!(els.iter().any(|e| e["t"] == 0x3fe && e["known"] == false));
        assert!(v["unknownCount"].as_u64().unwrap() >= 1);
    }

    #[test]
    fn export_includes_llm_section() {
        let tlv_bytes = tlv::build(&[tlv::Tlv::new(0x1f7, 1), tlv::Tlv::new(0x3fe, 9)]);
        let hex = crate::hex::encode(&tlv_bytes);
        let decoded = decode_hex_payload(&hex, Some("fromDevice")).unwrap();
        let text = re_export_text(&decoded, Some("HUM_056905_WW"), Some("fromDevice"));
        assert!(text.contains("UNKNOWN") || text.contains("Unknown"));
        assert!(text.contains("HUM_056905_WW"));
        assert!(text.contains("0x3fe") || text.contains("3fe"));
    }

    #[test]
    fn catalog_nonempty() {
        assert!(!KNOWN_TAGS.is_empty());
    }

    #[test]
    fn catalog_json_has_tags_and_count() {
        let v = tlv_catalog_json();
        let count = v["count"].as_u64().unwrap();
        assert_eq!(count, KNOWN_TAGS.len() as u64);
        assert_eq!(v["tags"].as_array().unwrap().len(), count as usize);
    }

    #[test]
    fn empty_hex_is_rejected() {
        assert!(decode_hex_payload("", None).is_err());
        assert!(decode_hex_payload("zz", None).is_err());
    }
}
