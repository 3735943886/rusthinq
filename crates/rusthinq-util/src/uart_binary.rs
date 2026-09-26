//! Heuristic analysis of non-TLV UART payloads (UartBinary).
//!
//! Climate state is usually kind 0x87/0xa7/0x65 TLV. Other kinds share the same
//! UART envelope but carry fixed binary layouts (private commands, SUPERSET,
//! sensor/filter blobs). We cannot invent TLV tags for them, but we *can*:
//! - document the envelope (kind, b5–b7, len, CRC)
//! - scan the body for byte patterns that often encode temps / humidity / counters
//! - emit structured RE notes for LLM / human follow-up
//!
//! Goal: never discard binary frames; always attempt a best-effort reading.

use serde::Serialize;

/// Known / hypothesized UART kind families (from wiki + captures).
pub fn kind_hint(kind: u8) -> &'static str {
    match kind {
        0x65 => "toDevice climate TLV dialect",
        0x87 => "fromDevice climate TLV (classic)",
        0xa7 => "fromDevice climate TLV (CST/DHUM dialect)",
        0xa8 => "fromDevice non-TLV (seen on DHUM SUPERSET/private body)",
        0xfd => "often private-command path (filter etc.) when b5=0xfd",
        _ => "unclassified UART kind — capture + correlate with app/cloud",
    }
}

pub fn b5_hint(b5: u8) -> &'static str {
    match b5 {
        0x01 | 0x02 => "standard values/query band (with climate kinds)",
        0x03..=0x67 => "SUPERSET/private band (non-TLV body; DHUM 0xa8 uses 0x66/0x67)",
        0xf0..=0xf9 => "wiki extended-TLV / special path band",
        0xfd => "private command / filter-style path (with matching b6)",
        _ => "see TLVProtocol wiki byte5 notes",
    }
}

pub fn b6_hint(b6: u8) -> &'static str {
    match b6 {
        0x01 => "often query / ACK-related on climate path",
        0x02 => "often command path on climate path",
        0x04 => "often values push (fromDevice climate)",
        0x0d => "DHUM 0xa8 periodic sensor stream (paired with ambient TLV)",
        0x10 => "ACK-like on climate; on 0xa8 = alternate/snapshot binary subtype",
        _ => "model-specific",
    }
}

/// Byte at `i`, or 0 past the end -- the analyzers guard lengths, this makes it total.
fn at(body: &[u8], i: usize) -> u8 {
    body.get(i).copied().unwrap_or(0)
}

/// DHUM kind=0xa8 fixed 73-byte body — fields proven vs concurrent climate TLV.
///
/// Long capture (2026-08-10, ~91 min, 71 binary frames): at(body, +44) tracks 0x1fd
/// (half-°C ambient), at(body, +45) tracks 0x336 (RH % on DHUM), at(body, +4) increments
/// every push (sequence).
pub fn dhum_a8_layout_fields(body: &[u8]) -> Vec<HeuristicHit> {
    if body.len() < 46 {
        return Vec::new();
    }
    let mut out = Vec::new();
    if body.len() > 4 {
        out.push(HeuristicHit {
            offset: 4,
            width: 1,
            endian: "u8",
            raw: at(body, 4) as u32,
            interpretation: "stream sequence (increments each 0xa8 push)".into(),
            confidence: "high",
        });
    }
    if body.len() > 3 {
        out.push(HeuristicHit {
            offset: 3,
            width: 1,
            endian: "u8",
            raw: at(body, 3) as u32,
            interpretation: format!(
                "body subtype 0x{:02x} (0x0d≈periodic stream, 0x10≈snapshot; often mirrors envelope b6)",
                at(body, 3)
            ),
            confidence: "medium",
        });
    }
    let amb = at(body, 44);
    out.push(HeuristicHit {
        offset: 44,
        width: 1,
        endian: "u8",
        raw: amb as u32,
        interpretation: format!(
            "ambient half-°C → {:.1} °C (tracks TLV 0x1fd)",
            amb as f64 / 2.0
        ),
        confidence: "high",
    });
    let rh = at(body, 45);
    out.push(HeuristicHit {
        offset: 45,
        width: 1,
        endian: "u8",
        raw: rh as u32,
        interpretation: format!("relative humidity {rh}% (tracks DHUM TLV 0x336; not ×10)"),
        confidence: "high",
    });
    if body.len() > 33 {
        out.push(HeuristicHit {
            offset: 29,
            width: 1,
            endian: "u8",
            raw: at(body, 29) as u32,
            interpretation: "slow counter A (steps with +33; minutes-scale)".into(),
            confidence: "medium",
        });
        out.push(HeuristicHit {
            offset: 33,
            width: 1,
            endian: "u8",
            raw: at(body, 33) as u32,
            interpretation: "slow counter B (paired with +29)".into(),
            confidence: "medium",
        });
    }
    out
}

#[derive(Debug, Clone, Serialize)]
pub struct UartEnvelope {
    pub kind: u8,
    pub byte5: u8,
    pub byte6: u8,
    pub byte7: u8,
    pub body_len: usize,
    pub crc_ok: Option<bool>,
    pub kind_hint: &'static str,
    pub b5_hint: &'static str,
    pub b6_hint: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct HeuristicHit {
    /// Byte offset within the **body** (not full packet).
    pub offset: usize,
    pub width: u8,
    pub endian: &'static str,
    pub raw: u32,
    pub interpretation: String,
    pub confidence: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct UartBinaryAnalysis {
    pub envelope: UartEnvelope,
    pub body_hex: String,
    pub body_len: usize,
    pub zero_bytes: usize,
    pub nonzero_bytes: usize,
    pub unique_bytes: usize,
    pub heuristics: Vec<HeuristicHit>,
    pub re_notes: Vec<String>,
}

/// Analyze a UART binary body with envelope metadata from the outer frame.
pub fn analyze_uart_binary(
    kind: u8,
    byte5: u8,
    byte6: u8,
    byte7: u8,
    body: &[u8],
    crc_ok: Option<bool>,
) -> UartBinaryAnalysis {
    let mut hist = [0u32; 256];
    let mut zero = 0usize;
    for &b in body {
        if let Some(c) = hist.get_mut(b as usize) {
            *c += 1;
        }
        if b == 0 {
            zero += 1;
        }
    }
    let unique = hist.iter().filter(|&&c| c > 0).count();

    let mut heuristics = Vec::new();
    let mut known_offsets = std::collections::HashSet::new();

    // Proven layouts first (high confidence)
    if kind == 0xa8 && body.len() == 73 {
        for h in dhum_a8_layout_fields(body) {
            known_offsets.insert(h.offset);
            if h.width > 1 {
                for o in 0..h.width as usize {
                    known_offsets.insert(h.offset + o);
                }
            }
            heuristics.push(h);
        }
    }

    // Generic low-confidence scan — skip offsets already explained by layout
    for (i, &b) in body.iter().enumerate() {
        if known_offsets.contains(&i) {
            continue;
        }
        if (32..=80).contains(&b) {
            let c = b as f64 / 2.0;
            heuristics.push(HeuristicHit {
                offset: i,
                width: 1,
                endian: "u8",
                raw: b as u32,
                interpretation: format!(
                    "plausible half-°C temperature → {c:.1} °C (like TLV 0x1fd)"
                ),
                confidence: "low",
            });
        }
        if (20..=100).contains(&b) && b % 5 == 0 {
            heuristics.push(HeuristicHit {
                offset: i,
                width: 1,
                endian: "u8",
                raw: b as u32,
                interpretation: format!("plausible humidity % (RH={b})"),
                confidence: "low",
            });
        }
    }
    if body.len() >= 2 {
        for i in 0..body.len() - 1 {
            if known_offsets.contains(&i) {
                continue;
            }
            let le = u16::from_le_bytes([at(body, i), at(body, i + 1)]) as u32;
            let be = u16::from_be_bytes([at(body, i), at(body, i + 1)]) as u32;
            for (raw, endian) in [(le, "u16le"), (be, "u16be")] {
                if (200..=1000).contains(&raw) && raw % 10 == 0 {
                    heuristics.push(HeuristicHit {
                        offset: i,
                        width: 2,
                        endian,
                        raw,
                        interpretation: format!(
                            "plausible RH×10 → {:.1}% (CST-style 0x336)",
                            raw as f64 / 10.0
                        ),
                        confidence: "low",
                    });
                }
                if (500..=5000).contains(&raw) {
                    heuristics.push(HeuristicHit {
                        offset: i,
                        width: 2,
                        endian,
                        raw,
                        interpretation: "counter-scale value (filter hours / runtime / energy?)"
                            .into(),
                        confidence: "low",
                    });
                }
            }
        }
    }
    if heuristics.len() > 40 {
        heuristics.truncate(40);
        heuristics.push(HeuristicHit {
            offset: 0,
            width: 0,
            endian: "—",
            raw: 0,
            interpretation: "(truncated — many low-confidence hits; prefer paired captures)".into(),
            confidence: "info",
        });
    }

    let mut re_notes =
        vec!["This is NOT climate TLV. Do not invent 10-bit tags from the body.".into()];
    if kind == 0xa8 && body.len() == 73 {
        re_notes.push(
            "DHUM 0xa8/73B layout: +4 seq, +44 ambient half-°C (=0x1fd), +45 RH% (=0x336).".into(),
        );
        re_notes.push(
            "b6=0x0d periodic stream (pairs with sparse TLV); b6=0x10 alternate snapshot.".into(),
        );
    } else if kind == 0xa8 {
        re_notes.push(
            "kind 0xa8 on DHUM: private/SUPERSET blob — diff same-length bodies to map fields."
                .into(),
        );
    }
    if (0x03..=0x67).contains(&byte5) {
        re_notes.push("b5 in SUPERSET/private band: fixed binary layout, not values TLV.".into());
    }

    UartBinaryAnalysis {
        envelope: UartEnvelope {
            kind,
            byte5,
            byte6,
            byte7,
            body_len: body.len(),
            crc_ok,
            kind_hint: kind_hint(kind),
            b5_hint: b5_hint(byte5),
            b6_hint: b6_hint(byte6),
        },
        body_hex: crate::hex::encode(body),
        body_len: body.len(),
        zero_bytes: zero,
        nonzero_bytes: body.len().saturating_sub(zero),
        unique_bytes: unique,
        heuristics,
        re_notes,
    }
}

/// Compact binary-frame breakdown (envelope + body + top heuristics).
pub fn uart_binary_export_text(
    model_id: Option<&str>,
    direction: Option<&str>,
    full_hex: &str,
    analysis: &UartBinaryAnalysis,
) -> String {
    let mut out = String::new();
    out.push_str("# ThinQ UART binary\n");
    if let Some(m) = model_id {
        out.push_str(&format!("modelId: {m}\n"));
    }
    if let Some(d) = direction {
        out.push_str(&format!("direction: {d}\n"));
    }
    let e = &analysis.envelope;
    out.push_str(&format!(
        "kind=0x{:02x} b5=0x{:02x} b6=0x{:02x} b7=0x{:02x} body_len={}\n",
        e.kind, e.byte5, e.byte6, e.byte7, e.body_len
    ));
    out.push_str(&format!("kind: {}\n", e.kind_hint));
    if let Some(c) = e.crc_ok {
        out.push_str(&format!("crc_ok: {c}\n"));
    }
    out.push_str(&format!("hex: {full_hex}\n"));
    out.push_str(&format!("body: {}\n", analysis.body_hex));
    out.push_str(&format!(
        "stats: zero={} nonzero={} unique={}\n",
        analysis.zero_bytes, analysis.nonzero_bytes, analysis.unique_bytes
    ));
    let high: Vec<_> = analysis
        .heuristics
        .iter()
        .filter(|h| h.confidence == "high" && h.width > 0)
        .collect();
    if !high.is_empty() {
        out.push_str("fields:\n");
        for h in &high {
            out.push_str(&format!(
                "  +{} = {} — {}\n",
                h.offset, h.raw, h.interpretation
            ));
        }
    }
    out.push_str("candidates:\n");
    let mut n = 0;
    for h in &analysis.heuristics {
        if h.width == 0 || h.confidence == "high" {
            continue; // high already under fields:
        }
        if n >= 10 {
            out.push_str("  …\n");
            break;
        }
        out.push_str(&format!(
            "  +{} {} raw={} ({}) — {}\n",
            h.offset, h.endian, h.raw, h.confidence, h.interpretation
        ));
        n += 1;
    }
    if n == 0 && high.is_empty() {
        out.push_str("  (none)\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzes_dhum_a8_body() {
        let body = crate::hex::decode(
            "0a010d10cf0111320200000000000001000000030100000000000000331e0007b81e0000000002260226024e365000fa00002100000000000000000222011e011c1e011e1e2f90bc00",
        )
        .unwrap();
        let a = analyze_uart_binary(0xa8, 0x66, 0x10, 0x01, &body, Some(true));
        assert_eq!(a.envelope.kind, 0xa8);
        assert!(!a.body_hex.is_empty());
        assert!(
            a.heuristics
                .iter()
                .any(|h| h.interpretation.contains("half-°C") || h.interpretation.contains("RH"))
        );
        let text = uart_binary_export_text(Some("DHUM_056905_WW"), Some("fromDevice"), "00", &a);
        assert!(text.contains("UART binary"));
        assert!(text.contains("kind=0xa8") || text.contains("0xa8"));
        assert!(text.contains("body:"));
        assert!(!text.to_lowercase().contains("please help"));
    }

    /// Live DHUM stream body (2026-08-10): +44=64 → 32.0°C, +45=60% RH, seq=0x35.
    #[test]
    fn dhum_a8_layout_matches_tlv_correlation() {
        let body = crate::hex::decode(
            "0a010d0d3501111e020000000000000100000003010000020000000036300007bb300d0000000000000a0000403c00fa0400000000000000000000058e00000100000000006b6ce000",
        )
        .unwrap();
        assert_eq!(body.len(), 73);
        assert_eq!(body[4], 0x35);
        assert_eq!(body[44], 64); // half-°C → 32.0
        assert_eq!(body[45], 60); // RH %
        let fields = dhum_a8_layout_fields(&body);
        assert!(
            fields
                .iter()
                .any(|h| h.offset == 44 && h.confidence == "high")
        );
        assert!(
            fields
                .iter()
                .any(|h| h.offset == 45 && h.interpretation.contains("60%"))
        );
        let a = analyze_uart_binary(0xa8, 0x67, 0x0d, 0x01, &body, Some(true));
        let text = uart_binary_export_text(Some("DHUM_056905_WW"), Some("fromDevice"), "00", &a);
        assert!(text.contains("fields:"));
        assert!(text.contains("tracks TLV 0x1fd"));
        assert!(text.contains("32.0"));
    }
}
