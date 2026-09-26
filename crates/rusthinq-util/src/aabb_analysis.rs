//! Structured analysis of AABB (0xAA…0xBB) fixed-layout packets.
//!
//! Envelope is device-agnostic; body layouts are per product family.
//! Dryer/washer status (kind 0x30) follows the RH10V9 / laundry pattern used
//! by `rh10v9_ch` and related handlers.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct AabbField {
    pub name: &'static str,
    pub offset: usize,
    pub width: u8,
    pub raw: u32,
    pub interpretation: String,
    pub confidence: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct AabbAnalysis {
    pub packet_len: usize,
    pub length_byte: u8,
    pub checksum_ok: Option<bool>,
    pub body_hex: String,
    pub body_len: usize,
    /// First body byte when present (device family / product class).
    pub kind: Option<u8>,
    /// Second body byte: command/status subtype (F0, EB, EC, …).
    pub frame_type: Option<u8>,
    pub kind_label: String,
    pub frame_type_label: String,
    pub fields: Vec<AabbField>,
    pub re_notes: Vec<String>,
}

fn dryer_phase_name(phase: u8) -> &'static str {
    match phase {
        0x00 => "Off",
        0x01 => "Initial",
        0x02 => "Drying", // RH10 heat-pump live capture
        0x03 => "Pause",
        0x04 => "End",
        0x32 => "Drying",
        0x33 => "Cooling",
        _ => "unknown",
    }
}

fn kind_label(kind: u8) -> String {
    match kind {
        0x10 => "fridge family".into(),
        0x20 => "washer/laundry status family".into(),
        0x30 => "dryer status family (RH10 / heat-pump)".into(),
        0x43 => "hood family".into(),
        0xf0 => "host command (F0…)".into(),
        _ => format!("unclassified kind 0x{kind:02x}"),
    }
}

/// Byte at `i`, or 0 past the end -- the analyzers guard lengths, this makes it total.
fn at(buf: &[u8], i: usize) -> u8 {
    buf.get(i).copied().unwrap_or(0)
}

fn frame_type_label(ft: u8) -> String {
    match ft {
        0xeb => "single status (EB)".into(),
        0xec => "dual status prev+cur (EC)".into(),
        0xed => "command tail (ED, often with F0)".into(),
        0x31 => "ack/ignore subtype (0x31)".into(),
        _ => format!("subtype 0x{ft:02x}"),
    }
}

/// Parse one 27-byte dryer status record (as in RH10V9_CH).
fn dryer_record_fields(rec: &[u8], base: usize, label: &str) -> Vec<AabbField> {
    if rec.len() < 27 {
        return Vec::new();
    }
    let phase = at(rec, 2);
    let programmed_min = at(rec, 0) as u32 * 60 + at(rec, 1) as u32;
    let live_rem = at(rec, 4) as u32;
    let remaining_min = if live_rem > 0 {
        live_rem
    } else {
        programmed_min
    };
    let course = at(rec, 6) as u32;
    let dry_level = at(rec, 7) as u32;
    let temp = at(rec, 10) as u32;
    let flags = at(rec, 17) as u32;
    let tick = at(rec, 20) as u32;
    let phase_name = dryer_phase_name(phase);
    let prefix = if label == "status" {
        String::new()
    } else {
        format!("{label} ")
    };
    let rem_note = if live_rem > 0 {
        format!("{remaining_min} min (live at(rec, 4); programmed H:M={programmed_min})")
    } else if phase == 0 && programmed_min > 0 {
        format!("{programmed_min} min (H:M; non-zero while Off — residual/display)")
    } else {
        format!("{remaining_min} min (H:M)")
    };
    vec![
        AabbField {
            name: "remaining_min",
            offset: base + if live_rem > 0 { 4 } else { 0 },
            width: if live_rem > 0 { 1 } else { 2 },
            raw: remaining_min,
            interpretation: format!("{prefix}{rem_note}"),
            confidence: "high",
        },
        AabbField {
            name: "phase",
            offset: base + 2,
            width: 1,
            raw: phase as u32,
            interpretation: format!("{prefix}0x{phase:02x} {phase_name}"),
            confidence: "high",
        },
        AabbField {
            name: "course",
            offset: base + 6,
            width: 1,
            raw: course,
            interpretation: format!("{prefix}0x{course:02x}"),
            confidence: "medium",
        },
        AabbField {
            name: "dry_level",
            offset: base + 7,
            width: 1,
            raw: dry_level,
            interpretation: format!("{prefix}{dry_level}"),
            confidence: "medium",
        },
        AabbField {
            name: "temp_code",
            offset: base + 10,
            width: 1,
            raw: temp,
            interpretation: format!("{prefix}{temp}"),
            confidence: "medium",
        },
        AabbField {
            name: "flags",
            offset: base + 17,
            width: 1,
            raw: flags,
            interpretation: format!("{prefix}0x{flags:02x}"),
            confidence: "medium",
        },
        AabbField {
            name: "tick_6s",
            offset: base + 20,
            width: 1,
            raw: tick,
            interpretation: format!("{prefix}{tick} (~6s ticks)"),
            confidence: "high",
        },
    ]
}

/// Analyze an AABB body (bytes between AA/len and checksum/BB).
pub fn analyze_aabb_body(
    body: &[u8],
    packet_len: usize,
    length_byte: u8,
    checksum_ok: Option<bool>,
) -> AabbAnalysis {
    let kind = body.first().copied();
    let frame_type = body.get(1).copied();
    let mut fields = Vec::new();
    let mut re_notes = vec![
        "AABB: AA | len | body | checksum | BB (checksum = sum(bytes incl AA) mod 256 xor 0x55)."
            .into(),
    ];

    let (kl, ftl) = match (kind, frame_type) {
        (Some(k), Some(ft)) => (kind_label(k), frame_type_label(ft)),
        (Some(k), None) => (kind_label(k), "—".into()),
        _ => ("empty body".into(), "—".into()),
    };

    // Host command F0 ED … (monitor enable / set)
    if body.first() == Some(&0xf0) && body.len() >= 2 {
        let hx = crate::hex::encode(body);
        if body == hex_decode_static("f0ed1121010000001800") {
            fields.push(AabbField {
                name: "monitor_enable",
                offset: 0,
                width: 10,
                raw: 1,
                interpretation: "RH10/laundry poll (F0ED1121010000001800) ~15s".into(),
                confidence: "high",
            });
        } else {
            fields.push(AabbField {
                name: "command",
                offset: 0,
                width: body.len().min(16) as u8,
                raw: 0,
                interpretation: format!("host F0 command {hx}"),
                confidence: "medium",
            });
        }
        re_notes
            .push("TX F0… commands are host→device; pair with following EB/EC status RX.".into());
    }

    // Dryer family 0x30
    if body.first() == Some(&0x30) && body.len() >= 2 {
        let ft = at(body, 1);
        const REC: usize = 27;
        if ft == 0xeb && body.len() == 2 + REC {
            fields.extend(dryer_record_fields(
                body.get(2..).unwrap_or_default(),
                2,
                "status",
            ));
            re_notes.push("0x30 EB: single 27-byte dryer status record (RH10V9_CH layout).".into());
        } else if ft == 0xec && body.len() == 2 + 2 * REC {
            fields.extend(dryer_record_fields(
                body.get(2..2 + REC).unwrap_or_default(),
                2,
                "prev",
            ));
            fields.extend(dryer_record_fields(
                body.get(2 + REC..2 + 2 * REC).unwrap_or_default(),
                2 + REC,
                "cur",
            ));
            re_notes.push(
                "0x30 EC: dual status (prev + cur records); handlers often use the second.".into(),
            );
        } else if ft == 0x31 {
            re_notes.push("0x30 0x31: ignored/ack-like on RH10 handler.".into());
        } else {
            re_notes.push(format!(
                "0x30 subtype 0x{ft:02x} len={} — not the standard 27-byte EB/EC layout.",
                body.len()
            ));
        }
    }

    // Washer family 0x20 — note only (layout model-specific)
    if body.first() == Some(&0x20) {
        re_notes
            .push("kind 0x20 laundry/washer family — see washer handlers for field maps.".into());
    }

    AabbAnalysis {
        packet_len,
        length_byte,
        checksum_ok,
        body_hex: crate::hex::encode(body),
        body_len: body.len(),
        kind,
        frame_type,
        kind_label: kl,
        frame_type_label: ftl,
        fields,
        re_notes,
    }
}

fn hex_decode_static(s: &str) -> Vec<u8> {
    crate::hex::decode(s).unwrap_or_default()
}

/// Compact AABB breakdown for text export / multi-frame paste.
pub fn aabb_export_text(
    model_id: Option<&str>,
    direction: Option<&str>,
    full_hex: &str,
    analysis: &AabbAnalysis,
) -> String {
    let mut out = String::from("# ThinQ AABB frame\n");
    if let Some(m) = model_id {
        out.push_str(&format!("modelId: {m}\n"));
    }
    if let Some(d) = direction {
        out.push_str(&format!("direction: {d}\n"));
    }
    out.push_str(&format!(
        "packet_len={} length_byte={} body_len={}\n",
        analysis.packet_len, analysis.length_byte, analysis.body_len
    ));
    if let Some(c) = analysis.checksum_ok {
        out.push_str(&format!("checksum_ok: {c}\n"));
    }
    if let Some(k) = analysis.kind {
        out.push_str(&format!(
            "kind=0x{k:02x} ({}) type={} ({})\n",
            analysis.kind_label,
            analysis
                .frame_type
                .map(|t| format!("0x{t:02x}"))
                .unwrap_or_else(|| "—".into()),
            analysis.frame_type_label
        ));
    }
    out.push_str(&format!("hex: {full_hex}\n"));
    out.push_str(&format!("body: {}\n", analysis.body_hex));
    if !analysis.fields.is_empty() {
        out.push_str("fields:\n");
        for f in &analysis.fields {
            // Prefer human interpretation; include raw when it is the primary value
            if matches!(f.name, "monitor_enable" | "command") {
                out.push_str(&format!("  {}: {}\n", f.name, f.interpretation));
            } else {
                out.push_str(&format!(
                    "  {}=+{} raw={} · {}\n",
                    f.name, f.offset, f.raw, f.interpretation
                ));
            }
        }
    }
    if !analysis.re_notes.is_empty() {
        out.push_str("notes:\n");
        for n in &analysis.re_notes {
            out.push_str(&format!("  - {n}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzes_rh10_monitor_enable() {
        let body = crate::hex::decode("f0ed1121010000001800").unwrap();
        let a = analyze_aabb_body(&body, 14, 0x0e, Some(true));
        assert_eq!(a.kind, Some(0xf0));
        assert!(a.fields.iter().any(|f| f.name == "monitor_enable"));
        let t = aabb_export_text(Some("RH10V9_CH"), Some("toDevice"), "aa0e…", &a);
        assert!(t.contains("AABB"));
        assert!(t.contains("monitor_enable"));
        assert!(!t.contains("4042068257"));
    }

    #[test]
    fn analyzes_rh10_single_status_off() {
        // body from live capture #3
        let body = crate::hex::decode("30eb001900000000000000000000000000000000000000000000007500")
            .unwrap();
        assert_eq!(body.len(), 29);
        let a = analyze_aabb_body(&body, 33, 0x21, Some(true));
        assert_eq!(a.kind, Some(0x30));
        assert_eq!(a.frame_type, Some(0xeb));
        let phase = a.fields.iter().find(|f| f.name == "phase").unwrap();
        assert_eq!(phase.raw, 0);
        assert!(phase.interpretation.contains("Off"));
        let rem = a.fields.iter().find(|f| f.name == "remaining_min").unwrap();
        assert_eq!(rem.raw, 25); // 0*60+0x19
        assert!(rem.interpretation.contains("residual") || rem.interpretation.contains("25"));
        let t = aabb_export_text(Some("RH10V9_CH"), Some("fromDevice"), "aa21…", &a);
        assert!(t.contains("fields:"));
        assert!(t.contains("Off"));
    }

    #[test]
    fn analyzes_rh10_dual_status() {
        let body = crate::hex::decode(
            "30ec001900000000000000000000000000000000000000000000007500001900000000000000000000000000000000000000000000007500",
        )
        .unwrap();
        assert_eq!(body.len(), 56);
        let a = analyze_aabb_body(&body, 60, 0x3c, Some(true));
        assert_eq!(a.frame_type, Some(0xec));
        assert!(a.fields.iter().any(|f| f.interpretation.contains("prev")));
        assert!(a.fields.iter().any(|f| f.interpretation.contains("cur")));
    }
}
