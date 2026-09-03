//! Known ThinQ TLV tag catalog for RE tooling (unknown highlighting).

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct KnownTag {
    pub id: u16,
    pub name: &'static str,
    pub family: &'static str,
}

/// Tags observed across RAC/WIN/POT/DHUM/HUM and wiki annotations.
pub static KNOWN_TAGS: &[KnownTag] = &[
    KnownTag {
        id: 0x1f5,
        name: "query_type",
        family: "clip",
    },
    KnownTag {
        id: 0x1f7,
        name: "power",
        family: "climate",
    },
    KnownTag {
        id: 0x1f9,
        name: "op_mode",
        family: "climate",
    },
    KnownTag {
        id: 0x1fa,
        name: "fan_strength",
        family: "climate",
    },
    KnownTag {
        id: 0x1fc,
        name: "temp_related_1fc",
        family: "climate",
    },
    KnownTag {
        id: 0x1fd,
        name: "current_temp_half_c",
        family: "climate",
    },
    KnownTag {
        id: 0x1fe,
        name: "setpoint_temp_half_c",
        family: "climate",
    },
    KnownTag {
        id: 0x253,
        name: "target_humidity",
        family: "humidity",
    },
    // RAC/CST: often RH×10 (900 → 90%). DHUM: RH percent (60 → 60%).
    // Panel hint: wire ≥200 → treat as ×10; else percent.
    KnownTag {
        id: 0x336,
        name: "current_humidity",
        family: "humidity",
    },
    KnownTag {
        id: 0x2da,
        name: "eeprom_checksum",
        family: "caps",
    },
    KnownTag {
        id: 0x2cc,
        name: "feature_bits_2cc",
        family: "caps",
    },
    KnownTag {
        id: 0x2cd,
        name: "feature_bits_2cd",
        family: "caps",
    },
    KnownTag {
        id: 0x2d3,
        name: "feature_bits_2d3",
        family: "caps",
    },
    KnownTag {
        id: 0x2d5,
        name: "cap_tag_2d5",
        family: "caps",
    },
    KnownTag {
        id: 0x2d6,
        name: "cap_tag_2d6",
        family: "caps",
    },
    KnownTag {
        id: 0x205,
        name: "wind_or_vane_205",
        family: "climate",
    },
    KnownTag {
        id: 0x206,
        name: "wind_or_vane_206",
        family: "climate",
    },
    KnownTag {
        id: 0x20d,
        name: "energy_save",
        family: "climate",
    },
    KnownTag {
        id: 0x20e,
        name: "autodry",
        family: "climate",
    },
    KnownTag {
        id: 0x20f,
        name: "airclean",
        family: "climate",
    },
    KnownTag {
        id: 0x21a,
        name: "sleep_timer",
        family: "timer",
    },
    KnownTag {
        id: 0x23f,
        name: "aq_or_comfort_flag",
        family: "aq",
    },
    KnownTag {
        id: 0x271,
        name: "power_or_limit_flag",
        family: "climate",
    },
    // RAC vertical swing (not "filter"); 0=off, 1–6 steps, 100=on/auto
    KnownTag {
        id: 0x321,
        name: "swing_vertical",
        family: "climate",
    },
    KnownTag {
        id: 0x322,
        name: "swing_horizontal",
        family: "climate",
    },
    // RAC/CST jet cool/heat (handler jet field); 0=off in command polls
    KnownTag {
        id: 0x323,
        name: "jet",
        family: "climate",
    },
    KnownTag {
        id: 0x325,
        name: "humidity_ctrl_flag",
        family: "humidity",
    },
    // Near filter pair 0x355/0x356 on CST values dumps
    KnownTag {
        id: 0x2f2,
        name: "filter_status_code",
        family: "diag",
    },
    KnownTag {
        id: 0x353,
        name: "pm_or_aq_flag_353",
        family: "aq",
    },
    KnownTag {
        id: 0x354,
        name: "pm_or_aq_flag_354",
        family: "aq",
    },
    KnownTag {
        id: 0x357,
        name: "filter_param_357",
        family: "diag",
    },
    KnownTag {
        id: 0x358,
        name: "filter_param_358",
        family: "diag",
    },
    // DHUM bucket event; on AC often 0 — event channel
    KnownTag {
        id: 0x2b1,
        name: "event_channel_2b1",
        family: "diag",
    },
    KnownTag {
        id: 0x3a7,
        name: "feature_bit_3a7",
        family: "climate",
    },
    KnownTag {
        id: 0x109,
        name: "night_mode",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1e3,
        name: "product_status",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1e4,
        name: "sleep_mode",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1e6,
        name: "auto_operation",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1e7,
        name: "humidify_switch",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1e9,
        name: "hygiene_dry",
        family: "humidifier",
    },
    KnownTag {
        id: 0x117,
        name: "over_prevention",
        family: "humidifier",
    },
    KnownTag {
        id: 0x161,
        name: "anti_glare",
        family: "humidifier",
    },
    KnownTag {
        id: 0x164,
        name: "standby_sterilize",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1b8,
        name: "mood_light_power",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1ed,
        name: "water_filter_level",
        family: "humidifier",
    },
    KnownTag {
        id: 0x1ee,
        name: "watertank_remain",
        family: "humidifier",
    },
    KnownTag {
        id: 0x21b,
        name: "off_timer",
        family: "timer",
    },
    // RAC/DHUM: turn-on timer (paired with 0x21b); hours, 0=off
    KnownTag {
        id: 0x21c,
        name: "on_timer",
        family: "timer",
    },
    KnownTag {
        id: 0x21e,
        name: "tank_or_bucket_light",
        family: "humidity",
    },
    KnownTag {
        id: 0x21f,
        name: "display_brightness",
        family: "humidifier",
    },
    KnownTag {
        id: 0x221,
        name: "error_code",
        family: "diag",
    },
    KnownTag {
        id: 0x225,
        name: "auto_dry_remain",
        family: "humidifier",
    },
    // DHUM values: often 0; likely reserved / feature flag group with 0x21b–0x226
    KnownTag {
        id: 0x226,
        name: "timer_or_sched_flag",
        family: "timer",
    },
    KnownTag {
        id: 0x240,
        name: "air_quality",
        family: "aq",
    },
    // DHUM: cumulative counter (runtime/energy-ish); units unconfirmed — seen ~7k
    KnownTag {
        id: 0x232,
        name: "usage_counter",
        family: "diag",
    },
    // DHUM: small status/aux reading (seen 26); not half-°C ambient (that's 0x1fd)
    KnownTag {
        id: 0x233,
        name: "aux_reading",
        family: "diag",
    },
    KnownTag {
        id: 0x2a2,
        name: "uv_nano",
        family: "humidity",
    },
    // DHUM: often 0 with values; filter/sensor related family near 0x2a2
    KnownTag {
        id: 0x2ac,
        name: "filter_or_sensor_flag",
        family: "diag",
    },
    KnownTag {
        id: 0x2ad,
        name: "watertank_time",
        family: "humidifier",
    },
    KnownTag {
        id: 0x2b2,
        name: "bucket_full",
        family: "humidity",
    },
    // DHUM fan-per-mode memory table (triple stream, not stored as HA state)
    KnownTag {
        id: 0x2d7,
        name: "fan_table_mode",
        family: "climate",
    },
    KnownTag {
        id: 0x2d8,
        name: "fan_table_pad",
        family: "climate",
    },
    KnownTag {
        id: 0x2d9,
        name: "fan_table_fan",
        family: "climate",
    },
    // DHUM: often 0 in values; likely capability/echo bit near humidity block
    KnownTag {
        id: 0x324,
        name: "humidity_feature_flag",
        family: "humidity",
    },
    KnownTag {
        id: 0x333,
        name: "pm1",
        family: "aq",
    },
    KnownTag {
        id: 0x334,
        name: "pm25",
        family: "aq",
    },
    KnownTag {
        id: 0x335,
        name: "pm10",
        family: "aq",
    },
    KnownTag {
        id: 0x337,
        name: "sensor_mon",
        family: "humidifier",
    },
    KnownTag {
        id: 0x33a,
        name: "humidity_aux_flag",
        family: "humidity",
    },
    KnownTag {
        id: 0x355,
        name: "filter_used",
        family: "diag",
    },
    KnownTag {
        id: 0x356,
        name: "filter_max",
        family: "diag",
    },
    KnownTag {
        id: 0x35a,
        name: "start_time",
        family: "timer",
    },
    KnownTag {
        id: 0x35b,
        name: "stop_time",
        family: "timer",
    },
    KnownTag {
        id: 0x360,
        name: "ionizer",
        family: "humidity",
    },
    KnownTag {
        id: 0x3a0,
        name: "bell_sound",
        family: "humidifier",
    },
    KnownTag {
        id: 0x3e0,
        name: "mood_color",
        family: "humidifier",
    },
    KnownTag {
        id: 0x3e8,
        name: "caps_marker_hum",
        family: "caps",
    },
];

pub fn known_tag_name(id: u16) -> Option<&'static str> {
    KNOWN_TAGS.iter().find(|t| t.id == id).map(|t| t.name)
}

pub fn is_known_tag(id: u16) -> bool {
    known_tag_name(id).is_some()
}

#[derive(Debug, Clone, Serialize)]
pub struct ClassifiedTlv {
    pub t: u16,
    pub v: u32,
    pub known: bool,
    pub name: Option<&'static str>,
}

/// Classify a TLV list against the known catalog.
pub fn classify_tlvs(items: &[(u16, u32)]) -> Vec<ClassifiedTlv> {
    items
        .iter()
        .map(|(t, v)| {
            let name = known_tag_name(*t);
            ClassifiedTlv {
                t: *t,
                v: *v,
                known: name.is_some(),
                name,
            }
        })
        .collect()
}

/// Compact single-frame breakdown for humans and analysis tools.
pub fn llm_export_text(
    model_id: Option<&str>,
    direction: Option<&str>,
    hex_packet: &str,
    classified: &[ClassifiedTlv],
) -> String {
    let mut out = String::new();
    out.push_str("# ThinQ TLV frame\n");
    if let Some(m) = model_id {
        out.push_str(&format!("modelId: {m}\n"));
    }
    if let Some(d) = direction {
        out.push_str(&format!("direction: {d}\n"));
    }
    out.push_str(&format!("hex: {hex_packet}\n"));
    out.push_str("tags:\n");
    let mut unknowns = Vec::new();
    for c in classified {
        if c.known {
            out.push_str(&format!(
                "  0x{:03x} {} = {}\n",
                c.t,
                c.name.unwrap_or("?"),
                c.v
            ));
        } else {
            out.push_str(&format!("  0x{:03x} UNKNOWN = {}\n", c.t, c.v));
            unknowns.push(c);
        }
    }
    if !unknowns.is_empty() {
        out.push_str("unknown:\n");
        for c in unknowns {
            out.push_str(&format!("  0x{:03x} = {}\n", c.t, c.v));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_known_and_unknown() {
        let c = classify_tlvs(&[(0x1f7, 1), (0x999, 42)]);
        assert!(c[0].known);
        assert_eq!(c[0].name, Some("power"));
        assert!(!c[1].known);
    }

    #[test]
    fn llm_export_includes_unknown_section() {
        let c = classify_tlvs(&[(0x1f7, 1), (0xabc, 9)]);
        let text = llm_export_text(Some("RAC_056905_WW"), Some("fromDevice"), "deadbeef", &c);
        assert!(text.contains("UNKNOWN"));
        assert!(text.contains("0xabc"));
        assert!(text.contains("RAC_056905_WW"));
        assert!(text.contains("unknown:"));
        assert!(!text.to_lowercase().contains("copy into an llm"));
    }
}
