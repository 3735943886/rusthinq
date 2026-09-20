//! The CST_570004_WW (ceiling cassette) Rhai driver, run against frames captured from a real
//! unit (from rethink's test suite; note the 0xa7 in byte 6 of this model's frames).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;
use rusthinq_util::{hex, tlv};

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../scripts/CST_570004_WW.rhai"
);
const CAPS: &str = "000004000000A702010077B00AB04FB0A001D5B0C1B103B4F0401011B710FEBB81BBC0BC41BC88BCC1B3701FE037B381B54EB2E01006B7600120B85020B8903CB8D020B9103CBD600203B5C0B646B61030B5C1B646B61030B5C2B646B61030B5C3B646B603B69037B6F0570004D41010BD30800400DD1020B5B010000064903EFA05EFE0";
// Captured while the unit was off: power 0, mode cool, fan high, room 26.0, setpoint 24.0.
const STATE: &str = "000004000000A7020400837DC07E407E867F50347F90307F0086808840D4C0D500C84181408180C940A380A3C0A400A440F540F580F5C08340838389501E83C08FC0CD40CD00CCC0CDA0032BADA01CC6ACC0AD41B54ED56002FBD5A00960BC88D5D03CD61020C9009C40A88087D0C8AC40E9C16640668066C067006740678067C068008F80F7407C8189501E9380D558";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "CST_570004_WW")
}

fn query(kind: u32) -> Vec<u8> {
    tlv::frame_build(&[1, 1, 2, 2, 1], &[tlv::Tlv::new(0x1f5, kind)]).unwrap()
}

fn sent_tlvs(frame: &[u8]) -> Vec<(u16, u32)> {
    tlv::parse(&frame[11..frame.len() - 2])
        .into_iter()
        .map(|t| (t.t, t.v))
        .collect()
}

fn notify(tags: &[(u16, u32)]) -> String {
    let elements: Vec<_> = tags.iter().map(|&(t, v)| tlv::Tlv::new(t, v)).collect();
    let body = tlv::build(&elements);
    let mut frame = vec![0, 0, 0x04, 0, 0, 0, 0xa7, 0x02, 0x04, 0, body.len() as u8];
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&[0, 0]);
    hex::encode(&frame)
}

/// Through the handshake, holding the captured (powered off) state.
fn running() -> ScriptHarness {
    let h = harness();
    h.start().feed_hex(CAPS).feed_hex(STATE);
    h
}

/// The frames sent after `n` earlier ones.
fn sent_since(h: &ScriptHarness, n: usize) -> Vec<Vec<(u16, u32)>> {
    h.sent_raw()[n..].iter().map(|f| sent_tlvs(f)).collect()
}

#[test]
fn the_0xa7_capabilities_reply_moves_on_to_the_values_query() {
    let h = harness();
    h.start().feed_hex(CAPS);
    assert_eq!(h.sent_raw(), vec![query(1), query(2)]);
}

#[test]
fn the_captured_state_becomes_il_properties() {
    let h = running();
    for (prop, want) in [
        ("available", "true"),
        ("power", "false"),
        ("mode", "cool"),
        ("fan", "high"),
        ("temperature", "26"),
        ("target", "24"),
        ("humidity", "81"),
        ("action", "off"),
        ("swing_vertical", "false"),
        ("swing_horizontal", "false"),
        ("comfort_saving", "false"),
        ("wind_mode", "off"),
        ("auto_dry", "60min"),
        ("auto_dry_remaining", "30"),
        ("sleep_timer", "0"),
        ("display", "100%"),
        ("power_draw", "0"),
        ("filter_remaining", "32"),
        ("filter_used", "1637"),
        ("filter_life", "2400"),
        ("error", "0"),
    ] {
        assert_eq!(h.property(prop).as_deref(), Some(want), "{prop}");
    }
}

#[test]
fn energy_saving_is_not_reported_while_the_unit_is_off() {
    // 0x20d is in the captured frame, but it means nothing while the unit is not cooling
    assert_eq!(running().property("energy_save"), None);
}

#[test]
fn the_first_values_frame_masks_the_noisy_tag_and_starts_the_refresh() {
    let h = running();
    let clips = h.sent_clip();
    assert_eq!(clips.len(), 1);
    assert_eq!(clips[0].0, "setMaskingInfo");
    assert_eq!(h.pending_timers(), vec![("refresh".to_string(), 900000)]);
}

#[test]
fn a_mode_write_powers_the_unit_on() {
    let h = running();
    let n = h.sent_raw().len();
    h.set_property("mode", "auto");
    assert_eq!(
        sent_since(&h, n),
        vec![vec![(0x1f9, 3), (0x1f7, 1), (0x1fa, 6), (0x1fe, 48)]]
    );
    h.set_property("mode", "cool");
    assert_eq!(sent_since(&h, n)[1][0], (0x1f9, 0));
}

#[test]
fn power_on_carries_mode_fan_and_setpoint_and_power_off_only_itself() {
    let h = running();
    let n = h.sent_raw().len();
    h.set_property("power", "true");
    h.set_property("power", "false");
    assert_eq!(
        sent_since(&h, n),
        vec![
            vec![(0x1f7, 1), (0x1f9, 0), (0x1fa, 6), (0x1fe, 48)],
            vec![(0x1f7, 0)],
        ]
    );
}

#[test]
fn fan_writes_use_this_units_scale() {
    let h = running();
    let n = h.sent_raw().len();
    for name in ["auto", "very_low", "low", "medium", "high", "power"] {
        h.set_property("fan", name);
    }
    let firsts: Vec<_> = sent_since(&h, n).iter().map(|f| f[0]).collect();
    assert_eq!(
        firsts,
        vec![
            (0x1fa, 8),
            (0x1fa, 1),
            (0x1fa, 2),
            (0x1fa, 4),
            (0x1fa, 6),
            (0x1fa, 7)
        ]
    );
    assert_eq!(
        sent_since(&h, n)[0],
        vec![(0x1fa, 8), (0x1f9, 0), (0x1fe, 48)]
    );
}

#[test]
fn the_setpoint_goes_out_in_half_degrees() {
    let h = running();
    let n = h.sent_raw().len();
    h.set_property("target", "24.5");
    h.set_property("target", "16");
    assert_eq!(
        sent_since(&h, n),
        vec![
            vec![(0x1fe, 49), (0x1f9, 0), (0x1fa, 6)],
            vec![(0x1fe, 32), (0x1f9, 0), (0x1fa, 6)],
        ]
    );
}

#[test]
fn the_host_refuses_a_setpoint_outside_the_units_range_or_step() {
    let h = running();
    let n = h.sent_raw().len();
    for v in ["15.5", "30.5", "24.3"] {
        h.set_property("target", v);
    }
    assert_eq!(h.sent_raw().len(), n);
    assert!(h.event("reject").unwrap().contains("\"prop\":\"target\""));
}

#[test]
fn switches_and_selects_write_their_own_tag_alone() {
    let h = running();
    let n = h.sent_raw().len();
    h.set_property("swing_vertical", "true");
    h.set_property("swing_horizontal", "on");
    h.set_property("comfort_saving", "true");
    h.set_property("auto_dry", "smart");
    h.set_property("display", "50%");
    h.set_property("sleep_timer", "30");
    assert_eq!(
        sent_since(&h, n),
        vec![
            vec![(0x205, 1)],
            vec![(0x206, 1)],
            vec![(0x23f, 1)],
            vec![(0x20e, 255)],
            vec![(0x21f, 150)],
            vec![(0x21a, 30)],
        ]
    );
}

#[test]
fn a_bad_write_is_refused_and_sends_nothing() {
    let h = running();
    let n = h.sent_raw().len();
    h.set_property("fan", "turbo");
    h.set_property("mode", "heat");
    h.set_property("sleep_timer", "901");
    h.set_property("humidity", "50"); // read only
    assert_eq!(h.sent_raw().len(), n);
}

#[test]
fn wind_mode_is_read_from_the_flags_and_written_as_one_exclusive_set() {
    let h = running();
    h.feed_hex(&notify(&[(0x3d7, 1)]));
    assert_eq!(h.property("wind_mode").as_deref(), Some("long_power"));
    h.feed_hex(&notify(&[(0x3d7, 0), (0x291, 1)]));
    assert_eq!(h.property("wind_mode").as_deref(), Some("study"));

    let n = h.sent_raw().len();
    h.set_property("wind_mode", "manner");
    h.set_property("wind_mode", "off");
    assert_eq!(
        sent_since(&h, n),
        vec![
            vec![(0x290, 0), (0x291, 0), (0x3d5, 0), (0x3d6, 1), (0x3d7, 0)],
            vec![(0x290, 0), (0x291, 0), (0x3d5, 0), (0x3d6, 0), (0x3d7, 0)],
        ]
    );
    assert_eq!(h.property("wind_mode").as_deref(), Some("off"));
}

#[test]
fn energy_saving_is_trusted_only_while_cooling() {
    let h = running();
    h.feed_hex(&notify(&[(0x1f7, 1), (0x1f9, 1), (0x20d, 1)])); // dry: ignored
    assert_eq!(h.property("energy_save"), None);
    h.feed_hex(&notify(&[(0x1f9, 0), (0x20d, 1)])); // cool: adopted
    assert_eq!(h.property("energy_save").as_deref(), Some("true"));
}

#[test]
fn energy_saving_set_while_off_is_written_when_cooling_starts() {
    let h = running();
    let n = h.sent_raw().len();
    h.set_property("energy_save", "true"); // off: nothing can take it yet
    assert_eq!(h.sent_raw().len(), n);
    assert_eq!(h.property("energy_save").as_deref(), Some("true"));

    // the unit powers up in cool and reports the setting as off: write it again
    h.feed_hex(&notify(&[(0x1f7, 1), (0x1f9, 0), (0x20d, 0)]));
    assert_eq!(sent_since(&h, n), vec![vec![(0x20d, 1)]]);
    assert_eq!(h.property("energy_save").as_deref(), Some("true"));

    // while cooling a change is written at once
    h.set_property("energy_save", "false");
    assert_eq!(sent_since(&h, n)[1], vec![(0x20d, 0)]);
}

#[test]
fn action_follows_power_and_mode_and_polling_runs_only_while_active() {
    let h = running();
    assert!(!h.pending_timers().iter().any(|(n, _)| n == "poll"));

    h.feed_hex(&notify(&[(0x1f7, 1), (0x1f9, 0)]));
    assert_eq!(h.property("action").as_deref(), Some("cooling"));
    assert!(h.pending_timers().contains(&("poll".to_string(), 28000)));

    let n = h.sent_raw().len();
    h.fire_timer("poll");
    assert_eq!(h.sent_raw()[n..], [query(2)]);
    assert!(h.pending_timers().contains(&("poll".to_string(), 28000)));

    h.feed_hex(&notify(&[(0x189, 0)])); // the unit says it is not cooling right now
    assert_eq!(h.property("action").as_deref(), Some("idle"));
    assert!(!h.pending_timers().iter().any(|(n, _)| n == "poll"));

    h.feed_hex(&notify(&[(0x189, 1), (0x1f9, 1)]));
    assert_eq!(h.property("action").as_deref(), Some("drying"));
    h.feed_hex(&notify(&[(0x1f9, 2)]));
    assert_eq!(h.property("action").as_deref(), Some("fan"));
    h.feed_hex(&notify(&[(0x1f9, 3)]));
    assert_eq!(h.property("action").as_deref(), Some("active"));
    h.feed_hex(&notify(&[(0x1f7, 0)]));
    assert_eq!(h.property("action").as_deref(), Some("off"));
    assert!(!h.pending_timers().iter().any(|(n, _)| n == "poll"));
}

#[test]
fn losing_the_connection_marks_it_unavailable() {
    let h = running();
    h.drop_device();
    assert_eq!(h.property("available").as_deref(), Some("false"));
}
