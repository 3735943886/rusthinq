//! The 1WPU4CIGCR__2 Rhai driver, run against frames captured from a real appliance (the
//! captures and the write frames the appliance accepted come from rethink's own test suite
//! for this model).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../scripts/1WPU4CIGCR__2.rhai"
);

// The tap's UV lamp finishing its cycle: cockState 1 in the previous record, 0 in the current.
const UV_DONE: &str = "AA3A12EC020103010101FFFF01FF030101FFFF08041000FF01FF00000001020003010101FFFF01FF030101FFFF08041000FF01FF0000000178BB";
// The panel switched the amount from 120ml to 250ml: byte 3, and nothing else, moved.
const AMOUNT_250: &str = "AA3A12EC020003010101FFFF01FF030101FFFF08041000FF01FF00000001020003020101FFFF01FF030101FFFF08041000FF01FF0000000178BB";
// Today's dispensed totals; the cloud read coldWaterAmount 252 (0x00FC) at this moment.
const COUNTERS: &str = "AA12121F0000000000FC000000000000BCBB";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "1WPU4CIGCR__2")
}

#[test]
fn the_captured_status_becomes_il_properties() {
    let h = harness();
    h.start().feed_hex(UV_DONE);
    for (prop, want) in [
        ("available", "true"),
        ("status", "normal"),
        ("tap_uv", "standby"),
        ("water_selection", "cold"),
        ("water_amount", "120ml"),
        ("default_water", "cold"),
        ("default_amount", "120ml"),
        ("auto_care", "true"),
        ("button_sound", "true"),
        ("not_use_notice", "true"),
        // the wire carries 08-04 16:00, in UTC
        ("self_clean_next", "08-04 16:00"),
    ] {
        assert_eq!(h.property(prop).as_deref(), Some(want), "{prop}");
    }
}

#[test]
fn only_the_current_record_counts_not_the_previous_one() {
    let h = harness();
    // the previous record has the lamp running (cockState 1), the current one does not
    h.feed_hex(UV_DONE);
    assert_eq!(h.property("tap_uv").as_deref(), Some("standby"));
}

#[test]
fn the_selected_amount_follows_the_panel_and_nothing_else_moves() {
    let h = harness();
    h.feed_hex(AMOUNT_250);
    assert_eq!(h.property("water_amount").as_deref(), Some("250ml"));
    assert_eq!(h.property("water_selection").as_deref(), Some("cold"));
    assert_eq!(h.property("default_amount").as_deref(), Some("120ml"));
}

#[test]
fn todays_totals_are_sixteen_bit() {
    let h = harness();
    h.feed_hex(COUNTERS);
    assert_eq!(h.property("cold_water_today").as_deref(), Some("252"));
    assert_eq!(h.property("hot_water_today").as_deref(), Some("0"));
    // the same frame shape with 0x1234 on the cold tap; the checksum is not validated
    h.feed_hex("AA12121F00000000123400000000000000BB");
    assert_eq!(h.property("cold_water_today").as_deref(), Some("4660"));
}

// The frames the appliance was really sent, each of which changed its setting and came back
// in the next status frame.
#[test]
fn writes_are_byte_for_byte_the_frames_the_appliance_accepted() {
    let h = harness();
    h.set_property("default_amount", "250ml");
    h.set_property("button_sound", "false");
    let sent = h.sent_raw();
    assert_eq!(
        rusthinq_util::hex::encode(&sent[0]),
        "aa20f017ffffffffffffffffffffff02ffffffffffffffffffffffffffffefbb"
    );
    assert_eq!(
        rusthinq_util::hex::encode(&sent[1]),
        "aa20f017ffffffffffffffffffffffff00ffffffffffffffffffffffffffedbb"
    );
}

#[test]
fn one_setting_travels_at_a_time() {
    let h = harness();
    h.set_property("auto_care", "true");
    let frame = &h.sent_raw()[0];
    let record = &frame[4..frame.len() - 2];
    assert_eq!(record.len(), 26);
    assert_eq!(record[20], 1);
    assert_eq!(record.iter().filter(|&&b| b != 0xff).count(), 1);
    h.set_property("default_water", "normal");
    assert_eq!(h.sent_raw()[1][4..][10], 2);
}

#[test]
fn nothing_is_published_from_a_write() {
    let h = harness();
    h.set_property("auto_care", "true");
    assert_eq!(h.property("auto_care"), None);
}

#[test]
fn a_bad_value_or_a_read_only_property_sends_nothing() {
    let h = harness();
    h.set_property("default_water", "sparkling");
    h.set_property("default_amount", "continuous");
    h.set_property("water_selection", "cold");
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").is_some());
}

#[test]
fn other_and_malformed_frames_are_ignored() {
    let h = harness();
    h.feed_hex(UV_DONE);
    let before = h.property("water_amount");
    for frame in [
        "AA0A12E2440102591DBB", // 0xE2, not decoded
        "AA0912AF0D0003D1BB",   // 0xAF, not decoded
        "001122",               // not an AABB frame at all
        "AA0612EC0000BB",       // 0x12EC but far too short
        "AA0A121F000000BB",     // counters, but not six of them
    ] {
        h.feed_hex(frame);
    }
    assert_eq!(h.property("water_amount"), before);
}

#[test]
fn dropping_the_device_marks_it_unavailable() {
    let h = harness();
    h.feed_hex(UV_DONE).drop_device();
    assert_eq!(h.property("available").as_deref(), Some("false"));
}

#[test]
fn the_descriptor_is_published() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, _) = h.raw_publish("il/harness").expect("descriptor published");
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["class"], "water_purifier");
    assert_eq!(d["props"]["self_clean_next"]["type"], "text");
    assert_eq!(d["props"]["default_amount"]["rw"], true);
    rusthinq_devices::scripting::set_il_prefix(None);
}

#[test]
fn the_host_validates_selects_and_binaries_against_the_descriptor() {
    let h = harness();
    h.start().feed_hex(UV_DONE);
    h.set_property("default_amount", "continuous"); // not among the writable options
    h.set_property("auto_care", "maybe");
    h.set_property("water_selection", "cold"); // read only
    assert!(h.sent_raw().is_empty());
    h.set_property("auto_care", "ON"); // normalised to true for the script
    assert_eq!(h.sent_raw().len(), 1);
}
