//! The F24VDD Rhai driver. The idle frame is a real capture from the washer on r5c (off; 191 Wh
//! from the last cycle; the "Kids' Wear" download course; 24 cycles since the last Tub Clean;
//! Heavy Duty as the last operating course; the end-of-cycle sound on): each of those agrees
//! with what the rethink adapter had retained for this unit at that moment. The start frames
//! are the ones the LG app sent, byte for byte (from rethink's test suite for this family).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

const SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/F24VDD.rhai");

const IDLE: &str = "aa4e20ec0022000000000000000000000000000000020000bf3c39180e0000000003000402022d1e0022000000000000000000000000000000020000bf3c39180e0000000003000402022d1ef9bb";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "F24VDD")
}

fn expect(h: &ScriptHarness, pairs: &[(&str, &str)]) {
    for (prop, want) in pairs {
        assert_eq!(h.property(prop).as_deref(), Some(*want), "{prop}");
    }
}

fn framed(inner: &[u8]) -> String {
    rusthinq_util::hex::encode(rusthinq_devices::wrap_aabb(inner))
}

/// A 0xEC frame whose previous and current record are the same 34-byte `payload`.
fn status(payload: [u8; 34]) -> String {
    let mut inner = vec![0x20, 0xec];
    for _ in 0..2 {
        inner.extend_from_slice(&[0x00, 34]);
        inner.extend_from_slice(&payload);
    }
    framed(&inner)
}

fn inner_of(h: &ScriptHarness, index: usize) -> Vec<u8> {
    let sent = h.sent_raw();
    sent[index][2..sent[index].len() - 2].to_vec()
}

fn payload_of(h: &ScriptHarness, index: usize) -> String {
    rusthinq_util::hex::encode(&inner_of(h, index)[2..])
}

/// The washer mid-wash: the record the source comments describe (17 01 21 01 2c 06 00 03 03 04
/// 03 ...), Heavy Duty at heavy soil, medium spin, 60 C, three rinses, door locked, Add Garment.
fn heavy_duty_wash() -> [u8; 34] {
    let mut p = [0u8; 34];
    p[..11].copy_from_slice(&[23, 1, 33, 1, 44, 6, 0, 3, 3, 4, 3]);
    p[14] = 0x20 | 0x08; // remote start, child lock
    p[15] = 0x02 | 0x04 | 0x40; // alarm, door lock, add garment
    p[17] = 0x01;
    p[18] = 0x2c; // 300 Wh so far
    p[22] = 14; // operating course: Heavy Duty
    p
}

#[test]
fn a_real_idle_frame_becomes_il_properties() {
    let h = harness();
    h.feed_hex(IDLE);
    assert_eq!(h.script_error(), None);
    expect(
        &h,
        &[
            ("available", "true"),
            ("power", "false"),
            ("status", "off"),
            ("course", "none"),
            ("op_course", "heavy_duty"),
            ("download_course", "kids_wear"),
            ("remaining_time", "0"),
            ("initial_time", "0"),
            ("reserve_time", "0"),
            ("soil", "none"),
            ("spin", "no_spin"),
            ("temp", "none"),
            ("rinse", "0"),
            ("energy", "191"),
            ("load_level", "0"),
            ("tub_clean_count", "24"),
            ("error", "false"),
            ("error_message", "OK"),
            ("alarm", "true"),
            ("child_lock", "false"),
            ("door_lock", "false"),
            ("remote_start", "false"),
            ("steam", "false"),
        ],
    );
}

#[test]
fn a_running_wash_matches_the_record_the_cloud_decoded() {
    let h = harness();
    h.feed_hex(&status(heavy_duty_wash()));
    expect(
        &h,
        &[
            ("status", "washing"),
            ("course", "heavy_duty"),
            ("op_course", "heavy_duty"),
            ("remaining_time", "93"), // 1h33m left
            ("initial_time", "104"),  // of 1h44m
            ("soil", "heavy"),
            ("spin", "medium"),
            ("temp", "60"),
            ("rinse", "3"),
            ("door_lock", "true"),
            ("add_load", "true"),
            ("child_lock", "true"),
            ("remote_start", "true"),
            ("energy", "300"),
        ],
    );
}

#[test]
fn the_dial_course_and_the_running_course_are_reported_apart() {
    let h = harness();
    let mut p = heavy_duty_wash();
    p[5] = 14; // the dial is on "Downloaded course"
    p[22] = 5; // and the machine is running Cotton
    h.feed_hex(&status(p));
    expect(
        &h,
        &[("course", "downloaded_course"), ("op_course", "cotton")],
    );
    // a dial position start cannot express is not adopted as the selection
    assert_eq!(h.property("course_select"), None);
}

#[test]
fn the_flags_decode_from_their_bits() {
    let h = harness();
    let mut p = heavy_duty_wash();
    p[14] = 0x01 | 0x02 | 0x04 | 0x10 | 0x40;
    p[15] = 0x80;
    h.feed_hex(&status(p));
    expect(
        &h,
        &[
            ("crease_care", "true"),
            ("fresh_care", "true"),
            ("rinse_hold", "true"),
            ("steam", "true"),
            ("favorite", "true"),
            ("turbo_shot", "true"),
            ("alarm", "false"),
        ],
    );
}

// ---- commands, as the app sent them ------------------------------------------------

#[test]
fn colour_care_start_as_the_app_sent_it() {
    let h = harness();
    h.set_property("course_select", "colour_care");
    h.set_property("start", "");
    assert_eq!(
        payload_of(&h, 0),
        "0a0202020300002000201000000000000000000000"
    );
}

#[test]
fn heavy_duty_start_as_the_app_sent_it() {
    let h = harness();
    h.set_property("course_select", "heavy_duty");
    h.set_property("start", "");
    assert_eq!(
        payload_of(&h, 0),
        "060303040300002000200e00000000000000000000"
    );
}

#[test]
fn steam_refresh_sets_the_steam_bit_and_the_app_agreed() {
    let h = harness();
    h.set_property("course_select", "steam_refresh");
    h.set_property("start", "");
    // the course fixes steam on: the flags byte carries 0x10 beside the remote-start 0x20
    assert_eq!(
        payload_of(&h, 0),
        "010200000000003000200100000000000000000000"
    );
}

#[test]
fn resuming_clears_the_initial_bit_and_changes_nothing_else() {
    let h = harness();
    h.set_property("course_select", "heavy_duty");
    h.set_property("start", "");
    let mut paused = heavy_duty_wash();
    paused[0] = 6;
    h.feed_hex(&status(paused));
    h.set_property("start", "");
    let start = inner_of(&h, 0);
    let resume = inner_of(&h, 1);
    assert_eq!(start[2 + 9], 0x20);
    assert_eq!(resume[2 + 9], 0);
    assert_eq!(start[..2 + 9], resume[..2 + 9]);
    assert_eq!(start[2 + 10..], resume[2 + 10..]);
}

#[test]
fn a_course_with_no_preset_the_downloaded_dial_position_is_not_started() {
    let h = harness();
    h.set_property("course_select", "downloaded_course");
    h.set_property("course_select", "none");
    h.set_property("start", ""); // nothing valid selected
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").is_some());
}

#[test]
fn pause_and_power_off_are_the_short_controls() {
    let h = harness();
    h.set_property("power_off", "");
    h.set_property("pause", "");
    assert_eq!(rusthinq_util::hex::encode(inner_of(&h, 0)), "f024010100");
    assert_eq!(rusthinq_util::hex::encode(inner_of(&h, 1)), "f024040100");
}

#[test]
fn a_washer_that_refuses_a_power_off_says_so() {
    let h = harness();
    h.set_property("power_off", "");
    // this washer answered 0xff to a power-off three times, each while it had nothing to power off
    h.feed_hex(&framed(&[0x20, 0x00, 0x24, 0xff]));
    let reject = h.event("reject").expect("a rejection");
    assert!(
        reject.contains("power_off") && reject.contains("refused"),
        "{reject}"
    );
}

#[test]
fn a_record_of_another_length_reports_nothing() {
    let h = harness();
    let mut inner = vec![0x20, 0xeb, 0x00, 33];
    inner.extend_from_slice(&[0u8; 33]);
    h.feed_hex(&framed(&inner));
    assert_eq!(h.property("status"), None);
}

#[test]
fn dropping_the_device_marks_it_unavailable() {
    let h = harness();
    h.feed_hex(IDLE).drop_device();
    assert_eq!(h.property("available").as_deref(), Some("false"));
}

#[test]
fn the_descriptor_offers_fourteen_startable_courses_and_no_downloaded_one() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, _) = h.raw_publish("il/harness").expect("descriptor published");
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["class"], "washing_machine");
    let options = d["props"]["course_select"]["options"].as_array().unwrap();
    assert_eq!(options.len(), 14);
    assert!(!options.iter().any(|o| o == "downloaded_course"));
    assert_eq!(d["props"]["start"]["requires"], "remote_start");
    rusthinq_devices::scripting::set_il_prefix(None);
}
