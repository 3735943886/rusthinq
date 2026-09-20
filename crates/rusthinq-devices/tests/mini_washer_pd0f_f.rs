//! The Pd0F_F Rhai driver. The status frames are built from the documented record layout
//! (payload offsets read out of the appliance's own cloud), the idle one matching what the
//! rethink adapter had retained for this unit; the command frames are the ones the LG app sent,
//! byte for byte (from rethink's test suite for this family).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

const SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/Pd0F_F.rhai");

/// A real capture from the drawer on r5c, taken when it woke briefly.
const REAL_IDLE: &str = "aa3c20ec001900000100010100000100000000000000000000000000006700001900000100010100000100000000000000000000000000006700afbb";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "Pd0F_F")
}

fn expect(h: &ScriptHarness, pairs: &[(&str, &str)]) {
    for (prop, want) in pairs {
        assert_eq!(h.property(prop).as_deref(), Some(*want), "{prop}");
    }
}

/// `AA len inner checksum BB`, as the appliance frames it.
fn framed(inner: &[u8]) -> String {
    rusthinq_util::hex::encode(rusthinq_devices::wrap_aabb(inner))
}

/// A 0xEC status frame whose previous and current record are the same `payload`.
fn status(payload: [u8; 25]) -> String {
    let mut inner = vec![0x20, 0xec];
    for _ in 0..2 {
        inner.extend_from_slice(&[0x00, 25]);
        inner.extend_from_slice(&payload);
    }
    framed(&inner)
}

/// The payload for an off drawer washer with Small Load on the dial (spin on).
fn idle() -> [u8; 25] {
    let mut p = [0u8; 25];
    p[5] = 1; // course: Small Load
    p[8] = 1; // spin
    p
}

/// Washing: 41 of 45 minutes left, Small Load, 40 C, three rinses, remote start armed, child lock.
fn washing() -> [u8; 25] {
    let mut p = idle();
    p[0] = 5; // washing
    p[2] = 41;
    p[4] = 45;
    p[9] = 40;
    p[10] = 3;
    p[14] = 0x01 | 0x08; // child lock, door lock
    p[15] = 0x02; // sterilize
    p[16] = 0x04; // remote start
    p
}

fn inner_of(h: &ScriptHarness, index: usize) -> String {
    let sent = h.sent_raw();
    rusthinq_util::hex::encode(&sent[index][2..sent[index].len() - 2])
}

#[test]
fn idle_matches_what_the_adapter_reported_for_this_unit() {
    let h = harness();
    h.feed_hex(&status(idle()));
    expect(
        &h,
        &[
            ("available", "true"),
            ("power", "false"),
            ("status", "off"),
            ("course", "small_load"),
            ("course_select", "small_load"),
            ("remaining_time", "0"),
            ("initial_time", "0"),
            ("reserve_time", "0"),
            ("temp", "cold"),
            ("rinse", "0"),
            ("spin", "true"),
            ("error", "false"),
            ("error_message", "OK"),
            ("child_lock", "false"),
            ("audible_diagnosis", "false"),
            ("door_lock", "false"),
            ("sterilize", "false"),
            ("warm_water", "false"),
            ("remote_start", "false"),
        ],
    );
}

#[test]
fn a_running_cycle_reports_its_times_and_flags() {
    let h = harness();
    h.feed_hex(&status(washing()));
    expect(
        &h,
        &[
            ("power", "true"),
            ("status", "washing"),
            ("remaining_time", "41"),
            ("initial_time", "45"),
            ("temp", "40"),
            ("rinse", "3"),
            ("child_lock", "true"),
            ("door_lock", "true"),
            ("sterilize", "true"),
            ("remote_start", "true"),
        ],
    );
}

#[test]
fn times_are_zeroed_while_off_not_left_frozen_mid_cycle() {
    let h = harness();
    let mut p = idle();
    p[2] = 30; // the appliance keeps the last cycle's figures in these bytes
    h.feed_hex(&status(p));
    expect(&h, &[("remaining_time", "0"), ("status", "off")]);
}

#[test]
fn the_single_record_frames_are_read_too() {
    let h = harness();
    // 0xEB: the current state alone, sent right after reconnecting
    let mut inner = vec![0x20, 0xeb, 0x00, 25];
    inner.extend_from_slice(&washing());
    h.feed_hex(&framed(&inner));
    expect(&h, &[("status", "washing")]);
    // 0xE2: the same record behind header byte 03, sent when a cycle begins
    let mut inner = vec![0x20, 0xe2, 0x03, 25];
    inner.extend_from_slice(&idle());
    h.feed_hex(&framed(&inner));
    expect(&h, &[("status", "off")]);
}

#[test]
fn a_record_with_the_wrong_payload_length_reports_nothing() {
    let h = harness();
    let mut inner = vec![0x20, 0xeb, 0x00, 24];
    inner.extend_from_slice(&[0u8; 24]);
    h.feed_hex(&framed(&inner));
    assert_eq!(h.property("status"), None);
}

// ---- commands, as the app sent them ------------------------------------------------

#[test]
fn small_load_start_as_the_app_sent_it() {
    let h = harness();
    h.feed_hex(&status(idle()));
    h.set_property("start", "");
    assert_eq!(inner_of(&h, 0), "f026010001000200000000000000000005");
}

#[test]
fn resume_is_the_same_frame_with_the_initial_bit_cleared() {
    let h = harness();
    let mut paused = idle();
    paused[0] = 2;
    h.feed_hex(&status(paused));
    h.set_property("start", "");
    assert_eq!(inner_of(&h, 0), "f026010001000200000000000000000004");
}

#[test]
fn pause_and_power_off_are_the_short_controls() {
    let h = harness();
    h.set_property("power_off", "");
    h.set_property("pause", "");
    assert_eq!(inner_of(&h, 0), "f024010100");
    assert_eq!(inner_of(&h, 1), "f024040100");
}

#[test]
fn the_selected_course_is_what_start_asks_for() {
    let h = harness();
    h.feed_hex(&status(idle()));
    h.set_property("course_select", "tub_clean");
    assert_eq!(h.property("course_select").as_deref(), Some("tub_clean"));
    h.set_property("start", "");
    // Tub Clean: soil 0, spin 1, 60 C, one rinse
    assert_eq!(inner_of(&h, 0), "f0260700013c0100000000000000000005");
}

#[test]
fn the_selection_follows_the_dial_but_survives_a_status_repeat_and_a_power_off() {
    let h = harness();
    h.feed_hex(&status(idle())); // dial on Small Load
    h.set_property("course_select", "wool");
    h.feed_hex(&status(idle())); // the next report repeats the dial: must not undo the choice
    assert_eq!(h.property("course_select").as_deref(), Some("wool"));
    let mut off = idle();
    off[5] = 0; // powered off zeroes the course byte
    h.feed_hex(&status(off));
    h.set_property("start", "");
    assert!(inner_of(&h, 0).starts_with("f026"));
    assert_eq!(&inner_of(&h, 0)[4..6], "03"); // still wool
    let mut turned = idle();
    turned[5] = 6; // someone turned the dial to Rinse + Spin
    h.feed_hex(&status(turned));
    assert_eq!(h.property("course_select").as_deref(), Some("rinse_spin"));
}

#[test]
fn a_dial_position_start_cannot_express_is_not_adopted() {
    let h = harness();
    h.feed_hex(&status(idle()));
    let mut odd = idle();
    odd[5] = 10; // Speed Wash: on the family table, not startable here
    h.feed_hex(&status(odd));
    assert_eq!(h.property("course").as_deref(), Some("speed_wash"));
    assert_eq!(h.property("course_select").as_deref(), Some("small_load"));
}

#[test]
fn a_course_the_drawer_does_not_have_is_rejected() {
    let h = harness();
    h.set_property("course_select", "speed_wash");
    h.set_property("course_select", "nonsense");
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").is_some());
}

#[test]
fn a_refused_command_is_reported_against_the_property_that_sent_it() {
    let h = harness();
    h.set_property("power_off", "");
    // this washer answered 0xff to a power-off while it had nothing to power off
    h.feed_hex(&framed(&[0x20, 0x00, 0x24, 0xff]));
    let reject = h.event("reject").expect("a rejection");
    assert!(reject.contains("power_off") && reject.contains("refused"));
}

#[test]
fn an_accepted_command_publishes_nothing_and_another_devices_ack_is_ignored() {
    let h = harness();
    h.set_property("power_off", "");
    h.feed_hex(&framed(&[0x20, 0x00, 0x24, 0x00]));
    h.feed_hex(&framed(&[0x30, 0x00, 0x24, 0xff])); // the dryer's address, not ours
    assert_eq!(h.event("reject"), None);
}

#[test]
fn dropping_the_device_marks_it_unavailable() {
    let h = harness();
    h.feed_hex(&status(idle())).drop_device();
    assert_eq!(h.property("available").as_deref(), Some("false"));
}

#[test]
fn the_descriptor_marks_the_controls_and_the_dial_is_separate_from_the_selection() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, _) = h.raw_publish("il/harness").expect("descriptor published");
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["class"], "mini_washer");
    assert_eq!(d["props"]["start"]["type"], "trigger");
    assert_eq!(d["props"]["start"]["requires"], "remote_start");
    assert_eq!(d["props"]["course_select"]["rw"], true);
    assert_eq!(d["props"]["course"].get("rw"), None);
    rusthinq_devices::scripting::set_il_prefix(None);
}

#[test]
fn a_real_frame_from_the_appliance_reads_as_idle_small_load() {
    let h = harness();
    h.feed_hex(REAL_IDLE);
    assert_eq!(h.script_error(), None);
    expect(
        &h,
        &[
            ("available", "true"),
            ("power", "false"),
            ("status", "off"),
            ("course", "small_load"),
        ],
    );
}

#[test]
fn the_host_holds_start_back_until_remote_start_is_armed() {
    let h = harness();
    h.start().feed_hex(&status(idle())); // remote start is not armed at the panel
    h.set_property("start", "");
    h.set_property("power_off", "");
    assert!(
        h.sent_raw().is_empty(),
        "nothing is sent to an appliance that would refuse it"
    );
    assert_eq!(
        h.event("reject").unwrap(),
        r#"{"prop":"power_off","reason":"requires remote_start"}"#
    );
    h.feed_hex(&status(washing())); // now armed
    h.set_property("start", "");
    assert_eq!(h.sent_raw().len(), 1);
}
