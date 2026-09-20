//! The RH14_N_KR Rhai driver. The three status frames are real captures from the appliance on
//! r5c, taken one minute apart while it ran a Cotton Normal cycle (each carries the previous
//! record too, and the first's previous record agrees with what the rethink adapter had
//! retained for this unit at that moment: 46 minutes left, 126 Wh). The command frames are
//! the ones the LG app sent (from rethink's test suite for this family).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

const SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/RH14_N_KR.rhai");

const RUNNING_1: &str = "aa3c30ec001902002e010a070003020200000000001900007e010000007000001902002d010a07000302020000000000190000830100000070008fbb";
const RUNNING_2: &str = "aa3c30ec001902002d010a0700030202000000000019000083010000007000001902002c010a0700030202000000000019000088010000007000b7bb";
const RUNNING_3: &str = "aa3c30ec001902002c010a0700030202000000000019000088010000007000001902002b010a070003020200000000001900008c010000007000bcbb";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "RH14_N_KR")
}

fn expect(h: &ScriptHarness, pairs: &[(&str, &str)]) {
    for (prop, want) in pairs {
        assert_eq!(h.property(prop).as_deref(), Some(*want), "{prop}");
    }
}

fn framed(inner: &[u8]) -> String {
    rusthinq_util::hex::encode(rusthinq_devices::wrap_aabb(inner))
}

/// A 0xEC frame whose previous and current record are the same `payload`.
fn status(payload: [u8; 25]) -> String {
    let mut inner = vec![0x30, 0xec];
    for _ in 0..2 {
        inner.extend_from_slice(&[0x00, 25]);
        inner.extend_from_slice(&payload);
    }
    framed(&inner)
}

fn inner_of(h: &ScriptHarness, index: usize) -> String {
    let sent = h.sent_raw();
    rusthinq_util::hex::encode(&sent[index][2..sent[index].len() - 2])
}

/// The record of the first real frame, as a template to vary.
fn cotton() -> [u8; 25] {
    let mut p = [0u8; 25];
    p[..10].copy_from_slice(&[2, 0, 45, 1, 10, 7, 0, 3, 2, 2]);
    p[15] = 0x19; // remote start armed, plus bits this driver leaves alone
    p
}

#[test]
fn a_real_frame_becomes_il_properties() {
    let h = harness();
    h.feed_hex(RUNNING_1);
    expect(
        &h,
        &[
            ("available", "true"),
            ("power", "true"),
            ("status", "running"),
            ("process", "drying_1"),
            ("course", "cotton_normal"),
            ("course_select", "cotton_normal"),
            ("remaining_time", "45"),
            ("initial_time", "70"),
            ("reserve_time", "0"),
            ("dry_level", "iron_dry"),
            ("eco_hybrid", "normal"),
            ("energy", "131"),
            ("error", "false"),
            ("error_message", "OK"),
            ("remote_start", "true"),
            ("steam", "false"),
            ("child_lock", "false"),
            ("reservation", "false"),
        ],
    );
}

#[test]
fn only_the_current_record_counts_and_the_countdown_and_energy_follow_it() {
    let h = harness();
    h.feed_hex(RUNNING_2);
    expect(&h, &[("remaining_time", "44"), ("energy", "136")]);
    h.feed_hex(RUNNING_3);
    expect(&h, &[("remaining_time", "43"), ("energy", "140")]);
}

#[test]
fn off_zeroes_the_times_and_the_process() {
    let h = harness();
    let mut p = cotton();
    p[0] = 0;
    h.feed_hex(&status(p));
    expect(
        &h,
        &[
            ("power", "false"),
            ("status", "off"),
            ("remaining_time", "0"),
            ("process", "none"),
        ],
    );
}

#[test]
fn the_flags_decode_from_their_bits() {
    let h = harness();
    let mut p = cotton();
    p[14] = 0x01 | 0x02 | 0x10 | 0x20 | 0x40 | 0x80;
    p[16] = 0x08;
    h.feed_hex(&status(p));
    expect(
        &h,
        &[
            ("reservation", "true"),
            ("anti_crease", "true"),
            ("child_lock", "true"),
            ("self_clean", "true"),
            ("damp_dry_beep", "true"),
            ("hand_iron", "true"),
            ("steam", "true"),
        ],
    );
}

#[test]
fn an_error_code_is_reported_with_its_message() {
    let h = harness();
    let mut p = cotton();
    p[6] = 14;
    h.feed_hex(&status(p));
    expect(
        &h,
        &[("error", "true"), ("error_message", "Water tank empty")],
    );
    p[6] = 99;
    h.feed_hex(&status(p));
    expect(&h, &[("error_message", "Unknown error (99)")]);
}

// ---- commands, as the app sent them ------------------------------------------------

#[test]
fn cotton_normal_start_as_the_app_sent_it() {
    let h = harness();
    h.feed_hex(RUNNING_1); // the dial is on Cotton Normal
    h.set_property("start", "");
    assert_eq!(inner_of(&h, 0), "f0260703020000000000000003000000");
}

#[test]
fn resuming_clears_the_initial_bit_keeping_remote_start() {
    let h = harness();
    let mut p = cotton();
    p[0] = 3; // paused
    h.feed_hex(&status(p));
    h.set_property("start", "");
    let sent = inner_of(&h, 0);
    assert_eq!(&sent[..4], "f026");
    assert_eq!(&sent[4 + 20..4 + 22], "01"); // byte 10
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
fn a_course_this_dial_does_not_have_is_not_started() {
    let h = harness();
    h.set_property("course_select", "refresh"); // code 1: on the family table, not on this dial
    h.set_property("course_select", "nonsense");
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").is_some());
}

#[test]
fn a_chosen_course_is_what_start_asks_for() {
    let h = harness();
    h.feed_hex(RUNNING_1);
    h.set_property("course_select", "towels");
    h.set_property("start", "");
    // Towels: dry level 0, Eco Hybrid Normal
    assert_eq!(inner_of(&h, 0), "f0260200020000000000000003000000");
}

#[test]
fn a_refusal_is_reported_and_an_acceptance_is_not() {
    let h = harness();
    h.set_property("start", ""); // nothing selected yet
    assert!(h.event("reject").is_some());
    let h = harness();
    h.feed_hex(RUNNING_1);
    h.set_property("start", "");
    h.feed_hex(&framed(&[0x30, 0x00, 0x26, 0x00]));
    assert_eq!(h.event("reject"), None);
    h.feed_hex(&framed(&[0x30, 0x00, 0x26, 0xff]));
    assert!(h.event("reject").unwrap().contains("refused"));
}

#[test]
fn the_descriptor_is_a_dryer_with_conditional_controls() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, _) = h.raw_publish("il/harness").expect("descriptor published");
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["kind"], "dryer");
    assert_eq!(d["props"]["start"]["requires"], "remote_start");
    assert_eq!(d["props"]["energy"]["unit"], "Wh");
    rusthinq_devices::scripting::set_il_prefix(None);
}

#[test]
fn the_host_holds_start_back_until_remote_start_is_armed() {
    let h = harness();
    let mut disarmed = cotton();
    disarmed[15] = 0;
    h.start().feed_hex(&status(disarmed));
    h.set_property("start", "");
    h.set_property("pause", "");
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").unwrap().contains("requires remote_start"));
    h.feed_hex(&status(cotton())); // armed
    h.set_property("pause", "");
    assert_eq!(h.sent_raw().len(), 1);
}
