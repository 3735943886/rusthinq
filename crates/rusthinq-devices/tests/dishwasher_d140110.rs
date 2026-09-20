//! The D140110 Rhai driver, run against frames captured from a real appliance over one full
//! 1:33 Auto cycle (the captures come from rethink's test suite for this model).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

const SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/D140110.rhai");

// Frame: AA 0x3A 32 EC <26 bytes previous record> <26 bytes current record> <cksum> BB, where a
// record is <flags> 18 <24 bytes of payload>.
const READY: &str = "AA3A32EC0018000000012100000001000012000000000000000000000000081801000001210100012100001000020009000000000000000184BB";
const RUNNING: &str = "AA3A32EC00180100000121010001210000100002000900000000000000010018020200012101000121000010000200090000000000000001ACBB";
const CHILD_LOCK: &str = "AA3A32EC001802020001210100011D000010000200090000000000000001001802020001210100011D000011000200090000000000000001A0BB";
const RINSING: &str = "AA3A32EC001802030001210100000A000010000200090000000000000001001802030001210100000900001000020009000000000000000198BB";
const DRYING: &str = "AA3A32EC0018020300012101000009000010000200090000000000000001001802040001210100000900001000020009000000000000000198BB";
const DOOR_OPEN: &str = "AA3A32EC0018020400012101000002000010000200090000000000000001001802040001210100000200001200020009000000000000000197BB";
const FINISHED: &str = "AA3A32EC0018020400012101000001000012000200090000000000000001001805050001210100000100001200020009000000000000000193BB";
const STANDBY: &str = "AA3A32EC001805050001210100000100001200020009000000000000000108180400000121000000010000120002000900000000000000019EBB";
const OFF: &str = "AA3A32EC00180400000121000000010000120002000900000000000000010018000000012100000001000012000200090000000000000001EDBB";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "D140110")
}

fn expect(h: &ScriptHarness, pairs: &[(&str, &str)]) {
    for (prop, want) in pairs {
        assert_eq!(h.property(prop).as_deref(), Some(*want), "{prop}");
    }
}

#[test]
fn ready_to_start_the_course_and_the_full_cycle_time() {
    let h = harness();
    h.feed_hex(READY);
    expect(
        &h,
        &[
            ("available", "true"),
            ("status", "ready"),
            ("process", "none"),
            ("course", "auto"),
            ("error", "none"),
            ("running", "false"),
            ("total_time", "93"), // 1:33, the 93 minutes the app showed
            ("remaining_time", "93"),
            ("reserve_time", "0"),
            ("door", "false"),
            ("child_lock", "false"),
            ("chime", "true"),
            ("rinse_refill", "false"),
            ("salt_refill", "false"),
            ("rinse_aid_level", "2"),
            ("softening_level", "0"),
        ],
    );
}

#[test]
fn running() {
    let h = harness();
    h.feed_hex(RUNNING);
    expect(
        &h,
        &[
            ("status", "running"),
            ("process", "washing"),
            ("running", "true"),
        ],
    );
}

#[test]
fn the_phases_of_a_cycle_are_reported_apart_from_the_state() {
    let h = harness();
    h.feed_hex(RINSING);
    expect(
        &h,
        &[
            ("process", "rinsing"),
            ("status", "running"),
            ("remaining_time", "9"),
        ],
    );
    h.feed_hex(DRYING);
    expect(&h, &[("process", "drying"), ("status", "running")]);
}

#[test]
fn the_child_lock_does_not_disturb_anything_else() {
    let h = harness();
    h.feed_hex(RUNNING).feed_hex(CHILD_LOCK);
    expect(
        &h,
        &[
            ("child_lock", "true"),
            ("status", "running"),
            ("chime", "true"), // the same byte, another bit
            ("door", "false"),
            ("remaining_time", "89"), // 1:29
        ],
    );
}

#[test]
fn the_auto_open_door_is_reported_without_ending_the_cycle() {
    let h = harness();
    h.feed_hex(DRYING).feed_hex(DOOR_OPEN);
    expect(
        &h,
        &[
            ("door", "true"),
            ("status", "running"),
            ("process", "drying"),
            ("child_lock", "false"),
        ],
    );
}

#[test]
fn finished_then_standby_then_off() {
    let h = harness();
    h.feed_hex(FINISHED);
    expect(&h, &[("status", "finished"), ("running", "false")]);
    h.feed_hex(STANDBY);
    expect(&h, &[("status", "standby"), ("course", "none")]);
    h.feed_hex(OFF);
    expect(&h, &[("status", "off")]);
}

#[test]
fn other_frames_and_malformed_ones_are_ignored() {
    let h = harness();
    h.feed_hex(READY);
    for frame in [
        "AA0A32B2000000BB", // 0xB2, not decoded
        "001122",           // not an AABB frame
        "AA0632EC0000BB",   // 0x32EC but far too short
        // a record whose payload length byte is not 0x18
        "AA3A32EC0018000000012100000001000012000000000000000000000000\
0819010000012101000121000010000200090000000000000001 84BB",
    ] {
        h.feed_hex(frame);
    }
    expect(&h, &[("status", "ready")]);
}

#[test]
fn it_is_read_only() {
    let h = harness();
    h.set_property("running", "true");
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").is_some());
}

#[test]
fn dropping_the_device_marks_it_unavailable() {
    let h = harness();
    h.feed_hex(READY).drop_device();
    assert_eq!(h.property("available").as_deref(), Some("false"));
}

#[test]
fn the_descriptor_is_published() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, _) = h.raw_publish("il/harness").expect("descriptor published");
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["class"], "dishwasher");
    assert_eq!(d["props"]["remaining_time"]["unit"], "min");
    rusthinq_devices::scripting::set_il_prefix(None);
}
