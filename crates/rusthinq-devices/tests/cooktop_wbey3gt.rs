//! The WBEY3GT Rhai driver, run against frames captured from a real cooktop, taken a ring at
//! a time (the captures and the command frames the LG app sent come from rethink's test suite
//! for this model).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

const SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/WBEY3GT.rhai");
const BURNERS: [&str; 3] = ["left_rear", "left_front", "right"];

// Frame: AA 0x66 42 EC <48 bytes previous state> <48 bytes current state> <cksum> BB.
const IDLE: &str = "AA6642EC00000000000000000000000109010400013B37000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000095BB";
const POWER_ON_LOCKED: &str = "AA6642EC0100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000030000000000000000030000000000000000030000000000000000030000000000000000000000000000000000001EBB";
const ARMED: &str = "AA6642EC0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000900000001000001000000000000000000000000000000000000000000000000000000001CBB";
const COOKING: &str = "AA6642EC00000000000000000000000009000000010000010000000000000000000000000000000000000000000000000000000000000000000000000000000109010000013B3B00000000000000000000000000000000000000000000000000000000009EBB";
const TWO_BURNERS: &str = "AA6642EC0000000000000000000000010900020000003A000105010000013B3B000000000000000000000000000000000000000000000000000000000000000109010200003B39000105020000013A3B000000000000000000000000000000000000000054BB";
const LOCKED_WHILE_COOKING: &str = "AA6642EC00000000000000000000000109010200003B39000105020000013A3B0000000000000000000000000000000000000000000003000000000000000003091C02000020390003051D0000011F3B000300000000000000000000000000000000000013BB";
const UNLOCKED: &str = "AA6642EC000003000000000000000003091C02000020390003051D0000011F3B000300000000000000000000000000000000000000000000000000000000000109200200001C39000105210000011B3B000000000000000000000000000000000000000013BB";
const REMOTE_START: &str = "AA6642EC00000000000000000000000109240200011839000000000000000000000000000000000000000000000000000000000002000000000000000000000109250200011739000000000000000000000000000000000000000000000000000000000011BB";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "WBEY3GT")
}

fn expect(h: &ScriptHarness, pairs: &[(&str, &str)]) {
    for (prop, want) in pairs {
        assert_eq!(h.property(prop).as_deref(), Some(*want), "{prop}");
    }
}

/// The inner body of the one frame that was sent, upper-case hex, framing stripped.
fn inner(h: &ScriptHarness) -> String {
    let sent = h.sent_raw();
    assert_eq!(sent.len(), 1, "exactly one packet sent");
    rusthinq_util::hex::encode_upper(&sent[0][2..sent[0].len() - 2])
}

#[test]
fn idle_every_burner_off_nothing_locked_no_remote_start() {
    let h = harness();
    h.feed_hex(IDLE);
    expect(
        &h,
        &[
            ("cooking", "false"),
            ("locked", "false"),
            ("remote_start", "false"),
        ],
    );
    for k in BURNERS {
        expect(
            &h,
            &[
                (&format!("{k}_state"), "off"),
                (&format!("{k}_power_level"), "0"),
                (&format!("{k}_cook_time"), "0"),
                (&format!("{k}_remaining_time"), "0"),
            ],
        );
    }
}

#[test]
fn locked_from_cold_the_lock_shows_but_nothing_is_cooking() {
    let h = harness();
    h.feed_hex(POWER_ON_LOCKED);
    expect(&h, &[("locked", "true"), ("cooking", "false")]);
    for k in BURNERS {
        expect(&h, &[(&format!("{k}_state"), "off")]);
    }
}

#[test]
fn power_level_set_before_the_ring_lights() {
    let h = harness();
    h.feed_hex(ARMED);
    expect(
        &h,
        &[
            ("right_power_level", "9"),
            ("right_state", "off"),
            ("cooking", "false"),
            ("right_remaining_time", "60"), // the hour of auto-off is already on the clock
        ],
    );
}

#[test]
fn cooking_state_level_and_both_timers() {
    let h = harness();
    h.feed_hex(COOKING);
    expect(
        &h,
        &[
            ("cooking", "true"),
            ("right_state", "cooking"),
            ("right_power_level", "9"),
            ("right_cook_time", "0"),       // 0:00:01
            ("right_remaining_time", "59"), // 0:59:59
            ("left_rear_state", "off"),
            ("left_front_power_level", "0"),
        ],
    );
}

#[test]
fn two_rings_at_once_keep_separate_counters() {
    let h = harness();
    h.feed_hex(TWO_BURNERS);
    expect(
        &h,
        &[
            ("right_power_level", "9"),
            ("right_cook_time", "2"),
            ("right_remaining_time", "57"),
            ("left_rear_power_level", "5"),
            ("left_rear_cook_time", "0"),
            ("left_rear_remaining_time", "59"),
        ],
    );
}

#[test]
fn locking_mid_cook_does_not_put_the_rings_out() {
    let h = harness();
    h.feed_hex(TWO_BURNERS).feed_hex(LOCKED_WHILE_COOKING);
    expect(
        &h,
        &[
            ("locked", "true"),
            ("cooking", "true"),
            ("right_state", "cooking"),
            ("left_rear_state", "cooking"),
            ("left_front_state", "off"), // not alight: reads off, not locked
            ("right_cook_time", "2"),
            ("right_remaining_time", "57"),
            ("left_rear_remaining_time", "59"),
        ],
    );
}

#[test]
fn unlocking_leaves_the_cook_alone() {
    let h = harness();
    h.feed_hex(LOCKED_WHILE_COOKING).feed_hex(UNLOCKED);
    expect(
        &h,
        &[
            ("locked", "false"),
            ("cooking", "true"),
            ("right_state", "cooking"),
            ("left_rear_state", "cooking"),
        ],
    );
}

#[test]
fn the_panel_granting_remote_start_shows_up() {
    let h = harness();
    h.feed_hex(REMOTE_START);
    expect(&h, &[("remote_start", "true")]);
}

#[test]
fn switched_off_everything_clears() {
    let h = harness();
    h.feed_hex(COOKING).feed_hex(IDLE);
    expect(
        &h,
        &[
            ("cooking", "false"),
            ("right_state", "off"),
            ("right_power_level", "0"),
            ("right_remaining_time", "0"),
        ],
    );
}

// The command frames below are the ones the LG app sent, byte for byte.
#[test]
fn switching_a_burner_off_names_that_burner() {
    let h = harness();
    h.feed_hex(REMOTE_START); // right ring at 9, remote start granted
    h.set_property("right_off", "");
    assert_eq!(inner(&h), "F043200805000000".to_owned() + "00000000");
}

#[test]
fn setting_a_timer_restates_the_burner_power_level() {
    let h = harness();
    h.feed_hex(REMOTE_START);
    h.set_property("right_remaining_time", "56");
    // burner 5, still at level 9, 0h 56m
    assert_eq!(inner(&h), "F043200805090038".to_owned() + "00000000");
}

#[test]
fn a_timer_past_the_appliance_maximum_is_clamped_not_wrapped() {
    let h = harness();
    h.feed_hex(REMOTE_START);
    h.set_property("right_remaining_time", "9999");
    // 11:59, the most ControlTimerHour/Min can carry
    assert_eq!(inner(&h), "F043200805090B3B".to_owned() + "00000000");
}

#[test]
fn switching_the_whole_cooktop_off() {
    let h = harness();
    h.feed_hex(REMOTE_START);
    h.set_property("power_off", "");
    assert_eq!(inner(&h), "F04400");
}

#[test]
fn nothing_is_sent_while_the_panel_withholds_remote_start() {
    let h = harness();
    h.feed_hex(COOKING); // cooking, but remote start not granted
    h.set_property("right_off", "");
    h.set_property("power_off", "");
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").unwrap().contains("remote start"));
}

#[test]
fn nothing_is_sent_before_any_status_has_been_seen() {
    let h = harness();
    h.set_property("power_off", "");
    assert!(h.sent_raw().is_empty());
}

#[test]
fn a_timer_on_a_ring_that_is_not_lit_is_not_sent() {
    let h = harness();
    h.feed_hex(REMOTE_START);
    // the left rear ring is off in that frame, and a level of 0 would mean "switch off"
    h.set_property("left_rear_remaining_time", "30");
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").is_some());
}

#[test]
fn no_command_can_light_a_ring() {
    let h = harness();
    h.feed_hex(REMOTE_START);
    for prop in [
        "right_power_level",
        "left_rear_state",
        "cooking",
        "right_on",
    ] {
        h.set_property(prop, "9");
    }
    assert!(h.sent_raw().is_empty());
}

#[test]
fn other_and_malformed_frames_are_ignored() {
    let h = harness();
    h.feed_hex(COOKING);
    let before = h.property("right_state");
    for frame in [
        "AA12426500000000000000000000000432BB", // 0x65, switch-off, not decoded
        "AA13427207E40A0000000000000000000033BB", // 0x72, not decoded
        "001122",                               // not an AABB frame at all
        "AA0642EC0000BB",                       // 0x42EC but far too short
    ] {
        h.feed_hex(frame);
    }
    assert_eq!(h.property("right_state"), before);
}

#[test]
fn the_descriptor_groups_burner_properties_and_marks_the_controls() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, _) = h.raw_publish("il/harness").expect("descriptor published");
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["kind"], "cooktop");
    assert_eq!(d["props"]["right_state"]["group"], "right");
    assert_eq!(d["props"]["right_off"]["type"], "trigger");
    assert_eq!(
        d["props"]["right_remaining_time"]["requires"],
        "remote_start"
    );
    assert_eq!(d["props"]["power_off"]["requires"], "remote_start");
    rusthinq_devices::scripting::set_il_prefix(None);
}
