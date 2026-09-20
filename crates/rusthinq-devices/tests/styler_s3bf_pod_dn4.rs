//! The S3BF_POD_DN4 Rhai driver. The status frame is a real capture from the cabinet on r5c
//! (idle: off, 715 Wh from the last cycle, the "Trousers" download course, all of which the
//! rethink adapter had retained for it at that moment); the start frame is the one that
//! really made this cabinet run, as the LG app sent it (from rethink's test suite).
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../scripts/S3BF_POD_DN4.rhai"
);

const IDLE: &str = "aa4231ec001c000200020000003700000000000020000002cb000000000142420000001c000200020000003700000000000020000002cb000000000142420000cebb";
// The whole 46-byte recipe, not just a course code.
const FINE_DUST: &str =
    "1e010004000000000082000000000000000000000005c80000000000000000000003000002c80001c80028c80000";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "S3BF_POD_DN4")
}

fn expect(h: &ScriptHarness, pairs: &[(&str, &str)]) {
    for (prop, want) in pairs {
        assert_eq!(h.property(prop).as_deref(), Some(*want), "{prop}");
    }
}

fn framed(inner: &[u8]) -> String {
    rusthinq_util::hex::encode(rusthinq_devices::wrap_aabb(inner))
}

/// A 0xEC frame whose previous and current record are the same 28-byte .
fn status(payload: [u8; 28]) -> String {
    let mut inner = vec![0x31, 0xec];
    for _ in 0..2 {
        inner.extend_from_slice(&[0x00, 28]);
        inner.extend_from_slice(&payload);
    }
    framed(&inner)
}

fn inner_of(h: &ScriptHarness, index: usize) -> Vec<u8> {
    let sent = h.sent_raw();
    sent[index][2..sent[index].len() - 2].to_vec()
}

#[test]
fn a_real_idle_frame_becomes_il_properties() {
    let h = harness();
    h.feed_hex(IDLE);
    expect(
        &h,
        &[
            ("available", "true"),
            ("power", "false"),
            ("status", "off"),
            ("course", "none"),
            ("smart_course", "none"),
            ("download_course", "trousers"),
            ("remaining_time", "0"),
            ("initial_time", "0"),
            ("reserve_time", "0"),
            ("energy", "715"),
            ("error", "false"),
            ("error_message", "OK"),
            ("child_lock", "false"),
            ("night_dry", "false"),
            ("remote_start", "false"),
        ],
    );
}

fn drying() -> [u8; 28] {
    let mut p = [0u8; 28];
    p[0] = 55; // drying, the state a real Indoor Dry run reported
    p[1] = 1; // 1h 53m left
    p[2] = 53;
    p[3] = 2;
    p[5] = 30; // Fine Dust
    p[14] = 0x08 | 0x20; // remote start, plus the bit set on every record
    p[17] = 0x02;
    p[18] = 0xcb;
    p
}

#[test]
fn a_running_cycle_reports_its_times_and_the_remote_start_flag() {
    let h = harness();
    h.feed_hex(&status(drying()));
    expect(
        &h,
        &[
            ("status", "drying"),
            ("power", "true"),
            ("course", "fine_dust"),
            ("remaining_time", "113"),
            ("initial_time", "120"),
            ("remote_start", "true"),
        ],
    );
}

#[test]
fn the_child_lock_and_night_dry_bits_and_an_error_decode() {
    let h = harness();
    let mut p = drying();
    p[14] |= 0x01 | 0x02;
    p[6] = 31;
    h.feed_hex(&status(p));
    expect(
        &h,
        &[
            ("child_lock", "true"),
            ("night_dry", "true"),
            ("error", "true"),
            (
                "error_message",
                "Door closed - this course needs the door open",
            ),
        ],
    );
}

// ---- commands ----------------------------------------------------------------------

#[test]
fn fine_dust_start_as_the_app_sent_it() {
    let h = harness();
    let mut p = drying();
    p[0] = 0;
    p[5] = 30;
    h.feed_hex(&status(p)); // the dial is on Fine Dust
    h.set_property("start", "");
    let sent = inner_of(&h, 0);
    assert_eq!(&sent[..2], &[0xf0, 0x26]);
    assert_eq!(rusthinq_util::hex::encode(&sent[2..]), FINE_DUST);
}

#[test]
fn every_startable_course_fills_the_frame_and_marks_the_first_duration() {
    for (name, code) in [
        ("standard", 1u8),
        ("quick", 3),
        ("heavy", 5),
        ("wool_knitwear", 6),
        ("suits_coats", 7),
        ("sportswear", 8),
        ("sanitary_standard", 11),
        ("bedding", 12),
        ("drying_normal", 15),
        ("rain_snow", 22),
        ("padding_care", 28),
        ("fine_dust", 30),
        ("virus_care", 31),
        ("jeans", 32),
        ("fur_leather", 33),
        ("static_removal", 34),
    ] {
        let h = harness();
        h.set_property("course_select", name);
        h.set_property("start", "");
        let payload = &inner_of(&h, 0)[2..];
        assert_eq!(payload.len(), 46, "{name}");
        assert_eq!(payload[0], code, "{name}");
        assert_eq!(
            payload[3], 0x04,
            "{name}: initial bit, this is a start not a resume"
        );
        assert_eq!(payload[9] & 0x80, 0x80, "{name}");
    }
}

#[test]
fn resume_is_the_shorter_frame_empty_apart_from_the_course() {
    let h = harness();
    let mut p = drying();
    p[0] = 3; // paused
    p[5] = 30;
    h.feed_hex(&status(p));
    h.set_property("start", "");
    let payload = &inner_of(&h, 0)[2..];
    assert_eq!(payload.len(), 45);
    assert_eq!(payload[0], 30);
    assert!(payload[1..].iter().all(|&b| b == 0));
}

#[test]
fn a_course_the_app_will_not_start_remotely_either_is_not_started() {
    let h = harness();
    h.set_property("course_select", "indoor_dry_120"); // dries the room: needs the door open
    h.set_property("course_select", "nonsense");
    h.set_property("start", ""); // nothing valid selected
    assert!(h.sent_raw().is_empty());
    assert!(h.event("reject").is_some());
}

#[test]
fn pause_and_power_off_are_the_short_controls_and_there_is_no_power_on() {
    let h = harness();
    h.set_property("power_off", "");
    h.set_property("pause", "");
    assert_eq!(rusthinq_util::hex::encode(inner_of(&h, 0)), "f024010100");
    assert_eq!(rusthinq_util::hex::encode(inner_of(&h, 1)), "f024040100");
    // the cabinet acknowledges a power-on and then does nothing, so none is offered
    h.set_property("power", "true");
    h.set_property("power_on", "");
    assert_eq!(h.sent_raw().len(), 2);
}

#[test]
fn a_refusal_is_reported_against_the_command_that_caused_it() {
    let h = harness();
    h.set_property("pause", "");
    h.feed_hex(&framed(&[0x31, 0x00, 0x24, 0xff]));
    let reject = h.event("reject").expect("a rejection");
    assert!(
        reject.contains("pause") && reject.contains("refused"),
        "{reject}"
    );
}

#[test]
fn a_record_of_another_length_reports_nothing() {
    let h = harness();
    let mut inner = vec![0x31, 0xeb, 0x00, 27];
    inner.extend_from_slice(&[0u8; 27]);
    h.feed_hex(&framed(&inner));
    assert_eq!(h.property("status"), None);
}

#[test]
fn the_descriptor_lists_the_courses_start_can_express() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, _) = h.raw_publish("il/harness").expect("descriptor published");
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["class"], "styler");
    let options = d["props"]["course_select"]["options"].as_array().unwrap();
    assert_eq!(options.len(), 16);
    assert!(!options.iter().any(|o| o == "indoor_dry_120"));
    assert_eq!(d["props"]["start"]["requires"], "remote_start");
    rusthinq_devices::scripting::set_il_prefix(None);
}
