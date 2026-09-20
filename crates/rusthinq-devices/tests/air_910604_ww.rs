//! The AIR_910604_WW Rhai driver, run against frames captured from a real appliance.
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;
use rusthinq_util::tlv;

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../scripts/AIR_910604_WW.rhai"
);
// Captured from the appliance on r5c while it was off, in auto mode.
const CAPS: &str = "000004000000a702012650b00cb07001e000b0a001d4b3200800b340b4d080b584b540b6a032f8b6e02115b700b7509fbc50c1b3c0b42001d4b441bd206004bd501fb5d010b600b648b5cdb600b648b5cfb600b648b5ceb600b6485437";
const VALUES: &str = "000004000000a7020429647dc07e50107e887f007f807f5050868086c087008980d7c0d801d840d88087809380cd4acd08ccc890419001cd901e8840d5600e7bd5a00fa0d8e00e56d9200fa0ce80ab00c988c9c0ca00b5d010b600b648b5cdb600b648b5cfb600b648b5ceb60eb64eebf9";

fn running() -> ScriptHarness {
    let h = ScriptHarness::t2(SCRIPT, "AIR_910604_WW");
    h.start().feed_hex(CAPS).feed_hex(VALUES);
    h
}

/// The `(tag, value)` pairs of a frame the script sent.
fn sent_tlvs(frame: &[u8]) -> Vec<(u16, u32)> {
    tlv::parse(&frame[11..frame.len() - 2])
        .into_iter()
        .map(|t| (t.t, t.v))
        .collect()
}

fn last_sent(h: &ScriptHarness) -> Vec<(u16, u32)> {
    sent_tlvs(h.sent_raw().last().expect("a frame was sent"))
}

#[test]
fn the_captured_state_becomes_il_properties() {
    let h = running();
    for (prop, want) in [
        ("available", "true"),
        ("power", "false"),
        ("mode", "auto"),
        ("fan", "auto"),
        ("sterilize", "true"),
        ("light", "false"),
        ("pm1", "8"),
        ("pm25", "8"),
        ("pm10", "10"),
        ("air_quality", "1"),
        ("odor", "1"),
        ("filter_life", "93"),     // 3707 of 4000 hours left
        ("top_filter_life", "92"), // 3670 of 4000
        ("sleep_timer", "0"),
        ("error", "0"),
    ] {
        assert_eq!(h.property(prop).as_deref(), Some(want), "{prop}");
    }
}

#[test]
fn temperature_and_humidity_are_not_exposed() {
    let h = running();
    assert_eq!(h.property("temperature"), None);
    assert_eq!(h.property("humidity"), None);
}

#[test]
fn power_on_restores_mode_and_fan_in_the_same_frame() {
    let h = running();
    h.set_property("power", "true");
    assert_eq!(last_sent(&h), vec![(0x1f7, 1), (0x1f9, 16), (0x1fa, 8)]);
}

#[test]
fn power_off_sends_only_the_power_tag() {
    let h = running();
    h.set_property("power", "false");
    assert_eq!(last_sent(&h), vec![(0x1f7, 0)]);
}

#[test]
fn a_fan_write_forces_power_on_and_carries_the_mode() {
    let h = running();
    h.set_property("fan", "high");
    assert_eq!(last_sent(&h), vec![(0x1fa, 6), (0x1f7, 1), (0x1f9, 16)]);
}

#[test]
fn a_mode_write_forces_power_on_and_carries_the_fan() {
    let h = running();
    h.set_property("mode", "baby_care");
    assert_eq!(last_sent(&h), vec![(0x1f9, 14), (0x1f7, 1), (0x1fa, 8)]);
}

#[test]
fn light_sterilize_and_sleep_timer_are_bare_writes() {
    let h = running();
    h.set_property("light", "true");
    assert_eq!(last_sent(&h), vec![(0x24e, 1)]);
    h.set_property("sterilize", "false");
    assert_eq!(last_sent(&h), vec![(0x360, 0)]);
    h.set_property("sleep_timer", "60");
    assert_eq!(last_sent(&h), vec![(0x21a, 60)]);
}

#[test]
fn bad_writes_are_rejected_and_send_nothing() {
    let h = running();
    let before = h.sent_raw().len();
    h.set_property("fan", "warp");
    h.set_property("mode", "turbo");
    h.set_property("sleep_timer", "9999");
    h.set_property("pm25", "1"); // read only
    assert_eq!(h.sent_raw().len(), before);
    assert!(h.event("reject").is_some());
}

#[test]
fn filter_life_recomputes_when_only_the_hours_left_arrive() {
    let h = running();
    // a later notify carries just the bottom filter's hours left: 3600 of 4000
    let body = tlv::build(&[tlv::Tlv::new(0x355, 3600)]);
    let mut frame = vec![0, 0, 0x04, 0, 0, 0, 0xa7, 0x02, 0x04, 0, body.len() as u8];
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&[0, 0]);
    h.feed_hex(&rusthinq_util::hex::encode(&frame));
    assert_eq!(h.property("filter_life").as_deref(), Some("90"));
}

#[test]
fn the_descriptor_is_published_with_roles() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = ScriptHarness::t2(SCRIPT, "AIR_910604_WW");
    h.start();
    let (payload, retained) = h.raw_publish("il/harness").expect("descriptor published");
    assert!(retained);
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["kind"], "fan");
    assert_eq!(d["class"], "air_purifier");
    assert_eq!(d["props"]["fan"]["role"], "fan_speed");
    assert_eq!(d["props"]["mode"]["role"], "mode");
    rusthinq_devices::scripting::set_il_prefix(None);
}

#[test]
fn the_handshake_is_the_shared_one() {
    let h = ScriptHarness::t2(SCRIPT, "AIR_910604_WW");
    h.start();
    assert_eq!(h.pending_timers(), vec![("caps_retry".to_string(), 15000)]);
    h.feed_hex(CAPS);
    assert_eq!(
        h.pending_timers(),
        vec![("values_retry".to_string(), 15000)]
    );
    h.feed_hex(VALUES);
    assert_eq!(h.pending_timers(), vec![("refresh".to_string(), 900000)]);
}
