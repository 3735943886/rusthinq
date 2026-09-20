//! The DHUM_056905_WW Rhai driver, run against frames captured from a real appliance.
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;
use rusthinq_util::{hex, tlv};

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../scripts/DHUM_056905_WW.rhai"
);
const CAPS: &str = "000004000000A70201000AB6A00A7CB541B5A004023220";
const STATE: &str = "000004000000a702041a5a7dc07e50117e8294d0287f503e86c087008980c900d8008780cd902c8840ce80ab00a88187c1e801ee408c808cc0b5d011b600b642b5d012b600b642b5d013b600b642b5d014b600b642b5d015b600b642b5d016b600b642fa80bad3";

fn harness() -> ScriptHarness {
    ScriptHarness::t2(SCRIPT, "DHUM_056905_WW")
}

fn query(kind: u32) -> Vec<u8> {
    tlv::frame_build(&[1, 1, 2, 2, 1], &[tlv::Tlv::new(0x1f5, kind)]).unwrap()
}

/// The `(tag, value)` pairs of a frame the script sent.
fn sent_tlvs(frame: &[u8]) -> Vec<(u16, u32)> {
    tlv::parse(&frame[11..frame.len() - 2])
        .into_iter()
        .map(|t| (t.t, t.v))
        .collect()
}

/// A harness that has been through the whole handshake and holds the captured state.
fn running() -> ScriptHarness {
    let h = harness();
    h.start().feed_hex(CAPS).feed_hex(STATE);
    h
}

#[test]
fn start_asks_for_capabilities_and_arms_a_retry() {
    let h = harness();
    h.start();
    assert_eq!(h.sent_raw(), vec![query(1)]);
    assert_eq!(h.pending_timers(), vec![("caps_retry".to_string(), 15000)]);
}

#[test]
fn a_lost_capabilities_reply_is_asked_for_again() {
    let h = harness();
    h.start().fire_timer("caps_retry");
    assert_eq!(h.sent_raw(), vec![query(1), query(1)]);
    assert_eq!(h.pending_timers(), vec![("caps_retry".to_string(), 15000)]);
}

#[test]
fn the_capabilities_reply_moves_on_to_the_values_query() {
    let h = harness();
    h.start().feed_hex(CAPS);
    assert_eq!(h.sent_raw(), vec![query(1), query(2)]);
    assert_eq!(
        h.pending_timers(),
        vec![("values_retry".to_string(), 15000)]
    );
}

#[test]
fn the_captured_state_becomes_il_properties() {
    let h = running();
    for (prop, want) in [
        ("available", "true"),
        ("power", "false"),
        ("mode", "smart"),
        ("fan", "low"),
        ("target", "40"),
        ("humidity", "44"),
        ("temperature", "31"),
        ("sterilize", "false"),
        ("uvnano", "true"),
        ("bucket_light", "false"),
        ("off_timer", "0"),
        ("error", "0"),
    ] {
        assert_eq!(h.property(prop).as_deref(), Some(want), "{prop}");
    }
}

#[test]
fn the_first_values_frame_masks_the_noisy_tag_and_starts_the_refresh() {
    let h = running();
    let clips = h.sent_clip();
    assert_eq!(clips.len(), 1);
    assert_eq!(clips[0].0, "setMaskingInfo");
    assert_eq!(clips[0].2["blacklist_tlv"], "1200");
    assert_eq!(h.pending_timers(), vec![("refresh".to_string(), 900000)]);
    // a second values frame must not repeat the one-off masking
    h.feed_hex(STATE);
    assert_eq!(h.sent_clip().len(), 1);
}

#[test]
fn the_refresh_timer_requeries_and_rearms() {
    let h = running();
    let before = h.sent_raw().len();
    h.fire_timer("refresh");
    assert_eq!(h.sent_raw()[before..], [query(2)]);
    assert_eq!(h.pending_timers(), vec![("refresh".to_string(), 900000)]);
}

#[test]
fn target_humidity_write_attaches_power_and_mode() {
    let h = running();
    let before = h.sent_raw().len();
    h.set_property("target", "45");
    let sent = h.sent_raw();
    assert_eq!(
        sent_tlvs(&sent[before]),
        vec![(0x253, 45), (0x1f7, 1), (0x1f9, 17)]
    );
}

#[test]
fn target_humidity_is_clamped_to_the_supported_range() {
    let h = running();
    let before = h.sent_raw().len();
    h.set_property("target", "10");
    assert_eq!(sent_tlvs(&h.sent_raw()[before])[0], (0x253, 30));
}

#[test]
fn power_off_writes_only_the_power_tag() {
    let h = running();
    let before = h.sent_raw().len();
    h.set_property("power", "false");
    assert_eq!(sent_tlvs(&h.sent_raw()[before]), vec![(0x1f7, 0)]);
}

#[test]
fn power_on_attaches_the_current_mode() {
    let h = running();
    let before = h.sent_raw().len();
    h.set_property("power", "true");
    assert_eq!(
        sent_tlvs(&h.sent_raw()[before]),
        vec![(0x1f7, 1), (0x1f9, 17)]
    );
}

#[test]
fn selecting_silent_powers_on_and_drops_the_fan_to_low() {
    let h = running();
    h.feed_hex(&notify(&[(0x1fa, 6)]));
    assert_eq!(h.property("fan").as_deref(), Some("high"));
    let before = h.sent_raw().len();
    h.set_property("mode", "silent");
    assert_eq!(h.property("fan").as_deref(), Some("low"));
    assert_eq!(
        sent_tlvs(&h.sent_raw()[before]),
        vec![(0x1f9, 19), (0x1f7, 1)]
    );
}

#[test]
fn a_fan_write_carries_the_per_mode_capability_table() {
    let h = running();
    let before = h.sent_raw().len();
    h.set_property("fan", "high");
    let tlvs = sent_tlvs(&h.sent_raw()[before]);
    assert_eq!(tlvs[0], (0x1fa, 6));
    let rows: Vec<_> = tlvs[1..].chunks(3).map(|c| (c[0].1, c[2].1)).collect();
    // laundry (21) stays at high whatever was asked
    assert_eq!(rows, vec![(17, 6), (18, 6), (20, 6), (21, 6), (22, 6)]);
    h.set_property("fan", "low");
    let low = sent_tlvs(h.sent_raw().last().unwrap());
    let rows: Vec<_> = low[1..].chunks(3).map(|c| (c[0].1, c[2].1)).collect();
    assert_eq!(rows, vec![(17, 2), (18, 2), (20, 2), (21, 6), (22, 2)]);
}

#[test]
fn a_bad_write_is_reported_and_sends_nothing() {
    let h = running();
    let before = h.sent_raw().len();
    h.set_property("mode", "turbo");
    h.set_property("humidity", "50"); // read only
    assert_eq!(h.sent_raw().len(), before);
    assert!(h.event("reject").is_some());
}

#[test]
fn the_tank_states_map_to_full_and_not_full() {
    let h = running();
    h.feed_hex(&notify(&[(0x186, 1)]));
    assert_eq!(h.property("tank_full").as_deref(), Some("true"));
    assert_eq!(h.property("tank_state").as_deref(), Some("full_stopped"));
    h.feed_hex(&notify(&[(0x186, 2)]));
    assert_eq!(h.property("tank_state").as_deref(), Some("full_fan_mode"));
    h.feed_hex(&notify(&[(0x186, 0)]));
    assert_eq!(h.property("tank_full").as_deref(), Some("false"));
}

#[test]
fn dropping_the_device_marks_it_unavailable() {
    let h = running();
    h.drop_device();
    assert_eq!(h.property("available").as_deref(), Some("false"));
}

#[test]
fn the_descriptor_is_published_with_the_host_supplied_binding() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    let h = harness();
    h.start();
    let (payload, retained) = h.raw_publish("il/harness").expect("descriptor published");
    assert!(retained);
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["id"], "harness");
    assert_eq!(d["kind"], "humidifier");
    assert_eq!(d["props"]["target"]["role"], "target_humidity");
    assert_eq!(d["props"]["uvnano"]["rw"], true);
    assert_eq!(d["props"]["fan"]["role"], "fan_speed");
    assert!(d["x-mqtt"]["state"].as_str().unwrap().ends_with("/{prop}"));
    assert!(
        d["x-mqtt"]["reject"]
            .as_str()
            .unwrap()
            .ends_with("/{id}/reject")
    );
    rusthinq_devices::scripting::set_il_prefix(None);
}

/// A state notify carrying only the given tags, in this appliance's 0xa7 envelope.
fn notify(tags: &[(u16, u32)]) -> String {
    let elements: Vec<_> = tags.iter().map(|&(t, v)| tlv::Tlv::new(t, v)).collect();
    let body = tlv::build(&elements);
    let mut frame = vec![0, 0, 0x04, 0, 0, 0, 0xa7, 0x02, 0x04, 0, body.len() as u8];
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&[0, 0]);
    hex::encode(&frame)
}
