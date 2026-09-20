// Tests of DHUM_056905_WW.rhai, against frames captured from a real appliance.

import "tlv_test" as t;

fn caps() { "000004000000A70201000AB6A00A7CB541B5A004023220" }
fn state() { "000004000000a702041a5a7dc07e50117e8294d0287f503e86c087008980c900d8008780cd902c8840ce80ab00a88187c1e801ee408c808cc0b5d011b600b642b5d012b600b642b5d013b600b642b5d014b600b642b5d015b600b642b5d016b600b642fa80bad3" }

fn running() { t::running(device(), caps(), state()) }

fn test_start_asks_for_capabilities_and_arms_a_retry() {
    let d = device();
    d.start();
    expect_eq(d.sent(), [t::query(1)]);
    expect_eq(d.timers(), [["caps_retry", 15000]]);
}

fn test_a_lost_capabilities_reply_is_asked_for_again() {
    let d = device();
    d.start();
    d.fire("caps_retry");
    expect_eq(d.sent(), [t::query(1), t::query(1)]);
    expect_eq(d.timers(), [["caps_retry", 15000]]);
}

fn test_the_capabilities_reply_moves_on_to_the_values_query() {
    let d = device();
    d.start();
    d.feed(caps());
    expect_eq(d.sent(), [t::query(1), t::query(2)]);
    expect_eq(d.timers(), [["values_retry", 15000]]);
}

fn test_the_captured_state_becomes_il_properties() {
    expect_props(running(), #{
        available: "true", power: "false", mode: "smart", fan: "low", target: "40",
        humidity: "44", temperature: "31", sterilize: "false", uvnano: "true",
        bucket_light: "false", off_timer: "0", error: "0"
    });
}

fn test_the_first_values_frame_masks_the_noisy_tag_and_starts_the_refresh() {
    let d = running();
    let clips = d.clips();
    expect_eq(clips.len(), 1);
    expect_eq(clips[0].cmd, "setMaskingInfo");
    expect_eq(clips[0].data.blacklist_tlv, "1200");
    expect_eq(d.timers(), [["refresh", 900000]]);
    d.feed(state()); // a second values frame must not repeat the one-off masking
    expect_eq(d.clips().len(), 1);
}

fn test_the_refresh_timer_requeries_and_rearms() {
    let d = running();
    let n = d.sent().len();
    d.fire("refresh");
    expect_eq(d.sent().extract(n), [t::query(2)]);
    expect_eq(d.timers(), [["refresh", 900000]]);
}

fn test_target_humidity_write_attaches_power_and_mode() {
    let d = running();
    let n = d.sent().len();
    d.set("target", "45");
    expect_eq(t::sent_since(d, n), [[[0x253, 45], [0x1f7, 1], [0x1f9, 17]]]);
}

fn test_a_target_outside_the_descriptors_range_is_rejected_by_the_host_not_clamped() {
    let d = running();
    let n = d.sent().len();
    d.set("target", "10");
    d.set("target", "75");
    d.set("target", "45.5"); // the descriptor's step is 1
    expect_eq(d.sent().len(), n, "nothing reaches the appliance");
    expect(d.event("reject").contains("\"prop\":\"target\""), "a reject event names the property");
}

fn test_the_script_receives_the_canonical_form_of_a_valid_command() {
    let d = running();
    let n = d.sent().len();
    d.set("power", "ON"); // the descriptor says binary: the script sees "true"
    d.set("target", "45.0");
    let sent = t::sent_since(d, n);
    expect_eq(sent[0], [[0x1f7, 1], [0x1f9, 17]]);
    expect_eq(sent[1][0], [0x253, 45]);
}

fn test_power_off_writes_only_the_power_tag() {
    let d = running();
    let n = d.sent().len();
    d.set("power", "false");
    expect_eq(t::sent_since(d, n), [[[0x1f7, 0]]]);
}

fn test_power_on_attaches_the_current_mode() {
    let d = running();
    let n = d.sent().len();
    d.set("power", "true");
    expect_eq(t::sent_since(d, n), [[[0x1f7, 1], [0x1f9, 17]]]);
}

fn test_selecting_silent_powers_on_and_drops_the_fan_to_low() {
    let d = running();
    d.feed(t::notify([[0x1fa, 6]]));
    expect_eq(d.property("fan"), "high");
    let n = d.sent().len();
    d.set("mode", "silent");
    expect_eq(d.property("fan"), "low");
    expect_eq(t::sent_since(d, n), [[[0x1f9, 19], [0x1f7, 1]]]);
}

fn fan_rows(tlvs) {
    // the per-mode capability table: rows of (mode, 0, speed)
    let rows = [];
    let i = 1;
    while i + 2 < tlvs.len() {
        rows.push([tlvs[i][1], tlvs[i + 2][1]]);
        i += 3;
    }
    rows
}

fn test_a_fan_write_carries_the_per_mode_capability_table() {
    let d = running();
    let n = d.sent().len();
    d.set("fan", "high");
    let tlvs = d.sent_tlvs(n);
    expect_eq(tlvs[0], [0x1fa, 6]);
    // laundry (21) stays at high whatever was asked
    expect_eq(fan_rows(tlvs), [[17, 6], [18, 6], [20, 6], [21, 6], [22, 6]]);
    d.set("fan", "low");
    let low = d.sent_tlvs(d.sent().len() - 1);
    expect_eq(fan_rows(low), [[17, 2], [18, 2], [20, 2], [21, 6], [22, 2]]);
}

fn test_a_bad_write_is_reported_and_sends_nothing() {
    let d = running();
    let n = d.sent().len();
    d.set("mode", "turbo");
    d.set("humidity", "50"); // read only
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_the_tank_states_map_to_full_and_not_full() {
    let d = running();
    d.feed(t::notify([[0x186, 1]]));
    expect_props(d, #{ tank_full: "true", tank_state: "full_stopped" });
    d.feed(t::notify([[0x186, 2]]));
    expect_eq(d.property("tank_state"), "full_fan_mode");
    d.feed(t::notify([[0x186, 0]]));
    expect_eq(d.property("tank_full"), "false");
}

fn test_dropping_the_device_marks_it_unavailable() {
    let d = running();
    d.drop_device();
    expect_eq(d.property("available"), "false");
}
