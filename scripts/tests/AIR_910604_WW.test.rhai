// Tests of AIR_910604_WW.rhai, against frames captured from a real appliance.

import "tlv_test" as t;

fn caps() { "000004000000a702012650b00cb07001e000b0a001d4b3200800b340b4d080b584b540b6a032f8b6e02115b700b7509fbc50c1b3c0b42001d4b441bd206004bd501fb5d010b600b648b5cdb600b648b5cfb600b648b5ceb600b6485437" }
fn values() { "000004000000a7020429647dc07e50107e887f007f807f5050868086c087008980d7c0d801d840d88087809380cd4acd08ccc890419001cd901e8840d5600e7bd5a00fa0d8e00e56d9200fa0ce80ab00c988c9c0ca00b5d010b600b648b5cdb600b648b5cfb600b648b5ceb60eb64eebf9" }

fn running() { t::running(device(), caps(), values()) }

fn test_the_captured_state_becomes_il_properties() {
    expect_props(running(), #{
        available: "true", power: "false", mode: "auto", fan: "auto", sterilize: "true",
        light: "false", pm1: "8", pm25: "8", pm10: "10", air_quality: "1", odor: "1",
        filter_life: "93",      // 3707 of 4000 hours left
        top_filter_life: "92",  // 3670 of 4000
        sleep_timer: "0", error: "0"
    });
}

fn test_temperature_and_humidity_are_not_exposed() {
    let d = running();
    expect_eq(d.property("temperature"), ());
    expect_eq(d.property("humidity"), ());
}

fn test_power_on_restores_mode_and_fan_in_the_same_frame() {
    let d = running();
    d.set("power", "true");
    expect_eq(t::last_sent(d), [[0x1f7, 1], [0x1f9, 16], [0x1fa, 8]]);
}

fn test_power_off_sends_only_the_power_tag() {
    let d = running();
    d.set("power", "false");
    expect_eq(t::last_sent(d), [[0x1f7, 0]]);
}

fn test_a_fan_write_forces_power_on_and_carries_the_mode() {
    let d = running();
    d.set("fan", "high");
    expect_eq(t::last_sent(d), [[0x1fa, 6], [0x1f7, 1], [0x1f9, 16]]);
}

fn test_a_mode_write_forces_power_on_and_carries_the_fan() {
    let d = running();
    d.set("mode", "baby_care");
    expect_eq(t::last_sent(d), [[0x1f9, 14], [0x1f7, 1], [0x1fa, 8]]);
}

fn test_light_sterilize_and_sleep_timer_are_bare_writes() {
    let d = running();
    d.set("light", "true");
    expect_eq(t::last_sent(d), [[0x24e, 1]]);
    d.set("sterilize", "false");
    expect_eq(t::last_sent(d), [[0x360, 0]]);
    d.set("sleep_timer", "60");
    expect_eq(t::last_sent(d), [[0x21a, 60]]);
}

fn test_bad_writes_are_rejected_and_send_nothing() {
    let d = running();
    let n = d.sent().len();
    d.set("fan", "warp");
    d.set("mode", "turbo");
    d.set("sleep_timer", "9999");
    d.set("pm25", "1"); // read only
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_filter_life_recomputes_when_only_the_hours_left_arrive() {
    let d = running();
    // a later notify carries just the bottom filter's hours left: 3600 of 4000
    d.feed(t::notify([[0x355, 3600]]));
    expect_eq(d.property("filter_life"), "90");
}

fn test_the_descriptor_is_an_air_purifier_with_roles() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc.kind, "fan");
    expect_eq(desc["class"], "air_purifier");
    expect_eq(desc.props.fan.role, "fan_speed");
    expect_eq(desc.props.mode.role, "mode");
}

fn test_the_handshake_is_the_shared_one() {
    let d = device();
    d.start();
    expect_eq(d.timers(), [["caps_retry", 15000]]);
    d.feed(caps());
    expect_eq(d.timers(), [["values_retry", 15000]]);
    d.feed(values());
    expect_eq(d.timers(), [["refresh", 900000]]);
}

fn test_the_host_refuses_what_the_descriptor_forbids_before_the_script_sees_it() {
    let d = running();
    let n = d.sent().len();
    d.set("sleep_timer", "9999"); // above the maximum of 720
    d.set("sleep_timer", "65");   // the step is 10
    d.set("fan", "warp");         // not one of the options
    d.set("pm25", "1");           // read only
    d.set("nope", "1");           // not in the descriptor at all
    expect_eq(d.sent().len(), n);
    expect(d.event("reject").contains("unknown property"), "the last refusal is for the unknown property");
}

fn test_a_stray_notification_before_the_values_does_not_end_the_retry() {
    let d = device();
    d.start();
    d.feed(caps());
    d.feed(t::notify([[0x1fa, 6]]));
    expect_eq(d.timers(), [["values_retry", 15000]]);
    expect_eq(d.property("available"), ());
    d.feed(values());
    expect_eq(d.timers(), [["refresh", 900000]]);
    expect_eq(d.property("available"), "true");
}
