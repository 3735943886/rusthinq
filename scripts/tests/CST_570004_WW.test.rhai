// Tests of CST_570004_WW.rhai, against frames captured from a real unit (from rethink's test
// suite; note the 0xa7 in byte 6 of this model's frames).

import "tlv_test" as t;

fn caps() { "000004000000A702010077B00AB04FB0A001D5B0C1B103B4F0401011B710FEBB81BBC0BC41BC88BCC1B3701FE037B381B54EB2E01006B7600120B85020B8903CB8D020B9103CBD600203B5C0B646B61030B5C1B646B61030B5C2B646B61030B5C3B646B603B69037B6F0570004D41010BD30800400DD1020B5B010000064903EFA05EFE0" }
// Captured while the unit was off: power 0, mode cool, fan high, room 26.0, setpoint 24.0.
fn state() { "000004000000A7020400837DC07E407E867F50347F90307F0086808840D4C0D500C84181408180C940A380A3C0A400A440F540F580F5C08340838389501E83C08FC0CD40CD00CCC0CDA0032BADA01CC6ACC0AD41B54ED56002FBD5A00960BC88D5D03CD61020C9009C40A88087D0C8AC40E9C16640668066C067006740678067C068008F80F7407C8189501E9380D558" }

fn running() { t::running(device(), caps(), state()) }

fn test_the_0xa7_capabilities_reply_moves_on_to_the_values_query() {
    let d = device();
    d.start();
    d.feed(caps());
    expect_eq(d.sent(), [t::query(1), t::query(2)]);
}

fn test_the_captured_state_becomes_il_properties() {
    expect_props(running(), #{
        available: "true", power: "false", mode: "cool", fan: "high", temperature: "26",
        target: "24", humidity: "81", action: "off", swing_vertical: "false",
        swing_horizontal: "false", comfort_saving: "false", wind_mode: "off",
        auto_dry: "60min", auto_dry_remaining: "30", sleep_timer: "0", display: "100%",
        power_draw: "0", filter_remaining: "32", filter_used: "1637", filter_life: "2400",
        error: "0"
    });
}

fn test_energy_saving_is_not_reported_while_the_unit_is_off() {
    // 0x20d is in the captured frame, but it means nothing while the unit is not cooling
    expect_eq(running().property("energy_save"), ());
}

fn test_the_first_values_frame_masks_the_noisy_tag_and_starts_the_refresh() {
    let d = running();
    expect_eq(d.clips().len(), 1);
    expect_eq(d.clips()[0].cmd, "setMaskingInfo");
    expect_eq(d.timers(), [["refresh", 900000]]);
}

fn test_a_mode_write_powers_the_unit_on() {
    let d = running();
    let n = d.sent().len();
    d.set("mode", "auto");
    expect_eq(t::sent_since(d, n), [[[0x1f9, 3], [0x1f7, 1], [0x1fa, 6], [0x1fe, 48]]]);
    d.set("mode", "cool");
    expect_eq(t::sent_since(d, n)[1][0], [0x1f9, 0]);
}

fn test_power_on_carries_mode_fan_and_setpoint_and_power_off_only_itself() {
    let d = running();
    let n = d.sent().len();
    d.set("power", "true");
    d.set("power", "false");
    expect_eq(t::sent_since(d, n), [
        [[0x1f7, 1], [0x1f9, 0], [0x1fa, 6], [0x1fe, 48]],
        [[0x1f7, 0]],
    ]);
}

fn test_fan_writes_use_this_units_scale() {
    let d = running();
    let n = d.sent().len();
    for name in ["auto", "very_low", "low", "medium", "high", "power"] { d.set("fan", name); }
    let firsts = [];
    for f in t::sent_since(d, n) { firsts.push(f[0]); }
    expect_eq(firsts, [[0x1fa, 8], [0x1fa, 1], [0x1fa, 2], [0x1fa, 4], [0x1fa, 6], [0x1fa, 7]]);
    expect_eq(t::sent_since(d, n)[0], [[0x1fa, 8], [0x1f9, 0], [0x1fe, 48]]);
}

fn test_the_setpoint_goes_out_in_half_degrees() {
    let d = running();
    let n = d.sent().len();
    d.set("target", "24.5");
    d.set("target", "16");
    expect_eq(t::sent_since(d, n), [
        [[0x1fe, 49], [0x1f9, 0], [0x1fa, 6]],
        [[0x1fe, 32], [0x1f9, 0], [0x1fa, 6]],
    ]);
}

fn test_the_host_refuses_a_setpoint_outside_the_units_range_or_step() {
    let d = running();
    let n = d.sent().len();
    for v in ["15.5", "30.5", "24.3"] { d.set("target", v); }
    expect_eq(d.sent().len(), n);
    expect(d.event("reject").contains("\"prop\":\"target\""), "a reject event names the property");
}

fn test_switches_and_selects_write_their_own_tag_alone() {
    let d = running();
    let n = d.sent().len();
    d.set("swing_vertical", "true");
    d.set("swing_horizontal", "on");
    d.set("comfort_saving", "true");
    d.set("auto_dry", "smart");
    d.set("display", "50%");
    d.set("sleep_timer", "30");
    expect_eq(t::sent_since(d, n), [
        [[0x205, 1]], [[0x206, 1]], [[0x23f, 1]], [[0x20e, 255]], [[0x21f, 150]], [[0x21a, 30]],
    ]);
}

fn test_a_bad_write_is_refused_and_sends_nothing() {
    let d = running();
    let n = d.sent().len();
    d.set("fan", "turbo");
    d.set("mode", "heat");
    d.set("sleep_timer", "901");
    d.set("humidity", "50"); // read only
    expect_eq(d.sent().len(), n);
}

fn test_wind_mode_is_read_from_the_flags_and_written_as_one_exclusive_set() {
    let d = running();
    d.feed(t::notify([[0x3d7, 1]]));
    expect_eq(d.property("wind_mode"), "long_power");
    d.feed(t::notify([[0x3d7, 0], [0x291, 1]]));
    expect_eq(d.property("wind_mode"), "study");

    let n = d.sent().len();
    d.set("wind_mode", "manner");
    d.set("wind_mode", "off");
    expect_eq(t::sent_since(d, n), [
        [[0x290, 0], [0x291, 0], [0x3d5, 0], [0x3d6, 1], [0x3d7, 0]],
        [[0x290, 0], [0x291, 0], [0x3d5, 0], [0x3d6, 0], [0x3d7, 0]],
    ]);
    expect_eq(d.property("wind_mode"), "off");
}

fn test_energy_saving_is_trusted_only_while_cooling() {
    let d = running();
    d.feed(t::notify([[0x1f7, 1], [0x1f9, 1], [0x20d, 1]])); // dry: ignored
    expect_eq(d.property("energy_save"), ());
    d.feed(t::notify([[0x1f9, 0], [0x20d, 1]])); // cool: adopted
    expect_eq(d.property("energy_save"), "true");
}

fn test_energy_saving_set_while_off_is_written_when_cooling_starts() {
    let d = running();
    let n = d.sent().len();
    d.set("energy_save", "true"); // off: nothing can take it yet
    expect_eq(d.sent().len(), n);
    expect_eq(d.property("energy_save"), "true");

    // the unit powers up in cool and reports the setting as off: write it again
    d.feed(t::notify([[0x1f7, 1], [0x1f9, 0], [0x20d, 0]]));
    expect_eq(t::sent_since(d, n), [[[0x20d, 1]]]);
    expect_eq(d.property("energy_save"), "true");

    // while cooling a change is written at once
    d.set("energy_save", "false");
    expect_eq(t::sent_since(d, n)[1], [[0x20d, 0]]);
}

fn has_timer(d, name) {
    for x in d.timers() { if x[0] == name { return true; } }
    false
}

fn test_action_follows_power_and_mode_and_polling_runs_only_while_active() {
    let d = running();
    expect(!has_timer(d, "poll"), "no polling while off");

    d.feed(t::notify([[0x1f7, 1], [0x1f9, 0]]));
    expect_eq(d.property("action"), "cooling");
    expect(d.timers().contains(["poll", 28000]), "polling starts while cooling");

    let n = d.sent().len();
    d.fire("poll");
    expect_eq(d.sent().extract(n), [t::query(2)]);
    expect(d.timers().contains(["poll", 28000]), "and re-arms");

    d.feed(t::notify([[0x189, 0]])); // the unit says it is not cooling right now
    expect_eq(d.property("action"), "idle");
    expect(!has_timer(d, "poll"), "no polling while idle");

    d.feed(t::notify([[0x189, 1], [0x1f9, 1]]));
    expect_eq(d.property("action"), "drying");
    d.feed(t::notify([[0x1f9, 2]]));
    expect_eq(d.property("action"), "fan");
    d.feed(t::notify([[0x1f9, 3]]));
    expect_eq(d.property("action"), "active");
    d.feed(t::notify([[0x1f7, 0]]));
    expect_eq(d.property("action"), "off");
    expect(!has_timer(d, "poll"), "no polling once off");
}

fn test_losing_the_connection_marks_it_unavailable() {
    let d = running();
    d.drop_device();
    expect_eq(d.property("available"), "false");
}

fn test_a_stray_notification_before_the_values_does_not_end_the_retry() {
    let d = device();
    d.start();
    d.feed(caps());
    d.feed(t::notify([[0x1fa, 6]])); // a change notification, not the state
    expect(has_timer(d, "values_retry"), "the values query is still being retried");
    expect_eq(d.property("available"), ());
    d.feed(state());
    expect(!has_timer(d, "values_retry"), "the full state ends the retry");
    expect_eq(d.property("available"), "true");
}

fn test_the_setpoint_range_follows_the_capability_reply() {
    let d = device();
    d.start();
    expect_eq(d.descriptor().props.target.min, 16);
    // a unit that reports a narrower range: 18.0 .. 29.0 (half degrees on the wire)
    d.feed(t::notify([[0x2da, 55], [0x2e1, 36], [0x2e2, 58]]));
    let target = d.descriptor().props.target;
    expect_eq(target.min, 18);
    expect_eq(target.max, 29);
    d.feed(state());
    let n = d.sent().len();
    d.set("target", "17.5"); // below what this unit takes: the host refuses
    expect_eq(d.sent().len(), n);
    d.set("target", "18.5");
    expect_eq(t::sent_since(d, n), [[[0x1fe, 37], [0x1f9, 0], [0x1fa, 6]]]);
}

fn test_the_captured_capabilities_keep_the_range_they_report() {
    let d = device();
    d.start();
    d.feed(caps()); // 0x2e1 = 32, 0x2e2 = 60: 16.0 .. 30.0
    expect_eq(d.descriptor().props.target.min, 16);
    expect_eq(d.descriptor().props.target.max, 30);
}
