// Tests of 2RSFL2DBN3K_Z.rhai (fridge), against a status frame captured live from a real
// unit (a query sent, then this reply) and write frames computed from the model's own F0 17
// template (or, for AI Saving Mode, its separate F0 10 frame).

import "aabb_test" as a;

// The reply to the start() query, captured live: fridge wire 5, freezer wire 4, express
// freeze off, door closed, Celsius, Smart Care off.
fn live_reply() {
    "AA6510EB0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF010101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF00E8BB"
}

fn test_start_sends_the_status_query() {
    let d = device();
    d.start();
    expect_eq(a::inner_hex(d, 0), "f0ed1211010000010400");
}

fn test_the_live_reply_becomes_il_properties() {
    let d = device();
    d.feed(live_reply());
    expect_props(d, #{
        available: "true",
        fridge_setpoint: "3",
        freezer_setpoint: "-18",
        express_freeze: "false",
        door: "false",
        smart_care: "false",
    });
}

fn test_the_descriptor_defaults_to_celsius_before_any_frame_arrives() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc.props.fridge_setpoint.unit, "°C");
    expect_eq(desc.props.fridge_setpoint.min, 1);
    expect_eq(desc.props.fridge_setpoint.max, 7);
    expect_eq(desc.props.freezer_setpoint.min, -23);
    expect_eq(desc.props.freezer_setpoint.max, -15);
}

fn test_a_door_value_of_two_still_means_closed() {
    // The live reply with offset 7 (door) changed from 0 to 2 (this model family's own
    // quirk: 0 and 2 both mean closed, only 1 is open) and the checksum recomputed.
    let d = device();
    d.feed("AA6710EB0205040102FFFF02010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF010101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF00E8FDBB");
    expect_props(d, #{ door: "false" });
}

fn test_fridge_setpoint_write_is_the_f017_template_with_one_byte_changed() {
    let d = device();
    d.set("fridge_setpoint", "5");
    expect_eq(a::inner_hex(d, 0),
        "f017ff03ffffffffffff01ffffffffffffffffffffffff000000ffff00ffffffff00ffffffffffffffffff00ffffff1effffffffffffffffffffffffffffffffffffffffffffffff0affffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ffffffffffffffffffffffffffffff");
}

fn test_freezer_setpoint_write() {
    let d = device();
    d.set("freezer_setpoint", "-20");
    expect_eq(a::inner_hex(d, 0),
        "f017ffff06ffffffffff01ffffffffffffffffffffffff000000ffff00ffffffff00ffffffffffffffffff00ffffff1effffffffffffffffffffffffffffffffffffffffffffffff0affffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ffffffffffffffffffffffffffffff");
}

fn test_express_freeze_on() {
    let d = device();
    d.set("express_freeze", "true");
    expect_eq(a::inner_hex(d, 0),
        "f017ffffff02ffffffff01ffffffffffffffffffffffff000000ffff00ffffffff00ffffffffffffffffff00ffffff1effffffffffffffffffffffffffffffffffffffffffffffff0affffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ffffffffffffffffffffffffffffff");
}

// Confirmed writable at the protocol level (the same F0 17 write as everything else, acked and
// reflected the same way) but exposed read-only: kept as a sensor rather than a control.
fn test_smart_care_cannot_be_written() {
    let d = device();
    d.start();
    let n = d.sent().len();
    d.set("smart_care", "true");
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

fn test_a_write_always_restates_the_unit_last_reported() {
    let d = device();
    d.feed(live_reply()); // reports Celsius (offset 8 = 1)
    d.set("express_freeze", "true");
    // offset 8 is inner byte 2+8=10, two hex characters per byte -> hex index 20.
    expect_eq(a::inner_hex(d, 0).sub_string(20, 2), "01");
}

fn test_a_bad_value_is_rejected_and_nothing_is_sent() {
    let d = device();
    d.start();
    let n = d.sent().len();
    d.set("fridge_setpoint", "not a number");
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

fn test_an_out_of_range_setpoint_is_rejected_by_the_host() {
    let d = device();
    d.start();
    let n = d.sent().len();
    d.set("fridge_setpoint", "50");
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

// The rest of this file is captured against real 0xEC frames from a live unit, one per
// property below toggled once from the LG app while this driver's decode was checked live.

fn test_ai_saving_mode_balanced() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF010101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF000205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF010101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640101010181BB");
    expect_props(d, #{ ai_saving: "balanced" });
}

fn test_ai_saving_mode_off() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF020101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF64010101010205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF020101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF0083BB");
    expect_props(d, #{ ai_saving: "off" });
}

// Real capture of the balanced -> max transition (the LG app taking AI Saving from "균형절전"
// to "최대절전"): offset 93 alone moves 1 -> 2, offsets 92/94 stay 1.
fn test_ai_saving_mode_max() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF080101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF64010101010205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF080101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF6401010201B2BB");
    expect_props(d, #{ ai_saving: "max" });
}

// The write is a short separate F0 10 frame, not the F0 17 record -- these three are the
// literal frames a live toggle through each transition sent (off -> balanced -> max -> off),
// captured verbatim.
fn test_ai_saving_write_off() {
    let d = device();
    d.set("ai_saving", "off");
    expect_eq(a::inner_hex(d, 0), "f010070d00000000000000000000000000");
}

fn test_ai_saving_write_balanced() {
    let d = device();
    d.set("ai_saving", "balanced");
    expect_eq(a::inner_hex(d, 0), "f010070d01000000000000000000000000");
}

fn test_ai_saving_write_max() {
    let d = device();
    d.set("ai_saving", "max");
    expect_eq(a::inner_hex(d, 0), "f010070d031a09160c00001a0916150000");
}

// The active-hours window inside max mode: bytes 8/9 (start) and 14/15 (end) are hour/minute,
// the rest is the same fixed record max mode always sends. "12:00-21:00" is the exact frame a
// live nudge of this control produced (start hour -1, then +1 minute, confirmed by hand).
fn test_ai_saving_max_schedule_write() {
    let d = device();
    d.set("ai_saving", "max"); // satisfies the host's `requires`
    d.set("ai_saving_max_schedule", "12:00-21:00");
    expect_eq(a::inner_hex(d, 1), "f010070d031a09160c00001a0916150000");
    // No status frame ever confirms this, so the driver echoes the write itself.
    expect_props(d, #{ ai_saving_max_schedule: "12:00-21:00" });
}

fn test_ai_saving_max_schedule_one_hour_earlier() {
    let d = device();
    d.set("ai_saving", "max");
    d.set("ai_saving_max_schedule", "11:00-20:00");
    expect_eq(a::inner_hex(d, 1), "f010070d031a09160b00001a0916140000");
}

fn test_ai_saving_max_schedule_one_minute_later() {
    let d = device();
    d.set("ai_saving", "max");
    d.set("ai_saving_max_schedule", "12:01-21:01");
    expect_eq(a::inner_hex(d, 1), "f010070d031a09160c01001a0916150100");
}

fn test_ai_saving_max_schedule_needs_max_mode_first() {
    let d = device();
    d.start();
    let n = d.sent().len();
    d.set("ai_saving_max_schedule", "12:00-21:00"); // ai_saving is still unset
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

fn test_ai_saving_max_schedule_rejects_a_malformed_value() {
    let d = device();
    d.set("ai_saving", "max");
    let n = d.sent().len();
    d.set("ai_saving_max_schedule", "noon to nine");
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

fn test_night_glare_custom_schedule() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF020101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF000205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF030101FF03FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF0083BB");
    expect_props(d, #{ night_glare: "custom_schedule" });
}

fn test_night_glare_off() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF030101FF03FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF000205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF040101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF008DBB");
    expect_props(d, #{ night_glare: "off" });
}

// Confirmed live: offset 30 = 2 for the LG app's other night-glare mode, alongside the
// already-known 0 (off) and 3 (custom schedule).
fn test_night_glare_sunset_to_sunrise() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF110101FF03FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF000205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF120101FF02FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF00A3BB");
    expect_props(d, #{ night_glare: "sunset_to_sunrise" });
}

fn test_door_alarm_mute_on() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF040101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF000205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF040101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0105FFFFFFFF640100FF0082BB");
    expect_props(d, #{ door_alarm_mute: "true" });
}

fn test_door_alarm_mute_off() {
    let d = device();
    d.feed("AAC410EC0205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF040101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0105FFFFFFFF640100FF000205040102FFFF00010001FFFFFFFFFFFF00FFFFFFFFFFFFFFFF040101FF00FFFFFFFFFFFFFFFFFF01FF0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0078FFFFFFFFFFFFFFFFFFFFFFFF000000FFFFFFFF0005FFFFFFFF640100FF0082BB");
    expect_props(d, #{ door_alarm_mute: "false" });
}

// Confirmed writable at the protocol level (same F0 17 write, acked and reflected the same
// way as offset 85, the mute duration) but exposed read-only: kept as a sensor.
fn test_door_alarm_mute_cannot_be_written() {
    let d = device();
    d.start();
    let n = d.sent().len();
    d.set("door_alarm_mute", "true");
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

fn test_ai_saving_is_a_writable_select_of_three_levels() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc.props.ai_saving.type, "select");
    expect_eq(desc.props.ai_saving.rw, true);
    expect_eq(desc.props.ai_saving.options, ["off", "balanced", "max"]);
}

fn test_an_unknown_ai_saving_value_is_rejected() {
    let d = device();
    d.start();
    let n = d.sent().len();
    d.set("ai_saving", "extreme");
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

// Confirmed writable at the protocol level (a separate short F0 10 02 frame, acked and
// reflected in the next status frame for plain on/off) but exposed read-only: kept as a
// sensor. That same write also carries a custom schedule's start/end time and LCD brightness,
// confirmed live too, but neither is ever echoed back, so this driver does not expose them.
fn test_night_glare_cannot_be_written() {
    let d = device();
    d.start();
    let n = d.sent().len();
    d.set("night_glare", "custom_schedule");
    expect_eq(d.sent().len(), n);
    expect(d.event("reject") != (), "a reject event is published");
}

// Two real captures of the unprompted ~15-minute 0xC5 report, spanning a live door test
// (both freezer doors, then the fridge door, opened and closed once each): offset 9 (fridge)
// moved 14 -> 15, offset 15 (freezer) moved 10 -> 12, matching the LG app's own per-compartment
// counts for that hour exactly.
fn test_door_open_counts_before_the_door_test() {
    let d = device();
    d.feed("AA2010C50C0401000000000E03000000000A11000000033A13000000009C9DBB");
    expect_props(d, #{ door_open_count_fridge: "14", door_open_count_freezer: "10" });
}

fn test_door_open_counts_after_the_door_test() {
    let d = device();
    d.feed("AA2010C5110401000100000F03000200000C11000400033E1300060000A2B2BB");
    expect_props(d, #{ door_open_count_fridge: "15", door_open_count_freezer: "12" });
}

// Two real captures of the unprompted ~15-minute 0x3E report, taken ~48 minutes apart with no
// door or setting changes in between (just the compressor running): the cumulative total
// climbed from 234 to 312, confirmed against the LG app's own energy-monitoring figure at a
// later point in the same sequence (503 on the wire, 503 Wh in the app).
fn test_energy_today_first_reading() {
    let d = device();
    d.feed("AA0B103E001E00EA0747BB");
    expect_props(d, #{ energy_today: "234" });
}

fn test_energy_today_climbs_with_no_state_change() {
    let d = device();
    d.feed("AA0B103E001E01380A31BB");
    expect_props(d, #{ energy_today: "312" });
}
