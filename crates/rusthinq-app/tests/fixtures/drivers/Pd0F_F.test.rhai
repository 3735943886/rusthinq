// Tests of Pd0F_F.rhai (mini washer). The commands are byte for byte as the LG app sent them;
// the one real status frame is an idle one, the others are built from the documented offsets.

import "aabb_test" as a;

fn real_idle() { "aa3c20ec001900000100010100000100000000000000000000000000006700001900000100010100000100000000000000000000000000006700afbb" }

fn status(p) { a::status(0x20, p) }

// The payload for an off drawer washer with Small Load on the dial (spin on).
fn idle() {
    let p = blob(25);
    p[5] = 1; // course: Small Load
    p[8] = 1; // spin
    p
}

// Washing: 41 of 45 minutes left, Small Load, 40 C, three rinses, remote start armed, child lock.
fn washing() {
    let p = idle();
    p[0] = 5;      // washing
    p[2] = 41;
    p[4] = 45;
    p[9] = 40;
    p[10] = 3;
    p[14] = 0x01 | 0x08; // child lock, door lock
    p[15] = 0x02;        // sterilize
    p[16] = 0x04;        // remote start
    p
}

fn test_idle_matches_what_the_adapter_reported_for_this_unit() {
    let d = device();
    d.feed(status(idle()));
    expect_props(d, #{
        available: "true", power: "false", status: "off", course: "small_load",
        course_select: "small_load", remaining_time: "0", initial_time: "0", reserve_time: "0",
        temp: "cold", rinse: "0", spin: "true", error: "false", error_message: "OK",
        child_lock: "false", audible_diagnosis: "false", door_lock: "false", sterilize: "false",
        warm_water: "false", remote_start: "false"
    });
}

fn test_a_running_cycle_reports_its_times_and_flags() {
    let d = device();
    d.feed(status(washing()));
    expect_props(d, #{
        power: "true", status: "washing", remaining_time: "41", initial_time: "45", temp: "40",
        rinse: "3", child_lock: "true", door_lock: "true", sterilize: "true", remote_start: "true"
    });
}

fn test_times_are_zeroed_while_off_not_left_frozen_mid_cycle() {
    let d = device();
    let p = idle();
    p[2] = 30; // the appliance keeps the last cycle's figures in these bytes
    d.feed(status(p));
    expect_props(d, #{ remaining_time: "0", status: "off" });
}

fn test_the_single_record_frames_are_read_too() {
    let d = device();
    // 0xEB: the current state alone, sent right after reconnecting
    d.feed(a::record(0x20, 0xeb, 0x00, washing()));
    expect_props(d, #{ status: "washing" });
    // 0xE2: the same record behind header byte 03, sent when a cycle begins
    d.feed(a::record(0x20, 0xe2, 0x03, idle()));
    expect_props(d, #{ status: "off" });
}

fn test_a_record_with_the_wrong_payload_length_reports_nothing() {
    let d = device();
    d.feed(a::record(0x20, 0xeb, 0x00, blob(24)));
    expect_eq(d.property("status"), ());
}

// ---- commands, as the app sent them ------------------------------------------------

fn test_small_load_start_as_the_app_sent_it() {
    let d = device();
    d.feed(status(idle()));
    d.set("start", "");
    expect_eq(a::inner_hex(d, 0), "f026010001000200000000000000000005");
}

fn test_resume_is_the_same_frame_with_the_initial_bit_cleared() {
    let d = device();
    let paused = idle();
    paused[0] = 2;
    d.feed(status(paused));
    d.set("start", "");
    expect_eq(a::inner_hex(d, 0), "f026010001000200000000000000000004");
}

fn test_pause_and_power_off_are_the_short_controls() {
    let d = device();
    d.set("power_off", "");
    d.set("pause", "");
    expect_eq(a::inner_hex(d, 0), "f024010100");
    expect_eq(a::inner_hex(d, 1), "f024040100");
}

fn test_the_selected_course_is_what_start_asks_for() {
    let d = device();
    d.feed(status(idle()));
    d.set("course_select", "tub_clean");
    expect_eq(d.property("course_select"), "tub_clean");
    d.set("start", "");
    // Tub Clean: soil 0, spin 1, 60 C, one rinse
    expect_eq(a::inner_hex(d, 0), "f0260700013c0100000000000000000005");
}

fn test_the_selection_follows_the_dial_but_survives_a_status_repeat_and_a_power_off() {
    let d = device();
    d.feed(status(idle())); // dial on Small Load
    d.set("course_select", "wool");
    d.feed(status(idle())); // the next report repeats the dial: must not undo the choice
    expect_eq(d.property("course_select"), "wool");
    let off = idle();
    off[5] = 0; // powered off zeroes the course byte
    d.feed(status(off));
    d.set("start", "");
    let sent = a::inner_hex(d, 0);
    expect_eq(sent.sub_string(0, 4), "f026");
    expect_eq(sent.sub_string(4, 2), "03"); // still wool
    let turned = idle();
    turned[5] = 6; // someone turned the dial to Rinse + Spin
    d.feed(status(turned));
    expect_eq(d.property("course_select"), "rinse_spin");
}

fn test_a_dial_position_start_cannot_express_is_not_adopted() {
    let d = device();
    d.feed(status(idle()));
    let odd = idle();
    odd[5] = 10; // Speed Wash: on the family table, not startable here
    d.feed(status(odd));
    expect_eq(d.property("course"), "speed_wash");
    expect_eq(d.property("course_select"), "small_load");
}

fn test_a_course_the_drawer_does_not_have_is_rejected() {
    let d = device();
    d.set("course_select", "speed_wash");
    d.set("course_select", "nonsense");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_a_refused_command_is_reported_against_the_property_that_sent_it() {
    let d = device();
    d.set("power_off", "");
    // this washer answered 0xff to a power-off while it had nothing to power off
    d.feed(a::framed([0x20, 0x00, 0x24, 0xff]));
    let reject = d.event("reject");
    expect(reject != (), "a rejection");
    expect(reject.contains("power_off") && reject.contains("refused"), reject);
}

fn test_an_accepted_command_publishes_nothing_and_another_devices_ack_is_ignored() {
    let d = device();
    d.set("power_off", "");
    d.feed(a::framed([0x20, 0x00, 0x24, 0x00]));
    d.feed(a::framed([0x30, 0x00, 0x24, 0xff])); // the dryer's address, not ours
    expect_eq(d.event("reject"), ());
}

fn test_dropping_the_device_marks_it_unavailable() {
    let d = device();
    d.feed(status(idle()));
    d.drop_device();
    expect_eq(d.property("available"), "false");
}

fn test_the_descriptor_marks_the_controls_and_the_dial_is_separate_from_the_selection() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc["class"], "mini_washer");
    expect_eq(desc.props.start.type, "trigger");
    expect_eq(desc.props.start.requires, "remote_start");
    expect_eq(desc.props.course_select.rw, true);
    expect_eq(desc.props.course.rw, ());
}

fn test_a_real_frame_from_the_appliance_reads_as_idle_small_load() {
    let d = device();
    d.feed(real_idle());
    expect_eq(d.script_error(), ());
    expect_props(d, #{ available: "true", power: "false", status: "off", course: "small_load" });
}

fn test_the_host_holds_start_back_until_remote_start_is_armed() {
    let d = device();
    d.start();
    d.feed(status(idle())); // remote start is not armed at the panel
    d.set("start", "");
    d.set("power_off", "");
    expect_eq(d.sent().len(), 0, "nothing is sent to an appliance that would refuse it");
    expect_eq(d.event("reject"), "{\"code\":\"requires_unmet\",\"prop\":\"power_off\",\"reason\":\"requires remote_start\"}");
    d.feed(status(washing())); // now armed
    d.set("start", "");
    expect_eq(d.sent().len(), 1);
}
