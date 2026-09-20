// Tests of RH14_N_KR.rhai (dryer), against frames captured while it ran a cycle and the
// start frame the LG app sent.

import "aabb_test" as a;

fn running_1() { "aa3c30ec001902002e010a070003020200000000001900007e010000007000001902002d010a07000302020000000000190000830100000070008fbb" }
fn running_2() { "aa3c30ec001902002d010a0700030202000000000019000083010000007000001902002c010a0700030202000000000019000088010000007000b7bb" }
fn running_3() { "aa3c30ec001902002c010a0700030202000000000019000088010000007000001902002b010a070003020200000000001900008c010000007000bcbb" }

fn status(p) { a::status(0x30, p) }

// The record of the first real frame, as a template to vary.
fn cotton() {
    a::payload(25, [2, 0, 45, 1, 10, 7, 0, 3, 2, 2, 0, 0, 0, 0, 0, 0x19])
}

fn test_a_real_frame_becomes_il_properties() {
    let d = device();
    d.feed(running_1());
    expect_props(d, #{
        available: "true", power: "true", status: "running", process: "drying_1",
        course: "cotton_normal", course_select: "cotton_normal", remaining_time: "45",
        initial_time: "70", reserve_time: "0", dry_level: "iron_dry", eco_hybrid: "normal",
        energy: "131", error: "false", error_message: "OK", remote_start: "true",
        steam: "false", child_lock: "false", reservation: "false"
    });
}

fn test_only_the_current_record_counts_and_the_countdown_and_energy_follow_it() {
    let d = device();
    d.feed(running_2());
    expect_props(d, #{ remaining_time: "44", energy: "136" });
    d.feed(running_3());
    expect_props(d, #{ remaining_time: "43", energy: "140" });
}

fn test_off_zeroes_the_times_and_the_process() {
    let d = device();
    let p = cotton();
    p[0] = 0;
    d.feed(status(p));
    expect_props(d, #{ power: "false", status: "off", remaining_time: "0", process: "none" });
}

fn test_the_flags_decode_from_their_bits() {
    let d = device();
    let p = cotton();
    p[14] = 0x01 | 0x02 | 0x10 | 0x20 | 0x40 | 0x80;
    p[16] = 0x08;
    d.feed(status(p));
    expect_props(d, #{
        reservation: "true", anti_crease: "true", child_lock: "true", self_clean: "true",
        damp_dry_beep: "true", hand_iron: "true", steam: "true"
    });
}

fn test_an_error_code_is_reported_with_its_message() {
    let d = device();
    let p = cotton();
    p[6] = 14;
    d.feed(status(p));
    expect_props(d, #{ error: "true", error_message: "Water tank empty" });
    p[6] = 99;
    d.feed(status(p));
    expect_props(d, #{ error_message: "Unknown error (99)" });
}

// ---- commands, as the app sent them ------------------------------------------------

fn test_cotton_normal_start_as_the_app_sent_it() {
    let d = device();
    d.feed(running_1()); // the dial is on Cotton Normal
    d.set("start", "");
    expect_eq(a::inner_hex(d, 0), "f0260703020000000000000003000000");
}

fn test_resuming_clears_the_initial_bit_keeping_remote_start() {
    let d = device();
    let p = cotton();
    p[0] = 3; // paused
    d.feed(status(p));
    d.set("start", "");
    let sent = a::inner_hex(d, 0);
    expect_eq(sent.sub_string(0, 4), "f026");
    expect_eq(sent.sub_string(4 + 20, 2), "01"); // byte 10
}

fn test_pause_and_power_off_are_the_short_controls() {
    let d = device();
    d.set("power_off", "");
    d.set("pause", "");
    expect_eq(a::inner_hex(d, 0), "f024010100");
    expect_eq(a::inner_hex(d, 1), "f024040100");
}

fn test_a_course_this_dial_does_not_have_is_not_started() {
    let d = device();
    d.set("course_select", "refresh"); // code 1: on the family table, not on this dial
    d.set("course_select", "nonsense");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_a_chosen_course_is_what_start_asks_for() {
    let d = device();
    d.feed(running_1());
    d.set("course_select", "towels");
    d.set("start", "");
    // Towels: dry level 0, Eco Hybrid Normal
    expect_eq(a::inner_hex(d, 0), "f0260200020000000000000003000000");
}

fn test_a_refusal_is_reported_and_an_acceptance_is_not() {
    let d = device();
    d.set("start", ""); // nothing selected yet
    expect(d.event("reject") != (), "a reject event was published");
    let d = device();
    d.feed(running_1());
    d.set("start", "");
    d.feed(a::framed([0x30, 0x00, 0x26, 0x00]));
    expect_eq(d.event("reject"), ());
    d.feed(a::framed([0x30, 0x00, 0x26, 0xff]));
    expect(d.event("reject").contains("refused"), "a refusal is reported");
}

fn test_the_descriptor_is_a_dryer_with_conditional_controls() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc.kind, "dryer");
    expect_eq(desc.props.start.requires, "remote_start");
    expect_eq(desc.props.energy.unit, "Wh");
}

fn test_the_host_holds_start_back_until_remote_start_is_armed() {
    let d = device();
    let disarmed = cotton();
    disarmed[15] = 0;
    d.start();
    d.feed(status(disarmed));
    d.set("start", "");
    d.set("pause", "");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject").contains("requires remote_start"), "the host names the requirement");
    d.feed(status(cotton())); // armed
    d.set("pause", "");
    expect_eq(d.sent().len(), 1);
}
