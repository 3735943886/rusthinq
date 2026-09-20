// Tests of F24VDD.rhai (washer), against a real idle frame and the start frames the LG app sent.

import "aabb_test" as a;

fn idle() { "aa4e20ec0022000000000000000000000000000000020000bf3c39180e0000000003000402022d1e0022000000000000000000000000000000020000bf3c39180e0000000003000402022d1ef9bb" }

fn status(p) { a::status(0x20, p) }

// The washer mid-wash: the record the source comments describe (17 01 21 01 2c 06 00 03 03 04
// 03 ...), Heavy Duty at heavy soil, medium spin, 60 C, three rinses, door locked, Add Garment.
fn heavy_duty_wash() {
    let p = a::payload(34, [23, 1, 33, 1, 44, 6, 0, 3, 3, 4, 3]);
    p[14] = 0x20 | 0x08;        // remote start, child lock
    p[15] = 0x02 | 0x04 | 0x40; // alarm, door lock, add garment
    p[17] = 0x01;
    p[18] = 0x2c;               // 300 Wh so far
    p[22] = 14;                 // operating course: Heavy Duty
    p
}

fn test_a_real_idle_frame_becomes_il_properties() {
    let d = device();
    d.feed(idle());
    expect_eq(d.script_error(), ());
    expect_props(d, #{
        available: "true", power: "false", status: "off", course: "none",
        op_course: "heavy_duty", download_course: "kids_wear", remaining_time: "0",
        initial_time: "0", reserve_time: "0", soil: "none", spin: "no_spin", temp: "none",
        rinse: "0", energy: "191", load_level: "0", tub_clean_count: "24", error: "false",
        error_message: "OK", alarm: "true", child_lock: "false", door_lock: "false",
        remote_start: "false", steam: "false"
    });
}

fn test_a_running_wash_matches_the_record_the_cloud_decoded() {
    let d = device();
    d.feed(status(heavy_duty_wash()));
    expect_props(d, #{
        status: "washing", course: "heavy_duty", op_course: "heavy_duty",
        remaining_time: "93",  // 1h33m left
        initial_time: "104",   // of 1h44m
        soil: "heavy", spin: "medium", temp: "60", rinse: "3", door_lock: "true",
        add_load: "true", child_lock: "true", remote_start: "true", energy: "300"
    });
}

fn test_the_dial_course_and_the_running_course_are_reported_apart() {
    let d = device();
    let p = heavy_duty_wash();
    p[5] = 14; // the dial is on "Downloaded course"
    p[22] = 5; // and the machine is running Cotton
    d.feed(status(p));
    expect_props(d, #{ course: "downloaded_course", op_course: "cotton" });
    // a dial position start cannot express is not adopted as the selection
    expect_eq(d.property("course_select"), ());
}

fn test_the_flags_decode_from_their_bits() {
    let d = device();
    let p = heavy_duty_wash();
    p[14] = 0x01 | 0x02 | 0x04 | 0x10 | 0x40;
    p[15] = 0x80;
    d.feed(status(p));
    expect_props(d, #{
        crease_care: "true", fresh_care: "true", rinse_hold: "true", steam: "true",
        favorite: "true", turbo_shot: "true", alarm: "false"
    });
}

// ---- commands, as the app sent them ------------------------------------------------

fn test_colour_care_start_as_the_app_sent_it() {
    let d = device();
    d.set("course_select", "colour_care");
    d.set("start", "");
    expect_eq(a::payload_hex(d, 0), "0a0202020300002000201000000000000000000000");
}

fn test_heavy_duty_start_as_the_app_sent_it() {
    let d = device();
    d.set("course_select", "heavy_duty");
    d.set("start", "");
    expect_eq(a::payload_hex(d, 0), "060303040300002000200e00000000000000000000");
}

fn test_steam_refresh_sets_the_steam_bit_and_the_app_agreed() {
    let d = device();
    d.set("course_select", "steam_refresh");
    d.set("start", "");
    // the course fixes steam on: the flags byte carries 0x10 beside the remote-start 0x20
    expect_eq(a::payload_hex(d, 0), "010200000000003000200100000000000000000000");
}

fn test_resuming_clears_the_initial_bit_and_changes_nothing_else() {
    let d = device();
    d.set("course_select", "heavy_duty");
    d.set("start", "");
    let paused = heavy_duty_wash();
    paused[0] = 6;
    d.feed(status(paused));
    d.set("start", "");
    let start = a::inner_of(d, 0);
    let resume = a::inner_of(d, 1);
    expect_eq(start[2 + 9].to_int(), 0x20);
    expect_eq(resume[2 + 9].to_int(), 0);
    expect_eq(start.extract(0, 2 + 9), resume.extract(0, 2 + 9));
    expect_eq(start.extract(2 + 10), resume.extract(2 + 10));
}

fn test_a_course_with_no_preset_the_downloaded_dial_position_is_not_started() {
    let d = device();
    d.set("course_select", "downloaded_course");
    d.set("course_select", "none");
    d.set("start", ""); // nothing valid selected
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_pause_and_power_off_are_the_short_controls() {
    let d = device();
    d.set("power_off", "");
    d.set("pause", "");
    expect_eq(a::inner_hex(d, 0), "f024010100");
    expect_eq(a::inner_hex(d, 1), "f024040100");
}

fn test_a_washer_that_refuses_a_power_off_says_so() {
    let d = device();
    d.set("power_off", "");
    // this washer answered 0xff to a power-off three times, each while it had nothing to power off
    d.feed(a::framed([0x20, 0x00, 0x24, 0xff]));
    let reject = d.event("reject");
    expect(reject != (), "a rejection");
    expect(reject.contains("power_off") && reject.contains("refused"), reject);
}

fn test_a_record_of_another_length_reports_nothing() {
    let d = device();
    d.feed(a::record(0x20, 0xeb, 0x00, blob(33)));
    expect_eq(d.property("status"), ());
}

fn test_dropping_the_device_marks_it_unavailable() {
    let d = device();
    d.feed(idle());
    d.drop_device();
    expect_eq(d.property("available"), "false");
}

fn test_the_descriptor_offers_fourteen_startable_courses_and_no_downloaded_one() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc["class"], "washing_machine");
    let options = desc.props.course_select.options;
    expect_eq(options.len(), 14);
    expect(!options.contains("downloaded_course"), "the dial's downloaded position is not startable");
    expect_eq(desc.props.start.requires, "remote_start");
}

fn test_the_host_holds_start_back_until_remote_start_is_armed() {
    let d = device();
    let disarmed = heavy_duty_wash();
    disarmed[14] = disarmed[14] & 0xdf;
    d.start();
    d.feed(status(disarmed));
    d.set("power_off", "");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject").contains("requires remote_start"), "the host names the requirement");
    d.feed(status(heavy_duty_wash())); // armed
    d.set("power_off", "");
    expect_eq(d.sent().len(), 1);
}
