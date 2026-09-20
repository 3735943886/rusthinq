// Tests of S3BF_POD_DN4.rhai (styler), against a real idle frame and the 46-byte Fine Dust start
// the LG app sent.

import "aabb_test" as a;

fn idle() { "aa4231ec001c000200020000003700000000000020000002cb000000000142420000001c000200020000003700000000000020000002cb000000000142420000cebb" }
// The whole 46-byte recipe, not just a course code.
fn fine_dust() { "1e010004000000000082000000000000000000000005c80000000000000000000003000002c80001c80028c80000" }

fn status(p) { a::status(0x31, p) }

fn drying() {
    let p = blob(28);
    p[0] = 55;  // drying, the state a real Indoor Dry run reported
    p[1] = 1;   // 1h 53m left
    p[2] = 53;
    p[3] = 2;
    p[5] = 30;  // Fine Dust
    p[14] = 0x08 | 0x20; // remote start, plus the bit set on every record
    p[17] = 0x02;
    p[18] = 0xcb;
    p
}

fn test_a_real_idle_frame_becomes_il_properties() {
    let d = device();
    d.feed(idle());
    expect_props(d, #{
        available: "true", power: "false", status: "off", course: "none", smart_course: "none",
        download_course: "trousers", remaining_time: "0", initial_time: "0", reserve_time: "0",
        energy: "715", error: "false", error_message: "OK", child_lock: "false",
        night_dry: "false", remote_start: "false"
    });
}

fn test_a_running_cycle_reports_its_times_and_the_remote_start_flag() {
    let d = device();
    d.feed(status(drying()));
    expect_props(d, #{
        status: "drying", power: "true", course: "fine_dust", remaining_time: "113",
        initial_time: "120", remote_start: "true"
    });
}

fn test_the_child_lock_and_night_dry_bits_and_an_error_decode() {
    let d = device();
    let p = drying();
    p[14] = p[14] | 0x01 | 0x02;
    p[6] = 31;
    d.feed(status(p));
    expect_props(d, #{
        child_lock: "true", night_dry: "true", error: "true",
        error_message: "Door closed - this course needs the door open"
    });
}

// ---- commands ----------------------------------------------------------------------

fn test_fine_dust_start_as_the_app_sent_it() {
    let d = device();
    let p = drying();
    p[0] = 0;
    p[5] = 30;
    d.feed(status(p)); // the dial is on Fine Dust
    d.set("start", "");
    let sent = a::inner_of(d, 0);
    expect_eq(hex_encode(sent.extract(0, 2)), "f026");
    expect_eq(hex_encode(sent.extract(2)), fine_dust());
}

fn test_every_startable_course_fills_the_frame_and_marks_the_first_duration() {
    let courses = [
        ["standard", 1], ["quick", 3], ["heavy", 5], ["wool_knitwear", 6], ["suits_coats", 7],
        ["sportswear", 8], ["sanitary_standard", 11], ["bedding", 12], ["drying_normal", 15],
        ["rain_snow", 22], ["padding_care", 28], ["fine_dust", 30], ["virus_care", 31],
        ["jeans", 32], ["fur_leather", 33], ["static_removal", 34]
    ];
    for c in courses {
        let name = c[0];
        let d = device();
        d.set("course_select", name);
        d.set("start", "");
        let payload = a::inner_of(d, 0).extract(2);
        expect_eq(payload.len(), 46, name);
        expect_eq(payload[0].to_int(), c[1], name);
        expect_eq(payload[3].to_int(), 0x04, name + ": initial bit, this is a start not a resume");
        expect_eq(payload[9].to_int() & 0x80, 0x80, name);
    }
}

fn test_resume_is_the_shorter_frame_empty_apart_from_the_course() {
    let d = device();
    let p = drying();
    p[0] = 3; // paused
    p[5] = 30;
    d.feed(status(p));
    d.set("start", "");
    let payload = a::inner_of(d, 0).extract(2);
    expect_eq(payload.len(), 45);
    expect_eq(payload[0].to_int(), 30);
    for i in 1..payload.len() { expect_eq(payload[i].to_int(), 0, "byte " + i); }
}

fn test_a_course_the_app_will_not_start_remotely_either_is_not_started() {
    let d = device();
    d.set("course_select", "indoor_dry_120"); // dries the room: needs the door open
    d.set("course_select", "nonsense");
    d.set("start", ""); // nothing valid selected
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_pause_and_power_off_are_the_short_controls_and_there_is_no_power_on() {
    let d = device();
    d.set("power_off", "");
    d.set("pause", "");
    expect_eq(a::inner_hex(d, 0), "f024010100");
    expect_eq(a::inner_hex(d, 1), "f024040100");
    // the cabinet acknowledges a power-on and then does nothing, so none is offered
    d.set("power", "true");
    d.set("power_on", "");
    expect_eq(d.sent().len(), 2);
}

fn test_a_refusal_is_reported_against_the_command_that_caused_it() {
    let d = device();
    d.set("pause", "");
    d.feed(a::framed([0x31, 0x00, 0x24, 0xff]));
    let reject = d.event("reject");
    expect(reject != (), "a rejection");
    expect(reject.contains("pause") && reject.contains("refused"), reject);
}

fn test_a_record_of_another_length_reports_nothing() {
    let d = device();
    d.feed(a::record(0x31, 0xeb, 0x00, blob(27)));
    expect_eq(d.property("status"), ());
}

fn test_the_descriptor_lists_the_courses_start_can_express() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc["class"], "styler");
    let options = desc.props.course_select.options;
    expect_eq(options.len(), 16);
    expect(!options.contains("indoor_dry_120"), "a course the app will not start remotely is not offered");
    expect_eq(desc.props.start.requires, "remote_start");
}

fn test_the_host_holds_start_back_until_remote_start_is_armed() {
    let d = device();
    let disarmed = drying();
    disarmed[14] = disarmed[14] & 0xf7;
    d.start();
    d.feed(status(disarmed));
    d.set("power_off", "");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject").contains("requires remote_start"), "the host names the requirement");
    d.feed(status(drying())); // armed
    d.set("power_off", "");
    expect_eq(d.sent().len(), 1);
}
