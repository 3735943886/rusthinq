// Tests of 1WPU4CIGCR__2.rhai (water purifier), against real frames and the write frames the
// appliance accepted.

fn uv_done() { "AA3A12EC020103010101FFFF01FF030101FFFF08041000FF01FF00000001020003010101FFFF01FF030101FFFF08041000FF01FF0000000178BB" }
fn amount_250() { "AA3A12EC020003010101FFFF01FF030101FFFF08041000FF01FF00000001020003020101FFFF01FF030101FFFF08041000FF01FF0000000178BB" }
fn counters() { "AA12121F0000000000FC000000000000BCBB" }

fn test_the_captured_status_becomes_il_properties() {
    let d = device();
    d.start();
    d.feed(uv_done());
    expect_props(d, #{
        available: "true", status: "normal", tap_uv: "standby", water_selection: "cold",
        water_amount: "120ml", default_water: "cold", default_amount: "120ml", auto_care: "true",
        button_sound: "true", not_use_notice: "true",
        self_clean_next: "08-04 16:00" // the wire carries 08-04 16:00, in UTC
    });
}

fn test_only_the_current_record_counts_not_the_previous_one() {
    let d = device();
    // the previous record has the lamp running (cockState 1), the current one does not
    d.feed(uv_done());
    expect_eq(d.property("tap_uv"), "standby");
}

fn test_the_selected_amount_follows_the_panel_and_nothing_else_moves() {
    let d = device();
    d.feed(amount_250());
    expect_props(d, #{ water_amount: "250ml", water_selection: "cold", default_amount: "120ml" });
}

fn test_todays_totals_are_sixteen_bit() {
    let d = device();
    d.feed(counters());
    expect_props(d, #{ cold_water_today: "252", hot_water_today: "0" });
    // the same frame shape with 0x1234 on the cold tap; the checksum is not validated
    d.feed("AA12121F00000000123400000000000000BB");
    expect_eq(d.property("cold_water_today"), "4660");
}

// The frames the appliance was really sent, each of which changed its setting and came back
// in the next status frame.
fn test_writes_are_byte_for_byte_the_frames_the_appliance_accepted() {
    let d = device();
    d.set("default_amount", "250ml");
    d.set("button_sound", "false");
    let sent = d.sent();
    expect_eq(hex_encode(sent[0]), "aa20f017ffffffffffffffffffffff02ffffffffffffffffffffffffffffefbb");
    expect_eq(hex_encode(sent[1]), "aa20f017ffffffffffffffffffffffff00ffffffffffffffffffffffffffedbb");
}

fn test_one_setting_travels_at_a_time() {
    let d = device();
    d.set("auto_care", "true");
    let frame = d.sent()[0];
    let record = frame.extract(4, frame.len() - 6);
    expect_eq(record.len(), 26);
    expect_eq(record[20].to_int(), 1);
    let set = 0;
    for b in record { if b.to_int() != 0xff { set += 1; } }
    expect_eq(set, 1);
    d.set("default_water", "normal");
    expect_eq(d.sent()[1].extract(4)[10].to_int(), 2);
}

fn test_nothing_is_published_from_a_write() {
    let d = device();
    d.set("auto_care", "true");
    expect_eq(d.property("auto_care"), ());
}

fn test_a_bad_value_or_a_read_only_property_sends_nothing() {
    let d = device();
    d.set("default_water", "sparkling");
    d.set("default_amount", "continuous");
    d.set("water_selection", "cold");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_other_and_malformed_frames_are_ignored() {
    let d = device();
    d.feed(uv_done());
    let before = d.property("water_amount");
    for frame in [
        "AA0A12E2440102591DBB", // 0xE2, not decoded
        "AA0912AF0D0003D1BB",   // 0xAF, not decoded
        "001122",               // not an AABB frame at all
        "AA0612EC0000BB",       // 0x12EC but far too short
        "AA0A121F000000BB"      // counters, but not six of them
    ] {
        d.feed(frame);
    }
    expect_eq(d.property("water_amount"), before);
}

fn test_dropping_the_device_marks_it_unavailable() {
    let d = device();
    d.feed(uv_done());
    d.drop_device();
    expect_eq(d.property("available"), "false");
}

fn test_the_descriptor_is_published() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc["class"], "water_purifier");
    expect_eq(desc.props.self_clean_next.type, "text");
    expect_eq(desc.props.default_amount.rw, true);
}

fn test_the_host_validates_selects_and_binaries_against_the_descriptor() {
    let d = device();
    d.start();
    d.feed(uv_done());
    d.set("default_amount", "continuous"); // not among the writable options
    d.set("auto_care", "maybe");
    d.set("water_selection", "cold");      // read only
    expect_eq(d.sent().len(), 0);
    d.set("auto_care", "ON"); // normalised to true for the script
    expect_eq(d.sent().len(), 1);
}
