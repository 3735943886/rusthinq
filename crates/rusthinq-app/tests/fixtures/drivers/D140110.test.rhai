// Tests of D140110.rhai (dishwasher, read only), against nine real frames of a full cycle.

fn ready() { "AA3A32EC0018000000012100000001000012000000000000000000000000081801000001210100012100001000020009000000000000000184BB" }
fn running() { "AA3A32EC00180100000121010001210000100002000900000000000000010018020200012101000121000010000200090000000000000001ACBB" }
fn child_lock() { "AA3A32EC001802020001210100011D000010000200090000000000000001001802020001210100011D000011000200090000000000000001A0BB" }
fn rinsing() { "AA3A32EC001802030001210100000A000010000200090000000000000001001802030001210100000900001000020009000000000000000198BB" }
fn drying() { "AA3A32EC0018020300012101000009000010000200090000000000000001001802040001210100000900001000020009000000000000000198BB" }
fn door_open() { "AA3A32EC0018020400012101000002000010000200090000000000000001001802040001210100000200001200020009000000000000000197BB" }
fn finished() { "AA3A32EC0018020400012101000001000012000200090000000000000001001805050001210100000100001200020009000000000000000193BB" }
fn standby() { "AA3A32EC001805050001210100000100001200020009000000000000000108180400000121000000010000120002000900000000000000019EBB" }
fn off() { "AA3A32EC00180400000121000000010000120002000900000000000000010018000000012100000001000012000200090000000000000001EDBB" }

fn test_ready_to_start_the_course_and_the_full_cycle_time() {
    let d = device();
    d.feed(ready());
    expect_props(d, #{
        available: "true", status: "ready", process: "none", course: "auto", error: "none",
        running: "false",
        total_time: "93", // 1:33, the 93 minutes the app showed
        remaining_time: "93", reserve_time: "0", door: "false", child_lock: "false",
        chime: "true", rinse_refill: "false", salt_refill: "false", rinse_aid_level: "2",
        softening_level: "0"
    });
}

fn test_running() {
    let d = device();
    d.feed(running());
    expect_props(d, #{ status: "running", process: "washing", running: "true" });
}

fn test_the_phases_of_a_cycle_are_reported_apart_from_the_state() {
    let d = device();
    d.feed(rinsing());
    expect_props(d, #{ process: "rinsing", status: "running", remaining_time: "9" });
    d.feed(drying());
    expect_props(d, #{ process: "drying", status: "running" });
}

fn test_the_child_lock_does_not_disturb_anything_else() {
    let d = device();
    d.feed(running());
    d.feed(child_lock());
    expect_props(d, #{
        child_lock: "true", status: "running",
        chime: "true", // the same byte, another bit
        door: "false",
        remaining_time: "89" // 1:29
    });
}

fn test_the_auto_open_door_is_reported_without_ending_the_cycle() {
    let d = device();
    d.feed(drying());
    d.feed(door_open());
    expect_props(d, #{ door: "true", status: "running", process: "drying", child_lock: "false" });
}

fn test_finished_then_standby_then_off() {
    let d = device();
    d.feed(finished());
    expect_props(d, #{ status: "finished", running: "false" });
    d.feed(standby());
    expect_props(d, #{ status: "standby", course: "none" });
    d.feed(off());
    expect_props(d, #{ status: "off" });
}

fn test_other_frames_and_malformed_ones_are_ignored() {
    let d = device();
    d.feed(ready());
    for frame in [
        "AA0A32B2000000BB", // 0xB2, not decoded
        "001122",           // not an AABB frame
        "AA0632EC0000BB",   // 0x32EC but far too short
        // a record whose payload length byte is not 0x18
        "AA3A32EC00180000000121000000010000120000000000000000000000000819010000012101000121000010000200090000000000000001 84BB"
    ] {
        d.feed(frame);
    }
    expect_props(d, #{ status: "ready" });
}

fn test_it_is_read_only() {
    let d = device();
    d.set("running", "true");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_dropping_the_device_marks_it_unavailable() {
    let d = device();
    d.feed(ready());
    d.drop_device();
    expect_eq(d.property("available"), "false");
}

fn test_the_descriptor_is_published() {
    let d = device();
    d.start();
    let desc = json_parse(d.published("il/test-device"));
    expect_eq(desc["class"], "dishwasher");
    expect_eq(desc.props.remaining_time.unit, "min");
}

fn test_every_write_is_refused_by_il_common_because_no_property_is_writable() {
    let d = device();
    d.start();
    d.feed(ready());
    d.set("running", "true");
    d.set("status", "off");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject").contains("read-only property"), "the refusal says why");
}
