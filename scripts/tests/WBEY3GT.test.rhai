// Tests of WBEY3GT.rhai (cooktop), against real frames. The command frames are the ones the LG app
// sent, byte for byte.

import "aabb_test" as a;

fn idle() { "AA6642EC00000000000000000000000109010400013B37000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000095BB" }
fn power_on_locked() { "AA6642EC0100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000030000000000000000030000000000000000030000000000000000030000000000000000000000000000000000001EBB" }
fn armed() { "AA6642EC0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000900000001000001000000000000000000000000000000000000000000000000000000001CBB" }
fn cooking() { "AA6642EC00000000000000000000000009000000010000010000000000000000000000000000000000000000000000000000000000000000000000000000000109010000013B3B00000000000000000000000000000000000000000000000000000000009EBB" }
fn two_burners() { "AA6642EC0000000000000000000000010900020000003A000105010000013B3B000000000000000000000000000000000000000000000000000000000000000109010200003B39000105020000013A3B000000000000000000000000000000000000000054BB" }
fn locked_while_cooking() { "AA6642EC00000000000000000000000109010200003B39000105020000013A3B0000000000000000000000000000000000000000000003000000000000000003091C02000020390003051D0000011F3B000300000000000000000000000000000000000013BB" }
fn unlocked() { "AA6642EC000003000000000000000003091C02000020390003051D0000011F3B000300000000000000000000000000000000000000000000000000000000000109200200001C39000105210000011B3B000000000000000000000000000000000000000013BB" }
fn remote_start() { "AA6642EC00000000000000000000000109240200011839000000000000000000000000000000000000000000000000000000000002000000000000000000000109250200011739000000000000000000000000000000000000000000000000000000000011BB" }

// What the panel answers when the LG cloud asks for its status as it connects, captured live:
// the current record alone, everything off.
fn query_reply() { "AA3642EB0100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005BBB" }

fn burners() { ["left_rear", "left_front", "right"] }

// The body of the one frame that was sent, upper-case hex, framing stripped.
fn inner(d) {
    expect_eq(d.sent().len(), 1, "exactly one packet sent");
    hex_encode_upper(a::inner_of(d, 0))
}

fn test_the_answer_to_a_status_query_gives_the_state_at_startup() {
    let d = device();
    d.feed(query_reply());
    expect_props(d, #{ available: "true", cooking: "false", locked: "false", remote_start: "false" });
}

fn test_idle_every_burner_off_nothing_locked_no_remote_start() {
    let d = device();
    d.feed(idle());
    expect_props(d, #{ cooking: "false", locked: "false", remote_start: "false" });
    for k in burners() {
        let want = #{};
        want[k + "_state"] = "off";
        want[k + "_power_level"] = "0";
        want[k + "_cook_time"] = "0";
        want[k + "_remaining_time"] = "0";
        expect_props(d, want);
    }
}

fn test_locked_from_cold_the_lock_shows_but_nothing_is_cooking() {
    let d = device();
    d.feed(power_on_locked());
    expect_props(d, #{ locked: "true", cooking: "false" });
    for k in burners() { expect_eq(d.property(k + "_state"), "off"); }
}

fn test_power_level_set_before_the_ring_lights() {
    let d = device();
    d.feed(armed());
    expect_props(d, #{
        right_power_level: "9", right_state: "off", cooking: "false",
        right_remaining_time: "60" // the hour of auto-off is already on the clock
    });
}

fn test_cooking_state_level_and_both_timers() {
    let d = device();
    d.feed(cooking());
    expect_props(d, #{
        cooking: "true", right_state: "cooking", right_power_level: "9",
        right_cook_time: "0",        // 0:00:01
        right_remaining_time: "59",  // 0:59:59
        left_rear_state: "off", left_front_power_level: "0"
    });
}

fn test_two_rings_at_once_keep_separate_counters() {
    let d = device();
    d.feed(two_burners());
    expect_props(d, #{
        right_power_level: "9", right_cook_time: "2", right_remaining_time: "57",
        left_rear_power_level: "5", left_rear_cook_time: "0", left_rear_remaining_time: "59"
    });
}

fn test_locking_mid_cook_does_not_put_the_rings_out() {
    let d = device();
    d.feed(two_burners());
    d.feed(locked_while_cooking());
    expect_props(d, #{
        locked: "true", cooking: "true", right_state: "cooking", left_rear_state: "cooking",
        left_front_state: "off", // not alight: reads off, not locked
        right_cook_time: "2", right_remaining_time: "57", left_rear_remaining_time: "59"
    });
}

fn test_unlocking_leaves_the_cook_alone() {
    let d = device();
    d.feed(locked_while_cooking());
    d.feed(unlocked());
    expect_props(d, #{ locked: "false", cooking: "true", right_state: "cooking", left_rear_state: "cooking" });
}

fn test_the_panel_granting_remote_start_shows_up() {
    let d = device();
    d.feed(remote_start());
    expect_props(d, #{ remote_start: "true" });
}

fn test_switched_off_everything_clears() {
    let d = device();
    d.feed(cooking());
    d.feed(idle());
    expect_props(d, #{
        cooking: "false", right_state: "off", right_power_level: "0", right_remaining_time: "0"
    });
}

// The command frames below are the ones the LG app sent, byte for byte.

fn test_switching_a_burner_off_names_that_burner() {
    let d = device();
    d.feed(remote_start()); // right ring at 9, remote start granted
    d.set("right_off", "");
    expect_eq(inner(d), "F043200805000000" + "00000000");
}

fn test_setting_a_timer_restates_the_burner_power_level() {
    let d = device();
    d.feed(remote_start());
    d.set("right_remaining_time", "56");
    // burner 5, still at level 9, 0h 56m
    expect_eq(inner(d), "F043200805090038" + "00000000");
}

fn test_a_timer_past_the_appliance_maximum_is_refused_not_clamped_or_wrapped() {
    let d = device();
    d.start();
    d.feed(remote_start());
    d.set("right_remaining_time", "9999");
    // 11:59 is the most ControlTimerHour/Min can carry: the descriptor's maximum is 719
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject").contains("above the maximum of 719"), "the host names the maximum");
    d.set("right_remaining_time", "719");
    expect_eq(inner(d), "F043200805090B3B" + "00000000");
}

fn test_the_host_enforces_requires_remote_start_before_the_script_is_asked() {
    let d = device();
    d.start();
    d.feed(cooking()); // cooking, but remote start is not granted
    d.set("right_off", "");
    d.set("power_off", "");
    d.set("right_remaining_time", "30");
    expect_eq(d.sent().len(), 0);
    expect_eq(d.event("reject"), "{\"code\":\"requires_unmet\",\"prop\":\"right_remaining_time\",\"reason\":\"requires remote_start\"}");
}

fn test_switching_the_whole_cooktop_off() {
    let d = device();
    d.feed(remote_start());
    d.set("power_off", "");
    expect_eq(inner(d), "F04400");
}

fn test_nothing_is_sent_while_the_panel_withholds_remote_start() {
    let d = device();
    d.feed(cooking()); // cooking, but remote start not granted
    d.set("right_off", "");
    d.set("power_off", "");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject").contains("remote start"), "the refusal names remote start");
}

fn test_nothing_is_sent_before_any_status_has_been_seen() {
    let d = device();
    d.set("power_off", "");
    expect_eq(d.sent().len(), 0);
}

fn test_a_timer_on_a_ring_that_is_not_lit_is_not_sent() {
    let d = device();
    d.feed(remote_start());
    // the left rear ring is off in that frame, and a level of 0 would mean "switch off"
    d.set("left_rear_remaining_time", "30");
    expect_eq(d.sent().len(), 0);
    expect(d.event("reject") != (), "a reject event was published");
}

fn test_no_command_can_light_a_ring() {
    let d = device();
    d.feed(remote_start());
    for prop in ["right_power_level", "left_rear_state", "cooking", "right_on"] {
        d.set(prop, "9");
    }
    expect_eq(d.sent().len(), 0);
}

fn test_other_and_malformed_frames_are_ignored() {
    let d = device();
    d.feed(cooking());
    let before = d.property("right_state");
    for frame in [
        "AA12426500000000000000000000000432BB",   // 0x65, switch-off, not decoded
        "AA13427207E40A0000000000000000000033BB", // 0x72, not decoded
        "001122",                                 // not an AABB frame at all
        "AA0642EC0000BB"                          // 0x42EC but far too short
    ] {
        d.feed(frame);
    }
    expect_eq(d.property("right_state"), before);
}

fn test_the_descriptor_groups_burner_properties_and_marks_the_controls() {
    let d = device();
    d.start();
    let desc = d.descriptor();
    expect_eq(desc.kind, "cooktop");
    expect_eq(desc.props.right_state.group, "right");
    expect_eq(desc.props.right_off.type, "trigger");
    expect_eq(desc.props.right_remaining_time.requires, "remote_start");
    expect_eq(desc.props.power_off.requires, "remote_start");
}
