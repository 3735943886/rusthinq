// LG refrigerator 2RSFL2DBN3K_Z: device -> IL driver. An AABB (`AA len .. checksum BB`)
// appliance; nothing is asked for on its own, so `start` sends the same status query the LG
// app sends on connect. Frames are discriminated by inner bytes 0-1 (always 0x10, then):
//   0xEB  the answer to the query: one status record
//   0xEC  a spontaneous update: the previous record then the current one, stacked
//   0xC5  an unprompted report the appliance sends on its own every ~15 minutes (never asked
//         for): a 28-byte record, mostly unidentified, but offsets 9 and 15 are confirmed live
//         as today's cumulative fridge-door and freezer-door open counts (see door_stats()) --
//         this is where the LG app's per-compartment door-open counts actually come from; the
//         0xEC/0xEB status record carries only a single "any door open" bit (offset 7 below),
//         with no per-compartment information anywhere in it
//   0x3E  a second unprompted report on the same ~15-minute cadence, 7 bytes: a big-endian
//         uint16 (offsets 2-3) that is this report's energy delta, a big-endian uint16
//         (offsets 4-5) that is the running total for the day (each report's total is exactly
//         the previous total plus that report's delta -- confirmed exactly across 12 live
//         reports), and a one-byte sequence number (offset 6, increments with every report,
//         shared with nothing else). The running total matches the LG app's own "전력
//         사용량" (energy monitoring) figure exactly (503 on the wire, 503 Wh in the app), so
//         the unit is Wh and this is genuinely the appliance's power consumption -- see
//         energy_today() below. Resets on some cycle not yet observed, presumably daily
//
// Status record offsets (index within the record, matching this model's own field layout;
// the fields beyond these were never exercised in a real capture, so only these are
// published, same discipline as this fleet's other AABB drivers):
//   0  monStatus (unidentified)
//   1  fridgeSetpoint   -- wire, see fridge_temp()
//   2  freezerSetpoint  -- wire, see freezer_temp()
//   3  expressFreeze    -- 1 off, 2 on
//   7  anyDoorOpen      -- 1 open; 0 and (per this model family's own documentation) 2 both
//                          mean closed, so only 1 counts, matching the value this driver
//                          reports. Confirmed live it really is just one shared bit: opening
//                          and closing three different physical doors in a row (both freezer
//                          doors, then the fridge door) produced six identical transitions on
//                          this one byte and nothing else changed anywhere in the record --
//                          the per-compartment counts the LG app shows come from the 0xC5
//                          report instead (see door_stats() below), not from here
//   8  tempUnit         -- 0 Fahrenheit, 1 Celsius; the unit the appliance's own display
//                          (and so its wire encoding) is currently set to
//   17 smartCare        -- 0 off, 1 on. Offset 4 moves in lockstep (2 when off, 7 when on --
//                          confirmed live, matching a sibling model's own documentation of it
//                          as a separate "Pure N Fresh mode" field); it is not published as
//                          its own property since it carries no information offset 17 doesn't.
//                          The write travels the same F0 17 way as everything else here (acked
//                          and reflected in the next status frame) but is exposed read-only:
//                          kept as a sensor rather than a control
//   30 nightGlare       -- night-glare-prevention (LCD dimming) mode; 0 off, 2 sunset-to-
//                          sunrise, 3 a custom-time schedule is active, all three confirmed
//                          live. Exposed read-only: its write (a separate short F0 10 02
//                          frame, byte 3 selecting the mode -- 0/1/2 for off/sunset-to-
//                          sunrise/custom, confirmed live for every transition between them)
//                          also carries, for the custom schedule, a start/end time and LCD
//                          brightness this driver decoded but does not expose, and, for
//                          sunset-to-sunrise, two bytes that changed between two live captures
//                          of the same mode (likely a computed sunset/sunrise time, not a
//                          user-set one) and so are not understood either way. None of this is
//                          ever echoed back in the status record, so this driver only offers
//                          the plain mode name this offset itself reports
//   84 doorAlarmMute    -- the app's "temporarily silence the door-open alarm" toggle;
//                          0 off, 1 on. Exposed read-only. Offset 85 (how long the silence
//                          lasts, 5-15 minutes) is confirmed writable the same F0 17 way,
//                          independently of offset 84, but is not exposed here
//   92-94 aiSaving      -- AI Saving Mode. Offset 93 alone carries the state: 255 off, 1
//                          balanced saving, 2 max saving; offsets 92 and 94 are redundant
//                          (0 when off, 1 at either level) and not published separately.
//                          Its write command is not F0 17: it is a short, separate F0 10
//                          frame (see ai_saving_frame() below), acked with an inner 0x67
//                          frame (not the 0x17 every F0 17 write acks with) followed by the
//                          usual spontaneous 0xEC carrying the new state -- confirmed live
//                          for all three transitions (off -> balanced -> max -> off).
//                          The same F0 10 frame carries an active-hours window that only
//                          means anything in max mode (byte 4 of the frame, "03" for max,
//                          selects this; "off"/"balanced" zero-fill the rest of the record):
//                          bytes 8/9 are the window's start hour/minute, bytes 14/15 the end
//                          hour/minute, both confirmed live by single-field nudges (-1 hour
//                          on both start and end together, +1 minute on both together). This
//                          window is never echoed in the 0xEC/0xEB status record (every write
//                          to it only ever got the ack, no spontaneous status followed), so
//                          unlike everything else here it has no confirmed read path -- see
//                          ai_saving_max_schedule's write handling below
//
// Verified against a real unit: a 0xEB reply captured live (offsets 1/2/3/7/8/17 = 5, 4, 1,
// 0, 1, 0 -> fridge 3°C, freezer -18°C, express freeze off, door closed, Celsius, Smart
// Care off, all plausible defaults) and every property below toggled once from the LG app
// while this driver's decode was checked live against the resulting frames.
//
// The write command is F0 17 followed by a 118-byte record, captured verbatim from a live
// unit with every setting byte blanked to FF (0xFF means "leave this field as it is"); unlike
// a plain FF-everywhere-but-one-field template, this one carries a handful of fixed non-FF
// bytes at other offsets that a generic fill would lose, so the base is the literal capture.
// Every field this driver offers a write for also has a confirmed read path, echoed back by
// the appliance's own status frame -- except `ai_saving_max_schedule`, which has none (see the
// offset table above) and so is the only property here that echoes its own write instead.

import "aabb_common" as c;

fn f017_base() {
    "F017FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF000000FFFF00FFFFFFFF00FFFFFFFFFFFFFFFFFF00FFFFFF1EFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0AFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00FFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
}

// The unit the appliance's display (and so the wire values) currently uses, "C" until the
// first status frame says otherwise.
fn unit(ctx) {
    let u = ctx.state_get("unit");
    if u == () { "C" } else { u }
}

// Fridge setpoint, wire <-> actual: the same formula both ways (an involution), like the
// appliance's own encoding.
fn fridge_temp(u, x) { (if u == "F" { 44 } else { 8 }) - x }
fn freezer_temp(u, x) { (if u == "F" { 6 } else { -14 }) - x }

fn fridge_min(u) { if u == "F" { 33 } else { 1 } }
fn fridge_max(u) { if u == "F" { 43 } else { 7 } }
fn freezer_min(u) { if u == "F" { -7 } else { -23 } }
fn freezer_max(u) { if u == "F" { 5 } else { -15 } }
fn temp_unit(u) { if u == "F" { "°F" } else { "°C" } }

fn descriptor(ctx) {
    let u = unit(ctx);
    json_stringify(#{
        il: 0,
        kind: "fridge",
        label: "Refrigerator",
        vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            fridge_setpoint: #{ type: "number", rw: true, class: "temperature", unit: temp_unit(u),
                                 min: fridge_min(u), max: fridge_max(u), step: 1, label: "Fridge temperature" },
            freezer_setpoint: #{ type: "number", rw: true, class: "temperature", unit: temp_unit(u),
                                  min: freezer_min(u), max: freezer_max(u), step: 1, label: "Freezer temperature" },
            express_freeze: #{ type: "binary", rw: true, label: "Express freeze" },
            door: #{ type: "binary", class: "door" },
            // Cumulative for the current day (matches the LG app's own "1일" door-open-count
            // view); resets at some point not yet observed, presumably midnight.
            door_open_count_fridge: #{ type: "number", label: "Fridge compartment door opens today" },
            door_open_count_freezer: #{ type: "number", label: "Freezer compartment door opens today" },
            // Confirmed live against the LG app's own energy-monitoring figure (503 on the
            // wire, 503 Wh in the app). Cumulative for the current day, same as the door
            // counts above; resets on some cycle not yet observed.
            energy_today: #{ type: "number", class: "energy", unit: "Wh", label: "Energy used today" },
            smart_care: #{ type: "binary", label: "Smart Care+" },
            night_glare: #{ type: "select", options: ["off", "sunset_to_sunrise", "custom_schedule"],
                            label: "Night glare prevention" },
            door_alarm_mute: #{ type: "binary", label: "Door-open alarm temporarily muted" },
            ai_saving: #{ type: "select", rw: true, options: ["off", "balanced", "max"],
                          label: "AI Saving Mode" },
            // No confirmed read path (see the offset table above): never published from a
            // status frame, only echoed back with whatever was last written successfully.
            ai_saving_max_schedule: #{ type: "text", rw: true, requires: #{ prop: "ai_saving", "in": ["max"] },
                                        label: "AI Saving max-level active hours (HH:MM-HH:MM)" }
        }
    })
}

fn start(ctx) {
    ctx.send_raw(aabb_wrap([0xf0, 0xed, 0x12, 0x11, 0x01, 0x00, 0x00, 0x01, 0x04, 0x00]));
}

fn publish_config(ctx) { ctx.publish_il(descriptor(ctx)); }

fn on_drop(ctx) { ctx.publish_property("available", "false"); }

fn on_data(ctx, data) {
    let inner = c::inner(data, 0x10);
    if inner == () { return; }

    if inner[1] == 0xec {
        let block = (inner.len() - 2) / 2;
        if block < 18 { return; }
        status(ctx, inner, 2 + block); // the second, current half
    } else if inner[1] == 0xeb {
        let block = inner.len() - 2;
        if block < 18 { return; }
        status(ctx, inner, 2);
    } else if inner[1] == 0xc5 {
        if inner.len() < 16 { return; }
        door_stats(ctx, inner);
    } else if inner[1] == 0x3e {
        if inner.len() < 6 { return; }
        ctx.publish_property("energy_today", (inner[4] * 256 + inner[5]).to_string());
    }
}

// The unprompted ~15-minute report; only offsets 9 and 15 are understood (see the frame-family
// comment at the top of this file). The other bytes that move alongside them, confirmed live,
// are not published: two more counters whose deltas (this report's door test moved them by 4
// and 6) do not match either door count or their sum, so what they count is not identified.
fn door_stats(ctx, r) {
    ctx.publish_property("door_open_count_fridge", r[9].to_string());
    ctx.publish_property("door_open_count_freezer", r[15].to_string());
}

fn status(ctx, r, o) {
    ctx.publish_property("available", "true");

    let u = if r[o + 8] == 1 { "C" } else { "F" };
    if u != unit(ctx) {
        ctx.state_set("unit", u);
        ctx.publish_il(descriptor(ctx)); // the setpoints' unit and range just changed
    }

    ctx.publish_property("fridge_setpoint", fridge_temp(u, r[o + 1]).to_string());
    ctx.publish_property("freezer_setpoint", freezer_temp(u, r[o + 2]).to_string());
    ctx.publish_property("express_freeze", if r[o + 3] == 2 { "true" } else { "false" });
    ctx.publish_property("door", if r[o + 7] == 1 { "true" } else { "false" });
    ctx.publish_property("smart_care", if r[o + 17] == 1 { "true" } else { "false" });
    c::pub_name(ctx, "night_glare", night_glare_name(r[o + 30]));
    ctx.publish_property("door_alarm_mute", if r[o + 84] == 1 { "true" } else { "false" });
    c::pub_name(ctx, "ai_saving", ai_saving_name(r[o + 93]));
}

fn night_glare_name(v) {
    switch v { 0 => "off", 2 => "sunset_to_sunrise", 3 => "custom_schedule", _ => () }
}

fn ai_saving_name(v) {
    switch v { 255 => "off", 1 => "balanced", 2 => "max", _ => () }
}

// Nothing is published from a write: the appliance sends a status frame back, so the
// property follows what the appliance says and a declined setting simply stays where it was.
fn write_field(ctx, offset, value) {
    let msg = hex_decode(f017_base());
    msg[2 + 8] = if unit(ctx) == "F" { 0 } else { 1 }; // every write restates the current unit
    msg[2 + offset] = value;
    ctx.send_raw(aabb_wrap(msg));
}

// AI Saving Mode's write is a short, separate F0 10 frame, not the F0 17 record every other
// write here uses -- these three are the literal frames a live toggle through each transition
// sent, verbatim (the "max" one carries several bytes beyond a plain level byte that this
// driver does not otherwise understand, so, like f017_base(), it is used as-captured rather
// than reconstructed field by field).
fn ai_saving_frame(name) {
    switch name {
        "off" => "F010070D00000000000000000000000000",
        "balanced" => "F010070D01000000000000000000000000",
        "max" => "F010070D031A09160C00001A0916150000",
        _ => ()
    }
}

// The fixed part of the AI Saving Mode record when it is carrying a max-mode schedule (byte
// 4 = 0x03); bytes 8/9 (start hour/minute) and 14/15 (end hour/minute) are filled in by the
// caller. The bytes this driver does not understand (5-7, 10-13, 16) are copied verbatim from
// a live capture, same discipline as f017_base().
fn ai_saving_schedule_base() {
    "F010070D031A09160000001A0916000000"
}

// "HH:MM" at a fixed offset into "HH:MM-HH:MM", or `()` if it is not a valid one.
fn parse_hhmm(text, offset) {
    let h = c::to_number(text.sub_string(offset, 2));
    let m = c::to_number(text.sub_string(offset + 3, 2));
    if h == () || m == () { return (); }
    let hi = h.round().to_int();
    let mi = m.round().to_int();
    if hi < 0 || hi > 23 || mi < 0 || mi > 59 { return (); }
    [hi, mi]
}

fn on_set_property(ctx, prop, value) {
    let u = unit(ctx);
    switch prop {
        "fridge_setpoint" => {
            let n = c::to_number(value);
            if n == () { c::reject(ctx, prop, "not a number"); return; }
            write_field(ctx, 1, fridge_temp(u, n.round().to_int()));
        }
        "freezer_setpoint" => {
            let n = c::to_number(value);
            if n == () { c::reject(ctx, prop, "not a number"); return; }
            write_field(ctx, 2, freezer_temp(u, n.round().to_int()));
        }
        "express_freeze" => { write_field(ctx, 3, if c::to_bool(value) { 2 } else { 1 }); }
        "ai_saving" => {
            let f = ai_saving_frame(value);
            if f == () { c::reject(ctx, prop, "unknown value"); return; }
            ctx.send_raw(aabb_wrap(hex_decode(f)));
        }
        "ai_saving_max_schedule" => {
            if value.len() != 11 || value.sub_string(2, 1) != ":" || value.sub_string(5, 1) != "-"
                || value.sub_string(8, 1) != ":" {
                c::reject(ctx, prop, "expected HH:MM-HH:MM"); return;
            }
            let start = parse_hhmm(value, 0);
            let end = parse_hhmm(value, 6);
            if start == () || end == () { c::reject(ctx, prop, "expected HH:MM-HH:MM"); return; }
            let msg = hex_decode(ai_saving_schedule_base());
            msg[8] = start[0]; msg[9] = start[1];
            msg[14] = end[0]; msg[15] = end[1];
            ctx.send_raw(aabb_wrap(msg));
            // No status frame ever echoes this back (see the offset table above), so this is
            // the one property in this driver that publishes straight from its own write
            // rather than waiting for the appliance to say it back.
            ctx.publish_property(prop, value);
        }
        _ => { c::reject(ctx, prop, "unknown or read-only property"); }
    }
}
