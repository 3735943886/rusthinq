// LG "2nd Mini Washer" (the pedestal drawer washer sold in Korea) Pd0F_F, deviceType 201:
// device -> IL driver. AABB frames start with 0x20 and carry a 25-byte monitoring record (see
// monitoring_common). It answers every command with a four-byte 0x00 frame and reports two
// board serial numbers in ASCII behind 0x31 (not decoded).
//
// This appliance switches its own Wi-Fi module off when it has nothing to do, so it spends most
// of its life offline and only appears on the wire while powered on or running. That is normal
// for the model, not a fault.
//
// The offsets were read out of the appliance's own cloud, not guessed: a frame whose payload
// byte i held the value i was injected and the cloud replied with its decoded state naming each
// field beside the value it had been handed; the option bits by a bit-sweep, the course codes
// by stepping rec[5] through every value and reading back the name.

import "aabb_common" as c;
import "monitoring_common" as m;

fn dev() { 0x20 }
fn payload_len() { 25 }

// The courses this unit's dial has and a start command can express, with what each runs with:
// [soil, spin, temperature, rinses, water level, water flow, soak]. A start command carries
// the whole cycle, not just a course code; these are the drawer's own panel defaults from the
// model JSON. The last three are zero on every course (this washer has no such controls).
// Confirmed on the wire: the app's Small Load start was 01 00 01 00 02 ... 05.
fn preset(course) {
    switch course {
        1 => [0, 1, 0, 2, 0, 0, 0],   // Small Load
        2 => [0, 1, 40, 3, 0, 0, 0],  // Underwear
        3 => [0, 1, 0, 2, 0, 0, 0],   // Wool
        4 => [0, 1, 90, 2, 0, 0, 0],  // Light Boil
        5 => [0, 1, 60, 4, 0, 0, 0],  // Baby Care
        6 => [0, 1, 0, 1, 0, 0, 0],   // Rinse + Spin
        7 => [0, 1, 60, 1, 0, 0, 0],  // Tub Clean
        _ => ()
    }
}

fn startable() { [1, 2, 3, 4, 5, 6, 7] }

// The start command, 15 bytes: the settings in the order the model JSON lists them, the reserve
// time and the smart-course slot, then four option bytes of which only the last has ever been
// non-zero. Resuming is the same frame with the initial bit (0x01) cleared, recorded from the
// app's own Resume button (0x04 in place of 0x05; 0x04 is remote start).
fn start_payload(course, resume) {
    let p = preset(course);
    if p == () { return (); }
    let out = [course, p[0], p[1], p[2], p[3], p[4], p[5], p[6], 0, 0, 0, 0, 0, 0, 0];
    out[14] = 0x04 | (if resume { 0 } else { 0x01 });
    out
}

fn course_name(v) {
    switch v {
        0 => "none", 1 => "small_load", 2 => "underwear", 3 => "wool", 4 => "light_boil",
        5 => "baby_care", 6 => "rinse_spin", 7 => "tub_clean", 8 => "spin_only", 9 => "hand_wash",
        10 => "speed_wash", 11 => "active_wear", 12 => "intimate", 13 => "cotton_eco_full",
        14 => "cotton_eco_half", 15 => "cotton_20",
        _ => ()
    }
}

fn course_code(name) {
    for v in 0..16 { if course_name(v) == name { return v; } }
    ()
}

fn state_name(v) {
    switch v {
        0 => "off", 1 => "initial", 2 => "paused", 3 => "sensing", 4 => "soaking", 5 => "washing",
        6 => "rinsing", 7 => "spinning", 8 => "complete", 9 => "reserved", 10 => "firmware_update",
        11 => "diagnosis",
        _ => ()
    }
}

// The two "cold" entries are distinct settings on this model (tap cold and the cold-wash
// option); everything else is the water temperature in degrees C.
fn temp_name(v) {
    switch v {
        0 => "cold", 1 => "cold_wash", 30 => "30", 35 => "35", 38 => "38", 40 => "40", 60 => "60",
        90 => "90",
        _ => ()
    }
}

fn error_message(v) {
    switch v {
        0 => "OK", 1 => "Water supply error (IE)", 2 => "Water drain error (OE)",
        3 => "Out of balance error (UE)", 4 => "Door open error (DE1)",
        5 => "Water level sensor error (PE)", 8 => "Door error (dO)", 9 => "Locked motor error (LE)",
        10 => "Water supply error (AE)", 11 => "Temperature sensor error (TE)",
        12 => "Overfill error (FE)", 16 => "Door lock error (DE2)", 27 => "Unknown error (FF)",
        36 => "Unknown error (E7)",
        _ => "Unknown error (" + v + ")"
    }
}

fn descriptor(ctx) {
    let courses = [];
    for v in 0..16 { courses.push(course_name(v)); }
    let startable_names = [];
    for v in startable() { startable_names.push(course_name(v)); }
    json_stringify(#{
        il: 0, kind: "washer", class: "mini_washer", label: "Mini washer", vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            power: #{ type: "binary", class: "running", label: "Powered on" },
            status: #{ type: "select", options: ["off", "initial", "paused", "sensing", "soaking",
                      "washing", "rinsing", "spinning", "complete", "reserved", "firmware_update",
                      "diagnosis"] },
            // "course" is where the dial sits; "course_select" is what Start will ask for. They
            // agree until one is changed here, and turning the dial brings them back together.
            course: #{ type: "select", label: "Course (dial)", options: courses },
            course_select: #{ type: "select", rw: true, label: "Course for Start",
                              options: startable_names },
            start: #{ type: "trigger", requires: "remote_start" },
            pause: #{ type: "trigger", requires: "remote_start" },
            power_off: #{ type: "trigger", requires: "remote_start" },
            remaining_time: #{ type: "number", class: "duration", unit: "min" },
            initial_time: #{ type: "number", class: "duration", category: "diagnostic", unit: "min", label: "Cycle length" },
            reserve_time: #{ type: "number", class: "duration", unit: "min", label: "Reserved finish time" },
            temp: #{ type: "select", label: "Water temperature",
                     options: ["cold", "cold_wash", "30", "35", "38", "40", "60", "90"] },
            rinse: #{ type: "number", series: "gauge", label: "Rinse cycles" },
            spin: #{ type: "binary" },
            error: #{ type: "binary", class: "problem", category: "diagnostic" },
            error_message: #{ type: "text", category: "diagnostic" },
            sterilize: #{ type: "binary" },
            warm_water: #{ type: "binary" },
            door_lock: #{ type: "binary", category: "diagnostic" },
            child_lock: #{ type: "binary", category: "diagnostic" },
            audible_diagnosis: #{ type: "binary", category: "diagnostic" },
            remote_start: #{ type: "binary", category: "diagnostic", label: "Remote start armed at the panel" }
        }
    })
}

// ---- host hooks -------------------------------------------------------------------

fn publish_config(ctx) { ctx.publish_il(descriptor(ctx)); }

fn on_drop(ctx) { ctx.publish_property("available", "false"); }

fn on_data(ctx, data) {
    let a = m::ack(data, dev());
    if a != () { m::on_ack(ctx, a); return; }

    let r = m::record(data, dev(), payload_len());
    if r == () { return; }
    let b = r.body;
    let p = r.p;

    ctx.publish_property("available", "true");
    let state = b[p];
    let off = state == 0;
    ctx.state_set("state", state);

    let chosen = m::follow(ctx, b[p + 5], startable());
    if chosen != () { ctx.publish_property("course_select", course_name(chosen)); }

    ctx.publish_property("power", if off { "false" } else { "true" });
    c::pub_name(ctx, "status", state_name(state));
    c::pub_name(ctx, "course", course_name(b[p + 5]));

    // Zeroed while off: the appliance keeps the last cycle's figures in these bytes, which would
    // otherwise read as a countdown frozen mid-cycle.
    ctx.publish_property("remaining_time", if off { "0" } else { (b[p + 1] * 60 + b[p + 2]).to_string() });
    ctx.publish_property("initial_time", if off { "0" } else { (b[p + 3] * 60 + b[p + 4]).to_string() });
    ctx.publish_property("reserve_time", if off { "0" } else { (b[p + 12] * 60 + b[p + 13]).to_string() });

    c::pub_name(ctx, "temp", temp_name(b[p + 9]));
    ctx.publish_property("rinse", b[p + 10].to_string());
    ctx.publish_property("spin", if b[p + 8] != 0 { "true" } else { "false" });

    let error = b[p + 6];
    ctx.publish_property("error", if error != 0 { "true" } else { "false" });
    ctx.publish_property("error_message", error_message(error));

    let flags = b[p + 14];
    ctx.publish_property("child_lock", c::bit(flags, 0x01));
    ctx.publish_property("audible_diagnosis", c::bit(flags, 0x02));
    ctx.publish_property("door_lock", c::bit(flags, 0x08));
    let opt2 = b[p + 15];
    ctx.publish_property("sterilize", c::bit(opt2, 0x02));
    ctx.publish_property("warm_water", c::bit(opt2, 0x10));
    ctx.publish_property("remote_start", c::bit(b[p + 16], 0x04));
    // Decoded by the cloud but not published: preState (rec[19]), the smart / downloaded course
    // slots (rec[20], rec[23]), and soil / water level / water flow / soak, for which the model
    // JSON gives exactly one value each: this drawer washer has no such controls. rec[11],
    // rec[17], rec[18] are named by nobody and have only ever been zero.
}

// The drawer obeys none of this unless Remote Start is armed on its own panel, a deliberate
// piece of LG's design that a command cannot switch on. While the appliance is asleep with its
// Wi-Fi module off nothing reaches it at all.
fn on_set_property(ctx, prop, value) {
    if prop == "course_select" {
        let code = course_code(value);
        if code == () || !m::select(ctx, code, startable()) {
            c::reject(ctx, prop, "not a course this drawer can start");
            return;
        }
        ctx.publish_property("course_select", value);
    } else if prop == "start" {
        let resume = ctx.state_get("state") == 2;
        let payload = start_payload(m::selected(ctx), resume);
        if payload == () { c::reject(ctx, prop, "no course selected"); return; }
        m::course_control(ctx, payload, prop);
    } else if prop == "pause" {
        m::short_control(ctx, m::control_pause(), prop);
    } else if prop == "power_off" {
        m::short_control(ctx, m::control_power_off(), prop);
    } else {
        c::reject(ctx, prop, "unknown or read-only property");
    }
}
