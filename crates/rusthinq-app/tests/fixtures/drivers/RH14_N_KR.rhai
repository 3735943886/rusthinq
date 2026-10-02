// LG heat-pump dryer RH14_N_KR ("Hisen Refresh 27inch 14kg"), deviceType 202: device -> IL
// driver. AABB frames start with 0x30 and carry a 25-byte monitoring record (see
// monitoring_common); 0x72 heartbeats are not decoded. The dryer answers every command with a
// four-byte 0x00 frame, repeats its record behind 0xE2 when a cycle starts, and reports two
// board serial numbers in ASCII behind 0x31 (not decoded).
//
// The offsets came from the appliance's own cloud (a frame whose payload byte i held the value
// i was injected and the cloud named each field), cross-checked against a real drying cycle:
// 02 00 18 01 23 07 00 03 02 02 ... is Running, 24 min left of a 1h35m Cotton Normal at Iron
// dry level on Eco Hybrid Normal, matching the cloud's snapshot field for field.

import "aabb_common" as c;
import "monitoring_common" as m;
import "il_common" as il;

fn dev() { 0x30 }
fn payload_len() { 25 }

// What each course this unit's dial has runs with: [dry level, Eco Hybrid]. A start command
// carries the settings as well as the course; these are the defaults the panel would fill in,
// from the model JSON. The wider course table exists so a sibling model reports a name, not so
// this one can start a cycle it has no program for. Confirmed on the wire for Cotton Normal:
// the app's start was 07 03 02 ..., and the record a second later reported exactly that.
fn preset(course) {
    switch course {
        2 => [0, 2], 4 => [0, 3], 5 => [3, 2], 7 => [3, 2], 8 => [0, 1], 9 => [0, 3],
        11 => [0, 2], 12 => [0, 1], 13 => [0, 0], 14 => [0, 2], 15 => [0, 3], 16 => [0, 3],
        17 => [0, 2], 18 => [0, 3], 19 => [0, 3], 20 => [0, 3], 21 => [0, 0],
        _ => ()
    }
}

fn startable() { [2, 4, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21] }

// 14 bytes: course, dry level, Eco Hybrid, then zeros, with byte 10 carrying the option bits
// (0x01 remote start, 0x02 the initial bit that separates starting from resuming a paused cycle).
fn start_payload(course, resume) {
    let p = preset(course);
    if p == () { return (); }
    let out = [course, p[0], p[1], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    out[10] = 0x01 | (if resume { 0 } else { 0x02 });
    out
}

fn course_name(v) {
    switch v {
        0 => "none", 1 => "refresh", 2 => "towels", 3 => "jeans", 4 => "bulky_items",
        5 => "easy_care", 6 => "mixed_fabrics", 7 => "cotton_normal", 8 => "sportswear",
        9 => "quick_dry", 10 => "delicates", 11 => "wool", 12 => "rack_dry", 13 => "cool_air",
        14 => "warm_air", 15 => "bedding_brush", 16 => "allergy_care", 17 => "power",
        18 => "condenser_care", 19 => "tub_clean", 20 => "padding_refresh", 21 => "time_dry",
        22 => "water_repellent", 23 => "baby_wear", 24 => "small_load", 25 => "cotton_plus",
        26 => "perm_press", 27 => "pet_care",
        _ => ()
    }
}

fn course_code(name) {
    for v in 0..28 { if course_name(v) == name { return v; } }
    ()
}

fn state_name(v) {
    switch v {
        0 => "off", 1 => "initial", 2 => "running", 3 => "paused", 4 => "complete", 5 => "error",
        8 => "audible_diagnosis", 100 => "reserved",
        _ => ()
    }
}

// The step within the cycle, which the state byte alone does not distinguish: everything from
// the first moisture sensing to the anti-crease tumble at the end reports state running.
fn process_name(v) {
    switch v {
        0 => "sensing", 1 => "steam", 2 => "drying_1", 3 => "drying_2", 4 => "drying_3",
        5 => "cooling", 6 => "anti_crease", 7 => "end",
        _ => ()
    }
}

fn dry_level_name(v) {
    switch v {
        0 => "none", 1 => "damp_dry", 2 => "less_dry", 3 => "iron_dry", 4 => "cupboard_dry",
        5 => "very_dry",
        _ => ()
    }
}

fn eco_hybrid_name(v) {
    switch v { 0 => "none", 1 => "eco", 2 => "normal", 3 => "turbo", _ => () }
}

fn error_message(v) {
    switch v {
        0 => "OK", 1 => "Temperature sensor error (TE1)", 2 => "Temperature sensor error (TE2)",
        7 => "Communication error (CE1)", 13 => "Drain pump error", 14 => "Water tank empty",
        15 => "Door error", 17 => "Filter missing", 19 => "Unknown error (F1)",
        20 => "Locked motor error (LE2)", 21 => "Water supply error (AE)",
        30 => "Locked motor error (LE1)", 37 => "Door sensor error (DE4)",
        39 => "Locked motor error (LE3)", 42 => "Door lock error (DE2)",
        _ => "Unknown error (" + v + ")"
    }
}

fn descriptor(ctx) {
    let courses = [];
    for v in 0..28 { courses.push(course_name(v)); }
    let startable_names = [];
    for v in startable() { startable_names.push(course_name(v)); }
    json_stringify(#{
        il: 0, kind: "dryer", label: "Dryer", vendor: "LG", model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            power: #{ type: "binary", class: "running", label: "Powered on" },
            status: #{ type: "select", options: ["off", "initial", "running", "paused", "complete",
                                                  "error", "audible_diagnosis", "reserved"] },
            process: #{ type: "select", label: "Cycle step",
                        options: ["none", "sensing", "steam", "drying_1", "drying_2", "drying_3",
                                  "cooling", "anti_crease", "end"] },
            course: #{ type: "select", label: "Course (dial)", options: courses },
            course_select: #{ type: "select", rw: true, label: "Course for Start",
                              options: startable_names },
            start: #{ type: "trigger", requires: "remote_start" },
            pause: #{ type: "trigger", requires: "remote_start" },
            power_off: #{ type: "trigger", requires: "remote_start" },
            remaining_time: #{ type: "number", class: "duration", unit: "min" },
            initial_time: #{ type: "number", class: "duration", category: "diagnostic", unit: "min", label: "Cycle length" },
            reserve_time: #{ type: "number", class: "duration", unit: "min", label: "Reserved finish time" },
            dry_level: #{ type: "select", options: ["none", "damp_dry", "less_dry", "iron_dry",
                                                    "cupboard_dry", "very_dry"] },
            eco_hybrid: #{ type: "select", options: ["none", "eco", "normal", "turbo"] },
            // climbs during the cycle and resets to 0 at the next one
            energy: #{ type: "number", class: "energy", series: "counter", unit: "Wh", label: "Cycle energy" },
            error: #{ type: "binary", class: "problem", category: "diagnostic" },
            error_message: #{ type: "text", category: "diagnostic" },
            steam: #{ type: "binary" },
            anti_crease: #{ type: "binary" },
            hand_iron: #{ type: "binary", label: "Easy iron" },
            damp_dry_beep: #{ type: "binary" },
            self_clean: #{ type: "binary", label: "Condenser self-clean" },
            reservation: #{ type: "binary" },
            child_lock: #{ type: "binary", category: "diagnostic" },
            remote_start: #{ type: "binary", category: "diagnostic", label: "Remote start armed at the panel" }
        }
    })
}

// ---- host hooks -------------------------------------------------------------------

fn publish_config(ctx) { il::publish_il(ctx, descriptor(ctx)); }

fn on_drop(ctx) { il::publish_property(ctx, "available", "false"); }

fn on_data(ctx, data) {
    let a = m::ack(data, dev());
    if a != () { m::on_ack(ctx, a); return; }

    let r = m::record(data, dev(), payload_len());
    if r == () { return; }
    let b = r.body;
    let p = r.p;

    il::publish_property(ctx, "available", "true");
    let state = b[p];
    let off = state == 0;
    ctx.state_set("state", state);

    let chosen = m::follow(ctx, b[p + 5], startable());
    if chosen != () { il::publish_property(ctx, "course_select", course_name(chosen)); }

    il::publish_property(ctx, "power", if off { "false" } else { "true" });
    c::pub_name(ctx, "status", state_name(state));
    il::publish_property(ctx, "process", if off { "none" } else { let n = process_name(b[p + 9]); if n == () { "none" } else { n } });
    c::pub_name(ctx, "course", course_name(b[p + 5]));

    il::publish_property(ctx, "remaining_time", if off { "0" } else { (b[p + 1] * 60 + b[p + 2]).to_string() });
    il::publish_property(ctx, "initial_time", if off { "0" } else { (b[p + 3] * 60 + b[p + 4]).to_string() });
    il::publish_property(ctx, "reserve_time", if off { "0" } else { (b[p + 12] * 60 + b[p + 13]).to_string() });

    c::pub_name(ctx, "dry_level", dry_level_name(b[p + 7]));
    c::pub_name(ctx, "eco_hybrid", eco_hybrid_name(b[p + 8]));
    // rec[17:19]: energy used by the running cycle, big-endian, in Wh. The counter climbs at
    // about 12 Wh/min during a cycle, a heat pump's ~0.7 kW.
    il::publish_property(ctx, "energy", (b[p + 17] * 256 + b[p + 18]).to_string());

    let error = b[p + 6];
    il::publish_property(ctx, "error", if error != 0 { "true" } else { "false" });
    il::publish_property(ctx, "error_message", error_message(error));

    let flags = b[p + 14];
    il::publish_property(ctx, "reservation", c::bit(flags, 0x01));
    il::publish_property(ctx, "anti_crease", c::bit(flags, 0x02));
    il::publish_property(ctx, "child_lock", c::bit(flags, 0x10));
    il::publish_property(ctx, "self_clean", c::bit(flags, 0x20));
    il::publish_property(ctx, "damp_dry_beep", c::bit(flags, 0x40));
    il::publish_property(ctx, "hand_iron", c::bit(flags, 0x80));
    il::publish_property(ctx, "remote_start", c::bit(b[p + 15], 0x01));
    il::publish_property(ctx, "steam", c::bit(b[p + 16], 0x08));
}

fn set_property(ctx, prop, value) {
    if prop == "course_select" {
        let code = course_code(value);
        if code == () || !m::select(ctx, code, startable()) {
            c::reject(ctx, prop, "not a course this dryer can start");
            return;
        }
        il::publish_property(ctx, "course_select", value);
    } else if prop == "start" {
        let resume = ctx.state_get("state") == 3;
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

// Commands are validated against the IL descriptor here, in the scripts, not by rusthinq.
fn on_set_property(ctx, prop, value) {
    let value = il::validate(ctx, prop, value);
    if value != () { set_property(ctx, prop, value); }
}
