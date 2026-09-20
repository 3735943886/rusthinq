// LG front-load washer F24VDD (an AI DD 24 kg drum washer sold in Korea), deviceType 201:
// device -> IL driver. AABB frames start with 0x20 and are discriminated by the second byte:
//   0xEC  the regular status report, two stacked 34-byte monitoring records (see
//         monitoring_common); 0xEB / 0xE2 the same record on its own, after a (re)connect and
//         at the start of a cycle
//   0x00  the answer to a command
//   0x31  two board serial numbers in ASCII, each with a counter beside it
//   0xBD / 0xCD  ~405-byte diagnostic dumps, and 0x72, 0xD8, 0x19: not decoded. The dumps are
//         telemetry, not settings; the rest is sensor history in rolling groups of four to five
//         bytes that nothing names (the obvious reading, that the ramp climbing to ~88 is the
//         water temperature, dies on the evidence: it peaks at the same value in a 60 C wash
//         and a 95 C one).
//
// The offsets came from the appliance's own cloud (a frame whose payload byte i held the value
// i was injected and the cloud named each field; a second pass with i+100 confirmed each byte
// and is what moved the operating course off rec[20], where the first pass had looked like it
// lived, onto rec[22]); the option bits by a bit sweep; the course table by stepping rec[5]
// through every code. Checked against a real wash: 17 01 21 01 2c 06 00 03 03 04 03 ... 20 46
// is Running, 1h33m left of a 1h44m Heavy Duty at heavy soil / medium spin / 60 C / 3 rinses.

import "aabb_common" as c;
import "monitoring_common" as m;

fn dev() { 0x20 }
fn payload_len() { 34 }

// state and preState share this table (the model JSON gives both the same enum).
fn states() {
    [[0, "off"], [5, "initial"], [6, "paused"], [7, "error_auto_off"], [10, "reserved"],
     [20, "sensing"], [21, "draining"], [22, "detergent_dosing"], [23, "washing"], [24, "pre_wash"],
     [30, "rinsing"], [31, "rinse_hold"], [40, "spinning"], [50, "drying"], [60, "complete"],
     [61, "refreshing"], [83, "freeze_protection_initial"], [84, "freeze_protection_running"],
     [85, "freeze_protection_paused"], [101, "audible_diagnosis"]]
}

// The dial's course. The names are what the panel and the app print (from this model's
// language pack), not the enum identifiers, which have drifted: code 1 is STEAM_CLEANING on
// the wire but the machine calls it Steam Refresh.
fn courses() {
    [[0, "none"], [1, "steam_refresh"], [2, "allergy_care"], [3, "functional_wear"],
     [4, "economy_boil"], [5, "baby_wear"], [6, "heavy_duty"], [7, "cotton"], [8, "speed_wash"],
     [9, "quiet"], [10, "colour_care"], [11, "duvet"], [12, "lingerie_wool"],
     [13, "rinse_spin"], [14, "downloaded_course"], [15, "tub_clean"]]
}

// The course the machine is actually running (rec[22]); it differs from the dial whenever the
// dial sits on "downloaded course" or a smart course has been sent to it.
fn op_courses() {
    [[0, "none"], [1, "steam_cleaning"], [2, "allergy_care"], [3, "utility"], [4, "speed_wash"],
     [5, "cotton"], [6, "cotton_eco"], [7, "silent"], [8, "lingerie_wool"], [9, "duvet"],
     [10, "tub_clean"], [11, "baby_wear"], [12, "easy_care"], [13, "boil_wash"],
     [14, "heavy_duty"], [15, "cold_wash"], [16, "colour_care"], [17, "rinse_spin"],
     [18, "smart_energy"], [19, "blanket"], [20, "air_wash"], [21, "spin_only"],
     [22, "double_rinse"], [23, "speed_dry"], [24, "speed_wash_dry"], [25, "pet_care"],
     [26, "quick_tub_clean"]]
}

// The course sitting in the download slot (rec[20]). Confirmed against real traffic: this
// washer sends 0x39 (Kids' Wear) there, and 0x41 read as DEODORIZATION in the cloud snapshot.
fn download_courses() {
    [[0, "none"], [51, "cold_wash"], [52, "small_load"], [53, "skin_care"], [54, "rainy_day"],
     [55, "sweat_stains"], [56, "single_garments"], [57, "kids_wear"], [58, "shirts"],
     [59, "school_uniform"], [60, "static_reduce"], [62, "colour_care"], [63, "spin_only"],
     [64, "air_wash"], [65, "deodorisation"], [66, "bedding_care"], [67, "cloth_care"],
     [68, "smart_rinse"], [69, "eco_wash"], [70, "double_rinse"], [71, "heavy_duty"],
     [73, "wash_dry"], [74, "silent_wash"], [75, "wrinkle_care"], [77, "rinse_spin"],
     [78, "baby_wear"], [79, "sportswear"], [80, "towels"], [81, "shirts_1h"],
     [82, "allergy_care"], [83, "hygiene"], [84, "speed_wash"], [85, "pet_care"],
     [86, "quick_tub_clean"]]
}

fn soils() { [[0, "none"], [1, "light"], [2, "normal"], [3, "heavy"], [4, "pre_wash"], [5, "soaking"]] }
fn spins() {
    [[0, "no_spin"], [1, "very_low"], [2, "low"], [3, "medium"], [4, "high"], [5, "extra_high"]]
}
fn temps() {
    [[0, "none"], [1, "cold"], [2, "30"], [3, "40"], [4, "60"], [5, "95"]]
}

fn error_message(v) {
    switch v {
        0 => "OK", 1 => "Door lock error (DE2)", 2 => "Water supply error (IE)",
        3 => "Water drain error (OE)", 4 => "Out of balance error (UE)", 5 => "Overfill error (FE)",
        6 => "Water level sensor error (PE)", 7 => "Temperature sensor error (TE)",
        8 => "Locked motor error (LE)", 9 => "Communication error (CE)", 10 => "Heater error (dHE)",
        11 => "Power fail error (PF)", 12 => "Unknown error (FF)", 13 => "Unknown error (dCE)",
        14 => "Vibration sensor error (VS)", 15 => "EEPROM error", 16 => "Unknown error (PS)",
        17 => "Door open error (DE1)", 18 => "Unknown error (LOE)", 19 => "Door sensor error (DE4)",
        _ => "Unknown error (" + v + ")"
    }
}

// What each of the fourteen dial courses runs with: [operating course, soil, spin, temperature,
// rinses, steam, TurboShot]. A start command carries the whole cycle, not just a course code;
// these are the defaults the panel fills in, from the model JSON. Courses 14 (downloaded) has
// no preset and is never started. Confirmed byte for byte against Colour Care, Heavy Duty and
// Steam Refresh as the LG app sent them.
fn preset(course) {
    switch course {
        1 => [1, 2, 0, 0, 0, true, false],    // Steam Refresh
        2 => [2, 2, 3, 0, 4, true, false],    // Allergy Care
        3 => [3, 2, 1, 0, 3, true, false],    // Functional Wear
        4 => [13, 2, 3, 5, 3, false, false],  // Economy Boil
        5 => [11, 2, 3, 0, 4, true, false],   // Baby Wear
        6 => [14, 3, 3, 4, 3, false, false],  // Heavy Duty
        7 => [5, 2, 4, 3, 2, false, true],    // Cotton
        8 => [4, 2, 3, 0, 1, true, true],     // Speed Wash
        9 => [7, 2, 2, 3, 4, false, false],   // Quiet
        10 => [16, 2, 2, 2, 3, false, false], // Colour Care
        11 => [9, 2, 3, 1, 4, false, false],  // Duvet
        12 => [8, 2, 2, 1, 3, false, false],  // Lingerie / Wool
        13 => [17, 0, 3, 0, 1, false, false], // Rinse + Spin
        15 => [10, 2, 3, 4, 2, false, false], // Tub Clean
        _ => ()
    }
}

fn startable() { [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15] }

// 21 bytes: course, soil, spin, temperature, rinses, then the flags (byte 7: 0x20 remote start,
// 0x10 steam), option 2 (byte 8: 0x80 TurboShot), option 3 (byte 9: 0x20 the initial bit, which
// is what separates starting from resuming) and the operating course (byte 10).
fn start_payload(course, resume) {
    let p = preset(course);
    if p == () { return (); }
    let out = [];
    for i in 0..21 { out.push(0); }
    out[0] = course;
    out[1] = p[1];
    out[2] = p[2];
    out[3] = p[3];
    out[4] = p[4];
    out[7] = 0x20 | (if p[5] { 0x10 } else { 0 });
    out[8] = if p[6] { 0x80 } else { 0 };
    out[9] = if resume { 0 } else { 0x20 };
    out[10] = p[0];
    out
}

fn descriptor(ctx) {
    json_stringify(#{
        il: 0, kind: "washer", class: "washing_machine", label: "Washer", vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            power: #{ type: "binary", label: "Powered on" },
            status: #{ type: "select", options: m::names(states()) },
            course: #{ type: "select", label: "Course (dial)", options: m::names(courses()) },
            course_select: #{ type: "select", rw: true, label: "Course for Start",
                              options: startable().map(|v| m::name_of(courses(), v)) },
            start: #{ type: "trigger", requires: "remote_start" },
            pause: #{ type: "trigger", requires: "remote_start" },
            power_off: #{ type: "trigger", requires: "remote_start" },
            op_course: #{ type: "select", label: "Running course", options: m::names(op_courses()) },
            download_course: #{ type: "select", label: "Downloaded course",
                                options: m::names(download_courses()) },
            remaining_time: #{ type: "number", unit: "min" },
            initial_time: #{ type: "number", unit: "min", label: "Cycle length" },
            reserve_time: #{ type: "number", unit: "min", label: "Reserved finish time" },
            soil: #{ type: "select", label: "Soil level", options: m::names(soils()) },
            spin: #{ type: "select", options: m::names(spins()) },
            temp: #{ type: "select", label: "Water temperature", options: m::names(temps()) },
            rinse: #{ type: "number", label: "Rinse cycles" },
            // climbs at ~26 Wh/min while the washer heats; a 95 C boil wash finished at 1684 Wh
            energy: #{ type: "number", unit: "Wh", label: "Cycle energy" },
            load_level: #{ type: "number", label: "Load level (AI DD estimate, 0-4)" },
            tub_clean_count: #{ type: "number", label: "Cycles since Tub Clean" },
            error: #{ type: "binary" },
            error_message: #{ type: "text" },
            steam: #{ type: "binary" },
            turbo_shot: #{ type: "binary", label: "TurboShot" },
            crease_care: #{ type: "binary" },
            fresh_care: #{ type: "binary" },
            rinse_hold: #{ type: "binary" },
            add_load: #{ type: "binary", label: "Add Garment available" },
            favorite: #{ type: "binary", label: "Favourite course" },
            alarm: #{ type: "binary", label: "End-of-cycle sound" },
            door_lock: #{ type: "binary" },
            child_lock: #{ type: "binary" },
            remote_start: #{ type: "binary", label: "Remote start armed at the panel" }
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
    if chosen != () { ctx.publish_property("course_select", m::name_of(courses(), chosen)); }

    ctx.publish_property("power", if off { "false" } else { "true" });
    c::pub_name(ctx, "status", m::name_of(states(), state));
    // rec[5] is the course as selected on the dial; rec[22] is the one the machine is running
    c::pub_name(ctx, "course", m::name_of(courses(), b[p + 5]));
    c::pub_name(ctx, "op_course", m::name_of(op_courses(), b[p + 22]));
    c::pub_name(ctx, "download_course", m::name_of(download_courses(), b[p + 20]));

    ctx.publish_property("remaining_time", if off { "0" } else { (b[p + 1] * 60 + b[p + 2]).to_string() });
    ctx.publish_property("initial_time", if off { "0" } else { (b[p + 3] * 60 + b[p + 4]).to_string() });
    ctx.publish_property("reserve_time", if off { "0" } else { (b[p + 12] * 60 + b[p + 13]).to_string() });

    c::pub_name(ctx, "soil", m::name_of(soils(), b[p + 7]));
    c::pub_name(ctx, "spin", m::name_of(spins(), b[p + 8]));
    c::pub_name(ctx, "temp", m::name_of(temps(), b[p + 9]));
    ctx.publish_property("rinse", b[p + 10].to_string());
    ctx.publish_property("energy", (b[p + 17] * 256 + b[p + 18]).to_string());
    // rec[23] is the AI DD load estimate, 0-4; rec[21] counts wash cycles since the last Tub Clean
    ctx.publish_property("load_level", b[p + 23].to_string());
    ctx.publish_property("tub_clean_count", b[p + 21].to_string());

    let error = b[p + 6];
    ctx.publish_property("error", if error != 0 { "true" } else { "false" });
    ctx.publish_property("error_message", error_message(error));

    let flags = b[p + 14];
    ctx.publish_property("crease_care", c::bit(flags, 0x01));
    ctx.publish_property("fresh_care", c::bit(flags, 0x02));
    ctx.publish_property("rinse_hold", c::bit(flags, 0x04));
    ctx.publish_property("child_lock", c::bit(flags, 0x08));
    ctx.publish_property("steam", c::bit(flags, 0x10));
    ctx.publish_property("remote_start", c::bit(flags, 0x20));
    ctx.publish_property("favorite", c::bit(flags, 0x40));
    let opt2 = b[p + 15];
    ctx.publish_property("alarm", c::bit(opt2, 0x02));
    ctx.publish_property("door_lock", c::bit(opt2, 0x04));
    ctx.publish_property("add_load", c::bit(opt2, 0x40)); // the door may be opened to add a forgotten sock
    ctx.publish_property("turbo_shot", c::bit(opt2, 0x80));
    // rec[16] carries only the cloud's initialBit (0x20) and the AI DD panel LED (0x40); neither
    // is a setting worth a property. The initial bit does matter in a command (see start_payload).
}

fn on_set_property(ctx, prop, value) {
    if prop == "course_select" {
        let code = m::code_of(courses(), value);
        if code == () || !m::select(ctx, code, startable()) {
            c::reject(ctx, prop, "not a course this washer can start");
            return;
        }
        ctx.publish_property("course_select", value);
    } else if prop == "start" {
        let resume = ctx.state_get("state") == 6;
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
