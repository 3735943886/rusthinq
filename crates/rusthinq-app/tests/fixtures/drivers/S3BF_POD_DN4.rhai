// LG Styler (steam clothing care cabinet) S3BF_POD_DN4 (nameplate "Essence_ESL"), deviceType
// 203: device -> IL driver. AABB frames start with 0x31 and carry a 28-byte monitoring record
// (see monitoring_common); 0x72 heartbeats are not decoded. The cabinet answers every command
// with a four-byte 0x00 frame and reports two board serial numbers in ASCII behind 0x31. Its
// 0xB2 frames repeat the cycle time and energy counter and then, in the longer variant, what
// looks like a history log in six-byte entries: left alone, nothing names those fields.
//
// The offsets were read out of the appliance's own cloud (a frame whose payload byte i held
// the value i was injected and the cloud named each field); the option bits by a bit sweep;
// the state and error bytes by a sweep of their own, code by code through 0..127, because the
// model JSON spells those two enums out as label dicts with no index (its key order is NOT the
// wire code: it lists PRESTEAM..FOTA as entries 10-19 while the appliance sends 50-59 and 98).
//
// There is no way to switch this cabinet ON, and the absence was measured: a newer styler of
// this family takes a power-on as `f0 24 01 01 01`, a remote-control switch as `f0 24 10 01 01`
// and its buzzer / end melody / night-care times as `f0 24 13` sub-commands. All four were sent
// to this appliance; it acknowledged each and did nothing, exactly as it does for a control type
// of 0x7f. Its model JSON lists only downloadCourse, offPower, startCourse, resumeCourse,
// pauseCourse and wakeup, and wakeup wakes the Wi-Fi module, not the cabinet.

import "aabb_common" as c;
import "monitoring_common" as m;

fn dev() { 0x31 }
fn payload_len() { 28 }

fn states() {
    [[0, "off"], [1, "initial"], [2, "running"], [3, "paused"], [4, "complete"], [5, "error"],
     [6, "diagnosis"], [7, "night_dry"], [8, "reserved"], [9, "sleep"],
     // 50-59 are the steps of a running cycle; a real Indoor Dry run reported 55
     [50, "pre_steam"], [51, "pre_heat"], [52, "steam"], [53, "stay"], [54, "cooling"],
     [55, "drying"], [56, "end_cooling"], [57, "sterilizing"], [58, "finishing"],
     [59, "complete_keeping_fresh"], [98, "firmware_update"]]
}

// Swept the same way. Code 31 is the one a user meets by accident: the courses that dry into
// the room refuse to start with the door shut.
fn error_message(v) {
    switch v {
        0 => "OK", 1 => "Temperature sensor error (TE1)", 2 => "Temperature sensor error (TE2)",
        3 => "Temperature sensor error (TE3)", 4 => "Temperature sensor error (TE4)",
        5 => "Temperature sensor error (TE5)", 6 => "System error (E1)", 7 => "System error (E2)",
        8 => "System error (E3)", 9 => "System error (E4)", 10 => "Motor error (LE2)",
        11 => "Water supply error (AE)", 18 => "Motor error (LE)", 25 => "Water drain error",
        26 => "Door open error", 31 => "Door closed - this course needs the door open",
        32 => "System error (E6)", 33 => "Power supply error (PS)", 34 => "Unknown error (IF)",
        _ => "Unknown error (" + v + ")"
    }
}

// Course codes come from the model JSON's own ConvertingRule (code -> name), which for this
// family is authoritative: the same table the cloud decoded an injected course=5 with (STRONG).
fn courses() {
    [[0, "none"], [1, "standard"], [3, "quick"], [5, "heavy"], [6, "wool_knitwear"],
     [7, "suits_coats"], [8, "sportswear"], [9, "download_course"], [10, "functional_wear"],
     [11, "sanitary_standard"], [12, "bedding"], [13, "kids"], [14, "dolls"],
     [15, "drying_normal"], [16, "drying_wool_knitwear"], [17, "time_dry_30"],
     [18, "time_dry_60"], [19, "time_dry_90"], [20, "time_dry_120"], [21, "time_dry_150"],
     [22, "rain_snow"],
     // dries the room, hence the door-open requirement
     [23, "indoor_dry_120"], [24, "indoor_dry_240"],
     [25, "night_care"], [26, "sanitary_professional"], [27, "uniform_care"],
     [28, "padding_care"], [29, "trouser_crease_care"], [30, "fine_dust"], [31, "virus_care"],
     [32, "jeans"], [33, "fur_leather"], [34, "static_removal"], [35, "kids_items"],
     [67, "silent_downloaded"], [78, "fur_leather_downloaded"]]
}

// Smart (downloadable) course codes, likewise from the model JSON's ConvertingRule.
fn smart_courses() {
    [[0, "none"], [61, "suits_uniforms"], [62, "scarves"], [63, "athletic_wear"],
     [64, "smoke_removal"], [65, "food_odour_removal"], [66, "trousers"], [67, "silent"],
     [68, "coat_warmer"], [69, "static_removal"], [70, "uniform_care"], [71, "stored_items"],
     [72, "allergy_care"], [73, "blanket_warmer"], [74, "uniform_drying"], [75, "dress_shirts"],
     [76, "rain_snow_drying"], [77, "spot_drying"], [78, "fur_leather"], [90, "swimwear_drying"],
     [91, "fine_dust_removal"], [92, "virus_care"], [93, "jeans_care"],
     [94, "baby_clothes_sanitary"], [95, "doll_sanitary"], [96, "wool_delicate_drying"],
     [97, "rainy_days"], [98, "uniform"], [99, "padding_care"], [100, "thin_padding_care"],
     [101, "thick_padding_care"], [102, "silk_care"]]
}

// A start command carries the whole recipe (the styler is told the entire cycle), 36 bytes per
// course, copied from what the LG app sent and checked against the model JSON. Only the courses
// the app was seen to start remotely are here; the timed indoor dries are refused by the app too.
fn recipe(course) {
    switch course {
        1 => "02000000000006000003b40001b4001bb400000000000000000000000000000000000000",  // Standard
        3 => "02000000000003000000000001b4000eb400000000000000000000000000000000000000",  // Quick
        5 => "02006900000006006905c86903c8002bb400000000000000000000000000000000000000",  // Heavy
        6 => "020000000000040000000000010000147800000000000000000000000000000000000000",  // Wool / Knitwear
        7 => "02000000000005000002b40001b40018b400000000000000000000000000000000000000",  // Suits / Coats
        8 => "020000000000030000000000000000140000000000000000040000030000017800157800",  // Sportswear
        11 => "0200002300000500000a00000300001c7800000000000000000000000000000000000000", // Sanitary Standard
        12 => "0200002300000500000a0000030000260000000000000000000000000000000000000000", // Bedding
        15 => "0000000000000000000000000000005a7800000000000000000000000000000000000000", // Drying (Normal)
        22 => "0200000000000300000000000100002d7800000000000000000000000000000000000000", // Rain / Snow
        28 => "000000000000000000000000000000f00000000000000000000000000000000000000000", // Padding Care
        30 => "02000000000000000000000005c80000000000000000000003000002c80001c80028c800", // Fine Dust
        31 => "0200002300000500000a00000300003a0000000000000000000000000000000000000000", // Virus Care
        32 => "0200002300000500000a00000300002b0000000000000000000000000000000000000000", // Jeans
        33 => "0000000000000000000000000000001e7800000000000000000000000000000000000000", // Fur / Leather
        34 => "020000000000020000000000017800057800000000000000000000000000000000000000", // Static Removal
        _ => ()
    }
}

fn startable() { [1, 3, 5, 6, 7, 8, 11, 12, 15, 22, 28, 30, 31, 32, 33, 34] }

// 46 bytes (zero-filled): course, download slot 1, smart course 0, the flags byte (the cloud's initialBit
// 0x04: this is a start, not a resume), reserve h/m, then the recipe with its marker bit on
// its first byte. Resuming is a shorter 45-byte frame, empty apart from the course.
fn start_payload(course) {
    let hex = recipe(course);
    if hex == () { return (); }
    let out = [course, 1, 0, 0x04, 0, 0, 0, 0, 0];
    // (elements of a blob are bytes, not integers: convert before mixing with integer ops)
    for b in hex_decode(hex) { out.push(b.to_int()); }
    out[9] = out[9] | 0x80;
    while out.len() < 46 { out.push(0); } // the recipe is 36 bytes in a zero-filled 46
    out
}

fn resume_payload(course) {
    let out = [course];
    for i in 0..44 { out.push(0); }
    out
}

fn descriptor(ctx) {
    json_stringify(#{
        il: 0, kind: "clothing_care", class: "styler", label: "Styler", vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            power: #{ type: "binary", class: "running", label: "Powered on" },
            status: #{ type: "select", options: m::names(states()) },
            course: #{ type: "select", label: "Course (dial)", options: m::names(courses()) },
            course_select: #{ type: "select", rw: true, label: "Course for Start",
                              options: startable().map(|v| m::name_of(courses(), v)) },
            start: #{ type: "trigger", requires: "remote_start" },
            pause: #{ type: "trigger", requires: "remote_start" },
            power_off: #{ type: "trigger", requires: "remote_start" },
            smart_course: #{ type: "select", options: m::names(smart_courses()) },
            download_course: #{ type: "select", category: "diagnostic", label: "Downloaded course",
                                options: m::names(smart_courses()) },
            remaining_time: #{ type: "number", class: "duration", unit: "min" },
            initial_time: #{ type: "number", class: "duration", category: "diagnostic", unit: "min", label: "Cycle length" },
            reserve_time: #{ type: "number", class: "duration", unit: "min", label: "Reserved finish time" },
            // holds the last cycle's total while idle, climbs during a cycle, resets at the next
            energy: #{ type: "number", class: "energy", series: "counter", unit: "Wh", label: "Cycle energy" },
            error: #{ type: "binary", class: "problem", category: "diagnostic" },
            error_message: #{ type: "text", category: "diagnostic" },
            night_dry: #{ type: "binary" },
            child_lock: #{ type: "binary", category: "diagnostic" },
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
    if chosen != () { ctx.publish_property("course_select", m::name_of(courses(), chosen)); }

    ctx.publish_property("power", if off { "false" } else { "true" });
    c::pub_name(ctx, "status", m::name_of(states(), state));
    c::pub_name(ctx, "course", m::name_of(courses(), b[p + 5]));
    c::pub_name(ctx, "smart_course", m::name_of(smart_courses(), b[p + 20]));
    // rec[23] counts the download-course slots in use, rec[24:28] are the slots; only the first
    // is published (this model has maxDownloadCourseNum 1, the rest are always empty)
    c::pub_name(ctx, "download_course", m::name_of(smart_courses(), b[p + 24]));

    ctx.publish_property("remaining_time", if off { "0" } else { (b[p + 1] * 60 + b[p + 2]).to_string() });
    ctx.publish_property("initial_time", if off { "0" } else { (b[p + 3] * 60 + b[p + 4]).to_string() });
    ctx.publish_property("reserve_time", if off { "0" } else { (b[p + 12] * 60 + b[p + 13]).to_string() });
    ctx.publish_property("energy", (b[p + 17] * 256 + b[p + 18]).to_string());

    let error = b[p + 6];
    ctx.publish_property("error", if error != 0 { "true" } else { "false" });
    ctx.publish_property("error_message", error_message(error));

    // rec[14]: bit 0 child lock, bit 1 night dry, bit 3 remote start. Bit 5 is set on every
    // record this unit has ever sent, and no cloud field follows it: left alone rather than
    // exposed as an invented toggle. rec[7] is the previous state (the cloud's preState).
    let flags = b[p + 14];
    ctx.publish_property("child_lock", c::bit(flags, 0x01));
    ctx.publish_property("night_dry", c::bit(flags, 0x02));
    ctx.publish_property("remote_start", c::bit(flags, 0x08));
}

fn on_set_property(ctx, prop, value) {
    if prop == "course_select" {
        let code = m::code_of(courses(), value);
        if code == () || !m::select(ctx, code, startable()) {
            c::reject(ctx, prop, "not a course this cabinet can start");
            return;
        }
        ctx.publish_property("course_select", value);
    } else if prop == "start" {
        let course = m::selected(ctx);
        let payload = if ctx.state_get("state") == 3 { resume_payload(course) } else { start_payload(course) };
        if payload == () || course == 0 { c::reject(ctx, prop, "no course selected"); return; }
        m::course_control(ctx, payload, prop);
    } else if prop == "pause" {
        m::short_control(ctx, m::control_pause(), prop);
    } else if prop == "power_off" {
        m::short_control(ctx, m::control_power_off(), prop);
    } else {
        c::reject(ctx, prop, "unknown or read-only property");
    }
}
