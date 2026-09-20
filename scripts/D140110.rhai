// LG dishwasher D140110: device -> IL driver. A read-only AABB appliance whose body starts
// with 0x32. Frame 0xEC carries two stacked 26-byte records (the previous state, then the
// current one; only the current counts). A record is `<flags> 0x18 <24 bytes>`, where 0x18 is
// the payload length; the payload is one byte per entry of the model's protocol list, except
// that the run of booleans in the middle is packed into bits (byte 11).
//
// Every field below was pinned by two independent readings (the appliance during a full 1:33
// Auto cycle, and the real cloud's decoded snapshot at the same moment). Not published,
// because nothing confirmed them: byte 12 (the option flags, 0 on a cycle that used none),
// byte 15, bytes 16-22, byte 23. The other frames (0xB2, 0x31, 0xD8, 0x72, 0xBF, 0xCF) carry
// nothing 0xEC does not, or nothing that was confirmed.
//
// Read-only: the model describes a control API but no command was ever captured, so the
// encoding is unknown.

import "aabb_common" as c;

fn record_len() { 26 }

fn descriptor(ctx) {
    json_stringify(#{
        il: 0,
        kind: "washer",
        class: "dishwasher",
        label: "Dishwasher",
        vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            status: #{ type: "select",
                       options: ["off", "ready", "running", "paused", "standby", "finished", "power_failed"] },
            process: #{ type: "select", label: "Cycle phase",
                        options: ["none", "reserved", "washing", "rinsing", "drying", "finished", "night_dry", "cancelled"] },
            course: #{ type: "select", label: "Cycle",
                       options: ["none", "auto", "intensive", "delicate", "turbo", "eco", "rinse",
                                 "refresh", "express", "machine_clean", "download_cycle"] },
            error: #{ type: "select", options: ["none", "HE", "IE", "OE", "FE", "TE", "AE", "EE", "LE", "NE", "BE", "F3"] },
            running: #{ type: "binary" },
            door: #{ type: "binary", label: "Door open" },
            child_lock: #{ type: "binary" },
            chime: #{ type: "binary" },
            rinse_refill: #{ type: "binary", label: "Rinse aid low" },
            salt_refill: #{ type: "binary", label: "Salt low" },
            total_time: #{ type: "number", unit: "min" },
            remaining_time: #{ type: "number", unit: "min" },
            reserve_time: #{ type: "number", unit: "min", label: "Delayed start" },
            rinse_aid_level: #{ type: "number" },
            softening_level: #{ type: "number", label: "Water softening level" }
        }
    })
}

// The model gives POWEROFF and STANDBY the same label, so the app cannot tell them apart.
// They are distinct states (a finished cycle passes through standby on its way to off).
fn state_name(v) {
    switch v {
        0 => "off", 1 => "ready", 2 => "running", 3 => "paused",
        4 => "standby", 5 => "finished", 6 => "power_failed",
        _ => ()
    }
}

fn process_name(v) {
    switch v {
        0 => "none", 1 => "reserved", 2 => "washing", 3 => "rinsing", 4 => "drying",
        5 => "finished", 6 => "night_dry", 7 => "cancelled",
        _ => ()
    }
}

// The model's Course table, keyed by the id it carries on the wire.
fn course_name(v) {
    switch v {
        0 => "none", 1 => "auto", 2 => "intensive", 3 => "delicate", 4 => "turbo", 5 => "eco",
        6 => "rinse", 7 => "refresh", 8 => "express", 9 => "machine_clean", 11 => "download_cycle",
        _ => ()
    }
}

// The model's error table in its own order. Only "no error" has ever been seen; the rest
// keep LG's two-letter codes, which are what the panel shows.
fn error_name(v) {
    switch v {
        0 => "none", 1 => "HE", 2 => "IE", 3 => "OE", 4 => "FE", 5 => "TE",
        6 => "AE", 7 => "EE", 8 => "LE", 9 => "NE", 10 => "BE", 11 => "F3",
        _ => ()
    }
}

// ---- host hooks -------------------------------------------------------------------

fn publish_config(ctx) { ctx.publish_il(descriptor(ctx)); }

fn on_drop(ctx) { ctx.publish_property("available", "false"); }

fn on_data(ctx, data) {
    let body = c::inner(data, 0x32);
    if body == () || body[1] != 0xec || body.len() != 2 + record_len() * 2 { return; }

    // The record states its own payload length; anything else is a frame we do not understand.
    let rec = 2 + record_len();
    if body[rec + 1] != 0x18 { return; }

    let p = rec + 2; // offset of the payload
    ctx.publish_property("available", "true");

    let state = body[p];
    c::pub_name(ctx, "status", state_name(state));
    c::pub_name(ctx, "process", process_name(body[p + 1]));
    c::pub_name(ctx, "course", course_name(body[p + 5]));
    c::pub_name(ctx, "error", error_name(body[p + 2]));
    ctx.publish_property("running", if state == 2 { "true" } else { "false" });

    // bit 0 child lock, bit 1 door, bit 2 rinse aid refill, bit 3 salt refill, bit 4 chime
    let flags = body[p + 11];
    ctx.publish_property("child_lock", c::bit(flags, 0x01));
    ctx.publish_property("door", c::bit(flags, 0x02));
    ctx.publish_property("rinse_refill", c::bit(flags, 0x04));
    ctx.publish_property("salt_refill", c::bit(flags, 0x08));
    ctx.publish_property("chime", c::bit(flags, 0x10));

    ctx.publish_property("total_time", (body[p + 3] * 60 + body[p + 4]).to_string());
    ctx.publish_property("remaining_time", (body[p + 7] * 60 + body[p + 8]).to_string());
    ctx.publish_property("reserve_time", (body[p + 9] * 60 + body[p + 10]).to_string());
    ctx.publish_property("rinse_aid_level", body[p + 13].to_string());
    ctx.publish_property("softening_level", body[p + 14].to_string());
}

fn on_set_property(ctx, prop, value) {
    c::reject(ctx, prop, "read-only appliance");
}
