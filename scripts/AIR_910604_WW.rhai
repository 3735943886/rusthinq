// LG air purifier AIR_910604_WW: device -> IL driver. A TLV (DualCool family) appliance;
// the handshake, timers and write framing are shared in `tlv_common`.
//
// Tag names are the cloud's own (established live against the appliance): 0x1f7 power,
// 0x1f9 operating mode, 0x1fa wind strength, 0x24e indicator lamp, 0x360 sterilization,
// 0x21a sleep timer (minutes, counts down), 0x333/0x334/0x335 PM1/PM2.5/PM10, 0x240 air
// quality index, 0x241 odour index, 0x355/0x356 and 0x363/0x364 filter hours left / budget
// (bottom and top filter), 0x221 error.
//
// Not exposed: temperature (0x1fd) and humidity (0x336). Both are named and scaled but
// read like internal sensors of the unit, not the room (a steady 40 C), so publishing them
// would put wrong numbers on a dashboard. Tags this unit always reports as 0 are likewise
// left out until a capture shows the hardware has the feature.

import "tlv_common" as c;

fn descriptor(ctx) {
    json_stringify(#{
        il: 0,
        kind: "fan",
        class: "air_purifier",
        label: "Air purifier",
        vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            power: #{ type: "binary", rw: true, role: "on", src: #{ tlv_tag: "0x1f7" } },
            mode: #{ type: "select", rw: true, role: "mode", src: #{ tlv_tag: "0x1f9" },
                     options: ["circulator_clean", "baby_care", "dual_clean", "auto"] },
            fan: #{ type: "select", rw: true, role: "fan_speed", src: #{ tlv_tag: "0x1fa" },
                    options: ["low", "mid", "high", "power", "auto"] },
            pm1: #{ type: "number", src: #{ tlv_tag: "0x333" }, unit: "µg/m³" },
            pm25: #{ type: "number", src: #{ tlv_tag: "0x334" }, unit: "µg/m³" },
            pm10: #{ type: "number", src: #{ tlv_tag: "0x335" }, unit: "µg/m³" },
            air_quality: #{ type: "number", src: #{ tlv_tag: "0x240" }, min: 1, max: 4 },
            odor: #{ type: "number", src: #{ tlv_tag: "0x241" }, min: 1, max: 4 },
            filter_life: #{ type: "number", src: #{ tlv_tag: "0x355" }, unit: "%" },
            top_filter_life: #{ type: "number", src: #{ tlv_tag: "0x363" }, unit: "%" },
            light: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x24e" } },
            sterilize: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x360" } },
            sleep_timer: #{ type: "number", rw: true, src: #{ tlv_tag: "0x21a" },
                            min: 0, max: 720, step: 10, unit: "min" },
            error: #{ type: "number", src: #{ tlv_tag: "0x221" } }
        }
    })
}

// The operating modes the unit's capability reply (0x2c1) selects, and the wind strengths
// (0x2c2). Both are wire numbers.
fn mode_name(raw) {
    switch raw {
        13 => "circulator_clean",
        14 => "baby_care",
        15 => "dual_clean",
        16 => "auto",
        _ => ()
    }
}

fn mode_wire(name) {
    switch name {
        "circulator_clean" => 13,
        "baby_care" => 14,
        "dual_clean" => 15,
        "auto" => 16,
        _ => ()
    }
}

fn fan_name(raw) {
    switch raw {
        2 => "low",
        4 => "mid",
        6 => "high",
        7 => "power",
        8 => "auto",
        _ => ()
    }
}

fn fan_wire(name) {
    switch name {
        "low" => 2,
        "mid" => 4,
        "high" => 6,
        "power" => 7,
        "auto" => 8,
        _ => ()
    }
}

// Publish a filter's life as a percentage. Both tags feed it because either may arrive
// first and the budget is only resent with a full values response. Despite the cloud
// calling the first tag "useTime" it counts the hours LEFT (3716 of 4000 was 93 % in the
// app), so it is remaining / budget.
fn filter_life(ctx, prop, remain_tag, max_tag) {
    let remain = c::get(ctx, remain_tag);
    let budget = c::get(ctx, max_tag);
    if budget <= 0 { return; }
    let pct = (remain * 100.0 / budget).round().to_int();
    if pct < 0 { pct = 0; }
    if pct > 100 { pct = 100; }
    ctx.publish_property(prop, pct.to_string());
}

// ---- host hooks -------------------------------------------------------------------

fn start(ctx) { c::start(ctx); }

fn publish_config(ctx) { ctx.publish_il(descriptor(ctx)); }

fn on_drop(ctx) { ctx.publish_property("available", "false"); }

fn on_timer(ctx, name) { c::on_timer(ctx, name); }

fn on_data(ctx, data) {
    let frame = c::receive(ctx, data);
    if frame == () { return; }
    for e in frame.tlvs { apply(ctx, e.t, e.v); }
    if frame.first { ctx.publish_property("available", "true"); }
}

fn apply(ctx, tag, v) {
    switch tag {
        0x1f7 => { c::put(ctx, tag, v); ctx.publish_property("power", c::flag(v)); }
        0x1f9 => {
            c::put(ctx, tag, v);
            let name = mode_name(v);
            if name != () { ctx.publish_property("mode", name); }
        }
        0x1fa => {
            c::put(ctx, tag, v);
            let name = fan_name(v);
            if name != () { ctx.publish_property("fan", name); }
        }
        0x333 => { ctx.publish_property("pm1", v.to_string()); }
        0x334 => { ctx.publish_property("pm25", v.to_string()); }
        0x335 => { ctx.publish_property("pm10", v.to_string()); }
        0x240 => { ctx.publish_property("air_quality", v.to_string()); }
        0x241 => { ctx.publish_property("odor", v.to_string()); }
        0x355 | 0x356 => { c::put(ctx, tag, v); filter_life(ctx, "filter_life", 0x355, 0x356); }
        0x363 | 0x364 => { c::put(ctx, tag, v); filter_life(ctx, "top_filter_life", 0x363, 0x364); }
        0x24e => { c::put(ctx, tag, v); ctx.publish_property("light", c::flag(v)); }
        0x360 => { c::put(ctx, tag, v); ctx.publish_property("sterilize", c::flag(v)); }
        0x21a => { c::put(ctx, tag, v); ctx.publish_property("sleep_timer", v.to_string()); }
        0x221 => { ctx.publish_property("error", v.to_string()); }
        // the per-mode wind-strength table (0x2d7/8/9) and everything not exposed
        _ => {}
    }
}

// A write while the unit is off is acknowledged and then ignored. Sending the power tag on
// together with the mode / fan works and starts the unit in that setting, so those writes
// force power on and attach it. In auto mode the unit picks the fan itself and a bare fan
// write is ignored; nothing is done about that (silently changing the user's mode to make
// a fan request stick would be worse).
fn on_set_property(ctx, prop, value) {
    switch prop {
        "power" => {
            let on = c::to_bool(value);
            // turning on restores the mode and fan in the same frame
            c::write(ctx, 0x1f7, if on { 1 } else { 0 }, if on { [0x1f9, 0x1fa] } else { [] });
        }
        "fan" => {
            let wire = fan_wire(value);
            if wire == () { c::reject(ctx, prop, "unknown fan speed"); return; }
            c::put(ctx, 0x1f7, 1);
            c::write(ctx, 0x1fa, wire, [0x1f7, 0x1f9]);
        }
        "mode" => {
            let wire = mode_wire(value);
            if wire == () { c::reject(ctx, prop, "unknown mode"); return; }
            c::put(ctx, 0x1f7, 1);
            c::write(ctx, 0x1f9, wire, [0x1f7, 0x1fa]);
        }
        "light" => { c::write(ctx, 0x24e, if c::to_bool(value) { 1 } else { 0 }, []); }
        "sterilize" => { c::write(ctx, 0x360, if c::to_bool(value) { 1 } else { 0 }, []); }
        "sleep_timer" => {
            let n = c::to_number(value);
            if n == () || n < 0 || n > 720 { c::reject(ctx, prop, "out of range 0..720"); return; }
            c::write(ctx, 0x21a, n.round().to_int(), []);
        }
        _ => { c::reject(ctx, prop, "unknown or read-only property"); }
    }
}
