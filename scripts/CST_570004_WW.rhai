// LG ceiling-cassette indoor unit CST_570004_WW (multi-split, cooling only): device -> IL driver.
//
// A TLV (DualCool family) appliance; the handshake, timers and write framing are shared in
// `tlv_common`. This script is for this model only, so it carries no capability probing: the
// tag numbers, wire scales and feature set below are what this unit reports and accepts.
//
// Wire tags: 0x1f7 power, 0x1f9 mode (cool 0, dry 1, fan 2, auto 3), 0x1fa fan (auto 8,
// very low 1, low 2, medium 4, high 6, power 7), 0x1fd room temperature and 0x1fe setpoint
// (half degrees), 0x205 / 0x206 vertical / horizontal swing (on/off), 0x20d energy saving
// (only meaningful in cool), 0x20e auto dry (0 off, 1 10 min, 2 30 min, 3 60 min, 255 smart),
// 0x225 auto dry minutes left, 0x21a sleep timer (minutes), 0x21f display brightness
// (100 off, 150 half, 200 full), 0x221 error, 0x23f comfort energy saving, 0x2b3 power draw
// (W), 0x336 humidity (tenths of a percent), 0x355 / 0x356 filter hours left / rated life,
// 0x189 or 0x6c whether the unit is actually cooling right now (when it reports either),
// 0x290 / 0x291 / 0x3d5 / 0x3d6 / 0x3d7 the wind-mode flags (exactly one set, or none).
//
// The unit ignores a mode write while it is off, so a mode write carries power on; the
// app does the same. Frames from this model have byte 6 = 0xa7, which `tlv_frame_parse`
// accepts.

import "tlv_common" as c;

// ---- model ------------------------------------------------------------------------

fn descriptor(ctx) {
    json_stringify(#{
        il: 0,
        kind: "climate",
        class: "air_conditioner",
        label: "Air conditioner",
        vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            power: #{ type: "binary", rw: true, role: "on", src: #{ tlv_tag: "0x1f7" } },
            mode: #{ type: "select", rw: true, role: "mode", src: #{ tlv_tag: "0x1f9" },
                     options: ["cool", "dry", "fan_only", "auto"] },
            fan: #{ type: "select", rw: true, role: "fan_speed", src: #{ tlv_tag: "0x1fa" },
                    options: ["auto", "very_low", "low", "medium", "high", "power"] },
            target: #{ type: "number", rw: true, role: "target_temperature", src: #{ tlv_tag: "0x1fe" },
                       min: 16, max: 30, step: 0.5, unit: "°C" },
            temperature: #{ type: "number", role: "current_temperature", src: #{ tlv_tag: "0x1fd" },
                            unit: "°C" },
            humidity: #{ type: "number", role: "current_humidity", src: #{ tlv_tag: "0x336" }, unit: "%" },
            action: #{ type: "select", options: ["off", "idle", "cooling", "drying", "fan", "active"] },
            swing_vertical: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x205" } },
            swing_horizontal: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x206" } },
            energy_save: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x20d" } },
            comfort_saving: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x23f" } },
            wind_mode: #{ type: "select", rw: true,
                          options: ["off", "manner", "long_power", "study", "auto_temp"] },
            auto_dry: #{ type: "select", rw: true, src: #{ tlv_tag: "0x20e" },
                         options: ["off", "10min", "30min", "60min", "smart"] },
            auto_dry_remaining: #{ type: "number", src: #{ tlv_tag: "0x225" }, unit: "min" },
            sleep_timer: #{ type: "number", rw: true, src: #{ tlv_tag: "0x21a" },
                            min: 0, max: 900, step: 1, unit: "min" },
            display: #{ type: "select", rw: true, src: #{ tlv_tag: "0x21f" },
                        options: ["off", "50%", "100%"] },
            power_draw: #{ type: "number", src: #{ tlv_tag: "0x2b3" }, unit: "W" },
            filter_remaining: #{ type: "number", src: #{ tlv_tag: "0x355" }, unit: "%" },
            filter_used: #{ type: "number", src: #{ tlv_tag: "0x355" }, unit: "h" },
            filter_life: #{ type: "number", src: #{ tlv_tag: "0x356" }, unit: "h" },
            error: #{ type: "number", src: #{ tlv_tag: "0x221" } }
        }
    })
}

fn mode_name(raw) {
    switch raw {
        0 => "cool",
        1 => "dry",
        2 => "fan_only",
        3 => "auto",
        _ => ()
    }
}

fn mode_wire(name) {
    switch name {
        "cool" => 0,
        "dry" => 1,
        "fan_only" => 2,
        "auto" => 3,
        _ => ()
    }
}

fn fan_name(raw) {
    switch raw {
        8 => "auto",
        1 => "very_low",
        2 => "low",
        4 => "medium",
        6 => "high",
        7 => "power",
        _ => ()
    }
}

fn fan_wire(name) {
    switch name {
        "auto" => 8,
        "very_low" => 1,
        "low" => 2,
        "medium" => 4,
        "high" => 6,
        "power" => 7,
        _ => ()
    }
}

fn auto_dry_name(raw) {
    switch raw {
        0 => "off",
        1 => "10min",
        2 => "30min",
        3 => "60min",
        255 => "smart",
        _ => ()
    }
}

fn auto_dry_wire(name) {
    switch name {
        "off" => 0,
        "10min" => 1,
        "30min" => 2,
        "60min" => 3,
        "smart" => 255,
        _ => ()
    }
}

fn display_name(raw) {
    switch raw {
        100 => "off",
        150 => "50%",
        200 => "100%",
        _ => ()
    }
}

fn display_wire(name) {
    switch name {
        "off" => 100,
        "50%" => 150,
        "100%" => 200,
        _ => ()
    }
}

// The five wind-mode flags, in the order the app writes them.
fn wind_flags() { [0x290, 0x291, 0x3d5, 0x3d6, 0x3d7] }

fn wind_flag(name) {
    switch name {
        "manner" => 0x3d6,
        "long_power" => 0x3d7,
        "study" => 0x291,
        "auto_temp" => 0x290,
        _ => ()
    }
}

fn wind_name(ctx) {
    if c::get(ctx, 0x3d6) != 0 { "manner" }
    else if c::get(ctx, 0x3d7) != 0 { "long_power" }
    else if c::get(ctx, 0x291) != 0 { "study" }
    else if c::get(ctx, 0x290) != 0 { "auto_temp" }
    else { "off" }
}

// ---- derived state ----------------------------------------------------------------

// "off" while the unit is powered off, else the mode name.
fn effective_mode(ctx) {
    if c::get(ctx, 0x1f7) == 0 { "off" } else { mode_name(c::get(ctx, 0x1f9)) }
}

// Whether the indoor unit is really cooling, when the unit says (true if it never does).
fn running(ctx) {
    for tag in [0x189, 0x6c] {
        let v = ctx.state_get("t" + tag);
        if v != () { return v != 0; }
    }
    true
}

fn action_name(ctx) {
    let mode = effective_mode(ctx);
    if mode == "off" { "off" }
    else if mode == () { () }
    else if mode == "fan_only" { "fan" }
    else if !running(ctx) { "idle" }
    else if mode == "cool" { "cooling" }
    else if mode == "dry" { "drying" }
    else { "active" }
}

// The compressor side changes fast while running, so poll more often then.
fn poll_ms() { 28000 }

fn half(v) {
    if v % 2 == 0 { (v / 2).to_string() } else { (v / 2.0).to_string() }
}

// ---- host hooks -------------------------------------------------------------------

fn start(ctx) {
    ctx.state_set("active", false);
    c::start(ctx);
}

fn publish_config(ctx) {
    ctx.publish_il(descriptor(ctx));
}

fn on_drop(ctx) {
    ctx.publish_property("available", "false");
}

fn on_timer(ctx, name) {
    if name == "poll" {
        if ctx.state_get("active") == true {
            c::query(ctx, 2);
            ctx.set_timer("poll", poll_ms());
        }
    } else {
        c::on_timer(ctx, name);
    }
}

fn on_data(ctx, data) {
    let frame = c::receive(ctx, data);
    if frame == () { return; }

    let before = ctx.state_get("emode");
    let energy = ();
    for e in frame.tlvs {
        if e.t == 0x20d { energy = e.v; }
        else { apply(ctx, e.t, e.v); }
    }

    let mode = effective_mode(ctx);
    ctx.state_set("emode", mode);
    settle_energy(ctx, energy, before != mode && mode == "cool");
    if frame.tlvs.some(|e| e.t == 0x290 || e.t == 0x291 || e.t == 0x3d5 || e.t == 0x3d6 || e.t == 0x3d7) {
        ctx.publish_property("wind_mode", wind_name(ctx));
    }
    if frame.tlvs.some(|e| e.t == 0x355 || e.t == 0x356) { publish_filter(ctx); }
    publish_action(ctx);

    if frame.first {
        ctx.publish_property("available", "true");
        // Ask the cloud not to relay tag 1200 (the appliance's noisy periodic report).
        ctx.send_clip("setMaskingInfo", 0, "{\"blacklist_tlv\":\"1200\"}");
    }
}

// Apply one wire tag.
fn apply(ctx, tag, v) {
    c::put(ctx, tag, v);
    switch tag {
        0x1f7 => { ctx.publish_property("power", c::flag(v)); }
        0x1f9 => {
            let name = mode_name(v);
            if name != () { ctx.publish_property("mode", name); }
        }
        0x1fa => {
            let name = fan_name(v);
            if name != () { ctx.publish_property("fan", name); }
        }
        0x1fd => { ctx.publish_property("temperature", half(v)); }
        0x1fe => { ctx.publish_property("target", half(v)); }
        0x336 => { ctx.publish_property("humidity", (v / 10.0).round().to_int().to_string()); }
        0x205 => { ctx.publish_property("swing_vertical", c::flag(v)); }
        0x206 => { ctx.publish_property("swing_horizontal", c::flag(v)); }
        0x23f => { ctx.publish_property("comfort_saving", c::flag(v)); }
        0x20e => {
            let name = auto_dry_name(v);
            if name != () { ctx.publish_property("auto_dry", name); }
        }
        0x225 => { ctx.publish_property("auto_dry_remaining", v.to_string()); }
        0x21a => { ctx.publish_property("sleep_timer", v.to_string()); }
        0x21f => {
            let name = display_name(v);
            if name != () { ctx.publish_property("display", name); }
        }
        0x221 => { ctx.publish_property("error", v.to_string()); }
        0x2b3 => { ctx.publish_property("power_draw", v.to_string()); }
        _ => {}
    }
}

// Energy saving only takes effect in cool, and the unit forgets it over a power cycle. So a
// report is trusted only while running in cool; on entering cool the wanted value is written
// again (in its own frame) rather than adopting whatever the unit reports right after power up.
fn settle_energy(ctx, reported, entered_cool) {
    let want = ctx.state_get("energy_want");
    if entered_cool && want != () {
        if (reported != 0) != want { c::write(ctx, 0x20d, if want { 1 } else { 0 }, []); }
        ctx.publish_property("energy_save", c::flag(if want { 1 } else { 0 }));
    } else if reported != () && effective_mode(ctx) == "cool" {
        c::put(ctx, 0x20d, reported);
        ctx.state_set("energy_want", reported != 0);
        ctx.publish_property("energy_save", c::flag(reported));
    }
}

fn publish_filter(ctx) {
    let life = c::get(ctx, 0x356);
    let left = c::get(ctx, 0x355);
    if life > 0 {
        ctx.publish_property("filter_remaining", (left * 100.0 / life).round().to_int().to_string());
        ctx.publish_property("filter_used", (life - left).to_string());
        ctx.publish_property("filter_life", life.to_string());
    }
}

fn publish_action(ctx) {
    let action = action_name(ctx);
    if action != () { ctx.publish_property("action", action); }

    let active = action == "cooling" || action == "drying" || action == "active";
    let was = ctx.state_get("active") == true;
    ctx.state_set("active", active);
    if active && !was { ctx.set_timer("poll", poll_ms()); }
    if !active && was { ctx.cancel_timer("poll"); }
}

// ---- writes -----------------------------------------------------------------------

fn on_set_property(ctx, prop, value) {
    switch prop {
        "power" => {
            let on = c::to_bool(value);
            c::write(ctx, 0x1f7, if on { 1 } else { 0 }, if on { [0x1f9, 0x1fa, 0x1fe] } else { [] });
        }
        "mode" => {
            let wire = mode_wire(value);
            if wire == () { c::reject(ctx, prop, "unknown mode"); return; }
            c::put(ctx, 0x1f7, 1); // the mode write powers the unit on
            c::write(ctx, 0x1f9, wire, [0x1f7, 0x1fa, 0x1fe]);
        }
        "fan" => {
            let wire = fan_wire(value);
            if wire == () { c::reject(ctx, prop, "unknown fan speed"); return; }
            c::write(ctx, 0x1fa, wire, [0x1f9, 0x1fe]);
        }
        "target" => {
            let n = c::to_number(value);
            if n == () { c::reject(ctx, prop, "not a number"); return; }
            c::write(ctx, 0x1fe, (n * 2).round().to_int(), [0x1f9, 0x1fa]);
        }
        "swing_vertical" => { c::write(ctx, 0x205, if c::to_bool(value) { 1 } else { 0 }, []); }
        "swing_horizontal" => { c::write(ctx, 0x206, if c::to_bool(value) { 1 } else { 0 }, []); }
        "comfort_saving" => { c::write(ctx, 0x23f, if c::to_bool(value) { 1 } else { 0 }, []); }
        "energy_save" => {
            let on = c::to_bool(value);
            ctx.state_set("energy_want", on);
            ctx.publish_property("energy_save", c::flag(if on { 1 } else { 0 }));
            // Only takes effect while running in cool; otherwise it is written on entering cool.
            if effective_mode(ctx) == "cool" { c::write(ctx, 0x20d, if on { 1 } else { 0 }, []); }
        }
        "auto_dry" => {
            let wire = auto_dry_wire(value);
            if wire == () { c::reject(ctx, prop, "unknown auto dry setting"); return; }
            c::write(ctx, 0x20e, wire, []);
        }
        "display" => {
            let wire = display_wire(value);
            if wire == () { c::reject(ctx, prop, "unknown brightness"); return; }
            c::write(ctx, 0x21f, wire, []);
        }
        "sleep_timer" => {
            let n = c::to_number(value);
            if n == () { c::reject(ctx, prop, "not a number"); return; }
            c::write(ctx, 0x21a, n.round().to_int(), []);
        }
        "wind_mode" => {
            let on = wind_flag(value);
            if on == () && value != "off" { c::reject(ctx, prop, "unknown wind mode"); return; }
            // exactly one flag set, or none
            let tlvs = [];
            for f in wind_flags() {
                let v = if f == on { 1 } else { 0 };
                c::put(ctx, f, v);
                tlvs.push(#{ t: f, v: v });
            }
            ctx.send_raw(tlv_frame_build(c::write_header(), tlvs));
            ctx.publish_property("wind_mode", value);
        }
        _ => { c::reject(ctx, prop, "unknown or read-only property"); }
    }
}
