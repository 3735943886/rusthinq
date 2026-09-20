// LG dehumidifier DHUM_056905_WW: device -> IL driver.
//
// Speaks the TLV protocol to the appliance and describes/reports it in the neutral IL
// (see the IL specification, a separate repository). It knows nothing about any consumer:
// values go out through `publish_property`, the descriptor through `publish_il`, and
// writes arrive at `on_set_property`. Timers are requests (`set_timer`) answered by
// `on_timer`; the host owns the clock.
//
// Wire tags: 0x1f7 power, 0x1f9 mode, 0x1fa fan (2 low, 6 high), 0x253 target humidity,
// 0x336 humidity, 0x1fd temperature (half degrees), 0x186 tank, 0x360 sterilization,
// 0x2a2 UVnano, 0x21e tank light, 0x21b off timer (minutes), 0x221 error.

import "tlv_common" as c;

// ---- model ------------------------------------------------------------------------

fn descriptor(ctx) {
    json_stringify(#{
        il: 0,
        kind: "humidifier",
        class: "dehumidifier",
        label: "Dehumidifier",
        vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            power: #{ type: "binary", rw: true, role: "on", src: #{ tlv_tag: "0x1f7" } },
            mode: #{ type: "select", rw: true, role: "mode", src: #{ tlv_tag: "0x1f9" },
                     options: ["smart", "jet", "silent", "spot", "laundry"] },
            target: #{ type: "number", rw: true, role: "target_humidity", src: #{ tlv_tag: "0x253" },
                       min: 30, max: 70, step: 1, unit: "%" },
            humidity: #{ type: "number", class: "humidity", series: "gauge", role: "current_humidity", src: #{ tlv_tag: "0x336" }, unit: "%" },
            temperature: #{ type: "number", class: "temperature", series: "gauge", role: "current_temperature", src: #{ tlv_tag: "0x1fd" },
                            unit: "°C" },
            fan: #{ type: "select", rw: true, role: "fan_speed", src: #{ tlv_tag: "0x1fa" },
                    options: ["low", "high"] },
            tank_full: #{ type: "binary", class: "problem", src: #{ tlv_tag: "0x186" } },
            tank_state: #{ type: "select", category: "diagnostic", src: #{ tlv_tag: "0x186" },
                           options: ["not_full", "full_stopped", "full_fan_mode"] },
            sterilize: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x360" } },
            uvnano: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x2a2" } },
            bucket_light: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x21e" } },
            off_timer: #{ type: "number", class: "duration", rw: true, src: #{ tlv_tag: "0x21b" },
                          min: 0, max: 540, step: 60, unit: "min" },
            error: #{ type: "number", category: "diagnostic", src: #{ tlv_tag: "0x221" } }
        }
    })
}

// wire mode -> IL mode. Two numbering schemes are seen on the wire (0..5 and 17..21).
fn mode_name(raw) {
    switch raw {
        0 | 17 => "smart",
        1 | 18 => "jet",
        2 | 19 => "silent",
        4 | 20 => "spot",
        5 | 21 => "laundry",
        _ => ()
    }
}

fn mode_wire(name) {
    switch name {
        "smart" => 17,
        "jet" => 18,
        "silent" => 19,
        "spot" => 20,
        "laundry" => 21,
        _ => ()
    }
}

fn is_silent(raw) { raw == 2 || raw == 19 }

fn tank_state_name(raw) {
    switch raw {
        0 => "not_full",
        1 => "full_stopped",
        2 => "full_fan_mode",
        _ => ()
    }
}

// ---- helpers ----------------------------------------------------------------------

// The fan write carries a per-mode capability table: mode id, 0, the speed that mode runs at.
// Laundry is fixed at high.
fn write_fan(ctx, speed) {
    c::put(ctx, 0x1fa, speed);
    let tlvs = [#{ t: 0x1fa, v: speed }];
    for row in [[17, speed], [18, speed], [20, speed], [21, 6], [22, speed]] {
        tlvs.push(#{ t: 0x2d7, v: row[0] });
        tlvs.push(#{ t: 0x2d8, v: 0 });
        tlvs.push(#{ t: 0x2d9, v: row[1] });
    }
    ctx.send_raw(tlv_frame_build(c::write_header(), tlvs));
}

// ---- host hooks -------------------------------------------------------------------

fn start(ctx) { c::start(ctx); }

fn publish_config(ctx) {
    ctx.publish_il(descriptor(ctx));
}

fn on_drop(ctx) {
    ctx.publish_property("available", "false");
}

fn on_timer(ctx, name) { c::on_timer(ctx, name); }

fn on_data(ctx, data) {
    let frame = c::receive(ctx, data);
    if frame == () { return; }

    for e in frame.tlvs { apply(ctx, e.t, e.v); }

    if frame.first {
        ctx.publish_property("available", "true");
        // Ask the cloud not to relay tag 1200 (the appliance's noisy periodic report).
        ctx.send_clip("setMaskingInfo", 0, "{\"blacklist_tlv\":\"1200\"}");
    }
}

// Apply one wire tag. Returns true if it was one of the tags that count as "values".
fn apply(ctx, tag, v) {
    switch tag {
        // per-mode fan capability rows repeat once per mode: not device state
        0x2d7 | 0x2d8 | 0x2d9 => { return false; }
        0x1f7 => {
            c::put(ctx, tag, v);
            ctx.publish_property("power", c::flag(v));
        }
        0x1f9 => {
            let prev = ctx.state_get("t505");
            c::put(ctx, tag, v);
            let name = mode_name(v);
            if name != () { ctx.publish_property("mode", name); }
            // entering silent from another mode drops the fan to low
            if is_silent(v) && (prev == () || !is_silent(prev)) {
                ctx.publish_property("fan", "low");
            }
        }
        0x1fa => {
            c::put(ctx, tag, v);
            if v == 2 { ctx.publish_property("fan", "low"); }
            else if v == 6 { ctx.publish_property("fan", "high"); }
        }
        0x253 => {
            c::put(ctx, tag, v);
            if v >= 30 && v <= 70 { ctx.publish_property("target", v.to_string()); }
        }
        0x336 => { ctx.publish_property("humidity", v.to_string()); }
        0x1fd => {
            let t = if v % 2 == 0 { (v / 2).to_string() } else { (v / 2.0).to_string() };
            ctx.publish_property("temperature", t);
        }
        0x186 => {
            let name = tank_state_name(v);
            if name != () {
                ctx.publish_property("tank_full", c::flag(v));
                ctx.publish_property("tank_state", name);
            }
        }
        // secondary tank signals seen when the tank is reinstalled / filled
        0x2b1 => { if v == 256 { ctx.publish_property("tank_full", "false"); } }
        0x2b2 => { ctx.publish_property("tank_full", c::flag(v)); }
        0x360 => { c::put(ctx, tag, v); ctx.publish_property("sterilize", c::flag(v)); }
        0x2a2 => { c::put(ctx, tag, v); ctx.publish_property("uvnano", c::flag(v)); }
        0x21e => { c::put(ctx, tag, v); ctx.publish_property("bucket_light", c::flag(v)); }
        0x21b => { c::put(ctx, tag, v); ctx.publish_property("off_timer", v.to_string()); }
        0x221 => { ctx.publish_property("error", v.to_string()); }
        _ => { return false; }
    }
    true
}

fn on_set_property(ctx, prop, value) {
    switch prop {
        "power" => {
            let on = c::to_bool(value);
            c::write(ctx, 0x1f7, if on { 1 } else { 0 }, if on { [0x1f9] } else { [] });
        }
        "mode" => {
            let wire = mode_wire(value);
            if wire == () { c::reject(ctx, prop, "unknown mode"); return; }
            let prev = c::get(ctx, 0x1f9);
            c::put(ctx, 0x1f7, 1); // selecting a mode powers on
            if wire == 19 && !is_silent(prev) {
                c::put(ctx, 0x1fa, 2); // silent runs at low fan
                ctx.publish_property("fan", "low");
            }
            c::write(ctx, 0x1f9, wire, [0x1f7]);
        }
        "target" => {
            let n = c::to_number(value);
            if n == () { c::reject(ctx, prop, "not a number"); return; }
            let t = n.round().to_int();
            if t < 30 { t = 30; }
            if t > 70 { t = 70; }
            c::put(ctx, 0x1f7, 1);
            c::write(ctx, 0x253, t, [0x1f7, 0x1f9]);
        }
        "fan" => {
            if value == "low" { write_fan(ctx, 2); }
            else if value == "high" { write_fan(ctx, 6); }
            else { c::reject(ctx, prop, "unknown fan speed"); }
        }
        "sterilize" => { c::write(ctx, 0x360, if c::to_bool(value) { 1 } else { 0 }, [0x1f7, 0x1f9]); }
        "uvnano" => { c::write(ctx, 0x2a2, if c::to_bool(value) { 1 } else { 0 }, [0x1f7, 0x1f9]); }
        "bucket_light" => { c::write(ctx, 0x21e, if c::to_bool(value) { 1 } else { 0 }, []); }
        "off_timer" => {
            let n = c::to_number(value);
            if n == () || n < 0 { c::reject(ctx, prop, "not a non-negative number"); return; }
            c::write(ctx, 0x21b, n.round().to_int(), []);
        }
        _ => { c::reject(ctx, prop, "unknown or read-only property"); }
    }
}
