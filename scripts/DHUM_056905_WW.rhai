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

// (Rhai functions cannot see top-level constants, so these are functions.)
fn query_header() { [1, 1, 2, 2, 1] }
fn write_header() { [1, 1, 2, 1, 1] }
fn retry_ms() { 15000 }
fn refresh_ms() { 900000 }

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
            humidity: #{ type: "number", role: "current_humidity", src: #{ tlv_tag: "0x336" }, unit: "%" },
            temperature: #{ type: "number", src: #{ tlv_tag: "0x1fd" }, unit: "°C" },
            fan: #{ type: "select", rw: true, src: #{ tlv_tag: "0x1fa" }, options: ["low", "high"] },
            tank_full: #{ type: "binary", src: #{ tlv_tag: "0x186" } },
            tank_state: #{ type: "select", src: #{ tlv_tag: "0x186" },
                           options: ["not_full", "full_stopped", "full_fan_mode"] },
            sterilize: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x360" } },
            uvnano: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x2a2" } },
            bucket_light: #{ type: "binary", rw: true, src: #{ tlv_tag: "0x21e" } },
            off_timer: #{ type: "number", rw: true, src: #{ tlv_tag: "0x21b" },
                          min: 0, max: 540, step: 60, unit: "min" },
            error: #{ type: "number", src: #{ tlv_tag: "0x221" } }
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

fn sget(ctx, tag) {
    let v = ctx.state_get("t" + tag);
    if v == () { 0 } else { v }
}

fn sset(ctx, tag, v) { ctx.state_set("t" + tag, v); }

fn is_set(ctx, key) { ctx.state_get(key) == true }

fn flag(v) { if v != 0 { "true" } else { "false" } }

fn to_bool(text) {
    let t = text.to_lower();
    t == "true" || t == "on" || t == "1"
}

fn to_number(text) {
    try { text.parse_float() } catch { () }
}

fn reject(ctx, prop, reason) {
    ctx.publish_event("reject", json_stringify(#{ prop: prop, reason: reason }));
}

fn query(ctx, kind) {
    ctx.send_raw(tlv_frame_build(query_header(), [#{ t: 0x1f5, v: kind }]));
}

// Send a write: `tag=value`, plus the attached tags at their last known values.
fn write(ctx, tag, value, attach) {
    sset(ctx, tag, value);
    let tlvs = [#{ t: tag, v: value }];
    for a in attach {
        tlvs.push(#{ t: a, v: sget(ctx, a) });
    }
    ctx.send_raw(tlv_frame_build(write_header(), tlvs));
}

// The fan write carries a per-mode capability table: mode id, 0, the speed that mode runs at.
// Laundry is fixed at high.
fn write_fan(ctx, speed) {
    sset(ctx, 0x1fa, speed);
    let tlvs = [#{ t: 0x1fa, v: speed }];
    for row in [[17, speed], [18, speed], [20, speed], [21, 6], [22, speed]] {
        tlvs.push(#{ t: 0x2d7, v: row[0] });
        tlvs.push(#{ t: 0x2d8, v: 0 });
        tlvs.push(#{ t: 0x2d9, v: row[1] });
    }
    ctx.send_raw(tlv_frame_build(write_header(), tlvs));
}

// ---- host hooks -------------------------------------------------------------------

fn start(ctx) {
    ctx.state_set("caps", false);
    ctx.state_set("values", false);
    query(ctx, 1);
    ctx.set_timer("caps_retry", retry_ms());
}

fn publish_config(ctx) {
    ctx.publish_il(descriptor(ctx));
}

fn on_drop(ctx) {
    ctx.publish_property("available", "false");
}

fn on_timer(ctx, name) {
    switch name {
        "caps_retry" => {
            if !is_set(ctx, "caps") {
                query(ctx, 1);
                ctx.set_timer("caps_retry", retry_ms());
            }
        }
        "values_retry" => {
            if !is_set(ctx, "values") {
                query(ctx, 2);
                ctx.set_timer("values_retry", retry_ms());
            }
        }
        "refresh" => {
            // Not every change produces a notify, so ask again on a slow cadence.
            query(ctx, 2);
            ctx.set_timer("refresh", refresh_ms());
        }
    }
}

fn on_data(ctx, data) {
    let tlvs = tlv_frame_parse(data);
    if tlvs == () { return; }

    // The capabilities reply is recognised by tag 0x2da. It only starts the next step.
    if tlvs.some(|t| t.t == 0x2da) {
        if !is_set(ctx, "caps") {
            ctx.state_set("caps", true);
            ctx.cancel_timer("caps_retry");
            query(ctx, 2);
            ctx.set_timer("values_retry", retry_ms());
        }
        return;
    }

    let saw_values = false;
    for e in tlvs {
        if apply(ctx, e.t, e.v) { saw_values = true; }
    }

    if saw_values && !is_set(ctx, "values") {
        ctx.state_set("values", true);
        ctx.cancel_timer("values_retry");
        ctx.publish_property("available", "true");
        // Ask the cloud not to relay tag 1200 (the appliance's noisy periodic report).
        ctx.send_clip("setMaskingInfo", 0, "{\"blacklist_tlv\":\"1200\"}");
        ctx.set_timer("refresh", refresh_ms());
    }
}

// Apply one wire tag. Returns true if it was one of the tags that count as "values".
fn apply(ctx, tag, v) {
    switch tag {
        // per-mode fan capability rows repeat once per mode: not device state
        0x2d7 | 0x2d8 | 0x2d9 => { return false; }
        0x1f7 => {
            sset(ctx, tag, v);
            ctx.publish_property("power", flag(v));
        }
        0x1f9 => {
            let prev = ctx.state_get("t505");
            sset(ctx, tag, v);
            let name = mode_name(v);
            if name != () { ctx.publish_property("mode", name); }
            // entering silent from another mode drops the fan to low
            if is_silent(v) && (prev == () || !is_silent(prev)) {
                ctx.publish_property("fan", "low");
            }
        }
        0x1fa => {
            sset(ctx, tag, v);
            if v == 2 { ctx.publish_property("fan", "low"); }
            else if v == 6 { ctx.publish_property("fan", "high"); }
        }
        0x253 => {
            sset(ctx, tag, v);
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
                ctx.publish_property("tank_full", flag(v));
                ctx.publish_property("tank_state", name);
            }
        }
        // secondary tank signals seen when the tank is reinstalled / filled
        0x2b1 => { if v == 256 { ctx.publish_property("tank_full", "false"); } }
        0x2b2 => { ctx.publish_property("tank_full", flag(v)); }
        0x360 => { sset(ctx, tag, v); ctx.publish_property("sterilize", flag(v)); }
        0x2a2 => { sset(ctx, tag, v); ctx.publish_property("uvnano", flag(v)); }
        0x21e => { sset(ctx, tag, v); ctx.publish_property("bucket_light", flag(v)); }
        0x21b => { sset(ctx, tag, v); ctx.publish_property("off_timer", v.to_string()); }
        0x221 => { ctx.publish_property("error", v.to_string()); }
        _ => { return false; }
    }
    true
}

fn on_set_property(ctx, prop, value) {
    switch prop {
        "power" => {
            let on = to_bool(value);
            write(ctx, 0x1f7, if on { 1 } else { 0 }, if on { [0x1f9] } else { [] });
        }
        "mode" => {
            let wire = mode_wire(value);
            if wire == () { reject(ctx, prop, "unknown mode"); return; }
            let prev = sget(ctx, 0x1f9);
            sset(ctx, 0x1f7, 1); // selecting a mode powers on
            if wire == 19 && !is_silent(prev) {
                sset(ctx, 0x1fa, 2); // silent runs at low fan
                ctx.publish_property("fan", "low");
            }
            write(ctx, 0x1f9, wire, [0x1f7]);
        }
        "target" => {
            let n = to_number(value);
            if n == () { reject(ctx, prop, "not a number"); return; }
            let t = n.round().to_int();
            if t < 30 { t = 30; }
            if t > 70 { t = 70; }
            sset(ctx, 0x1f7, 1);
            write(ctx, 0x253, t, [0x1f7, 0x1f9]);
        }
        "fan" => {
            if value == "low" { write_fan(ctx, 2); }
            else if value == "high" { write_fan(ctx, 6); }
            else { reject(ctx, prop, "unknown fan speed"); }
        }
        "sterilize" => { write(ctx, 0x360, if to_bool(value) { 1 } else { 0 }, [0x1f7, 0x1f9]); }
        "uvnano" => { write(ctx, 0x2a2, if to_bool(value) { 1 } else { 0 }, [0x1f7, 0x1f9]); }
        "bucket_light" => { write(ctx, 0x21e, if to_bool(value) { 1 } else { 0 }, []); }
        "off_timer" => {
            let n = to_number(value);
            if n == () || n < 0 { reject(ctx, prop, "not a non-negative number"); return; }
            write(ctx, 0x21b, n.round().to_int(), []);
        }
        _ => { reject(ctx, prop, "unknown or read-only property"); }
    }
}
