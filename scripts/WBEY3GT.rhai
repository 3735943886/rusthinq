// LG electric range / cooktop WBEY3GT ("Aries Hybrid2"): device -> IL driver. AABB, body
// starting 0x42; frame 0xEC carries two stacked 48-byte records (previous, then current; only
// the current counts). The record is one byte per entry of the model's protocol list, which
// runs WiFiAccess, FlexMode, five 9-byte burner groups, DrawerPowerLevel.
//
// LG's names do not describe this Korean model: it has three rings, and which group is which
// was settled by lighting each ring alone and watching which group moved. The names here are
// the physical ones (left rear = group at offset 20, left front = 2, right = 11).
//
// SAFETY. The appliance only acts on a command from the network while the panel's own
// remote-control button has been pressed (byte 0 bit 1, "remote start"), and clears that when
// the cooktop is switched off. This driver enforces the same: every write is rejected unless
// remote start is currently granted. And it can only turn things OFF or change a running
// ring's timer -- the LG app offers no way to light a ring on this model and never sent one,
// so no command here lights one.
//
// The lock is not a burner state: locking the panel overwrites every burner's state byte with
// 3, while the counters of a lit ring carry on. So "is this ring cooking" is the state byte OR
// (locked AND its elapsed counter is running).

import "aabb_common" as c;

fn record_len() { 48 }

// [key, label, group offset in the record, burner id in a command]. Each id was read off a
// command the LG app sent and matched to the ring that went out; the ids count a six-place
// cooktop column by column, and this model's single right-hand ring answers to 5.
fn burners() {
    [["left_rear", "Left rear", 20, 0x00],
     ["left_front", "Left front", 2, 0x01],
     ["right", "Right", 11, 0x05]]
}

fn timer_max_minutes() { 11 * 60 + 59 }

fn descriptor(ctx) {
    let props = #{
        available: #{ type: "binary", role: "available" },
        remote_start: #{ type: "binary", category: "diagnostic", label: "Remote start granted at the panel" },
        cooking: #{ type: "binary", class: "heat" },
        locked: #{ type: "binary", label: "Panel lock" },
        power_off: #{ type: "trigger", label: "Switch the cooktop off", requires: "remote_start" }
    };
    for b in burners() {
        let k = b[0];
        props[k + "_state"] = #{ type: "select", group: k, label: b[1] + " state",
                                 options: ["off", "cooking", "paused"] };
        // 0 is off and the dial runs 1..9; the model allows up to 11 (presumably a boost step)
        props[k + "_power_level"] = #{ type: "number", series: "gauge", group: k, label: b[1] + " power level" };
        props[k + "_cook_time"] = #{ type: "number", class: "duration", group: k, unit: "min", label: b[1] + " cooking time" };
        // settable, but only while the ring is lit: the command restates the power level, and
        // a level of 0 is the command to switch the ring off
        props[k + "_remaining_time"] = #{ type: "number", class: "duration", rw: true, group: k, unit: "min",
                                          min: 0, max: timer_max_minutes(), step: 1,
                                          requires: "remote_start", label: b[1] + " remaining time" };
        props[k + "_off"] = #{ type: "trigger", group: k, requires: "remote_start",
                               label: "Switch " + b[1] + " off" };
    }
    json_stringify(#{
        il: 0, kind: "cooktop", label: "Cooktop", vendor: "LG", model: ctx.model_id(), props: props
    })
}

// The model's burner-state enum lists INIT, COOKING_IN_PROGRESS, PAUSED, LOCK. 0, 1 and 3 are
// confirmed against captures; PAUSED has never appeared.
fn state_name(v) {
    switch v { 0 => "off", 1 => "cooking", 2 => "paused", _ => () }
}

// ---- host hooks -------------------------------------------------------------------

fn publish_config(ctx) { ctx.publish_il(descriptor(ctx)); }

fn on_drop(ctx) { ctx.publish_property("available", "false"); }

fn on_data(ctx, data) {
    let body = c::inner(data, 0x42);
    if body == () || body[1] != 0xec || body.len() != 2 + record_len() * 2 { return; }

    let cur = 2 + record_len(); // the current record
    ctx.publish_property("available", "true");

    let remote = (body[cur] & 0x02) != 0;
    ctx.state_set("remote", remote);
    ctx.publish_property("remote_start", if remote { "true" } else { "false" });

    let any_cooking = false;
    let locked = false;
    for b in burners() {
        let g = cur + b[2];
        let state = body[g];
        let level = body[g + 1];
        // Seconds are reported too, but the appliance only speaks up twice a minute, so a
        // second-precision property would be stale more often than right.
        let elapsed = body[g + 4] * 3600 + body[g + 3] * 60 + body[g + 2];
        let cooking = state == 1 || (state == 3 && elapsed > 0);
        if cooking { any_cooking = true; }
        if state == 3 { locked = true; }

        let name = if state == 3 { if cooking { "cooking" } else { "off" } } else { state_name(state) };
        c::pub_name(ctx, b[0] + "_state", name);
        ctx.state_set(b[0] + "_level", level);
        ctx.publish_property(b[0] + "_power_level", level.to_string());
        ctx.publish_property(b[0] + "_cook_time", (body[g + 4] * 60 + body[g + 3]).to_string());
        ctx.publish_property(b[0] + "_remaining_time", (body[g + 8] * 60 + body[g + 7]).to_string());
    }
    ctx.publish_property("cooking", if any_cooking { "true" } else { "false" });
    ctx.publish_property("locked", if locked { "true" } else { "false" });
}

// F0 43 20 08 <burner> <level> <timer h> <timer m> 00 00 00 00   set one burner
// F0 44 00                                                       switch the whole cooktop off
// The appliance acks each with 42 00 43 00 / 42 00 44 00; nothing waits on it.
fn send_burner(ctx, id, level, minutes) {
    ctx.send_raw(aabb_wrap([0xf0, 0x43, 0x20, 0x08, id, level, minutes / 60, minutes % 60, 0, 0, 0, 0]));
}

fn on_set_property(ctx, prop, value) {
    // The appliance would refuse these anyway; better to say so than to send them.
    if ctx.state_get("remote") != true {
        c::reject(ctx, prop, "remote start is not granted at the panel");
        return;
    }
    if prop == "power_off" {
        ctx.send_raw(aabb_wrap([0xf0, 0x44, 0x00]));
        return;
    }
    for b in burners() {
        if prop == b[0] + "_off" {
            send_burner(ctx, b[3], 0, 0);
            return;
        }
        if prop == b[0] + "_remaining_time" {
            let level = ctx.state_get(b[0] + "_level");
            // A timer on a ring that is not lit would mean sending level 0, which is the
            // command to switch it off. There is nothing to time, so do not.
            if level == () || level == 0 { c::reject(ctx, prop, "that ring is not lit"); return; }
            let n = c::to_number(value);
            if n == () { c::reject(ctx, prop, "not a number"); return; }
            let minutes = n.round().to_int();
            if minutes < 0 { minutes = 0; }
            if minutes > timer_max_minutes() { minutes = timer_max_minutes(); }
            send_burner(ctx, b[3], level, minutes);
            return;
        }
    }
    c::reject(ctx, prop, "unknown or read-only property");
}
