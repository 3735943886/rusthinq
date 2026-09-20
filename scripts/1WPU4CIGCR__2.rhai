// LG water purifier 1WPU4CIGCR__2 (cold / hot / purified / sterilised water): device -> IL
// driver. An AABB (`AA len .. checksum BB`) appliance that reports on its own; nothing is
// asked for. Frames are discriminated by inner byte 1 (inner byte 0 is 0x12):
//   0xEC  status: two stacked 26-byte records, the previous state then the current one
//   0x1F  today's dispensed water, six 16-bit big-endian totals in millilitres
//
// The record offsets are the model description's own field indexes, checked against the real
// cloud's snapshot of the same moment. A 255 is the model's IGNORE: a feature this unit does
// not have (sound, energy saving, clean mode) and, in the other direction, "leave this field
// as it is" -- which is what makes writing one setting possible: the write frame is the
// record written back, 0xff everywhere but the field being changed.
//
// It is not read-only, though the cloud's own config says so; the five writable settings
// below each changed and came back in the next status frame. The four sterilisation-schedule
// bytes are writable too but deliberately not offered: a time-only write moves the weekly run
// to another weekday.
//
// Not published: fields that only ever read IGNORE, the unidentified bytes (7, 9, 21, 23,
// 24), and mineral / sparkling totals (this model has no such tap). Energy is not on the
// wire at all.

fn record_len() { 26 }

fn descriptor(ctx) {
    json_stringify(#{
        il: 0,
        kind: "dispenser",
        class: "water_purifier",
        label: "Water purifier",
        vendor: "LG",
        model: ctx.model_id(),
        props: #{
            available: #{ type: "binary", role: "available" },
            status: #{ type: "select", options: ["failed", "not_working", "normal"] },
            tap_uv: #{ type: "select", label: "Tap UV lamp",
                       options: ["standby", "cleaning", "cleaning_manual"] },
            water_selection: #{ type: "select", label: "Water selected",
                                options: ["hot", "normal", "cold", "sterilized"] },
            water_amount: #{ type: "select", label: "Amount selected",
                             options: ["120ml", "250ml", "500ml", "continuous"] },
            default_water: #{ type: "select", rw: true,
                              options: ["last_used", "normal", "cold"] },
            default_amount: #{ type: "select", rw: true, options: ["120ml", "250ml", "500ml"] },
            auto_care: #{ type: "binary", rw: true },
            button_sound: #{ type: "binary", rw: true },
            not_use_notice: #{ type: "binary", rw: true, label: "Unused-water notice" },
            self_clean_next: #{ type: "text", label: "Next self-clean, UTC (MM-DD HH:MM)" },
            hot_water_today: #{ type: "number", unit: "mL", label: "Hot water dispensed today" },
            normal_water_today: #{ type: "number", unit: "mL", label: "Purified water dispensed today" },
            cold_water_today: #{ type: "number", unit: "mL", label: "Cold water dispensed today" },
            sterilized_water_today: #{ type: "number", unit: "mL", label: "Sterilised water dispensed today" }
        }
    })
}

// (Rhai functions cannot see top-level constants, so the tables are functions.)
fn status_name(v) {
    switch v { 0 => "failed", 1 => "not_working", 2 => "normal", _ => () }
}
fn tap_uv_name(v) {
    switch v { 0 => "standby", 1 => "cleaning", 2 => "cleaning_manual", _ => () }
}
fn water_name(v) {
    switch v { 1 => "hot", 2 => "normal", 3 => "cold", 4 => "sterilized", _ => () }
}
fn amount_name(v) {
    switch v { 1 => "120ml", 2 => "250ml", 3 => "500ml", 4 => "continuous", _ => () }
}
fn default_water_name(v) {
    switch v { 1 => "last_used", 2 => "normal", 3 => "cold", _ => () }
}
fn default_water_wire(name) {
    switch name { "last_used" => 1, "normal" => 2, "cold" => 3, _ => () }
}
// The default may not be "continuous": that is the button held down, not a quantity.
fn default_amount_wire(name) {
    switch name { "120ml" => 1, "250ml" => 2, "500ml" => 3, _ => () }
}

fn pad2(n) { if n < 10 { "0" + n } else { n.to_string() } }

fn pub_name(ctx, prop, name) {
    if name != () { ctx.publish_property(prop, name); }
}

fn on_off(v) { if v == 1 { "true" } else { "false" } }

fn to_bool(text) {
    let t = text.to_lower();
    t == "true" || t == "on" || t == "1"
}

// ---- host hooks -------------------------------------------------------------------

fn publish_config(ctx) { ctx.publish_il(descriptor(ctx)); }

fn on_drop(ctx) { ctx.publish_property("available", "false"); }

fn on_data(ctx, data) {
    let inner = aabb_unwrap(data);
    if inner == () || inner.len() < 2 || inner[0] != 0x12 { return; }

    if inner[1] == 0xec && inner.len() == 2 + record_len() * 2 {
        // The frame restates the previous record so a listener that missed one can see what
        // changed; only where the appliance is now matters.
        status(ctx, inner, 2 + record_len());
    } else if inner[1] == 0x1f && inner.len() == 2 + 6 * 2 {
        counters(ctx, inner);
    }
}

fn status(ctx, r, o) {
    ctx.publish_property("available", "true");
    pub_name(ctx, "status", status_name(r[o + 0]));
    pub_name(ctx, "tap_uv", tap_uv_name(r[o + 1]));
    pub_name(ctx, "water_selection", water_name(r[o + 2]));
    pub_name(ctx, "water_amount", amount_name(r[o + 3]));
    pub_name(ctx, "default_water", default_water_name(r[o + 10]));
    pub_name(ctx, "default_amount", amount_name(r[o + 11]));
    ctx.publish_property("auto_care", on_off(r[o + 20]));
    ctx.publish_property("button_sound", on_off(r[o + 12]));
    ctx.publish_property("not_use_notice", on_off(r[o + 8]));

    // The next self-clean, as month / day / hour / minute with no year, in UTC. Converting
    // to local time needs a timezone and a calendar, which is a consumer's business.
    let month = r[o + 15];
    let day = r[o + 16];
    if month >= 1 && month <= 12 && day >= 1 && day <= 31 {
        ctx.publish_property("self_clean_next",
            pad2(month) + "-" + pad2(day) + " " + pad2(r[o + 17]) + ":" + pad2(r[o + 18]));
    }
}

// Six 16-bit totals in the order of the model's tap list: hot, normal, cold, sterilised,
// mineral, sparkling. They are today's totals and start again each day.
fn counters(ctx, r) {
    let names = ["hot_water_today", "normal_water_today", "cold_water_today", "sterilized_water_today"];
    for i in 0..names.len() {
        let v = r[2 + i * 2] * 256 + r[3 + i * 2];
        ctx.publish_property(names[i], v.to_string());
    }
}

// One setting travels at a time; the rest of the record says "leave it". Nothing is
// published from a write: the appliance sends a status frame about a second later, so the
// property follows what the appliance says and a declined setting simply stays where it was.
fn set_field(ctx, offset, value) {
    let inner = [0xf0, 0x17];
    for i in 0..record_len() { inner.push(if i == offset { value } else { 0xff }); }
    ctx.send_raw(aabb_wrap(inner));
}

fn reject(ctx, prop, reason) {
    ctx.publish_event("reject", json_stringify(#{ prop: prop, reason: reason }));
}

fn on_set_property(ctx, prop, value) {
    switch prop {
        "default_water" => {
            let w = default_water_wire(value);
            if w == () { reject(ctx, prop, "unknown water"); return; }
            set_field(ctx, 10, w);
        }
        "default_amount" => {
            let w = default_amount_wire(value);
            if w == () { reject(ctx, prop, "unknown amount"); return; }
            set_field(ctx, 11, w);
        }
        "auto_care" => { set_field(ctx, 20, if to_bool(value) { 1 } else { 0 }); }
        "button_sound" => { set_field(ctx, 12, if to_bool(value) { 1 } else { 0 }); }
        "not_use_notice" => { set_field(ctx, 8, if to_bool(value) { 1 } else { 0 }); }
        _ => { reject(ctx, prop, "unknown or read-only property"); }
    }
}
