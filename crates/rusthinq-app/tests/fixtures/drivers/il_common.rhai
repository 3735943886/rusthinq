// IL output for drivers that speak the IL: `import "il_common" as il;`. rusthinq itself is
// IL-agnostic: it only carries `ctx.publish_raw(topic, payload, retain)` to MQTT and delivers
// `<prefix>/<id>/<prop>/set` commands to `on_set_property`. Everything below (descriptor,
// values, command validation and rejects) is this repository's, per il.md / il-mqtt.md.
//
// A driver publishes through `il::publish_il(ctx, descriptor)`, `il::publish_property(ctx,
// prop, value)` and `il::publish_event(ctx, suffix, payload)`, and validates a command before
// acting on it:
//
//   fn on_set_property(ctx, prop, value) {
//       let value = il::validate(ctx, prop, value);
//       if value != () { set_property(ctx, prop, value); }
//   }

// il-mqtt M-1: the descriptor goes to `<il_prefix>/<id>`, retained; `il` is the default.
// Return "" to publish no descriptor.
fn prefix() { "il" }

fn publish_property(ctx, prop, value) {
    let values = ctx.state_get("__il_values");
    if values == () { values = #{}; }
    values[prop] = value;
    ctx.state_set("__il_values", values);
    ctx.publish_raw(ctx.device_topic() + "/" + prop, value, true);
}

fn publish_event(ctx, suffix, payload) {
    if suffix == "reject" {
        let doc = json_parse(payload);
        if doc.code == () { doc.code = "invalid_value"; }
        payload = json_stringify(doc);
    }
    ctx.publish_raw(ctx.device_topic() + "/" + suffix, payload, false);
}

fn publish_il(ctx, text) {
    let doc = json_parse(text);
    doc.id = ctx.id();
    doc.source = "rusthinq";
    if doc["x-mqtt"] == () {
        let base = ctx.device_topic();
        let root = base.sub_string(0, base.len() - ctx.id().len());
        doc["x-mqtt"] = #{state: root + "{id}/{prop}", set: root + "{id}/{prop}/set", reject: root + "{id}/reject"};
    }
    ctx.state_set("__il_descriptor", doc);
    if prefix() != "" { ctx.publish_raw(prefix() + "/" + ctx.id(), json_stringify(doc), true); }
    let values = ctx.state_get("__il_values");
    if doc.props != () {
        for prop in doc.props.keys() {
            let definition = doc.props[prop];
            if definition.role == "available" && (values == () || values[prop] == ()) {
                publish_property(ctx, prop, "false");
            }
        }
    }
}

fn to_bool(text) {
    if type_of(text) != "string" { return (); }
    text.trim();
    let value = text.to_lower();
    if ["true", "on", "1"].contains(value) { return true; }
    if ["false", "off", "0"].contains(value) { return false; }
    ()
}

fn reject(ctx, prop, code, reason) {
    publish_event(ctx, "reject", json_stringify(#{prop: prop, code: code, reason: reason}));
    ()
}

// The canonical value for a command against the published descriptor, or () after publishing
// its reject. Without a descriptor the value passes through unchanged.
fn validate(ctx, prop, value) {
    let doc = ctx.state_get("__il_descriptor");
    if doc == () || doc.props == () { return value; }
    let def = doc.props[prop];
    if def == () { return reject(ctx, prop, "unknown_property", "unknown property"); }
    if def.type != "trigger" && def.rw != true { return reject(ctx, prop, "read_only", "read-only property"); }
    let values = ctx.state_get("__il_values");
    if type_of(def.requires) == "string" {
        if values == () || to_bool(values[def.requires]) != true {
            return reject(ctx, prop, "requires_unmet", "requires " + def.requires);
        }
    } else if type_of(def.requires) == "map" {
        let requirement = def.requires;
        if values == () || requirement["in"] == () || !requirement["in"].contains(values[requirement.prop]) {
            return reject(ctx, prop, "requires_unmet", "requires " + requirement.prop);
        }
    }
    switch def.type {
        "binary" => {
            let canonical = to_bool(value);
            if canonical == () { return reject(ctx, prop, "invalid_value", "not a boolean"); }
            return canonical.to_string();
        }
        "number" => {
            let n = (); try { let trimmed = value; trimmed.trim(); n = trimmed.parse_float(); } catch {}
            if n == () || n.is_nan() || n.is_infinite() { return reject(ctx, prop, "invalid_value", "not a number"); }
            if def.min != () && n < def.min { return reject(ctx, prop, "out_of_range", "below the minimum of " + def.min); }
            if def.max != () && n > def.max { return reject(ctx, prop, "out_of_range", "above the maximum of " + def.max); }
            if def.step != () && def.step > 0 {
                let base = if def.min == () { 0.0 } else { def.min };
                let steps = (n - base) / def.step;
                if abs(steps - round(steps)) > 0.000000001 { return reject(ctx, prop, "bad_step", "not a multiple of step"); }
            }
            return n.to_string();
        }
        "select" => {
            if def.options != () && !def.options.contains(value) { return reject(ctx, prop, "invalid_value", "not one of the options"); }
        }
        "text" => { if value == "" { return reject(ctx, prop, "invalid_value", "empty text"); } }
    }
    value
}
