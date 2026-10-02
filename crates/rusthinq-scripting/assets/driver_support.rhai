// Semantic driver compatibility lives in Rhai. Rust routes opaque MQTT messages.
fn DeviceContext.publish_raw(topic, payload, retain) {
    let ctx = this;
    ctx.publish(json_stringify(#{topic:topic,payload:payload,retain:retain}));
}
fn DeviceContext.publish_property(prop, value) {
    let ctx = this;
    let values = ctx.state_get("__il_values");
    if values == () { values = #{}; }
    values[prop] = value;
    ctx.state_set("__il_values", values);
    ctx.publish_raw(ctx.device_topic() + "/" + prop, value, true);
}
fn DeviceContext.publish_event(suffix, payload) {
    let ctx = this;
    if suffix == "reject" {
        let doc = json_parse(payload);
        if doc.code == () { doc.code = "invalid_value"; }
        payload = json_stringify(doc);
    }
    ctx.publish_raw(ctx.device_topic() + "/" + suffix, payload, false);
}
fn DeviceContext.publish_il(text) {
    let ctx = this;
    let doc = json_parse(text);
    doc.id = ctx.id();
    doc.source = "rusthinq";
    if doc["x-mqtt"] == () {
        let base = ctx.device_topic();
        let prefix = base.sub_string(0, base.len() - ctx.id().len());
        doc["x-mqtt"] = #{state:prefix+"{id}/{prop}",set:prefix+"{id}/{prop}/set",reject:prefix+"{id}/reject"};
    }
    ctx.state_set("__il_descriptor", doc);
    if ctx.il_topic() != "" { ctx.publish_raw(ctx.il_topic(), json_stringify(doc), true); }
    let values = ctx.state_get("__il_values");
    if doc.props != () {
        for prop in doc.props.keys() {
            let definition = doc.props[prop];
            if definition.role == "available" && (values == () || values[prop] == ()) {
                ctx.publish_property(prop, "false");
            }
        }
    }
}
fn __bool(text) {
    if type_of(text) != "string" { return (); }
    text.trim();
    let value = text.to_lower();
    if ["true","on","1"].contains(value) { return true; }
    if ["false","off","0"].contains(value) { return false; }
    ()
}
fn __reject(ctx, prop, code, reason) {
    ctx.publish_event("reject",json_stringify(#{prop:prop,code:code,reason:reason}));
    ()
}
fn __validate(ctx, prop, value) {
    let doc = ctx.state_get("__il_descriptor");
    if doc == () || doc.props == () { return value; }
    let def = doc.props[prop];
    if def == () { return __reject(ctx,prop,"unknown_property","unknown property"); }
    if def.type != "trigger" && def.rw != true { return __reject(ctx,prop,"read_only","read-only property"); }
    let values = ctx.state_get("__il_values");
    if type_of(def.requires) == "string" {
        if values == () || __bool(values[def.requires]) != true {
            return __reject(ctx,prop,"requires_unmet","requires "+def.requires);
        }
    } else if type_of(def.requires) == "map" {
        let requirement = def.requires;
        if values == () || requirement["in"] == () || !requirement["in"].contains(values[requirement.prop]) {
            return __reject(ctx,prop,"requires_unmet","requires "+requirement.prop);
        }
    }
    switch def.type {
        "binary" => {
            let canonical = __bool(value);
            if canonical == () { return __reject(ctx,prop,"invalid_value","not a boolean"); }
            return canonical.to_string();
        }
        "number" => {
            let n = (); try { let trimmed = value; trimmed.trim(); n = trimmed.parse_float(); } catch {}
            if n == () || n.is_nan() || n.is_infinite() { return __reject(ctx,prop,"invalid_value","not a number"); }
            if def.min != () && n < def.min { return __reject(ctx,prop,"out_of_range","below minimum"); }
            if def.max != () && n > def.max { return __reject(ctx,prop,"out_of_range","above maximum"); }
            if def.step != () && def.step > 0 {
                let base = if def.min == () {0.0} else {def.min};
                let steps = (n-base)/def.step;
                if abs(steps-round(steps)) > 0.000000001 { return __reject(ctx,prop,"bad_step","not a multiple of step"); }
            }
            return n.to_string();
        }
        "select" => {
            if def.options != () && !def.options.contains(value) { return __reject(ctx,prop,"invalid_value","not one of the options"); }
        }
        "text" => {if value == "" {return __reject(ctx,prop,"invalid_value","empty text");}}
    }
    value
}
