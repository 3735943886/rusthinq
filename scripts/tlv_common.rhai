// Shared by the TLV (DualCool family) device drivers: `import "tlv_common" as c;`.
//
// A module cannot call back into the script that imports it, so the device script keeps
// the hooks (start / on_data / on_timer / on_set_property) and delegates the parts that
// are the same for every TLV appliance: the capability -> values handshake with retries,
// the slow refresh, and building write frames. What is left in a device script is its tag
// map. Per-device state lives in `ctx.state_*`, under the keys "caps", "values" and
// "t<tag>" (the last written or reported value of each tag).
//
// (Rhai functions cannot see top-level constants, so the tunables are functions.)

fn retry_ms() { 15000 }
fn refresh_ms() { 900000 }

// The query header and the write header differ only in the third-from-last byte.
fn query_header() { [1, 1, 2, 2, 1] }
fn write_header() { [1, 1, 2, 1, 1] }

// ---- state ------------------------------------------------------------------------

fn is_set(ctx, key) { ctx.state_get(key) == true }

// Last known value of a tag (0 if never seen).
fn get(ctx, tag) {
    let v = ctx.state_get("t" + tag);
    if v == () { 0 } else { v }
}

fn put(ctx, tag, v) { ctx.state_set("t" + tag, v); }

// ---- value helpers ------------------------------------------------------------------

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

// ---- protocol -----------------------------------------------------------------------

// 1 = capabilities, 2 = values.
fn query(ctx, kind) {
    ctx.send_raw(tlv_frame_build(query_header(), [#{ t: 0x1f5, v: kind }]));
}

// Send a write: `tag=value`, plus the attached tags at their last known values (the
// appliance ignores some writes unless the power/mode tags come with them).
fn write(ctx, tag, value, attach) {
    put(ctx, tag, value);
    let tlvs = [#{ t: tag, v: value }];
    for a in attach {
        tlvs.push(#{ t: a, v: get(ctx, a) });
    }
    ctx.send_raw(tlv_frame_build(write_header(), tlvs));
}

// ---- hooks to delegate to -------------------------------------------------------------

// From the device's `start(ctx)`.
fn start(ctx) {
    ctx.state_set("caps", false);
    ctx.state_set("values", false);
    query(ctx, 1);
    ctx.set_timer("caps_retry", retry_ms());
}

// From the device's `on_timer(ctx, name)`.
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

// From the device's `on_data(ctx, data)`. Consumes the capability reply (tag 0x2da) and
// the frames that are not standard TLV state frames, and returns `()` for those. For a
// state frame it returns `#{ tlvs, first }`: the tag list to apply, and whether this is
// the first values frame (then the retry timer is already cancelled, the refresh armed,
// and the device should do its one-off first-values work).
fn receive(ctx, data) {
    let tlvs = tlv_frame_parse(data);
    if tlvs == () { return (); }

    if tlvs.some(|t| t.t == 0x2da) {
        if !is_set(ctx, "caps") {
            ctx.state_set("caps", true);
            ctx.cancel_timer("caps_retry");
            query(ctx, 2);
            ctx.set_timer("values_retry", retry_ms());
        }
        return ();
    }

    let first = !is_set(ctx, "values");
    if first {
        ctx.state_set("values", true);
        ctx.cancel_timer("values_retry");
        ctx.set_timer("refresh", refresh_ms());
    }
    #{ tlvs: tlvs, first: first }
}
