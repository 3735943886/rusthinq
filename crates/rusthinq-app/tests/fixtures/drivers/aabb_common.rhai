// Shared by the AABB (`AA len .. checksum BB`) device drivers: `import "aabb_common" as c;`.
// A module cannot call back into its importer, so each device script keeps its hooks and
// its tables and delegates the plain helpers here. AABB appliances report on their own, so
// there is no handshake to share, only framing and value helpers.

// The inner body of an `AA..BB` frame whose first byte is `first`, or `()` for anything else
// (a frame of another kind, an unframed buffer). The checksum is not validated, as elsewhere.
import "il_common" as il;

fn inner(data, first) {
    let body = aabb_unwrap(data);
    if body == () || body.len() < 2 || body[0] != first { return (); }
    body
}

fn pad2(n) { if n < 10 { "0" + n } else { n.to_string() } }

// Publish `name` unless it is `()` (a wire code the table does not know is left unpublished
// rather than published as a made-up value).
fn pub_name(ctx, prop, name) {
    if name != () { il::publish_property(ctx, prop, name); }
}

// A binary property from a wire number that is 1 when on.
fn on_off(v) { if v == 1 { "true" } else { "false" } }

// A binary property from a bit of a flags byte.
fn bit(v, mask) { if (v & mask) != 0 { "true" } else { "false" } }

fn to_bool(text) {
    let t = text.to_lower();
    t == "true" || t == "on" || t == "1"
}

// A number from text, or `()` if it is not one.
fn to_number(text) {
    try { text.parse_float() } catch { () }
}

fn reject(ctx, prop, reason) {
    il::publish_event(ctx, "reject", json_stringify(#{ prop: prop, reason: reason }));
}
