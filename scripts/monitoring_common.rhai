// The "monitoring record" flavour of the AABB protocol, shared by the Korean washer / dryer /
// styler family (F24VDD, RH14_N_KR, Pd0F_F, S3BF_POD_DN4): `import "monitoring_common" as m;`.
//
// The appliance reports its whole state as one fixed-length payload in a record header:
//
//   0xEC  [dev][EC] [00][len][payload: previous state] [00][len][payload: current state]
//   0xEB  [dev][EB] [00][len][payload: current state]      right after (re)connecting
//   0xE2  [dev][E2] [03][len][payload: current state]      sent when a cycle begins
//
// and takes commands as
//
//   f0 24 <type> 01 00      short control: type 1 = power off, type 4 = pause
//   f0 26 <payload>         start a course, or resume a paused one (same frame, initial bit cleared)
//
// answered about a second later with a four-byte frame `[dev] 00 <command> <status>`, status 0
// taken and 0xff refused. A zero says the frame arrived and was well formed, not that the
// appliance understood it. The appliance obeys none of this unless "remote start" is armed on
// its own panel.

import "aabb_common" as c;

fn control_power_off() { 0x01 }
fn control_pause() { 0x04 }

// The offset of the current record's payload in the inner body, or `()` when `data` is not a
// record of this device. Both header bytes are checked, so a model whose payload length
// changes under a firmware update reports nothing rather than fields read off wrong offsets.
// Returns `#{ body, p }`: read a field as `body[p + OFFSET]`.
fn record(data, dev, payload_len) {
    let body = c::inner(data, dev);
    if body == () { return (); }

    let record_len = payload_len + 2;
    let start = 0;
    let header = 0x00;
    if body[1] == 0xec {
        if body.len() != 2 + 2 * record_len { return (); }
        start = 2 + record_len;
    } else if body[1] == 0xeb || body[1] == 0xe2 {
        if body.len() != 2 + record_len { return (); }
        start = 2;
        if body[1] == 0xe2 { header = 0x03; }
    } else {
        return ();
    }
    if body[start] != header || body[start + 1] != payload_len { return (); }
    #{ body: body, p: start + 2 }
}

// `#{ command, refused }` if `data` is an acknowledgement from this device, else `()`.
fn ack(data, dev) {
    let body = c::inner(data, dev);
    if body == () || body.len() != 4 || body[1] != 0x00 { return (); }
    #{ command: body[2], refused: body[3] != 0 }
}

// Report a refusal against the property whose command it answers. An accepted command
// publishes nothing: the appliance changes state, or it does not, and the record says which.
fn on_ack(ctx, ack) {
    if ack.refused {
        let prop = ctx.state_get("last_cmd");
        c::reject(ctx, if prop == () { "command" } else { prop }, "refused by the appliance");
    }
}

fn short_control(ctx, kind, prop) {
    ctx.state_set("last_cmd", prop);
    ctx.send_raw(aabb_wrap([0xf0, 0x24, kind, 0x01, 0x00]));
}

fn course_control(ctx, payload, prop) {
    ctx.state_set("last_cmd", prop);
    let inner = [0xf0, 0x26];
    for b in payload { inner.push(b); }
    ctx.send_raw(aabb_wrap(inner));
}

// ---- the course a Start will ask for -----------------------------------------------
//
// The appliance has no notion of a pending course: its record reports where the dial sits,
// and that only moves when someone turns it. A start command has to name one anyway, so the
// driver keeps a selection of its own, seeded from the appliance and changed by a write.
// It FOLLOWS the dial rather than mirroring it (mirroring would undo a chosen course at the
// next report, which repeats the same dial position): it changes when the appliance's own
// course changes. It only ever holds a course the driver can start, and power-off (which
// zeroes the course byte) is ignored by the same rule, so the choice survives a power cycle.

// Returns the new selection if `course` changed it, else `()`.
fn follow(ctx, course, startable) {
    if ctx.state_get("reported") == course { return (); }
    ctx.state_set("reported", course);
    if startable.contains(course) {
        ctx.state_set("selected", course);
        return course;
    }
    ()
}

fn select(ctx, code, startable) {
    if startable.contains(code) { ctx.state_set("selected", code); return true; }
    false
}

fn selected(ctx) {
    let s = ctx.state_get("selected");
    if s == () { 0 } else { s }
}
