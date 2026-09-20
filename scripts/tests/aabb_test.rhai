// Helpers for the tests of AABB drivers: `import "aabb_test" as a;`.

// `AA len inner checksum BB`, as the appliance frames it. `inner` is a blob or an array.
fn framed(inner) { hex_encode(aabb_wrap(inner)) }

fn bytes(list) {
    let b = blob();
    for x in list { b.push(x); }
    b
}

// A payload of `len` zero bytes with `head` written from offset 0.
fn payload(len, head) {
    let p = blob(len);
    for i in 0..head.len() { p[i] = head[i]; }
    p
}

// A 0xEC status frame of the monitoring-record family whose previous and current record are
// the same payload `p`. `addr` is the appliance's address byte (0x20 washer, 0x30 dryer, ...).
fn status(addr, p) {
    let inner = bytes([addr, 0xec]);
    for r in 0..2 {
        inner.push(0);
        inner.push(p.len());
        inner += p;
    }
    framed(inner)
}

// A single-record frame: 0xEB (header byte 0) or 0xE2 (header byte 3).
fn record(addr, kind, header, p) {
    let inner = bytes([addr, kind, header, p.len()]);
    inner += p;
    framed(inner)
}

// The body of the `i`th frame the driver sent, framing stripped (a blob).
fn inner_of(d, i) { aabb_unwrap(d.sent()[i]) }

fn inner_hex(d, i) { hex_encode(inner_of(d, i)) }

// The body after the two command bytes, as hex.
fn payload_hex(d, i) { hex_encode(inner_of(d, i).extract(2)) }
