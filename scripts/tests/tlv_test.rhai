// Helpers for the tests of TLV (DualCool family) drivers: `import "tlv_test" as t;`.

// The frame a driver sends to ask for capabilities (1) or values (2).
fn query(kind) {
    tlv_frame_build([1, 1, 2, 2, 1], [#{ t: 0x1f5, v: kind }])
}

// A state notification from the appliance carrying only `tags` (`[[tag, value], ..]`), in the
// 0xa7 envelope these units use. Returned as hex.
fn notify(tags) {
    let items = [];
    for p in tags { items.push(#{ t: p[0], v: p[1] }); }
    let body = tlv_build(items);
    let frame = blob();
    for b in [0, 0, 4, 0, 0, 0, 0xa7, 2, 4, 0] { frame.push(b); }
    frame.push(body.len());
    frame += body;
    frame.push(0);
    frame.push(0);
    hex_encode(frame)
}

// A device that has been through the handshake and holds the captured state.
fn running(d, caps, state) {
    d.start();
    d.feed(caps);
    d.feed(state);
    d
}

// The frames sent after the first `n`, each as `[[tag, value], ..]`.
fn sent_since(d, n) {
    let out = [];
    for i in n..d.sent().len() { out.push(d.sent_tlvs(i)); }
    out
}

// The most recent frame sent, as `[[tag, value], ..]`.
fn last_sent(d) { d.sent_tlvs(d.sent().len() - 1) }
