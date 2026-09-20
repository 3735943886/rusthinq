//! ThinQ TLV encode/decode (10-bit type + variable-length value).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tlv {
    pub t: u16,
    pub l: Option<u8>,
    pub v: u32,
}

impl Tlv {
    pub fn new(t: u16, v: u32) -> Self {
        Self { t, l: None, v }
    }
}

/// One TLV element with byte range `[byte_start, byte_end)` in the parsed buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlvSpan {
    pub tlv: Tlv,
    pub byte_start: usize,
    pub byte_end: usize,
}

/// Parse a TLV sequence. Truncation is tolerated (returns what was successfully parsed).
pub fn parse(buf: &[u8]) -> Vec<Tlv> {
    parse_with_spans(buf).into_iter().map(|s| s.tlv).collect()
}

/// Parse TLV and record each element's byte span within `buf`.
pub fn parse_with_spans(buf: &[u8]) -> Vec<TlvSpan> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < buf.len() {
        if i + 2 > buf.len() {
            return out;
        }
        let start = i;
        let t = (u16::from(buf[i]) << 2) + u16::from(buf[i + 1] >> 6);
        let l = (buf[i + 1] >> 4) & 3;
        let mut v = u32::from(buf[i + 1] & 15);

        if i + 2 + l as usize > buf.len() {
            return out;
        }

        if l > 0 {
            v = 0;
            for j in 0..l as usize {
                v = (v << 8) | u32::from(buf[i + 2 + j]);
            }
        }
        let end = i + 2 + l as usize;
        out.push(TlvSpan {
            tlv: Tlv { t, l: Some(l), v },
            byte_start: start,
            byte_end: end,
        });
        i = end;
    }
    out
}

/// Build a TLV byte sequence from elements.
pub fn build(elements: &[Tlv]) -> Vec<u8> {
    let mut out = Vec::new();
    for el in elements {
        let t0 = ((el.t >> 2) & 255) as u8;
        out.push(t0);
        let tl = ((el.t & 3) << 6) as u8;

        if el.v < 0x10 {
            out.push(tl | (el.v as u8));
        } else if el.v < 0x100 {
            out.push(tl | 0x10);
            out.push(el.v as u8);
        } else if el.v < 0x10000 {
            out.push(tl | 0x20);
            out.push(((el.v >> 8) & 0xff) as u8);
            out.push((el.v & 0xff) as u8);
        } else {
            out.push(tl | 0x30);
            out.push(((el.v >> 16) & 0xff) as u8);
            out.push(((el.v >> 8) & 0xff) as u8);
            out.push((el.v & 0xff) as u8);
        }
    }
    out
}

/// Build a complete TLV device frame: `[b0, b1, 04 00 00 00 65, b2, b3, b4, len, tlv.., crc16]`.
/// `header` is `[b0, b1]` optionally followed by `b2, b3, b4` (defaults `2, 2, 1`) — the
/// same shape `TlvDeviceCore::send` takes, so `[1, 1, 2, 2, 1]` is the values/caps query.
pub fn frame_build(header: &[u8], elements: &[Tlv]) -> Option<Vec<u8>> {
    if header.len() < 2 {
        return None;
    }
    let tlv_array = build(elements);
    let len = u8::try_from(tlv_array.len()).ok()?;
    let mut body = vec![
        0x04,
        0x00,
        0x00,
        0x00,
        0x65,
        header.get(2).copied().unwrap_or(2),
        header.get(3).copied().unwrap_or(2),
        header.get(4).copied().unwrap_or(1),
        len,
    ];
    body.extend_from_slice(&tlv_array);
    let crc = crate::crc16::crc16(&body);
    let mut out = vec![header[0], header[1]];
    out.extend_from_slice(&body);
    out.push((crc >> 8) as u8);
    out.push((crc & 0xff) as u8);
    Some(out)
}

/// Parse a device-to-host TLV state frame (the `0x87`/`0xa7`, `0x02` form
/// `TlvDeviceCore::process_data` treats as the standard one). `None` for anything else
/// — an ack, a private-command frame, a truncated buffer. The CRC is not checked, as in
/// `process_data`.
pub fn frame_parse(buf: &[u8]) -> Option<Vec<Tlv>> {
    if buf.len() < 13 {
        return None;
    }
    let standard = buf[2..6] == [0x04, 0x00, 0x00, 0x00]
        && (buf[6] == 0x87 || buf[6] == 0xa7)
        && buf[7] == 0x02
        && (buf[8] == 0x01 || buf[8] == 0x04)
        && buf[10] as usize == buf.len() - 13;
    standard.then(|| parse(&buf[11..buf.len() - 2]))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Case {
        name: &'static str,
        tlv: Tlv,
        bytes: Vec<u8>,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "l=0 single nibble",
                tlv: Tlv::new(0x1f7, 1),
                bytes: vec![0x7d, 0xc1],
            },
            Case {
                name: "l=1 byte value",
                tlv: Tlv::new(0x1fe, 0x42),
                bytes: vec![0x7f, 0x90, 0x42],
            },
            Case {
                name: "l=2 word value",
                tlv: Tlv::new(0x1fd, 0x1234),
                bytes: vec![0x7f, 0x60, 0x12, 0x34],
            },
            Case {
                name: "l=3 24-bit value",
                tlv: Tlv::new(0x100, 0x123456),
                bytes: vec![0x40, 0x30, 0x12, 0x34, 0x56],
            },
        ]
    }

    #[test]
    fn build_and_parse_all_length_encodings() {
        for c in cases() {
            assert_eq!(build(&[c.tlv]), c.bytes, "build {}", c.name);
            let parsed = parse(&c.bytes);
            assert_eq!(parsed.len(), 1, "parse {}", c.name);
            assert_eq!(parsed[0].t, c.tlv.t, "parse t {}", c.name);
            assert_eq!(parsed[0].v, c.tlv.v, "parse v {}", c.name);
        }
    }

    #[test]
    fn parse_with_spans_covers_full_buffer() {
        let bytes = build(&[Tlv::new(0x1f7, 1), Tlv::new(0x1fa, 6)]);
        let spans = parse_with_spans(&bytes);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].byte_start, 0);
        assert_eq!(spans[0].byte_end, spans[1].byte_start);
        assert_eq!(spans[1].byte_end, bytes.len());
        assert_eq!(spans[0].tlv.t, 0x1f7);
        assert_eq!(spans[1].tlv.v, 6);
    }

    #[test]
    fn round_trip_mixed_sequence() {
        let seq = [
            Tlv::new(0x1f7, 0),
            Tlv::new(0x1f9, 4),
            Tlv::new(0x1fa, 8),
            Tlv::new(0x1fe, 42),
            Tlv::new(0x2da, 0xabcd),
            Tlv::new(0x300, 0x010203),
        ];
        let bytes = build(&seq);
        let back = parse(&bytes);
        assert_eq!(back.len(), seq.len());
        for (i, s) in seq.iter().enumerate() {
            assert_eq!(back[i].t, s.t);
            assert_eq!(back[i].v, s.v);
        }
    }

    #[test]
    fn parse_vector_from_real_capture() {
        let buf = crate::hex::decode("7E427DC17E837F502D7F902A").unwrap();
        let out = parse(&buf);
        let expected = [
            (0x1f9u16, 2u32),
            (0x1f7, 1),
            (0x1fa, 3),
            (0x1fd, 0x2d),
            (0x1fe, 0x2a),
        ];
        assert_eq!(out.len(), expected.len());
        for (i, (t, v)) in expected.iter().enumerate() {
            assert_eq!(out[i].t, *t);
            assert_eq!(out[i].v, *v);
        }
    }

    #[test]
    fn parse_tolerates_truncation() {
        let buf = [0x7f, 0x60, 0x12];
        assert!(parse(&buf).is_empty());
    }

    #[test]
    fn parse_tolerates_1_byte_truncation() {
        assert!(parse(&[0x7e]).is_empty());
    }

    #[test]
    fn frame_build_matches_the_values_query_seen_on_the_wire() {
        // `01010400000065020201027d425a6e`, captured from a real device (query = 0x1f5 -> 2).
        let frame = frame_build(&[1, 1, 2, 2, 1], &[Tlv::new(0x1f5, 2)]).unwrap();
        assert_eq!(crate::hex::encode(&frame), "01010400000065020201027d425a6e");
    }

    #[test]
    fn frame_parse_reads_a_captured_state_frame_and_rejects_an_ack() {
        let state = crate::hex::decode(
            "000004000000a702041a5a7dc07e50117e8294d0287f503e86c087008980c900d8008780cd902c8840ce80ab00a88187c1e801ee408c808cc0b5d011b600b642b5d012b600b642b5d013b600b642b5d014b600b642b5d015b600b642b5d016b600b642fa80bad3",
        )
        .unwrap();
        let tlvs = frame_parse(&state).expect("standard state frame");
        assert!(tlvs.iter().any(|t| t.t == 0x1f7));
        let ack = crate::hex::decode("0301040000008701100000ec3c").unwrap();
        assert!(frame_parse(&ack).is_none());
    }
}
