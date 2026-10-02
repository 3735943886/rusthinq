//! Pure encode/decode of wire packets between appliance and (re)cloud.
//!
//! Framing mirrors the TypeScript implementation:
//! - TLV toDevice / fromDevice
//! - AABB both directions

use crate::crc16::crc16;
use crate::tlv::{self, Tlv};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Tlv,
    Aabb,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    FromDevice,
    ToDevice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlvEncodeInput {
    pub direction: Direction,
    pub tlv: Vec<Tlv>,
    pub a: Option<u8>,
    pub s: Option<u8>,
    pub byte5: Option<u8>,
    pub byte6: Option<u8>,
    pub byte7: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AabbEncodeInput {
    pub body_hex: String,
    pub direction: Option<Direction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeInput {
    Tlv(TlvEncodeInput),
    Aabb(AabbEncodeInput),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlvFrame {
    pub kind: u8,
    pub byte5: u8,
    pub byte6: u8,
    pub byte7: u8,
    pub len: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedTlv {
    pub direction: Direction,
    pub crc_ok: bool,
    pub tlv: Vec<Tlv>,
    pub frame: TlvFrame,
    pub a: Option<u8>,
    pub s: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedAabb {
    pub checksum_ok: bool,
    pub length: u8,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedUnknown {
    pub hex: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    Tlv(DecodedTlv),
    Aabb(DecodedAabb),
    Unknown(DecodedUnknown),
}

impl Decoded {
    pub fn protocol(&self) -> Protocol {
        match self {
            Decoded::Tlv(_) => Protocol::Tlv,
            Decoded::Aabb(_) => Protocol::Aabb,
            Decoded::Unknown(_) => Protocol::Unknown,
        }
    }
}

#[derive(Debug)]
pub enum EncodeError {
    TlvTooLarge,
    AabbTooLarge,
    InvalidHex(String),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AabbTooLarge => f.write_str("AABB payload exceeds short framing"),
            Self::TlvTooLarge => f.write_str("TLV payload exceeds 255 bytes"),
            Self::InvalidHex(s) => write!(f, "invalid hex body: {s}"),
        }
    }
}

impl std::error::Error for EncodeError {}

/// AABB checksum: sum of every byte preceding checksum+BB, mod 256, xor 0x55.
pub fn aabb_checksum(packet_without_checksum: &[u8]) -> u8 {
    let sum: u32 = packet_without_checksum.iter().map(|&b| u32::from(b)).sum();
    ((sum & 0xff) as u8) ^ 0x55
}

pub fn encode_packet(input: &EncodeInput) -> Result<(String, Vec<u8>), EncodeError> {
    let buffer = match input {
        EncodeInput::Tlv(t) => encode_tlv(t)?,
        EncodeInput::Aabb(a) => encode_aabb(a)?,
    };
    Ok((crate::hex::encode(&buffer), buffer))
}

fn encode_tlv(input: &TlvEncodeInput) -> Result<Vec<u8>, EncodeError> {
    let tlv_bytes = tlv::build(&input.tlv);
    if tlv_bytes.len() > 255 {
        return Err(EncodeError::TlvTooLarge);
    }

    let (uart, prefix): (Vec<u8>, Vec<u8>) = if input.direction == Direction::FromDevice {
        let mut uart = vec![
            0x04,
            0x00,
            0x00,
            0x00,
            0x87,
            input.byte5.unwrap_or(0x02),
            input.byte6.unwrap_or(0x04),
            input.byte7.unwrap_or(0x00),
            tlv_bytes.len() as u8,
        ];
        uart.extend_from_slice(&tlv_bytes);
        (uart, vec![0x00, 0x00])
    } else {
        let mut uart = vec![
            0x04,
            0x00,
            0x00,
            0x00,
            0x65,
            input.byte5.unwrap_or(2),
            input.byte6.unwrap_or(2),
            input.byte7.unwrap_or(1),
            tlv_bytes.len() as u8,
        ];
        uart.extend_from_slice(&tlv_bytes);
        (uart, vec![input.a.unwrap_or(0), input.s.unwrap_or(0)])
    };

    let crc = crc16(&uart);
    let mut out = prefix;
    out.extend_from_slice(&uart);
    out.push(((crc >> 8) & 0xff) as u8);
    out.push((crc & 0xff) as u8);
    Ok(out)
}

fn encode_aabb(input: &AabbEncodeInput) -> Result<Vec<u8>, EncodeError> {
    let inner = crate::hex::decode(input.body_hex.replace(' ', ""))
        .map_err(|e| EncodeError::InvalidHex(e.to_string()))?;
    crate::aabb::wrap(&inner).ok_or(EncodeError::AabbTooLarge)
}

pub fn decode_packet(hex_str: &str) -> Decoded {
    let cleaned: String = hex_str.chars().filter(|c| !c.is_whitespace()).collect();
    let buf = match crate::hex::decode(&cleaned) {
        Ok(b) => b,
        Err(_) => {
            return Decoded::Unknown(DecodedUnknown {
                hex: cleaned,
                reason: "invalid hex".into(),
            });
        }
    };

    // AABB: AA <len> ...body <checksum> BB
    if buf.len() >= 5
        && let [0xaa, length, body @ .., checksum, 0xbb] = buf.as_slice()
        && let Some(covered) = buf.get(..buf.len() - 2)
    {
        return Decoded::Aabb(DecodedAabb {
            checksum_ok: *checksum == aabb_checksum(covered),
            length: *length,
            body: crate::hex::encode(body),
        });
    }

    // UART envelope: 04 00 00 00 | kind | b5 b6 b7 | len | body | crc16
    // Standard climate TLV uses kind 0x87/0xa7 (fromDevice) or 0x65 (toDevice).
    // Other kinds (e.g. 0xa8 SUPERSET/private blobs) share the envelope but are not TLV.
    if buf.len() >= 13
        && let &[
            a,
            s,
            0x04,
            0x00,
            0x00,
            0x00,
            kind,
            b5,
            b6,
            b7,
            len_byte,
            ref rest @ ..,
        ] = buf.as_slice()
    {
        let len = len_byte as usize;
        let Some(body) = rest.get(..len).filter(|_| rest.len() >= len + 2) else {
            return Decoded::Unknown(DecodedUnknown {
                hex: cleaned,
                reason: "UART length field overruns buffer".into(),
            });
        };
        let crc_ok = crc16(buf.get(2..).unwrap_or_default()) == 0;
        let frame = TlvFrame {
            kind,
            byte5: b5,
            byte6: b6,
            byte7: b7,
            len: len_byte,
        };
        let from_device = kind != 0x65;
        let is_standard_tlv_kind = kind == 0x87 || kind == 0xa7 || kind == 0x65;
        // Values/query path: b5 in {1,2} and b6 in {1,2,4} is the climate TLV dialect.
        let looks_like_climate_tlv =
            is_standard_tlv_kind && (b5 == 0x01 || b5 == 0x02) && matches!(b6, 0x01 | 0x02 | 0x04);
        // Empty-body frames on standard kinds (often b6=0x10 ACK) are still TLV envelope —
        // not binary blobs. Surface as Tlv with empty tag list.
        let empty_standard_ack = is_standard_tlv_kind && len == 0;

        if looks_like_climate_tlv || empty_standard_ack {
            let tlv = tlv::parse(body);
            return if from_device {
                Decoded::Tlv(DecodedTlv {
                    direction: Direction::FromDevice,
                    crc_ok,
                    tlv,
                    frame,
                    a: None,
                    s: None,
                })
            } else {
                Decoded::Tlv(DecodedTlv {
                    direction: Direction::ToDevice,
                    crc_ok,
                    tlv,
                    frame,
                    a: Some(a),
                    s: Some(s),
                })
            };
        }

        // Non-TLV UART (private / SUPERSET / extended). Surface as Unknown with structured reason
        // so management UI does not invent phantom TLV tags from binary body.
        return Decoded::Unknown(DecodedUnknown {
            hex: cleaned,
            reason: format!(
                "uart_binary kind=0x{kind:02x} b5=0x{b5:02x} b6=0x{b6:02x} b7=0x{b7:02x} body_len={len} crc_ok={crc_ok}"
            ),
        });
    }

    Decoded::Unknown(DecodedUnknown {
        hex: cleaned,
        reason: "unrecognized framing".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FROM_DEVICE: &str = "000004000000870204690b8ca036458cd059ace001de6ac9";
    const TO_DEVICE: &str = "00010400000065020201027d416a0d";
    const AABB: &str = "aa16f0263a03ff040100000000000300000000004fbb";
    const FROM_DEVICE_A7: &str = "000004000000a70204000c7dc17e417f902a7e887f502d19e9";

    #[test]
    fn decode_from_device_tlv_crc_valid() {
        let d = decode_packet(FROM_DEVICE);
        assert_eq!(d.protocol(), Protocol::Tlv);
        if let Decoded::Tlv(t) = d {
            assert_eq!(t.direction, Direction::FromDevice);
            assert!(t.crc_ok);
            assert_eq!(t.frame.kind, 0x87);
            assert_eq!(t.frame.len, 0x0b);
            assert!(!t.tlv.is_empty());
        }
    }

    #[test]
    fn decode_from_device_kind_a7() {
        let d = decode_packet(FROM_DEVICE_A7);
        if let Decoded::Tlv(t) = d {
            assert_eq!(t.direction, Direction::FromDevice);
            assert!(t.crc_ok);
            assert_eq!(t.frame.kind, 0xa7);
            assert!(t.tlv.iter().any(|x| x.t == 0x1f7));
        } else {
            panic!("expected tlv");
        }
    }

    #[test]
    fn decode_to_device_tlv() {
        let d = decode_packet(TO_DEVICE);
        if let Decoded::Tlv(t) = d {
            assert_eq!(t.direction, Direction::ToDevice);
            assert!(t.crc_ok);
            assert_eq!(t.frame.kind, 0x65);
            assert_eq!(t.a, Some(0x00));
            assert_eq!(t.s, Some(0x01));
        } else {
            panic!("expected tlv");
        }
    }

    #[test]
    fn decode_aabb() {
        let d = decode_packet(AABB);
        if let Decoded::Aabb(a) = d {
            assert!(a.checksum_ok);
            assert_eq!(a.length, 0x16);
        } else {
            panic!("expected aabb");
        }
    }

    /// Empty-body fromDevice ACK (kind 0x87, b6=0x10) is TLV envelope, not uart_binary.
    #[test]
    fn decode_empty_kind_87_ack_as_tlv() {
        // 02 01 | 04 00 00 00 | 87 01 10 00 | len=00 | crc
        let hex = "0201040000008701100000ec3c";
        let d = decode_packet(hex);
        assert_eq!(d.protocol(), Protocol::Tlv, "reason-like: {:?}", d);
        if let Decoded::Tlv(t) = d {
            assert_eq!(t.frame.kind, 0x87);
            assert_eq!(t.frame.len, 0);
            assert_eq!(t.frame.byte6, 0x10);
            assert!(t.tlv.is_empty());
            assert!(t.crc_ok);
        } else {
            panic!("expected empty Tlv");
        }
    }

    /// DHUM private/SUPERSET-class frame (kind 0xa8) must not be misread as climate TLV.
    #[test]
    fn decode_uart_binary_kind_a8_not_tlv() {
        let hex = "000004000000a8661001490a010d10cf0111320200000000000001000000030100000000000000331e0007b81e0000000002260226024e365000fa00002100000000000000000222011e011c1e011e1e2f90bc00ef61";
        let d = decode_packet(hex);
        match d {
            Decoded::Unknown(u) => {
                assert!(u.reason.starts_with("uart_binary"), "reason={}", u.reason);
                assert!(u.reason.contains("kind=0xa8"));
                assert!(u.reason.contains("crc_ok=true"));
            }
            other => panic!("expected uart_binary unknown, got {:?}", other.protocol()),
        }
    }

    #[test]
    fn decode_corrupted_crc_reported() {
        let bad = format!("{}00", &FROM_DEVICE[..FROM_DEVICE.len() - 2]);
        let d = decode_packet(&bad);
        if let Decoded::Tlv(t) = d {
            assert!(!t.crc_ok);
        } else {
            panic!("expected tlv");
        }
    }

    #[test]
    fn decode_garbage_unknown() {
        assert_eq!(decode_packet("deadbeef").protocol(), Protocol::Unknown);
    }

    #[test]
    fn encode_from_device_round_trip() {
        let tlv = vec![Tlv::new(0x2c1, 1), Tlv::new(0x2c2, 380)];
        let (hex, _) = encode_packet(&EncodeInput::Tlv(TlvEncodeInput {
            direction: Direction::FromDevice,
            tlv: tlv.clone(),
            a: None,
            s: None,
            byte5: None,
            byte6: None,
            byte7: Some(0x69),
        }))
        .unwrap();
        let d = decode_packet(&hex);
        if let Decoded::Tlv(t) = d {
            assert_eq!(t.direction, Direction::FromDevice);
            assert!(t.crc_ok);
            assert_eq!(
                t.tlv.iter().map(|e| (e.t, e.v)).collect::<Vec<_>>(),
                tlv.iter().map(|e| (e.t, e.v)).collect::<Vec<_>>()
            );
        } else {
            panic!("expected tlv");
        }
    }

    #[test]
    fn encode_from_device_honors_bytes() {
        let (hex, _) = encode_packet(&EncodeInput::Tlv(TlvEncodeInput {
            direction: Direction::FromDevice,
            tlv: vec![Tlv::new(0x1fa, 3)],
            a: None,
            s: None,
            byte5: Some(0x02),
            byte6: Some(0x04),
            byte7: Some(0x9a),
        }))
        .unwrap();
        let d = decode_packet(&hex);
        if let Decoded::Tlv(t) = d {
            assert_eq!(t.frame.kind, 0x87);
            assert_eq!(t.frame.byte5, 0x02);
            assert_eq!(t.frame.byte6, 0x04);
            assert_eq!(t.frame.byte7, 0x9a);
        } else {
            panic!("expected tlv");
        }
    }

    #[test]
    fn encode_from_device_default_byte6() {
        let (hex, _) = encode_packet(&EncodeInput::Tlv(TlvEncodeInput {
            direction: Direction::FromDevice,
            tlv: vec![Tlv::new(0x1fa, 3)],
            a: None,
            s: None,
            byte5: None,
            byte6: None,
            byte7: None,
        }))
        .unwrap();
        if let Decoded::Tlv(t) = decode_packet(&hex) {
            assert_eq!(t.frame.byte6, 0x04);
        } else {
            panic!("expected tlv");
        }
    }

    #[test]
    fn encode_from_device_byte_for_byte_capture() {
        let (hex, _) = encode_packet(&EncodeInput::Tlv(TlvEncodeInput {
            direction: Direction::FromDevice,
            tlv: vec![Tlv::new(506, 3)],
            a: None,
            s: None,
            byte5: Some(0x02),
            byte6: Some(0x04),
            byte7: Some(0x9a),
        }))
        .unwrap();
        assert_eq!(hex, "0000040000008702049a027e83ab55");
    }

    #[test]
    fn encode_to_device_round_trip() {
        let (hex, _) = encode_packet(&EncodeInput::Tlv(TlvEncodeInput {
            direction: Direction::ToDevice,
            tlv: vec![Tlv::new(0x1f5, 2)],
            a: Some(1),
            s: Some(1),
            byte5: Some(2),
            byte6: Some(2),
            byte7: Some(1),
        }))
        .unwrap();
        if let Decoded::Tlv(t) = decode_packet(&hex) {
            assert!(t.crc_ok);
            assert_eq!(t.a, Some(1));
            assert_eq!(t.s, Some(1));
            assert_eq!(t.frame.kind, 0x65);
            assert_eq!(t.frame.byte5, 2);
            assert_eq!(t.frame.byte6, 2);
            assert_eq!(t.frame.byte7, 1);
        } else {
            panic!("expected tlv");
        }
    }

    #[test]
    fn encode_to_device_query_caps() {
        let (hex, _) = encode_packet(&EncodeInput::Tlv(TlvEncodeInput {
            direction: Direction::ToDevice,
            tlv: vec![Tlv::new(0x1f5, 1)],
            a: Some(1),
            s: Some(1),
            byte5: Some(2),
            byte6: Some(2),
            byte7: Some(1),
        }))
        .unwrap();
        assert!(
            hex.starts_with("0101040000006502020102"),
            "unexpected framing: {hex}"
        );
        assert_eq!(decode_packet(&hex).protocol(), Protocol::Tlv);
    }

    #[test]
    fn encode_aabb_round_trip() {
        let d0 = decode_packet(AABB);
        if let Decoded::Aabb(a) = d0 {
            let (hex, _) = encode_packet(&EncodeInput::Aabb(AabbEncodeInput {
                body_hex: a.body,
                direction: None,
            }))
            .unwrap();
            assert_eq!(hex, AABB);
        } else {
            panic!("expected aabb");
        }
    }
}
