//! AABB framing helpers (`AA <len> <inner> <csum> BB`) used by the script engine.

/// Wrap an AABB inner body as `AA <len> <inner> <csum> BB`.
pub fn wrap_aabb(inner: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(inner.len() + 4);
    packet.push(0xaa);
    packet.push((inner.len() + 4) as u8);
    packet.extend_from_slice(inner);
    let sum: u32 = packet.iter().map(|&b| u32::from(b)).sum();
    packet.push(((sum & 0xff) as u8) ^ 0x55);
    packet.push(0xbb);
    packet
}

/// Strip `AA … BB` framing. Checksum is not validated (devices send it; tests often omit it).
pub fn unwrap_aabb(buf: &[u8]) -> Option<Vec<u8>> {
    match buf {
        [0xaa, _, inner @ .., _, 0xbb] => Some(inner.to_vec()),
        _ => None,
    }
}

// Frame types the cloud does not ack: the appliance's own acks, the c3 heartbeat, and the
// eb/ec status records outside an envelope (as checked against the real cloud upstream).
const UNACKED_TYPES: [u8; 4] = [0x00, 0xc3, 0xeb, 0xec];
const FRAME_TYPE_ENVELOPE: u8 = 0x0a;
const ENVELOPE_SEQ_OFFSET: usize = 5;
// The envelope header, up to and including the 16-bit inner length; the cloud ignores
// anything shorter.
const ENVELOPE_MIN_LEN: usize = 13;
// `AA FF` marks the long framing, whose 16-bit checksum leaves one byte on the end of the
// inner body `unwrap_aabb` returns.
const LONG_FRAME: u8 = 0xff;
// `f0 00 <type> 04 [<seq16>]`, sent under the CLIP `ack` command. `00` in place of `04`, or
// `packet` in place of `ack`, stops the retransmits but leaves the content sync re-running.
const CLOUD_ACK: [u8; 2] = [0xf0, 0x00];
const CLOUD_ACK_STATUS: u8 = 0x04;

/// The framed ack the ThinQ cloud would send for `frame`, an AABB frame received from the
/// appliance — or `None` when the cloud sends none (not AABB, an unacked type, or an
/// envelope too short to carry its sequence number). Envelopes (`<class> 0a ...`) are
/// acked by sequence number whatever their delivery class; other frames by type.
/// Port of upstream rethink's `AABBDevice.ack` (anszom/rethink@09f2530).
pub fn cloud_ack(frame: &[u8]) -> Option<Vec<u8>> {
    let long_frame = frame.get(1) == Some(&LONG_FRAME);
    let inner = unwrap_aabb(frame)?;
    let ty = *inner.get(1)?;
    if UNACKED_TYPES.contains(&ty) {
        return None;
    }
    let mut ack = CLOUD_ACK.to_vec();
    ack.extend([ty, CLOUD_ACK_STATUS]);
    if ty == FRAME_TYPE_ENVELOPE {
        if inner.len() - usize::from(long_frame) < ENVELOPE_MIN_LEN {
            return None;
        }
        ack.extend_from_slice(inner.get(ENVELOPE_SEQ_OFFSET..ENVELOPE_SEQ_OFFSET + 2)?);
    }
    Some(wrap_aabb(&ack))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        rusthinq_util::hex::decode(s).unwrap()
    }

    fn ack_hex(frame: &str) -> Option<String> {
        cloud_ack(&hex(frame)).map(|a| rusthinq_core::hex_encode(&a).to_ascii_uppercase())
    }

    // The vectors below are upstream's (tests/cloud/devices/aabb_device.test.ts): frames
    // captured from or injected into bridged appliances, and the real cloud's acks to them.

    #[test]
    fn a_non_envelope_frame_is_acked_by_type_as_the_cloud_does() {
        let frame = rusthinq_core::hex_encode(&wrap_aabb(&[0x30, 0x4d, 0x01]));
        assert_eq!(ack_hex(&frame).as_deref(), Some("AA08F0004D04A6BB"));
    }

    #[test]
    fn an_envelope_is_acked_by_its_sequence_number_whatever_its_class() {
        assert_eq!(
            ack_hex("aaff310a00230053ed000101030011100b0306100000008a00000000000000059317bb"),
            Some(rusthinq_core::hex_encode(&wrap_aabb(&hex("f0000a0453ed"))).to_ascii_uppercase())
        );
        assert_eq!(
            ack_hex("aaff200a00a2001002000100ec00900aff000e00000000bb"),
            Some(rusthinq_core::hex_encode(&wrap_aabb(&hex("f0000a041002"))).to_ascii_uppercase())
        );
    }

    #[test]
    fn a_truncated_envelope_is_not_acked() {
        assert_eq!(ack_hex("AA08200A01020304BB"), None);
    }

    #[test]
    fn an_envelope_needs_its_full_13_byte_header_in_either_framing() {
        assert_eq!(
            ack_hex("aaff200a00120010130001007f0000e91dbb").as_deref(),
            Some("AA0AF0000A04101380BB")
        );
        assert_eq!(ack_hex("aaff200a00110010120001007f00decabb"), None);
        assert_eq!(
            ack_hex("aa11200a00110010b00001007f000063bb").as_deref(),
            Some("AA0AF0000A0410B027BB")
        );
        assert_eq!(ack_hex("aa10200a00100010c00001007f0011bb"), None);
    }

    #[test]
    fn heartbeats_other_than_c3_are_acked_by_type() {
        assert_eq!(
            ack_hex("aa0720d801ffbb").as_deref(),
            Some("AA08F000D8042BBB")
        );
        assert_eq!(
            ack_hex("aa0720720111bb").as_deref(),
            Some("AA08F00072044DBB")
        );
        assert_eq!(
            ack_hex("aa0720e901eebb").as_deref(),
            Some("AA08F000E904DABB")
        );
    }

    #[test]
    fn own_acks_c3_heartbeats_bare_status_records_and_non_aabb_are_not_acked() {
        for frame in [
            "AA084000430060BB",
            "aa0731c302f2bb",
            "aa0720eb01e8bb",
            "aa0720ec01ebbb",
            "aa5420ec002501000000000000000000000000000000000000000001006400000000000000000000000000002501033a033a0100030b04010000000000010002000001006400000400000000000000000000e0bb",
            "0102030405",
        ] {
            assert_eq!(ack_hex(frame), None, "{frame}");
        }
    }

    #[test]
    fn wrap_unwrap_roundtrip() {
        let inner = vec![0x20, 0xde, 0x01, 0x02];
        let pkt = wrap_aabb(&inner);
        assert_eq!(pkt[0], 0xaa);
        assert_eq!(*pkt.last().unwrap(), 0xbb);
        assert_eq!(pkt[1] as usize, inner.len() + 4);
        assert_eq!(unwrap_aabb(&pkt).as_deref(), Some(inner.as_slice()));
    }

    #[test]
    fn unwrap_rejects_short_or_unframed() {
        assert!(unwrap_aabb(&[]).is_none());
        assert!(unwrap_aabb(&[0xaa, 0x04, 0xbb]).is_none()); // len 3
        assert!(unwrap_aabb(&[0x00, 0x04, 0x00, 0xbb]).is_none());
        assert!(unwrap_aabb(&[0xaa, 0x04, 0x00, 0x00]).is_none());
    }

    #[test]
    fn laundry_status_query_frame_matches_known_hex() {
        let inner = [0xf0, 0xed, 0x11, 0x21, 0x01, 0x00, 0x00, 0x00, 0x18, 0x00];
        let pkt = wrap_aabb(&inner);
        assert_eq!(
            rusthinq_core::hex_encode(&pkt).to_ascii_uppercase(),
            "AA0EF0ED1121010000001800B5BB"
        );
    }
}
