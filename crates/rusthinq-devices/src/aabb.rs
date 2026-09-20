//! AABB framing helpers (`AA <len> <inner> <csum> BB`) used by the script engine.

/// Wrap an AABB inner body as `AA <len> <inner> <csum> BB`.
pub fn wrap_aabb(inner: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(inner.len() + 4);
    packet.push(0xaa);
    packet.push((inner.len() + 4) as u8);
    packet.extend_from_slice(inner);
    packet.push(0x00);
    packet.push(0x00);
    let sum: u32 = packet.iter().map(|&b| u32::from(b)).sum();
    let last = packet.len() - 2;
    packet[last] = ((sum & 0xff) as u8) ^ 0x55;
    packet[last + 1] = 0xbb;
    packet
}

/// Strip `AA … BB` framing. Checksum is not validated (devices send it; tests often omit it).
pub fn unwrap_aabb(buf: &[u8]) -> Option<Vec<u8>> {
    if buf.len() >= 4 && buf[0] == 0xaa && buf[buf.len() - 1] == 0xbb {
        Some(buf[2..buf.len() - 2].to_vec())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
