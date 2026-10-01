//! Wire ACK compatibility ported from the reviewed 0.1 AABB helper.
//! Incoming checksum/length semantics are deliberately not newly enforced here:
//! long AA FF framing is preserved from captured cloud ACK vectors.

/// Short framing is used for all generated ACKs. FF is reserved for long framing.
pub fn wrap(inner: &[u8]) -> Option<Vec<u8>> {
    let length = inner.len().checked_add(4)?;
    if length >= 255 {
        return None;
    }
    let mut frame = Vec::with_capacity(length);
    frame.extend([0xaa, length as u8]);
    frame.extend_from_slice(inner);
    let sum = frame.iter().fold(0u8, |sum, byte| sum.wrapping_add(*byte));
    frame.extend([sum ^ 0x55, 0xbb]);
    Some(frame)
}

/// None means opaque/non-AABB, unacked type, or too-short envelope.
/// This is not a general AABB validator; scripts still receive the original bytes.
pub fn cloud_ack(frame: &[u8]) -> Option<Vec<u8>> {
    let [0xaa, length, inner @ .., _, 0xbb] = frame else {
        return None;
    };
    let ty = *inner.get(1)?;
    if [0x00, 0xc3, 0xeb, 0xec].contains(&ty) {
        return None;
    }
    let mut ack = vec![0xf0, 0x00, ty, 0x04];
    if ty == 0x0a {
        if inner.len().saturating_sub(usize::from(*length == 0xff)) < 13 {
            return None;
        }
        ack.extend_from_slice(inner.get(5..7)?);
    }
    wrap(&ack)
}
