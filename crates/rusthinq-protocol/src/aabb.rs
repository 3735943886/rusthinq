//! AABB short framing for scripts (`aabb_wrap`). Frame meaning, including which
//! frames the cloud acknowledges, belongs to the drivers.

/// Short framing only: a length of FF is reserved for long framing.
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
