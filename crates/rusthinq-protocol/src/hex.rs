//! Tiny hex encode/decode helpers (replaces the `hex` crate for our call sites).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError;

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid hex")
    }
}

impl std::error::Error for DecodeError {}

/// Lowercase hex encoding (like `hex::encode`).
pub fn encode(data: impl AsRef<[u8]>) -> String {
    encode_with(data.as_ref(), b"0123456789abcdef")
}

/// Uppercase hex encoding (like `hex::encode_upper`).
pub fn encode_upper(data: impl AsRef<[u8]>) -> String {
    encode_with(data.as_ref(), b"0123456789ABCDEF")
}

fn encode_with(data: &[u8], alphabet: &[u8; 16]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for &byte in data {
        out.push(alphabet[usize::from(byte >> 4)] as char);
        out.push(alphabet[usize::from(byte & 15)] as char);
    }
    out
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decode a hex string (even length, no `0x` prefix). Whitespace is not allowed
/// (callers that accept mixed input should strip first).
pub fn decode(s: impl AsRef<[u8]>) -> Result<Vec<u8>, DecodeError> {
    let (pairs, rest) = s.as_ref().as_chunks::<2>();
    if !rest.is_empty() {
        return Err(DecodeError);
    }
    pairs
        .iter()
        .map(|&[h, l]| Ok((nibble(h).ok_or(DecodeError)? << 4) | nibble(l).ok_or(DecodeError)?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_lower_and_upper() {
        let data = [0x00u8, 0x0f, 0xa0, 0xff, 0x12];
        assert_eq!(encode(data), "000fa0ff12");
        assert_eq!(encode_upper(data), "000FA0FF12");
        assert_eq!(decode("000fa0ff12").unwrap(), data);
        assert_eq!(decode("000FA0FF12").unwrap(), data);
    }

    #[test]
    fn rejects_odd_and_non_hex() {
        assert!(decode("abc").is_err());
        assert!(decode("zz").is_err());
    }
}
