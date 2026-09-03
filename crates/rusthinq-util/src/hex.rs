//! Tiny hex encode/decode helpers (replaces the `hex` crate for our call sites).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError;

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid hex")
    }
}

impl std::error::Error for DecodeError {}

const HEX_LO: &[u8; 16] = b"0123456789abcdef";
const HEX_UP: &[u8; 16] = b"0123456789ABCDEF";

/// Lowercase hex encoding (like `hex::encode`).
pub fn encode(data: impl AsRef<[u8]>) -> String {
    encode_with(data.as_ref(), HEX_LO)
}

/// Uppercase hex encoding (like `hex::encode_upper`).
pub fn encode_upper(data: impl AsRef<[u8]>) -> String {
    encode_with(data.as_ref(), HEX_UP)
}

fn encode_with(data: &[u8], alphabet: &[u8; 16]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for &b in data {
        out.push(alphabet[(b >> 4) as usize] as char);
        out.push(alphabet[(b & 0x0f) as usize] as char);
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
    let bytes = s.as_ref();
    if bytes.len() % 2 != 0 {
        return Err(DecodeError);
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = nibble(bytes[i]).ok_or(DecodeError)?;
        let lo = nibble(bytes[i + 1]).ok_or(DecodeError)?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
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
