//! Length-prefixed frames: 4-byte big-endian length + payload (ThinQ1).

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    NegativeLength,
    PayloadExceeded,
    Truncated,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeLength => f.write_str("Payload length cannot be negative"),
            Self::PayloadExceeded => f.write_str("Payload length exceeded"),
            Self::Truncated => f.write_str("Truncated length-prefixed frame"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Build a length-prefixed frame from payload bytes.
pub fn make(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + input.len());
    out.extend_from_slice(&(input.len() as i32).to_be_bytes());
    out.extend_from_slice(input);
    out
}

/// Build a length-prefixed frame from a UTF-8 string.
pub fn make_str(input: &str) -> Vec<u8> {
    make(input.as_bytes())
}

/// Incremental splitter for length-prefixed frames.
pub struct Splitter {
    accum: Vec<u8>,
    failed: bool,
    max_payload_length: usize,
}

impl Default for Splitter {
    fn default() -> Self {
        Self::new(65536)
    }
}

impl Splitter {
    pub fn new(max_payload_length: usize) -> Self {
        Self {
            accum: Vec::new(),
            failed: false,
            max_payload_length,
        }
    }

    /// Feed bytes; returns complete payloads delivered so far (may be empty).
    pub fn feed(&mut self, buf: &[u8]) -> Result<Vec<Vec<u8>>, FrameError> {
        if self.failed {
            return Ok(vec![]);
        }
        self.accum.extend_from_slice(buf);
        let mut out = Vec::new();

        while let Some(&head) = self.accum.first_chunk::<4>() {
            let payload_len = i32::from_be_bytes(head);
            if payload_len < 0 {
                self.failed = true;
                self.accum.clear();
                return Err(FrameError::NegativeLength);
            }
            let payload_len = payload_len as usize;
            if payload_len > self.max_payload_length {
                self.failed = true;
                self.accum.clear();
                return Err(FrameError::PayloadExceeded);
            }
            let Some(payload) = self.accum.get(4..4 + payload_len) else {
                break;
            };
            out.push(payload.to_vec());
            self.accum.drain(..4 + payload_len);
        }
        Ok(out)
    }

    /// Signal end of stream; errors if residual bytes remain.
    pub fn end(&mut self) -> Result<(), FrameError> {
        if !self.failed && !self.accum.is_empty() {
            self.failed = true;
            self.accum.clear();
            return Err(FrameError::Truncated);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_splitter_round_trip() {
        let mut split = Splitter::default();
        let mut out = split.feed(&make(b"hello")).unwrap();
        out.extend(split.feed(&make_str("world")).unwrap());
        assert_eq!(
            out.iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect::<Vec<_>>(),
            vec!["hello", "world"]
        );
    }

    #[test]
    fn splitter_chunked_deliveries() {
        let framed = [make(b"abc"), make(b"defg")].concat();
        let mut split = Splitter::default();
        let mut out = Vec::new();
        for i in 0..framed.len() {
            out.extend(split.feed(&framed[i..i + 1]).unwrap());
        }
        assert_eq!(
            out.iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect::<Vec<_>>(),
            vec!["abc", "defg"]
        );
    }

    #[test]
    fn splitter_two_frames_single_chunk() {
        let framed = [make(b"one"), make(b"two")].concat();
        let mut split = Splitter::default();
        let out = split.feed(&framed).unwrap();
        assert_eq!(
            out.iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect::<Vec<_>>(),
            vec!["one", "two"]
        );
    }

    #[test]
    fn real_world_thinq1_provisioning_frame() {
        let json = concat!(
            r#"{"Header":{"x-lgedm-deviceId":"48552db0-1ab4-11e9-b4fb-7c1c4ec8cc53"},"#,
            r#""Body":{"CmdWId":"e5d59e90-99a2-11f0-8ef9-7c1c4ec8cc53","Cmd":"DevInfo","Format":"B64","#,
            r#""Data":"UnVsZVZlcj0xLjMsRndWZXI9UUNfTW9kZW1fMS4yLjgwLHJlZ0ZhaWw9Tg=="}}"#
        );
        assert_eq!(json.len(), 0xe4);
        let mut framed = vec![0x00, 0x00, 0x00, 0xe4];
        framed.extend_from_slice(json.as_bytes());
        let mut split = Splitter::default();
        let out = split.feed(&framed).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(String::from_utf8_lossy(&out[0]), json);
    }

    #[test]
    fn enforces_max_payload_length() {
        let framed = make(&[0u8; 100]);
        let mut split = Splitter::new(50);
        assert_eq!(
            split.feed(&framed).unwrap_err(),
            FrameError::PayloadExceeded
        );
    }

    #[test]
    fn rejects_negative_lengths() {
        let mut split = Splitter::default();
        assert_eq!(
            split.feed(&[0xff, 0xff, 0xff, 0xff]).unwrap_err(),
            FrameError::NegativeLength
        );
        // After failure, further data is ignored
        assert!(
            split
                .feed(&make(b"ignored after terminal parse failure"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn reports_truncated_at_end() {
        let mut split = Splitter::default();
        split.feed(&[0x00, 0x00, 0x00, 0x05, 0x01, 0x02]).unwrap();
        assert_eq!(split.end().unwrap_err(), FrameError::Truncated);
    }
}
