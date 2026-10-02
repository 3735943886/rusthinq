//! MTOSP framing: 0xAA | length BE16 | XML payload | CRC16 BE | 0xBB

use crate::crc16::crc16;

#[derive(Debug, PartialEq, Eq)]
pub enum MtospError {
    InvalidHeader,
    Exceeded,
    InvalidTrailer,
    InvalidChecksum,
}

impl std::fmt::Display for MtospError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exceeded => f.write_str("MTOSP payload exceeded"),
            Self::InvalidHeader => f.write_str("invalid header byte"),
            Self::InvalidTrailer => f.write_str("invalid trailer byte"),
            Self::InvalidChecksum => f.write_str("invalid checksum"),
        }
    }
}

impl std::error::Error for MtospError {}

/// Format an XML payload as an MTOSP frame.
pub fn format(xml: &str) -> Result<Vec<u8>, MtospError> {
    if xml.len() > u16::MAX as usize {
        return Err(MtospError::Exceeded);
    }
    let payload = xml.as_bytes();
    let mut header = vec![
        0xaa,
        (payload.len() >> 8) as u8,
        (payload.len() & 0xff) as u8,
    ];
    let mut body = header.clone();
    body.extend_from_slice(payload);
    let crc = crc16(&body);
    header.extend_from_slice(payload);
    header.push((crc >> 8) as u8);
    header.push((crc & 0xff) as u8);
    header.push(0xbb);
    Ok(header)
}

/// Incremental byte-oriented MTOSP splitter.
pub struct Splitter {
    state: u8,
    prev: u8,
    total: i32,
    buf: Vec<u8>,
    /// Set on the first error and never cleared — mirrors
    /// the length-prefixed splitter's `failed` flag. Without this, a bad
    /// byte returned `Err` but left `state`/`buf` exactly as they were, so a caller that
    /// kept feeding bytes after an error (to resync past one or two bad ones, say) would
    /// keep pushing onto `buf` forever with nothing ever clearing it.
    failed: bool,
}

impl Default for Splitter {
    fn default() -> Self {
        Self::new()
    }
}

impl Splitter {
    pub fn new() -> Self {
        Self {
            state: 0,
            prev: 0,
            total: 0,
            buf: Vec::new(),
            failed: false,
        }
    }

    /// Feed one byte. On complete frame, returns Some(xml_payload). Once an error has
    /// occurred, every further call is a no-op returning `Ok(None)` — this splitter
    /// cannot resync on its own, so nothing accumulates in `buf` past that point.
    pub fn feed(&mut self, byte: u8) -> Result<Option<String>, MtospError> {
        if self.failed {
            return Ok(None);
        }
        self.buf.push(byte);
        let mut result = None;

        match self.state {
            0 => {
                if byte != 0xaa {
                    self.failed = true;
                    self.buf.clear();
                    return Err(MtospError::InvalidHeader);
                }
                self.state = 1;
            }
            1 => {
                self.state = 2;
            }
            2 => {
                self.total = i32::from(byte) | (i32::from(self.prev) << 8);
                self.state = 3;
            }
            3 => {
                if self.total > 0 {
                    self.total -= 1;
                } else {
                    self.state = 4;
                }
            }
            4 => {
                if crc16(&self.buf) != 0 {
                    self.failed = true;
                    self.buf.clear();
                    return Err(MtospError::InvalidChecksum);
                }
                self.state = 5;
            }
            5 => {
                if byte != 0xbb {
                    self.failed = true;
                    self.buf.clear();
                    return Err(MtospError::InvalidTrailer);
                }
                self.state = 0;
                let xml = String::from_utf8_lossy(
                    self.buf
                        .get(3..self.buf.len().saturating_sub(3))
                        .unwrap_or_default(),
                )
                .into_owned();
                self.buf.clear();
                result = Some(xml);
            }
            _ => {}
        }
        self.prev = byte;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_splitter_round_trip() {
        let xml = "<root><a>hello</a></root>";
        let framed = format(xml).unwrap();
        let mut split = Splitter::new();
        let mut received = Vec::new();
        for b in framed {
            if let Some(s) = split.feed(b).unwrap() {
                received.push(s);
            }
        }
        assert_eq!(received, vec![xml]);
    }

    #[test]
    fn multiple_frames_back_to_back() {
        let a = format("<a/>").unwrap();
        let b = format("<b>x</b>").unwrap();
        let stream = [a, b].concat();
        let mut split = Splitter::new();
        let mut received = Vec::new();
        for byte in stream {
            if let Some(s) = split.feed(byte).unwrap() {
                received.push(s);
            }
        }
        assert_eq!(received, vec!["<a/>", "<b>x</b>"]);
    }

    #[test]
    fn bad_header_byte() {
        let mut split = Splitter::new();
        assert_eq!(split.feed(0x00).unwrap_err(), MtospError::InvalidHeader);
    }

    /// #33: an error used to leave `buf` untouched and `failed` didn't exist at all, so
    /// a caller that kept feeding bytes after an error (attempting to resync past a bad
    /// byte or two, say) would keep pushing onto `buf` forever with nothing ever
    /// clearing it. Mirrors `length_prefixed_frame::Splitter`'s `failed` flag.
    #[test]
    fn error_latches_and_clears_the_buffer_instead_of_accumulating_forever() {
        let mut split = Splitter::new();
        assert_eq!(split.feed(0x00).unwrap_err(), MtospError::InvalidHeader);
        assert!(
            split.buf.is_empty(),
            "buf must be cleared once the splitter has failed"
        );

        // Keep feeding well past any real frame's size — none of it must accumulate,
        // and no further Err (or Some) should surface once latched.
        for _ in 0..10_000 {
            assert_eq!(split.feed(0xaa).unwrap(), None);
        }
        assert!(
            split.buf.is_empty(),
            "a failed splitter must never accumulate bytes fed to it afterward"
        );
    }

    #[test]
    fn bad_trailer_byte() {
        let mut framed = format("<x/>").unwrap();
        let last = framed.len() - 1;
        framed[last] = 0xcc;
        let mut split = Splitter::new();
        let mut err = None;
        for byte in framed {
            match split.feed(byte) {
                Ok(_) => {}
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert_eq!(err, Some(MtospError::InvalidTrailer));
    }

    #[test]
    fn real_captured_wtdn3_deviceinfo() {
        let xml = concat!(
            r#"<mTosp><response type="deviceinfo">"#,
            r#"<protocolVer>2.0</protocolVer>"#,
            r#"<mac>7c:1c:4e:c8:cc:53</mac>"#,
            r#"<uuid>48552db0-1ab4-11e9-b4fb-7c1c4ec8cc53</uuid>"#,
            r#"<deviceType>201</deviceType>"#,
            r#"<modelName>WTDN3</modelName>"#,
            r#"<softwareVer>1.3</softwareVer>"#,
            r#"<countryCode>WW</countryCode>"#,
            r#"<remainingTime>388</remainingTime>"#,
            r#"<errorcodeDisplay>0</errorcodeDisplay>"#,
            r#"<modemVer>QC_Modem_1.2.80</modemVer>"#,
            r#"<demandType>MODEM_3k_SoC</demandType>"#,
            r#"</response></mTosp>"#
        );
        assert_eq!(xml.len(), 0x01a5);

        let mut framed = vec![0xaa, 0x01, 0xa5];
        framed.extend_from_slice(xml.as_bytes());
        framed.extend_from_slice(&[0x73, 0xda, 0xbb]);

        let mut split = Splitter::new();
        let mut received = Vec::new();
        for byte in framed {
            if let Some(s) = split.feed(byte).unwrap() {
                received.push(s);
            }
        }
        assert_eq!(received, vec![xml]);
    }

    #[test]
    fn bad_checksum() {
        let mut framed = format("<x/>").unwrap();
        framed[3] ^= 0x01;
        let mut split = Splitter::new();
        let mut err = None;
        for byte in framed {
            match split.feed(byte) {
                Ok(_) => {}
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert_eq!(err, Some(MtospError::InvalidChecksum));
    }
}
