//! Bounded, pure ClientHello routing parser. TLS cryptography remains in L3.
//! Parses handshake fragmentation across records, without consuming caller bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Exceeded,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hello {
    Incomplete,
    Ready(Option<String>),
}

pub fn hostname(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.parse::<std::net::IpAddr>().is_err()
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

pub fn parse(bytes: &[u8], max_bytes: usize) -> Result<Hello, Error> {
    if bytes.len() > max_bytes {
        return Err(Error::Exceeded);
    }
    let mut records = bytes;
    let mut handshake = Vec::new();
    loop {
        if records.len() < 5 {
            return Ok(Hello::Incomplete);
        }
        if records[0] != 22 || records[1] != 3 || records[2] > 3 {
            return Err(Error::Invalid);
        }
        let length = u16::from_be_bytes([records[3], records[4]]) as usize;
        if length == 0 || length > 16384 {
            return Err(Error::Invalid);
        }
        if length + 5 > max_bytes {
            return Err(Error::Exceeded);
        }
        if records.len() < length + 5 {
            return Ok(Hello::Incomplete);
        }
        handshake.extend_from_slice(&records[5..length + 5]);
        records = &records[length + 5..];
        if handshake.first() != Some(&1) {
            return Err(Error::Invalid);
        }
        if handshake.len() < 4 {
            continue;
        }
        let length = ((handshake[1] as usize) << 16)
            | ((handshake[2] as usize) << 8)
            | handshake[3] as usize;
        if length + 4 > max_bytes {
            return Err(Error::Exceeded);
        }
        if handshake.len() >= length + 4 {
            return body(&handshake[4..length + 4]).map(Hello::Ready);
        }
    }
}

struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let bytes = self.0.get(..length).ok_or(Error::Invalid)?;
        self.0 = &self.0[length..];
        Ok(bytes)
    }
    fn u8(&mut self) -> Result<usize, Error> {
        Ok(self.take(1)?[0] as usize)
    }
    fn u16(&mut self) -> Result<usize, Error> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("two bytes")) as usize)
    }
}
fn body(bytes: &[u8]) -> Result<Option<String>, Error> {
    let mut cursor = Cursor(bytes);
    cursor.take(34)?; // legacy_version and random
    let session = cursor.u8()?;
    if session > 32 {
        return Err(Error::Invalid);
    }
    cursor.take(session)?;
    let ciphers = cursor.u16()?;
    if ciphers == 0 || ciphers % 2 != 0 {
        return Err(Error::Invalid);
    }
    cursor.take(ciphers)?;
    let compression = cursor.u8()?;
    if compression == 0 {
        return Err(Error::Invalid);
    }
    cursor.take(compression)?;
    // TLS 1.0/1.2 may omit extensions entirely.
    if cursor.0.is_empty() {
        return Ok(None);
    }
    let extension_length = cursor.u16()?;
    let mut extensions = Cursor(cursor.take(extension_length)?);
    if !cursor.0.is_empty() {
        return Err(Error::Invalid);
    }
    let mut seen = std::collections::HashSet::new();
    let mut name = None;
    while !extensions.0.is_empty() {
        let kind = extensions.u16()?;
        if !seen.insert(kind) {
            return Err(Error::Invalid);
        }
        let length = extensions.u16()?;
        let bytes = extensions.take(length)?;
        if kind != 0 {
            continue;
        }
        let mut sni = Cursor(bytes);
        let length = sni.u16()?;
        let mut names = Cursor(sni.take(length)?);
        if length == 0 || !sni.0.is_empty() {
            return Err(Error::Invalid);
        }
        let mut types = std::collections::HashSet::new();
        while !names.0.is_empty() {
            let kind = names.u8()?;
            if !types.insert(kind) {
                return Err(Error::Invalid);
            }
            let length = names.u16()?;
            let bytes = names.take(length)?;
            if kind == 0 {
                let text = std::str::from_utf8(bytes).map_err(|_| Error::Invalid)?;
                if !hostname(text) {
                    return Err(Error::Invalid);
                }
                name = Some(text.to_ascii_lowercase());
            }
        }
        if name.is_none() {
            return Err(Error::Invalid);
        }
    }
    Ok(name)
}
