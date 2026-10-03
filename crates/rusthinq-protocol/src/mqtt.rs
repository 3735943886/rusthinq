//! Bounded MQTT 3.1.1 device codec. Clean, local sessions; QoS 0/1/2 ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Malformed,
    Exceeded,
    Unsupported,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect {
        client: String,
        keep_alive: u16,
        will: Option<Will>,
    },
    Subscribe {
        id: u16,
        filters: Vec<String>,
    },
    Unsubscribe {
        id: u16,
        filters: Vec<String>,
    },
    Publish {
        topic: String,
        payload: Vec<u8>,
        id: Option<u16>,
        duplicate: bool,
        qos: u8,
    },
    PubRel {
        id: u16,
    },
    Ping,
    Disconnect,
}
/// Return complete packet length once its bounded fixed header is available.
pub fn length(bytes: &[u8], maximum: usize) -> Result<Option<usize>, Error> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut length = 0usize;
    for index in 0..4 {
        let Some(&byte) = bytes.get(index + 1) else {
            return Ok(None);
        };
        length |= ((byte & 127) as usize) << (7 * index);
        if byte & 128 == 0 {
            if index > 0 && byte == 0 {
                return Err(Error::Malformed);
            }
            let total = length + index + 2;
            if total > maximum {
                return Err(Error::Exceeded);
            }
            return Ok(Some(total));
        }
    }
    Err(Error::Malformed)
}
struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let result = self.0.get(..n).ok_or(Error::Malformed)?;
        self.0 = &self.0[n..];
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn number(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }
    fn string(&mut self) -> Result<String, Error> {
        let n = self.number()? as usize;
        if n > 1024 {
            return Err(Error::Exceeded);
        }
        let text = std::str::from_utf8(self.take(n)?).map_err(|_| Error::Malformed)?;
        if text
            .chars()
            .any(|c| c == '\0' || c.is_control() || matches!(c as u32, 0xfffe | 0xffff))
        {
            return Err(Error::Malformed);
        }
        Ok(text.into())
    }
    fn id(&mut self) -> Result<u16, Error> {
        let id = self.number()?;
        if id == 0 {
            Err(Error::Malformed)
        } else {
            Ok(id)
        }
    }
}
pub fn decode(bytes: &[u8], maximum: usize) -> Result<Packet, Error> {
    if length(bytes, maximum)? != Some(bytes.len()) {
        return Err(Error::Malformed);
    }
    let header = bytes[1..]
        .iter()
        .position(|byte| byte & 128 == 0)
        .ok_or(Error::Malformed)?
        + 2;
    let mut body = Cursor(&bytes[header..]);
    let flags = bytes[0] & 15;
    let packet = match bytes[0] >> 4 {
        1 if flags == 0 => {
            // As 0.1's broker: protocol name/level, the clean-session and reserved flags and
            // credentials are not checked, so MQTT 3.1 (`MQIsdp`/3) and appliances asking for
            // a persistent session still connect. Sessions are always clean; the CONNACK
            // reports no session present. A will that does not parse is dropped, not fatal.
            body.string()?;
            body.byte()?;
            let flags = body.byte()?;
            let keep_alive = body.number()?;
            let client = body.string()?;
            let will_qos = (flags >> 3) & 3;
            let will = if flags & 4 != 0 {
                (|| {
                    let topic = body.string().ok()?;
                    let size = body.number().ok()? as usize;
                    let payload = body.take(size).ok()?.to_vec();
                    (!topic.is_empty() && !topic.contains(['+', '#']) && will_qos != 3).then_some(
                        Will {
                            topic,
                            payload,
                            qos: will_qos,
                            retain: flags & 0x20 != 0,
                        },
                    )
                })()
            } else {
                None
            };
            // Credentials and anything after them are ignored, as 0.1 did.
            body.0 = &[];
            Packet::Connect {
                client,
                keep_alive,
                will,
            }
        }
        3 => {
            let qos = (flags >> 1) & 3;
            if qos == 3 || flags & 1 != 0 {
                return Err(Error::Unsupported);
            }
            if qos == 0 && flags & 8 != 0 {
                return Err(Error::Malformed);
            }
            let topic = body.string()?;
            if topic.is_empty() || topic.contains(['+', '#']) {
                return Err(Error::Malformed);
            }
            let id = if qos > 0 { Some(body.id()?) } else { None };
            let payload = body.0.to_vec();
            body.0 = &[];
            Packet::Publish {
                topic,
                payload,
                id,
                duplicate: flags & 8 != 0,
                qos,
            }
        }
        kind @ (8 | 10) if flags == 2 => {
            let id = body.id()?;
            let mut filters = Vec::new();
            while !body.0.is_empty() {
                if filters.len() == 16 {
                    return Err(Error::Exceeded);
                }
                let filter = body.string()?;
                if !valid_filter(&filter) {
                    return Err(Error::Malformed);
                }
                if kind == 8 && body.byte()? > 2 {
                    return Err(Error::Malformed);
                }
                filters.push(filter);
            }
            if filters.is_empty() {
                return Err(Error::Malformed);
            }
            if kind == 8 {
                Packet::Subscribe { id, filters }
            } else {
                Packet::Unsubscribe { id, filters }
            }
        }
        6 if flags == 2 => Packet::PubRel { id: body.id()? },
        12 if flags == 0 => Packet::Ping,
        14 if flags == 0 => Packet::Disconnect,
        _ => return Err(Error::Unsupported),
    };
    if !body.0.is_empty() {
        return Err(Error::Malformed);
    }
    Ok(packet)
}
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }
    let levels: Vec<_> = filter.split('/').collect();
    levels.iter().enumerate().all(|(i, level)| {
        (!level.contains('#') || *level == "#" && i + 1 == levels.len())
            && (!level.contains('+') || *level == "+")
    })
}
pub fn matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }
    let mut topics = topic.split('/');
    for level in filter.split('/') {
        if level == "#" {
            return true;
        }
        let Some(topic) = topics.next() else {
            return false;
        };
        if level != "+" && level != topic {
            return false;
        }
    }
    topics.next().is_none()
}
pub fn frame(first: u8, body: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    if body.len() > 268435455 || body.len().saturating_add(5) > maximum {
        return Err(Error::Exceeded);
    }
    let mut result = vec![first];
    let mut length = body.len();
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 128;
        }
        result.push(byte);
        if length == 0 {
            break;
        }
    }
    result.extend_from_slice(body);
    Ok(result)
}
pub fn publish(topic: &str, payload: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    if topic.is_empty() || topic.len() > 1024 || topic.contains(['+', '#', '\0']) {
        return Err(Error::Malformed);
    }
    let mut body = (topic.len() as u16).to_be_bytes().to_vec();
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(payload);
    frame(0x30, &body, maximum)
}
