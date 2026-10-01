//! ThinQ1 uses a signed, big-endian four-byte length followed by UTF-8 JSON.
//! Time is monotonic elapsed time supplied by the caller, never wall-clock time.
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NegativeLength,
    PayloadExceeded,
    Truncated,
    InvalidJson,
    MissingDeviceId,
    DeviceIdChanged,
    IdleTimeout,
    TimeWentBackwards,
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_chunk_never_enters_the_retained_buffer() {
        let mut model = Session::new(64, Duration::from_secs(90), Duration::ZERO);
        let mut bytes = vec![0; 1_000_000];
        bytes[..4].copy_from_slice(&100_000_i32.to_be_bytes());
        assert_eq!(
            model.input(Input::Bytes(&bytes), Duration::ZERO).error,
            Some(Error::PayloadExceeded)
        );
        assert!(model.buffer.is_empty());
        assert!(model.buffer.capacity() <= 64 + 4);
        assert_eq!(
            model.input(Input::Bytes(&bytes), Duration::ZERO).error,
            Some(Error::Closed)
        );
        assert!(model.buffer.is_empty());
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ThinQ1 protocol error: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Actions are ordered, including ACKs relative to protocol events.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Identified(String),
    Response(Value),
    Send(Vec<u8>),
    /// Original JSON bytes, including fields the core does not interpret.
    Data(Vec<u8>),
}

#[derive(Debug, PartialEq)]
pub struct Outcome {
    pub actions: Vec<Action>,
    /// A fatal error closes this model. Earlier actions in this input remain valid.
    pub error: Option<Error>,
    pub next_deadline: Option<Duration>,
}

pub enum Input<'a> {
    Bytes(&'a [u8]),
    Tick,
    End,
}

/// Encode with the same bound used for incoming frames; never truncate a length.
pub fn encode(payload: &[u8], max_payload: usize) -> Result<Vec<u8>, Error> {
    if payload.len() > max_payload || payload.len() > i32::MAX as usize {
        return Err(Error::PayloadExceeded);
    }
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub struct Session {
    max_payload: usize,
    idle_timeout: Duration,
    last_now: Duration,
    deadline: Duration,
    buffer: Vec<u8>,
    device_id: Option<String>,
    closed: bool,
}

impl Session {
    pub fn new(max_payload: usize, idle_timeout: Duration, now: Duration) -> Self {
        Self {
            max_payload: max_payload.min(i32::MAX as usize),
            idle_timeout,
            last_now: now,
            deadline: now.saturating_add(idle_timeout),
            buffer: Vec::new(),
            device_id: None,
            closed: false,
        }
    }

    pub fn device_id(&self) -> Option<&str> {
        self.device_id.as_deref()
    }

    pub fn input(&mut self, input: Input<'_>, now: Duration) -> Outcome {
        let mut actions = Vec::new();
        let result = if self.closed {
            Err(Error::Closed)
        } else if now < self.last_now {
            Err(Error::TimeWentBackwards)
        } else if now >= self.deadline {
            Err(Error::IdleTimeout)
        } else {
            self.last_now = now;
            match input {
                Input::Bytes(bytes) => {
                    if !bytes.is_empty() {
                        self.deadline = now.saturating_add(self.idle_timeout);
                    }
                    self.feed(bytes, &mut actions)
                }
                Input::Tick => Ok(()),
                Input::End => {
                    self.closed = true;
                    if self.buffer.is_empty() {
                        Ok(())
                    } else {
                        Err(Error::Truncated)
                    }
                }
            }
        };
        if result.is_err() {
            self.closed = true;
        }
        if self.closed {
            self.buffer.clear();
        }
        Outcome {
            actions,
            error: result.err(),
            next_deadline: (!self.closed).then_some(self.deadline),
        }
    }

    fn feed(&mut self, mut bytes: &[u8], actions: &mut Vec<Action>) -> Result<(), Error> {
        // Copy only the current header/payload, never the entire supplied chunk.
        while !bytes.is_empty() {
            let target = if self.buffer.len() < 4 {
                4
            } else {
                self.payload_length()? + 4
            };
            let count = (target - self.buffer.len()).min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.buffer.len() >= 4 {
                let length = self.payload_length()?;
                if self.buffer.len() == length + 4 {
                    let payload = self.buffer[4..].to_vec();
                    self.process(&payload, actions)?;
                    self.buffer.clear();
                }
            }
        }
        Ok(())
    }

    fn payload_length(&self) -> Result<usize, Error> {
        let length = i32::from_be_bytes(self.buffer[..4].try_into().expect("complete header"));
        if length < 0 {
            return Err(Error::NegativeLength);
        }
        let length = length as usize;
        if length > self.max_payload {
            return Err(Error::PayloadExceeded);
        }
        Ok(length)
    }

    fn process(&mut self, payload: &[u8], actions: &mut Vec<Action>) -> Result<(), Error> {
        let message: Value = serde_json::from_slice(payload).map_err(|_| Error::InvalidJson)?;
        let id = message
            .pointer("/Header/x-lgedm-deviceId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(Error::MissingDeviceId)?;
        if let Some(current) = &self.device_id
            && current != id
        {
            return Err(Error::DeviceIdChanged);
        }
        // Validate outbound bounds before mutating identity or producing actions.
        let body = message.get("Body");
        let response = body.filter(|body| body.is_object() && body.get("ReturnCode").is_some());
        let poll = matches!(
            message.pointer("/Body/Cmd").and_then(Value::as_str),
            Some("Mon" | "DevInfo")
        );
        let ack = if poll && response.is_none() {
            let ack = json!({"Header": message["Header"], "Body": {"Return": "OK"}});
            Some(encode(
                &serde_json::to_vec(&ack).map_err(|_| Error::InvalidJson)?,
                self.max_payload,
            )?)
        } else {
            None
        };
        if self.device_id.is_none() {
            self.device_id = Some(id.to_owned());
            actions.push(Action::Identified(id.to_owned()));
        }
        if let Some(body) = response {
            actions.push(Action::Response(body.clone()));
        }
        if let Some(ack) = ack {
            actions.push(Action::Send(ack));
        }
        actions.push(Action::Data(payload.to_vec()));
        Ok(())
    }
}
