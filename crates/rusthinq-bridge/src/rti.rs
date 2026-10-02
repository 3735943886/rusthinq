//! Caller-owned ThinQ1 RTI framed transport. No tasks, retries or implicit ACKs.
use rusthinq_protocol::thinq1;
use std::{io, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

/// Owns one stream. An interrupted operation permanently fences the stream:
/// retrying a partially read/written frame would corrupt protocol boundaries.
/// Drop the session to close its socket after any failure or cancellation.
pub struct Session<S> {
    stream: S,
    max_payload: usize,
    deadline: Duration,
    ready: bool,
}
impl<S: AsyncRead + AsyncWrite + Unpin> Session<S> {
    pub fn new(stream: S, max_payload: usize, deadline: Duration) -> io::Result<Self> {
        if max_payload == 0
            || max_payload > 1_000_000
            || deadline.is_zero()
            || deadline > Duration::from_secs(300)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid RTI limits",
            ));
        }
        Ok(Self {
            stream,
            max_payload,
            deadline,
            ready: true,
        })
    }
    fn begin(&mut self) -> io::Result<()> {
        if !self.ready {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "RTI session interrupted",
            ));
        }
        self.ready = false;
        Ok(())
    }
    /// Writes the original JSON bytes. Success means full frame write, not a
    /// device acknowledgement. Invalid local input leaves the session usable.
    pub async fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        if !self.ready {
            return self.begin();
        }
        let frame = thinq1::encode(payload, self.max_payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "RTI payload exceeded"))?;
        validate(payload)?;
        self.begin()?;
        timeout(self.deadline, self.stream.write_all(&frame))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RTI write timed out"))??;
        self.ready = true;
        Ok(())
    }
    /// Reads exactly one bounded JSON object, preserving all original bytes.
    /// EOF between frames returns None; partial header/payload EOF is an error.
    /// One deadline covers the entire frame, including a stalled payload.
    pub async fn receive(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.begin()?;
        let result = timeout(self.deadline, async {
            let mut header = [0; 4];
            if self.stream.read(&mut header[..1]).await? == 0 {
                return Ok(None);
            }
            self.stream.read_exact(&mut header[1..]).await?;
            let length = i32::from_be_bytes(header);
            if length < 0 || length as usize > self.max_payload {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid RTI frame length",
                ));
            }
            let mut payload = vec![0; length as usize];
            self.stream.read_exact(&mut payload).await?;
            validate(&payload)?;
            Ok(Some(payload))
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RTI read timed out"))??;
        self.ready = result.is_some();
        Ok(result)
    }
}
fn validate(payload: &[u8]) -> io::Result<()> {
    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(value) if value.is_object() => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RTI requires a JSON object",
        )),
    }
}
