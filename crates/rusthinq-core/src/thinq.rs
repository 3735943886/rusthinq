//! ThinQ device abstractions (platform-agnostic + mock devices for tests).

use crate::metadata::Metadata;
use rusthinq_util::sync::Mutex;
use std::sync::Arc;

type DataHandler = Box<dyn Fn(&[u8]) + Send + Sync>;
type ResponseHandler = Box<dyn Fn(&serde_json::Value) + Send + Sync>;

/// Outbound ThinQ2 message recorded by mocks.
#[derive(Debug, Clone)]
pub struct SentMessage {
    pub cmd: String,
    pub msg_type: i32,
    pub data: serde_json::Value,
}

/// Trait for a ThinQ2-connected appliance (send packets / receive data).
pub trait Thinq2Device: Send + Sync {
    fn id(&self) -> &str;
    fn meta(&self) -> &Metadata;
    fn send_packet(&self, buf: &[u8]);
    fn send(&self, cmd: &str, msg_type: i32, data: serde_json::Value);
    fn on_data(&self, handler: DataHandler);
    /// Inject a data packet as if received from the appliance (tests + broker).
    fn emit_data(&self, buf: &[u8]);
}

/// Trait for ThinQ1 devices (JSON control body; binary status reports).
pub trait Thinq1Device: Send + Sync {
    fn id(&self) -> &str;
    fn meta(&self) -> &Metadata;
    fn send(&self, body: serde_json::Value);
    /// Status reports are binary frames (e.g. 28-byte washer state).
    fn on_data(&self, handler: DataHandler);
    fn emit_data(&self, buf: &[u8]);
    /// A command-ack envelope (a `Body` carrying `ReturnCode`) — lets a driver react
    /// to its own command's acknowledgement instead of waiting for the next status
    /// poll (see `thinq1/connection.rs`'s `on_response` in rusthinq-cloud).
    fn on_response(&self, handler: ResponseHandler);
    /// Inject a response envelope as if received from the appliance (tests).
    fn emit_response(&self, body: &serde_json::Value);
}

/// Mock ThinQ2 device for unit tests.
pub struct MockThinq2Device {
    id: String,
    meta: Metadata,
    outbox: Mutex<Vec<Vec<u8>>>,
    sent: Mutex<Vec<SentMessage>>,
    handlers: Mutex<Vec<DataHandler>>,
}

impl MockThinq2Device {
    pub fn new(id: impl Into<String>, meta: Metadata) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            meta,
            outbox: Mutex::new(Vec::new()),
            sent: Mutex::new(Vec::new()),
            handlers: Mutex::new(Vec::new()),
        })
    }

    pub fn outbox(&self) -> Vec<Vec<u8>> {
        self.outbox.lock().clone()
    }

    pub fn sent(&self) -> Vec<SentMessage> {
        self.sent.lock().clone()
    }

    pub fn reset_recorder(&self) {
        self.outbox.lock().clear();
        self.sent.lock().clear();
    }
}

impl Thinq2Device for MockThinq2Device {
    fn id(&self) -> &str {
        &self.id
    }

    fn meta(&self) -> &Metadata {
        &self.meta
    }

    fn send_packet(&self, buf: &[u8]) {
        self.outbox.lock().push(buf.to_vec());
        // also notify any send-data listeners if needed
    }

    fn send(&self, cmd: &str, msg_type: i32, data: serde_json::Value) {
        self.sent.lock().push(SentMessage {
            cmd: cmd.into(),
            msg_type,
            data,
        });
    }

    fn on_data(&self, handler: DataHandler) {
        self.handlers.lock().push(handler);
    }

    fn emit_data(&self, buf: &[u8]) {
        let guard = self.handlers.lock();
        for h in guard.iter() {
            h(buf);
        }
    }
}

/// Mock ThinQ1 device for unit tests.
pub struct MockThinq1Device {
    id: String,
    meta: Metadata,
    sent: Mutex<Vec<serde_json::Value>>,
    handlers: Mutex<Vec<DataHandler>>,
    response_handlers: Mutex<Vec<ResponseHandler>>,
}

impl MockThinq1Device {
    pub fn new(id: impl Into<String>, meta: Metadata) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            meta,
            sent: Mutex::new(Vec::new()),
            handlers: Mutex::new(Vec::new()),
            response_handlers: Mutex::new(Vec::new()),
        })
    }

    pub fn sent(&self) -> Vec<serde_json::Value> {
        self.sent.lock().clone()
    }

    pub fn reset_recorder(&self) {
        self.sent.lock().clear();
    }
}

impl Thinq1Device for MockThinq1Device {
    fn id(&self) -> &str {
        &self.id
    }

    fn meta(&self) -> &Metadata {
        &self.meta
    }

    fn send(&self, body: serde_json::Value) {
        self.sent.lock().push(body);
    }

    fn on_data(&self, handler: DataHandler) {
        self.handlers.lock().push(handler);
    }

    fn emit_data(&self, buf: &[u8]) {
        let guard = self.handlers.lock();
        for h in guard.iter() {
            h(buf);
        }
    }

    fn on_response(&self, handler: ResponseHandler) {
        self.response_handlers.lock().push(handler);
    }

    fn emit_response(&self, body: &serde_json::Value) {
        for h in self.response_handlers.lock().iter() {
            h(body);
        }
    }
}

/// Hex helpers used by tests.
pub fn hex_encode(b: &[u8]) -> String {
    rusthinq_util::hex::encode_upper(b)
}

pub fn hex_decode(s: &str) -> Vec<u8> {
    let cleaned: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    rusthinq_util::hex::decode(&cleaned).unwrap_or_default()
}
