//! ThinQ1 length-prefixed JSON connection over a TLS/TCP stream.

use rusthinq_util::length_prefixed_frame::{self, FrameError};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::warn;

const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

pub struct T1ConnectionEvents {
    pub on_init: Arc<dyn Fn(String) + Send + Sync>,
    pub on_status: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    /// A command-ack envelope (a `Body` carrying `ReturnCode`) — lets a driver react
    /// to its own command's acknowledgement instead of waiting for the next status
    /// poll. Distinct from `on_status`: this fires on the JSON `Body`, not the binary
    /// status payload the appliance separately reports on a timer.
    pub on_response: Arc<dyn Fn(serde_json::Value) + Send + Sync>,
    pub on_close: Arc<dyn Fn() + Send + Sync>,
}

pub async fn run_connection_with_acks<S>(
    stream: S,
    events: T1ConnectionEvents,
    mut outbound: mpsc::UnboundedReceiver<serde_json::Value>,
    ack_tx: mpsc::UnboundedSender<serde_json::Value>,
) where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);

    let write_task = tokio::spawn(async move {
        while let Some(json) = outbound.recv().await {
            let s = serde_json::to_string(&json).unwrap_or_default();
            rusthinq_core::logging::log("outgoing", &[&s]);
            let frame = length_prefixed_frame::make(s.as_bytes());
            if writer.write_all(&frame).await.is_err() {
                break;
            }
        }
    });

    let mut splitter = length_prefixed_frame::Splitter::new(1_000_000);
    let mut buf = [0u8; 8192];
    let mut device_id: Option<String> = None;
    let mut idle = tokio::time::interval(IDLE_TIMEOUT);
    idle.reset();

    'session: loop {
        tokio::select! {
            _ = idle.tick() => break,
            n = reader.read(&mut buf) => {
                match n {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        idle.reset();
                        match splitter.feed(buf.get(..n).unwrap_or_default()) {
                            Ok(frames) => {
                                for payload in frames {
                                    if !process_one(&payload, &mut device_id, &events, &ack_tx) {
                                        break 'session;
                                    }
                                }
                            }
                            Err(FrameError::PayloadExceeded) => break,
                            Err(e) => {
                                warn!("thinq1 split: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    // A frame the device started (a 4-byte length header, maybe some payload) but
    // never finished before the socket closed — surfaces a truncated connection as
    // such instead of it looking identical to a clean disconnect between frames.
    if let Err(e) = splitter.end() {
        warn!("thinq1 split: {e}");
    }
    (events.on_close)();
    write_task.abort();
}

/// Returns `false` if the frame is malformed enough that the connection should be
/// dropped rather than kept processing under an unproven state — matches rethink's
/// `Connection.fail()` path (JSON.parse failure or a missing device id now destroys
/// the socket there, instead of logging and carrying on).
fn process_one(
    payload: &[u8],
    device_id: &mut Option<String>,
    events: &T1ConnectionEvents,
    ack_tx: &mpsc::UnboundedSender<serde_json::Value>,
) -> bool {
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(payload) else {
        warn!("thinq1: malformed JSON frame, dropping connection");
        return false;
    };
    rusthinq_core::logging::log("incoming", &[&String::from_utf8_lossy(payload)]);

    // Device id from header — required on every frame: a real ThinQ1 appliance
    // always includes it, so a frame without one is either framing desync or an
    // unexpected peer, and it's safer to drop the connection than keep processing
    // (acking, forwarding status) under no proven identity.
    let Some(id) = json
        .pointer("/Header/x-lgedm-deviceId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    else {
        warn!("thinq1: frame missing a valid x-lgedm-deviceId, dropping connection");
        return false;
    };
    if device_id.is_none() {
        *device_id = Some(id.to_string());
        (events.on_init)(id.to_string());
    }

    // A command-ack envelope — surfaced before the DevInfo/Mon auto-ack below, which
    // only applies to a poll *request*, not this appliance's answer to rusthinq's own
    // command.
    if let Some(body) = json.get("Body")
        && body.get("ReturnCode").is_some()
        && body.is_object()
    {
        (events.on_response)(body.clone());
    }

    // ACK empty responses when needed
    if let Some(cmd) = json.pointer("/Body/Cmd").and_then(|v| v.as_str())
        && (cmd == "DevInfo" || cmd == "Mon")
    {
        let ack = serde_json::json!({
            "Header": json.get("Header").cloned().unwrap_or(serde_json::json!({})),
            "Body": { "Return": "OK" }
        });
        let _ = ack_tx.send(ack);
    }

    // Status payloads as raw bytes for handler
    (events.on_status)(payload.to_vec());
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusthinq_util::sync::Mutex;

    fn events_capturing(
        responses: Arc<Mutex<Vec<serde_json::Value>>>,
        statuses: Arc<Mutex<Vec<Vec<u8>>>>,
    ) -> T1ConnectionEvents {
        T1ConnectionEvents {
            on_init: Arc::new(|_id| {}),
            on_status: Arc::new(move |buf| statuses.lock().push(buf)),
            on_response: Arc::new(move |body| responses.lock().push(body)),
            on_close: Arc::new(|| {}),
        }
    }

    fn frame(body: serde_json::Value) -> Vec<u8> {
        serde_json::json!({
            "Header": { "x-lgedm-deviceId": "dev-1" },
            "Body": body,
        })
        .to_string()
        .into_bytes()
    }

    /// The bug this event exists to fix: a driver waiting on its own command's
    /// acknowledgement had no way to see it except polling the next status report.
    #[test]
    fn a_returncode_body_fires_on_response() {
        let responses = Arc::new(Mutex::new(Vec::new()));
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let events = events_capturing(responses.clone(), statuses.clone());
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
        let mut device_id = None;

        let payload = frame(serde_json::json!({ "ReturnCode": "0000", "CmdWId": "n-1" }));
        process_one(&payload, &mut device_id, &events, &ack_tx);

        let got = responses.lock().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["ReturnCode"], "0000");
        assert_eq!(got[0]["CmdWId"], "n-1");
        // Every frame still reaches on_status too, ReturnCode or not.
        assert_eq!(statuses.lock().len(), 1);
        // A command ack is not a poll request — no auto-ack should be sent for it.
        assert!(ack_rx.try_recv().is_err());
    }

    #[test]
    fn a_status_report_without_returncode_does_not_fire_on_response() {
        let responses = Arc::new(Mutex::new(Vec::new()));
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let events = events_capturing(responses.clone(), statuses.clone());
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
        let mut device_id = None;

        let payload = frame(serde_json::json!({ "Cmd": "Mon", "Format": "B64", "Data": "AQI=" }));
        process_one(&payload, &mut device_id, &events, &ack_tx);

        assert!(responses.lock().is_empty());
        assert_eq!(statuses.lock().len(), 1);
        // Mon *is* a poll request, so it still gets auto-acked.
        assert!(ack_rx.try_recv().is_ok());
    }

    /// The regression this test exists for: a frame with no (or an empty)
    /// `x-lgedm-deviceId` used to be processed anyway — acked, forwarded to
    /// `on_status` — under an unproven identity. rethink's `Connection` now treats
    /// this as fatal and destroys the socket; matched here by telling the caller to
    /// drop the connection instead of continuing to process the frame.
    #[test]
    fn a_frame_without_a_valid_device_id_is_rejected() {
        let responses = Arc::new(Mutex::new(Vec::new()));
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let events = events_capturing(responses.clone(), statuses.clone());
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();

        for header in [
            serde_json::json!({}),
            serde_json::json!({ "x-lgedm-deviceId": "" }),
        ] {
            let mut device_id = None;
            let payload = serde_json::json!({
                "Header": header,
                "Body": { "Cmd": "Mon", "Format": "B64", "Data": "AQI=" },
            })
            .to_string()
            .into_bytes();

            let keep_going = process_one(&payload, &mut device_id, &events, &ack_tx);

            assert!(!keep_going);
            assert!(device_id.is_none());
            assert!(statuses.lock().is_empty());
            assert!(ack_rx.try_recv().is_err());
        }
    }
}
