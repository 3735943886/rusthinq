//! Daemon log policy. Subscribers belong to executables, not library callers.
use crate::runtime::Event;
use rusthinq_lifecycle::Action;
use rusthinq_protocol::thinq2::Error as ThinQ2Error;
use rusthinq_server::{Delivery, Disconnect, DownlinkRefusal, Event as Transport, MqttDiagnostic};
use std::io;
use tracing_subscriber::EnvFilter;

const DEFAULT_FILTER: &str = "warn,rusthinq=info,rusthinq_app=info";

/// Default to application info; dependency diagnostics require an explicit filter.
/// Invalid RUST_LOG values fail startup instead of silently changing verbosity.
pub fn init() -> io::Result<()> {
    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => EnvFilter::try_new(value).map_err(io::Error::other)?,
        Err(std::env::VarError::NotPresent) => EnvFilter::new(DEFAULT_FILTER),
        Err(error) => return Err(io::Error::other(error)),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_ansi(false)
        .without_time()
        .try_init()
        .map_err(io::Error::other)
}

pub fn runtime_event(event: &Event) {
    match event {
        Event::ScriptExecuted {
            context,
            error: Some(error),
            ..
        }
        | Event::ScriptStopped {
            context,
            error: Some(error),
        } => {
            tracing::warn!(?context, %error, "script failed");
        }
        Event::GenerationRefillFailed { reason } | Event::CleanupFailed { reason } => {
            tracing::error!(%reason, "runtime persistence or cleanup failed");
        }
        Event::Rejected { device, reason } => {
            tracing::warn!(%device, %reason, "operation rejected")
        }
        Event::Lost { transport_events } => {
            tracing::warn!(transport_events, "transport events lost")
        }
        Event::ScriptDelivery {
            context,
            delivery: Delivery::Failed | Delivery::Unknown,
        } => {
            tracing::warn!(?context, ?event, "device delivery unsuccessful");
        }
        Event::Transport(Transport::Up(session)) => tracing::info!(?session, "device connected"),
        Event::Transport(Transport::Down(session, reason)) => {
            if matches!(reason, Disconnect::Eof | Disconnect::Closed) {
                tracing::info!(?session, ?reason, "device disconnected");
            } else {
                tracing::warn!(?session, ?reason, "device disconnected");
            }
        }
        Event::Transport(Transport::BridgeChanged(session, generation, connected)) => {
            tracing::info!(?session, generation, connected, "bridge connection changed");
        }
        Event::Lifecycle(Action::LedgerResult {
            revision,
            result: Err(error),
        }) => {
            tracing::error!(revision, %error, "device ledger persistence failed");
        }
        Event::Lifecycle(Action::ForgetFailed {
            id, step, reason, ..
        }) => {
            tracing::warn!(device = %id, ?step, %reason, "device removal failed");
        }
        Event::Lifecycle(
            Action::Online { id, .. } | Action::Offline { id, .. } | Action::Removed { id, .. },
        ) => {
            tracing::info!(device = %id, ?event, "device lifecycle changed");
        }
        // Full wire data and publications are available only with explicit trace logging.
        // CLIP payloads are JSON text; their Debug form would be a list of byte values.
        Event::Transport(Transport::PublishIgnored(session, reason, topic, payload)) => {
            tracing::trace!(
                ?session,
                ?reason,
                topic,
                payload = %String::from_utf8_lossy(payload),
                "ignored publish payload"
            );
        }
        Event::Transport(
            Transport::Sent(session, payload)
            | Transport::CloudBound(session, payload)
            | Transport::BridgedCloudBound(session, _, payload),
        ) => {
            tracing::trace!(
                ?session,
                direction = if matches!(event, Event::Transport(Transport::Sent(..))) {
                    "to device"
                } else {
                    "from device"
                },
                payload = %String::from_utf8_lossy(payload),
                "runtime payload"
            );
        }
        Event::Transport(
            Transport::Data(..)
            | Transport::Response(..)
            | Transport::Ready(..)
            | Transport::Will { .. },
        )
        | Event::Injected { .. }
        | Event::ScriptOutput { .. }
        | Event::Metadata(_) => {
            tracing::trace!(?event, "runtime payload");
        }
        Event::CloudChanged { device } => tracing::debug!(%device, "cloud state changed"),
        _ => tracing::debug!(?event, "runtime event"),
    }
}

pub fn mqtt_diagnostic(diagnostic: &MqttDiagnostic) {
    match diagnostic {
        MqttDiagnostic::Connected { .. } => tracing::info!(?diagnostic, "MQTT client connected"),
        MqttDiagnostic::Subscribed {
            generation,
            client,
            filters,
        } => tracing::info!(?filters, ?client, generation, "MQTT client subscribed"),
        // A bridge generation replaced mid-flight is routine; the rest lose a device write.
        MqttDiagnostic::DownlinkRefused {
            reason: DownlinkRefusal::Protocol(ThinQ2Error::StaleBridge),
            ..
        } => tracing::debug!(?diagnostic, "device write refused"),
        MqttDiagnostic::DownlinkRefused {
            generation,
            client,
            session,
            ready,
            reason,
            cmd,
            payload_len,
        } => tracing::warn!(
            ?reason,
            ?cmd,
            payload_len,
            ?client,
            generation,
            ready,
            ?session,
            "device write refused"
        ),
        // The reason leads: long lines get cut, and it is what the line exists to report.
        MqttDiagnostic::Closed {
            generation,
            client,
            session,
            ready,
            reason,
            rejected,
            ignored_publishes,
        } => {
            if matches!(reason, Disconnect::Eof | Disconnect::Closed) {
                tracing::info!(
                    ?reason,
                    ?client,
                    generation,
                    ready,
                    ignored_publishes,
                    ?session,
                    "MQTT client closed"
                );
            } else {
                tracing::warn!(
                    ?reason,
                    ?rejected,
                    ?client,
                    generation,
                    ready,
                    ignored_publishes,
                    ?session,
                    "MQTT client failed"
                );
            }
        }
        // Outside CLIP is ordinary firmware behavior; a broken CLIP message is worth a warning.
        MqttDiagnostic::PublishIgnored {
            generation,
            client,
            session,
            ready,
            reason,
            publish,
        } => {
            if *reason == ThinQ2Error::OutsideClip {
                tracing::info!(
                    ?reason,
                    ?publish,
                    ?client,
                    generation,
                    ready,
                    ?session,
                    "MQTT publish ignored"
                );
            } else {
                tracing::warn!(
                    ?reason,
                    ?publish,
                    ?client,
                    generation,
                    ready,
                    ?session,
                    "MQTT publish ignored"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn mqtt_failure_leads_with_its_reason_and_lg_steps_pass_the_default_filter() {
        let output = Buffer(Arc::new(Mutex::new(Vec::new())));
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(DEFAULT_FILTER))
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            mqtt_diagnostic(&MqttDiagnostic::Closed {
                generation: 1,
                client: Some("test-device".into()),
                session: None,
                ready: true,
                reason: Disconnect::Mqtt(rusthinq_server::MqttFailure::UnsubscribedTopic),
                rejected: None,
                ignored_publishes: 3,
            });
            mqtt_diagnostic(&MqttDiagnostic::PublishIgnored {
                generation: 1,
                client: Some("test-device".into()),
                session: None,
                ready: true,
                reason: ThinQ2Error::OutsideClip,
                publish: rusthinq_server::RejectedPublish {
                    topic: "express/monitoring".into(),
                    did: None,
                    cmd: None,
                    keys: Some(vec!["state".into()]),
                    payload_len: 5,
                },
            });
            tracing::info!(target: "rusthinq_bridge::cloud", step = "add_device", "LG API refused");
            tracing::debug!(target: "rusthinq_bridge::cloud", step = "route", "LG API response");
        });
        let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        let reason = text.find("UnsubscribedTopic").unwrap();
        assert!(reason < text.find("test-device").unwrap(), "{text}");
        assert!(text.contains("ignored_publishes=3"), "{text}");
        let ignored = text.find("MQTT publish ignored").unwrap();
        assert!(text[ignored..].contains("OutsideClip"), "{text}");
        assert!(text.contains("keys: Some([\"state\"])"), "{text}");
        assert!(text.contains("step=\"add_device\""), "{text}");
        assert!(!text.contains("step=\"route\""), "{text}");
    }

    #[test]
    fn subscriptions_and_refused_writes_are_visible_and_trace_shows_payload_text() {
        let output = Buffer(Arc::new(Mutex::new(Vec::new())));
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("warn,rusthinq_app=trace"))
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        let session = rusthinq_server::SessionId {
            device: "test-device".into(),
            generation: 1,
        };
        tracing::subscriber::with_default(subscriber, || {
            mqtt_diagnostic(&MqttDiagnostic::Subscribed {
                generation: 1,
                client: Some("test-device".into()),
                filters: vec!["lime/devices/test-device".into()],
            });
            mqtt_diagnostic(&MqttDiagnostic::DownlinkRefused {
                generation: 1,
                client: Some("test-device".into()),
                session: Some(session.clone()),
                ready: true,
                reason: DownlinkRefusal::NotSubscribed,
                cmd: Some("packet".into()),
                payload_len: 9,
            });
            runtime_event(&Event::Transport(Transport::PublishIgnored(
                session.clone(),
                ThinQ2Error::OutsideClip,
                "express/monitoring".into(),
                br#"{"mon":1}"#.to_vec(),
            )));
            runtime_event(&Event::Transport(Transport::CloudBound(
                session,
                br#"{"cmd":"device_packet"}"#.to_vec(),
            )));
        });
        let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        assert!(text.contains("lime/devices/test-device"), "{text}");
        let refused = text.find("device write refused").unwrap();
        assert!(text[refused..].contains("NotSubscribed"), "{text}");
        assert!(text.contains(r#"payload={"mon":1}"#), "{text}");
        assert!(
            text.contains(r#"payload={"cmd":"device_packet"}"#),
            "{text}"
        );
    }

    #[test]
    fn info_hides_payloads_but_keeps_connections_and_failures() {
        let output = Buffer(Arc::new(Mutex::new(Vec::new())));
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(DEFAULT_FILTER))
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        let session = rusthinq_server::SessionId {
            device: "test-device".into(),
            generation: 1,
        };
        tracing::subscriber::with_default(subscriber, || {
            runtime_event(&Event::Transport(Transport::Up(session.clone())));
            runtime_event(&Event::Transport(Transport::Response(
                session,
                serde_json::json!({"secret": "private-payload"}),
            )));
            runtime_event(&Event::CleanupFailed {
                reason: "disk failure".into(),
            });
            runtime_event(&Event::Lost {
                transport_events: 7,
            });
        });
        let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        assert!(text.contains("device connected"));
        assert!(text.contains("disk failure"));
        assert!(text.contains("transport events lost"));
        assert!(!text.contains("private-payload"));
    }
}
