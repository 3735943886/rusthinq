//! Daemon log policy. Subscribers belong to executables, not library callers.
use crate::runtime::Event;
use rusthinq_lifecycle::Action;
use rusthinq_server::{Delivery, Disconnect, Event as Transport, MqttDiagnostic};
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
        Event::Transport(
            Transport::Data(..)
            | Transport::Sent(..)
            | Transport::Response(..)
            | Transport::Ready(..)
            | Transport::CloudBound(..)
            | Transport::BridgedCloudBound(..)
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
        MqttDiagnostic::Closed {
            reason: Disconnect::Eof | Disconnect::Closed,
            ..
        } => {
            tracing::info!(?diagnostic, "MQTT client closed");
        }
        MqttDiagnostic::Closed { .. } => tracing::warn!(?diagnostic, "MQTT client failed"),
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
