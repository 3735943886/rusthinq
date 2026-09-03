//! ThinQ2 device simulator — the other half of what the old plaintext 1884 listener
//! did, moved onto the already-authenticated control-plane connection instead of a
//! second, unauthenticated port.
//!
//! 1884 worked by handing a plain (no TLS) TCP client the exact same wire-protocol
//! entry point (`Broker::accept_tcp`) a real appliance's TLS connection used, so a
//! human/tool could pretend to be a brand-new device — registration handshake and
//! all — for local RE/dev work with no physical appliance and no certs. `raw_bus.rs`
//! already covers watching/injecting an *already-connected* device's traffic; it can't
//! bootstrap a new one, because there is no `ConnectedDevice` to look up yet.
//!
//! Rather than reopen a port for that gap, this drives `DeviceAcceptor::handle_mqtt`
//! directly — the exact function a real device's CLIP publish would reach after the
//! broker decodes it — with a synthetic client_id allocated per simulated device id.
//! `ClientMeta`/`clients_by_id` bookkeeping in `mqtt_broker.rs` tolerates a client_id
//! that was never actually registered in `Broker`'s connection table (every read is a
//! `HashMap::get` with a default fallback, every write is a plain insert), so this
//! needs no changes there.
//!
//! Topics (`<prefix>` = `config.mqtt.raw_prefix`, same gate as raw_bus.rs — both are
//! `None` or both are set together):
//!   - `<prefix>/<did>/simdev/publish/set` (subscribed) — JSON body
//!     `{"topic": "clip/provisioning/devices/<did>", "payload": {...}}`; `payload` is
//!     whatever CLIP JSON a real device would have sent at that topic.
//!   - `<prefix>/<did>/simdev/event` (published, non-retained) — every
//!     `lime/devices/<did>` reply the broker generates for `did`, real or simulated:
//!     the deploy/pre-deploy handshake response, `resp_timesync`, etc. A simulated
//!     device that completes provisioning becomes a real `ConnectedDevice` from that
//!     point on, so its ordinary traffic shows up on raw_bus's own topics from there —
//!     this only needs to cover what raw_bus can't (there is no device yet, or the
//!     reply — e.g. timesync — never goes through a `ConnectedDevice` at all).

use crate::mqtt_broker::{Broker, PublishPacket};
use crate::thinq2::device::DeviceAcceptor;
use rusthinq_core::mqtt::{MqttConnection, MqttSink};
use rusthinq_util::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Never assigned by `Broker::handle_connection` (which starts at 1 and grows slowly),
/// so a synthetic id can never collide with a real connection's.
const SYNTHETIC_ID_BASE: u64 = 1 << 62;

struct SimIds {
    next: AtomicU64,
    by_did: Mutex<HashMap<String, u64>>,
}

impl SimIds {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(SYNTHETIC_ID_BASE),
            by_did: Mutex::new(HashMap::new()),
        }
    }

    /// Stable per-`did` id across a whole simulated session (deploy → ack → traffic),
    /// mirroring what one real TCP connection's client_id would give for free.
    fn for_did(&self, did: &str) -> u64 {
        let mut map = self.by_did.lock();
        *map.entry(did.to_string())
            .or_insert_with(|| self.next.fetch_add(1, Ordering::Relaxed))
    }
}

/// Relay every `lime/devices/<did>` reply the broker emits to `<raw_prefix>/<did>/simdev/event`.
/// Split out from `register` so a test can drive it against a mock connection directly.
fn wire_outgoing_relay(broker: &Broker, ha: Arc<dyn MqttConnection>, raw_prefix: String) {
    broker.on_outgoing(Arc::new(move |p: &PublishPacket| {
        let Some(did) = p.topic.strip_prefix("lime/devices/") else {
            return;
        };
        ha.publish_raw(
            &format!("{raw_prefix}/{did}/simdev/event"),
            &p.payload,
            false,
        );
    }));
}

/// Wire up the simulator. Only call this when `config.mqtt.raw_prefix` is set — same
/// as `raw_bus::register_inject`.
pub fn register(
    sink: &Arc<MqttSink>,
    acceptor: Arc<DeviceAcceptor>,
    broker: Arc<Broker>,
    raw_prefix: String,
) {
    let ids = Arc::new(SimIds::new());

    sink.on_set_property(move |did, prop, value| {
        if prop != "simdev/publish" {
            return;
        }
        let Ok(body) = serde_json::from_str::<serde_json::Value>(value) else {
            return;
        };
        let Some(topic) = body.get("topic").and_then(|t| t.as_str()) else {
            return;
        };
        let payload = body
            .get("payload")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let client_id = ids.for_did(did);
        acceptor.handle_mqtt(topic, &payload, client_id);
    });

    wire_outgoing_relay(&broker, sink.clone() as Arc<dyn MqttConnection>, raw_prefix);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::devmgr::DeviceManager;
    use rusthinq_core::MockMqttConnection;
    use rusthinq_core::config::MqttConfig;

    fn test_config() -> MqttConfig {
        MqttConfig {
            mqtt_url: "mqtt://127.0.0.1:1883".into(),
            rusthinq_prefix: "rusthinq".into(),
            mqtt_user: String::new(),
            mqtt_pass: String::new(),
            raw_prefix: Some("rusthinq-raw".into()),
            state_file: None,
        }
    }

    fn publish_cmd(topic: &str, payload: serde_json::Value) -> String {
        serde_json::json!({ "topic": topic, "payload": payload }).to_string()
    }

    #[tokio::test]
    async fn a_simulated_device_completes_registration_and_becomes_a_real_device() {
        let sink = MqttSink::new(test_config());
        let manager = DeviceManager::new();
        let broker = Arc::new(Broker::new());
        let acceptor = DeviceAcceptor::new(broker.clone(), manager.clone());
        register(&sink, acceptor, broker, "rusthinq-raw".into());

        let did = "sim-1";
        let deploy = serde_json::json!({
            "did": did,
            "cmd": "deploy",
            "kind": "RAC_TEST_MODEL",
            "data": {
                "appInfo": { "modelName": "Test AC", "softVer": "1.0", "DeviceType": "401" },
            },
        });
        sink.handle_message(
            "rusthinq-raw/sim-1/simdev/publish/set",
            publish_cmd(&format!("clip/provisioning/devices/{did}"), deploy).as_bytes(),
            false,
        );
        sink.handle_message(
            "rusthinq-raw/sim-1/simdev/publish/set",
            publish_cmd(
                &format!("clip/message/devices/{did}"),
                serde_json::json!({ "did": did, "cmd": "completeProvisioning_ack" }),
            )
            .as_bytes(),
            false,
        );

        assert!(
            manager.get(did).is_some(),
            "simulated device must reach DeviceManager exactly like a real one"
        );
    }

    #[tokio::test]
    async fn repeated_publishes_for_the_same_did_reuse_one_synthetic_client_id() {
        // If they didn't, completeProvisioning_ack (which needs the deploy_msg stored
        // against the *same* client_id as the preceding preDeploy/deploy) would never
        // find it — this is exactly the bug a fresh id per call would cause.
        let sink = MqttSink::new(test_config());
        let manager = DeviceManager::new();
        let broker = Arc::new(Broker::new());
        let acceptor = DeviceAcceptor::new(broker.clone(), manager.clone());
        register(&sink, acceptor, broker, "rusthinq-raw".into());

        let did = "sim-3";
        for cmd_topic_pair in [
            (
                format!("clip/provisioning/devices/{did}"),
                serde_json::json!({ "did": did, "cmd": "preDeploy", "kind": "X" }),
            ),
            (
                format!("clip/message/devices/{did}"),
                serde_json::json!({ "did": did, "cmd": "completeProvisioning_ack" }),
            ),
        ] {
            sink.handle_message(
                "rusthinq-raw/sim-3/simdev/publish/set",
                publish_cmd(&cmd_topic_pair.0, cmd_topic_pair.1).as_bytes(),
                false,
            );
        }

        assert!(manager.get(did).is_some());
    }

    #[test]
    fn the_deploy_response_is_relayed_to_simdev_event() {
        let mock = MockMqttConnection::new();
        let manager = DeviceManager::new();
        let broker = Arc::new(Broker::new());
        let acceptor = DeviceAcceptor::new(broker.clone(), manager);
        wire_outgoing_relay(
            &broker,
            mock.clone() as Arc<dyn MqttConnection>,
            "rusthinq-raw".into(),
        );

        let did = "sim-2";
        let deploy =
            serde_json::json!({ "did": did, "cmd": "preDeploy", "kind": "RAC_TEST_MODEL" });
        acceptor.handle_mqtt(
            &format!("clip/provisioning/devices/{did}"),
            &deploy,
            999_999_000,
        );

        let raw = mock.raw_publishes();
        assert!(
            raw.iter()
                .any(|(t, _, retain)| t == &format!("rusthinq-raw/{did}/simdev/event") && !retain)
        );
    }

    #[test]
    fn a_real_devices_reply_is_relayed_too_not_only_simulated_ones() {
        // wire_outgoing_relay taps every lime/devices/<id> publish the broker makes,
        // regardless of whether the id behind it came from a real socket or
        // handle_mqtt called directly — there is nothing in the topic that says which.
        let mock = MockMqttConnection::new();
        let broker = Arc::new(Broker::new());
        wire_outgoing_relay(
            &broker,
            mock.clone() as Arc<dyn MqttConnection>,
            "rusthinq-raw".into(),
        );

        broker.publish(
            PublishPacket {
                topic: "lime/devices/real-device-1".into(),
                payload: b"{\"cmd\":\"resp_timesync\"}".to_vec(),
                retain: false,
                qos: 0,
                dup: false,
            },
            None,
        );

        let raw = mock.raw_publishes();
        assert!(
            raw.iter()
                .any(|(t, _, _)| t == "rusthinq-raw/real-device-1/simdev/event")
        );
    }

    #[test]
    fn a_client_originated_publish_is_never_relayed() {
        let mock = MockMqttConnection::new();
        let broker = Arc::new(Broker::new());
        wire_outgoing_relay(
            &broker,
            mock.clone() as Arc<dyn MqttConnection>,
            "rusthinq-raw".into(),
        );

        broker.publish(
            PublishPacket {
                topic: "lime/devices/x".into(),
                payload: b"hi".to_vec(),
                retain: false,
                qos: 0,
                dup: false,
            },
            Some(1),
        );

        assert!(mock.raw_publishes().is_empty());
    }
}
