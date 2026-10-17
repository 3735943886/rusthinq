use rusthinq_protocol::{
    aabb,
    thinq2::{self, Action, Error, Input, Session},
};
use serde_json::{Value, json};
use std::time::Duration;

fn input(model: &mut Session, topic: &str, value: Value) -> thinq2::Outcome {
    let bytes = serde_json::to_vec(&value).unwrap();
    model.input(
        Input::Device {
            topic,
            payload: &bytes,
        },
        Duration::ZERO,
    )
}
fn deploy(model: &mut Session, kind: &str) {
    let value = json!({"did":"dev-1","cmd":"deploy","kind":kind,"data":{"appInfo":{"softVer":"unrecorded"}},"extra":7});
    let outcome = input(model, "clip/provisioning/devices/dev-1", value.clone());
    assert_eq!(outcome.error, None);
    assert_eq!(
        outcome.actions,
        vec![Action::Provision {
            operation: 1,
            request: value
        }]
    );
    assert_eq!(
        model
            .input(
                Input::ProvisionResult {
                    operation: 1,
                    sent: true
                },
                Duration::ZERO
            )
            .error,
        None
    );
}
fn ready(model: &mut Session) {
    let ack = json!({"did":"dev-1","cmd":"completeProvisioning_ack"});
    let outcome = input(model, "clip/message/devices/dev-1", ack.clone());
    assert_eq!(outcome.error, None);
    assert!(
        matches!(&outcome.actions[..], [Action::Ready { device_id, deploy }] if device_id == "dev-1" && deploy["extra"] == 7)
    );
    assert!(
        input(model, "clip/message/devices/dev-1", ack)
            .actions
            .is_empty()
    );
}

#[test]
fn device_packet_is_delivered_without_a_host_ack() {
    // Which frames the cloud acknowledges is the driver's business (aabb_common's ack).
    for bridge in [false, true] {
        let mut model = Session::new(4096, bridge, Duration::ZERO);
        deploy(&mut model, "unknown");
        ready(&mut model);
        let data = aabb::wrap(&[0x30, 0x4d, 1]).unwrap();
        let packet = json!({"did":"dev-1","cmd":"device_packet","data":thinq2::encode_hex(&data),"unknown":{"x":9}});
        let raw = serde_json::to_vec(&packet).unwrap();
        let outcome = input(&mut model, "clip/message/devices/dev-1", packet);
        assert_eq!(outcome.error, None);
        assert_eq!(
            outcome.actions,
            vec![
                Action::Data(data),
                Action::CloudBound {
                    payload: raw,
                    bridge_generation: bridge.then_some(1)
                }
            ]
        );
    }
}

#[test]
fn bridged_session_relays_cloud_bytes_exactly() {
    let mut model = Session::new(4096, true, Duration::ZERO);
    deploy(&mut model, "unknown");
    ready(&mut model);
    let outcome = input(
        &mut model,
        "clip/message/devices/dev-1",
        json!({"did":"dev-1","cmd":"device_packet","data":thinq2::encode_hex(&aabb::wrap(&[0x30,0x4d,1]).unwrap())}),
    );
    assert_eq!(outcome.error, None);
    assert!(
        outcome
            .actions
            .iter()
            .all(|a| !matches!(a, Action::Send { .. }))
    );
    let raw = br#"{ "did":"dev-1", "mid":1785283454163, "cmd":"ack", "data":"AA08F000C5043EBB", "future":true }"#;
    let outcome = model.input(
        Input::Cloud {
            generation: 1,
            payload: raw,
        },
        Duration::ZERO,
    );
    assert_eq!(
        outcome.actions,
        vec![Action::Send {
            topic: "lime/devices/dev-1".into(),
            payload: raw.to_vec(),
            bridge_generation: Some(1)
        }]
    );
}

#[test]
fn unavailable_or_invalid_cloud_input_does_not_fault_local_device() {
    for bridge in [false, true] {
        let mut model = Session::new(4096, bridge, Duration::ZERO);
        deploy(&mut model, "unknown");
        ready(&mut model);
        for bytes in [
            b"{".as_slice(),
            br#"{"did":"other","cmd":"ack"}"#,
            br#"{"did":"dev-1","cmd":"ack"}"#,
        ] {
            let outcome = model.input(
                Input::Cloud {
                    generation: 1,
                    payload: bytes,
                },
                Duration::ZERO,
            );
            assert!(!outcome.closed);
        }
        let result = input(
            &mut model,
            "clip/message/devices/dev-1",
            json!({"did":"dev-1","cmd":"req_timesync"}),
        );
        assert_eq!(result.actions, vec![Action::TimeSyncRequested]);
    }
}

#[test]
fn first_packet_completes_provisioning_for_any_model() {
    // T17A1EFHU_F is one firmware that never sends completeProvisioning_ack (upstream rethink 8f6d19e).
    for kind in ["T17A1EFHU_F", "unknown"] {
        let mut model = Session::new(4096, false, Duration::ZERO);
        deploy(&mut model, kind);
        let outcome = input(
            &mut model,
            "clip/message/devices/dev-1",
            json!({"did":"dev-1","cmd":"device_packet","data":"AABB"}),
        );
        assert_eq!(outcome.error, None);
        assert!(matches!(outcome.actions[0], Action::Ready { .. }));
        assert_eq!(outcome.actions[1], Action::Data(vec![0xaa, 0xbb]));
        assert!(
            input(
                &mut model,
                "clip/message/devices/dev-1",
                json!({"did":"dev-1","cmd":"completeProvisioning_ack"})
            )
            .actions
            .is_empty()
        );
    }
}

#[test]
fn aws_republish_topic_and_single_trailing_nul_are_accepted() {
    let mut model = Session::new(4096, false, Duration::ZERO);
    let raw = b"{\"did\":\"dev-1\",\"cmd\":\"deploy\"}\0";
    let outcome = model.input(
        Input::Device {
            topic: "$aws/rules/clip_provisioning_rule/clip/provisioning/devices/dev-1",
            payload: raw,
        },
        Duration::ZERO,
    );
    assert_eq!(outcome.error, None);
    assert!(matches!(outcome.actions[0], Action::Provision { .. }));
    assert_eq!(thinq2::normalize_clip_topic("foo/bar/clip"), "foo/bar/clip");
    assert_eq!(
        thinq2::normalize_clip_topic("foo/clipwrong/message/devices/dev-1"),
        "foo/clipwrong/message/devices/dev-1"
    );
}

#[test]
fn unknown_clip_is_preserved_and_not_confused_with_command_completion() {
    let mut model = Session::new(4096, false, Duration::ZERO);
    deploy(&mut model, "unknown");
    ready(&mut model);
    let raw = br#"{"did":"dev-1","cmd":"respUniversalCtrl","data":{"reqType":"online_check","responseCode":"0000"},"future":123}"#;
    let outcome = model.input(
        Input::Device {
            topic: "clip/message/devices/dev-1",
            payload: raw,
        },
        Duration::ZERO,
    );
    assert_eq!(
        outcome.actions,
        vec![Action::CloudBound {
            payload: raw.to_vec(),
            bridge_generation: None
        }]
    );
}

#[test]
fn device_errors_are_explicit_and_terminal() {
    for (topic, value, expected) in [
        (
            "clip/message/devices/other",
            json!({"did":"dev-1","cmd":"req_timesync"}),
            Error::InvalidTopic,
        ),
        (
            "clip/message/devices/other",
            json!({"did":"other","cmd":"req_timesync"}),
            Error::DeviceIdChanged,
        ),
        (
            "clip/message/devices/dev-1",
            json!({"did":"dev-1","cmd":"device_packet","data":"GG"}),
            Error::InvalidHex,
        ),
        (
            "clip/message/devices/dev-1",
            json!({"did":"dev-1","cmd":"device_packet","data":"0"}),
            Error::InvalidHex,
        ),
        (
            "clip/message/devices/dev-1",
            json!({"did":"dev-1","cmd":"device_packet","data":4}),
            Error::InvalidHex,
        ),
        (
            "clip/message/devices/dev-1",
            json!({"did":"dev-1","cmd":3}),
            Error::InvalidEnvelope,
        ),
    ] {
        let mut model = Session::new(4096, false, Duration::ZERO);
        deploy(&mut model, "unknown");
        ready(&mut model);
        let outcome = input(&mut model, topic, value);
        assert_eq!(outcome.error, Some(expected));
        assert!(outcome.closed);
        assert!(outcome.actions.is_empty());
        assert_eq!(
            model.input(Input::End, Duration::ZERO).error,
            Some(Error::Closed)
        );
    }
}

#[test]
fn bounds_clock_and_end_are_enforced_without_io() {
    let mut model = Session::new(8, false, Duration::ZERO);
    assert_eq!(
        model
            .input(
                Input::Device {
                    topic: "clip/message/devices/dev-1",
                    payload: &[0; 1000],
                },
                Duration::ZERO
            )
            .error,
        Some(Error::PayloadExceeded)
    );
    let mut model = Session::new(4096, false, Duration::from_secs(1));
    assert_eq!(
        model.input(Input::End, Duration::ZERO).error,
        Some(Error::TimeWentBackwards)
    );
    assert!(
        Session::new(4096, false, Duration::ZERO)
            .input(Input::End, Duration::ZERO)
            .closed
    );
    assert_eq!(thinq2::decode_hex("aBcD").unwrap(), vec![0xab, 0xcd]);
    assert!(thinq2::decode_hex("é").is_err());
}

#[test]
fn aabb_short_framing_rejects_long_lengths() {
    assert_eq!(
        thinq2::encode_hex(&aabb::wrap(&[0xf0, 0x00, 0x4d, 0x04]).unwrap()),
        "AA08F0004D04A6BB"
    );
    assert!(aabb::wrap(&[0; 251]).is_none());
}

#[test]
fn provisioning_completion_waits_for_correlated_send_success() {
    let mut m = Session::new(4096, false, Duration::ZERO);
    let provision = input(
        &mut m,
        "clip/provisioning/devices/dev-1",
        json!({"did":"dev-1","cmd":"deploy"}),
    );
    assert!(matches!(
        provision.actions[0],
        Action::Provision { operation: 1, .. }
    ));
    let ack = input(
        &mut m,
        "clip/message/devices/dev-1",
        json!({"did":"dev-1","cmd":"completeProvisioning_ack"}),
    );
    assert!(ack.actions.is_empty());
    assert!(
        m.input(
            Input::ProvisionResult {
                operation: 0,
                sent: true
            },
            Duration::ZERO
        )
        .actions
        .is_empty()
    );
    let sent = m.input(
        Input::ProvisionResult {
            operation: 1,
            sent: true,
        },
        Duration::ZERO,
    );
    assert!(matches!(sent.actions[0], Action::Ready { .. }));
    assert!(
        m.input(
            Input::ProvisionResult {
                operation: 1,
                sent: false
            },
            Duration::ZERO
        )
        .actions
        .is_empty()
    );
}

#[test]
fn stale_provision_results_cannot_complete_replacement_and_send_failure_closes() {
    let mut m = Session::new(4096, false, Duration::ZERO);
    for operation in 1..=2 {
        let result = input(
            &mut m,
            "clip/provisioning/devices/dev-1",
            json!({"did":"dev-1","cmd":"deploy","mid":operation}),
        );
        assert!(matches!(result.actions[0],Action::Provision {operation:op,..} if op==operation));
    }
    assert!(
        m.input(
            Input::ProvisionResult {
                operation: 1,
                sent: true
            },
            Duration::ZERO
        )
        .actions
        .is_empty()
    );
    let busy = input(
        &mut m,
        "clip/message/devices/dev-1",
        json!({"did":"dev-1","cmd":"device_packet","data":"AABB"}),
    );
    assert_eq!(busy.error, Some(Error::Busy));
    assert!(!busy.closed);
    let failed = m.input(
        Input::ProvisionResult {
            operation: 2,
            sent: false,
        },
        Duration::ZERO,
    );
    assert_eq!(failed.error, Some(Error::ProvisionFailed));
    assert!(failed.closed);
}

#[test]
fn live_bridge_transitions_fence_old_inputs_and_queued_writes() {
    let mut m = Session::new(4096, true, Duration::ZERO);
    deploy(&mut m, "unknown");
    ready(&mut m);
    let packet = json!({"did":"dev-1","cmd":"device_packet","data":thinq2::encode_hex(&aabb::wrap(&[0x30,0x4d,1]).unwrap())});
    let bridged = input(&mut m, "clip/message/devices/dev-1", packet.clone());
    assert!(
        !bridged
            .actions
            .iter()
            .any(|a| matches!(a, Action::Send { .. }))
    );
    assert!(bridged.actions.iter().any(|a| matches!(
        a,
        Action::CloudBound {
            bridge_generation: Some(1),
            ..
        }
    )));
    let raw = br#"{"did":"dev-1","cmd":"ack","data":"AA08F0004D04A6BB"}"#;
    let queued = m.input(
        Input::Cloud {
            generation: 1,
            payload: raw,
        },
        Duration::ZERO,
    );
    assert!(matches!(
        queued.actions[0],
        Action::Send {
            bridge_generation: Some(1),
            ..
        }
    ));
    let disabled = m.input(
        Input::BridgeState {
            generation: 2,
            active: false,
        },
        Duration::ZERO,
    );
    assert_eq!(
        disabled.actions,
        vec![Action::BridgeChanged {
            generation: 2,
            active: false
        }]
    );
    assert!(!m.accepts_bridge_generation(1));
    assert!(!m.accepts_bridge_generation(2));
    let local = input(&mut m, "clip/message/devices/dev-1", packet.clone());
    assert!(
        local
            .actions
            .iter()
            .all(|a| !matches!(a, Action::Send { .. }))
    );
    assert!(local.actions.iter().any(|a| matches!(
        a,
        Action::CloudBound {
            bridge_generation: None,
            ..
        }
    )));
    let late = m.input(
        Input::Cloud {
            generation: 1,
            payload: raw,
        },
        Duration::ZERO,
    );
    assert_eq!(late.error, Some(Error::StaleBridge));
    assert!(!late.closed);
    m.input(
        Input::BridgeState {
            generation: 3,
            active: true,
        },
        Duration::ZERO,
    );
    assert!(m.accepts_bridge_generation(3));
    assert!(!m.accepts_bridge_generation(1));
    let enabled = input(&mut m, "clip/message/devices/dev-1", packet);
    assert!(
        !enabled
            .actions
            .iter()
            .any(|a| matches!(a, Action::Send { .. }))
    );
    assert!(enabled.actions.iter().any(|a| matches!(
        a,
        Action::CloudBound {
            bridge_generation: Some(3),
            ..
        }
    )));
    let old = m.input(
        Input::Cloud {
            generation: 1,
            payload: raw,
        },
        Duration::ZERO,
    );
    assert_eq!(old.error, Some(Error::StaleBridge));
    let current = m.input(
        Input::Cloud {
            generation: 3,
            payload: raw,
        },
        Duration::ZERO,
    );
    assert_eq!(current.error, None);
    assert!(matches!(
        current.actions[0],
        Action::Send {
            bridge_generation: Some(3),
            ..
        }
    ));
    assert_eq!(m.device_id(), Some("dev-1"));
}

#[test]
fn stale_bridge_updates_cannot_reactivate_or_close_a_session() {
    let mut m = Session::new(4096, false, Duration::ZERO);
    m.input(
        Input::BridgeState {
            generation: 5,
            active: true,
        },
        Duration::ZERO,
    );
    for generation in [0, 1, 4, 5] {
        let result = m.input(
            Input::BridgeState {
                generation,
                active: false,
            },
            Duration::ZERO,
        );
        assert_eq!(result.error, Some(Error::StaleBridge));
        assert!(!result.closed);
        assert!(result.actions.is_empty());
        assert!(m.accepts_bridge_generation(5));
    }
    m.input(Input::End, Duration::ZERO);
    assert!(!m.accepts_bridge_generation(5));
}
