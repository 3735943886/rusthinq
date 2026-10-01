use rusthinq_protocol::thinq1::{Action, Error, Input, Session, encode};
use serde_json::json;
use std::time::Duration;

fn session() -> Session {
    Session::new(4096, Duration::from_secs(90), Duration::ZERO)
}
fn payload(id: &str, body: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"Header":{"x-lgedm-deviceId":id,"unknown":7},"Body":body})).unwrap()
}
fn wire(payload: &[u8]) -> Vec<u8> {
    encode(payload, 4096).unwrap()
}

#[test]
fn poll_replay_at_every_split_preserves_ack_header_and_data() {
    for cmd in ["DevInfo", "Mon"] {
        let data = payload(
            "dev-1",
            json!({"Cmd":cmd,"Format":"B64","Data":"AQI=","unknown":42}),
        );
        let frame = wire(&data);
        for split in 0..=frame.len() {
            let mut model = session();
            let first = model.input(Input::Bytes(&frame[..split]), Duration::ZERO);
            let second = model.input(Input::Bytes(&frame[split..]), Duration::ZERO);
            assert_eq!(first.error, None);
            assert_eq!(second.error, None);
            let actions: Vec<_> = first.actions.into_iter().chain(second.actions).collect();
            assert_eq!(actions.len(), 3);
            assert_eq!(actions[0], Action::Identified("dev-1".into()));
            let Action::Send(ack) = &actions[1] else {
                panic!("missing ACK")
            };
            let ack: serde_json::Value = serde_json::from_slice(&ack[4..]).unwrap();
            assert_eq!(
                ack,
                json!({"Header":{"x-lgedm-deviceId":"dev-1","unknown":7},"Body":{"Return":"OK"}})
            );
            assert_eq!(actions[2], Action::Data(data.clone()));
        }
    }
}

#[test]
fn command_response_is_observed_without_generating_an_ack() {
    // A response carrying Cmd=Mon is still a response, not a poll request.
    let body = json!({"Cmd":"Mon","ReturnCode":"0000","CmdWId":"n-1"});
    let data = payload("dev-1", body.clone());
    let outcome = session().input(Input::Bytes(&wire(&data)), Duration::ZERO);
    assert_eq!(
        outcome.actions,
        vec![
            Action::Identified("dev-1".into()),
            Action::Response(body),
            Action::Data(data)
        ]
    );
}

#[test]
fn valid_prefix_actions_survive_a_later_invalid_frame() {
    let data = payload("dev-1", json!({"Cmd":"Other"}));
    let bytes = [wire(&data), wire(b"{")].concat();
    let mut model = session();
    let outcome = model.input(Input::Bytes(&bytes), Duration::ZERO);
    assert_eq!(outcome.error, Some(Error::InvalidJson));
    assert_eq!(
        outcome.actions,
        vec![Action::Identified("dev-1".into()), Action::Data(data)]
    );
    assert_eq!(outcome.next_deadline, None);
    assert_eq!(
        model.input(Input::Bytes(&bytes), Duration::ZERO).error,
        Some(Error::Closed)
    );
}

#[test]
fn invalid_identity_never_produces_actions() {
    for id in ["", "dev-2"] {
        let mut model = session();
        model.input(
            Input::Bytes(&wire(&payload("dev-1", json!({})))),
            Duration::ZERO,
        );
        let outcome = model.input(
            Input::Bytes(&wire(&payload(id, json!({"Cmd":"Mon"})))),
            Duration::ZERO,
        );
        assert_eq!(
            outcome.error,
            Some(if id.is_empty() {
                Error::MissingDeviceId
            } else {
                Error::DeviceIdChanged
            })
        );
        assert!(outcome.actions.is_empty());
    }
    for data in [b"null".as_slice(), br#"{"Header":{"x-lgedm-deviceId":3}}"#] {
        assert_eq!(
            session()
                .input(Input::Bytes(&wire(data)), Duration::ZERO)
                .error,
            Some(Error::MissingDeviceId)
        );
    }
}

#[test]
fn rejects_lengths_and_truncated_end() {
    for (bytes, error) in [
        ([255; 4], Error::NegativeLength),
        (4097_i32.to_be_bytes(), Error::PayloadExceeded),
    ] {
        assert_eq!(
            session().input(Input::Bytes(&bytes), Duration::ZERO).error,
            Some(error)
        );
    }
    let frame = wire(&payload("dev-1", json!({})));
    for cut in 1..frame.len() {
        let mut model = session();
        assert_eq!(
            model
                .input(Input::Bytes(&frame[..cut]), Duration::ZERO)
                .error,
            None
        );
        assert_eq!(
            model.input(Input::End, Duration::ZERO).error,
            Some(Error::Truncated)
        );
    }
    assert_eq!(
        session().input(Input::End, Duration::ZERO).next_deadline,
        None
    );
    assert_eq!(
        session().input(Input::Bytes(&[0; 4]), Duration::ZERO).error,
        Some(Error::InvalidJson)
    );
    assert_eq!(encode(&[0; 5], 4), Err(Error::PayloadExceeded));
}

#[test]
fn injected_time_enforces_idle_deadline_and_monotonicity() {
    let mut model = session();
    assert_eq!(
        model
            .input(Input::Tick, Duration::from_secs(20))
            .next_deadline,
        Some(Duration::from_secs(90))
    );
    assert_eq!(
        model
            .input(Input::Bytes(&[0]), Duration::from_secs(30))
            .next_deadline,
        Some(Duration::from_secs(120))
    );
    assert_eq!(
        model
            .input(Input::Bytes(&[]), Duration::from_secs(119))
            .next_deadline,
        Some(Duration::from_secs(120))
    );
    assert_eq!(
        model.input(Input::Tick, Duration::from_secs(120)).error,
        Some(Error::IdleTimeout)
    );
    let mut model = session();
    model.input(Input::Tick, Duration::from_secs(10));
    assert_eq!(
        model.input(Input::Tick, Duration::from_secs(9)).error,
        Some(Error::TimeWentBackwards)
    );
}

#[test]
fn multiple_frames_identify_once() {
    let data = payload("dev-1", json!({}));
    let bytes = [wire(&data), wire(&data)].concat();
    let mut model = session();
    let outcome = model.input(Input::Bytes(&bytes), Duration::ZERO);
    assert_eq!(
        outcome.actions,
        vec![
            Action::Identified("dev-1".into()),
            Action::Data(data.clone()),
            Action::Data(data)
        ]
    );
    assert_eq!(model.device_id(), Some("dev-1"));
}

#[test]
fn captured_provisioning_payload_replays_with_original_length() {
    // Reference fixture: legacy rusthinq-util length_prefixed_frame.rs,
    // real_world_thinq1_provisioning_frame (WTDN3).
    let data = concat!(
        r#"{"Header":{"x-lgedm-deviceId":"48552db0-1ab4-11e9-b4fb-7c1c4ec8cc53"},"#,
        r#""Body":{"CmdWId":"e5d59e90-99a2-11f0-8ef9-7c1c4ec8cc53","Cmd":"DevInfo","Format":"B64","#,
        r#""Data":"UnVsZVZlcj0xLjMsRndWZXI9UUNfTW9kZW1fMS4yLjgwLHJlZ0ZhaWw9Tg=="}}"#
    ).as_bytes();
    assert_eq!(data.len(), 0xe4);
    let mut framed = vec![0, 0, 0, 0xe4];
    framed.extend_from_slice(data);
    let outcome = session().input(Input::Bytes(&framed), Duration::ZERO);
    assert_eq!(outcome.error, None);
    assert_eq!(outcome.actions.len(), 3);
    assert_eq!(outcome.actions[2], Action::Data(data.to_vec()));
}
