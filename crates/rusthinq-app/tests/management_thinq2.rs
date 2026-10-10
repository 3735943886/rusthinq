use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use rusthinq_app::{
    lifecycle_storage::Storage,
    management::{self, Credentials},
    runtime::{Event, Runtime},
};
use rusthinq_lifecycle::Action;
use rusthinq_protocol::mqtt;
use rusthinq_server::{
    Config,
    mqtt::{Broker, SystemClock},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{broadcast, watch},
    time::timeout,
};
use tower::ServiceExt;
fn request(path: &str, body: Value, auth: bool) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(path)
        .method("POST")
        .header("content-type", "application/json");
    if auth {
        builder = builder.header("authorization", "Basic YWRtaW46c2VjcmV0");
    }
    builder.body(Body::from(body.to_string())).unwrap()
}
#[tokio::test]
async fn generated_commands_are_authenticated_fenced_and_work_without_scripts_or_external_mqtt() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::new(Config::default(), Arc::new(SystemClock)).unwrap();
    let runtime = Runtime::new_mqtt(
        Storage::open(&dir.path().join("devices.json"), 8).unwrap(),
        broker.handle(),
        Duration::ZERO,
        128,
    )
    .unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let options = management::Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        gui: false,
        credentials: Some(Credentials {
            user: "admin".into(),
            password: "secret".into(),
        }),
        raw_inject_toggle: false,
        raw_inject: Default::default(),
    };
    let app = management::router(handle.clone(), options, stopped.clone()).unwrap();
    let task = tokio::spawn(runtime.run(stopped));
    let (stream, mut peer) = tokio::io::duplex(8192);
    let running = broker.clone();
    let device = tokio::spawn(async move { running.run(stream).await.unwrap() });
    connect(&mut peer).await;
    deploy(&mut peer).await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    let session = handle.snapshot()[0].session.unwrap();
    let scope = json!({"incarnation": session.incarnation.to_string(), "generation": session.generation.to_string()});
    let mut raw = scope.clone();
    raw["hex"] = "00aAff".into();
    let mut clip = scope.clone();
    clip["cmd"] = "setMaskingInfo".into();
    clip["type"] = 1.into();
    clip["data"] = json!({"mask":true});
    let mut mids = Vec::new();
    for (path, body, cmd, data) in [
        ("packet", raw.clone(), "packet", json!("00AAFF")),
        ("clip", clip.clone(), "setMaskingInfo", json!({"mask":true})),
    ] {
        let path = format!("/api/devices/d/{path}");
        assert_eq!(
            app.clone()
                .oneshot(request(&path, body.clone(), false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let response = app
            .clone()
            .oneshot(request(&path, body, true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let result: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(
            result,
            json!({"delivery":"Sent","deviceAcknowledged":false})
        );
        let bytes = packet(&mut peer).await;
        assert_eq!(bytes[0] & 1, 0);
        let mqtt::Packet::Publish { topic, payload, .. } = mqtt::decode(&bytes, 8192).unwrap()
        else {
            panic!("publish expected")
        };
        assert_eq!(topic, "lime/devices/d");
        let sent: Value = serde_json::from_slice(&payload).unwrap();
        let mid = sent["mid"].as_u64().unwrap();
        mids.push(mid);
        assert_eq!(
            sent,
            json!({"did":"d", "mid":mid, "cmd":cmd, "type":1, "data":data})
        );
    }
    assert!(mids[1] > mids[0]);
    // Diagnostic outbound injection uses the same ID namespace without altering its contract.
    let delivery = handle
        .adapter_inject("d".into(), session, vec![0xab], true)
        .await
        .unwrap();
    assert_eq!(delivery, Some(rusthinq_app::api::Delivery::Sent));
    let bytes = packet(&mut peer).await;
    let mqtt::Packet::Publish { payload, .. } = mqtt::decode(&bytes, 8192).unwrap() else {
        panic!("publish expected")
    };
    let injected: Value = serde_json::from_slice(&payload).unwrap();
    assert!(injected["mid"].as_u64().unwrap() > mids[1]);
    assert_eq!(injected["cmd"], "packet");
    assert_eq!(injected["data"], "AB");
    // A live device without a matching subscription cannot receive the downlink.
    let topic = b"lime/devices/d";
    let mut unsubscribe = vec![0, 2, 0, topic.len() as u8];
    unsubscribe.extend_from_slice(topic);
    peer.write_all(&mqtt::frame(0xa2, &unsubscribe, 8192).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&mut peer).await, [0xb0, 2, 0, 2]);
    let response = app
        .clone()
        .oneshot(request("/api/devices/d/clip", clip.clone(), true))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let result: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(
        result,
        json!({"delivery":"Failed","deviceAcknowledged":false})
    );
    subscribe(&mut peer).await;
    for hex in [
        json!(null),
        json!(123),
        json!(""),
        json!("0"),
        json!("gg"),
        json!("00 ff"),
    ] {
        let mut body = raw.clone();
        body["hex"] = hex;
        assert_eq!(
            app.clone()
                .oneshot(request("/api/devices/d/packet", body, true))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    for (key, value) in [
        ("cmd", json!("")),
        ("cmd", json!(42)),
        ("type", json!("1")),
        ("type", json!(null)),
        ("type", json!(1.5)),
        ("mid", json!(1)),
    ] {
        let mut body = clip.clone();
        body[key] = value;
        assert_eq!(
            app.clone()
                .oneshot(request("/api/devices/d/clip", body, true))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let mut missing = clip.clone();
    missing.as_object_mut().unwrap().remove("data");
    assert_eq!(
        app.clone()
            .oneshot(request("/api/devices/d/clip", missing, true))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let mut large = raw.clone();
    large["hex"] = "AA".repeat(500_001).into();
    assert_eq!(
        app.clone()
            .oneshot(request("/api/devices/d/packet", large, true))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    // JSON escaping also counts toward the HTTP body limit.
    let mut large = clip.clone();
    large["data"] = "\u{0001}".repeat(170_000).into();
    assert_eq!(
        app.clone()
            .oneshot(request("/api/devices/d/clip", large, true))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert!(matches!(
        handle
            .send_clip(
                "d".into(),
                session,
                "large",
                1,
                json!("x".repeat(1_000_000))
            )
            .await,
        Err(rusthinq_server::Reject::PayloadExceeded)
    ));
    for key in ["incarnation", "generation"] {
        let mut body = raw.clone();
        body[key] = json!(if key == "incarnation" {
            session.incarnation + 1
        } else {
            session.generation + 1
        });
        assert_eq!(
            app.clone()
                .oneshot(request("/api/devices/d/packet", body, true))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
    }
    assert_eq!(
        app.clone()
            .oneshot(request("/api/devices/d/inject", raw.clone(), true))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    // Replace the connection during a partially written generated command. Its
    // outcome is Unknown; an admitted command waiting behind it is Failed and
    // must never migrate to the replacement session.
    let mut partial = clip.clone();
    partial["data"] = "x".repeat(20_000).into();
    let pending_app = app.clone();
    let pending = tokio::spawn(async move {
        pending_app
            .oneshot(request("/api/devices/d/clip", partial, true))
            .await
            .unwrap()
    });
    peer.read_u8().await.unwrap();
    let queued = handle
        .send_clip("d".into(), session, "queued", 1, json!({}))
        .await
        .unwrap();
    let (stream, mut replacement) = tokio::io::duplex(8192);
    let running = broker.clone();
    let new_device = tokio::spawn(async move { running.run(stream).await.unwrap() });
    connect(&mut replacement).await;
    deploy(&mut replacement).await;
    let response = pending.await.unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let result: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(
        result,
        json!({"delivery":"Unknown","deviceAcknowledged":false})
    );
    assert_eq!(queued.wait().await, rusthinq_server::Delivery::Failed);
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    assert_eq!(
        app.clone()
            .oneshot(request("/api/devices/d/clip", clip.clone(), true))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert!(
        timeout(Duration::from_millis(50), replacement.read_u8())
            .await
            .is_err()
    );
    drop(peer);
    device.await.unwrap();
    let new_session = handle.snapshot()[0].session.unwrap();
    clip["generation"] = new_session.generation.to_string().into();
    clip["incarnation"] = new_session.incarnation.to_string().into();
    drop(replacement);
    new_device.await.unwrap();
    assert_eq!(
        app.oneshot(request("/api/devices/d/clip", clip, true))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}
async fn packet(peer: &mut DuplexStream) -> Vec<u8> {
    timeout(Duration::from_secs(3), async {
        let mut bytes = vec![peer.read_u8().await.unwrap()];
        loop {
            if let Some(total) = mqtt::length(&bytes, 8192).unwrap() {
                let offset = bytes.len();
                bytes.resize(total, 0);
                peer.read_exact(&mut bytes[offset..]).await.unwrap();
                return bytes;
            }
            bytes.push(peer.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap()
}
async fn until(events: &mut broadcast::Receiver<Event>, predicate: impl Fn(&Event) -> bool) {
    timeout(Duration::from_secs(3), async {
        loop {
            if predicate(&events.recv().await.unwrap()) {
                return;
            }
        }
    })
    .await
    .unwrap();
}
async fn publish(peer: &mut DuplexStream, topic: &str, value: serde_json::Value) {
    peer.write_all(&mqtt::publish(topic, value.to_string().as_bytes(), 8192).unwrap())
        .await
        .unwrap();
}
async fn connect(peer: &mut DuplexStream) {
    peer.write_all(
        &mqtt::frame(
            0x10,
            &[0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'x'],
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(packet(peer).await, [0x20, 2, 0, 0]);
    subscribe(peer).await;
}
async fn subscribe(peer: &mut DuplexStream) {
    let topic = b"lime/devices/d";
    let mut body = vec![0, 1, 0, topic.len() as u8];
    body.extend_from_slice(topic);
    body.push(0);
    peer.write_all(&mqtt::frame(0x82, &body, 8192).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(peer).await, [0x90, 3, 0, 1, 0]);
}
async fn deploy(peer: &mut DuplexStream) {
    publish(
        peer,
        "clip/provisioning/devices/d",
        json!({"did":"d","cmd":"deploy","kind":"model","data":{}}),
    )
    .await;
    assert!(matches!(
        mqtt::decode(&packet(peer).await, 8192).unwrap(),
        mqtt::Packet::Publish { .. }
    ));
    publish(
        peer,
        "clip/message/devices/d",
        json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
}
