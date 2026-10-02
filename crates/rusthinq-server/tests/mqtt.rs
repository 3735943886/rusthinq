use base64::Engine;
use rusthinq_protocol::mqtt;
use rusthinq_server::{
    Config, Delivery, Disconnect, Event, Reject,
    mqtt::{Broker, Clock, Sample},
};
use serde_json::{Value, json};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::broadcast,
    time::timeout,
};
struct FixedClock;

#[tokio::test]
async fn bridge_downlink_is_fenced_and_bridged_ingress_has_no_local_clip_ack() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let session = ready(&mut peer, "d", &mut events).await;
    // A bridge transition must preserve an in-progress device packet read.
    peer.write_all(&[0xc0]).await.unwrap();
    assert_eq!(
        handle.bridge_state(&session, 2, true).unwrap().wait().await,
        Delivery::Sent
    );
    assert_eq!(
        event(&mut events).await,
        Event::BridgeChanged(session.clone(), 2, true)
    );
    peer.write_all(&[0]).await.unwrap();
    assert_eq!(wire(&mut peer).await, [0xd0, 0]);
    let cloud = b"{\"did\":\"d\",\"cmd\":\"opaque\"}\0";
    let receipt = handle.cloud(&session, 2, cloud).unwrap();
    assert!(
        matches!(mqtt::decode(&wire(&mut peer).await, 8192).unwrap(), mqtt::Packet::Publish {payload, ..} if payload == cloud)
    );
    assert_eq!(receipt.wait().await, Delivery::Sent);
    let packet = json!({"did":"d","cmd":"device_packet","data":rusthinq_protocol::thinq2::encode_hex(&rusthinq_protocol::aabb::wrap(&[0xf0,1,4]).unwrap())});
    publish(&mut peer, "clip/message/devices/d", &packet).await;
    assert!(matches!(event(&mut events).await, Event::Data(ref id,_) if id == &session));
    assert!(
        matches!(event(&mut events).await, Event::BridgedCloudBound(ref id,2,_) if id == &session)
    );
    peer.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(wire(&mut peer).await, [0xd0, 0]);
    assert_eq!(
        handle
            .bridge_state(&session, 3, false)
            .unwrap()
            .wait()
            .await,
        Delivery::Sent
    );
    assert_eq!(
        event(&mut events).await,
        Event::BridgeChanged(session.clone(), 3, false)
    );
    assert_eq!(
        handle.cloud(&session, 2, cloud).unwrap().wait().await,
        Delivery::Failed
    );
    assert_eq!(
        handle.bridge_state(&session, 2, true).unwrap().wait().await,
        Delivery::Failed
    );
    assert!(matches!(
        handle.cloud(&session, 2, br#"{"did":"other","cmd":"opaque"}"#),
        Err(Reject::InvalidJson)
    ));
    peer.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(wire(&mut peer).await, [0xd0, 0]);
    publish(&mut peer, "clip/message/devices/d", &packet).await;
    let mqtt::Packet::Publish { payload, .. } = mqtt::decode(&wire(&mut peer).await, 8192).unwrap()
    else {
        panic!("local ack")
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&payload).unwrap()["cmd"],
        "ack"
    );
    assert!(matches!(event(&mut events).await, Event::Data(ref id,_) if id == &session));
    assert!(matches!(event(&mut events).await, Event::CloudBound(ref id,_) if id == &session));
    broker.stop();
    task.await.unwrap();
}

#[tokio::test]
async fn redeploy_drops_inherited_cloud_ack_ownership() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let old = ready(&mut peer, "d", &mut events).await;
    assert_eq!(
        handle.bridge_state(&old, 2, true).unwrap().wait().await,
        Delivery::Sent
    );
    event(&mut events).await;
    provisioning(&mut peer, "d").await;
    assert_eq!(
        event(&mut events).await,
        Event::Down(old.clone(), Disconnect::Closed)
    );
    assert!(matches!(
        event(&mut events).await,
        Event::BridgeChanged(_, 3, false)
    ));
    publish(
        &mut peer,
        "clip/message/devices/d",
        &json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
    let Event::Up(current) = event(&mut events).await else {
        panic!("up")
    };
    event(&mut events).await;
    assert!(current.generation > old.generation);
    assert!(matches!(
        handle.bridge_state(&old, 4, true),
        Err(Reject::StaleSession)
    ));
    assert_eq!(
        handle
            .cloud(&current, 2, br#"{"did":"d","cmd":"opaque"}"#)
            .unwrap()
            .wait()
            .await,
        Delivery::Failed
    );
    let packet = json!({"did":"d","cmd":"device_packet","data":rusthinq_protocol::thinq2::encode_hex(&rusthinq_protocol::aabb::wrap(&[0xf0,1,4]).unwrap())});
    publish(&mut peer, "clip/message/devices/d", &packet).await;
    let mqtt::Packet::Publish { payload, .. } = mqtt::decode(&wire(&mut peer).await, 8192).unwrap()
    else {
        panic!("ack")
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&payload).unwrap()["cmd"],
        "ack"
    );
    assert!(matches!(event(&mut events).await, Event::Data(_, _)));
    assert!(matches!(event(&mut events).await, Event::CloudBound(_, _)));
    broker.stop();
    task.await.unwrap();
}

#[tokio::test]
async fn reserved_generation_ceiling_fences_redeploy_and_new_transport_admission() {
    let broker = Broker::new(
        Config {
            generation_floor: 10,
            generation_ceiling: 11,
            ..Config::default()
        },
        Arc::new(FixedClock),
    )
    .unwrap();
    let mut events = broker.handle().subscribe();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let session = ready(&mut peer, "d", &mut events).await;
    assert_eq!(session.generation, 11);
    publish(
        &mut peer,
        "clip/provisioning/devices/d",
        &json!({"did":"d","cmd":"deploy","kind":"model","data":{}}),
    )
    .await;
    assert_eq!(
        event(&mut events).await,
        Event::Down(
            session,
            Disconnect::ThinQ2(rusthinq_protocol::thinq2::Error::CounterExhausted)
        )
    );
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert!(broker.handle().snapshot().is_empty());
    let (stream, _peer) = tokio::io::duplex(8192);
    assert!(broker.run(stream).await.is_err());
    assert_eq!(
        broker.handle().extend_generations(10, 12),
        Err(Reject::InvalidConfig)
    );
    broker.handle().extend_generations(11, 12).unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let current = ready(&mut peer, "d", &mut events).await;
    assert_eq!(current.generation, 12);
    broker.stop();
    task.await.unwrap();
}

#[tokio::test]
async fn qos2_release_delivers_once_with_retries_and_reusable_identifiers() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let mut events = broker.handle().subscribe();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let session = ready(&mut peer, "d", &mut events).await;
    for sequence in [1, 2] {
        let topic = "clip/message/devices/d";
        let payload = json!({"did":"d","cmd":"opaque","sequence":sequence}).to_string();
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&9u16.to_be_bytes());
        body.extend_from_slice(payload.as_bytes());
        for header in [0x34, 0x3c] {
            peer.write_all(&mqtt::frame(header, &body, 8192).unwrap())
                .await
                .unwrap();
            assert_eq!(wire(&mut peer).await, [0x50, 2, 0, 9]);
            assert!(matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ));
        }
        peer.write_all(&[0x62, 2, 0, 9]).await.unwrap();
        assert_eq!(wire(&mut peer).await, [0x70, 2, 0, 9]);
        assert_eq!(
            event(&mut events).await,
            Event::CloudBound(session.clone(), payload.into_bytes())
        );
        peer.write_all(&[0x62, 2, 0, 9]).await.unwrap();
        assert_eq!(wire(&mut peer).await, [0x70, 2, 0, 9]);
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }
    broker.stop();
    task.await.unwrap();
}

#[tokio::test]
async fn qos2_pending_storage_is_bounded() {
    let broker = Broker::new(
        Config {
            outbound_capacity: 1,
            ..Config::default()
        },
        Arc::new(FixedClock),
    )
    .unwrap();
    let mut events = broker.handle().subscribe();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let session = ready(&mut peer, "d", &mut events).await;
    let topic = "clip/message/devices/d";
    for id in [9u16, 10] {
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(br#"{"did":"d","cmd":"opaque"}"#);
        peer.write_all(&mqtt::frame(0x34, &body, 8192).unwrap())
            .await
            .unwrap();
        if id == 9 {
            assert_eq!(wire(&mut peer).await, [0x50, 2, 0, 9]);
        }
    }
    assert_eq!(
        event(&mut events).await,
        Event::Down(session, Disconnect::Mqtt)
    );
    task.await.unwrap();
    broker.stop();
}

#[tokio::test]
async fn will_is_observed_on_eof_but_suppressed_on_disconnect_and_shutdown() {
    for mode in 0..3 {
        let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
        let mut events = broker.handle().subscribe();
        let (stream, mut peer) = tokio::io::duplex(8192);
        let runtime = broker.clone();
        let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
        let body = [
            0, 4, b'M', b'Q', b'T', b'T', 4, 0x2e, 0, 60, 0, 0, 0, 1, b't', 0, 2, 0, 255,
        ];
        peer.write_all(&mqtt::frame(0x10, &body, 8192).unwrap())
            .await
            .unwrap();
        assert_eq!(wire(&mut peer).await, [0x20, 2, 0, 0]);
        match mode {
            0 => {
                peer.shutdown().await.unwrap();
            }
            1 => {
                peer.write_all(&[0xe0, 0]).await.unwrap();
            }
            _ => broker.stop(),
        }
        task.await.unwrap();
        if mode == 0 {
            assert!(
                matches!(event(&mut events).await, Event::Will {generation, session:None, message} if generation > 0 && message.topic == "t" && message.payload == [0,255] && message.qos == 1 && message.retain)
            );
        }
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        broker.stop();
    }
}

#[tokio::test]
async fn empty_client_ids_provision_independent_device_sessions() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let mut tasks = Vec::new();
    let mut peers = Vec::new();
    let mut ids = Vec::new();
    for did in ["first", "second"] {
        let (stream, mut peer) = tokio::io::duplex(8192);
        let runtime = broker.clone();
        tasks.push(tokio::spawn(
            async move { runtime.run(stream).await.unwrap() },
        ));
        setup_with_client(&mut peer, did, 60, "").await;
        provisioning(&mut peer, did).await;
        publish(
            &mut peer,
            &format!("clip/message/devices/{did}"),
            &json!({"did":did,"cmd":"completeProvisioning_ack"}),
        )
        .await;
        let Event::Up(id) = event(&mut events).await else {
            panic!("up")
        };
        assert_eq!(id.device, did);
        assert!(matches!(event(&mut events).await, Event::Ready(ref current, _) if current == &id));
        ids.push(id);
        peers.push(peer);
    }
    assert_eq!(handle.snapshot().len(), 2);
    for (id, peer) in ids.iter().zip(peers.iter_mut()) {
        let receipt = handle.send(id, b"{}").unwrap();
        assert!(
            matches!(mqtt::decode(&wire(peer).await, 8192).unwrap(), mqtt::Packet::Publish {topic, ..} if topic == format!("lime/devices/{}", id.device))
        );
        assert_eq!(receipt.wait().await, Delivery::Sent);
    }
    broker.stop();
    for task in tasks {
        task.await.unwrap();
    }
    assert!(handle.snapshot().is_empty());
}
impl Clock for FixedClock {
    fn sample(&self) -> io::Result<Sample> {
        Ok(Sample {
            mid: 123456,
            calendar: [26, 9, 1, 5, 6, 7, 4],
        })
    }
}

#[tokio::test]
async fn redeploy_on_one_transport_replaces_generation_without_reconnect() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let old = ready(&mut peer, "d", &mut events).await;
    provisioning(&mut peer, "d").await;
    assert_eq!(
        event(&mut events).await,
        Event::Down(old.clone(), Disconnect::Closed)
    );
    assert!(handle.snapshot().is_empty());
    assert_eq!(handle.close(&old), Err(Reject::StaleSession));
    assert!(matches!(
        handle.send(&old, b"{}"),
        Err(Reject::StaleSession)
    ));
    // A second deploy before completion also stays on the same transport.
    provisioning(&mut peer, "d").await;
    publish(
        &mut peer,
        "clip/message/devices/d",
        &json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
    let Event::Up(current) = event(&mut events).await else {
        panic!("up")
    };
    assert!(current.generation > old.generation + 1);
    assert_eq!(current.device, old.device);
    assert!(matches!(event(&mut events).await, Event::Ready(ref id, _) if id == &current));
    assert_eq!(handle.snapshot(), vec![current.clone()]);
    let receipt = handle.send(&current, b"{}").unwrap();
    assert!(
        matches!(mqtt::decode(&wire(&mut peer).await, 8192).unwrap(), mqtt::Packet::Publish {payload, ..} if payload == b"{}")
    );
    assert_eq!(receipt.wait().await, Delivery::Sent);
    broker.stop();
    task.await.unwrap();
    assert_eq!(
        event(&mut events).await,
        Event::Down(current, Disconnect::Closed)
    );
}
async fn wire<S: AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    timeout(Duration::from_secs(3), async {
        let mut bytes = vec![stream.read_u8().await.unwrap()];
        loop {
            if let Some(total) = mqtt::length(&bytes, 2_000_000).unwrap() {
                let header = bytes.len();
                bytes.resize(total, 0);
                stream.read_exact(&mut bytes[header..]).await.unwrap();
                return bytes;
            }
            bytes.push(stream.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap()
}
async fn event(events: &mut broadcast::Receiver<Event>) -> Event {
    timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.unwrap();
            // Wire observation is independent of the protocol sequence under test.
            if !matches!(event, Event::Sent(..)) {
                return event;
            }
        }
    })
    .await
    .unwrap()
}
async fn setup<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, did: &str, keep_alive: u16) {
    setup_with_client(stream, did, keep_alive, "x").await;
}
async fn setup_with_client<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    did: &str,
    keep_alive: u16,
    client: &str,
) {
    let mut connect = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 2];
    connect.extend_from_slice(&keep_alive.to_be_bytes());
    connect.extend_from_slice(&(client.len() as u16).to_be_bytes());
    connect.extend_from_slice(client.as_bytes());
    stream
        .write_all(&mqtt::frame(0x10, &connect, 4096).unwrap())
        .await
        .unwrap();
    assert_eq!(wire(stream).await, vec![0x20, 2, 0, 0]);
    let filter = format!("lime/devices/{did}");
    let mut body = vec![0, 1];
    body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    body.extend_from_slice(filter.as_bytes());
    body.push(0);
    stream
        .write_all(&mqtt::frame(0x82, &body, 4096).unwrap())
        .await
        .unwrap();
    assert_eq!(wire(stream).await, vec![0x90, 3, 0, 1, 0]);
}
async fn publish<S: AsyncWrite + Unpin>(stream: &mut S, topic: &str, payload: &Value) {
    stream
        .write_all(&mqtt::publish(topic, payload.to_string().as_bytes(), 2_000_000).unwrap())
        .await
        .unwrap();
}
async fn provisioning<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, did: &str) -> Value {
    publish(
        stream,
        &format!("clip/provisioning/devices/{did}"),
        &json!({"did":did,"cmd":"deploy","kind":"model","data":{"unknown":7}}),
    )
    .await;
    let mqtt::Packet::Publish { payload, .. } =
        mqtt::decode(&wire(stream).await, 2_000_000).unwrap()
    else {
        panic!("publish response")
    };
    let response: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(response["cmd"], "completeProvisioning");
    assert_eq!(response["mid"], 123456);
    assert_eq!(response["data"]["provisioningType"], "deploy");
    response
}
async fn ready<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    did: &str,
    events: &mut broadcast::Receiver<Event>,
) -> rusthinq_server::SessionId {
    setup(stream, did, 60).await;
    provisioning(stream, did).await;
    publish(
        stream,
        &format!("clip/message/devices/{did}"),
        &json!({"did":did,"cmd":"completeProvisioning_ack"}),
    )
    .await;
    let Event::Up(id) = event(events).await else {
        panic!("up")
    };
    assert!(matches!(event(events).await, Event::Ready(ref current, _) if current == &id));
    id
}
#[tokio::test]
async fn local_deploy_timesync_ack_and_raw_data_are_ordered() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (server, mut peer) = tokio::io::duplex(8192);
    let task_broker = broker.clone();
    let task = tokio::spawn(async move {
        task_broker.run(server).await.unwrap();
    });
    setup(&mut peer, "d", 60).await;
    provisioning(&mut peer, "d").await;
    assert!(handle.snapshot().is_empty());
    assert!(events.try_recv().is_err());
    publish(
        &mut peer,
        "clip/message/devices/d",
        &json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
    let Event::Up(id) = event(&mut events).await else {
        panic!("up")
    };
    assert!(matches!(event(&mut events).await, Event::Ready(_, _)));
    publish(
        &mut peer,
        "clip/message/devices/d",
        &json!({"did":"d","cmd":"req_timesync"}),
    )
    .await;
    let mqtt::Packet::Publish { payload, .. } =
        mqtt::decode(&wire(&mut peer).await, 2_000_000).unwrap()
    else {
        panic!("timesync")
    };
    let sync: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(sync["cmd"], "resp_timesync");
    assert_eq!(
        sync["data"],
        base64::engine::general_purpose::STANDARD.encode([26, 9, 1, 5, 6, 7, 4])
    );
    let payload = json!({"did":"d","cmd":"device_packet","data": rusthinq_protocol::thinq2::encode_hex(&rusthinq_protocol::aabb::wrap(&[0xf0, 1, 4]).unwrap()),"unknown":42});
    publish(&mut peer, "clip/message/devices/d", &payload).await;
    let mqtt::Packet::Publish { payload: ack, .. } =
        mqtt::decode(&wire(&mut peer).await, 2_000_000).unwrap()
    else {
        panic!("ACK")
    };
    assert_eq!(serde_json::from_slice::<Value>(&ack).unwrap()["cmd"], "ack");
    assert!(matches!(event(&mut events).await, Event::Data(ref current, _) if current == &id));
    assert_eq!(
        event(&mut events).await,
        Event::CloudBound(id.clone(), payload.to_string().into_bytes())
    );
    let command = json!({"did":"d","cmd":"packet","type":1,"data":"00"}).to_string();
    let receipt = handle.send(&id, command.as_bytes()).unwrap();
    wire(&mut peer).await;
    assert_eq!(receipt.wait().await, Delivery::Sent);
    peer.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(wire(&mut peer).await, vec![0xd0, 0]);
    broker.stop();
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(handle.snapshot().is_empty());
}
#[tokio::test]
async fn replacement_and_shutdown_fence_generations_and_partial_read_is_preserved() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (a, mut old) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let old_task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    let previous = ready(&mut old, "d", &mut events).await;
    // A pending fixed header must survive an intervening outbound command.
    old.write_all(&[0xc0]).await.unwrap();
    let receipt = handle.send(&previous, b"{}").unwrap();
    wire(&mut old).await;
    assert_eq!(receipt.wait().await, Delivery::Sent);
    old.write_all(&[0]).await.unwrap();
    assert_eq!(wire(&mut old).await, vec![0xd0, 0]);
    let (b, mut new) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let new_task = tokio::spawn(async move {
        runtime.run(b).await.unwrap();
    });
    setup(&mut new, "d", 60).await;
    provisioning(&mut new, "d").await;
    assert_eq!(
        event(&mut events).await,
        Event::Down(previous.clone(), Disconnect::Closed)
    );
    assert!(handle.snapshot().is_empty());
    assert!(matches!(
        handle.send(&previous, b"{}"),
        Err(Reject::StaleSession)
    ));
    publish(
        &mut new,
        "clip/message/devices/d",
        &json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
    let Event::Up(current) = event(&mut events).await else {
        panic!("up")
    };
    assert!(current.generation > previous.generation);
    event(&mut events).await;
    old_task.await.unwrap();
    broker.stop();
    timeout(Duration::from_secs(2), new_task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event(&mut events).await,
        Event::Down(current, Disconnect::Closed)
    );
}
#[tokio::test]
async fn failed_provisioning_and_malformed_frames_never_register() {
    let broker = Broker::new(
        Config {
            idle_timeout: Duration::from_millis(100),
            ..Config::default()
        },
        Arc::new(FixedClock),
    )
    .unwrap();
    let handle = broker.handle();
    let (a, mut peer) = tokio::io::duplex(4096);
    let runtime = broker.clone();
    let task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    peer.write_all(&[0x10, 0xff, 0xff, 0xff, 0xff])
        .await
        .unwrap();
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(handle.snapshot().is_empty());
    let (a, mut peer) = tokio::io::duplex(4096);
    let runtime = broker.clone();
    let task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    peer.write_all(&[
        0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 1, b'x',
    ])
    .await
    .unwrap();
    wire(&mut peer).await;
    publish(
        &mut peer,
        "clip/provisioning/devices/d",
        &json!({"did":"d","cmd":"deploy"}),
    )
    .await;
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(handle.snapshot().is_empty());
    let (a, _silent) = tokio::io::duplex(1);
    timeout(Duration::from_secs(2), broker.run(a))
        .await
        .unwrap()
        .unwrap();
    broker.stop();
}

#[tokio::test]
async fn tls_front_door_runs_real_thinq2_mqtt_session_without_an_external_broker() {
    use openssl::{
        ssl::{SslConnector, SslMethod},
        x509::X509,
    };
    use rusthinq_protocol::lg_compat::TlsPolicy;
    use rusthinq_server::{
        certificates::Authority,
        tls::{Config as TlsConfig, FrontDoor},
    };
    use tokio::net::{TcpListener, TcpStream};
    use tokio_openssl::SslStream;
    let ca = Authority::generate("root.example", 2048).unwrap();
    let front = FrontDoor::new(
        TlsConfig::default(),
        vec![
            ca.server_identity("local.example", TlsPolicy::Baseline)
                .unwrap(),
        ],
        None,
    )
    .unwrap();
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let mut events = broker.handle().subscribe();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let owner = tokio::spawn(front.serve_service(listener, Arc::new(broker.clone()), stopped));
    let mut client = SslConnector::builder(SslMethod::tls_client()).unwrap();
    client
        .cert_store_mut()
        .add_cert(X509::from_pem(ca.certificate_pem().as_bytes()).unwrap())
        .unwrap();
    let ssl = client
        .build()
        .configure()
        .unwrap()
        .into_ssl("local.example")
        .unwrap();
    let mut peer = SslStream::new(ssl, TcpStream::connect(address).await.unwrap()).unwrap();
    timeout(
        Duration::from_secs(3),
        std::pin::Pin::new(&mut peer).connect(),
    )
    .await
    .unwrap()
    .unwrap();
    ready(&mut peer, "tls-device", &mut events).await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), owner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    broker.stop();
    assert!(broker.handle().snapshot().is_empty());
}

#[tokio::test]
async fn full_queue_and_partial_write_cannot_delay_shutdown_or_other_devices() {
    let broker = Broker::new(
        Config {
            outbound_capacity: 1,
            write_timeout: Duration::from_secs(60),
            ..Config::default()
        },
        Arc::new(FixedClock),
    )
    .unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (a, mut stalled) = tokio::io::duplex(512);
    let runtime = broker.clone();
    let task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    let id = ready(&mut stalled, "stalled", &mut events).await;
    let payload = json!({"data":"x".repeat(4096)}).to_string();
    let first = handle.send(&id, payload.as_bytes()).unwrap();
    let mut byte = [0];
    timeout(Duration::from_secs(2), stalled.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    let queued = handle.send(&id, b"{}").unwrap();
    assert!(matches!(handle.send(&id, b"{}"), Err(Reject::Busy)));
    let (a, mut healthy) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let healthy_task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    ready(&mut healthy, "healthy", &mut events).await;
    broker.stop();
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), healthy_task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.wait().await, Delivery::Unknown);
    assert_eq!(queued.wait().await, Delivery::Failed);
}

#[tokio::test(start_paused = true)]
async fn qos1_puback_and_declared_keepalive_are_honored() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let mut events = broker.handle().subscribe();
    let (a, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    let id = ready(&mut peer, "d", &mut events).await;
    let topic = "clip/message/devices/d";
    let mut body = (topic.len() as u16).to_be_bytes().to_vec();
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&[0, 9]);
    body.extend_from_slice(br#"{"did":"d","cmd":"opaque"}"#);
    peer.write_all(&mqtt::frame(0x32, &body, 4096).unwrap())
        .await
        .unwrap();
    assert_eq!(wire(&mut peer).await, vec![0x40, 2, 0, 9]);
    assert!(matches!(event(&mut events).await, Event::CloudBound(_, _)));
    broker.stop();
    task.await.unwrap();
    event(&mut events).await;
    let (a, mut peer) = tokio::io::duplex(8192);
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let mut events = broker.handle().subscribe();
    let runtime = broker.clone();
    let task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    setup(&mut peer, "d", 1).await;
    provisioning(&mut peer, "d").await;
    publish(
        &mut peer,
        "clip/message/devices/d",
        &json!({"did":"d","cmd":"completeProvisioning_ack"}),
    )
    .await;
    let Event::Up(current) = event(&mut events).await else {
        panic!("up")
    };
    event(&mut events).await;
    let start = tokio::time::Instant::now();
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Down(current, Disconnect::IdleTimeout)
    );
    assert_eq!(start.elapsed(), Duration::from_secs(300));
    task.await.unwrap();
    broker.stop();
    assert_eq!(id.device, "d");
}

#[tokio::test]
async fn qos1_retransmission_first_duplicate_and_reused_ids_keep_session_alive() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let mut events = broker.handle().subscribe();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move { runtime.run(stream).await.unwrap() });
    let session = ready(&mut peer, "d", &mut events).await;
    let topic = "clip/message/devices/d";
    // First-seen DUP, retransmission, then packet-ID reuse with distinct content.
    for (header, sequence) in [(0x3a, 1), (0x3a, 1), (0x32, 2), (0x3a, 2)] {
        let payload = json!({"did":"d", "cmd":"opaque", "sequence":sequence}).to_string();
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&9u16.to_be_bytes());
        body.extend_from_slice(payload.as_bytes());
        peer.write_all(&mqtt::frame(header, &body, 4096).unwrap())
            .await
            .unwrap();
        assert_eq!(wire(&mut peer).await, vec![0x40, 2, 0, 9]);
        assert_eq!(
            event(&mut events).await,
            Event::CloudBound(session.clone(), payload.into_bytes())
        );
        peer.write_all(&[0xc0, 0]).await.unwrap();
        assert_eq!(wire(&mut peer).await, vec![0xd0, 0]);
    }
    assert_eq!(broker.handle().snapshot(), vec![session.clone()]);
    broker.stop();
    task.await.unwrap();
    assert_eq!(
        event(&mut events).await,
        Event::Down(session, Disconnect::Closed)
    );
}

#[tokio::test]
async fn stale_delayed_deploy_cannot_resurrect_a_closed_newer_session() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (a, mut old) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let old_task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    setup(&mut old, "d", 60).await;
    let (b, mut new) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let new_task = tokio::spawn(async move {
        runtime.run(b).await.unwrap();
    });
    let current = ready(&mut new, "d", &mut events).await;
    handle.close(&current).unwrap();
    assert_eq!(
        event(&mut events).await,
        Event::Down(current, Disconnect::Closed)
    );
    new_task.await.unwrap();
    publish(
        &mut old,
        "clip/provisioning/devices/d",
        &json!({"did":"d","cmd":"deploy"}),
    )
    .await;
    timeout(Duration::from_secs(2), old_task)
        .await
        .unwrap()
        .unwrap();
    assert!(handle.snapshot().is_empty());
    assert!(events.try_recv().is_err());
    broker.stop();
}

#[tokio::test]
async fn unsubscribe_ack_and_failed_command_leave_connection_usable() {
    let broker = Broker::new(Config::default(), Arc::new(FixedClock)).unwrap();
    let handle = broker.handle();
    let mut events = handle.subscribe();
    let (a, mut peer) = tokio::io::duplex(8192);
    let runtime = broker.clone();
    let task = tokio::spawn(async move {
        runtime.run(a).await.unwrap();
    });
    let id = ready(&mut peer, "d", &mut events).await;
    let topic = "lime/devices/d";
    let mut body = vec![0, 9];
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    peer.write_all(&mqtt::frame(0xa2, &body, 4096).unwrap())
        .await
        .unwrap();
    assert_eq!(wire(&mut peer).await, vec![0xb0, 2, 0, 9]);
    assert_eq!(
        handle.send(&id, b"{}").unwrap().wait().await,
        Delivery::Failed
    );
    peer.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(wire(&mut peer).await, vec![0xd0, 0]);
    broker.stop();
    task.await.unwrap();
}
