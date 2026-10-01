use rusthinq_bridge::devices::{BridgeHandle, Error};
use rusthinq_protocol::thinq1;
use rusthinq_server::{Config, Delivery, Server, SessionId};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

fn local(generation: u64) -> SessionId {
    SessionId {
        device: "d".into(),
        generation,
    }
}

#[test]
fn registration_survives_disable_reenable_and_local_replacement() {
    let bridge = BridgeHandle::new(2, 8192, 16).unwrap();
    let clone = bridge.clone();
    let registration = bridge.registered("d".into(), 1).unwrap();
    bridge.bind(&registration, local(1)).unwrap();
    bridge.bind(&registration, local(2)).unwrap();
    assert_eq!(bridge.unbind(&registration, &local(1)), Err(Error::Stale));
    bridge.disable(&registration).unwrap();
    assert!(!clone.snapshot()[0].enabled);
    assert_eq!(clone.registered("d".into(), 1).unwrap(), registration);
    assert_eq!(bridge.snapshot()[0].local, Some(local(2)));
    bridge.unbind(&registration, &local(2)).unwrap();
    assert_eq!(bridge.bind(&registration, local(1)), Err(Error::Stale));
    assert_eq!(bridge.bind(&registration, local(2)), Err(Error::Stale));
    bridge.bind(&registration, local(3)).unwrap();
    assert_eq!(bridge.snapshot()[0].registration, registration);
}

#[test]
fn bounded_registration_and_late_deregistration_cannot_remove_successor() {
    assert!(matches!(
        BridgeHandle::new(0, 10, 1),
        Err(Error::InvalidConfig)
    ));
    let bridge = BridgeHandle::new(1, 8192, 1).unwrap();
    let mut events = bridge.subscribe();
    assert_eq!(
        bridge.registered("d".into(), 0),
        Err(Error::InvalidIdentity)
    );
    let first = bridge.registered("d".into(), 1).unwrap();
    assert_eq!(bridge.registered("other".into(), 1), Err(Error::Capacity));
    assert_eq!(
        bridge.registered("d".into(), 2),
        Err(Error::IncarnationConflict)
    );
    bridge.disable(&first).unwrap();
    assert_eq!(bridge.snapshot().len(), 1); // disable keeps upstream registration
    bridge.deregistered(&first).unwrap();
    let second = bridge.registered("d".into(), 2).unwrap();
    assert!(second.generation > first.generation);
    assert_eq!(bridge.deregistered(&first), Err(Error::Stale));
    assert_eq!(bridge.disable(&first), Err(Error::Stale));
    assert_eq!(bridge.snapshot()[0].registration, second);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
}

async fn identify(server: &mut Server) -> (tokio::io::DuplexStream, SessionId) {
    let (stream, mut peer) = tokio::io::duplex(8192);
    let generation = server.admit(stream).unwrap();
    let payload = br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#;
    peer.write_all(&thinq1::encode(payload, 8192).unwrap())
        .await
        .unwrap();
    let length = timeout(Duration::from_secs(2), peer.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut ack = vec![0; length as usize];
    peer.read_exact(&mut ack).await.unwrap();
    (peer, local(generation))
}

#[tokio::test]
async fn downlink_preserves_bytes_and_rejects_old_disabled_offline_or_invalid_targets() {
    let mut server = Server::new(Config::default()).unwrap();
    let bridge = BridgeHandle::new(2, 8192, 16).unwrap();
    let registration = bridge.registered("d".into(), 1).unwrap();
    let (_old_peer, old) = identify(&mut server).await;
    bridge.bind(&registration, old.clone()).unwrap();
    let (mut peer, new) = identify(&mut server).await;
    bridge.bind(&registration, new.clone()).unwrap();
    let payload =
        br#"{ "Header":{"x-lgedm-deviceId":"d"}, "Body":{"Cmd":"Control","Unknown":42} }"#;
    assert!(matches!(
        bridge.downlink(&registration, &old, payload, &server.handle(), None),
        Err(Error::Stale)
    ));
    bridge.disable(&registration).unwrap();
    assert!(matches!(
        bridge.downlink(&registration, &new, payload, &server.handle(), None),
        Err(Error::Disabled)
    ));
    bridge.registered("d".into(), 1).unwrap();
    for invalid in [
        b"[]".as_slice(),
        br#"{"Header":{"x-lgedm-deviceId":"other"}}"#,
        b"broken",
    ] {
        assert!(matches!(
            bridge.downlink(&registration, &new, invalid, &server.handle(), None),
            Err(Error::InvalidPayload)
        ));
    }
    let receipt = bridge
        .downlink(&registration, &new, payload, &server.handle(), None)
        .unwrap();
    let length = timeout(Duration::from_secs(2), peer.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut delivered = vec![0; length as usize];
    peer.read_exact(&mut delivered).await.unwrap();
    assert_eq!(delivered, payload);
    assert_eq!(receipt.wait().await, Delivery::Sent);
    server.handle().close_and_wait(&new).await.unwrap();
    assert!(matches!(
        bridge.downlink(&registration, &new, payload, &server.handle(), None),
        Err(Error::Offline)
    ));
    server.shutdown().await;
}

#[tokio::test]
async fn firmware_learning_requires_current_enabled_cloud_context() {
    use rusthinq_bridge::passthrough::{Config as RelayConfig, HttpsConnector, Relay};
    let relay = Relay::new(
        RelayConfig {
            host_capacity: 1,
            ..RelayConfig::default()
        },
        std::sync::Arc::new(HttpsConnector),
    )
    .unwrap();
    let bridge = BridgeHandle::new(1, 8192, 16).unwrap();
    let registration = bridge.registered("d".into(), 1).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (mut peer, local) = identify(&mut server).await;
    bridge.bind(&registration, local.clone()).unwrap();
    let rejected = br#"{"Body":{"url":"https://rejected.example/fw"}}"#;
    let accepted = br#"{"Body":{"url":"https://accepted.example/fw"}}"#;
    bridge.disable(&registration).unwrap();
    assert!(matches!(
        bridge.downlink(
            &registration,
            &local,
            rejected,
            &server.handle(),
            Some(&relay)
        ),
        Err(Error::Disabled)
    ));
    bridge.registered("d".into(), 1).unwrap();
    let receipt = bridge
        .downlink(
            &registration,
            &local,
            accepted,
            &server.handle(),
            Some(&relay),
        )
        .unwrap();
    let length = timeout(Duration::from_secs(2), peer.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![0; length as usize];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, accepted);
    assert_eq!(receipt.wait().await, Delivery::Sent);
    assert!(matches!(
        bridge.downlink(
            &registration,
            &local,
            rejected,
            &server.handle(),
            Some(&relay)
        ),
        Err(Error::HostEvidence(_))
    ));
    assert!(
        timeout(Duration::from_millis(20), peer.read_u8())
            .await
            .is_err()
    );
    server.shutdown().await;
}
