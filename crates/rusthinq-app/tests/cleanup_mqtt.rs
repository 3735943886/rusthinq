use rusthinq_app::{cleanup_mqtt::Session, retained_cleanup::Ledger};
use rusthinq_server::retained::Tombstone;
use std::{io, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

#[tokio::test]
async fn inventory_admission_failure_sends_no_retained_value_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let mut ledger = Ledger::open(&path, 1).unwrap();
    ledger
        .enqueue(&[Tombstone {
            owner: "old".into(),
            topic: "occupied".into(),
        }])
        .unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        packet(&mut peer).await;
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        peer
    });
    let mut session = Session::connect(stream, "retained", Duration::from_secs(2))
        .await
        .unwrap();
    let mut peer = remote.await.unwrap();
    assert!(
        session
            .publish_owned_retained(ledger, "new".into(), "untracked".into(), b"value")
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.read_u8())
            .await
            .is_err()
    );
    assert_eq!(
        Ledger::open(&path, 1).unwrap().pending(),
        vec![Tombstone {
            owner: "old".into(),
            topic: "occupied".into()
        }]
    );
}

#[tokio::test]
async fn retained_values_always_leave_durable_inventory_and_owner_cleanup_is_isolated() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let ledger = Ledger::open(&path, 4).unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let checkpoint = path.clone();
    let remote = tokio::spawn(async move {
        packet(&mut peer).await;
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        for (id, topic, payload) in [
            (1, b'a', b"value".as_slice()),
            (2, b'b', b"other".as_slice()),
            (3, b'a', b"".as_slice()),
        ] {
            let mut expected = vec![0x33, (5 + payload.len()) as u8, 0, 1, topic, 0, id];
            expected.extend_from_slice(payload);
            assert_eq!(packet(&mut peer).await, expected);
            // File already inventories each publication before its first network bytes.
            let bytes = std::fs::read(&checkpoint).unwrap();
            assert!(
                String::from_utf8(bytes)
                    .unwrap()
                    .contains(&format!("\"topic\":\"{}\"", char::from(topic)))
            );
            peer.write_all(&[0x40, 2, 0, id]).await.unwrap();
        }
    });
    let mut session = Session::connect(stream, "retained", Duration::from_secs(2))
        .await
        .unwrap();
    let ledger = session
        .publish_owned_retained(ledger, "first".into(), "a".into(), b"value")
        .await
        .unwrap();
    let ledger = session
        .publish_owned_retained(ledger, "second".into(), "b".into(), b"other")
        .await
        .unwrap();
    assert_eq!(ledger.pending().len(), 2);
    let (ledger, count) = session.remove_owner(ledger, "first").await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        ledger.pending(),
        vec![Tombstone {
            owner: "second".into(),
            topic: "b".into()
        }]
    );
    drop(ledger);
    assert_eq!(Ledger::open(&path, 4).unwrap().pending().len(), 1);
    remote.await.unwrap();
}

#[tokio::test]
async fn unconfirmed_retained_value_keeps_its_removal_route_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let ledger = Ledger::open(&path, 4).unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        packet(&mut peer).await;
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        assert_eq!(packet(&mut peer).await[0], 0x33);
        // Connection loss after accepting bytes leaves publication outcome unknown.
    });
    let mut session = Session::connect(stream, "retained", Duration::from_secs(2))
        .await
        .unwrap();
    assert!(
        session
            .publish_owned_retained(ledger, "d".into(), "t".into(), b"value")
            .await
            .is_err()
    );
    remote.await.unwrap();
    assert_eq!(
        Ledger::open(&path, 4).unwrap().pending(),
        vec![Tombstone {
            owner: "d".into(),
            topic: "t".into()
        }]
    );
}

async fn packet(peer: &mut DuplexStream) -> Vec<u8> {
    let mut packet = vec![peer.read_u8().await.unwrap()];
    let mut multiplier = 1usize;
    let mut length = 0;
    loop {
        let byte = peer.read_u8().await.unwrap();
        packet.push(byte);
        length += usize::from(byte & 127) * multiplier;
        if byte & 128 == 0 {
            break;
        }
        multiplier *= 128;
    }
    let offset = packet.len();
    packet.resize(offset + length, 0);
    peer.read_exact(&mut packet[offset..]).await.unwrap();
    packet
}

#[tokio::test]
async fn drain_recovers_only_unconfirmed_work_after_partial_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let mut ledger = Ledger::open(&path, 4).unwrap();
    ledger
        .enqueue(&["a", "b", "c"].map(|topic| Tombstone {
            owner: "d".into(),
            topic: topic.into(),
        }))
        .unwrap();
    drop(ledger);
    let ledger = Ledger::open(&path, 4).unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        packet(&mut peer).await;
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        assert_eq!(packet(&mut peer).await, [0x33, 5, 0, 1, b'a', 0, 1]);
        peer.write_all(&[0x40, 2, 0, 1]).await.unwrap();
        assert_eq!(packet(&mut peer).await, [0x33, 5, 0, 1, b'b', 0, 2]);
        // Drop before acknowledgement; c must never be sent on this connection.
    });
    let mut session = Session::connect(stream, "cleanup", Duration::from_secs(2))
        .await
        .unwrap();
    assert!(session.drain(ledger).await.is_err());
    remote.await.unwrap();
    let ledger = Ledger::open(&path, 4).unwrap();
    assert_eq!(
        ledger
            .pending()
            .iter()
            .map(|d| d.topic.as_str())
            .collect::<Vec<_>>(),
        ["b", "c"]
    );
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        packet(&mut peer).await;
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        for (topic, id) in [(b'b', 1), (b'c', 2)] {
            assert_eq!(packet(&mut peer).await, [0x33, 5, 0, 1, topic, 0, id]);
            peer.write_all(&[0x40, 2, 0, id]).await.unwrap();
        }
    });
    let mut session = Session::connect(stream, "cleanup", Duration::from_secs(2))
        .await
        .unwrap();
    let (ledger, completed) = session.drain(ledger).await.unwrap();
    assert_eq!(completed, 2);
    assert!(ledger.pending().is_empty());
    drop(ledger);
    assert!(Ledger::open(&path, 4).unwrap().pending().is_empty());
    remote.await.unwrap();
}

#[tokio::test]
async fn matching_puback_removes_durable_deletion() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let mut ledger = Ledger::open(&path, 4).unwrap();
    ledger
        .enqueue(&[Tombstone {
            owner: "d".into(),
            topic: "t".into(),
        }])
        .unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        assert_eq!(packet(&mut peer).await[0], 0x10);
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        assert_eq!(packet(&mut peer).await, [0x33, 5, 0, 1, b't', 0, 1]);
        peer.write_all(&[0x40, 2, 0, 1]).await.unwrap();
    });
    let mut session = Session::connect(stream, "cleanup", Duration::from_secs(2))
        .await
        .unwrap();
    let ledger = session.delete_one(ledger, "t".into()).await.unwrap();
    assert!(ledger.pending().is_empty());
    drop(ledger);
    assert!(Ledger::open(&path, 4).unwrap().pending().is_empty());
    remote.await.unwrap();
}

#[tokio::test]
async fn wrong_puback_and_no_ack_leave_recoverable_work() {
    for wrong_ack in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cleanup.json");
        let mut ledger = Ledger::open(&path, 4).unwrap();
        ledger
            .enqueue(&[Tombstone {
                owner: "d".into(),
                topic: "t".into(),
            }])
            .unwrap();
        let (stream, mut peer) = tokio::io::duplex(8192);
        let remote = tokio::spawn(async move {
            packet(&mut peer).await;
            peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            packet(&mut peer).await;
            if wrong_ack {
                peer.write_all(&[0x40, 2, 0, 2]).await.unwrap();
            }
            peer
        });
        let mut session = Session::connect(stream, "cleanup", Duration::from_millis(100))
            .await
            .unwrap();
        let peer = remote;
        // Keep the remote stream alive during the timeout through its completed task result.
        let result = session.delete_one(ledger, "t".into());
        let (result, remote) = tokio::join!(result, peer);
        let _remote = remote.unwrap();
        let error = result
            .err()
            .expect("must not complete without matching ack");
        assert_eq!(
            error.kind(),
            if wrong_ack {
                io::ErrorKind::InvalidData
            } else {
                io::ErrorKind::TimedOut
            }
        );
        let ledger = Ledger::open(&path, 4).unwrap();
        assert_eq!(ledger.pending().len(), 1);
        assert_eq!(
            session
                .delete_one(ledger, "t".into())
                .await
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(Ledger::open(&path, 4).unwrap().pending().len(), 1);
    }
}
