use rusthinq_app::{
    cleanup_mqtt::Session,
    lifecycle_cleanup::{self, Outcome},
    retained_cleanup::Ledger,
};
use rusthinq_lifecycle::Action;
use rusthinq_server::retained::Tombstone;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn packet(peer: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let first = peer.read_u8().await.unwrap();
    let length_byte = peer.read_u8().await.unwrap();
    let length = usize::from(length_byte);
    let mut result = vec![first, length_byte];
    let offset = result.len();
    result.resize(offset + length, 0);
    peer.read_exact(&mut result[offset..]).await.unwrap();
    result
}

#[tokio::test]
async fn removed_cleans_only_matching_incarnation_after_durable_action() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let old = lifecycle_cleanup::device_owner("dev", 7).unwrap();
    let new = lifecycle_cleanup::device_owner("dev", 8).unwrap();
    let mut ledger = Ledger::open(&path, 8).unwrap();
    ledger
        .enqueue(&[
            Tombstone {
                owner: old.clone(),
                topic: "device/dev/old/a".into(),
            },
            Tombstone {
                owner: old.clone(),
                topic: "device/dev/old/b".into(),
            },
            Tombstone {
                owner: new.clone(),
                topic: "device/dev/new".into(),
            },
        ])
        .unwrap();
    let (stream, mut peer) = tokio::io::duplex(8192);
    let remote = tokio::spawn(async move {
        assert_eq!(packet(&mut peer).await[0], 0x10);
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        for id in 1..=2 {
            let publish = packet(&mut peer).await;
            assert_eq!(publish[0], 0x33);
            assert_eq!(&publish[publish.len() - 2..], &[0, id]);
            peer.write_all(&[0x40, 2, 0, id]).await.unwrap();
        }
    });
    let mut session = Session::connect(stream, "cleanup", Duration::from_secs(2))
        .await
        .unwrap();
    let ignored = Action::Offline {
        id: "dev".into(),
        incarnation: 7,
    };
    let (ledger, outcome) = lifecycle_cleanup::apply(&mut session, ledger, &ignored)
        .await
        .unwrap();
    assert_eq!(outcome, Outcome::Ignored);
    assert_eq!(ledger.pending().len(), 3);
    let removed = Action::Removed {
        id: "dev".into(),
        incarnation: 7,
    };
    let (ledger, outcome) = lifecycle_cleanup::apply(&mut session, ledger, &removed)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Outcome::Removed {
            owner: old,
            topics: 2
        }
    );
    assert_eq!(
        ledger.pending(),
        vec![Tombstone {
            owner: new,
            topic: "device/dev/new".into()
        }]
    );
    remote.await.unwrap();
}

#[test]
fn owner_keys_are_unambiguous_and_bounded() {
    assert_ne!(
        lifecycle_cleanup::device_owner("a/b", 1).unwrap(),
        lifecycle_cleanup::device_owner("a", 1).unwrap()
    );
    assert!(lifecycle_cleanup::device_owner("", 1).is_err());
    assert!(lifecycle_cleanup::device_owner(&"x".repeat(221), 1).is_err());
}
