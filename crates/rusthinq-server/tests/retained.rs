use rusthinq_server::retained::{Cleanup, Tombstone};
use rusthinq_server::retained::{Error, Store};
fn deletion(topic: &str) -> Tombstone {
    Tombstone {
        owner: "d".into(),
        topic: topic.into(),
    }
}

#[test]
fn cleanup_failure_retry_and_late_results_do_not_lose_deletions() {
    let mut ledger = Cleanup::new(4).unwrap();
    ledger.enqueue(&[deletion("t")]).unwrap();
    let first = ledger.begin("t").unwrap();
    assert!(ledger.complete(&first, false));
    let second = ledger.begin("t").unwrap();
    assert!(!ledger.complete(&first, true));
    ledger.enqueue(&[deletion("t")]).unwrap();
    assert!(!ledger.complete(&second, true));
    let current = ledger.begin("t").unwrap();
    assert!(ledger.complete(&current, true));
    assert!(ledger.pending().is_empty());
}

#[test]
fn recovery_keeps_pending_work_but_invalidates_pre_restart_attempts() {
    let mut ledger = Cleanup::new(4).unwrap();
    ledger.enqueue(&[deletion("t")]).unwrap();
    let old = ledger.begin("t").unwrap();
    let mut restored = Cleanup::restore(4, &ledger.checkpoint()).unwrap();
    assert!(!restored.complete(&old, true));
    let next = restored.begin("t").unwrap();
    assert!(next.token > old.token);
    assert!(!restored.complete(&old, true));
    assert!(restored.complete(&next, true));
}

#[test]
fn cleanup_capacity_and_invalid_recovery_are_explicit() {
    let mut ledger = Cleanup::new(1).unwrap();
    assert_eq!(
        ledger.enqueue(&[deletion("a"), deletion("b")]),
        Err(Error::Capacity)
    );
    assert!(ledger.pending().is_empty());
    assert!(Cleanup::restore(1, br#"{"version":2,"next":0,"pending":[]}"#).is_err());
    assert!(
        Cleanup::restore(
            1,
            br##"{"version":1,"next":0,"pending":[{"owner":"d","topic":"#"}]}"##
        )
        .is_err()
    );
}

#[test]
fn every_owner_has_single_and_bulk_removal_routes() {
    let mut store = Store::new(4, 1024).unwrap();
    store.put("a", "lime/a", b"one").unwrap();
    store.put("a", "lime/a/config", b"two").unwrap();
    store.put("b", "lime/b", b"three").unwrap();
    let deletion = store.remove("a", "lime/a").unwrap().unwrap();
    assert_eq!(
        deletion.frame(1024).unwrap(),
        [0x31, 8, 0, 6, b'l', b'i', b'm', b'e', b'/', b'a']
    );
    assert_eq!(store.remove_owner("a").len(), 1);
    assert_eq!(store.snapshot().len(), 1);
    assert_eq!(store.clear().len(), 1);
    assert_eq!(store.bytes(), 0);
}

#[test]
fn empty_payload_deletes_and_unknown_topic_still_gets_remote_tombstone() {
    let mut store = Store::new(1, 100).unwrap();
    store.put("a", "t", b"x").unwrap();
    assert!(store.put("a", "t", b"").unwrap().is_some());
    assert!(store.snapshot().is_empty());
    assert_eq!(store.bytes(), 0);
    assert!(store.put("a", "unknown", b"").unwrap().is_some());
}

#[test]
fn ownership_replay_and_conflicts_are_explicit() {
    let mut store = Store::new(3, 1024).unwrap();
    store.put("a", "t/a", b"x").unwrap();
    store.put("b", "t/b", b"y").unwrap();
    assert_eq!(store.put("b", "t/a", b"z"), Err(Error::OwnerConflict));
    assert_eq!(store.remove("b", "t/a"), Err(Error::OwnerConflict));
    let replay = store.replay("a", &["#".into(), "t/#".into()]);
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].payload, b"x");
}

#[test]
fn storage_limits_include_metadata_and_replacement_is_atomic() {
    let mut store = Store::new(1, 4).unwrap();
    store.put("a", "t", b"xx").unwrap();
    assert_eq!(store.bytes(), 4);
    assert_eq!(store.put("a", "t", b"xxx"), Err(Error::Capacity));
    assert_eq!(store.put("a", "s", b"x"), Err(Error::Capacity));
    assert_eq!(store.snapshot()[0].payload, b"xx");
    store.put("a", "t", b"x").unwrap();
    assert_eq!(store.bytes(), 3);
    assert_eq!(store.put("a", "#", b"x"), Err(Error::Invalid));
}
