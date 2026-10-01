use rusthinq_app::retained_cleanup::Ledger;
use rusthinq_server::retained::Tombstone;
fn deletion() -> Tombstone {
    Tombstone {
        owner: "d".into(),
        topic: "home/d/config".into(),
    }
}

#[test]
fn restart_recovers_deletions_and_fences_old_attempts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let mut ledger = Ledger::open(&path, 4).unwrap();
    ledger.enqueue(&[deletion()]).unwrap();
    let old = ledger.begin(&deletion().topic).unwrap();
    drop(ledger);
    let mut ledger = Ledger::open(&path, 4).unwrap();
    assert_eq!(ledger.pending(), vec![deletion()]);
    assert!(!ledger.complete(&old, true).unwrap());
    let current = ledger.begin(&deletion().topic).unwrap();
    assert!(current.token > old.token);
    assert!(ledger.complete(&current, false).unwrap());
    let retry = ledger.begin(&deletion().topic).unwrap();
    assert!(ledger.complete(&retry, true).unwrap());
    drop(ledger);
    assert!(Ledger::open(&path, 4).unwrap().pending().is_empty());
}

#[test]
fn concurrent_owner_is_rejected_and_invalid_data_is_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let ledger = Ledger::open(&path, 4).unwrap();
    assert!(Ledger::open(&path, 4).is_err());
    drop(ledger);
    std::fs::write(&path, b"invalid checkpoint").unwrap();
    assert!(Ledger::open(&path, 4).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"invalid checkpoint");
}

#[test]
fn save_failure_blocks_send_tokens_and_preserves_pending_memory() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cleanup.json");
    let mut ledger = Ledger::open(&path, 4).unwrap();
    ledger.enqueue(&[deletion()]).unwrap();
    let saved = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(ledger.begin(&deletion().topic).is_err());
    assert!(ledger.requires_reopen());
    assert_eq!(ledger.pending(), vec![deletion()]);
    assert!(ledger.begin(&deletion().topic).is_err());
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, saved).unwrap();
    drop(ledger);
    assert_eq!(Ledger::open(&path, 4).unwrap().pending(), vec![deletion()]);
}
