use rusthinq_app::lifecycle_storage::Storage;
use rusthinq_lifecycle::{Action, Input, Model, Step};
use std::time::Duration;

#[test]
fn actual_lifecycle_effect_is_saved_and_loaded_offline_with_generation_floor() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let mut model = Model::new(
        storage.state().ledger.clone(),
        Duration::ZERO,
        Duration::ZERO,
    )
    .unwrap();
    let outcome = model.input(
        Input::SessionUp {
            id: "d".into(),
            generation: 42,
        },
        Duration::ZERO,
    );
    let ledger = outcome
        .actions
        .iter()
        .find_map(|action| {
            if let Action::PersistLedger(ledger) = action {
                Some(ledger)
            } else {
                None
            }
        })
        .unwrap();
    storage.save(ledger).unwrap();
    let result = model.input(
        Input::LedgerResult {
            revision: ledger.revision,
            result: Ok(()),
        },
        Duration::ZERO,
    );
    assert!(result.error.is_none());
    drop(storage);
    let storage = Storage::open(&path, 8).unwrap();
    assert_eq!(storage.state().generation_floor, 42);
    let restored = Model::new(
        storage.state().ledger.clone(),
        Duration::ZERO,
        Duration::ZERO,
    )
    .unwrap();
    assert!(!restored.devices()[0].online);
    assert!(restored.devices()[0].session.is_none());
    assert_eq!(restored.devices()[0].entry.incarnation, 1);
}

#[test]
fn removing_all_devices_never_lowers_counters_and_stale_writes_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let first = rusthinq_lifecycle::Ledger {
        revision: 1,
        next_incarnation: 2,
        entries: vec![rusthinq_lifecycle::Entry {
            id: "d".into(),
            incarnation: 1,
            last_generation: 100,
        }],
    };
    storage.save(&first).unwrap();
    let removed = rusthinq_lifecycle::Ledger {
        revision: 2,
        next_incarnation: 2,
        entries: vec![],
    };
    storage.save(&removed).unwrap();
    assert!(storage.save(&first).is_err());
    storage.save(&removed).unwrap();
    drop(storage);
    let storage = Storage::open(&path, 8).unwrap();
    assert_eq!(storage.state().generation_floor, 100);
    assert_eq!(storage.state().ledger.next_incarnation, 2);
}

#[test]
fn lock_invalid_checkpoint_and_invalid_effect_preserve_existing_data() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 1).unwrap();
    assert!(Storage::open(&path, 1).is_err());
    storage
        .save(&rusthinq_lifecycle::Ledger::default())
        .unwrap();
    let saved = std::fs::read(&path).unwrap();
    let invalid = rusthinq_lifecycle::Ledger {
        revision: 1,
        next_incarnation: 0,
        entries: vec![],
    };
    assert!(storage.save(&invalid).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), saved);
    drop(storage);
    std::fs::write(&path, b"corrupt").unwrap();
    assert!(Storage::open(&path, 1).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"corrupt");
}

#[test]
fn failed_commit_quiesces_storage_until_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(
        storage
            .save(&rusthinq_lifecycle::Ledger::default())
            .is_err()
    );
    assert!(storage.requires_reopen());
    assert!(
        storage
            .save(&rusthinq_lifecycle::Ledger::default())
            .is_err()
    );
    assert_eq!(storage.state().generation_floor, 0);
}

#[test]
fn removal_effect_commits_before_model_reports_removed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let mut model = Model::new(
        storage.state().ledger.clone(),
        Duration::ZERO,
        Duration::ZERO,
    )
    .unwrap();
    let up = model.input(
        Input::SessionUp {
            id: "d".into(),
            generation: 9,
        },
        Duration::ZERO,
    );
    for action in &up.actions {
        if let Some(result) = storage.execute(action) {
            assert!(model.input(result, Duration::ZERO).error.is_none());
        }
    }
    let forgetting = model.input(
        Input::Forget {
            id: "d".into(),
            bridge_active: false,
        },
        Duration::ZERO,
    );
    let close = forgetting
        .actions
        .iter()
        .find_map(|action| {
            if let Action::ForgetStep {
                id,
                incarnation,
                operation,
                step: Step::Close,
                ..
            } = action
            {
                Some(Input::StepResult {
                    id: id.clone(),
                    incarnation: *incarnation,
                    operation: *operation,
                    step: Step::Close,
                    result: Ok(()),
                })
            } else {
                None
            }
        })
        .unwrap();
    let persist = model.input(close, Duration::ZERO);
    assert_eq!(model.devices().len(), 1);
    let removal = persist
        .actions
        .iter()
        .find(|action| {
            matches!(
                action,
                Action::ForgetStep {
                    step: Step::PersistRemoval,
                    ..
                }
            )
        })
        .unwrap();
    let result = storage.execute(removal).unwrap();
    assert!(storage.state().ledger.entries.is_empty());
    assert_eq!(storage.state().generation_floor, 9);
    let removed = model.input(result, Duration::ZERO);
    assert!(
        removed
            .actions
            .iter()
            .any(|action| matches!(action,Action::Removed {id,incarnation:1} if id == "d"))
    );
    drop(storage);
    let restored = Storage::open(&path, 8).unwrap();
    assert!(restored.state().ledger.entries.is_empty());
    assert_eq!(restored.state().ledger.next_incarnation, 2);
}

#[test]
fn model_metadata_is_durable_incarnation_scoped_and_removed_with_its_owner() {
    use rusthinq_app::lifecycle_storage::DeviceMetadata;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 4).unwrap();
    let mut ledger = rusthinq_lifecycle::Ledger {
        revision: 1,
        next_incarnation: 2,
        entries: vec![rusthinq_lifecycle::Entry {
            id: "d".into(),
            incarnation: 1,
            last_generation: 10,
        }],
    };
    storage.save(&ledger).unwrap();
    let record = DeviceMetadata {
        incarnation: 1,
        model_name: "D140110".into(),
        device_type: "204".into(),
        thinq2: false,
        ..Default::default()
    };
    let batch = std::collections::BTreeMap::from([("d".into(), record.clone())]);
    storage.save_metadata(&batch).unwrap();
    drop(storage);
    let mut storage = Storage::open(&path, 4).unwrap();
    assert_eq!(storage.state().metadata["d"], record);
    storage.reserve_generations(10).unwrap();
    assert_eq!(storage.state().metadata["d"], record);
    ledger.revision += 1;
    ledger.entries.clear();
    storage.save(&ledger).unwrap();
    assert!(storage.state().metadata.is_empty());
    ledger.revision += 1;
    ledger.next_incarnation = 3;
    ledger.entries.push(rusthinq_lifecycle::Entry {
        id: "d".into(),
        incarnation: 2,
        last_generation: 21,
    });
    storage.save(&ledger).unwrap();
    assert!(storage.save_metadata(&batch).is_err());
    assert!(storage.state().metadata.is_empty());
}

#[test]
fn old_lifecycle_schema_upgrades_without_inventing_models_and_invalid_metadata_is_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let original=br#"{"version":1,"generation_floor":10,"revision":1,"next_incarnation":2,"entries":[{"id":"d","incarnation":1,"last_generation":10}]}"#;
    std::fs::write(&path, original).unwrap();
    let mut storage = Storage::open(&path, 4).unwrap();
    assert!(storage.state().metadata.is_empty());
    storage.reserve_generations(1).unwrap();
    drop(storage);
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(value["version"], 2);
    value["metadata"] = serde_json::json!({"d":{"incarnation":2,"model_name":"D140110","device_type":"204","thinq2":false}});
    let corrupted = value.to_string();
    std::fs::write(&path, &corrupted).unwrap();
    assert!(Storage::open(&path, 4).is_err());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), corrupted);
}
