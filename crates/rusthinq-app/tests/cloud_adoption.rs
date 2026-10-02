#![cfg(feature = "bridge")]
use rusthinq_app::{
    cloud_devices::{self, Operation},
    lifecycle_storage::Storage,
    pairing_storage::Store,
    runtime::Runtime,
};
use rusthinq_bridge::passthrough::{HttpsConnector, Relay};
use rusthinq_lifecycle::{Entry, Ledger};
use rusthinq_server::{Server, mqtt};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
#[tokio::test]
async fn unauthenticated_or_stale_adoption_never_persists_and_shutdown_releases_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pairings.json");
    let mut storage = Storage::open(&dir.path().join("devices.json"), 4).unwrap();
    storage
        .save(&Ledger {
            revision: 1,
            next_incarnation: 2,
            entries: vec![Entry {
                id: "d".into(),
                incarnation: 1,
                last_generation: 0,
            }],
        })
        .unwrap();
    let server = Server::new(Default::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32).unwrap();
    let (account, _account_runtime) =
        rusthinq_app::cloud_account::open(dir.path().join("account.json"))
            .await
            .unwrap();
    let broker = mqtt::Broker::sharing(server.handle(), Arc::new(mqtt::SystemClock));
    let relay = Relay::new(Default::default(), Arc::new(HttpsConnector)).unwrap();
    let (handle, cloud, registry) = cloud_devices::Runtime::open(
        path.clone(),
        account.clone(),
        runtime.handle(),
        server.handle(),
        broker.handle(),
        relay.clone(),
    )
    .await
    .unwrap();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(cloud.run(stopped));
    for incarnation in [1, 2] {
        let operation = Operation::Adopt {
            device: "d".into(),
            incarnation,
            archive: json!({"httpServer":"https://api.example","rtiServer":"cloud.example:5222"}),
        };
        let error = tokio::time::timeout(Duration::from_secs(3), handle.operate(operation))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
        assert!(!path.exists());
        assert!(handle.snapshot().is_empty());
        assert!(registry.snapshot().is_empty());
    }
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut imported = Store::open(&path, 4).unwrap();
    imported
        .adopt(
            rusthinq_app::pairing_storage::Owner {
                device: "d".into(),
                incarnation: 1,
                account: "saved-owner".into(),
            },
            rusthinq_bridge::pairing::Material::ThinQ1 {
                http_server: "https://api.example".into(),
                rti_server: "cloud.example:5222".into(),
            },
        )
        .unwrap();
    drop(imported);
    let (restored, cloud, registry) = cloud_devices::Runtime::open(
        path.clone(),
        account,
        runtime.handle(),
        server.handle(),
        broker.handle(),
        relay,
    )
    .await
    .unwrap();
    assert!(restored.snapshot()[0].paired);
    assert!(!restored.snapshot()[0].enabled);
    assert!(!restored.snapshot()[0].connected);
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(cloud.run(stopped));
    assert!(
        restored
            .operate(Operation::Enable {
                device: "d".into(),
                incarnation: 1,
                enabled: true
            })
            .await
            .is_err()
    );
    restored
        .operate(Operation::Enable {
            device: "d".into(),
            incarnation: 1,
            enabled: false,
        })
        .await
        .unwrap();
    assert_eq!(registry.snapshot().len(), 1);
    assert!(!registry.snapshot()[0].enabled);
    assert!(registry.snapshot()[0].local.is_none());
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!Store::open(&path, 4).unwrap().snapshot()[0].enabled);
}
