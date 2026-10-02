use rusthinq_tools::{Client, capture_event, decode, encode, mcp, read_capture};
use serde_json::json;
#[test]
fn management_endpoints_and_offline_bounds() {
    assert!(Client::new("http://example.com/", None, None).is_err());
    assert!(Client::new("http://localhost:8080/", None, None).is_ok());
    assert!(Client::new("https://user:secret@example.com/", None, None).is_err());
    assert!(Client::new("https://example.com/?token=secret", None, None).is_err());
    assert!(encode(&json!({"protocol":"tlv","tlv":[{"t":1024,"v":1}]})).is_err());
    assert!(encode(&json!({"protocol":"tlv","tlv":[{"t":1,"v":16777216}]})).is_err());
    assert_eq!(decode(&json!({"hex":"zz"})).unwrap()["protocol"], "Unknown");
}
#[test]
fn legacy_capture_and_actual_send_observation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capture.jsonl");
    std::fs::write(&path,"{\"k\":\"note\",\"text\":\"old\"}\n{\"k\":\"rx\",\"hex\":\"aabb\"}\n{\"k\":\"tx\",\"type\":\"packet\",\"hex\":\"0011\"}\n").unwrap();
    let result = read_capture(&json!({"path":path,"type":"packet","offset":1})).unwrap();
    assert_eq!(result["events"].as_array().unwrap().len(), 1);
    assert_eq!(result["events"][0]["hex"], "0011");
    let event = capture_event(&json!({"type":"sent","device":"d","hex":"0011"}), "d").unwrap();
    assert_eq!(event["k"], "tx");
    assert!(capture_event(&json!({"type":"sent","device":"other","hex":"0011"}), "d").is_none());
}
#[tokio::test]
async fn mcp_notifications_offline_tools_and_explicit_injection() {
    let mut client = None;
    assert!(
        mcp::dispatch(
            &mut client,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
        )
        .await
        .is_none()
    );
    let reply=mcp::dispatch(&mut client,json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"inject","arguments":{"device_id":"d","hex":"00","inject_ok":false}}})).await.unwrap();
    assert_eq!(reply["result"]["isError"], true);
    let reply = mcp::dispatch(
        &mut client,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
    )
    .await
    .unwrap();
    assert!(
        reply["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["name"] == "read_capture")
    );
}

#[test]
fn migration_stages_preserves_secrets_and_refuses_unsupported_cutover() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("old.toml");
    let state = dir.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let original = "hostname='rusthinq.lan'\nhttps_port=443\nmqtts_port=8883\nthinq1_port=47878\n[bridge]\nstorage_path='state'\n";
    std::fs::write(&source, original).unwrap();
    let credential = "{\"refreshToken\":\"private-secret\",\"env\":{\"countryCode\":\"KR\"}}";
    std::fs::write(state.join("oauth2.json"), credential).unwrap();
    std::fs::write(dir.path().join("mqtt_state.json"), br#"{"d":["power"]}"#).unwrap();
    std::fs::write(
        dir.path().join("known_devices.json"),
        br#"{"d":{"modelId":"m","modelName":"D140110","platform":"thinq1","last_seen_unix":123}}"#,
    )
    .unwrap();
    let dest = dir.path().join("new");
    let report = rusthinq_tools::migration::migrate(&source, &dest).unwrap();
    assert_eq!(report["requiresReview"], true);
    assert_eq!(report["retainedTopics"], 1);
    assert_eq!(report["migratedDevices"], 1);
    let imported =
        rusthinq_app::lifecycle_storage::Storage::open(&dest.join("devices.json"), 4096).unwrap();
    assert_eq!(imported.state().metadata["d"].model_name, "D140110");
    let retained = std::fs::read(dest.join("retained-import.json")).unwrap();
    assert!(
        rusthinq_server::retained::Cleanup::restore(16384, &retained)
            .unwrap()
            .requested()
            .is_empty()
    );
    assert!(!report.to_string().contains("private-secret"));
    assert_eq!(std::fs::read_to_string(&source).unwrap(), original);
    assert_eq!(
        std::fs::read_to_string(state.join("oauth2.json")).unwrap(),
        credential
    );
    let account: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dest.join("account.json")).unwrap()).unwrap();
    assert_eq!(account["credentials"]["refresh"], "private-secret");
    assert!(rusthinq_tools::migration::migrate(&source, &dest).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(dest.join("account.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    std::fs::write(
        &source,
        "hostname='rusthinq.lan'\nhttps_port={bind=4433,advertise=443}\n",
    )
    .unwrap();
    assert!(rusthinq_tools::migration::migrate(&source, &dir.path().join("bad")).is_err());
    assert!(!dir.path().join("bad").exists());
}

#[test]
fn legacy_retained_inventory_preserves_exact_topics_and_requires_explicit_cleanup() {
    use rusthinq_server::retained::Cleanup;
    let old = br#"{"d":["temperature","power"],"other":["power"]}"#;
    let current=br#"{"d":{"properties":["power","temperature"],"last_seen_unix":123},"other":{"properties":["power"],"last_seen_unix":0}}"#;
    let plan = rusthinq_tools::migration::legacy_retained(old, "home/lg").unwrap();
    assert_eq!(
        plan,
        rusthinq_tools::migration::legacy_retained(current, "home/lg").unwrap()
    );
    let bytes = serde_json::to_vec(&plan).unwrap();
    let mut ledger = Cleanup::restore(16384, &bytes).unwrap();
    assert_eq!(ledger.pending().len(), 3);
    assert!(ledger.requested().is_empty());
    assert_eq!(ledger.pending()[0].topic, "home/lg/d/power");
    assert!(
        ledger
            .pending()
            .iter()
            .all(|item| item.owner.starts_with("legacy/0.1:"))
    );
    ledger.request_delete(&ledger.pending()).unwrap();
    let mut restarted = Cleanup::restore(16384, &ledger.checkpoint()).unwrap();
    assert_eq!(restarted.requested().len(), 3);
    let attempt = restarted.begin("home/lg/d/power").unwrap();
    assert!(restarted.complete(&attempt, false));
    assert_eq!(restarted.pending().len(), 3);
    assert_eq!(restarted.requested().len(), 3);
    let attempt = restarted.begin("home/lg/d/power").unwrap();
    assert!(restarted.complete(&attempt, true));
    assert_eq!(restarted.pending().len(), 2);
    assert!(rusthinq_tools::migration::legacy_retained(br##"{"d":["#"]}"##, "home/lg").is_err());
    assert!(rusthinq_tools::migration::legacy_retained(old, "home/+").is_err());
}

#[test]
fn legacy_known_devices_open_in_production_storage_and_survive_restart() {
    use rusthinq_app::lifecycle_storage::Storage;
    let legacy=br#"{"d":{"modelId":"m","modelName":"D140110","deviceType":"201","platform":"thinq1","last_seen_unix":123},"orphan":{"modelId":"","modelName":"","platform":"","last_seen_unix":0}}"#;
    let ledger = rusthinq_tools::migration::legacy_devices(legacy).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("devices.json");
    std::fs::write(&path, serde_json::to_vec(&ledger).unwrap()).unwrap();
    let mut store = Storage::open(&path, 4096).unwrap();
    assert_eq!(store.state().ledger.entries.len(), 2);
    assert_eq!(store.state().metadata["d"].model_name, "D140110");
    assert!(!store.state().metadata["d"].thinq2);
    assert!(!store.state().metadata.contains_key("orphan"));
    let block = store.reserve_generations(16).unwrap();
    assert_eq!(block.floor, 0);
    drop(store);
    let mut restarted = Storage::open(&path, 4096).unwrap();
    assert_eq!(restarted.state().ledger.entries, store_entries(&ledger));
    assert_eq!(restarted.reserve_generations(16).unwrap().floor, 16);
    assert!(
        rusthinq_tools::migration::legacy_devices(
            br#"{"d":{"modelName":"x","platform":"unknown"}}"#
        )
        .is_err()
    );
}
fn store_entries(value: &serde_json::Value) -> Vec<rusthinq_lifecycle::Entry> {
    value["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| rusthinq_lifecycle::Entry {
            id: e["id"].as_str().unwrap().into(),
            incarnation: e["incarnation"].as_u64().unwrap(),
            last_generation: 0,
        })
        .collect()
}
