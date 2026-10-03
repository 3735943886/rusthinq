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
    // Settings 0.1 itself rejects are still refused, and nothing is staged.
    for bad in [
        "https_port={bind=4433,advertise=true}\n",
        "https_port={bind=4433,unknown=1}\n",
        "[mqtt]\nmqtt_url='ws://broker'\nrusthinq_prefix='p'\n",
        "[bridge]\nstorage_path='state'\ndns=['system']\n",
    ] {
        std::fs::write(&source, format!("hostname='rusthinq.lan'\n{bad}")).unwrap();
        assert!(
            rusthinq_tools::migration::migrate(&source, &dir.path().join("bad")).is_err(),
            "{bad}"
        );
        assert!(!dir.path().join("bad").exists());
    }
}

/// Every 0.1 setting with a 0.2 equivalent is carried over, so nothing a working 0.1
/// installation uses is refused or silently dropped.
#[test]
fn every_legacy_listener_proxy_mqtt_gui_and_dns_setting_carries_over() {
    use rusthinq_app::daemon::Advertise;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("state")).unwrap();
    let source = dir.path().join("config.toml");
    std::fs::write(
        &source,
        "hostname='rusthinq.lan'\nadvertise_requested_host=true\ncustom_root_cert_file='root.cert'\n\
         https_port={bind=4433,address='192.168.0.111',advertise=443}\n\
         mqtts_port={advertise='ssl://proxy.example:8883'}\nhttp_port={bind=80,address='127.0.0.1'}\n\
         thinq1_https_port=46031\nthinq1_port={bind=47879,advertise=1}\n\
         [mqtt]\nmqtt_url='mqtts://broker.lan'\nmqtt_user='u'\nmqtt_pass='p'\nrusthinq_prefix='home'\n\
         raw_prefix='home'\nraw=['rx','inject']\n\
         [bridge]\nstorage_path='state'\ndns=['https://1.1.1.1/dns-query','9.9.9.9:53']\n\
         [gui]\ngui_port={bind=8080,address='192.168.0.111'}\ngui_user='admin'\ngui_pass='secret'\n",
    )
    .unwrap();
    let dest = dir.path().join("new");
    rusthinq_tools::migration::migrate(&source, &dest).unwrap();
    let config = rusthinq_app::daemon::Config::load(&dest.join("config.toml")).unwrap();
    assert_eq!(
        config.https_bind,
        Some("192.168.0.111:4433".parse().unwrap())
    );
    assert_eq!(config.https_advertise, Some(Advertise::Port(443)));
    assert_eq!(config.mqtt_bind, None);
    assert_eq!(
        config.mqtt_advertise,
        Some(Advertise::Url("ssl://proxy.example:8883".into()))
    );
    assert_eq!(config.http_bind, Some("127.0.0.1:80".parse().unwrap()));
    assert_eq!(
        config.thinq1_http_bind,
        Some("0.0.0.0:46031".parse().unwrap())
    );
    assert_eq!(config.thinq1_bind, Some("0.0.0.0:47879".parse().unwrap()));
    assert_eq!(
        config.custom_root_certificate.unwrap(),
        dir.path().canonicalize().unwrap().join("root.cert")
    );
    assert_eq!(
        config.bridge_dns,
        ["https://1.1.1.1/dns-query", "9.9.9.9:53"]
    );
    let mqtt = config.external_mqtt.unwrap();
    assert_eq!(
        (mqtt.host.as_str(), mqtt.port, mqtt.tls),
        ("broker.lan", 8883, true)
    );
    std::fs::write(dir.path().join("mqtt_state.json"), br#"{"d":["power"]}"#).unwrap();
    assert_eq!(mqtt.username.as_deref(), Some("u"));
    assert_eq!(mqtt.password.as_deref(), Some("p"));
    let management = config.management.unwrap();
    assert_eq!(management.bind, "192.168.0.111:8080".parse().unwrap());
    assert_eq!(management.credentials.unwrap().user, "admin");
    assert!(management.raw_inject);

    // An unauthenticated LAN GUI cannot be served by 0.2; it moves to loopback, loudly.
    std::fs::write(
        &source,
        "hostname='rusthinq.lan'\n[mqtt]\nmqtt_url='mqtt://broker.lan:1884'\nrusthinq_prefix='home'\n\
         [gui]\ngui_port=8080\n",
    )
    .unwrap();
    let dest = dir.path().join("open-gui");
    let report = rusthinq_tools::migration::migrate(&source, &dest).unwrap();
    assert!(
        report["warnings"]
            .to_string()
            .contains("without authentication on 0.0.0.0:8080")
    );
    let config = rusthinq_app::daemon::Config::load(&dest.join("config.toml")).unwrap();
    assert_eq!(
        config.management.unwrap().bind,
        "127.0.0.1:8080".parse().unwrap()
    );
    let mqtt = config.external_mqtt.unwrap();
    assert_eq!(
        (mqtt.port, mqtt.tls, mqtt.username.clone()),
        (1884, false, None)
    );
    // The imported 0.1 inventory is the adapter's durable ledger, not a side file.
    assert!(mqtt.inventory.ends_with("retained-import.json"));
    let mut ledger = rusthinq_app::retained_cleanup::Ledger::open(&mqtt.inventory, 16384).unwrap();
    assert!(ledger.requested().is_empty());
    let imported = rusthinq_server::retained::Tombstone {
        owner: format!("{}d", rusthinq_app::retained_cleanup::IMPORTED_OWNER),
        topic: "home/d/power".into(),
    };
    assert_eq!(ledger.pending(), std::slice::from_ref(&imported));
    // 0.2's driver republishing a 0.1 topic takes it over instead of failing...
    ledger
        .inventory_topic("device:d:1".into(), "home/d/power".into())
        .unwrap();
    assert_eq!(ledger.pending()[0].owner, "device:d:1");
    // ...but two live owners still conflict, and a requested deletion is not overridden.
    assert!(
        ledger
            .inventory_topic("device:d:2".into(), "home/d/power".into())
            .is_err()
    );
    let dest = dir.path().join("requested");
    rusthinq_tools::migration::migrate(&source, &dest).unwrap();
    let mut ledger =
        rusthinq_app::retained_cleanup::Ledger::open(&dest.join("retained-import.json"), 16384)
            .unwrap();
    ledger.request_delete(&[imported]).unwrap();
    assert!(
        ledger
            .inventory_topic("device:d:1".into(), "home/d/power".into())
            .is_err()
    );
    assert_eq!(
        config.thinq1_http_bind,
        Some("0.0.0.0:46030".parse().unwrap())
    );
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

#[test]
fn staged_full_legacy_configuration_loads_in_the_daemon_and_flags_retired_settings() {
    let dir = tempfile::tempdir().unwrap();
    let scripts = dir.path().join("rusthinq-scripts");
    std::fs::create_dir(&scripts).unwrap();
    let source = dir.path().join("config.toml");
    let original = "hostname='rusthinq.lan'\nadvertise_requested_host=true\nca_key_file='ca.key'\nca_cert_file='ca.cert'\n\
        https_port=443\nmqtts_port=8883\nthinq1_port=47878\nlog=['status']\n\
        [mqtt]\nmqtt_url='mqtt://localhost:1883'\nrusthinq_prefix='home'\nraw_prefix='home-raw'\n\
        [scripting]\nrhai_dir='rusthinq-scripts'\nwatch=true\nil_prefix='il'\n\
        [gui]\ngui_port=8080\n";
    std::fs::write(&source, original).unwrap();
    let dest = dir.path().join("new");
    let report = rusthinq_tools::migration::migrate(&source, &dest).unwrap();
    let warnings = report["warnings"].to_string();
    for retired in [
        "il_prefix = \\\"il\\\"",
        "il_common.rhai",
        "raw observation streams",
        "log categories",
    ] {
        assert!(warnings.contains(retired), "{retired}: {warnings}");
    }
    let config = rusthinq_app::daemon::Config::load(&dest.join("config.toml")).unwrap();
    let drivers = config.drivers.unwrap();
    assert_eq!(drivers.topic_prefix, "home");
    assert!(drivers.watch);
    assert_eq!(
        drivers.directory.canonicalize().unwrap(),
        scripts.canonicalize().unwrap()
    );
    assert!(config.management.is_some());
    // 0.1's DNAT mode and its always-on legacy device TLS profile carry over.
    assert!(config.advertise_requested_host);
    assert!(config.legacy_tls);
    // The original installation is untouched: rollback is restarting 0.1 on it.
    assert_eq!(std::fs::read_to_string(&source).unwrap(), original);
    assert_eq!(
        std::fs::read_to_string(dest.join("config.0.1.toml")).unwrap(),
        original
    );
}
