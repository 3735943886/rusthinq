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
    let dest = dir.path().join("new");
    let report = rusthinq_tools::migration::migrate(&source, &dest).unwrap();
    assert_eq!(report["requiresReview"], true);
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
