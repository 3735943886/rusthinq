//! Structural checks that the shipped main path uses one MqttSink and management.

#[test]
fn decode_module_has_full_re_export_logic() {
    // The RE decode/export/catalog logic lives in rusthinq_util::decode as plain
    // library functions — no HTTP server, so rusthinq-cloud and rusthinq-tools/MCP
    // both link it directly. Check it's the real implementation, not a stub.
    let decode_mod = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../rusthinq-util/src/decode.rs"
    ));
    assert!(decode_mod.contains("pub fn decode_hex_payload"));
    assert!(decode_mod.contains("pub fn re_export_text"));
    assert!(decode_mod.contains("pub fn tlv_catalog_json"));
    assert!(decode_mod.contains("llm_export_text") || decode_mod.contains("classify_tlvs"));
    assert!(
        decode_mod.contains("byteStart")
            || decode_mod.contains("parse_with_spans")
            || decode_mod.contains("hexStart")
    );
}

#[test]
fn main_has_no_management_http_and_uses_single_mqtt_sink() {
    let main_src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"));
    let news: Vec<_> = main_src.match_indices("MqttSink::new").collect();
    assert_eq!(
        news.len(),
        1,
        "expected exactly one MqttSink::new in main.rs, found {}",
        news.len()
    );
    assert!(
        main_src.contains("attach_mqtt_sink"),
        "main must call attach_mqtt_sink so set/discovery reach DeviceBridge"
    );
    assert!(
        main_src.contains("start_mqtt_client(sink)")
            || main_src.contains("start_mqtt_client(mqtt_sink"),
        "MQTT client must receive the same sink clone"
    );
    // The always-on 44401 management HTTP/WS surface (module + axum::serve) is
    // gone for good — RE APIs are direct library calls, device/bridge status is
    // the retained MQTT `<prefix>/devices` topic, and bridge control (including
    // login/logout) lives entirely in bridge_control.rs. Guard against it creeping
    // back.
    assert!(
        !main_src.contains("mod management") && !main_src.contains("management::router"),
        "management HTTP module should stay removed — MQTT/library calls replace it"
    );
    assert!(
        main_src.contains("mod devlist") && main_src.contains("mod bridge_control"),
        "devlist/bridge_control must be linked as the MQTT replacement for management"
    );
    assert!(
        main_src.contains("bridge_control::register"),
        "bridge enable/disable must be wired to the MQTT control surface"
    );
}

#[test]
fn main_resyncs_the_device_list_snapshot_on_mqtt_reconnect() {
    // The retained <prefix>/devices snapshot is only ever *written* on a device
    // connect/disconnect (devmgr callbacks) — if the broker itself loses its retained
    // store around the same time this MQTT client reconnects (e.g. broker restart),
    // nothing else puts it back until the next device event. on_discovery is the
    // generic "resync everything on reconnect" hook device_bridge.rs's republish_all
    // also rides; main.rs must register device_list.publish() on it too.
    let main_src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"));
    assert!(
        main_src.contains("on_discovery(move || device_list.publish())")
            || (main_src.matches("on_discovery").count() >= 2
                && main_src.contains("device_list.publish()")),
        "main.rs must resync the devices snapshot via on_discovery, not just on connect/disconnect"
    );
}

#[test]
fn device_bridge_registers_set_and_discovery() {
    let src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/device_bridge.rs"));
    assert!(src.contains("fn attach_mqtt_sink"));
    assert!(src.contains("on_set_property"));
    assert!(src.contains("on_discovery"));
    assert!(
        src.contains("T2Clip") || src.contains("SendToDevice::T2Clip") || {
            // send path may reference T2Clip via fully qualified path
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/device_bridge.rs"))
                .contains("T2Clip")
        }
    );
}

#[test]
fn t2_adapter_send_not_no_op() {
    let src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/device_bridge.rs"));
    // Must not be empty body discard
    assert!(
        !src.contains("fn send(&self, cmd: &str, msg_type: i32, data: serde_json::Value) {\n        let _ = (cmd, msg_type, data);\n    }"),
        "T2Adapter::send must not discard CLIP commands"
    );
    assert!(src.contains("T2Clip"), "T2Adapter::send must use T2Clip");
}

#[test]
fn bridge_crate_has_real_upstream_connections() {
    let lib = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../rusthinq-bridge/src/lib.rs"
    ));
    let t2 = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../rusthinq-bridge/src/thinq2_conn.rs"
    ));
    let t1 = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../rusthinq-bridge/src/thinq1_conn.rs"
    ));
    let pair = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../rusthinq-bridge/src/pair.rs"
    ));
    assert!(t2.contains("connect_thinq2") || t2.contains("fn connect"));
    assert!(t2.contains("send_from_local"));
    assert!(t2.contains("device_packet") || t2.contains("format_device_packet"));
    assert!(t1.contains("connect_thinq1") || t1.contains("fn connect"));
    assert!(t1.contains("send_from_local"));
    assert!(pair.contains("pair_thinq2"));
    assert!(pair.contains("mqtt_server") || pair.contains("mqttServer"));
    // start_session must open upstream and wire on_data
    assert!(lib.contains("connect_thinq2") || lib.contains("connect_thinq2"));
    assert!(lib.contains("send_from_local"));
    assert!(lib.contains("on_data"));
    assert!(
        !lib.contains("let _ = buf"),
        "local→LG on_data must not ignore buffer"
    );
}
