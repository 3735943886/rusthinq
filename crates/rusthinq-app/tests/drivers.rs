#![cfg(feature = "scripting")]
use rusthinq_app::drivers::Config;
use rusthinq_scripting::{Host, Output};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};
fn config() -> Config {
    Config {
        directory: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/drivers"),
        topic_prefix: "rusthinq".into(),
        bindings: BTreeMap::new(),
        watch: false,
    }
}
fn messages(outputs: Vec<Output>) -> Vec<Value> {
    outputs
        .into_iter()
        .filter_map(|output| match output {
            Output::Publish(value) => Some(serde_json::from_str(&value).unwrap()),
            _ => None,
        })
        .collect()
}
#[test]
fn real_aabb_driver_publishes_and_gates_its_own_commands() {
    let mut host = Host::new(config().prepare("d", "Pd0F_F", true, true).unwrap());
    let initialized = host.invoke(1, "__init", "");
    assert_eq!(initialized.error, None);
    assert!(!messages(initialized.outputs).is_empty());
    // Without remote start the script itself refuses: a non-retained publication, no send.
    let rejected = host.invoke(
        1,
        "__command",
        &json!({"prop":"pause","value":""}).to_string(),
    );
    assert_eq!(rejected.error, None);
    assert!(
        !rejected
            .outputs
            .iter()
            .any(|output| matches!(output, Output::Send(_)))
    );
    assert_eq!(messages(rejected.outputs)[0]["retain"], false);
    let mut payload = vec![0; 25];
    payload[5] = 1;
    payload[16] = 4;
    let mut inner = vec![0x20, 0xeb, 0, 25];
    inner.extend(payload);
    let frame = rusthinq_protocol::aabb::wrap(&inner).unwrap();
    let outcome = host.invoke(1, "__data", &rusthinq_protocol::hex::encode(frame));
    assert_eq!(outcome.error, None);
    assert!(
        messages(outcome.outputs)
            .iter()
            .any(|message| message["topic"] == "rusthinq/d/remote_start"
                && message["payload"] == "true")
    );
    let command = host.invoke(
        1,
        "__command",
        &json!({"prop":"pause","value":""}).to_string(),
    );
    assert_eq!(command.error, None);
    let Output::Send(command) = &command.outputs[0] else {
        panic!("command")
    };
    let command: Value = serde_json::from_str(command).unwrap();
    assert_eq!(command["cmd"], "packet");
    assert_eq!(command["did"], "d");
    let bytes = rusthinq_protocol::hex::decode(command["data"].as_str().unwrap()).unwrap();
    assert_eq!(&bytes[2..7], [0xf0, 0x24, 4, 1, 0]);
}
#[test]
fn real_tlv_driver_prepares_query_and_owned_timer_effects() {
    let mut host = Host::new(config().prepare("d", "AIR_910604_WW", true, true).unwrap());
    let outcome = host.invoke(1, "__init", "");
    assert_eq!(outcome.error, None);
    assert!(outcome.outputs.iter().any(
        |output| matches!(output,Output::Timer {name,after_ms:Some(15000)} if name=="caps_retry")
    ));
    let outcome = host.invoke(1, "__timer", "caps_retry");
    assert_eq!(outcome.error, None);
    assert!(
        outcome
            .outputs
            .iter()
            .any(|output| matches!(output, Output::Send(_)))
    );
    assert!(config().prepare("d", "../Pd0F_F", true, true).is_err());
}

#[test]
fn commands_reach_the_script_unvalidated_and_are_ignored_without_a_callback() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("Echo.rhai"),
        r#"fn on_set_property(ctx,prop,value) {ctx.publish_raw(prop,value,false);}"#,
    )
    .unwrap();
    std::fs::write(directory.path().join("Silent.rhai"), "fn start(ctx) {}").unwrap();
    let mut options = config();
    options.directory = directory.path().into();
    let mut host = Host::new(options.prepare("d", "Echo", true, true).unwrap());
    for value in ["NaN", " 4 ", ""] {
        let outcome = host.invoke(
            1,
            "__command",
            &json!({"prop":"level","value":value}).to_string(),
        );
        assert_eq!(outcome.error, None, "{value}");
        assert_eq!(
            messages(outcome.outputs),
            vec![json!({"topic":"level","payload":value,"retain":false})]
        );
    }
    let mut host = Host::new(options.prepare("d", "Silent", true, true).unwrap());
    let outcome = host.invoke(
        1,
        "__command",
        &json!({"prop":"level","value":"1"}).to_string(),
    );
    assert_eq!((outcome.error, outcome.outputs), (None, vec![]));
}

#[test]
fn every_pinned_model_initializes_in_the_new_host() {
    for model in [
        "1WPU4CIGCR__2",
        "2RSFL2DBN3K_Z",
        "AIR_910604_WW",
        "CST_570004_WW",
        "D140110",
        "DHUM_056905_WW",
        "F24VDD",
        "Pd0F_F",
        "RH14_N_KR",
        "S3BF_POD_DN4",
        "WBEY3GT",
    ] {
        let prepared = config()
            .prepare("d", model, model != "D140110", true)
            .unwrap_or_else(|error| panic!("{model}: {error:?}"));
        let mut host = Host::new(prepared);
        let outcome = host.invoke(1, "__init", "");
        assert_eq!(outcome.error, None, "{model}");
        assert!(!messages(outcome.outputs).is_empty(), "{model}");
    }
}
