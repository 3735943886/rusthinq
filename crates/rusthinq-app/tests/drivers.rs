use rusthinq_app::drivers::Config;
use rusthinq_scripting::{Host, Output};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};
fn config() -> Config {
    Config {
        directory: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/drivers"),
        topic_prefix: "rusthinq".into(),
        il_prefix: Some("ildevice".into()),
        bindings: BTreeMap::new(),
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
fn real_aabb_driver_emits_descriptor_properties_and_gated_commands() {
    let mut host = Host::new(config().prepare("d", "Pd0F_F", true, true).unwrap());
    let initialized = host.invoke(1, "__init", "");
    assert_eq!(initialized.error, None);
    let initial = messages(initialized.outputs);
    assert_eq!(initial[0]["topic"], "ildevice/d");
    let descriptor: Value = serde_json::from_str(initial[0]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(descriptor["model"], "Pd0F_F");
    assert_eq!(descriptor["source"], "rusthinq");
    assert_eq!(descriptor["x-mqtt"]["state"], "rusthinq/{id}/{prop}");
    let rejected = host.invoke(
        1,
        "__command",
        &json!({"prop":"pause","value":""}).to_string(),
    );
    assert_eq!(rejected.error, None);
    let rejected = messages(rejected.outputs);
    assert_eq!(rejected[0]["retain"], false);
    let rejected: Value = serde_json::from_str(rejected[0]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(rejected["code"], "requires_unmet");
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
fn script_side_number_validation_rejects_nonfinite_range_and_step() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("Numeric.rhai"),r#"
        fn publish_config(ctx) {
            ctx.publish_il(json_stringify(#{props:#{level:#{type:"number",rw:true,min:0,max:10,step:2}}}));
        }
        fn on_set_property(ctx,prop,value) {ctx.publish_property(prop,value);}
    "#).unwrap();
    let mut options = config();
    options.directory = directory.path().into();
    let mut host = Host::new(options.prepare("d", "Numeric", true, true).unwrap());
    assert_eq!(host.invoke(1, "__init", "").error, None);
    for (value, code) in [
        ("NaN", "invalid_value"),
        ("inf", "invalid_value"),
        ("12", "out_of_range"),
        ("3", "bad_step"),
    ] {
        let outcome = host.invoke(
            1,
            "__command",
            &json!({"prop":"level","value":value}).to_string(),
        );
        assert_eq!(outcome.error, None, "{value}");
        let output = messages(outcome.outputs);
        let reject: Value = serde_json::from_str(output[0]["payload"].as_str().unwrap()).unwrap();
        assert_eq!(reject["code"], code, "{value}");
    }
    let outcome = host.invoke(
        1,
        "__command",
        &json!({"prop":"level","value":" 4 "}).to_string(),
    );
    assert_eq!(outcome.error, None);
    assert_eq!(
        messages(outcome.outputs)[0]["payload"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap(),
        4.0
    );
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
        assert!(
            messages(outcome.outputs)
                .iter()
                .any(|message| message["topic"] == "ildevice/d"),
            "{model}"
        );
    }
}
