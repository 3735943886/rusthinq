#![cfg(feature = "scripting")]
use rusthinq_app::drivers::Config;
use rusthinq_scripting::{Host, Output};
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn config() -> Config {
    Config {
        directory: std::env::temp_dir(),
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
fn driver_names_cannot_escape_the_configured_directory() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = config();
    options.directory = directory.path().into();
    assert!(options.prepare("d", "../outside", true, true).is_err());
}
