#![cfg(feature = "scripting")]
use rusthinq_app::drivers::Config;
use rusthinq_scripting::{Host, Output};
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn config() -> Config {
    Config {
        directory: std::env::temp_dir(),
        topic_prefix: "rusthinq".into(),
        limits: Default::default(),
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

#[test]
fn static_imports_follow_rhai_syntax_and_track_all_dependencies() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("a.rhai"), "fn value(){\"a\"}").unwrap();
    std::fs::write(directory.path().join("b.rhai"), "fn value(){\"b\"}").unwrap();
    std::fs::write(
        directory.path().join("adapter.rhai"),
        r#"import "a" as a; import "b" as b; fn value(){a::value()+b::value()}"#,
    )
    .unwrap();
    std::fs::write(
        directory.path().join("model.rhai"),
        r#"/*
        import "missing" as ignored;
        */
        let text = "import \"missing\" as ignored;";
        import /* comment */
            "adapter" as adapter; import "b" as b;
        fn start(ctx) {ctx.publish_raw("result",adapter::value()+b::value(),false);}
        "#,
    )
    .unwrap();
    let options = rusthinq_scripting::drivers::Config {
        directory: directory.path().into(),
        topic_prefix: "rusthinq".into(),
        limits: Default::default(),
        bindings: BTreeMap::new(),
        watch: false,
    };
    let revision = options.source_revision("d", "model").unwrap();
    let mut host = Host::new(options.prepare("d", "model", true, true).unwrap());
    let outcome = host.invoke(1, "__init", "");
    assert_eq!(outcome.error, None);
    assert_eq!(
        messages(outcome.outputs),
        vec![json!({"topic":"result","payload":"abb","retain":false})]
    );
    std::fs::write(directory.path().join("b.rhai"), "fn value(){\"changed\"}").unwrap();
    assert_ne!(options.source_revision("d", "model").unwrap(), revision);
}

#[test]
fn dynamic_and_cyclic_imports_fail_during_preparation() {
    let directory = tempfile::tempdir().unwrap();
    let options = rusthinq_scripting::drivers::Config {
        directory: directory.path().into(),
        topic_prefix: "rusthinq".into(),
        limits: Default::default(),
        bindings: BTreeMap::new(),
        watch: false,
    };
    std::fs::write(directory.path().join("a.rhai"), "import \"b\" as b;").unwrap();
    std::fs::write(directory.path().join("b.rhai"), "import \"a\" as a;").unwrap();
    for source in [
        "let module=\"a\"; import module as a; fn start(ctx){}",
        "import \"a\" as a; fn start(ctx){}",
    ] {
        std::fs::write(directory.path().join("model.rhai"), source).unwrap();
        assert!(options.prepare("d", "model", true, true).is_err());
    }
}

#[test]
fn configured_limits_cover_module_bundle_and_aggregate_outputs() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = config();
    options.directory = directory.path().into();
    let source = "import \"helper\" as h; fn start(ctx) {ctx.publish_raw(\"a\",h::value(),false);ctx.publish_raw(\"b\",h::value(),false);}";
    std::fs::write(directory.path().join("Budget.rhai"), source).unwrap();
    std::fs::write(
        directory.path().join("helper.rhai"),
        "fn value(){\"payload\"}",
    )
    .unwrap();
    let mut host = Host::new(options.prepare("d", "Budget", true, true).unwrap());
    assert_eq!(host.invoke(1, "__init", "").outputs.len(), 2);
    options.limits.outputs = 1;
    let mut host = Host::new(options.prepare("d", "Budget", true, true).unwrap());
    assert!(host.invoke(1, "__init", "").error.is_some());
    options.limits.outputs = 256;
    options.limits.output_bytes = 64;
    let mut host = Host::new(options.prepare("d", "Budget", true, true).unwrap());
    assert!(host.invoke(1, "__init", "").error.is_some());
    // The budget includes generated entry points and module names/sources,
    // even when every individual file fits.
    options.limits.source_bytes = source.len() + 25;
    assert!(options.prepare("d", "Budget", true, true).is_err());
    options.limits.operations = 0;
    assert!(options.prepare("d", "Budget", true, true).is_err());
}

#[test]
fn compiled_revision_keeps_the_prepared_bundle_when_files_change() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = config();
    options.directory = directory.path().into();
    std::fs::write(
        directory.path().join("M.rhai"),
        "import \"c\" as c; fn start(ctx) {}",
    )
    .unwrap();
    std::fs::write(directory.path().join("c.rhai"), "fn value(){1}").unwrap();
    let engine = rusthinq_scripting::drivers::Config {
        watch: false,
        directory: options.directory.clone(),
        topic_prefix: options.topic_prefix.clone(),
        bindings: Default::default(),
        limits: options.limits.clone(),
    };
    let revision = engine.source_revision("d", "M").unwrap();
    let compiled = options.prepare("d", "M", true, true).unwrap();
    std::fs::write(directory.path().join("c.rhai"), "fn value(){2}").unwrap();
    assert_eq!(compiled.driver_revision(), Some(revision));
    assert_ne!(
        compiled.driver_revision(),
        Some(engine.source_revision("d", "M").unwrap())
    );
    assert_eq!(
        engine.worker_config().source_bytes,
        options.limits.source_bytes
    );
}
