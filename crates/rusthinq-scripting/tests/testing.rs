use rusthinq_scripting::{Error, Host, Output, drivers::Config, testing};
use std::path::Path;

fn fixture(root: &Path, source: &str, tests: &str) {
    std::fs::create_dir_all(root.join("drivers")).unwrap();
    std::fs::create_dir_all(root.join("tests")).unwrap();
    std::fs::write(root.join("drivers/Broken.rhai"), source).unwrap();
    std::fs::write(root.join("tests/Broken.test.rhai"), tests).unwrap();
}

#[test]
fn every_hook_error_fails_without_an_assertion_and_reports_its_position() {
    let root = tempfile::tempdir().unwrap();
    for (hook, constructor, call, setup) in [
        ("on_data(ctx, data)", "device()", r#"d.feed("01")"#, ""),
        (
            "on_data(ctx, data)",
            "device()",
            r#"d.feed(hex_decode("01"))"#,
            "",
        ),
        ("on_data(ctx, data)", "device_t1()", r#"d.feed("01")"#, ""),
        (
            "on_data(ctx, data)",
            "device_t1()",
            r#"d.feed(hex_decode("01"))"#,
            "",
        ),
        (
            "on_set_property(ctx, prop, value)",
            "device()",
            r#"d.set("x", "1")"#,
            "",
        ),
        ("start(ctx)", "device()", "d.start()", ""),
        ("publish_config(ctx)", "device()", "d.start()", ""),
        ("on_drop(ctx)", "device()", "d.drop_device()", ""),
        (
            "on_timer(ctx, name)",
            "device()",
            r#"d.start(); d.fire("tick")"#,
            r#"fn start(ctx) {ctx.set_timer("tick", 1);}"#,
        ),
        (
            "describe(data, to_device)",
            "device()",
            r#"d.describe("01", false)"#,
            "",
        ),
    ] {
        for error in ["hex_encode([1])", "undefined_hook_function()"] {
            fixture(
                root.path(),
                &format!(
                    "fn {hook} {{
    {error};
}}
{setup}"
                ),
                &format!(
                    "fn test_error() {{
    let d = {constructor};
    {call};
}}"
                ),
            );
            let reports = testing::run(root.path()).unwrap();
            assert_eq!(reports.len(), 1);
            let detail = reports[0]
                .error
                .as_ref()
                .unwrap_or_else(|| panic!("{hook}: {call} silently passed"));
            assert!(detail.contains("Function not found"), "{detail}");
            assert!(
                detail.contains(error.split('(').next().unwrap()),
                "{detail}"
            );
            assert!(detail.contains("line 2"), "{detail}");
            assert!(detail.contains("position"), "{detail}");
            assert!(detail.contains(hook.split('(').next().unwrap()), "{detail}");
        }
    }
}

#[test]
fn caught_errors_and_discarded_devices_still_fail_only_the_current_test() {
    let root = tempfile::tempdir().unwrap();
    fixture(
        root.path(),
        r#"
fn on_data(ctx, data) {
    ctx.publish_raw("rusthinq/test-device/event/prefix", "kept", false);
    hex_encode([1]);
    ctx.publish_raw("rusthinq/test-device/event/reject", "unreachable", false);
}
"#,
        r#"
fn test_a_caught() {
    let d = device();
    try { d.feed("01"); } catch (error) {}
    expect(d.script_error() != (), "the error is available directly");
    expect_eq(d.event("script_error"), ());
    expect_eq(d.event("prefix"), "kept");
    expect_eq(d.event("reject"), ());
    expect_eq(d.clips(), []);
    let first = d.script_error();
    try { d.set("x", "1"); } catch (error) {}
    expect_eq(d.script_error(), first, "keep the original cause, not Faulted");
}
fn test_b_discarded() {
    try { let d = device(); d.feed(hex_decode("01")); } catch (error) {}
}
fn test_c_clean() {
    let d = device();
    expect_eq(d.script_error(), ());
}
"#,
    );
    let reports = testing::run(root.path()).unwrap();
    assert_eq!(reports.len(), 3);
    for report in &reports[..2] {
        let error = report
            .error
            .as_ref()
            .expect("caught hook errors must still fail");
        assert!(error.contains("hex_encode"), "{error}");
        assert!(!error.contains("expect_eq:"), "{error}");
    }
    assert!(reports[2].error.is_none(), "{reports:?}");
}

#[test]
fn production_host_still_returns_error_prefix_and_isolates_the_fault() {
    let root = tempfile::tempdir().unwrap();
    fixture(
        root.path(),
        r#"
fn on_data(ctx, data) {
    ctx.publish_raw("before", "kept", false);
    hex_encode([1]);
    ctx.publish_raw("after", "unreachable", false);
}
fn start(ctx) {ctx.publish_raw("healthy", "alive", false);}
"#,
        "",
    );
    let config = Config {
        directory: root.path().into(),
        topic_prefix: "rusthinq".into(),
        watch: false,
        bindings: Default::default(),
        limits: Default::default(),
    };
    let mut failed = Host::new(config.prepare("d", "Broken", true, true).unwrap());
    let outcome = failed.invoke(1, "__data", "01");
    assert!(
        matches!(outcome.error, Some(Error::Execution(ref text)) if text.contains("hex_encode") && text.contains("line 4"))
    );
    assert!(
        matches!(&outcome.outputs[..], [Output::Publish(text)] if text.contains("before") && !text.contains("after"))
    );
    assert_eq!(failed.invoke(1, "__data", "01").error, Some(Error::Faulted));
    let mut healthy = Host::new(config.prepare("other", "Broken", true, true).unwrap());
    let outcome = healthy.invoke(1, "__init", "");
    assert_eq!(outcome.error, None);
    assert!(matches!(&outcome.outputs[..], [Output::Publish(text)] if text.contains("healthy")));
}
