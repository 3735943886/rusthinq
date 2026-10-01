//! `ctx.set_timer` / `cancel_timer` / `on_timer`, and `ctx.send_clip`, through the harness.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::ScriptHarness;

fn harness(body: &str) -> (tempfile::TempDir, ScriptHarness) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("TIMER_TEST.rhai");
    std::fs::write(&path, body).unwrap();
    let h = ScriptHarness::t2(&path, "TIMER_TEST");
    (dir, h)
}

const SCRIPT: &str = r#"
    fn start(ctx) {
        ctx.set_timer("a", 1000);
        ctx.set_timer("b", 2000);
    }
    fn on_set_property(ctx, prop, value) {
        if prop == "rearm" { ctx.set_timer("a", 5000); }
        if prop == "cancel" { ctx.cancel_timer("a"); }
        if prop == "clip" { ctx.send_clip("hello", 3, "{\"k\":1}"); }
    }
    fn on_timer(ctx, name) {
        ctx.publish_property("fired", name);
    }
"#;

#[test]
fn timers_are_listed_and_firing_one_calls_on_timer_once() {
    let (_dir, h) = harness(SCRIPT);
    h.start();
    assert_eq!(
        h.pending_timers(),
        vec![("a".to_string(), 1000), ("b".to_string(), 2000)]
    );
    h.fire_timer("a");
    assert_eq!(h.property("fired").as_deref(), Some("a"));
    assert_eq!(h.pending_timers(), vec![("b".to_string(), 2000)]);
    // already fired: nothing to fire
    h.fire_timer("a");
}

#[test]
fn rearming_a_name_replaces_its_delay() {
    let (_dir, h) = harness(SCRIPT);
    h.start().set_property("rearm", "");
    assert_eq!(
        h.pending_timers(),
        vec![("a".to_string(), 5000), ("b".to_string(), 2000)]
    );
}

#[test]
fn a_cancelled_timer_is_gone_and_never_fires() {
    let (_dir, h) = harness(SCRIPT);
    h.start().set_property("cancel", "");
    assert_eq!(h.pending_timers(), vec![("b".to_string(), 2000)]);
    h.fire_timer("a");
    assert_eq!(h.property("fired"), None);
}

#[test]
fn dropping_the_device_cancels_every_timer() {
    let (_dir, h) = harness(SCRIPT);
    h.start().drop_device();
    assert!(h.pending_timers().is_empty());
}

#[test]
fn send_clip_reaches_the_device_as_a_structured_message() {
    let (_dir, h) = harness(SCRIPT);
    h.start().set_property("clip", "");
    let clips = h.sent_clip();
    assert_eq!(clips.len(), 1);
    assert_eq!(clips[0].0, "hello");
    assert_eq!(clips[0].1, 3);
    assert_eq!(clips[0].2["k"], 1);
}

#[test]
fn a_runtime_error_in_a_hook_is_published_as_a_script_error_event() {
    let (_dir, h) = harness("fn on_set_property(ctx, prop, value) { let x = 1 / 0; }");
    assert_eq!(h.script_error(), None);
    h.set_property("anything", "");
    let error = h.script_error().expect("the error is surfaced");
    assert!(error.starts_with("on_set_property:"), "{error}");
}

const IL_SCRIPT: &str = r#"
    fn publish_config(ctx) {
        ctx.publish_il(json_stringify(#{ il: 0, props: #{
            on: #{ type: "binary", rw: true },
            level: #{ type: "number", rw: true, min: 0, max: 10, step: 1 },
            fixed: #{ type: "number" },
            armed: #{ type: "binary" },
            fire: #{ type: "trigger", requires: "armed" }
        } }));
    }
    fn on_set_property(ctx, prop, value) { ctx.publish_property("got_" + prop, value); }
"#;

#[test]
fn a_valid_command_reaches_the_script_in_canonical_form() {
    let (_dir, h) = harness(IL_SCRIPT);
    h.start();
    h.set_property("on", "OFF");
    h.set_property("level", "7.0");
    assert_eq!(h.property("got_on").as_deref(), Some("false"));
    assert_eq!(h.property("got_level").as_deref(), Some("7"));
}

#[test]
fn an_invalid_command_is_a_reject_event_and_never_reaches_the_script() {
    let (_dir, h) = harness(IL_SCRIPT);
    h.start();
    h.set_property("level", "11");
    h.set_property("fixed", "1");
    h.set_property("missing", "1");
    for prop in ["got_level", "got_fixed", "got_missing"] {
        assert_eq!(h.property(prop), None, "{prop}");
    }
    assert_eq!(
        h.event("reject").unwrap(),
        r#"{"code":"unknown_property","prop":"missing","reason":"unknown property"}"#
    );
}

#[test]
fn requires_follows_the_last_value_the_script_published() {
    let (_dir, h) = harness(&IL_SCRIPT.replace(
        "fn on_set_property",
        "fn start(ctx) { ctx.publish_property(\"armed\", \"true\"); }\n    fn on_set_property",
    ));
    h.set_property("fire", ""); // before start: no descriptor yet, so it passes straight through
    assert_eq!(h.property("got_fire").as_deref(), Some(""));
    h.start(); // the script now reports armed = true
    h.set_property("fire", "x");
    assert_eq!(h.property("got_fire").as_deref(), Some("x"));
}

#[test]
fn a_script_that_publishes_no_descriptor_is_not_validated() {
    let (_dir, h) =
        harness("fn on_set_property(ctx, prop, value) { ctx.publish_property(\"got\", value); }");
    h.start().set_property("anything", "as-is");
    assert_eq!(h.property("got").as_deref(), Some("as-is"));
    assert_eq!(h.event("reject"), None);
}

#[test]
fn the_host_binds_a_published_descriptor_to_the_devices_own_topics() {
    rusthinq_devices::scripting::set_il_prefix(Some("il".into()));
    rusthinq_devices::scripting::set_name_source(Some(std::sync::Arc::new(|id| {
        (id == "harness").then(|| "Kitchen".to_string())
    })));
    let (_dir, h) = harness(IL_SCRIPT);
    h.start();
    let (payload, retained) = h.raw_publish("il/harness").expect("descriptor published");
    rusthinq_devices::scripting::set_il_prefix(None);
    rusthinq_devices::scripting::set_name_source(None);
    assert!(retained);
    let d: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(d["id"], "harness");
    assert_eq!(
        d["label"], "Kitchen",
        "the owner's name replaces the driver's label"
    );
    assert_eq!(d["source"], "rusthinq");
    assert_eq!(d["props"]["level"]["max"], 10);
    assert!(d["x-mqtt"]["state"].as_str().unwrap().ends_with("/{prop}"));
    assert!(
        d["x-mqtt"]["set"]
            .as_str()
            .unwrap()
            .ends_with("/{prop}/set")
    );
    assert!(
        d["x-mqtt"]["reject"]
            .as_str()
            .unwrap()
            .ends_with("/{id}/reject")
    );
}

const AVAILABILITY_SCRIPT: &str = r#"
    fn start(ctx) { ctx.publish_property("power", "true"); }
    fn publish_config(ctx) {
        ctx.publish_il(json_stringify(#{ il: 0, props: #{
            up: #{ type: "binary", role: "available" },
            power: #{ type: "binary", rw: true }
        } }));
    }
    fn on_data(ctx, data) { ctx.publish_property("up", "true"); }
    fn on_drop(ctx) { ctx.publish_property("up", "false"); }
"#;

#[test]
fn a_device_is_unavailable_until_its_script_says_otherwise() {
    let (_dir, h) = harness(AVAILABILITY_SCRIPT);
    h.start();
    assert_eq!(h.property("up").as_deref(), Some("false"));
}

#[test]
fn a_dropped_link_keeps_available_false_and_blanks_every_other_value() {
    let (_dir, h) = harness(AVAILABILITY_SCRIPT);
    h.start();
    assert_eq!(h.property("power").as_deref(), Some("true"));
    h.drop_device();
    assert_eq!(h.property("up").as_deref(), Some("false"));
    assert_eq!(
        h.property("power").as_deref(),
        Some(""),
        "absent = empty retained"
    );
}
