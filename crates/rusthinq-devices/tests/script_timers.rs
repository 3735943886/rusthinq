//! `ctx.set_timer` / `cancel_timer` / `on_timer`, and `ctx.send_clip`, through the harness.
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
