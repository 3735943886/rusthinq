//! Contain a panic from per-device callback code (data handlers, MQTT
//! set_property/discovery callbacks, and — once Phase 3 lands — Rhai scripts) to the
//! device that caused it, instead of letting it unwind the calling task.
//!
//! Several dispatch loops in this codebase (`MqttSink::handle_message`'s
//! `set_handlers`, `ConnectedDevice::notify_data`) run on a single shared task that
//! *all* devices depend on — the MQTT event loop, a device's own read loop with
//! multiple chained handlers. An uncaught panic there does not crash the process
//! (tokio catches panics per task), but it does take that whole shared task down
//! with it, which for the MQTT event loop means every device loses connectivity at
//! once. Wrapping each per-device/per-handler call with [`guard`] keeps a bad packet
//! or a buggy handler scoped to just that call.

use std::panic::{AssertUnwindSafe, catch_unwind};

/// Run `f`, converting a panic into a logged error instead of propagating the unwind.
/// `label` identifies what was running (e.g. a device id or handler name) for the log line.
pub fn guard(label: &str, f: impl FnOnce()) {
    if let Err(e) = catch_unwind(AssertUnwindSafe(f)) {
        let msg = e
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| e.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        eprintln!("[status] panic contained in {label}: {msg}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn panicking_call_is_contained_and_logged() {
        let ran_after = AtomicUsize::new(0);
        guard("thing-a", || panic!("bad packet"));
        guard("thing-b", || {
            ran_after.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(ran_after.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn non_panicking_call_runs_normally() {
        let ran = AtomicUsize::new(0);
        guard("thing", || {
            ran.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }
}
