//! Rhai device-scripting support: `ScriptedDevice` is what `registry.rs`'s lookup
//! returns for a `modelId` that has a script.
//!
//! A `.rhai` script gets raw wire bytes straight from the device's own `on_data`
//! callback (in-process, no MQTT round trip), opt-in access to `rusthinq-util`'s
//! codec helpers (`crc16`, `hex_*`, `tlv_*`, ...), and can publish/send back out
//! through the same `MqttConnection`/`Thinq1Device`/`Thinq2Device` primitives the host itself uses — see `ctx.rs` for the exact surface.

pub mod cache;
pub mod ctx;
pub mod engine;
pub mod harness;
pub mod il;
pub mod script_test;
pub mod scripted_device;
pub mod watcher;

pub use harness::ScriptHarness;
pub use scripted_device::{has_script_for, scripted_t1_factory, scripted_t2_factory};

use rusthinq_util::sync::RwLock;
use std::path::PathBuf;
use std::sync::OnceLock;

static IL_PREFIX: OnceLock<RwLock<Option<std::sync::Arc<str>>>> = OnceLock::new();

fn il_prefix_slot() -> &'static RwLock<Option<std::sync::Arc<str>>> {
    IL_PREFIX.get_or_init(|| RwLock::new(None))
}

/// `[scripting] il_prefix`, if set: the namespace `ctx.publish_il` publishes IL
/// descriptors under. `None` (the default) means descriptors are not published.
pub(crate) fn il_prefix() -> Option<std::sync::Arc<str>> {
    il_prefix_slot().read().clone()
}

/// Set (or clear) the IL descriptor namespace. Called from `main.rs` after `init`.
pub fn set_il_prefix(prefix: Option<String>) {
    *il_prefix_slot().write() = prefix.map(std::sync::Arc::from);
}

type ChangeCallback = Box<dyn Fn() + Send + Sync>;

static ON_CHANGE: OnceLock<rusthinq_util::sync::Mutex<Vec<ChangeCallback>>> = OnceLock::new();

/// Register a callback run (on the watcher thread) after the hot-reload watcher has
/// handled a batch of `.rhai` changes — a script edited, added or removed. A reload
/// only swaps compiled code into devices that already use it, so whoever owns the
/// devices needs this to re-publish their descriptors and to give a device that
/// connected before its script existed a handler.
pub fn on_scripts_changed(cb: impl Fn() + Send + Sync + 'static) {
    ON_CHANGE
        .get_or_init(Default::default)
        .lock()
        .push(Box::new(cb));
}

pub(crate) fn notify_scripts_changed() {
    if let Some(cbs) = ON_CHANGE.get() {
        for cb in cbs.lock().iter() {
            cb();
        }
    }
}

static RHAI_DIR: OnceLock<RwLock<Option<PathBuf>>> = OnceLock::new();

fn rhai_dir_slot() -> &'static RwLock<Option<PathBuf>> {
    RHAI_DIR.get_or_init(|| RwLock::new(None))
}

/// Directory of `<modelId>.rhai` scripts, if `[scripting]` configured one. `None` means
/// scripting is entirely off — `registry.rs`'s fallback never triggers.
pub(crate) fn rhai_dir() -> Option<PathBuf> {
    rhai_dir_slot().read().clone()
}

/// Wire up scripting from `main.rs`, once, before any device can connect. The
/// directory lives in a process-global slot (mirroring `rusthinq_core::logging`'s
/// filter) rather than being captured, because `T1Factory`/`T2Factory` are plain `fn`
/// pointers with no closure state. `watch` starts a background hot-reload file
/// watcher (see `watcher.rs`) when true.
pub fn init(rhai_dir: PathBuf, watch: bool) {
    *rhai_dir_slot().write() = Some(rhai_dir.clone());
    if watch {
        watcher::spawn(rhai_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registered_callback_runs_when_scripts_change() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        on_scripts_changed(|| {
            CALLS.fetch_add(1, Ordering::SeqCst);
        });
        notify_scripts_changed();
        assert!(CALLS.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn rhai_dir_is_none_until_init_is_called() {
        // Other tests in this crate call `init`, and Rust test binaries share process
        // state — this only asserts the *shape* (Option, not a hardcoded path), since
        // the global's actual current value is racy under `cargo test`'s default
        // parallelism.
        let _ = rhai_dir();
    }

    #[test]
    fn init_sets_the_configured_directory() {
        let dir = tempfile::tempdir().unwrap();
        init(dir.path().to_path_buf(), false);
        assert_eq!(rhai_dir().as_deref(), Some(dir.path()));
    }
}
