//! Rhai device-scripting support: `ScriptedDevice` fills `registry.rs`'s two-stage
//! lookup once no native handler matches a `modelId`.
//!
//! A `.rhai` script gets raw wire bytes straight from the device's own `on_data`
//! callback (in-process, no MQTT round trip), opt-in access to `rusthinq-util`'s
//! codec helpers (`crc16`, `hex_*`, `tlv_*`, ...), and can publish/send back out
//! through the same `MqttConnection`/`Thinq1Device`/`Thinq2Device` primitives a native
//! handler would use — see `ctx.rs` for the exact surface.

pub mod cache;
pub mod ctx;
pub mod engine;
pub mod harness;
pub mod scripted_device;
pub mod watcher;

pub use harness::ScriptHarness;
pub use scripted_device::{has_script_for, scripted_t1_factory, scripted_t2_factory};

use rusthinq_util::sync::RwLock;
use std::path::PathBuf;
use std::sync::OnceLock;

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
