//! Persisted "every device id ever seen, and when" ledger — independent of how (or
//! whether) anything drives its properties.
//!
//! `MqttSink::known_devices` (`rusthinq-core`) only sees ids that went through
//! `publish_property`, which a native Rust handler or `.rhai` script calls but the
//! raw bus (`raw_bus.rs`) never does -- it only ever calls `publish_raw`. A
//! deployment with no native/script handler for any of its models (a real, common
//! setup: raw bus + an external consumer, e.g. `rethink`'s TS adapter, driving
//! everything) never gets a single entry there no matter how many devices connect,
//! since nothing ever calls `publish_property` for them. This tracks every id
//! `DeviceManager::accept()` ever saw instead, which *every* connected device goes
//! through regardless of what (if anything) ends up driving its properties -- see
//! `main.rs`'s `manager.on_new_device`/`on_drop_device` wiring.

use crate::devmgr::Platform;
use rusthinq_core::metadata::Metadata;
use rusthinq_util::sync::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownDevice {
    pub last_seen_unix: i64,
    #[serde(flatten)]
    pub meta: Metadata,
    pub platform: String,
}

/// Shared across `main.rs`'s `DeviceManager` hooks, `devlist.rs` (reads it for the
/// offline listing), and `device_control.rs` (forget removes an entry).
pub struct KnownDevices {
    path: Option<PathBuf>,
    entries: Mutex<HashMap<String, KnownDevice>>,
}

impl KnownDevices {
    pub fn new(path: Option<PathBuf>) -> Arc<Self> {
        let entries = path.as_deref().map(load).unwrap_or_default();
        Arc::new(Self {
            path,
            entries: Mutex::new(entries),
        })
    }

    /// Call on every `DeviceManager::on_new_device` -- records/refreshes full
    /// metadata and bumps `last_seen_unix` to now.
    pub fn note_connected(&self, id: &str, meta: &Metadata, platform: Platform) {
        self.entries.lock().insert(
            id.to_string(),
            KnownDevice {
                last_seen_unix: now_unix(),
                meta: meta.clone(),
                platform: platform.as_str().to_string(),
            },
        );
        self.save();
    }

    /// Call on every `DeviceManager::on_drop_device` -- the actual close event, no
    /// grace period, so this is the most accurate "last seen" a genuine disconnect
    /// can give. A no-op if the id was never `note_connected` (shouldn't happen,
    /// but a stale/duplicate close event must not fabricate an entry).
    pub fn note_disconnected(&self, id: &str) {
        let mut entries = self.entries.lock();
        if let Some(e) = entries.get_mut(id) {
            e.last_seen_unix = now_unix();
        } else {
            return;
        }
        drop(entries);
        self.save();
    }

    /// Wired to `rusthinq_bridge::Bridge`'s `on_storage_orphaned` hook -- a
    /// `[bridge].storage_path` id this process's `DeviceManager` never actually
    /// saw connect this run (e.g. gone since before this process started), so
    /// there's no real `Metadata` for it. Only inserted if nothing already knows
    /// the id (never overwrites a richer `note_connected` entry), and never
    /// updates `last_seen_unix` on an existing entry either -- this only means
    /// "found to be gone," not "seen."
    #[cfg_attr(
        not(feature = "bridge"),
        allow(dead_code, reason = "only ever called from main.rs's bridge-only hook wiring")
    )]
    pub fn note_orphaned(&self, id: &str) {
        let mut entries = self.entries.lock();
        if entries.contains_key(id) {
            return;
        }
        entries.insert(
            id.to_string(),
            KnownDevice {
                last_seen_unix: 0,
                meta: Metadata {
                    model_id: String::new(),
                    model_name: String::new(),
                    device_type: None,
                    sw_version: None,
                },
                platform: String::new(),
            },
        );
        drop(entries);
        self.save();
    }

    /// Every id this ledger has ever recorded, live or not -- `devlist.rs` filters
    /// out whichever ids `DeviceManager::all()` currently has live.
    pub fn all(&self) -> HashMap<String, KnownDevice> {
        self.entries.lock().clone()
    }

    /// Erases an id regardless of whether it's currently connected -- the
    /// counterpart to `MqttConnection::clear_retained` for this ledger, called by
    /// `device_control.rs`'s `forget`.
    pub fn forget(&self, id: &str) {
        self.entries.lock().remove(id);
        self.save();
    }

    fn save(&self) {
        let Some(path) = &self.path else { return };
        let entries = self.entries.lock();
        match serde_json::to_string(&*entries) {
            Ok(json) => {
                if let Err(e) = rusthinq_core::atomic_file::write(path, json.as_bytes()) {
                    tracing::warn!(
                        error = %e,
                        path = %path.display(),
                        "failed to persist known-devices state"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize known-devices state");
            }
        }
    }
}

fn load(path: &std::path::Path) -> HashMap<String, KnownDevice> {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_else(|e| {
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "known-devices state file is corrupt, starting from an empty ledger"
            );
            HashMap::default()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::default(),
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "failed to read known-devices state, starting from an empty ledger"
            );
            HashMap::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_connected_then_disconnected_keeps_the_entry_and_bumps_last_seen() {
        let known = KnownDevices::new(None);
        let meta = Metadata::new("MODEL_A", "Model A", "1.0");
        known.note_connected("dev-1", &meta, Platform::Thinq2);
        let first_seen = known.all()["dev-1"].last_seen_unix;

        known.note_disconnected("dev-1");
        let all = known.all();
        assert_eq!(all.len(), 1, "disconnect must not remove the entry");
        assert!(all["dev-1"].last_seen_unix >= first_seen);
        assert_eq!(all["dev-1"].meta.model_id, "MODEL_A");
    }

    #[test]
    fn note_disconnected_is_a_no_op_for_an_unknown_id() {
        let known = KnownDevices::new(None);
        known.note_disconnected("never-connected");
        assert!(known.all().is_empty());
    }

    #[test]
    fn note_orphaned_inserts_a_bare_entry_but_never_overwrites_a_richer_one() {
        let known = KnownDevices::new(None);
        known.note_orphaned("dev-bridge-only");
        let all = known.all();
        assert_eq!(all.len(), 1);
        assert_eq!(all["dev-bridge-only"].meta.model_id, "");

        let meta = Metadata::new("MODEL_A", "Model A", "1.0");
        known.note_connected("dev-1", &meta, Platform::Thinq2);
        known.note_orphaned("dev-1");
        assert_eq!(
            known.all()["dev-1"].meta.model_id,
            "MODEL_A",
            "note_orphaned must not clobber an entry that already has real metadata"
        );
    }

    #[test]
    fn forget_removes_the_entry() {
        let known = KnownDevices::new(None);
        let meta = Metadata::new("MODEL_A", "Model A", "1.0");
        known.note_connected("dev-1", &meta, Platform::Thinq2);
        known.forget("dev-1");
        assert!(known.all().is_empty());
    }

    #[test]
    fn survives_a_restart_via_its_state_file() {
        let path = std::env::temp_dir().join(format!(
            "rusthinq-known-devices-test-{}-{}.json",
            std::process::id(),
            line!()
        ));
        let _ = std::fs::remove_file(&path);

        let meta = Metadata::new("MODEL_A", "Model A", "1.0");
        let known = KnownDevices::new(Some(path.clone()));
        known.note_connected("dev-1", &meta, Platform::Thinq2);

        let restarted = KnownDevices::new(Some(path.clone()));
        let all = restarted.all();
        assert_eq!(all.len(), 1);
        assert_eq!(all["dev-1"].meta.model_id, "MODEL_A");

        let _ = std::fs::remove_file(&path);
    }

    /// A truncated/corrupt state file (what a crash mid-`fs::write` used to be able
    /// to leave behind before `save` switched to `atomic_file::write`) must start
    /// the ledger empty rather than panicking, and a subsequent save must fully
    /// replace it.
    #[test]
    fn corrupt_state_file_loads_as_empty_and_a_save_replaces_it_cleanly() {
        let path = std::env::temp_dir().join(format!(
            "rusthinq-known-devices-corrupt-test-{}-{}.json",
            std::process::id(),
            line!()
        ));
        std::fs::write(&path, b"{\"dev-1\":{\"last_seen").unwrap();

        let known = KnownDevices::new(Some(path.clone()));
        assert!(known.all().is_empty());

        let meta = Metadata::new("MODEL_A", "Model A", "1.0");
        known.note_connected("dev-1", &meta, Platform::Thinq2);

        let restarted = KnownDevices::new(Some(path.clone()));
        assert_eq!(restarted.all()["dev-1"].meta.model_id, "MODEL_A");

        let _ = std::fs::remove_file(&path);
    }
}
