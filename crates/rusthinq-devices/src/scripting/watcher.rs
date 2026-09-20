//! Hot-reload: watch `rhai_dir` for `.rhai` saves and swap the compiled cache entry
//! for whichever path changed (see `cache::reload`). Runs on its own `std::thread` —
//! events are rare enough that a dedicated OS thread beats wiring this into tokio.

use notify_debouncer_mini::{DebounceEventResult, new_debouncer, notify::RecursiveMode};
use rusthinq_util::sync::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

/// Debounce window for the common "save = several rapid write events" case.
const DEBOUNCE: Duration = Duration::from_millis(250);

/// Start watching `dir` for `.rhai` changes. Never returns — call from a
/// `std::thread::spawn`, not the async runtime.
pub fn spawn(dir: PathBuf) {
    std::thread::spawn(move || run(dir));
}

fn run(dir: PathBuf) {
    let mut debouncer = match new_debouncer(DEBOUNCE, handle_events) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                target: "rusthinq_scripting",
                error = %e,
                dir = %dir.display(),
                "failed to start rhai hot-reload watcher"
            );
            return;
        }
    };

    if let Err(e) = debouncer.watcher().watch(&dir, RecursiveMode::NonRecursive) {
        tracing::warn!(
            target: "rusthinq_scripting",
            error = %e,
            dir = %dir.display(),
            "failed to watch rhai_dir for hot-reload"
        );
        return;
    }

    tracing::info!(target: "rusthinq_scripting", dir = %dir.display(), "watching for .rhai script changes");

    // Keep `debouncer` (and thus the watch) alive for the life of the process; this
    // thread has nothing else to do.
    loop {
        std::thread::park();
    }
}

/// True if `path` was written since the last time this returned true for it. The
/// debouncer reports *any* inotify event, and reloading a script opens and reads it (and
/// its imports), which is itself an event — without this check every reload triggers the
/// next one, forever.
fn modified_since_last_seen(path: &Path) -> bool {
    static SEEN: OnceLock<Mutex<HashMap<PathBuf, (SystemTime, u64)>>> = OnceLock::new();
    let Ok(meta) = std::fs::metadata(path) else {
        // Removed or unreadable: let the reload path report it, and forget the entry.
        SEEN.get_or_init(Default::default).lock().remove(path);
        return true;
    };
    let stamp = (
        meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        meta.len(),
    );
    SEEN.get_or_init(Default::default)
        .lock()
        .insert(path.to_path_buf(), stamp)
        != Some(stamp)
}

fn handle_events(result: DebounceEventResult) {
    let events = match result {
        Ok(events) => events,
        Err(e) => {
            tracing::warn!(target: "rusthinq_scripting", error = %e, "rhai_dir watch error");
            return;
        }
    };

    for event in events {
        if event.path.extension().and_then(|e| e.to_str()) != Some("rhai") {
            continue;
        }
        if !modified_since_last_seen(&event.path) {
            continue;
        }

        // Independent of whether `event.path` is a top-level device script (handled
        // below) or a module some script `import`s (e.g. shared logic) — a no-op if
        // it was never resolved as one.
        crate::scripting::engine::invalidate_module(&event.path);

        match crate::scripting::cache::reload(&event.path) {
            Ok(()) => {
                tracing::info!(
                    target: "rusthinq_scripting",
                    path = %event.path.display(),
                    "hot-reloaded rhai script"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "rusthinq_scripting",
                    path = %event.path.display(),
                    error = %e,
                    "rhai hot-reload failed — keeping the previously running script"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_only_event_is_not_a_change_but_a_write_is() {
        let dir = std::env::temp_dir().join(format!("rhai-watch-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("x.rhai");
        std::fs::write(&f, "fn a() {}").unwrap();

        assert!(modified_since_last_seen(&f), "first sighting reloads");
        let _ = std::fs::read(&f); // what a reload itself does
        assert!(
            !modified_since_last_seen(&f),
            "reading it again is not a change"
        );

        std::fs::write(&f, "fn a() { 1 }").unwrap();
        assert!(modified_since_last_seen(&f), "a real save reloads");
        assert!(!modified_since_last_seen(&f));

        std::fs::remove_file(&f).unwrap();
        assert!(
            modified_since_last_seen(&f),
            "a removal still reaches reload"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
