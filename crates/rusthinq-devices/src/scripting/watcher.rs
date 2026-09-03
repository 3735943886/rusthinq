//! Hot-reload: watch `rhai_dir` for `.rhai` saves and swap the compiled cache entry
//! for whichever path changed (see `cache::reload`). Runs on its own `std::thread` —
//! events are rare enough that a dedicated OS thread beats wiring this into tokio.

use notify_debouncer_mini::{DebounceEventResult, new_debouncer, notify::RecursiveMode};
use std::path::PathBuf;
use std::time::Duration;

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
