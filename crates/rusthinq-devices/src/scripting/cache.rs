//! Compiled-script cache, keyed by absolute path — shared across every model that
//! happens to point at the same `.rhai` file, and the thing the hot-reload watcher
//! swaps into place.
//!
//! Each cached entry is `Arc<RwLock<Arc<AST>>>`: `ScriptedDevice` holds the outer
//! `Arc` (cheap to clone, shared with anyone else using the same script) and reads
//! the inner `Arc<AST>` fresh on every script call (`slot.read().clone()`, a cheap
//! `Arc` clone) — so a successful reload is visible to already-connected devices on
//! their very next call, with no need to reconnect.

use crate::scripting::engine::engine;
use rhai::AST;
use rusthinq_util::sync::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

pub type AstSlot = Arc<RwLock<Arc<AST>>>;

static CACHE: LazyLock<Mutex<HashMap<PathBuf, AstSlot>>> = LazyLock::new(Default::default);

fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// `Engine::compile_file` (rather than `read_to_string` + `compile`) so the resulting
/// `AST` records its own path as its source (`AST::set_source`) — that's what lets a
/// script's top-level `import "common" as common;` resolve `common.rhai` relative to
/// the *importing script's own directory* (rhai's default `FileModuleResolver` falls
/// back to the importing AST's source path when no explicit base path is configured),
/// i.e. the same `rhai_dir` every device script already lives in. Shared logic
/// therefore doesn't need any dedicated support here — it's a plain `.rhai` file next
/// to the device scripts that import it.
fn compile_file(path: &Path) -> Result<AST, String> {
    engine()
        .compile_file(path.to_path_buf())
        .map_err(|e| format!("{}: {e}", path.display()))
}

/// Get the (possibly already-compiled) slot for `path`, compiling on first use.
pub fn get_or_compile(path: &Path) -> Result<AstSlot, String> {
    let path = canonical(path);
    if let Some(slot) = CACHE.lock().get(&path) {
        return Ok(slot.clone());
    }
    let ast = compile_file(&path)?;
    let slot: AstSlot = Arc::new(RwLock::new(Arc::new(ast)));
    CACHE.lock().insert(path, slot.clone());
    Ok(slot)
}

/// Recompile `path` and, on success, atomically swap it into the cached slot so every
/// `ScriptedDevice` sharing it picks up the change on their next call. A path with no
/// cached slot (no device has used it yet) is a no-op — the next connection compiles
/// it fresh via `get_or_compile`. On a compile error the old AST is left running and
/// the error is returned for the watcher to log — a bad save must never stop an
/// already-connected device.
pub fn reload(path: &Path) -> Result<(), String> {
    let path = canonical(path);
    let Some(slot) = CACHE.lock().get(&path).cloned() else {
        return Ok(());
    };
    let ast = compile_file(&path)?;
    *slot.write() = Arc::new(ast);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_script(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn compiles_once_and_reuses_the_same_slot() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(dir.path(), "a.rhai", "fn start(ctx) {}");
        let a = get_or_compile(&path).unwrap();
        let b = get_or_compile(&path).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn reload_swaps_the_ast_visible_through_the_same_slot() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(dir.path(), "b.rhai", "fn marker() { 1 }");
        let slot = get_or_compile(&path).unwrap();
        let before = slot.read().clone();

        write_script(dir.path(), "b.rhai", "fn marker() { 2 }");
        reload(&path).unwrap();

        let after = slot.read().clone();
        assert!(!Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn reload_keeps_the_old_ast_on_a_compile_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_script(dir.path(), "c.rhai", "fn marker() { 1 }");
        let slot = get_or_compile(&path).unwrap();
        let before = slot.read().clone();

        write_script(dir.path(), "c.rhai", "fn marker( {{{ not valid rhai");
        assert!(reload(&path).is_err());

        let after = slot.read().clone();
        assert!(Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn reload_of_an_unknown_path_is_a_silent_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("never-compiled.rhai");
        std::fs::write(&path, "fn marker() {}").unwrap();
        assert!(reload(&path).is_ok());
    }
}
