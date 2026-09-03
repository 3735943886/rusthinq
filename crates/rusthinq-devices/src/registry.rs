//! modelId → handler factory registry.
//!
//! Adding a new device is two steps:
//! 1. Implement `devices/<module>.rs` (`create` + `#[cfg(test)]` tests in that file)
//! 2. Add one line to the `register_devices!` table in `devices/mod.rs`

use crate::device_trait::{T1Factory, T2Factory};
use crate::scripting;

pub use crate::devices::{all_t1_model_ids, all_t2_model_ids, t1_factory, t2_factory};

/// Lookup used by ha_bridge / cloud. Native handlers (`devices/mod.rs`) win first;
/// once none matches, a `.rhai` script for `model_id` (see `scripting`) is the
/// fallback rather than "unknown model" — checked by file existence only, so a
/// `model_id` with neither a native handler nor a script still falls through to
/// `None` exactly as before.
pub fn lookup_t2(model_id: &str) -> Option<T2Factory> {
    t2_factory(model_id)
        .or_else(|| scripting::has_script_for(model_id).then_some(scripting::scripted_t2_factory))
}

pub fn lookup_t1(model_id: &str) -> Option<T1Factory> {
    t1_factory(model_id)
        .or_else(|| scripting::has_script_for(model_id).then_some(scripting::scripted_t1_factory))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_t2_models_have_factories() {
        for id in all_t2_model_ids() {
            assert!(t2_factory(id).is_some(), "missing t2 factory for {id}");
        }
    }

    #[test]
    fn all_t1_models_have_factories() {
        for id in all_t1_model_ids() {
            assert!(t1_factory(id).is_some(), "missing t1 factory for {id}");
        }
    }

    #[test]
    fn a_model_with_neither_native_handler_nor_script_stays_unknown() {
        // Regardless of whether some other test has pointed the process-global
        // `rhai_dir` at a tempdir (scripting on) or left it `None` (scripting off),
        // a model_id nothing has ever registered a `.rhai` file for must still fall
        // through to `None`, exactly like before scripting existed.
        const BOGUS: &str = "REGISTRY_TEST_NO_SUCH_MODEL_EVER";
        assert!(lookup_t2(BOGUS).is_none());
        assert!(lookup_t1(BOGUS).is_none());
    }
}
