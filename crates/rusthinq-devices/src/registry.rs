//! modelId → handler factory registry.
//!
//! Adding a new device is two steps:
//! 1. Implement `devices/<module>.rs` (`create` + `#[cfg(test)]` tests in that file)
//! 2. Add one line to the `register_devices!` table in `devices/mod.rs`

use crate::device_trait::{T1Factory, T2Factory};
#[cfg(feature = "scripting")]
use crate::scripting;

#[cfg(feature = "native")]
pub use crate::devices::{all_t1_model_ids, all_t2_model_ids, t1_factory, t2_factory};

/// Without the `native` feature, `devices/mod.rs` doesn't exist — stand in with
/// the same "nothing registered" answer its (currently empty) `register_devices!`
/// table already gives today.
macro_rules! native_stub {
    ($factory:ident -> $Factory:ty, $ids:ident) => {
        #[cfg(not(feature = "native"))]
        pub fn $factory(_model_id: &str) -> Option<$Factory> {
            None
        }

        #[cfg(not(feature = "native"))]
        pub fn $ids() -> Vec<&'static str> {
            Vec::new()
        }
    };
}

native_stub!(t1_factory -> T1Factory, all_t1_model_ids);
native_stub!(t2_factory -> T2Factory, all_t2_model_ids);

/// Lookup used by rusthinq-cloud's `device_bridge.rs`. Native handlers (`devices/mod.rs`) win first;
/// once none matches, a `.rhai` script for `model_id` (see `scripting`) is the
/// fallback rather than "unknown model" — checked by file existence only, so a
/// `model_id` with neither a native handler nor a script still falls through to
/// `None` exactly as before. Without the `scripting` feature there is no fallback
/// at all — this is just `t2_factory`/`t1_factory`.
macro_rules! lookup_fn {
    ($lookup:ident, $factory:ident -> $Factory:ty, $scripted:ident) => {
        #[cfg(feature = "scripting")]
        pub fn $lookup(model_id: &str) -> Option<$Factory> {
            $factory(model_id)
                .or_else(|| scripting::has_script_for(model_id).then_some(scripting::$scripted))
        }

        #[cfg(not(feature = "scripting"))]
        pub fn $lookup(model_id: &str) -> Option<$Factory> {
            $factory(model_id)
        }
    };
}

lookup_fn!(lookup_t1, t1_factory -> T1Factory, scripted_t1_factory);
lookup_fn!(lookup_t2, t2_factory -> T2Factory, scripted_t2_factory);

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
