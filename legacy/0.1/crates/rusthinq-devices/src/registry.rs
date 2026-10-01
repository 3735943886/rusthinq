//! modelId → handler factory lookup.
//!
//! A device is driven by a `<modelId>.rhai` script (see `scripting`); there are no
//! built-in per-model handlers. The lookup is by file existence only, so a `model_id`
//! with no script is unknown (`None`). Without the `scripting` feature nothing can be
//! looked up at all.

use crate::device_trait::{T1Factory, T2Factory};
#[cfg(feature = "scripting")]
use crate::scripting;

macro_rules! lookup_fn {
    ($lookup:ident, $Factory:ty, $scripted:ident) => {
        #[cfg(feature = "scripting")]
        pub fn $lookup(model_id: &str) -> Option<$Factory> {
            scripting::has_script_for(model_id).then_some(scripting::$scripted)
        }

        #[cfg(not(feature = "scripting"))]
        pub fn $lookup(_model_id: &str) -> Option<$Factory> {
            None
        }
    };
}

lookup_fn!(lookup_t1, T1Factory, scripted_t1_factory);
lookup_fn!(lookup_t2, T2Factory, scripted_t2_factory);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_model_without_a_script_stays_unknown() {
        // Whether or not some other test has pointed the process-global `rhai_dir`
        // at a tempdir, a model_id no `.rhai` file was ever written for is `None`.
        const BOGUS: &str = "REGISTRY_TEST_NO_SUCH_MODEL_EVER";
        assert!(lookup_t2(BOGUS).is_none());
        assert!(lookup_t1(BOGUS).is_none());
    }
}
