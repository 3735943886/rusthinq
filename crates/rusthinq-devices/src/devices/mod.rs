//! Per-model device handler modules.
//!
//! To add a new device:
//! 1. Create `devices/<module>.rs` with `create(...)` and `#[cfg(test)]` tests
//! 2. Add **one line** to the `register_devices!` table below
//!
//! Module names intentionally track LG `modelId` strings (double underscores, etc.).
//!
//! Pruned of all upstream device handlers to start this fork's own device set from
//! scratch (see `registry.rs` for the modelId -> factory lookup this feeds).

#![allow(non_snake_case)]

/// Declare modules + `modelId` factories from one table.
///
/// Syntax: `module_name => { "MODEL_ID" | "ALIAS" }`.
/// `t2` is ThinQ2 (`T2Factory`); `t1` is ThinQ1 (`T1Factory`).
macro_rules! register_devices {
    (
        t2 { $( $t2mod:ident => { $($t2id:literal)|+ } ),* $(,)? }
        t1 { $( $t1mod:ident => { $($t1id:literal)|+ } ),* $(,)? }
    ) => {
        $( pub mod $t2mod; )*
        $( pub mod $t1mod; )*

        pub fn t2_factory(model_id: &str) -> Option<crate::device_trait::T2Factory> {
            match model_id {
                $( $($t2id)|+ => Some($t2mod::create), )*
                _ => None,
            }
        }

        pub fn t1_factory(model_id: &str) -> Option<crate::device_trait::T1Factory> {
            match model_id {
                $( $($t1id)|+ => Some($t1mod::create), )*
                _ => None,
            }
        }

        pub fn all_t2_model_ids() -> Vec<&'static str> {
            vec![ $( $($t2id,)+ )* ]
        }

        pub fn all_t1_model_ids() -> Vec<&'static str> {
            vec![ $( $($t1id,)+ )* ]
        }
    };
}

register_devices! {
    t2 {
    }
    t1 {
    }
}
