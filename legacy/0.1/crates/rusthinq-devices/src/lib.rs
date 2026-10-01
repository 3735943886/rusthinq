//! Device handler trait, modelId lookup and the Rhai scripting engine for rusthinq.
//!
//! Consumer-neutral: nothing here knows about any specific downstream integration.

pub mod aabb;
pub mod device_trait;
pub mod registry;
#[cfg(feature = "scripting")]
pub mod scripting;

pub use aabb::{unwrap_aabb, wrap_aabb};
pub use device_trait::DeviceHandler;
