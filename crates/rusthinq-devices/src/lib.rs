//! Device handlers and modelId registry for rusthinq.
//!
//! Consumer-neutral: `device_base`/`property` model device protocol/state only.

pub mod device_base;
pub mod device_trait;
#[cfg(feature = "native")]
pub mod devices;
pub mod property;
pub mod registry;
#[cfg(feature = "scripting")]
pub mod scripting;

#[cfg(test)]
pub mod test_support;

pub use device_base::{unwrap_aabb, wrap_aabb};
pub use device_trait::DeviceHandler;
pub use property::PropertyValue;
pub use registry::{all_t1_model_ids, all_t2_model_ids, t1_factory, t2_factory};
