//! Device handler trait and helpers for modelId registration.

use crate::property::PropertyValue;
use rusthinq_core::metadata::Metadata;
use rusthinq_core::mqtt::MqttConnection;
use rusthinq_core::thinq::{Thinq1Device, Thinq2Device};
use std::sync::Arc;

/// Platform a device runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Thinq1,
    Thinq2,
}

/// Trait implemented by every HA device handler.
pub trait DeviceHandler: Send + Sync {
    fn id(&self) -> &str;
    fn start(&self);
    fn drop_device(&self);
    fn set_property(&self, prop: &str, value: &str);
    fn publish_config(&self);
    /// Release timers and listeners this handler is holding, without touching HA's
    /// availability state. `drop_device()` should call this on its way to publishing
    /// offline, but it also runs on its own when a device is superseded by its own
    /// replacement before its close event fires (see `DeviceBridge::new_device`) — where
    /// publishing offline would only be a flicker, since the replacement is about to
    /// publish online under the same id. Default no-op for a handler with nothing to
    /// release.
    fn cancel_pending_work(&self) {}
}

/// Factory for ThinQ2 devices.
pub type T2Factory =
    fn(Arc<dyn MqttConnection>, Arc<dyn Thinq2Device>, Metadata) -> Arc<dyn DeviceHandler>;

/// Factory for ThinQ1 devices.
pub type T1Factory =
    fn(Arc<dyn MqttConnection>, Arc<dyn Thinq1Device>, Metadata) -> Arc<dyn DeviceHandler>;

/// Helper: property equality for publish cache (string form).
pub fn prop_str(v: &PropertyValue) -> String {
    v.as_string()
}
