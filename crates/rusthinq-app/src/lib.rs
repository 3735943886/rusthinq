//! L6 application foundations: owns persistence, not device protocol decisions.
pub mod cleanup_mqtt;
pub mod daemon;
#[cfg(feature = "scripting")]
pub mod drivers;
pub mod external_mqtt;
pub mod lifecycle_cleanup;
pub mod lifecycle_storage;
pub mod management;
pub mod retained_cleanup;
pub mod runtime;
#[cfg(feature = "scripting")]
pub mod scripts;
pub mod tls_runtime;

#[cfg(feature = "scripting")]
pub mod driver_watch;

#[cfg(feature = "scripting")]
pub(crate) mod mqtt_commands;

#[cfg(feature = "bridge")]
pub mod cloud_account;

#[cfg(feature = "bridge")]
pub mod pairing_storage;

pub mod api;

pub mod script_types;
#[cfg(not(feature = "scripting"))]
pub mod scripts {
    pub use crate::script_types::{Context, PublishSink};
}

pub mod driver_config;
#[cfg(not(feature = "scripting"))]
pub mod drivers {
    pub use crate::driver_config::Config;
}

#[cfg(not(feature = "bridge"))]
#[path = "cloud_disabled.rs"]
pub mod cloud_account;
