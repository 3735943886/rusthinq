//! L6 application foundations: owns persistence, not device protocol decisions.
pub mod cleanup_mqtt;
pub mod daemon;
pub mod drivers;
pub mod external_mqtt;
pub mod lifecycle_cleanup;
pub mod lifecycle_storage;
pub mod management;
pub mod retained_cleanup;
pub mod runtime;
pub mod scripts;
pub mod tls_runtime;

pub mod driver_watch;

pub(crate) mod mqtt_commands;

pub mod cloud_account;

pub mod pairing_storage;

pub mod api;
