//! L6 application foundations: owns persistence, not device protocol decisions.
pub mod cleanup_mqtt;
pub mod daemon;
pub mod lifecycle_cleanup;
pub mod lifecycle_storage;
pub mod retained_cleanup;
pub mod runtime;
pub mod scripts;
pub mod tls_runtime;
