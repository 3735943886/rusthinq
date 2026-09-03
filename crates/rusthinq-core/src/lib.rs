//! Core rusthinq types: config, MQTT transport, ThinQ1/2 device plumbing.
//!
//! Deliberately consumer-neutral — no device-modeling schema and no downstream
//! integration's conventions (Home Assistant or otherwise) live here. See
//! `rusthinq-devices` for device state/protocol base classes.

pub mod config;
pub mod logging;
pub mod metadata;
pub mod mqtt;
pub mod panic_guard;
pub mod thinq;

pub use metadata::Metadata;
pub use mqtt::{MockMqttConnection, MqttConnection};
pub use thinq::{
    MockThinq1Device, MockThinq2Device, Thinq1Device, Thinq2Device, hex_decode, hex_encode,
};
