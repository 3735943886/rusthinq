//! Pure protocol utilities for rusthinq — LG ThinQ de-cloud codecs.
//!
//! These modules have no I/O dependencies and are safe to unit-test in isolation.

pub mod aabb_analysis;
pub mod backoff;
pub mod crc16;
pub mod decode;
pub mod hex;
pub mod json_splitter;
pub mod length_prefixed_frame;
pub mod mtosp;
pub mod packet_codec;
pub mod sync;
pub mod tlv;
pub mod tlv_catalog;
pub mod uart_binary;

pub use aabb_analysis::{AabbAnalysis, aabb_export_text, analyze_aabb_body};
pub use backoff::ExponentialBackoff;
pub use crc16::crc16;
pub use packet_codec::{Decoded, Direction, EncodeInput, Protocol, decode_packet, encode_packet};
pub use sync::{Mutex, RwLock};
pub use tlv::{Tlv, build as tlv_build, parse as tlv_parse};
pub use uart_binary::{UartBinaryAnalysis, analyze_uart_binary, uart_binary_export_text};

/// `rumqttc::MqttOptions::set_keep_alive` for every rusthinq-owned MQTT client talking
/// to a persistent broker (the control-plane client, the LG-cloud ThinQ2 bridge
/// session, the RE packet CLI tools) — one value so they can't drift apart by accident.
/// Not for a short-lived one-shot connection with its own tighter timing needs (see
/// rusthinq-tools::mqtt's own 5s, used only for quick CLI fetch/publish round trips).
pub const MQTT_KEEP_ALIVE: std::time::Duration = std::time::Duration::from_secs(30);
