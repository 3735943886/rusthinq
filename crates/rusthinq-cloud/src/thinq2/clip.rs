//! CLIP message types for ThinQ2.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// CLIP JSON envelope (device ↔ cloud). Kept for typed parsing / docs.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipMessage {
    pub mid: Option<i64>,
    pub did: String,
    #[serde(default)]
    pub kind: Option<String>,
    pub cmd: String,
    #[serde(default)]
    pub rssi: Option<i64>,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub r#type: Option<i64>,
}
