//! Device metadata shared across ThinQ platforms.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(rename = "modelId")]
    pub model_id: String,
    #[serde(rename = "modelName")]
    pub model_name: String,
    #[serde(
        rename = "deviceType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub device_type: Option<String>,
    #[serde(rename = "swVersion", default, skip_serializing_if = "Option::is_none")]
    pub sw_version: Option<String>,
}

impl Metadata {
    pub fn new(
        model_id: impl Into<String>,
        model_name: impl Into<String>,
        sw_version: impl Into<String>,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            model_name: model_name.into(),
            device_type: None,
            sw_version: Some(sw_version.into()),
        }
    }
}
