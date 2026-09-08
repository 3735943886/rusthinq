//! Optional web dashboard for rusthinq-cloud (the `gui` feature/`[gui]` config
//! section). See `mqtt.rs`'s doc comment for the central design point: this talks to
//! rusthinq-cloud only over the same MQTT control plane any other client would use.

mod http;
mod mqtt;
mod state;

pub use rusthinq_core::config::GuiConfig;

use anyhow::Result;
use rusthinq_core::config::MqttConfig;

pub async fn run(gui: GuiConfig, mqtt_cfg: MqttConfig) -> Result<()> {
    let shared = state::Shared::new();
    let handle = mqtt::start(mqtt_cfg, shared.clone())?;
    http::serve(gui.bind, shared, handle).await
}
