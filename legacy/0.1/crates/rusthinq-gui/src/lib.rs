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
    let auth = match (gui.gui_user, gui.gui_pass) {
        (Some(user), Some(pass)) => Some(http::BasicAuthCreds { user, pass }),
        (None, None) => None,
        _ => {
            tracing::warn!(
                "[gui] has only one of gui_user/gui_pass set -- ignoring, dashboard is \
                 unauthenticated"
            );
            None
        }
    };
    let shared = state::Shared::new();
    let handle = mqtt::start(mqtt_cfg, shared.clone())?;
    let address = gui.gui_port.address().unwrap_or("0.0.0.0").to_string();
    http::serve(&address, gui.gui_port.port(), auth, shared, handle).await
}
