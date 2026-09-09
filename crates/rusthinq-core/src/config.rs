//! Config loading and normalization (TOML).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    pub mqtt_url: String,
    pub rusthinq_prefix: String,
    #[serde(default)]
    pub mqtt_user: String,
    #[serde(default)]
    pub mqtt_pass: String,
    /// Topic prefix for the raw wire-frame observer/inject bus (`raw_bus.rs`) and the
    /// device simulator (`sim_device.rs`) — everything that lets a trusted local tool
    /// watch or fake device traffic. Unset by default, and then neither registers at
    /// all: no debug/inject surface exists anywhere, not even on this already-trusted
    /// connection. Set it to enable them; it can equal `rusthinq_prefix` or be its own
    /// value if the broker's ACLs should be able to gate this surface separately from
    /// the rest of the control plane.
    #[serde(default)]
    pub raw_prefix: Option<String>,
    /// Where to persist the `id -> property names ever published` map used by
    /// `MqttConnection::clear_retained` to clean up a permanently-gone device's
    /// retained topics (see `mqtt.rs`). Unlike `raw_prefix`/`bridge`, this isn't an
    /// opt-in feature — it fixes a side effect of `publish_property` itself, so it's
    /// on by default; leaving it unset just means `main.rs` fills in a default path
    /// next to the config file rather than the feature being off.
    #[serde(default)]
    pub state_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PortSpec {
    Number(u16),
    Full {
        bind: u16,
        advertise: u16,
        #[serde(default)]
        address: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Port {
    pub bind: u16,
    pub advertise: u16,
    pub address: Option<String>,
}

impl From<PortSpec> for Port {
    fn from(p: PortSpec) -> Self {
        match p {
            PortSpec::Number(n) => Port {
                bind: n,
                advertise: n,
                address: None,
            },
            PortSpec::Full {
                bind,
                advertise,
                address,
            } => Port {
                bind,
                advertise,
                address,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeConfig {
    pub storage_path: String,
}

/// Rhai device-scripting support (`rusthinq-devices::scripting`). Absent `[devices]`
/// means the feature doesn't exist at all — `registry.rs`'s script fallback never
/// triggers, same as `raw_prefix`/`bridge` being unset for their own opt-in features.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevicesConfig {
    /// Directory of `<modelId>.rhai` scripts.
    pub rhai_dir: String,
    /// Hot-reload `rhai_dir` on save. Off by default.
    #[serde(default)]
    pub watch: bool,
}

/// Optional web dashboard (`rusthinq-gui`, only compiled in with the `gui`
/// feature). Absent `[gui]` means it doesn't run at all, same opt-in-by-presence
/// pattern as `[bridge]`/`[devices]`. It talks to `mqtt.mqtt_url` as its own MQTT
/// client, same as any other tool would -- nothing here is a second control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuiConfig {
    /// Local TCP port the dashboard's HTTP server binds to.
    pub bind: u16,
    /// HTTP Basic Auth, checked on every request when both are set (named after
    /// `mqtt_user`/`mqtt_pass` above). Left unset, the dashboard is unauthenticated
    /// -- it binds `0.0.0.0` and can enable/disable bridging, trigger LG
    /// login/logout, and read raw device traffic, so that's only reasonable on a
    /// trusted LAN.
    #[serde(default)]
    pub gui_user: Option<String>,
    #[serde(default)]
    pub gui_pass: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawConfig {
    pub hostname: String,
    pub mqtt: MqttConfig,
    pub ca_key_file: String,
    pub ca_cert_file: String,
    pub https_port: PortSpec,
    pub mqtts_port: PortSpec,
    #[serde(default)]
    pub thinq1_https_port: Option<PortSpec>,
    #[serde(default)]
    pub thinq1_port: Option<PortSpec>,
    /// Whether to connect out to `mqtt.mqtt_url` at all. Defaults on; the only reason
    /// to turn it off is running with the `[mqtt]` table present but temporarily not
    /// wanting the daemon to actually dial it (e.g. during a migration).
    #[serde(default)]
    pub mqtt_enabled: Option<bool>,
    #[serde(default)]
    pub bridge: Option<BridgeConfig>,
    #[serde(default)]
    pub devices: Option<DevicesConfig>,
    #[serde(default)]
    pub gui: Option<GuiConfig>,
    #[serde(default)]
    pub log: Option<Vec<String>>,
    /// Let `/route` echo back the hostname a connecting appliance actually requested
    /// instead of always answering `hostname`. Off by default — only useful for an
    /// appliance adopted by port-redirection (DNAT) rather than SoftAP setup; see
    /// anszom/rethink#107. Ignored (falls back to `hostname`) for anything that isn't a
    /// plausible DNS hostname, including bare IP addresses.
    #[serde(default)]
    pub advertise_requested_host: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub hostname: String,
    pub mqtt: MqttConfig,
    pub ca_key_file: String,
    pub ca_cert_file: String,
    pub https_port: Port,
    pub mqtts_port: Port,
    pub thinq1_https_port: Port,
    pub thinq1_port: Port,
    pub mqtt_enabled: bool,
    pub bridge: Option<BridgeConfig>,
    pub devices: Option<DevicesConfig>,
    pub gui: Option<GuiConfig>,
    pub log: Vec<String>,
    pub advertise_requested_host: bool,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Toml(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Toml(s) => write!(f, "TOML parse error: {s}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Toml(_) => None,
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub fn normalize(raw: RawConfig) -> Config {
    Config {
        hostname: raw.hostname,
        mqtt: raw.mqtt,
        ca_key_file: raw.ca_key_file,
        ca_cert_file: raw.ca_cert_file,
        https_port: raw.https_port.into(),
        mqtts_port: raw.mqtts_port.into(),
        thinq1_https_port: raw.thinq1_https_port.map(Into::into).unwrap_or(Port {
            bind: 46030,
            advertise: 46030,
            address: None,
        }),
        thinq1_port: raw.thinq1_port.map(Into::into).unwrap_or(Port {
            bind: 47878,
            advertise: 47878,
            address: None,
        }),
        mqtt_enabled: raw.mqtt_enabled.unwrap_or(true),
        bridge: raw.bridge,
        devices: raw.devices,
        gui: raw.gui,
        log: raw
            .log
            .unwrap_or_else(|| vec!["status".into(), "incoming".into(), "HTTPS".into()]),
        advertise_requested_host: raw.advertise_requested_host.unwrap_or(false),
    }
}

/// Parse TOML text into a normalized Config.
pub fn parse_config_text(text: &str) -> Result<Config, ConfigError> {
    let raw: RawConfig = toml::from_str(text).map_err(|e| ConfigError::Toml(e.to_string()))?;
    Ok(normalize(raw))
}

pub fn load_config(path: &std::path::Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path)?;
    parse_config_text(&text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_toml_with_comments() {
        let text = r#"
hostname = "rusthinq.local" # comment
ca_key_file = "ca.key"
ca_cert_file = "ca.cert"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        let cfg = parse_config_text(text).unwrap();
        assert_eq!(cfg.hostname, "rusthinq.local");
        assert_eq!(cfg.https_port.bind, 443);
        assert_eq!(cfg.thinq1_https_port.bind, 46030);
        assert!(cfg.mqtt_enabled);
        assert_eq!(cfg.mqtt.raw_prefix, None);
    }

    #[test]
    fn parse_port_table() {
        let text = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = { bind = 4433, advertise = 443 }
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        let cfg = parse_config_text(text).unwrap();
        assert_eq!(cfg.https_port.bind, 4433);
        assert_eq!(cfg.https_port.advertise, 443);
    }

    #[test]
    fn parse_bridge_and_log_and_advertise_requested_host() {
        let text = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883
advertise_requested_host = true
log = ["status", "incoming"]

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""

[bridge]
storage_path = "./state"
"#;
        let cfg = parse_config_text(text).unwrap();
        assert!(cfg.advertise_requested_host);
        assert_eq!(cfg.log, vec!["status".to_string(), "incoming".to_string()]);
        assert_eq!(cfg.bridge.unwrap().storage_path, "./state");
    }

    #[test]
    fn raw_prefix_is_settable_and_independent_of_rusthinq_prefix() {
        let text = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
raw_prefix = "rusthinq-raw"
mqtt_user = ""
mqtt_pass = ""
"#;
        let cfg = parse_config_text(text).unwrap();
        assert_eq!(cfg.mqtt.rusthinq_prefix, "rusthinq");
        assert_eq!(cfg.mqtt.raw_prefix.as_deref(), Some("rusthinq-raw"));
    }

    #[test]
    fn state_file_defaults_to_none_and_is_settable() {
        let without = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        assert_eq!(parse_config_text(without).unwrap().mqtt.state_file, None);

        let with = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
state_file = "mqtt_state.json"
"#;
        assert_eq!(
            parse_config_text(with).unwrap().mqtt.state_file.as_deref(),
            Some("mqtt_state.json")
        );
    }

    #[test]
    fn devices_section_is_absent_by_default_and_parses_when_present() {
        let without = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        assert!(parse_config_text(without).unwrap().devices.is_none());

        let with = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""

[devices]
rhai_dir = "./scripts"
watch = true
"#;
        let devices = parse_config_text(with).unwrap().devices.unwrap();
        assert_eq!(devices.rhai_dir, "./scripts");
        assert!(devices.watch);
    }

    #[test]
    fn devices_watch_defaults_to_false() {
        let text = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""

[devices]
rhai_dir = "./scripts"
"#;
        assert!(!parse_config_text(text).unwrap().devices.unwrap().watch);
    }

    #[test]
    fn gui_section_is_absent_by_default_and_parses_when_present() {
        let without = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        assert!(parse_config_text(without).unwrap().gui.is_none());

        let with = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""

[gui]
bind = 8080
"#;
        assert_eq!(parse_config_text(with).unwrap().gui.unwrap().bind, 8080);
    }
}
