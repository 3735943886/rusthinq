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

/// Either a bare port number (used verbatim, e.g. in a `mqttServer`/`apiServer` URL a
/// device is told to use) or a full URL a reverse proxy sits behind, given verbatim
/// instead of being derived from `hostname` + a port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AdvertiseSpec {
    Port(u16),
    Url(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PortSpec {
    Number(u16),
    Full {
        /// Absent means don't bind this port at all — e.g. HTTPS terminated by a
        /// reverse proxy in front of rusthinq, with only `advertise` set so devices
        /// still get told the right endpoint.
        #[serde(default)]
        bind: Option<u16>,
        #[serde(default)]
        advertise: Option<AdvertiseSpec>,
        #[serde(default)]
        address: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Port {
    pub bind: Option<u16>,
    /// `None` means derive from `bind` (and, for an appliance-facing port, the
    /// connection's own hostname/scheme) rather than advertise anything fixed.
    pub advertise: Option<AdvertiseSpec>,
    pub address: Option<String>,
}

impl From<PortSpec> for Port {
    fn from(p: PortSpec) -> Self {
        match p {
            PortSpec::Number(n) => Port {
                bind: Some(n),
                advertise: None,
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

impl Port {
    /// What to tell a connecting device this port is reachable at: an explicit
    /// `advertise` URL verbatim, `scheme://hostname:port` for an explicit advertise
    /// port number, or the same derived from `hostname` + `bind` (falling back to
    /// `default_port` if unbound, e.g. a reverse proxy in front) when `advertise`
    /// isn't set at all.
    pub fn advertise_url(&self, scheme: &str, hostname: &str, default_port: u16) -> String {
        match &self.advertise {
            Some(AdvertiseSpec::Url(u)) => u.clone(),
            Some(AdvertiseSpec::Port(p)) => format!("{scheme}://{hostname}:{p}"),
            None => format!("{scheme}://{hostname}:{}", self.bind.unwrap_or(default_port)),
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

/// A port that's just bound and never advertised to anyone: `gui_port`'s shape
/// (either a bare port number, binding `0.0.0.0`, or `{ bind, address }` to restrict
/// it to one interface -- e.g. a LAN-only address instead of every interface this
/// host has, including ones a VPN or container network expose), and also used for
/// `http_port` (an optional unencrypted listener alongside `https_port`, for a
/// reverse proxy that terminates TLS itself). Deliberately not `PortSpec`
/// (`https_port`/`mqtts_port` above): those also carry
/// `advertise`, which only means something for an appliance-facing port a client
/// gets told about (`/route`, a pairing response) -- none of these are ever
/// advertised to anyone, they're just where rusthinq happens to listen.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PlainPortSpec {
    Number(u16),
    Full {
        bind: u16,
        #[serde(default)]
        address: Option<String>,
    },
}

impl PlainPortSpec {
    pub fn port(&self) -> u16 {
        match self {
            Self::Number(n) => *n,
            Self::Full { bind, .. } => *bind,
        }
    }

    pub fn address(&self) -> Option<&str> {
        match self {
            Self::Number(_) => None,
            Self::Full { address, .. } => address.as_deref(),
        }
    }
}

/// Optional web dashboard (`rusthinq-gui`, only compiled in with the `gui`
/// feature). Absent `[gui]` means it doesn't run at all, same opt-in-by-presence
/// pattern as `[bridge]`/`[devices]`. It talks to `mqtt.mqtt_url` as its own MQTT
/// client, same as any other tool would -- nothing here is a second control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuiConfig {
    /// See [`PlainPortSpec`].
    pub gui_port: PlainPortSpec,
    /// HTTP Basic Auth, checked on every request when both are set (named after
    /// `mqtt_user`/`mqtt_pass` above). Left unset, the dashboard is unauthenticated
    /// -- it binds `0.0.0.0` (or `gui_port`'s `address`, if narrowed) and can
    /// enable/disable bridging, trigger LG login/logout, and read raw device
    /// traffic, so that's only reasonable on a trusted LAN.
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
    /// Optional unencrypted HTTP listener alongside `https_port`, for a reverse proxy
    /// that terminates TLS itself and forwards plain HTTP here. Absent by default —
    /// same opt-in-by-presence pattern as `[bridge]`/`[devices]`; without it there's
    /// no plaintext surface at all.
    #[serde(default)]
    pub http_port: Option<PlainPortSpec>,
    /// Despite the name (kept for config-file compatibility), this is already a
    /// plain, unencrypted HTTP listener in rusthinq — there's no separate
    /// `thinq1_http_port` because there's no TLS on this port to offer an
    /// alternative to.
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
    pub http_port: Option<PlainPortSpec>,
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
        http_port: raw.http_port,
        thinq1_https_port: raw.thinq1_https_port.map(Into::into).unwrap_or(Port {
            bind: Some(46030),
            advertise: None,
            address: None,
        }),
        thinq1_port: raw.thinq1_port.map(Into::into).unwrap_or(Port {
            bind: Some(47878),
            advertise: None,
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
        assert_eq!(cfg.https_port.bind, Some(443));
        assert_eq!(cfg.thinq1_https_port.bind, Some(46030));
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
        assert_eq!(cfg.https_port.bind, Some(4433));
        assert_eq!(cfg.https_port.advertise, Some(AdvertiseSpec::Port(443)));
    }

    #[test]
    fn advertise_can_be_a_full_url() {
        let text = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = { bind = 443, advertise = "https://other.machine:8443" }
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        let cfg = parse_config_text(text).unwrap();
        assert_eq!(
            cfg.https_port.advertise,
            Some(AdvertiseSpec::Url("https://other.machine:8443".to_string()))
        );
        assert_eq!(
            cfg.https_port.advertise_url("https", "x", 443),
            "https://other.machine:8443"
        );
    }

    #[test]
    fn omitting_bind_leaves_a_port_unbound_but_still_advertised() {
        let text = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = { advertise = 443 }
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        let cfg = parse_config_text(text).unwrap();
        assert_eq!(cfg.https_port.bind, None);
        assert_eq!(cfg.https_port.advertise_url("https", "x", 443), "https://x:443");
    }

    #[test]
    fn advertise_url_derives_from_bind_when_unset() {
        let text = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 4433
mqtts_port = 8883

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        let cfg = parse_config_text(text).unwrap();
        assert_eq!(cfg.https_port.advertise, None);
        assert_eq!(cfg.https_port.advertise_url("https", "x", 443), "https://x:4433");
    }

    #[test]
    fn http_port_is_absent_by_default_and_settable() {
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
        let cfg = parse_config_text(without).unwrap();
        assert!(cfg.http_port.is_none());

        let with = r#"
hostname = "x"
ca_key_file = "k"
ca_cert_file = "c"
https_port = 443
mqtts_port = 8883
http_port = { bind = 80, address = "127.0.0.1" }

[mqtt]
mqtt_url = "mqtt://localhost:1883"
rusthinq_prefix = "rusthinq"
mqtt_user = ""
mqtt_pass = ""
"#;
        let cfg = parse_config_text(with).unwrap();
        let http = cfg.http_port.unwrap();
        assert_eq!(http.port(), 80);
        assert_eq!(http.address(), Some("127.0.0.1"));
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
gui_port = 8080
"#;
        let gui = parse_config_text(with).unwrap().gui.unwrap();
        assert_eq!(gui.gui_port.port(), 8080);
        assert_eq!(gui.gui_port.address(), None);
    }

    #[test]
    fn gui_port_parses_the_bind_address_table_form() {
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
gui_port = { bind = 8080, address = "192.168.0.111" }
"#;
        let gui = parse_config_text(with).unwrap().gui.unwrap();
        assert_eq!(gui.gui_port.port(), 8080);
        assert_eq!(gui.gui_port.address(), Some("192.168.0.111"));
    }
}
