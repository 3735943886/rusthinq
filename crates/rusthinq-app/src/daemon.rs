//! Runnable composition of device transports, HTTPS, signing, and durable lifecycle.
use crate::{lifecycle_storage::Storage, tls_runtime};
use rusthinq_protocol::{client_hello::hostname as valid_hostname, lg_compat::TlsPolicy};
use rusthinq_server::{
    Config as TransportConfig,
    certificates::{Authority, Signer},
    https,
    mqtt::SystemClock,
    provisioning, thinq1_http,
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{
    fs::File,
    io::{self, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{net::TcpListener, sync::watch};

/// 0.1 `advertise`: what `/route` tells devices instead of the bound endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Advertise {
    /// `scheme://<hostname>:<port>`.
    Port(u16),
    /// Used verbatim, e.g. a reverse proxy on its own hostname.
    Url(String),
}
#[derive(Clone, Debug)]
pub struct Config {
    /// Absent listeners are not bound (0.1 port without `bind`).
    pub thinq1_bind: Option<SocketAddr>,
    pub mqtt_bind: Option<SocketAddr>,
    pub https_bind: Option<SocketAddr>,
    pub https_advertise: Option<Advertise>,
    pub mqtt_advertise: Option<Advertise>,
    /// Plain HTTP with the HTTPS routes, behind a TLS-terminating proxy (0.1 `http_port`).
    pub http_bind: Option<SocketAddr>,
    /// ThinQ1 HTTPS (rethink/0.1 `thinq1_https_port`, default 46030), on the same minting
    /// TLS listener options as the other device ports, as upstream rethink serves it.
    pub thinq1_https_bind: Option<SocketAddr>,
    /// Optional plain HTTP with the same routes (rethink `thinq1_http_port`).
    pub thinq1_http_bind: Option<SocketAddr>,
    /// Served by `/route/certificate` instead of the CA (0.1 `custom_root_cert_file`).
    pub custom_root_certificate: Option<PathBuf>,
    pub hostname: String,
    pub ca_certificate: PathBuf,
    pub ca_key: PathBuf,
    pub device_ledger: PathBuf,
    pub legacy_tls: bool,
    /// Answer `/route` with the hostname the appliance requested (DNAT setups), as 0.1.
    pub advertise_requested_host: bool,
    pub management: Option<crate::management::Config>,
    pub drivers: Option<crate::drivers::Config>,
    pub external_mqtt: Option<crate::external_mqtt::Config>,
    pub cloud_account: Option<PathBuf>,
    /// Resolvers for outbound LG cloud connections (0.1 `[bridge] dns`): DoH URLs or
    /// plain DNS servers, tried in order. Empty means the system resolver.
    pub bridge_dns: Vec<String>,
}
impl Config {
    pub fn load(path: &Path) -> io::Result<Self> {
        let mut bytes = Vec::new();
        File::open(path)?.take(65537).read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err(invalid("configuration exceeded"));
        }
        let text =
            std::str::from_utf8(&bytes).map_err(|_| invalid("configuration must be UTF-8 TOML"))?;
        let document: toml::Value =
            toml::from_str(text).map_err(|_| invalid("invalid TOML configuration"))?;
        let value = serde_json::to_value(document).map_err(|_| invalid("invalid configuration"))?;
        let fields = value
            .as_object()
            .ok_or_else(|| invalid("configuration must be an object"))?;
        for key in fields.keys() {
            if ![
                "thinq1_bind",
                "mqtt_bind",
                "https_bind",
                "https_advertise",
                "mqtt_advertise",
                "http_bind",
                "thinq1_https_bind",
                "thinq1_http_bind",
                "custom_root_certificate",
                "hostname",
                "ca_certificate",
                "ca_key",
                "device_ledger",
                "legacy_tls",
                "advertise_requested_host",
                "management",
                "drivers",
                "external_mqtt",
                "cloud_account",
                "bridge_dns",
            ]
            .contains(&key.as_str())
            {
                return Err(invalid("unknown configuration field"));
            }
        }
        let string = |key: &str| {
            value[key]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| invalid("missing/invalid configuration string"))
        };
        let parent = path.parent().unwrap_or(Path::new("."));
        let file = |key| -> io::Result<PathBuf> { Ok(parent.join(string(key)?)) };
        let address = |key: &str| -> io::Result<Option<SocketAddr>> {
            match value.get(key) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(_) => string(key)?
                    .parse()
                    .map(Some)
                    .map_err(|_| invalid("invalid bind address")),
            }
        };
        let advertise = |key: &str| -> io::Result<Option<Advertise>> {
            match value.get(key) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(serde_json::Value::String(url)) if !url.is_empty() => {
                    Ok(Some(Advertise::Url(url.clone())))
                }
                Some(port) => port
                    .as_u64()
                    .and_then(|port| u16::try_from(port).ok())
                    .filter(|port| *port != 0)
                    .map(|port| Some(Advertise::Port(port)))
                    .ok_or_else(|| invalid("advertise must be a port or URL")),
            }
        };
        let hostname = string("hostname")?;
        if !valid_hostname(&hostname) {
            return Err(invalid("invalid served hostname"));
        }
        let legacy_tls = match value.get("legacy_tls") {
            Some(value) => value
                .as_bool()
                .ok_or_else(|| invalid("legacy_tls must be boolean"))?,
            None => false,
        };
        let advertise_requested_host = match value.get("advertise_requested_host") {
            Some(value) => value
                .as_bool()
                .ok_or_else(|| invalid("advertise_requested_host must be boolean"))?,
            None => false,
        };
        let management = match value.get("management") {
            None | Some(serde_json::Value::Null) => None,
            Some(management) => {
                let fields = management
                    .as_object()
                    .ok_or_else(|| invalid("management must be an object"))?;
                if fields.keys().any(|key| {
                    !["bind", "gui", "user", "password", "raw_inject_toggle"]
                        .contains(&key.as_str())
                }) {
                    return Err(invalid("unknown management field"));
                }
                let bind = management["bind"]
                    .as_str()
                    .ok_or_else(|| invalid("management bind required"))?
                    .parse()
                    .map_err(|_| invalid("invalid management bind"))?;
                let gui = match management.get("gui") {
                    None => cfg!(feature = "gui"),
                    Some(value) => value
                        .as_bool()
                        .ok_or_else(|| invalid("management gui must be boolean"))?,
                };
                let credentials = match (management.get("user"), management.get("password")) {
                    (None, None) => None,
                    (Some(user), Some(password)) => Some(crate::management::Credentials {
                        user: user
                            .as_str()
                            .ok_or_else(|| invalid("invalid management user"))?
                            .into(),
                        password: password
                            .as_str()
                            .ok_or_else(|| invalid("invalid management password"))?
                            .into(),
                    }),
                    _ => return Err(invalid("management requires both user and password")),
                };
                let config = crate::management::Config {
                    bind,
                    gui,
                    credentials,
                    raw_inject: Default::default(),
                    raw_inject_toggle: match management.get("raw_inject_toggle") {
                        None => false,
                        Some(value) => value.as_bool().ok_or_else(|| {
                            invalid("management raw_inject_toggle must be boolean")
                        })?,
                    },
                };
                config.validate()?;
                Some(config)
            }
        };
        let drivers = match value.get("drivers") {
            None | Some(serde_json::Value::Null) => None,
            Some(drivers) => {
                let fields = drivers
                    .as_object()
                    .ok_or_else(|| invalid("drivers must be an object"))?;
                if fields.keys().any(|key| {
                    !["directory", "topic_prefix", "bindings", "watch"].contains(&key.as_str())
                }) {
                    return Err(invalid("unknown driver field"));
                }
                let directory = drivers["directory"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| invalid("driver directory required"))?;
                let topic_prefix = match drivers.get("topic_prefix") {
                    None => "rusthinq".into(),
                    Some(value) => value
                        .as_str()
                        .ok_or_else(|| invalid("invalid driver topic_prefix"))?
                        .to_string(),
                };
                let bindings = match drivers.get("bindings") {
                    None => Default::default(),
                    Some(value) => serde_json::from_value(value.clone())
                        .map_err(|_| invalid("driver bindings must map device ids to models"))?,
                };
                let config = crate::drivers::Config {
                    directory: parent.join(directory),
                    topic_prefix,
                    bindings,
                    watch: match drivers.get("watch") {
                        None => false,
                        Some(value) => value
                            .as_bool()
                            .ok_or_else(|| invalid("driver watch must be boolean"))?,
                    },
                };
                config.validate()?;
                Some(config)
            }
        };
        let external_mqtt = match value.get("external_mqtt") {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => {
                let fields = value
                    .as_object()
                    .ok_or_else(|| invalid("external_mqtt must be an object"))?;
                if fields.keys().any(|key| {
                    ![
                        "host",
                        "port",
                        "tls",
                        "ca",
                        "client",
                        "username",
                        "password",
                        "inventory",
                    ]
                    .contains(&key.as_str())
                }) {
                    return Err(invalid("unknown external_mqtt field"));
                }
                let required = |key: &str| {
                    value[key]
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("invalid external_mqtt string"))
                };
                let optional = |key: &str| match value.get(key) {
                    None | Some(serde_json::Value::Null) => Ok(None),
                    Some(value) => value
                        .as_str()
                        .map(|value| Some(value.to_owned()))
                        .ok_or_else(|| invalid("invalid external_mqtt string")),
                };
                let tls = match value.get("tls") {
                    None => false,
                    Some(value) => value
                        .as_bool()
                        .ok_or_else(|| invalid("external_mqtt tls must be boolean"))?,
                };
                let port = match value.get("port") {
                    None => {
                        if tls {
                            8883
                        } else {
                            1883
                        }
                    }
                    Some(value) => value
                        .as_u64()
                        .and_then(|port| u16::try_from(port).ok())
                        .ok_or_else(|| invalid("invalid external_mqtt port"))?,
                };
                let config = crate::external_mqtt::Config {
                    host: required("host")?,
                    port,
                    tls,
                    ca: optional("ca")?.map(|file| parent.join(file)),
                    client: optional("client")?.unwrap_or_else(|| "rusthinq-0.2".into()),
                    username: optional("username")?,
                    password: optional("password")?,
                    inventory: parent.join(required("inventory")?),
                };
                config.validate()?;
                Some(config)
            }
        };
        let bridge_dns = match value.get("bridge_dns") {
            None => Vec::new(),
            Some(entries) => entries
                .as_array()
                .and_then(|entries| {
                    entries
                        .iter()
                        .map(|entry| entry.as_str().map(str::to_owned))
                        .collect::<Option<Vec<_>>>()
                })
                .ok_or_else(|| invalid("bridge_dns must be a list of strings"))?,
        };
        if !bridge_dns.is_empty() && !cfg!(feature = "bridge") {
            return Err(invalid("bridge feature is disabled"));
        }
        Ok(Self {
            bridge_dns,
            thinq1_bind: address("thinq1_bind")?,
            mqtt_bind: address("mqtt_bind")?,
            https_bind: address("https_bind")?,
            https_advertise: advertise("https_advertise")?,
            mqtt_advertise: advertise("mqtt_advertise")?,
            http_bind: address("http_bind")?,
            thinq1_https_bind: address("thinq1_https_bind")?,
            thinq1_http_bind: address("thinq1_http_bind")?,
            custom_root_certificate: match value.get("custom_root_certificate") {
                None => None,
                Some(_) => Some(file("custom_root_certificate")?),
            },
            hostname,
            ca_certificate: file("ca_certificate")?,
            ca_key: file("ca_key")?,
            device_ledger: file("device_ledger")?,
            legacy_tls,
            advertise_requested_host,
            management,
            drivers,
            external_mqtt,
            cloud_account: match value.get("cloud_account") {
                None => None,
                Some(value) => {
                    if !cfg!(feature = "bridge") {
                        return Err(invalid("bridge feature is disabled"));
                    }
                    Some(
                        parent.join(
                            value
                                .as_str()
                                .filter(|path| !path.is_empty())
                                .ok_or_else(|| invalid("invalid cloud_account path"))?,
                        ),
                    )
                }
            },
        })
    }
}

pub struct Daemon {
    service: tls_runtime::Service,
    signer: Signer,
    thin: Option<(FrontDoor, TcpListener)>,
    mqtt: Option<(FrontDoor, TcpListener)>,
    plain: Vec<(https::Service, TcpListener)>,
    /// Device TLS refusals, reported on stderr as 0.1 warned about failed handshakes.
    mqtt_diagnostics: tokio::sync::broadcast::Receiver<rusthinq_server::MqttDiagnostic>,
    rejections: Vec<(
        &'static str,
        tokio::sync::broadcast::Receiver<rusthinq_server::tls::Rejection>,
    )>,
    endpoints: Endpoints,
    #[cfg(feature = "bridge")]
    firmware: rusthinq_bridge::passthrough::Relay,
    management: Option<(crate::management::Config, TcpListener)>,
    external_mqtt: Option<crate::external_mqtt::Runtime>,
    #[cfg(feature = "bridge")]
    cloud_account: Option<(crate::cloud_account::Handle, crate::cloud_account::Runtime)>,
    #[cfg(feature = "bridge")]
    cloud_devices: Option<crate::cloud_devices::Runtime>,
    #[cfg(feature = "scripting")]
    driver_watch: Option<crate::drivers::Config>,
    #[cfg(feature = "scripting")]
    commands: Option<(
        crate::external_mqtt::Config,
        String,
        Arc<std::sync::atomic::AtomicBool>,
    )>,
}
impl Daemon {
    /// Binds all endpoints before durable generation reservation. No CA creation.
    pub async fn prepare(config: Config) -> io::Result<Self> {
        if !valid_hostname(&config.hostname) {
            return Err(invalid("invalid served hostname"));
        }
        if let Some(drivers) = &config.drivers {
            drivers.validate()?;
        }
        if let Some(management) = &config.management {
            management.validate()?;
        }
        if let Some(external) = &config.external_mqtt {
            external.validate()?;
        }
        if (config.cloud_account.is_some() || !config.bridge_dns.is_empty())
            && !cfg!(feature = "bridge")
        {
            return Err(invalid("bridge feature is disabled"));
        }
        #[cfg(feature = "bridge")]
        rusthinq_bridge::resolver::set_servers(&config.bridge_dns)?;
        #[cfg(feature = "bridge")]
        let pairing_path = config.cloud_account.as_ref().map(|path| {
            let mut name = path.as_os_str().to_os_string();
            name.push(".pairings");
            PathBuf::from(name)
        });
        #[cfg(feature = "bridge")]
        let cloud_account = match config.cloud_account.clone() {
            Some(path) => Some(crate::cloud_account::open(path).await?),
            None => None,
        };
        #[cfg(feature = "scripting")]
        let commands = config
            .external_mqtt
            .clone()
            .zip(
                config
                    .drivers
                    .as_ref()
                    .map(|drivers| drivers.topic_prefix.clone()),
            )
            .map(|(mqtt, prefix)| {
                (
                    mqtt,
                    prefix,
                    config
                        .management
                        .as_ref()
                        .map(|m| m.raw_inject.clone())
                        .unwrap_or_default(),
                )
            });
        let external = config
            .external_mqtt
            .clone()
            .map(crate::external_mqtt::new)
            .transpose()?;
        #[cfg(feature = "scripting")]
        let drivers = config.drivers.clone();
        let thin = listen(config.thinq1_bind).await?;
        let mqtt = listen(config.mqtt_bind).await?;
        let http = listen(config.https_bind).await?;
        let plain_http = listen(config.http_bind).await?;
        let thinq1_https = listen(config.thinq1_https_bind).await?;
        let thinq1_http = listen(config.thinq1_http_bind).await?;
        let management = if let Some(management) = config.management.clone() {
            management.validate()?;
            let listener = TcpListener::bind(management.bind).await?;
            Some((management, listener))
        } else {
            None
        };
        let endpoints = Endpoints {
            thinq1: local(&thin)?,
            mqtt: local(&mqtt)?,
            https: local(&http)?,
            http: local(&plain_http)?,
            thinq1_https: local(&thinq1_https)?,
            thinq1_http: local(&thinq1_http)?,
        };
        // As 0.1: an explicit advertise wins, else the bound port, else the LG default.
        let (https_port, api_server) = match &config.https_advertise {
            Some(Advertise::Port(port)) => (*port, None),
            Some(Advertise::Url(url)) => (443, Some(url.clone())),
            None => (endpoints.https.map_or(443, |a| a.port()), None),
        };
        let (mqtt_port, mqtt_server) = match &config.mqtt_advertise {
            Some(Advertise::Port(port)) => (*port, None),
            Some(Advertise::Url(url)) => (8883, Some(url.clone())),
            None => (endpoints.mqtt.map_or(8883, |a| a.port()), None),
        };
        let name = config.hostname.clone();
        let advertise_requested_host = config.advertise_requested_host;
        let policy = if config.legacy_tls {
            TlsPolicy::RtkRtl8711am
        } else {
            TlsPolicy::Baseline
        };
        let (ca, storage, block, identities, custom_root) =
            tokio::task::spawn_blocking(move || {
                let ca = Arc::new(Authority::load(&config.ca_certificate, &config.ca_key)?);
                let custom_root = match &config.custom_root_certificate {
                    None => None,
                    Some(path) => {
                        let mut bytes = Vec::new();
                        File::open(path)?.take(262145).read_to_end(&mut bytes)?;
                        Some(bytes)
                    }
                };
                let identities = (0..4)
                    .map(|_| ca.server_identity(&config.hostname, policy))
                    .collect::<io::Result<Vec<_>>>()?;
                let mut storage = Storage::open(&config.device_ledger, 256)?;
                let block = storage.reserve_generations(1_000_000)?;
                Ok::<_, io::Error>((ca, storage, block, identities, custom_root))
            })
            .await
            .map_err(io::Error::other)??;
        let mut identities = identities.into_iter();
        // As 0.1, every device listener answers any requested name with a CA-signed leaf
        // (DNAT setups keep the appliance's LG names) and peers without SNI with `hostname`.
        let thin_front = FrontDoor::new(
            TlsConfig::default(),
            vec![identities.next().expect("four identities")],
            None,
        )?
        .with_minting(ca.clone(), policy, &name)?;
        let mqtt_front = FrontDoor::new(
            TlsConfig::default(),
            vec![identities.next().expect("four identities")],
            None,
        )?
        .with_minting(ca.clone(), policy, &name)?;
        #[cfg(feature = "bridge")]
        let firmware = rusthinq_bridge::passthrough::Relay::new(
            Default::default(),
            Arc::new(rusthinq_bridge::passthrough::HttpsConnector),
        )?;
        #[cfg(feature = "bridge")]
        firmware.confirm_local(&name)?;
        #[cfg(feature = "bridge")]
        let passthrough =
            Some(Arc::new(firmware.clone()) as Arc<dyn rusthinq_server::tls::Passthrough>);
        #[cfg(not(feature = "bridge"))]
        let passthrough = None;
        let http_front = FrontDoor::new(
            TlsConfig::default(),
            vec![identities.next().expect("four identities")],
            passthrough,
        )?
        .with_minting(ca.clone(), policy, &name)?;
        let thinq1_https_front = FrontDoor::new(
            TlsConfig::default(),
            vec![identities.next().expect("four identities")],
            None,
        )?
        .with_minting(ca.clone(), policy, &name)?;
        let rejections = vec![
            ("ThinQ1 TLS", thin_front.subscribe()),
            ("MQTTS", mqtt_front.subscribe()),
            ("HTTPS", http_front.subscribe()),
            ("ThinQ1 HTTPS", thinq1_https_front.subscribe()),
        ];
        let (signer, signing) = Signer::new(ca.clone(), Default::default())?;
        let mut provisioning_config = provisioning::Config::new(name);
        provisioning_config.https_port = https_port;
        provisioning_config.mqtts_port = mqtt_port;
        provisioning_config.advertise_requested_host = advertise_requested_host;
        provisioning_config.api_server = api_server;
        provisioning_config.mqtt_server = mqtt_server;
        let provisioning =
            provisioning::Service::new(provisioning_config, &ca, signing, custom_root.as_deref())?;
        let (xml, metadata) = thinq1_http::Service::new(Default::default(), Arc::new(SystemClock))?;
        let https = https::Service::new(
            xml,
            provisioning,
            Duration::from_secs(10),
            Duration::from_secs(30),
        )?;
        let mut service = tls_runtime::Service::new(
            storage,
            TransportConfig {
                generation_floor: block.floor,
                generation_ceiling: block.ceiling,
                ..Default::default()
            },
            Arc::new(SystemClock),
            Duration::from_secs(10),
            256,
            Some((1_000_000, 10_000)),
        )?
        .with_metadata(metadata);
        let plain = [plain_http, thinq1_http]
            .into_iter()
            .flatten()
            .map(|listener| (https.clone(), listener))
            .collect();
        if let Some(listener) = thinq1_https {
            service = service.with_local_service(
                thinq1_https_front,
                listener,
                Arc::new(https.clone()),
            )?;
        }
        if let Some(http) = http {
            service = service.with_local_service(http_front, http, Arc::new(https))?;
        }
        #[cfg(feature = "bridge")]
        {
            service = service.with_firmware(firmware.clone());
        }
        #[cfg(feature = "scripting")]
        let driver_watch = drivers.as_ref().filter(|config| config.watch).cloned();
        #[cfg(feature = "scripting")]
        if let Some(drivers) = drivers {
            service = service.with_drivers(drivers)?;
        }
        let mqtt_diagnostics = service.mqtt_diagnostics();
        let external_mqtt = if let Some((handle, runtime)) = external {
            service = service.with_external_mqtt(handle);
            Some(runtime)
        } else {
            None
        };
        #[cfg(feature = "bridge")]
        if let Some((account, _)) = &cloud_account {
            service.handle().attach_cloud_account(account.clone())?;
        }
        #[cfg(feature = "bridge")]
        let cloud_devices = if let Some((account, _)) = &cloud_account {
            let (configured, cloud) = service
                .with_cloud_devices(
                    pairing_path.expect("account path"),
                    account.clone(),
                    firmware.clone(),
                )
                .await?;
            service = configured;
            Some(cloud)
        } else {
            None
        };
        Ok(Self {
            service,
            signer,
            thin: thin.map(|listener| (thin_front, listener)),
            mqtt: mqtt.map(|listener| (mqtt_front, listener)),
            plain,
            mqtt_diagnostics,
            rejections,
            endpoints,
            #[cfg(feature = "bridge")]
            firmware,
            management,
            external_mqtt,
            #[cfg(feature = "bridge")]
            cloud_account,
            #[cfg(feature = "bridge")]
            cloud_devices,
            #[cfg(feature = "scripting")]
            driver_watch,
            #[cfg(feature = "scripting")]
            commands,
        })
    }
    #[cfg(feature = "bridge")]
    pub fn firmware(&self) -> rusthinq_bridge::passthrough::Relay {
        self.firmware.clone()
    }
    pub fn endpoints(&self) -> Endpoints {
        self.endpoints
    }
    pub fn management_endpoint(&self) -> Option<SocketAddr> {
        self.management
            .as_ref()
            .and_then(|(_, listener)| listener.local_addr().ok())
    }
    pub fn handle(&self) -> crate::runtime::Handle {
        self.service.handle()
    }
    pub async fn serve(self, mut stop: watch::Receiver<bool>) -> io::Result<()> {
        let (signer_stop, signer_stopped) = crate::task::Shutdown::new(false);
        let signer = crate::task::OwnedTask::spawn(self.signer.run(signer_stopped));
        let (core_stop, core_stopped) = crate::task::Shutdown::new(*stop.borrow());
        let (services_stop, services_stopped) = crate::task::Shutdown::new(false);
        let mut tasks = tokio::task::JoinSet::new();
        let handle = self.service.handle();
        let external_handle = handle.external_mqtt();
        {
            let mut diagnostics = self.mqtt_diagnostics;
            let mut stop = services_stopped.clone();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = stop.changed() => return Ok(()),
                        diagnostic = diagnostics.recv() => match diagnostic {
                            Ok(diagnostic) => crate::logging::mqtt_diagnostic(&diagnostic),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(lost)) => tracing::warn!(lost, "MQTT diagnostics lost"),
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                let _ = stop.wait_for(|stopped| *stopped).await;
                                return Ok(());
                            }
                        }
                    }
                }
            });
        }
        for (label, mut rejections) in self.rejections {
            let mut stop = services_stopped.clone();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = stop.changed() => return Ok(()),
                        rejection = rejections.recv() => match rejection {
                            Ok(rejection) => tracing::warn!(listener = label, peer = %rejection.peer, reason = ?rejection.reason, "connection refused"),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(lost)) => tracing::warn!(listener = label, lost, "connection refusal diagnostics lost"),
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                // A finished listener is reported by the core; wait for shutdown.
                                let _ = stop.wait_for(|stopped| *stopped).await;
                                return Ok(());
                            }
                        }
                    }
                }
            });
        }
        for (service, listener) in self.plain {
            tasks.spawn(service.serve_plain(listener, 64, core_stop.subscribe()));
        }
        let mut core = crate::task::OwnedTask::spawn(self.service.serve_listeners(
            self.thin,
            self.mqtt,
            core_stopped,
        ));
        #[cfg(feature = "scripting")]
        if let Some(config) = self.driver_watch {
            tasks.spawn(crate::driver_watch::run(
                config,
                handle.clone(),
                services_stopped.clone(),
            ));
        }
        #[cfg(feature = "scripting")]
        if let Some((config, prefix, raw)) = self.commands {
            tasks.spawn(crate::mqtt_commands::run(
                config,
                prefix,
                handle.clone(),
                raw,
                core_stop.subscribe(),
            ));
        }
        if let Some(external) = self.external_mqtt {
            tasks.spawn(external.run(handle.clone(), services_stopped.clone()));
        }
        #[cfg(feature = "bridge")]
        if let Some(runtime) = self.cloud_devices {
            tasks.spawn(runtime.run(core_stop.subscribe()));
        }
        #[cfg(feature = "bridge")]
        if let Some((_, runtime)) = self.cloud_account {
            tasks.spawn(runtime.run(services_stopped.clone()));
        }
        if let Some((config, listener)) = self.management {
            tasks.spawn(crate::management::serve(
                listener,
                handle.clone(),
                config,
                services_stopped,
            ));
        }
        let mut failure = None;
        let mut core_finished = false;
        if !*stop.borrow() {
            tokio::select! {
                _=stop.changed()=>{},
                joined=&mut core=>{
                    core_finished=true;
                    failure=Some(match joined {
                        Ok(Err(error))=>error,
                        Err(error)=>io::Error::other(error),
                        Ok(Ok(()))=>io::Error::other("device service stopped unexpectedly"),
                    });
                },
                joined=tasks.join_next(), if !tasks.is_empty()=>{
                    failure=Some(match joined {
                        Some(Ok(Err(error)))=>error,
                        Some(Err(error))=>io::Error::other(error),
                        _=>io::Error::other("daemon service stopped unexpectedly"),
                    });
                }
            }
        }
        // Keep the publication adapter alive until device workers emit terminal output.
        core_stop.stop();
        if !core_finished {
            match core.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                }
                Err(error) => {
                    failure.get_or_insert_with(|| io::Error::other(error));
                }
            }
        }
        if let Some(external) = external_handle {
            match tokio::time::timeout(Duration::from_secs(10), external.flush()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                }
                Err(_) => {
                    failure.get_or_insert_with(|| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "terminal MQTT output unconfirmed; retained inventory preserved",
                        )
                    });
                }
            }
        }
        services_stop.stop();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                }
                Err(error) => {
                    failure.get_or_insert_with(|| io::Error::other(error));
                }
            }
        }
        signer_stop.stop();
        let joined = signer.await.map_err(io::Error::other);
        joined?;
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
/// Bound device-facing endpoints; `None` where the configuration binds nothing.
#[derive(Clone, Copy, Debug)]
pub struct Endpoints {
    pub thinq1: Option<SocketAddr>,
    pub mqtt: Option<SocketAddr>,
    pub https: Option<SocketAddr>,
    pub http: Option<SocketAddr>,
    pub thinq1_https: Option<SocketAddr>,
    pub thinq1_http: Option<SocketAddr>,
}
async fn listen(address: Option<SocketAddr>) -> io::Result<Option<TcpListener>> {
    match address {
        Some(address) => TcpListener::bind(address).await.map(Some),
        None => Ok(None),
    }
}
fn local(listener: &Option<TcpListener>) -> io::Result<Option<SocketAddr>> {
    listener.as_ref().map(TcpListener::local_addr).transpose()
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
