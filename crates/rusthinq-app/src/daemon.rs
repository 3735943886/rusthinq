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

#[derive(Clone, Debug)]
pub struct Config {
    pub thinq1_bind: SocketAddr,
    pub mqtt_bind: SocketAddr,
    pub https_bind: SocketAddr,
    pub hostname: String,
    pub ca_certificate: PathBuf,
    pub ca_key: PathBuf,
    pub device_ledger: PathBuf,
    pub legacy_tls: bool,
    pub management: Option<crate::management::Config>,
    pub drivers: Option<crate::drivers::Config>,
    pub external_mqtt: Option<crate::external_mqtt::Config>,
}
impl Config {
    pub fn load(path: &Path) -> io::Result<Self> {
        let mut bytes = Vec::new();
        File::open(path)?.take(65537).read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err(invalid("configuration exceeded"));
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        let fields = value
            .as_object()
            .ok_or_else(|| invalid("configuration must be an object"))?;
        for key in fields.keys() {
            if ![
                "thinq1_bind",
                "mqtt_bind",
                "https_bind",
                "hostname",
                "ca_certificate",
                "ca_key",
                "device_ledger",
                "legacy_tls",
                "management",
                "drivers",
                "external_mqtt",
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
        let address = |key| -> io::Result<SocketAddr> {
            string(key)?
                .parse()
                .map_err(|_| invalid("invalid bind address"))
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
        let management = match value.get("management") {
            None | Some(serde_json::Value::Null) => None,
            Some(management) => {
                let fields = management
                    .as_object()
                    .ok_or_else(|| invalid("management must be an object"))?;
                if fields.keys().any(|key| {
                    !["bind", "gui", "user", "password", "raw_inject"].contains(&key.as_str())
                }) {
                    return Err(invalid("unknown management field"));
                }
                let bind = management["bind"]
                    .as_str()
                    .ok_or_else(|| invalid("management bind required"))?
                    .parse()
                    .map_err(|_| invalid("invalid management bind"))?;
                let gui = match management.get("gui") {
                    None => true,
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
                    raw_inject: match management.get("raw_inject") {
                        None => false,
                        Some(value) => value
                            .as_bool()
                            .ok_or_else(|| invalid("management raw_inject must be boolean"))?,
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
                    !["directory", "topic_prefix", "il_prefix", "bindings"].contains(&key.as_str())
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
                let il_prefix = match drivers.get("il_prefix") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(value) => Some(
                        value
                            .as_str()
                            .ok_or_else(|| invalid("invalid driver il_prefix"))?
                            .to_string(),
                    ),
                };
                let bindings = match drivers.get("bindings") {
                    None => Default::default(),
                    Some(value) => serde_json::from_value(value.clone())
                        .map_err(|_| invalid("driver bindings must map device ids to models"))?,
                };
                let config = crate::drivers::Config {
                    directory: parent.join(directory),
                    topic_prefix,
                    il_prefix,
                    bindings,
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
        Ok(Self {
            thinq1_bind: address("thinq1_bind")?,
            mqtt_bind: address("mqtt_bind")?,
            https_bind: address("https_bind")?,
            hostname,
            ca_certificate: file("ca_certificate")?,
            ca_key: file("ca_key")?,
            device_ledger: file("device_ledger")?,
            legacy_tls,
            management,
            drivers,
            external_mqtt,
        })
    }
}

pub struct Daemon {
    service: tls_runtime::Service,
    signer: Signer,
    thin: (FrontDoor, TcpListener),
    mqtt: (FrontDoor, TcpListener),
    endpoints: (SocketAddr, SocketAddr, SocketAddr),
    firmware: rusthinq_bridge::passthrough::Relay,
    management: Option<(crate::management::Config, TcpListener)>,
    external_mqtt: Option<crate::external_mqtt::Runtime>,
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
        let external = config
            .external_mqtt
            .clone()
            .map(crate::external_mqtt::new)
            .transpose()?;
        let drivers = config.drivers.clone();
        let thin = TcpListener::bind(config.thinq1_bind).await?;
        let mqtt = TcpListener::bind(config.mqtt_bind).await?;
        let http = TcpListener::bind(config.https_bind).await?;
        let management = if let Some(management) = config.management.clone() {
            management.validate()?;
            let listener = TcpListener::bind(management.bind).await?;
            Some((management, listener))
        } else {
            None
        };
        let endpoints = (thin.local_addr()?, mqtt.local_addr()?, http.local_addr()?);
        let https_port = http.local_addr()?.port();
        let mqtt_port = mqtt.local_addr()?.port();
        let name = config.hostname.clone();
        let (ca, storage, block, identities) = tokio::task::spawn_blocking(move || {
            let ca = Arc::new(Authority::load(&config.ca_certificate, &config.ca_key)?);
            let policy = if config.legacy_tls {
                TlsPolicy::RtkRtl8711am
            } else {
                TlsPolicy::Baseline
            };
            let identities = (0..3)
                .map(|_| ca.server_identity(&config.hostname, policy))
                .collect::<io::Result<Vec<_>>>()?;
            let mut storage = Storage::open(&config.device_ledger, 256)?;
            let block = storage.reserve_generations(1_000_000)?;
            Ok::<_, io::Error>((ca, storage, block, identities))
        })
        .await
        .map_err(io::Error::other)??;
        let mut identities = identities.into_iter();
        let thin_front = FrontDoor::new(
            TlsConfig::default(),
            vec![identities.next().expect("three identities")],
            None,
        )?;
        let mqtt_front = FrontDoor::new(
            TlsConfig::default(),
            vec![identities.next().expect("three identities")],
            None,
        )?;
        let firmware = rusthinq_bridge::passthrough::Relay::new(
            Default::default(),
            Arc::new(rusthinq_bridge::passthrough::HttpsConnector),
        )?;
        firmware.confirm_local(&name)?;
        let http_front = FrontDoor::new(
            TlsConfig::default(),
            vec![identities.next().expect("three identities")],
            Some(Arc::new(firmware.clone())),
        )?;
        let (signer, signing) = Signer::new(ca.clone(), Default::default())?;
        let mut provisioning_config = provisioning::Config::new(name);
        provisioning_config.https_port = https_port;
        provisioning_config.mqtts_port = mqtt_port;
        let provisioning = provisioning::Service::new(provisioning_config, &ca, signing, None)?;
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
        .with_firmware(firmware.clone())
        .with_metadata(metadata)
        .with_local_service(http_front, http, Arc::new(https))?;
        if let Some(drivers) = drivers {
            service = service.with_drivers(drivers)?;
        }
        let external_mqtt = if let Some((handle, runtime)) = external {
            service = service.with_external_mqtt(handle);
            Some(runtime)
        } else {
            None
        };
        Ok(Self {
            service,
            signer,
            thin: (thin_front, thin),
            mqtt: (mqtt_front, mqtt),
            endpoints,
            firmware,
            management,
            external_mqtt,
        })
    }
    pub fn firmware(&self) -> rusthinq_bridge::passthrough::Relay {
        self.firmware.clone()
    }
    pub fn endpoints(&self) -> (SocketAddr, SocketAddr, SocketAddr) {
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
        let (signer_stop, signer_stopped) = watch::channel(false);
        let signer = tokio::spawn(self.signer.run(signer_stopped));
        let (services_stop, services_stopped) = watch::channel(*stop.borrow());
        let mut tasks = tokio::task::JoinSet::new();
        let handle = self.service.handle();
        tasks.spawn(
            self.service
                .serve(self.thin, self.mqtt, services_stopped.clone()),
        );
        if let Some(external) = self.external_mqtt {
            tasks.spawn(external.run(handle.clone(), services_stopped.clone()));
        }
        if let Some((config, listener)) = self.management {
            tasks.spawn(crate::management::serve(
                listener,
                handle,
                config,
                services_stopped,
            ));
        }
        let mut failure = None;
        if !*stop.borrow() {
            tokio::select! {
                _=stop.changed()=>{},
                joined=tasks.join_next()=>{
                    failure=Some(match joined {
                        Some(Ok(Err(error)))=>error,
                        Some(Err(error))=>io::Error::other(error),
                        _=>io::Error::other("daemon service stopped unexpectedly"),
                    });
                }
            }
        }
        services_stop.send_replace(true);
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
        signer_stop.send_replace(true);
        let joined = signer.await.map_err(io::Error::other);
        joined?;
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
