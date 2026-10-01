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
        Ok(Self {
            thinq1_bind: address("thinq1_bind")?,
            mqtt_bind: address("mqtt_bind")?,
            https_bind: address("https_bind")?,
            hostname,
            ca_certificate: file("ca_certificate")?,
            ca_key: file("ca_key")?,
            device_ledger: file("device_ledger")?,
            legacy_tls,
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
}
impl Daemon {
    /// Binds all endpoints before durable generation reservation. No CA creation.
    pub async fn prepare(config: Config) -> io::Result<Self> {
        if !valid_hostname(&config.hostname) {
            return Err(invalid("invalid served hostname"));
        }
        let thin = TcpListener::bind(config.thinq1_bind).await?;
        let mqtt = TcpListener::bind(config.mqtt_bind).await?;
        let http = TcpListener::bind(config.https_bind).await?;
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
        let service = tls_runtime::Service::new(
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
        Ok(Self {
            service,
            signer,
            thin: (thin_front, thin),
            mqtt: (mqtt_front, mqtt),
            endpoints,
            firmware,
        })
    }
    pub fn firmware(&self) -> rusthinq_bridge::passthrough::Relay {
        self.firmware.clone()
    }
    pub fn endpoints(&self) -> (SocketAddr, SocketAddr, SocketAddr) {
        self.endpoints
    }
    pub fn handle(&self) -> crate::runtime::Handle {
        self.service.handle()
    }
    pub async fn serve(self, stop: watch::Receiver<bool>) -> io::Result<()> {
        let (signer_stop, signer_stopped) = watch::channel(false);
        let signer = tokio::spawn(self.signer.run(signer_stopped));
        let result = self.service.serve(self.thin, self.mqtt, stop).await;
        signer_stop.send_replace(true);
        let joined = signer.await.map_err(io::Error::other);
        result?;
        joined?;
        Ok(())
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
