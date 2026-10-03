//! Authenticated outbound LG TLS. No spawned tasks, retries, queues or detached sockets.
//! Construct on the caller's owned blocking preparation task; connect is cancellable.
use crate::pairing::{Material, service};
use openssl::{
    pkey::PKey,
    ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion},
    x509::{X509, store::X509StoreBuilder},
};
use std::{fmt, io, pin::Pin, time::Duration};
use tokio::{net::TcpStream, time::timeout};
use tokio_openssl::SslStream;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    ThinQ1,
    ThinQ2,
}
#[derive(Clone)]
pub struct Config {
    pub timeout: Duration,
    /// Explicit ThinQ1 trust override (e.g. a private proxy CA). None uses OS roots.
    /// ThinQ2 always uses the regional root in its authenticated pairing material.
    pub thinq1_ca: Option<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            thinq1_ca: None,
        }
    }
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudTlsConfig")
            .field("timeout", &self.timeout)
            .field("trust_override", &self.thinq1_ca.is_some())
            .finish()
    }
}
#[derive(Clone)]
pub struct Connector {
    tls: SslConnector,
    client_certificate: Option<X509>,
    protocol: Protocol,
    host: String,
    port: u16,
    timeout: Duration,
}
impl fmt::Debug for Connector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudConnector")
            .field("protocol", &self.protocol)
            .field("host", &self.host)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid cloud TLS configuration or pairing material",
    )
}
fn roots(pem: &str) -> io::Result<X509StoreBuilder> {
    if pem.len() > 65536 {
        return Err(invalid());
    }
    let certificates = X509::stack_from_pem(pem.as_bytes()).map_err(|_| invalid())?;
    if certificates.is_empty() || certificates.len() > 16 {
        return Err(invalid());
    }
    let mut store = X509StoreBuilder::new().map_err(|_| invalid())?;
    for certificate in certificates {
        store.add_cert(certificate).map_err(|_| invalid())?;
    }
    Ok(store)
}
impl Connector {
    pub fn new(material: &Material, config: Config) -> io::Result<Self> {
        Self::prepare(material, config, false)
    }
    pub fn notifications(material: &Material, config: Config) -> io::Result<Self> {
        Self::prepare(material, config, true)
    }
    fn prepare(material: &Material, config: Config, notifications: bool) -> io::Result<Self> {
        if config.timeout.is_zero() || config.timeout > Duration::from_secs(60) {
            return Err(invalid());
        }
        material.validate().map_err(|_| invalid())?;
        let mut tls = SslConnector::builder(SslMethod::tls_client()).map_err(|_| invalid())?;
        let mut client_certificate = None;
        tls.set_verify(SslVerifyMode::PEER);
        if notifications {
            tls.set_alpn_protos(b"\x0ex-amzn-mqtt-ca")
                .map_err(|_| invalid())?;
        }
        tls.set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(|_| invalid())?;
        let (protocol, endpoint) = match material {
            Material::ThinQ1 { rti_server, .. } => {
                if let Some(ca) = &config.thinq1_ca {
                    tls.set_cert_store(roots(ca)?.build());
                } else {
                    tls.set_default_verify_paths().map_err(|_| invalid())?;
                }
                (
                    Protocol::ThinQ1,
                    service(rti_server).map_err(|_| invalid())?,
                )
            }
            Material::ThinQ2 {
                mqtt_server,
                ca_certificate,
                private_key,
                certificate,
                ..
            } => {
                if config.thinq1_ca.is_some() {
                    return Err(invalid());
                }
                tls.set_cert_store(roots(ca_certificate)?.build());
                let chain = X509::stack_from_pem(certificate.as_bytes()).map_err(|_| invalid())?;
                let leaf = chain.first().ok_or_else(invalid)?;
                client_certificate = Some(leaf.clone());
                let key =
                    PKey::private_key_from_pem(private_key.as_bytes()).map_err(|_| invalid())?;
                tls.set_certificate(leaf).map_err(|_| invalid())?;
                tls.set_private_key(&key).map_err(|_| invalid())?;
                tls.check_private_key().map_err(|_| invalid())?;
                for intermediate in chain.into_iter().skip(1) {
                    tls.add_extra_chain_cert(intermediate)
                        .map_err(|_| invalid())?;
                }
                (
                    Protocol::ThinQ2,
                    service(mqtt_server).map_err(|_| invalid())?,
                )
            }
        };
        let host = match endpoint.host().ok_or_else(invalid)? {
            url::Host::Domain(host) => host.to_owned(),
            url::Host::Ipv4(ip) => ip.to_string(),
            url::Host::Ipv6(ip) => ip.to_string(),
        };
        let port = endpoint
            .port()
            .or(match protocol {
                Protocol::ThinQ1 => None, // The fixed RTI reference requires an explicit port.
                Protocol::ThinQ2 => Some(8883),
            })
            .ok_or_else(invalid)?;
        Ok(Self {
            tls: tls.build(),
            client_certificate,
            protocol,
            host,
            port,
            timeout: config.timeout,
        })
    }
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }
    /// One bounded TCP+TLS attempt. The returned stream is owned by its caller;
    /// dropping this future or stream closes its socket and never starts a retry.
    pub async fn connect(&self) -> io::Result<SslStream<TcpStream>> {
        // A reusable connector must not dial again with an identity that expired
        // after its initial preparation. Retained cleanup uses stored material,
        // while live connections require current certificate validity.
        if let Some(certificate) = &self.client_certificate {
            let now = openssl::asn1::Asn1Time::days_from_now(0).map_err(|_| invalid())?;
            if certificate
                .not_before()
                .compare(&now)
                .map_err(|_| invalid())?
                == std::cmp::Ordering::Greater
                || certificate
                    .not_after()
                    .compare(&now)
                    .map_err(|_| invalid())?
                    != std::cmp::Ordering::Greater
            {
                return Err(invalid());
            }
        }
        timeout(self.timeout, async {
            let tcp = crate::resolver::connect_tcp(&self.host, self.port).await?;
            let ssl = self
                .tls
                .configure()
                .map_err(|_| invalid())?
                .into_ssl(&self.host)
                .map_err(|_| invalid())?;
            let mut stream = SslStream::new(ssl, tcp).map_err(|_| invalid())?;
            Pin::new(&mut stream).connect().await.map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "cloud TLS handshake rejected",
                )
            })?;
            Ok(stream)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cloud TCP/TLS connect timed out"))?
    }
}

/// Prepare ThinQ1 time-sync subscription before opening RTI. This request does
/// not register a device and is safe to repeat for each connection attempt.
pub async fn prepare_thinq1(
    material: &Material,
    device: &str,
    model: &str,
    device_type: &str,
) -> io::Result<()> {
    let Material::ThinQ1 { http_server, .. } = material else {
        return Err(invalid());
    };
    let mut endpoint = service(http_server).map_err(|_| invalid())?;
    if endpoint.scheme() != "https"
        || [device, model, device_type]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256 || s.chars().any(char::is_control))
    {
        return Err(invalid());
    }
    endpoint.set_path("/lgehadm/api/Device/TotalDeviceInfoSvc");
    let model = model
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;");
    let client = reqwest::Client::builder()
        .dns_resolver(crate::resolver::dns_resolver())
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| invalid())?;
    let response=client.post(endpoint).header("Accept","text/xml").header("content-type","text/xml;charset=utf-8").header("x-lgedm-userid","lgehadmUser").header("x-lgedm-password","bxLoLAZ+rp3oJDbEzRuIfAG4YumeqwWM9l6uUH6TupQ=").header("x-lgedm-deviceid",device).header("x-lgedm-devicetype",device_type).body(format!("<lgedmRoot><countryCode>WW</countryCode><modelName>{model}</modelName><itemList><item>THINQ_TIME_SYNC_URI</item><elementList><elementCode>pushDetailYn</elementCode><elementValue>Y</elementValue></elementList></itemList></lgedmRoot>")).send().await.map_err(|_|io::Error::other("ThinQ1 setup request failed"))?;
    if !response.status().is_success() {
        return Err(io::Error::other("ThinQ1 setup rejected"));
    }
    // No response body is required for the RTI subscription; dropping it bounds memory.
    Ok(())
}
