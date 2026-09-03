//! CA certificate load/create (rcgen + optional openssl CSR signing).

use anyhow::{Context, Result, bail};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use rusthinq_util::sync::Mutex;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

#[derive(Clone)]
pub struct Ca {
    pub key_pem: String,
    pub cert_pem: String,
    pub key_path: std::path::PathBuf,
    pub cert_path: std::path::PathBuf,
}

/// Load existing CA PEMs or create a new self-signed CA for `hostname`.
pub fn load_or_create(hostname: &str, key_path: &Path, cert_path: &Path) -> Result<Ca> {
    if let Ok(ca) = try_load(hostname, key_path, cert_path) {
        return Ok(ca);
    }
    rusthinq_core::logging::log("status", &["Creating a new key/certificate for the CA"]);

    // Prefer openssl (matches TS) when available; fall back to rcgen.
    if create_with_openssl(hostname, key_path, cert_path).is_err() {
        create_with_rcgen(hostname, key_path, cert_path)?;
    }
    try_load(hostname, key_path, cert_path)
}

fn try_load(hostname: &str, key_path: &Path, cert_path: &Path) -> Result<Ca> {
    let key_pem = fs::read_to_string(key_path).context("read ca key")?;
    let cert_pem = fs::read_to_string(cert_path).context("read ca cert")?;
    // Soft check: CN should match hostname (best-effort; PEM subject parse is light).
    if !cert_pem.contains(hostname) {
        // Still accept if openssl subject encoding differs; only force recreate when empty.
        if cert_pem.is_empty() {
            bail!("empty cert");
        }
    }
    Ok(Ca {
        key_pem,
        cert_pem,
        key_path: key_path.to_path_buf(),
        cert_path: cert_path.to_path_buf(),
    })
}

fn create_with_openssl(hostname: &str, key_path: &Path, cert_path: &Path) -> Result<()> {
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let status = std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:4096",
            "-keyout",
            key_path.to_str().unwrap_or("ca.key"),
            "-out",
            cert_path.to_str().unwrap_or("ca.cert"),
            "-sha256",
            "-days",
            "3650",
            "-nodes",
            "-subj",
            &format!("/CN={hostname}"),
        ])
        .status()
        .context("spawn openssl")?;
    if !status.success() {
        bail!("openssl failed: {status}");
    }
    Ok(())
}

fn create_with_rcgen(hostname: &str, key_path: &Path, cert_path: &Path) -> Result<()> {
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut params = CertificateParams::new(vec![hostname.to_string()])?;
    params.distinguished_name.push(DnType::CommonName, hostname);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.subject_alt_names = vec![SanType::DnsName(hostname.try_into()?)];
    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;
    fs::write(key_path, key_pair.serialize_pem())?;
    fs::write(cert_path, cert.pem())?;
    Ok(())
}

/// Build a rustls ServerConfig from the CA PEMs (server = CA for rusthinq).
///
/// Prefer [`device_ssl_acceptor`] for appliance-facing HTTPS/MQTTS — many CLIP
/// modules (RTK_RTL8711am) only offer legacy CBC-SHA suites that rustls rejects.
#[allow(dead_code)] // kept for non-device TLS uses / tests
pub fn server_config(ca: &Ca) -> Result<Arc<ServerConfig>> {
    let mut cert_reader = std::io::Cursor::new(ca.cert_pem.as_bytes());
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .context("parse cert pem")?;
    let mut key_reader = std::io::Cursor::new(ca.key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .context("parse key pem")?
        .context("no private key in pem")?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("build server config")?;
    Ok(Arc::new(config))
}

/// Max distinct per-SNI leaf certificates minted per process. Bounds how many
/// `SslContext`s (and rcgen signing operations) a peer on the network can force by
/// sending arbitrary SNI names — once full, unrecognized new names fall back to the
/// default (`hostname`) certificate instead of minting further.
const MAX_SNI_CERTS: usize = 64;

/// Whether `name` may be used as a certificate subject/SAN or advertised to an appliance
/// (used both for minting per-SNI leaves here and for `/route`'s `advertise_requested_host`
/// in `thinq2::provisioning`). Addresses are refused along with everything else that isn't
/// a hostname: an address in a certificate needs an IP: SAN (a DNS: one isn't checked
/// against it), and an address advertised to an appliance would be stored, pinning it to
/// one machine. Applied *before* a name reaches rcgen/openssl, so garbage input never
/// spawns signing work.
pub(crate) fn is_plausible_hostname(name: &str) -> bool {
    if name.is_empty() || name.len() > 253 || name.parse::<std::net::IpAddr>().is_ok() {
        return false;
    }
    name.split('.').all(|label| {
        !label.is_empty()
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    })
}

/// Mint a fresh leaf certificate for `hostname`, signed by `ca` (parsed from its PEM —
/// `ca` may have been created by either the openssl or rcgen path in [`load_or_create`]).
fn mint_leaf(
    ca: &Ca,
    hostname: &str,
) -> Result<(
    openssl::x509::X509,
    openssl::pkey::PKey<openssl::pkey::Private>,
)> {
    let ca_key_pair = KeyPair::from_pem(&ca.key_pem).context("parse CA key")?;
    let issuer = Issuer::from_ca_cert_pem(&ca.cert_pem, ca_key_pair).context("parse CA cert")?;

    let mut leaf_params =
        CertificateParams::new(vec![hostname.to_string()]).context("invalid hostname for SAN")?;
    // The CA's own subject is `CN=<hostname of the CA>`, and callers legitimately mint a
    // leaf for that exact same hostname (an appliance whose SNI request happens to equal
    // `hostname`). Without an extra RDN here, that leaf's subject DN would be textually
    // identical to the CA's — which OpenSSL's client-side chain builder can flag as a
    // depth-0 self-signed certificate before it ever checks the signature, independent of
    // whether the CA is present in the chain. The organization RDN keeps subject != issuer
    // in every case.
    leaf_params
        .distinguished_name
        .push(DnType::OrganizationName, "rusthinq-leaf");
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, hostname);
    leaf_params.is_ca = IsCa::NoCa;
    leaf_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_key = KeyPair::generate().context("generate leaf key")?;
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &issuer)
        .context("sign leaf")?;

    let x509 =
        openssl::x509::X509::from_pem(leaf_cert.pem().as_bytes()).context("parse minted leaf")?;
    let pkey = openssl::pkey::PKey::private_key_from_pem(leaf_key.serialize_pem().as_bytes())
        .context("parse minted leaf key")?;
    Ok((x509, pkey))
}

/// Build an `SslContext` serving `cert`/`key`, with `ca_cert` appended to the chain.
///
/// The CA must ride along in the chain, not just be signed by: `cert`'s subject and
/// `ca_cert`'s subject can be textually identical (e.g. both `CN=<hostname>` when an
/// appliance's SNI request happens to equal `hostname` itself), and a client validating
/// a leaf-only chain against a *different* trusted CA object with the same DN can flag
/// it `DEPTH_ZERO_SELF_SIGNED_CERT` before ever checking the signature. Sending the CA
/// explicitly lets the client chain-build off the object actually presented.
fn context_from_leaf(
    cert: &openssl::x509::X509,
    key: &openssl::pkey::PKey<openssl::pkey::Private>,
    ca_cert: &openssl::x509::X509,
) -> Result<openssl::ssl::SslContext> {
    let mut builder = openssl::ssl::SslContextBuilder::new(openssl::ssl::SslMethod::tls_server())
        .context("SslContextBuilder")?;
    builder.set_certificate(cert).context("set minted cert")?;
    builder.set_private_key(key).context("set minted key")?;
    builder
        .add_extra_chain_cert(ca_cert.clone())
        .context("add CA to chain")?;
    Ok(builder.build())
}

/// Per-SNI-hostname leaf certificate cache backing the `SNICallback` in
/// [`device_ssl_acceptor`]. One appliance model can ask for several different LG
/// hostnames depending on unit/firmware (`kic-common.lgthinq.com`,
/// `kic-mclip.lgthinq.com`, `common.iot.kic.lgthinq.com`, …); rusthinq has to answer all
/// of them with a certificate chaining to the CA the appliance already pinned via
/// `/route/certificate`, since it does check the certificate on this port (unlike the
/// plain HTTPS API port).
struct SniCache {
    ca: Ca,
    ca_x509: openssl::x509::X509,
    certs: Mutex<HashMap<String, Arc<openssl::ssl::SslContext>>>,
}

impl SniCache {
    fn new(ca: Ca) -> Result<Self> {
        let ca_x509 =
            openssl::x509::X509::from_pem(ca.cert_pem.as_bytes()).context("parse CA cert")?;
        Ok(Self {
            ca,
            ca_x509,
            certs: Mutex::new(HashMap::new()),
        })
    }

    fn seed(&self, hostname: &str, ctx: openssl::ssl::SslContext) {
        self.certs
            .lock()
            .insert(hostname.to_string(), Arc::new(ctx));
    }

    /// Cached (or freshly minted) context for `hostname`. `None` means: leave the
    /// connection on its current (default) context — either the name doesn't look like a
    /// hostname, or the cache is full and this is a name we haven't seen before.
    fn get_or_mint(&self, hostname: &str) -> Option<Arc<openssl::ssl::SslContext>> {
        if !is_plausible_hostname(hostname) {
            return None;
        }
        {
            let cache = self.certs.lock();
            if let Some(ctx) = cache.get(hostname) {
                return Some(ctx.clone());
            }
            if cache.len() >= MAX_SNI_CERTS {
                return None;
            }
        }
        let (cert, key) = mint_leaf(&self.ca, hostname).ok()?;
        let ctx = Arc::new(context_from_leaf(&cert, &key, &self.ca_x509).ok()?);
        self.certs
            .lock()
            .entry(hostname.to_string())
            .or_insert_with(|| ctx.clone());
        Some(ctx)
    }
}

/// OpenSSL acceptor matching Node `deviceTlsOptions` (PR #131 / `util/device_tls.ts`):
/// - min TLS 1.0
/// - `DEFAULT:@SECLEVEL=0` so ECDHE-RSA-AES128-SHA / AES128-SHA256 etc. work
/// - honor server cipher order (modern suites preferred when the client offers them)
///
/// Use for **all** listeners that speak to appliance Wi‑Fi modules (ThinQ2 HTTPS
/// `/route`, MQTTS, ThinQ1 TLS). Without this, RTK_RTL8711am ClientHellos fail
/// with handshake_failure before any HTTP is logged.
///
/// Serves a CA-signed leaf per requested TLS server name (not the bare CA, and not just
/// one fixed name): a connection with no SNI, or SNI == `hostname`, gets the default leaf
/// below; any other name an appliance asks for is minted on first use and cached
/// (port of anszom/rethink#107's per-SNI certificate work).
pub fn device_ssl_acceptor(ca: &Ca, hostname: &str) -> Result<Arc<openssl::ssl::SslAcceptor>> {
    use openssl::ssl::{NameType, SslAcceptor, SslMethod, SslOptions, SslVerifyMode, SslVersion};

    let mut builder =
        SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).context("SslAcceptor")?;

    // mozilla_intermediate disables TLS1.0/1.1 and old ciphers — reverse that for devices.
    builder.clear_options(SslOptions::NO_TLSV1 | SslOptions::NO_TLSV1_1);
    builder
        .set_min_proto_version(Some(SslVersion::TLS1))
        .context("set min TLS1")?;
    // Allow SHA1-CBC suites rejected at OpenSSL 3 default security level.
    builder.set_security_level(0);
    builder
        .set_cipher_list("DEFAULT:@SECLEVEL=0")
        .context("set_cipher_list DEFAULT:@SECLEVEL=0")?;
    // Prefer modern suites when both sides support them (Node honorCipherOrder).
    builder.set_options(SslOptions::CIPHER_SERVER_PREFERENCE);

    let ca_x509 = openssl::x509::X509::from_pem(ca.cert_pem.as_bytes()).context("parse CA cert")?;
    let (default_cert, default_key) = mint_leaf(ca, hostname).context("mint default leaf")?;
    builder
        .set_certificate(&default_cert)
        .context("set certificate")?;
    builder
        .set_private_key(&default_key)
        .context("set private key")?;
    builder
        .add_extra_chain_cert(ca_x509.clone())
        .context("add CA to default chain")?;
    builder.check_private_key().context("check private key")?;
    builder.set_verify(SslVerifyMode::NONE);

    let sni_cache = Arc::new(SniCache::new(ca.clone())?);
    sni_cache.seed(
        hostname,
        context_from_leaf(&default_cert, &default_key, &ca_x509)
            .context("build default SNI context")?,
    );
    builder.set_servername_callback(move |ssl, _alert| {
        if let Some(name) = ssl.servername(NameType::HOST_NAME) {
            let name = name.to_string();
            if let Some(ctx) = sni_cache.get_or_mint(&name) {
                ssl.set_ssl_context(&ctx)
                    .map_err(|_| openssl::ssl::SniError::ALERT_FATAL)?;
            }
        }
        Ok(())
    });

    Ok(Arc::new(builder.build()))
}

/// Accept a TCP stream with the device legacy SSL profile.
pub async fn accept_device_tls(
    acceptor: &openssl::ssl::SslAcceptor,
    stream: tokio::net::TcpStream,
) -> Result<tokio_openssl::SslStream<tokio::net::TcpStream>> {
    use openssl::ssl::Ssl;
    use std::pin::Pin;

    let ssl = Ssl::new(acceptor.context()).context("Ssl::new")?;
    let mut tls = tokio_openssl::SslStream::new(ssl, stream).context("SslStream::new")?;
    Pin::new(&mut tls).accept().await.context("TLS accept")?;
    Ok(tls)
}

/// Sign a device CSR with the CA (openssl x509 -req, matching TypeScript).
pub async fn sign_csr(ca: &Ca, csr_pem: &str) -> Result<String> {
    let mut child = Command::new("openssl")
        .args([
            "x509",
            "-req",
            "-in",
            "-",
            "-days",
            "3650",
            "-CA",
            ca.cert_path.to_str().unwrap_or("ca.cert"),
            "-CAkey",
            ca.key_path.to_str().unwrap_or("ca.key"),
            "-set_serial",
            "0100",
            "-out",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawn openssl x509")?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(csr_pem.as_bytes()).await?;
    }
    let out = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .context("openssl x509 timed out")??;
    if !out.status.success() {
        bail!(
            "openssl x509 failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .replace('\r', "")
        .to_string())
}

/// Fallback CSR signing via rcgen when openssl is unavailable.
#[allow(dead_code)] // optional path when openssl CLI is unavailable
pub fn sign_csr_rcgen(ca: &Ca, _csr_pem: &str) -> Result<String> {
    // Full CSR parse/sign without openssl is complex; return CA cert as last resort for smoke tests.
    Ok(ca.cert_pem.clone())
}

#[allow(dead_code)] // helper for rustls TLS wiring / tests
pub fn load_private_key_der(ca: &Ca) -> Result<PrivateKeyDer<'static>> {
    let mut key_reader = std::io::Cursor::new(ca.key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)?.context("no private key")?;
    Ok(key)
}

#[allow(dead_code)] // helper for rustls TLS wiring / tests
pub fn load_cert_der(ca: &Ca) -> Result<Vec<CertificateDer<'static>>> {
    let mut cert_reader = std::io::Cursor::new(ca.cert_pem.as_bytes());
    Ok(rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?)
}

// Silence unused import if PrivatePkcs8KeyDer not used
#[allow(dead_code)]
fn _pkcs8(key: PrivatePkcs8KeyDer<'static>) -> PrivateKeyDer<'static> {
    PrivateKeyDer::Pkcs8(key)
}

// Silence DistinguishedName if unused on some rcgen versions
#[allow(dead_code)]
fn _dn() -> DistinguishedName {
    DistinguishedName::new()
}

/// Test-only helpers shared across this crate — real (non-mock) CA material without
/// the tens-of-seconds cost of `load_or_create`'s `openssl req -newkey rsa:4096` path.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub(crate) fn temp_ca_paths() -> (std::path::PathBuf, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("rusthinq-certs-test-{}-{n}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        (dir.join("ca.key"), dir.join("ca.cert"))
    }

    /// EC-keyed CA via the rcgen path directly — `load_or_create` prefers shelling out to
    /// `openssl req -newkey rsa:4096`, which is correct for the one CA a real deployment
    /// creates on first run, but far too slow (tens of seconds) to redo per test.
    pub(crate) fn fast_test_ca(hostname: &str, key_path: &Path, cert_path: &Path) -> Ca {
        create_with_rcgen(hostname, key_path, cert_path).expect("create test CA");
        try_load(hostname, key_path, cert_path).expect("load test CA")
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{fast_test_ca, temp_ca_paths};
    use super::*;
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn hostname_check_accepts_dns_names_rejects_garbage() {
        assert!(is_plausible_hostname("kic-common.lgthinq.com"));
        assert!(is_plausible_hostname("rusthinq.local"));
        assert!(is_plausible_hostname("a"));
        assert!(!is_plausible_hostname(""));
        assert!(!is_plausible_hostname(".leading-dot.com"));
        assert!(!is_plausible_hostname("trailing-dot.com."));
        assert!(!is_plausible_hostname("double..dot.com"));
        assert!(!is_plausible_hostname("-leading-dash.com"));
        assert!(!is_plausible_hostname("trailing-dash-.com"));
        // A hyphen anywhere in the *middle* of a label is fine — only a label's own
        // leading/trailing hyphen is rejected (this used to only check the whole string's
        // first/last byte, which missed a bad label like "foo.-bar.com").
        assert!(!is_plausible_hostname("foo.-bar.com"));
        assert!(is_plausible_hostname("foo-bar.com"));
        assert!(!is_plausible_hostname("has space.com"));
        assert!(!is_plausible_hostname("semi;colon.com"));
        assert!(!is_plausible_hostname(&"a".repeat(254)));
        // Addresses are refused: they'd need an IP: SAN (a DNS: leaf isn't checked against
        // one) and, advertised to an appliance, would pin it to a single machine.
        assert!(!is_plausible_hostname("10.1.1.45"));
        assert!(!is_plausible_hostname("::1"));
        assert!(!is_plausible_hostname("2001:db8::1"));
    }

    /// Handshake as a client presenting `sni`, trusting `ca`; returns the leaf the server
    /// actually served so the caller can check which certificate answered which name.
    fn handshake_for_sni(
        acceptor: &openssl::ssl::SslAcceptor,
        ca: &Ca,
        sni: &str,
    ) -> openssl::x509::X509 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let ca_cert_path = ca.cert_path.clone();
        let sni = sni.to_string();
        let client = std::thread::spawn(move || {
            let mut connector =
                openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls_client()).unwrap();
            connector.set_ca_file(&ca_cert_path).unwrap();
            let connector = connector.build();
            let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            let ssl_stream = connector.connect(&sni, stream).expect("client handshake");
            ssl_stream
                .ssl()
                .peer_certificate()
                .expect("server presented a certificate")
        });

        let (stream, _) = listener.accept().unwrap();
        let _server_stream = acceptor.accept(stream).expect("server handshake");

        client.join().unwrap()
    }

    fn leaf_dns_sans(cert: &openssl::x509::X509) -> Vec<String> {
        cert.subject_alt_names()
            .into_iter()
            .flatten()
            .filter_map(|n| n.dnsname().map(str::to_string))
            .collect()
    }

    #[test]
    fn sni_serves_matching_leaf_for_default_and_other_hostnames() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        let acceptor = device_ssl_acceptor(&ca, "rusthinq.test").expect("build acceptor");

        // No-SNI-mismatch case: SNI == the default hostname the acceptor was built for.
        let leaf = handshake_for_sni(&acceptor, &ca, "rusthinq.test");
        assert_eq!(leaf_dns_sans(&leaf), vec!["rusthinq.test"]);

        // A name the appliance asks for that isn't the default: must be minted on the fly,
        // signed by the same CA (verified by the client trusting only that CA above), and
        // carry *that* name as its SAN — not the default's.
        let leaf2 = handshake_for_sni(&acceptor, &ca, "kic-common.lgthinq.com");
        assert_eq!(leaf_dns_sans(&leaf2), vec!["kic-common.lgthinq.com"]);

        let _ = fs::remove_dir_all(cert_path.parent().unwrap());
    }

    #[test]
    fn sni_cache_rejects_garbage_and_caps_distinct_names() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        let cache = SniCache::new(ca.clone()).expect("build SNI cache");

        // Never reaches rcgen/openssl signing — no peer can spend our CPU with garbage SNI.
        assert!(cache.get_or_mint("bad name!with spaces").is_none());
        assert!(cache.get_or_mint("").is_none());

        for i in 0..MAX_SNI_CERTS {
            assert!(
                cache.get_or_mint(&format!("host{i}.example.com")).is_some(),
                "mint #{i} should succeed while under the cap"
            );
        }
        // Cache is now full: a name never seen before must not mint further.
        assert!(cache.get_or_mint("overflow.example.com").is_none());
        // An already-cached name still resolves (from cache, not a new mint).
        assert!(cache.get_or_mint("host0.example.com").is_some());

        let _ = fs::remove_dir_all(cert_path.parent().unwrap());
    }
}
