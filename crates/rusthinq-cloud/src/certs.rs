//! CA certificate load/create (rcgen + optional openssl CSR signing).

use anyhow::{Context, Result, bail};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use rusthinq_util::sync::Mutex;
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
    if !cert_covers_hostname(&cert_pem, hostname) {
        rusthinq_core::logging::log(
            "status",
            &[&format!(
                "Existing CA certificate at {} does not cover hostname {hostname:?}; recreating it",
                cert_path.display()
            )],
        );
        bail!("CA certificate subject/SAN does not match hostname {hostname:?}");
    }
    // A key that doesn't belong to this cert is worse than a hostname mismatch:
    // `mint_leaf` would go on signing every leaf with it, producing certificates
    // that fail to chain to the CA actually being handed out at
    // `/route/certificate`, indistinguishable from random TLS failures. Detected
    // the same way a hostname mismatch is (bail here, `load_or_create` recreates)
    // rather than as a separate hard-failure mode -- e.g. `ca.key`/`ca.cert` from
    // two different runs ending up side by side after a manual copy.
    if !keypair_matches_cert(&key_pem, &cert_pem) {
        rusthinq_core::logging::log(
            "status",
            &[&format!(
                "Existing CA key at {} does not belong to the certificate at {}; recreating both",
                key_path.display(),
                cert_path.display()
            )],
        );
        bail!("CA private key does not belong to the CA certificate");
    }
    Ok(Ca {
        key_pem,
        cert_pem,
        key_path: key_path.to_path_buf(),
        cert_path: cert_path.to_path_buf(),
    })
}

/// Whether `key_pem` is the private key that signed `cert_pem` -- compares public
/// keys rather than re-verifying a signature, so it works the same for both the
/// openssl (RSA) and rcgen (EC) creation paths. A key or cert that fails to parse
/// counts as not matching, same as [`cert_covers_hostname`]'s parse failures.
fn keypair_matches_cert(key_pem: &str, cert_pem: &str) -> bool {
    let Ok(key) = openssl::pkey::PKey::private_key_from_pem(key_pem.as_bytes()) else {
        return false;
    };
    let Ok(cert) = openssl::x509::X509::from_pem(cert_pem.as_bytes()) else {
        return false;
    };
    let Ok(cert_public_key) = cert.public_key() else {
        return false;
    };
    key.public_eq(&cert_public_key)
}

/// Whether `cert_pem`'s subject CommonName or a subjectAltName DNSName equals
/// `hostname` exactly. Both the openssl path (`-subj "/CN={hostname}"`, no SAN) and
/// the rcgen path (CN plus a DNSName SAN) set the CA up this way, so either match is
/// accepted. A cert that fails to parse counts as not matching, forcing recreation
/// rather than silently trusting unreadable bytes.
fn cert_covers_hostname(cert_pem: &str, hostname: &str) -> bool {
    let Ok(ders) = rustls_pemfile::certs(&mut cert_pem.as_bytes()).collect::<Result<Vec<_>, _>>()
    else {
        return false;
    };
    let Some(der) = ders.first() else {
        return false;
    };
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(der) else {
        return false;
    };
    let cn_matches = cert
        .subject()
        .iter_common_name()
        .any(|cn| cn.as_str() == Ok(hostname));
    let san_matches = cert
        .subject_alternative_name()
        .ok()
        .flatten()
        .is_some_and(|san| {
            san.value
                .general_names
                .iter()
                .any(|name| matches!(name, x509_parser::extensions::GeneralName::DNSName(n) if *n == hostname))
        });
    cn_matches || san_matches
}

/// Read and sanity-check a custom `custom_root_cert_file` (config.rs) at startup, not per
/// request: a missing or unusable file should stop rusthinq before it ever hands a device a
/// broken trust anchor, not fail silently down the line. Only the first PEM block is parsed,
/// as a check that the file really is a certificate; it's still served byte-for-byte.
pub fn load_root_certificate(path: &Path) -> Result<String> {
    let pem = fs::read_to_string(path)
        .with_context(|| format!("read custom_root_cert_file {}", path.display()))?;
    let ders = rustls_pemfile::certs(&mut pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("{} is not a certificate", path.display()))?;
    let der = ders
        .first()
        .with_context(|| format!("{} contains no certificate", path.display()))?;
    x509_parser::parse_x509_certificate(der)
        .with_context(|| format!("{} is not a certificate", path.display()))?;
    Ok(pem)
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
        // Re-check both conditions under the lock: while minting (the slow part, done
        // unlocked above so concurrent SNI callbacks for other names aren't serialized
        // behind it) a concurrent winner may have already inserted this exact hostname,
        // or filled the cache with other entries. Without this re-check, several
        // concurrent ClientHellos for distinct never-seen hostnames could all pass the
        // first (unlocked-at-mint-time) length check and all insert, overshooting
        // MAX_SNI_CERTS by up to (concurrency - 1).
        let mut cache = self.certs.lock();
        if let Some(existing) = cache.get(hostname) {
            return Some(existing.clone());
        }
        if cache.len() >= MAX_SNI_CERTS {
            return None;
        }
        cache.insert(hostname.to_string(), ctx.clone());
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
    // Disable TLS session tickets: a long-lived process's ticket-encryption key can
    // rotate (OpenSSL does this periodically) or the process can restart, silently
    // invalidating tickets the appliance is still holding. A well-behaved client falls
    // back to a full handshake when resumption fails; the cheap embedded TLS stacks in
    // these appliances are not always well-behaved about that, and can get stuck
    // retrying the same failing resumption indefinitely — indistinguishable from the
    // appliance being offline until this process restarts and mints a fresh key.
    // Forcing a full handshake every time costs a bit of CPU but removes that failure
    // mode entirely.
    builder.set_options(SslOptions::NO_TICKET);

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

/// A serial no other certificate of ours will carry. Certificates from one issuer
/// are identified by their serial, so a fixed one would mean every appliance holds
/// a certificate claiming to be the same one. The top bit is cleared so openssl,
/// which reads this as a signed big-endian integer, doesn't see it as negative
/// (rcgen's `SerialNumber` wants the same big-endian-positive shape).
fn random_serial_bytes() -> [u8; 16] {
    let mut bytes = *uuid::Uuid::new_v4().as_bytes();
    bytes[0] &= 0x7f;
    bytes
}

fn random_serial() -> String {
    format!("0x{}", rusthinq_util::hex::encode(random_serial_bytes()))
}

/// Sign a device CSR with the CA, in-process (rcgen) when possible, falling back to
/// [`sign_csr_openssl`] for a CSR shape rcgen's parser doesn't recognize (its
/// extension support is narrower than openssl's) — the appliance still gets a
/// certificate either way, at the cost of one subprocess spawn instead of zero.
pub async fn sign_csr(ca: &Ca, csr_pem: &str) -> Result<String> {
    match sign_csr_in_process(ca, csr_pem) {
        Ok(pem) => Ok(pem),
        Err(e) => {
            rusthinq_core::logging::log(
                "status",
                &[&format!(
                    "in-process CSR signing failed ({e:#}); falling back to openssl"
                )],
            );
            sign_csr_openssl(ca, csr_pem).await
        }
    }
}

/// Sign a device CSR with the CA, entirely in-process (no subprocess). Only the
/// CSR's subject and public key are used — like bare `openssl x509 -req` (no
/// `-copy_extensions`), any extensions the CSR itself requested (SAN, key usage,
/// …) are dropped rather than carried into the issued certificate, so switching
/// from the openssl subprocess to this doesn't change what devices are handed.
fn sign_csr_in_process(ca: &Ca, csr_pem: &str) -> Result<String> {
    let mut csr = CertificateSigningRequestParams::from_pem(csr_pem).context("parse CSR")?;
    csr.params.is_ca = IsCa::NoCa;
    csr.params.key_usages.clear();
    csr.params.extended_key_usages.clear();
    csr.params.subject_alt_names.clear();
    csr.params.serial_number = Some(random_serial_bytes().to_vec().into());

    let ca_key_pair = KeyPair::from_pem(&ca.key_pem).context("parse CA key")?;
    let issuer = Issuer::from_ca_cert_pem(&ca.cert_pem, ca_key_pair).context("parse CA cert")?;
    let cert = csr.signed_by(&issuer).context("sign CSR")?;
    Ok(cert.pem())
}

/// Sign a device CSR with the CA via a subprocess (openssl x509 -req) — the
/// original implementation, kept as [`sign_csr`]'s fallback.
async fn sign_csr_openssl(ca: &Ca, csr_pem: &str) -> Result<String> {
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
            &random_serial(),
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

/// Cap on `openssl` child processes [`sign_csr_gated`] lets run at once. `/device/{id}
/// /certificate` (this gate's only caller) is reachable by anyone who completes the
/// legacy TLS handshake -- `device_ssl_acceptor` sets `SslVerifyMode::NONE` -- with no
/// other check in front of it, so nothing but this cap stood between that endpoint and
/// unbounded PID/FD exhaustion from a flood of requests, or just one device stuck in a
/// reconnect/re-provisioning loop.
const MAX_CONCURRENT_SIGNS: usize = 4;

/// How long a signed cert is reused for a repeat of the exact same device+CSR, instead
/// of spawning `openssl` again. A device stuck in a reconnect loop resends the identical
/// CSR it already has a valid cert for; there is no reason to re-sign it every time.
const SIGN_DEDUPE_WINDOW: Duration = Duration::from_secs(60);

/// Bounds and dedupes concurrent [`sign_csr`] calls -- see [`MAX_CONCURRENT_SIGNS`] and
/// [`SIGN_DEDUPE_WINDOW`]. One instance is shared (via `Arc`) across every request the
/// HTTPS provisioning routes handle.
pub struct CsrGate {
    permits: tokio::sync::Semaphore,
    recent: Mutex<HashMap<(String, u64), (std::time::Instant, String)>>,
}

impl CsrGate {
    pub fn new() -> Self {
        Self {
            permits: tokio::sync::Semaphore::new(MAX_CONCURRENT_SIGNS),
            recent: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for CsrGate {
    fn default() -> Self {
        Self::new()
    }
}

fn hash_csr(csr_pem: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    csr_pem.hash(&mut hasher);
    hasher.finish()
}

/// [`sign_csr`], but bounded by `gate`'s concurrency cap and short-circuited when
/// `device_id` already re-sent the same CSR within [`SIGN_DEDUPE_WINDOW`].
pub async fn sign_csr_gated(
    ca: &Ca,
    device_id: &str,
    csr_pem: &str,
    gate: &CsrGate,
) -> Result<String> {
    let key = (device_id.to_string(), hash_csr(csr_pem));
    let now = std::time::Instant::now();
    {
        let mut recent = gate.recent.lock();
        recent.retain(|_, (issued, _)| now.duration_since(*issued) < SIGN_DEDUPE_WINDOW);
        if let Some((_, pem)) = recent.get(&key) {
            return Ok(pem.clone());
        }
    }
    let _permit = gate
        .permits
        .acquire()
        .await
        .context("CSR signing gate closed")?;
    let pem = sign_csr(ca, csr_pem).await?;
    gate.recent
        .lock()
        .insert(key, (std::time::Instant::now(), pem.clone()));
    Ok(pem)
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
    fn load_root_certificate_reads_back_a_real_cert_verbatim() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        let loaded = load_root_certificate(&cert_path).expect("load a real certificate");
        assert_eq!(loaded, ca.cert_pem);
    }

    /// A missing or unusable `custom_root_cert_file` must fail startup outright, not
    /// hand a device a broken trust anchor the first time it asks.
    #[test]
    fn load_root_certificate_rejects_a_missing_file() {
        let (_key_path, cert_path) = temp_ca_paths();
        assert!(load_root_certificate(&cert_path).is_err());
    }

    #[test]
    fn load_root_certificate_rejects_a_file_that_is_not_a_certificate() {
        let (_key_path, cert_path) = temp_ca_paths();
        fs::write(&cert_path, "not a certificate").unwrap();
        assert!(load_root_certificate(&cert_path).is_err());
    }

    /// The regression this test exists for: every appliance certificate used to carry
    /// the same fixed `-set_serial 0100`, so certificates from our CA could only be
    /// told apart by inspecting more than the serial — a serial is supposed to be
    /// enough on its own.
    #[test]
    fn random_serial_is_unique_and_positive() {
        let a = random_serial();
        let b = random_serial();
        assert_ne!(a, b);
        for serial in [&a, &b] {
            assert!(serial.starts_with("0x"));
            let hex = &serial[2..];
            assert_eq!(hex.len(), 32);
            assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
            // Top bit of the first byte cleared, so openssl (which reads this as a
            // signed big-endian integer) never sees a negative serial.
            let first_byte = u8::from_str_radix(&hex[0..2], 16).unwrap();
            assert!(first_byte < 0x80);
        }
    }

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

    /// #25: the hostname-mismatch check in `try_load` used to be inert — it only
    /// bailed when the cert PEM was empty, so a CA created for one hostname kept
    /// being loaded and served under a different configured hostname forever, with
    /// no recreation and no warning.
    #[test]
    fn try_load_accepts_a_cert_whose_cn_and_san_match_hostname() {
        let (key_path, cert_path) = temp_ca_paths();
        fast_test_ca("rusthinq.local", &key_path, &cert_path);
        assert!(try_load("rusthinq.local", &key_path, &cert_path).is_ok());
    }

    #[test]
    fn try_load_rejects_a_cert_created_for_a_different_hostname() {
        let (key_path, cert_path) = temp_ca_paths();
        fast_test_ca("old-hostname.local", &key_path, &cert_path);
        assert!(
            try_load("new-hostname.local", &key_path, &cert_path).is_err(),
            "changing the configured hostname must force recreation instead of \
             silently reusing the stale CA under the new name"
        );
    }

    /// Mirrors upstream's "a key belonging to another CA is refused" case (rusthinq
    /// issue #46's investigation): a `ca.key` from one CA sitting next to a
    /// `ca.cert` from another (e.g. after a partial manual copy) must not be
    /// silently accepted as a working CA.
    #[test]
    fn try_load_rejects_a_key_that_does_not_belong_to_the_certificate() {
        let (key_path_a, cert_path_a) = temp_ca_paths();
        let (key_path_b, cert_path_b) = temp_ca_paths();
        fast_test_ca("rusthinq.local", &key_path_a, &cert_path_a);
        fast_test_ca("rusthinq.local", &key_path_b, &cert_path_b);

        // cert_a paired with key_b: same hostname, unrelated keypairs.
        assert!(
            try_load("rusthinq.local", &key_path_b, &cert_path_a).is_err(),
            "a key belonging to a different CA must be refused, not silently trusted"
        );
    }

    #[test]
    fn keypair_matches_cert_true_for_a_real_pair_false_for_a_mismatched_one() {
        let (key_path_a, cert_path_a) = temp_ca_paths();
        let (key_path_b, cert_path_b) = temp_ca_paths();
        let ca_a = fast_test_ca("rusthinq.local", &key_path_a, &cert_path_a);
        let ca_b = fast_test_ca("rusthinq.local", &key_path_b, &cert_path_b);

        assert!(keypair_matches_cert(&ca_a.key_pem, &ca_a.cert_pem));
        assert!(!keypair_matches_cert(&ca_b.key_pem, &ca_a.cert_pem));
        assert!(!keypair_matches_cert("not a key", &ca_a.cert_pem));
    }

    #[test]
    fn cert_covers_hostname_matches_cn_only_certs_like_the_openssl_path() {
        // Mimics `openssl req -subj "/CN={hostname}"`: CN set, no SAN extension —
        // unlike create_with_rcgen's CA, which also sets a DNSName SAN.
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "cn-only.local");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key_pair = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();

        assert!(cert_covers_hostname(&cert.pem(), "cn-only.local"));
        assert!(!cert_covers_hostname(&cert.pem(), "other.local"));
    }

    #[test]
    fn cert_covers_hostname_rejects_unparseable_input() {
        assert!(!cert_covers_hostname("not a cert", "anything"));
        assert!(!cert_covers_hostname("", "anything"));
    }

    fn test_csr_pem() -> String {
        let key = KeyPair::generate().expect("generate CSR key");
        let params =
            CertificateParams::new(vec!["device.local".to_string()]).expect("build CSR params");
        params
            .serialize_request(&key)
            .expect("serialize CSR")
            .pem()
            .expect("CSR to PEM")
    }

    /// #45: replacing the `openssl x509 -req` subprocess with in-process (rcgen)
    /// signing must not start carrying the CSR's own requested extensions into the
    /// issued certificate -- bare `openssl x509 -req` (no `-copy_extensions`) always
    /// dropped them, and every already-paired device has only ever gotten a leaf
    /// signed that way.
    #[tokio::test]
    async fn sign_csr_produces_a_leaf_chaining_to_the_ca_without_the_csrs_own_san() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        // test_csr_pem() requests a `device.local` SAN -- must not survive signing.
        let pem = sign_csr(&ca, &test_csr_pem()).await.expect("sign CSR");

        let leaf = openssl::x509::X509::from_pem(pem.as_bytes()).expect("parse signed leaf");
        let ca_x509 = openssl::x509::X509::from_pem(ca.cert_pem.as_bytes()).expect("parse CA");
        assert!(
            leaf.verify(&ca_x509.public_key().unwrap()).unwrap(),
            "leaf must be signed by the CA"
        );
        assert!(
            !pem.contains("device.local"),
            "the CSR's own SAN must be dropped, not carried into the issued certificate"
        );
    }

    /// A CSR rcgen's narrower extension support can't parse (or any other in-process
    /// parse failure) must still get the appliance a certificate: `sign_csr` falls
    /// back to the openssl subprocess rather than failing provisioning outright. The
    /// openssl path itself is exercised directly since crafting a real CSR that
    /// only rcgen chokes on isn't practical here.
    #[tokio::test]
    async fn sign_csr_falls_back_to_openssl_when_in_process_signing_cannot_parse_the_csr() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        assert!(sign_csr_in_process(&ca, "not a csr").is_err());

        if std::process::Command::new("openssl")
            .arg("version")
            .output()
            .is_err()
        {
            eprintln!("skipping: openssl not found on PATH");
            return;
        }
        let pem = sign_csr_openssl(&ca, &test_csr_pem())
            .await
            .expect("openssl fallback must still sign a real CSR");
        assert!(pem.contains("BEGIN CERTIFICATE"));
    }

    /// #26: `/device/{id}/certificate` spawned a real `openssl` subprocess per request
    /// with no cap and no dedupe, reachable by anyone who completes the (verify-none)
    /// legacy TLS handshake -- a device stuck in a reconnect loop, or a flood of
    /// requests, could exhaust host PIDs/FDs.
    #[tokio::test]
    async fn sign_csr_gated_reuses_a_recent_signature_for_the_same_device_and_csr() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        let gate = CsrGate::new();
        let csr = test_csr_pem();

        let first = sign_csr_gated(&ca, "dev-1", &csr, &gate)
            .await
            .expect("first sign must succeed");
        let second = sign_csr_gated(&ca, "dev-1", &csr, &gate)
            .await
            .expect("second sign must succeed");
        assert_eq!(
            first, second,
            "a device re-sending the exact CSR it already has a cert for must get the \
             cached signature back, not a freshly (re-)signed one"
        );
    }

    #[tokio::test]
    async fn sign_csr_gated_does_not_share_a_cached_signature_across_devices() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        let gate = CsrGate::new();
        let csr = test_csr_pem();

        let a = sign_csr_gated(&ca, "dev-a", &csr, &gate)
            .await
            .expect("dev-a sign must succeed");
        let b = sign_csr_gated(&ca, "dev-b", &csr, &gate)
            .await
            .expect("dev-b sign must succeed");
        assert_ne!(
            a, b,
            "two different devices must not share one cached signature even if their \
             CSRs happen to be byte-identical"
        );
    }

    #[tokio::test]
    async fn sign_csr_gated_blocks_until_a_permit_is_free() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        let gate = CsrGate::new();
        let csr = test_csr_pem();

        // Hold every permit the gate has, so a fresh (uncached) sign has nowhere to go.
        let held = gate
            .permits
            .acquire_many(MAX_CONCURRENT_SIGNS as u32)
            .await
            .expect("must be able to acquire every permit up front");

        let blocked = tokio::time::timeout(
            Duration::from_millis(200),
            sign_csr_gated(&ca, "dev-blocked", &csr, &gate),
        )
        .await;
        assert!(
            blocked.is_err(),
            "sign_csr_gated must not spawn openssl while every permit is held"
        );

        drop(held);
        let unblocked = tokio::time::timeout(
            Duration::from_secs(10),
            sign_csr_gated(&ca, "dev-blocked", &csr, &gate),
        )
        .await;
        assert!(
            matches!(unblocked, Ok(Ok(_))),
            "sign_csr_gated must proceed once a permit is released"
        );
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

    /// #29: the cache-full check happened under the lock, but the lock was released
    /// before the slow mint_leaf/context_from_leaf call and only re-acquired to insert.
    /// Several concurrent misses for distinct never-seen hostnames could all pass that
    /// check while the one remaining slot was still free, then all insert, overshooting
    /// MAX_SNI_CERTS by up to (concurrency - 1).
    #[test]
    fn get_or_mint_does_not_overshoot_the_cap_under_concurrent_misses() {
        let (key_path, cert_path) = temp_ca_paths();
        let ca = fast_test_ca("rusthinq.test", &key_path, &cert_path);
        let cache = Arc::new(SniCache::new(ca).expect("build SNI cache"));

        // Fill every slot but one.
        for i in 0..MAX_SNI_CERTS - 1 {
            assert!(
                cache.get_or_mint(&format!("host{i}.example.com")).is_some(),
                "mint #{i} should succeed while under the cap"
            );
        }

        // Race real OS threads for the single remaining slot with distinct
        // never-before-seen hostnames, so each one is a genuine cache miss that has to
        // mint (the slow, unlocked part the TOCTOU window lives in).
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let cache = cache.clone();
                std::thread::spawn(move || cache.get_or_mint(&format!("race{i}.example.com")))
            })
            .collect();
        for t in threads {
            let _ = t.join().unwrap();
        }

        assert!(
            cache.certs.lock().len() <= MAX_SNI_CERTS,
            "cache must never exceed MAX_SNI_CERTS ({}) even when concurrent misses race \
             the single remaining slot; got {}",
            MAX_SNI_CERTS,
            cache.certs.lock().len()
        );

        let _ = fs::remove_dir_all(cert_path.parent().unwrap());
    }
}
