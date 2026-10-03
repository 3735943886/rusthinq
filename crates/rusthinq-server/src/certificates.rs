//! Caller-owned CA material and an owned, bounded CSR signing worker.
//! Key generation/loading are startup operations; network signing runs off I/O loops.
use crate::tls::Identity;
use openssl::{
    asn1::Asn1Time,
    bn::{BigNum, MsbOption},
    ec::{EcGroup, EcKey},
    hash::MessageDigest,
    nid::Nid,
    pkey::{PKey, Private},
    rsa::Rsa,
    sha::sha256,
    stack::Stack,
    x509::{
        X509, X509Builder, X509NameBuilder, X509Req, X509StoreContext,
        extension::{
            AuthorityKeyIdentifier, BasicConstraints, ExtendedKeyUsage, KeyUsage,
            SubjectAlternativeName, SubjectKeyIdentifier,
        },
        store::X509StoreBuilder,
        verify::X509VerifyFlags,
    },
};
use rusthinq_protocol::{client_hello::hostname, lg_compat::TlsPolicy};
use std::{
    collections::{HashMap, HashSet},
    io,
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
    time::Instant,
};

pub struct Authority {
    cert: X509,
    key: PKey<Private>,
    pem: String,
}
impl Authority {
    /// Load exactly the supplied material. Invalid material is never silently regenerated.
    pub fn load(cert: &Path, key: &Path) -> io::Result<Self> {
        Self::from_pem(&std::fs::read(cert)?, &std::fs::read(key)?)
    }
    pub fn from_pem(cert: &[u8], key: &[u8]) -> io::Result<Self> {
        let parsed = X509::from_pem(cert).map_err(crypto)?;
        let key = PKey::private_key_from_pem(key).map_err(crypto)?;
        if !parsed.public_key().map_err(crypto)?.public_eq(&key) {
            return Err(invalid("CA key/certificate mismatch"));
        }
        validate_issuer(&parsed, &key)?;
        let pem = std::str::from_utf8(cert)
            .map_err(|_| invalid("CA PEM is not UTF-8"))?
            .to_owned();
        Ok(Self {
            cert: parsed,
            key,
            pem,
        })
    }
    /// Explicit creation only. Persistence of the returned key/certificate is caller-owned.
    pub fn generate(name: &str, rsa_bits: u32) -> io::Result<Self> {
        if !hostname(name) || !matches!(rsa_bits, 2048 | 3072 | 4096) {
            return Err(invalid("invalid CA parameters"));
        }
        let key = PKey::from_rsa(Rsa::generate(rsa_bits).map_err(crypto)?).map_err(crypto)?;
        let mut subject = X509NameBuilder::new().map_err(crypto)?;
        subject.append_entry_by_text("CN", name).map_err(crypto)?;
        let subject = subject.build();
        let mut cert = certificate(3650)?;
        cert.set_subject_name(&subject).map_err(crypto)?;
        cert.set_issuer_name(&subject).map_err(crypto)?;
        cert.set_pubkey(&key).map_err(crypto)?;
        cert.append_extension(
            BasicConstraints::new()
                .critical()
                .ca()
                .build()
                .map_err(crypto)?,
        )
        .map_err(crypto)?;
        cert.append_extension(
            KeyUsage::new()
                .critical()
                .key_cert_sign()
                .crl_sign()
                .build()
                .map_err(crypto)?,
        )
        .map_err(crypto)?;
        let san = SubjectAlternativeName::new()
            .dns(name)
            .build(&cert.x509v3_context(None, None))
            .map_err(crypto)?;
        cert.append_extension(san).map_err(crypto)?;
        let subject_key = SubjectKeyIdentifier::new()
            .build(&cert.x509v3_context(None, None))
            .map_err(crypto)?;
        cert.append_extension(subject_key).map_err(crypto)?;
        cert.sign(&key, MessageDigest::sha256()).map_err(crypto)?;
        let cert = cert.build();
        let pem = String::from_utf8(cert.to_pem().map_err(crypto)?)
            .map_err(|_| invalid("invalid PEM"))?;
        Ok(Self { cert, key, pem })
    }
    pub fn certificate_pem(&self) -> &str {
        &self.pem
    }
    pub fn private_key_pem(&self) -> io::Result<Vec<u8>> {
        self.key.private_key_to_pem_pkcs8().map_err(crypto)
    }
    /// Prebuild a CA-signed RSA leaf for a configured served name, never a peer-supplied name.
    pub fn server_identity(&self, name: &str, policy: TlsPolicy) -> io::Result<Identity> {
        if !hostname(name) {
            return Err(invalid("invalid served name"));
        }
        // ECDSA P-256 like 0.1's rcgen leaves, which appliances are proven against; it is
        // also cheap enough to mint per requested name.
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).map_err(crypto)?;
        let key = PKey::from_ec_key(EcKey::generate(&group).map_err(crypto)?).map_err(crypto)?;
        let mut subject = X509NameBuilder::new().map_err(crypto)?;
        subject
            .append_entry_by_text("O", "rusthinq-leaf")
            .map_err(crypto)?;
        subject.append_entry_by_text("CN", name).map_err(crypto)?;
        let mut cert = certificate(3650)?;
        cert.set_subject_name(&subject.build()).map_err(crypto)?;
        cert.set_issuer_name(self.cert.subject_name())
            .map_err(crypto)?;
        cert.set_pubkey(&key).map_err(crypto)?;
        cert.append_extension(BasicConstraints::new().critical().build().map_err(crypto)?)
            .map_err(crypto)?;
        cert.append_extension(
            KeyUsage::new()
                .digital_signature()
                .key_encipherment()
                .build()
                .map_err(crypto)?,
        )
        .map_err(crypto)?;
        cert.append_extension(
            ExtendedKeyUsage::new()
                .server_auth()
                .build()
                .map_err(crypto)?,
        )
        .map_err(crypto)?;
        let san = SubjectAlternativeName::new()
            .dns(name)
            .build(&cert.x509v3_context(Some(&self.cert), None))
            .map_err(crypto)?;
        cert.append_extension(san).map_err(crypto)?;
        let subject_key = SubjectKeyIdentifier::new()
            .build(&cert.x509v3_context(Some(&self.cert), None))
            .map_err(crypto)?;
        cert.append_extension(subject_key).map_err(crypto)?;
        let authority_key = AuthorityKeyIdentifier::new()
            .keyid(false)
            .issuer(true)
            .build(&cert.x509v3_context(Some(&self.cert), None))
            .map_err(crypto)?;
        cert.append_extension(authority_key).map_err(crypto)?;
        cert.sign(&self.key, MessageDigest::sha256())
            .map_err(crypto)?;
        let mut chain = cert.build().to_pem().map_err(crypto)?;
        chain.extend_from_slice(self.pem.as_bytes());
        Ok(Identity {
            name: name.into(),
            certificate_chain_pem: chain,
            private_key_pem: key.private_key_to_pem_pkcs8().map_err(crypto)?,
            policy,
        })
    }
    fn sign(&self, csr: &[u8]) -> Result<String, SignError> {
        let csr = X509Req::from_pem(csr).map_err(|_| SignError::InvalidCsr)?;
        let public = csr.public_key().map_err(|_| SignError::InvalidCsr)?;
        if public.bits() > 8192 || !csr.verify(&public).map_err(|_| SignError::InvalidCsr)? {
            return Err(SignError::InvalidCsr);
        }
        let mut cert = certificate(3650).map_err(|_| SignError::Signing)?;
        cert.set_subject_name(csr.subject_name())
            .map_err(|_| SignError::Signing)?;
        cert.set_issuer_name(self.cert.subject_name())
            .map_err(|_| SignError::Signing)?;
        cert.set_pubkey(&public).map_err(|_| SignError::Signing)?;
        // Preserve bare openssl x509 -req behavior: never copy CSR extensions.
        cert.sign(&self.key, MessageDigest::sha256())
            .map_err(|_| SignError::Signing)?;
        String::from_utf8(cert.build().to_pem().map_err(|_| SignError::Signing)?)
            .map_err(|_| SignError::Signing)
    }
}
// Verify a short-lived probe chain at startup so a matching end-entity certificate,
// expired issuer, or invalid self-signature cannot accidentally become our CA.
fn validate_issuer(issuer: &X509, key: &PKey<Private>) -> io::Result<()> {
    let mut subject = X509NameBuilder::new().map_err(crypto)?;
    subject
        .append_entry_by_text("CN", "rusthinq-ca-validation")
        .map_err(crypto)?;
    subject
        .append_entry_by_text("O", "rusthinq-ca-validation")
        .map_err(crypto)?;
    let mut probe = certificate(1)?;
    probe.set_subject_name(&subject.build()).map_err(crypto)?;
    probe
        .set_issuer_name(issuer.subject_name())
        .map_err(crypto)?;
    probe.set_pubkey(key).map_err(crypto)?;
    probe.sign(key, MessageDigest::sha256()).map_err(crypto)?;
    let probe = probe.build();
    let mut store = X509StoreBuilder::new().map_err(crypto)?;
    store.add_cert(issuer.clone()).map_err(crypto)?;
    store
        .set_flags(X509VerifyFlags::CHECK_SS_SIGNATURE)
        .map_err(crypto)?;
    let mut context = X509StoreContext::new().map_err(crypto)?;
    let chain = Stack::new().map_err(crypto)?;
    if !context
        .init(&store.build(), &probe, &chain, |context| {
            context.verify_cert()
        })
        .map_err(crypto)?
    {
        return Err(invalid("invalid or expired certificate authority"));
    }
    Ok(())
}
fn certificate(days: u32) -> io::Result<X509Builder> {
    let mut cert = X509::builder().map_err(crypto)?;
    cert.set_version(2).map_err(crypto)?;
    let mut serial = BigNum::new().map_err(crypto)?;
    serial.rand(128, MsbOption::ONE, false).map_err(crypto)?;
    let serial = serial.to_asn1_integer().map_err(crypto)?;
    let before = Asn1Time::days_from_now(0).map_err(crypto)?;
    let after = Asn1Time::days_from_now(days).map_err(crypto)?;
    cert.set_serial_number(&serial).map_err(crypto)?;
    cert.set_not_before(&before).map_err(crypto)?;
    cert.set_not_after(&after).map_err(crypto)?;
    Ok(cert)
}
fn crypto(error: openssl::error::ErrorStack) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignError {
    Busy,
    Stopped,
    TooLarge,
    InvalidDevice,
    InvalidCsr,
    Signing,
    Panic,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub queue_capacity: usize,
    pub concurrency: usize,
    pub max_csr: usize,
    pub cache_capacity: usize,
    pub cache_ttl: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            queue_capacity: 32,
            concurrency: 4,
            max_csr: 16384,
            cache_capacity: 128,
            cache_ttl: Duration::from_secs(60),
        }
    }
}
type Key = (String, [u8; 32]);
type SignResult = Result<String, SignError>;
type Reply = oneshot::Sender<SignResult>;
type Completion = (Key, SignResult, Reply);
struct Request {
    key: Key,
    csr: Vec<u8>,
    reply: oneshot::Sender<Result<String, SignError>>,
}
#[derive(Clone)]
pub struct SigningHandle {
    send: mpsc::Sender<Request>,
    max_csr: usize,
}
impl SigningHandle {
    pub async fn sign(&self, device: &str, csr: &[u8]) -> Result<String, SignError> {
        if device.is_empty() || device.len() > 256 || device.contains(['/', '+', '#', '\0']) {
            return Err(SignError::InvalidDevice);
        }
        if csr.len() > self.max_csr {
            return Err(SignError::TooLarge);
        }
        let key = (device.to_owned(), sha256(csr));
        let (reply, result) = oneshot::channel();
        self.send
            .try_send(Request {
                key,
                csr: csr.to_vec(),
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SignError::Busy,
                mpsc::error::TrySendError::Closed(_) => SignError::Stopped,
            })?;
        result.await.unwrap_or(Err(SignError::Stopped))
    }
}
pub struct Signer {
    authority: Arc<Authority>,
    config: Config,
    receive: mpsc::Receiver<Request>,
}
impl Signer {
    pub fn new(authority: Arc<Authority>, config: Config) -> io::Result<(Self, SigningHandle)> {
        if config.queue_capacity == 0
            || config.concurrency == 0
            || config.concurrency > 32
            || config.max_csr == 0
            || config.max_csr > 65536
            || config.cache_capacity == 0
            || config.cache_ttl.is_zero()
        {
            return Err(invalid("invalid signer configuration"));
        }
        let (send, receive) = mpsc::channel(config.queue_capacity);
        let handle = SigningHandle {
            send,
            max_csr: config.max_csr,
        };
        Ok((
            Self {
                authority,
                config,
                receive,
            },
            handle,
        ))
    }
    /// Caller owns this future. On explicit stop, queued work fails and started CPU work
    /// is joined (not aborted or retried), even if its HTTP requester disconnected.
    pub async fn run(mut self, mut stop: watch::Receiver<bool>) {
        let mut tasks: JoinSet<Completion> = JoinSet::new();
        let mut inflight = HashSet::new();
        let mut cache: HashMap<Key, (Instant, String)> = HashMap::new();
        loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                biased;
                _ = stop.changed() => break,
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Ok((key, result, reply))) = result {
                        inflight.remove(&key);
                        if let Ok(pem) = &result {
                            if cache.len() >= self.config.cache_capacity
                                && let Some(oldest) = cache.iter().min_by_key(|(_, value)| value.0).map(|(key, _)| key.clone()) { cache.remove(&oldest); }
                            cache.insert(key, (Instant::now(), pem.clone()));
                        }
                        let _ = reply.send(result);
                    }
                }
                request = self.receive.recv() => {
                    let Some(request) = request else { break; };
                    if request.reply.is_closed() { continue; }
                    cache.retain(|_, value| value.0.elapsed() < self.config.cache_ttl);
                    if let Some((_, pem)) = cache.get(&request.key) { let _ = request.reply.send(Ok(pem.clone())); continue; }
                    if inflight.contains(&request.key) || tasks.len() >= self.config.concurrency {
                        let _ = request.reply.send(Err(SignError::Busy)); continue;
                    }
                    inflight.insert(request.key.clone());
                    let authority = self.authority.clone();
                    tasks.spawn_blocking(move || {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| authority.sign(&request.csr))).unwrap_or(Err(SignError::Panic));
                        (request.key, result, request.reply)
                    });
                }
            }
        }
        self.receive.close();
        while let Some(request) = self.receive.recv().await {
            let _ = request.reply.send(Err(SignError::Stopped));
        }
        while let Some(result) = tasks.join_next().await {
            if let Ok((_, result, reply)) = result {
                let _ = reply.send(result);
            }
        }
    }
}
