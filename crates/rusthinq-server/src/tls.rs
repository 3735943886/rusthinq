//! Owned SNI front door for ThinQ1 TLS sessions and an optional L4 passthrough hook.
//! Certificates and TLS policy are supplied at construction. With minting enabled, other
//! requested names get a bounded set of CA-signed leaves, as 0.1 did (anszom/rethink#107).
use crate::{Server, certificates::Authority};
use openssl::{
    pkey::PKey,
    ssl::{Ssl, SslAcceptor, SslContext, SslMethod, SslOptions, SslVerifyMode, SslVersion},
    x509::X509,
};
use rusthinq_protocol::{
    client_hello::{self, Hello},
    lg_compat::TlsPolicy,
};
use std::{
    collections::HashMap,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{broadcast, watch},
    task::JoinSet,
    time::{Instant, timeout_at},
};
use tokio_openssl::SslStream;

/// A leaf followed by its certificate chain, with a matching PEM private key.
/// File loading, CA creation, and leaf issuance belong to the composition root/services.
pub struct Identity {
    pub name: String,
    pub certificate_chain_pem: Vec<u8>,
    pub private_key_pem: Vec<u8>,
    pub policy: TlsPolicy,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub max_pending: usize,
    pub max_client_hello: usize,
    pub handshake_timeout: Duration,
    pub event_capacity: usize,
    /// Explicit default for appliances omitting SNI. None refuses such peers.
    pub no_sni_name: Option<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_pending: 64,
            max_client_hello: 65536,
            handshake_timeout: Duration::from_secs(10),
            event_capacity: 64,
            no_sni_name: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    Capacity,
    InvalidHello,
    HelloExceeded,
    Eof,
    Timeout,
    Unserved,
    Tls,
    Io,
    SessionRejected,
    Panic,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejection {
    pub peer: std::net::SocketAddr,
    pub reason: Failure,
}
/// Owns bytes read for routing, replaying them exactly once before reading the socket.
pub struct PrefixedStream {
    stream: TcpStream,
    prefix: Vec<u8>,
    offset: usize,
}
impl AsyncRead for PrefixedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.offset < this.prefix.len() {
            let count = buffer.remaining().min(this.prefix.len() - this.offset);
            buffer.put_slice(&this.prefix[this.offset..this.offset + count]);
            this.offset += count;
            if this.offset == this.prefix.len() {
                this.prefix = Vec::new();
                this.offset = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.stream).poll_read(cx, buffer)
    }
}
impl AsyncWrite for PrefixedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
pub type LocalFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
/// A lower-layer protocol service (for example provisioning HTTP), owned by this listener.
pub trait LocalService: Send + Sync {
    fn serve(&self, stream: Transport) -> LocalFuture;
}
pub type PassthroughFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
/// L4 decides allowed destinations and performs its own outbound connection policy.
/// L3 supplies untouched TLS bytes and owns cancellation/join of this future.
pub trait Passthrough: Send + Sync {
    fn relay(&self, name: String, stream: PrefixedStream) -> PassthroughFuture;
    /// With minting, only claimed names are relayed; the rest are terminated locally.
    fn claims(&self, _name: &str) -> bool {
        true
    }
    /// A local handshake for `name` completed: proof this name is answered locally.
    fn handshake_succeeded(&self, _name: &str) {}
    /// A local handshake for `name` failed, as a firmware download expecting a real
    /// root does; 0.1 suspected the name so the appliance's retry is relayed.
    fn handshake_failed(&self, _name: &str) {}
}
/// Distinct minted names per front door. Beyond it, or for names that are not
/// hostnames, the default certificate is served (0.1 `MAX_SNI_CERTS`).
const MAX_MINTED: usize = 64;
struct Minter {
    authority: Arc<Authority>,
    policy: TlsPolicy,
    default: SslContext,
    minted: Mutex<HashMap<String, SslContext>>,
}
impl Minter {
    /// The context for `name`: minted once per name off the I/O thread, else the default.
    async fn context(self: &Arc<Self>, name: Option<&str>) -> SslContext {
        let Some(name) = name
            .map(str::to_ascii_lowercase)
            .filter(|name| client_hello::hostname(name))
        else {
            return self.default.clone();
        };
        {
            let minted = self.minted.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(context) = minted.get(&name) {
                return context.clone();
            }
            if minted.len() >= MAX_MINTED {
                return self.default.clone();
            }
        }
        let minter = self.clone();
        let issued = name.clone();
        let context = tokio::task::spawn_blocking(move || {
            let identity = minter.authority.server_identity(&issued, minter.policy)?;
            acceptor(identity).map(SslAcceptor::into_context)
        })
        .await;
        let Ok(Ok(context)) = context else {
            return self.default.clone();
        };
        // Recheck: a concurrent handshake may have minted this name or filled the set.
        let mut minted = self.minted.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = minted.get(&name) {
            return existing.clone();
        }
        if minted.len() >= MAX_MINTED {
            return self.default.clone();
        }
        minted.insert(name, context.clone());
        context
    }
}
struct Router {
    config: Config,
    local: HashMap<String, SslAcceptor>,
    passthrough: Option<Arc<dyn Passthrough>>,
    minter: Option<Arc<Minter>>,
}
pub struct FrontDoor {
    router: Arc<Router>,
    events: broadcast::Sender<Rejection>,
}
impl FrontDoor {
    pub fn new(
        config: Config,
        identities: Vec<Identity>,
        passthrough: Option<Arc<dyn Passthrough>>,
    ) -> io::Result<Self> {
        if config.max_pending == 0
            || config.max_client_hello < 64
            || config.max_client_hello > 1_000_000
            || config.event_capacity == 0
            || config.handshake_timeout.is_zero()
            || identities.is_empty()
            || identities.len() > 64
        {
            return Err(invalid("invalid TLS front door configuration"));
        }
        let mut local = HashMap::new();
        for identity in identities {
            if !client_hello::hostname(&identity.name) {
                return Err(invalid("invalid served name"));
            }
            let name = identity.name.to_ascii_lowercase();
            if local.contains_key(&name) {
                return Err(invalid("duplicate served name"));
            }
            local.insert(name, acceptor(identity)?);
        }
        if let Some(name) = &config.no_sni_name
            && !local.contains_key(&name.to_ascii_lowercase())
        {
            return Err(invalid("no-SNI default is not served"));
        }
        let (events, _) = broadcast::channel(config.event_capacity);
        Ok(Self {
            router: Arc::new(Router {
                config,
                local,
                passthrough,
                minter: None,
            }),
            events,
        })
    }
    /// Serve every other requested name with a leaf minted from `authority` under
    /// `policy`, and peers without SNI (or beyond the minting bound) with `default`'s
    /// configured certificate. Passthrough then relays only names it claims.
    pub fn with_minting(
        mut self,
        authority: Arc<Authority>,
        policy: TlsPolicy,
        default: &str,
    ) -> io::Result<Self> {
        let router = Arc::get_mut(&mut self.router).ok_or_else(|| invalid("front door shared"))?;
        let default = router
            .local
            .get(&default.to_ascii_lowercase())
            .ok_or_else(|| invalid("minting default is not served"))?
            .context()
            .to_owned();
        router.minter = Some(Arc::new(Minter {
            authority,
            policy,
            default,
            minted: Mutex::new(HashMap::new()),
        }));
        Ok(self)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Rejection> {
        self.events.subscribe()
    }
    /// Owns listener, pending TLS/passthrough tasks, and the supplied session server.
    /// True or sender disappearance stops admission, cancels pending work, then joins sessions.
    pub async fn serve(
        self,
        listener: TcpListener,
        server: Server,
        stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        self.serve_inner(listener, Some(server), None, stop).await
    }
    /// Serve an L3 protocol service over local TLS instead of ThinQ1 sessions.
    pub async fn serve_service(
        self,
        listener: TcpListener,
        service: Arc<dyn LocalService>,
        stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        self.serve_inner(listener, None, Some(service), stop).await
    }
    async fn serve_inner(
        self,
        listener: TcpListener,
        mut server: Option<Server>,
        service: Option<Arc<dyn LocalService>>,
        mut stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let mut pending = JoinSet::new();
        let result = loop {
            if *stop.borrow() {
                break Ok(());
            }
            tokio::select! {
                biased;
                _ = stop.changed() => break Ok(()),
                joined = pending.join_next(), if !pending.is_empty() => {
                    match joined {
                        Some(Ok((peer, Ok(Some(stream))))) => {
                            if let Some(service) = &service {
                                let service = service.clone();
                                let events = self.events.clone();
                                pending.spawn(async move {
                                    let mut guard = PanicGuard { peer, events, armed: true };
                                    let result = service.serve(stream).await.map(|_| None).map_err(|_| Failure::Io);
                                    guard.armed = false;
                                    (peer, result)
                                });
                            } else if let Some(server) = &mut server
                                && server.admit(stream).is_err() { let _ = self.events.send(Rejection { peer, reason: Failure::SessionRejected }); }
                        }
                        Some(Ok((peer, Err(reason)))) => { let _ = self.events.send(Rejection { peer, reason }); }
                        _ => {}, // Panic is reported by the per-peer guard, with no retry.
                    }
                }
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        if pending.len() >= self.router.config.max_pending {
                            let _ = self.events.send(Rejection { peer, reason: Failure::Capacity });
                            continue;
                        }
                        let router = self.router.clone();
                        let events = self.events.clone();
                        pending.spawn(async move {
                            let mut guard = PanicGuard { peer, events, armed: true };
                            let outcome = router.route(stream).await;
                            guard.armed = false;
                            (peer, outcome)
                        });
                    }
                    Err(error) => break Err(error),
                }
            }
        };
        drop(listener);
        pending.abort_all();
        while pending.join_next().await.is_some() {}
        if let Some(server) = server {
            server.shutdown().await;
        }
        result
    }
}
struct PanicGuard {
    peer: std::net::SocketAddr,
    events: broadcast::Sender<Rejection>,
    armed: bool,
}
impl Drop for PanicGuard {
    fn drop(&mut self) {
        if self.armed && std::thread::panicking() {
            let _ = self.events.send(Rejection {
                peer: self.peer,
                reason: Failure::Panic,
            });
        }
    }
}
pub type Transport = SslStream<PrefixedStream>;
impl Router {
    async fn route(&self, mut stream: TcpStream) -> Result<Option<Transport>, Failure> {
        let deadline = Instant::now() + self.config.handshake_timeout;
        let (name, prefix) = timeout_at(
            deadline,
            read_hello(&mut stream, self.config.max_client_hello),
        )
        .await
        .map_err(|_| Failure::Timeout)??;
        let stream = PrefixedStream {
            stream,
            prefix,
            offset: 0,
        };
        let local_name = name.as_ref().or(self.config.no_sni_name.as_ref());
        if let Some(acceptor) =
            local_name.and_then(|name| self.local.get(&name.to_ascii_lowercase()))
        {
            let ssl = Ssl::new(acceptor.context()).map_err(|_| Failure::Tls)?;
            let mut transport = SslStream::new(ssl, stream).map_err(|_| Failure::Tls)?;
            timeout_at(deadline, Pin::new(&mut transport).accept())
                .await
                .map_err(|_| Failure::Timeout)?
                .map_err(|_| Failure::Tls)?;
            return Ok(Some(transport));
        }
        if let (Some(name), Some(hook)) = (&name, &self.passthrough)
            && (self.minter.is_none() || hook.claims(name))
        {
            hook.relay(name.clone(), stream)
                .await
                .map_err(|_| Failure::Io)?;
            return Ok(None);
        }
        let Some(minter) = &self.minter else {
            return Err(Failure::Unserved);
        };
        let context = minter.context(name.as_deref()).await;
        let accepted = async {
            let ssl = Ssl::new(&context).map_err(|_| Failure::Tls)?;
            let mut transport = SslStream::new(ssl, stream).map_err(|_| Failure::Tls)?;
            timeout_at(deadline, Pin::new(&mut transport).accept())
                .await
                .map_err(|_| Failure::Timeout)?
                .map_err(|_| Failure::Tls)?;
            Ok(transport)
        }
        .await;
        if let (Some(name), Some(hook)) = (&name, &self.passthrough) {
            match &accepted {
                Ok(_) => hook.handshake_succeeded(name),
                Err(_) => hook.handshake_failed(name),
            }
        }
        accepted.map(Some)
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn acceptor(identity: Identity) -> io::Result<SslAcceptor> {
    let error = |error| io::Error::new(io::ErrorKind::InvalidInput, error);
    let mut builder =
        SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).map_err(error)?;
    builder.set_verify(SslVerifyMode::NONE);
    match identity.policy {
        TlsPolicy::Baseline => builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(error)?,
        TlsPolicy::RtkRtl8711am => {
            builder.clear_options(SslOptions::NO_TLSV1 | SslOptions::NO_TLSV1_1);
            builder
                .set_min_proto_version(Some(SslVersion::TLS1))
                .map_err(error)?;
            builder.set_security_level(0);
            builder
                .set_cipher_list("DEFAULT:@SECLEVEL=0")
                .map_err(error)?;
            builder.set_options(SslOptions::CIPHER_SERVER_PREFERENCE | SslOptions::NO_TICKET);
        }
    }
    let mut chain = X509::stack_from_pem(&identity.certificate_chain_pem)
        .map_err(error)?
        .into_iter();
    let leaf = chain
        .next()
        .ok_or_else(|| invalid("empty certificate chain"))?;
    if !leaf.subject_alt_names().is_some_and(|names| {
        names.iter().any(|name| {
            name.dnsname()
                .is_some_and(|name| name.eq_ignore_ascii_case(&identity.name))
        })
    }) {
        return Err(invalid("certificate SAN does not cover served name"));
    }
    builder.set_certificate(&leaf).map_err(error)?;
    for cert in chain {
        builder.add_extra_chain_cert(cert).map_err(error)?;
    }
    let key = PKey::private_key_from_pem(&identity.private_key_pem).map_err(error)?;
    builder.set_private_key(&key).map_err(error)?;
    builder.check_private_key().map_err(error)?;
    Ok(builder.build())
}
async fn read_hello(
    stream: &mut TcpStream,
    maximum: usize,
) -> Result<(Option<String>, Vec<u8>), Failure> {
    let mut prefix = Vec::new();
    loop {
        match client_hello::parse(&prefix, maximum) {
            Ok(Hello::Ready(name)) => return Ok((name, prefix)),
            Err(client_hello::Error::Exceeded) => return Err(Failure::HelloExceeded),
            Err(client_hello::Error::Invalid) => return Err(Failure::InvalidHello),
            Ok(Hello::Incomplete) => {}
        }
        if prefix.len() == maximum {
            return Err(Failure::HelloExceeded);
        }
        let mut chunk = [0; 4096];
        let count = stream
            .read(&mut chunk[..4096.min(maximum - prefix.len())])
            .await
            .map_err(|_| Failure::Io)?;
        if count == 0 {
            return Err(Failure::Eof);
        }
        prefix.extend_from_slice(&chunk[..count]);
    }
}
