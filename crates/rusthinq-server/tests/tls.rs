use openssl::{
    asn1::Asn1Time,
    bn::{BigNum, MsbOption},
    hash::MessageDigest,
    pkey::{PKey, Private},
    rsa::Rsa,
    ssl::{SslConnector, SslMethod, SslVersion},
    x509::{
        X509, X509NameBuilder,
        extension::{BasicConstraints, SubjectAlternativeName},
    },
};
use rusthinq_protocol::{lg_compat::TlsPolicy, thinq1};
use rusthinq_server::{
    Config as ServerConfig, Event, Server,
    tls::{Config, Failure, FrontDoor, Identity, Passthrough, PassthroughFuture, PrefixedStream},
};
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_openssl::SslStream;

struct Certificate {
    name: &'static str,
    key: PKey<Private>,
    cert: X509,
}
fn certificates() -> &'static [Certificate; 2] {
    static CERTIFICATES: OnceLock<[Certificate; 2]> = OnceLock::new();
    CERTIFICATES.get_or_init(|| {
        ["first.example", "second.example"].map(|name| {
            let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
            let mut subject = X509NameBuilder::new().unwrap();
            subject.append_entry_by_text("CN", name).unwrap();
            let subject = subject.build();
            let mut builder = X509::builder().unwrap();
            builder.set_version(2).unwrap();
            let mut serial = BigNum::new().unwrap();
            serial.rand(128, MsbOption::MAYBE_ZERO, false).unwrap();
            builder
                .set_serial_number(&serial.to_asn1_integer().unwrap())
                .unwrap();
            builder.set_subject_name(&subject).unwrap();
            builder.set_issuer_name(&subject).unwrap();
            builder.set_pubkey(&key).unwrap();
            builder
                .set_not_before(&Asn1Time::days_from_now(0).unwrap())
                .unwrap();
            builder
                .set_not_after(&Asn1Time::days_from_now(1).unwrap())
                .unwrap();
            builder
                .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                .unwrap();
            let san = SubjectAlternativeName::new()
                .dns(name)
                .build(&builder.x509v3_context(None, None))
                .unwrap();
            builder.append_extension(san).unwrap();
            builder.sign(&key, MessageDigest::sha256()).unwrap();
            Certificate {
                name,
                key,
                cert: builder.build(),
            }
        })
    })
}
fn identities(policy: TlsPolicy) -> Vec<Identity> {
    certificates()
        .iter()
        .map(|cert| Identity {
            name: cert.name.into(),
            certificate_chain_pem: cert.cert.to_pem().unwrap(),
            private_key_pem: cert.key.private_key_to_pem_pkcs8().unwrap(),
            policy,
        })
        .collect()
}
async fn start(
    config: Config,
    policy: TlsPolicy,
    hook: Option<Arc<dyn Passthrough>>,
) -> (
    SocketAddr,
    watch::Sender<bool>,
    JoinHandle<std::io::Result<()>>,
    broadcast::Receiver<Event>,
    broadcast::Receiver<rusthinq_server::tls::Rejection>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = Server::new(ServerConfig::default()).unwrap();
    let events = server.handle().subscribe();
    let front = FrontDoor::new(config, identities(policy), hook).unwrap();
    let rejects = front.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(front.serve(listener, server, stopped));
    (address, stop, task, events, rejects)
}
async fn connect(
    address: SocketAddr,
    name: &str,
    legacy: bool,
    sni: bool,
) -> Result<SslStream<TcpStream>, String> {
    let mut connector = SslConnector::builder(SslMethod::tls_client()).unwrap();
    for certificate in certificates() {
        connector
            .cert_store_mut()
            .add_cert(certificate.cert.clone())
            .unwrap();
    }
    if legacy {
        connector.set_security_level(0);
        connector
            .set_min_proto_version(Some(SslVersion::TLS1))
            .unwrap();
        connector
            .set_max_proto_version(Some(SslVersion::TLS1))
            .unwrap();
        connector.set_cipher_list("AES128-SHA:@SECLEVEL=0").unwrap();
    }
    let mut configured = connector.build().configure().unwrap();
    configured.set_use_server_name_indication(sni);
    let ssl = configured.into_ssl(name).unwrap();
    let stream = TcpStream::connect(address).await.unwrap();
    let mut stream = SslStream::new(ssl, stream).unwrap();
    timeout(Duration::from_secs(3), Pin::new(&mut stream).connect())
        .await
        .unwrap()
        .map_err(|error| error.to_string())?;
    Ok(stream)
}
async fn stop(sender: watch::Sender<bool>, task: JoinHandle<std::io::Result<()>>) {
    sender.send_replace(true);
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn matching_certificates_and_thinq1_ack_over_real_tls() {
    let (address, shutdown, task, mut events, _) =
        start(Config::default(), TlsPolicy::Baseline, None).await;
    for (index, certificate) in certificates().iter().enumerate() {
        let mut stream = connect(address, certificate.name, false, true)
            .await
            .unwrap();
        assert_eq!(
            stream.ssl().peer_certificate().unwrap().to_der().unwrap(),
            certificate.cert.to_der().unwrap()
        );
        let payload = serde_json::to_vec(&serde_json::json!({"Header":{"x-lgedm-deviceId":format!("tls-{index}")},"Body":{"Cmd":"Mon"}})).unwrap();
        stream
            .write_all(&thinq1::encode(&payload, 1_000_000).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap(),
            Event::Up(_)
        ));
        let size = timeout(Duration::from_secs(2), stream.read_u32())
            .await
            .unwrap()
            .unwrap();
        let mut ack = vec![0; size as usize];
        stream.read_exact(&mut ack).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&ack).unwrap()["Body"]["Return"],
            "OK"
        );
        assert!(matches!(events.recv().await.unwrap(), Event::Sent(_, bytes) if bytes == ack));
        assert!(matches!(events.recv().await.unwrap(), Event::Data(_, bytes) if bytes == payload));
        // Next iteration should observe only its own Up/Data.
        drop(stream);
        assert!(matches!(
            timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap(),
            Event::Down(_, _)
        ));
    }
    stop(shutdown, task).await;
}
#[tokio::test]
async fn tls10_cbc_is_enabled_only_by_explicit_compatibility_policy() {
    let (address, shutdown, task, _, mut rejects) =
        start(Config::default(), TlsPolicy::Baseline, None).await;
    assert!(connect(address, "first.example", true, true).await.is_err());
    assert_eq!(
        timeout(Duration::from_secs(2), rejects.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        Failure::Tls
    );
    stop(shutdown, task).await;
    let (address, shutdown, task, _, _) =
        start(Config::default(), TlsPolicy::RtkRtl8711am, None).await;
    let stream = connect(address, "first.example", true, true).await.unwrap();
    assert_eq!(stream.ssl().version_str(), "TLSv1");
    assert_eq!(stream.ssl().current_cipher().unwrap().name(), "AES128-SHA");
    stop(shutdown, task).await;
}
#[tokio::test]
async fn no_sni_is_refused_unless_a_served_default_is_explicit() {
    let (address, shutdown, task, _, mut rejects) =
        start(Config::default(), TlsPolicy::Baseline, None).await;
    assert!(
        connect(address, "first.example", false, false)
            .await
            .is_err()
    );
    assert_eq!(rejects.recv().await.unwrap().reason, Failure::Unserved);
    stop(shutdown, task).await;
    let (address, shutdown, task, _, _) = start(
        Config {
            no_sni_name: Some("first.example".into()),
            ..Config::default()
        },
        TlsPolicy::Baseline,
        None,
    )
    .await;
    let stream = connect(address, "first.example", false, false)
        .await
        .unwrap();
    assert_eq!(
        stream.ssl().peer_certificate().unwrap().to_der().unwrap(),
        certificates()[0].cert.to_der().unwrap()
    );
    stop(shutdown, task).await;
}
fn hello(name: &str) -> Vec<u8> {
    let mut body = vec![3, 3];
    body.extend_from_slice(&[0; 32]);
    body.extend_from_slice(&[0, 0, 2, 0, 47, 1, 0]);
    let mut sni = ((name.len() + 3) as u16).to_be_bytes().to_vec();
    sni.push(0);
    sni.extend_from_slice(&(name.len() as u16).to_be_bytes());
    sni.extend_from_slice(name.as_bytes());
    let mut extensions = vec![0, 0];
    extensions.extend_from_slice(&(sni.len() as u16).to_be_bytes());
    extensions.extend_from_slice(&sni);
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend_from_slice(&extensions);
    let mut handshake = vec![1];
    handshake.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    handshake.extend_from_slice(&body);
    let mut result = Vec::new();
    // Fragment the handshake header and body across TLS records too.
    for part in handshake.chunks(7) {
        result.extend_from_slice(&[22, 3, 1]);
        result.extend_from_slice(&(part.len() as u16).to_be_bytes());
        result.extend_from_slice(part);
    }
    result
}
struct EchoHook {
    expected: Vec<u8>,
}
impl Passthrough for EchoHook {
    fn relay(&self, name: String, mut stream: PrefixedStream) -> PassthroughFuture {
        let expected = self.expected.clone();
        Box::pin(async move {
            assert_eq!(name, "outside.example");
            let mut bytes = vec![0; expected.len()];
            stream.read_exact(&mut bytes).await?;
            assert_eq!(bytes, expected);
            stream.write_all(&bytes).await?;
            Ok(())
        })
    }
}
#[tokio::test]
async fn fragmented_passthrough_replays_original_tls_bytes_or_refuses_without_hook() {
    let bytes = hello("outside.example");
    let (address, shutdown, task, _, mut rejects) =
        start(Config::default(), TlsPolicy::Baseline, None).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), rejects.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        Failure::Unserved
    );
    stop(shutdown, task).await;
    let hook = Arc::new(EchoHook {
        expected: bytes.clone(),
    });
    let (address, shutdown, task, _, _) =
        start(Config::default(), TlsPolicy::Baseline, Some(hook)).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    for byte in &bytes {
        stream.write_all(&[*byte]).await.unwrap();
        tokio::task::yield_now().await;
    }
    let mut echoed = vec![0; bytes.len()];
    timeout(Duration::from_secs(2), stream.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(echoed, bytes);
    stop(shutdown, task).await;
}
#[tokio::test]
async fn silent_peers_timeout_and_pending_capacity_and_shutdown_are_bounded() {
    let (address, shutdown, task, _, mut rejects) = start(
        Config {
            max_pending: 1,
            handshake_timeout: Duration::from_millis(200),
            ..Config::default()
        },
        TlsPolicy::Baseline,
        None,
    )
    .await;
    let _silent = TcpStream::connect(address).await.unwrap();
    // Accept a second peer in FIFO order without completing the first hello.
    let _excess = TcpStream::connect(address).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), rejects.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        Failure::Capacity
    );
    assert_eq!(
        timeout(Duration::from_secs(2), rejects.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        Failure::Timeout
    );
    let _pending = TcpStream::connect(address).await.unwrap();
    stop(shutdown, task).await;
}
#[test]
fn construction_rejects_bad_keys_names_and_defaults() {
    let mut values = identities(TlsPolicy::Baseline);
    values[0].private_key_pem = certificates()[1].key.private_key_to_pem_pkcs8().unwrap();
    assert!(FrontDoor::new(Config::default(), values, None).is_err());
    let mut values = identities(TlsPolicy::Baseline);
    values[0].name = "wrong.example".into();
    assert!(FrontDoor::new(Config::default(), values, None).is_err());
    let mut values = identities(TlsPolicy::Baseline);
    values[1].name = values[0].name.to_ascii_uppercase();
    assert!(FrontDoor::new(Config::default(), values, None).is_err());
    assert!(
        FrontDoor::new(
            Config {
                no_sni_name: Some("outside.example".into()),
                ..Config::default()
            },
            identities(TlsPolicy::Baseline),
            None
        )
        .is_err()
    );
    assert!(
        FrontDoor::new(
            Config {
                max_pending: 0,
                ..Config::default()
            },
            identities(TlsPolicy::Baseline),
            None
        )
        .is_err()
    );
}

#[tokio::test]
async fn malformed_and_oversized_hello_are_rejected_before_tls_or_passthrough() {
    let (address, shutdown, task, _, mut rejects) = start(
        Config {
            max_client_hello: 64,
            ..Config::default()
        },
        TlsPolicy::Baseline,
        Some(Arc::new(EchoHook {
            expected: Vec::new(),
        })),
    )
    .await;
    let mut invalid = TcpStream::connect(address).await.unwrap();
    invalid.write_all(&[23, 3, 3, 0, 1, 0]).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), rejects.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        Failure::InvalidHello
    );
    let mut oversized = TcpStream::connect(address).await.unwrap();
    oversized
        .write_all(&hello("outside.example"))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), rejects.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        Failure::HelloExceeded
    );
    stop(shutdown, task).await;
}

struct PendingHook {
    entered: tokio::sync::mpsc::Sender<()>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}
struct DropCounter(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Passthrough for PendingHook {
    fn relay(&self, _name: String, stream: PrefixedStream) -> PassthroughFuture {
        let entered = self.entered.clone();
        let drops = self.drops.clone();
        Box::pin(async move {
            let _counter = DropCounter(drops);
            let _stream = stream;
            entered.send(()).await.unwrap();
            std::future::pending::<()>().await;
            Ok(())
        })
    }
}
#[tokio::test]
async fn shutdown_cancels_and_joins_an_active_passthrough() {
    let (entered, mut receive) = tokio::sync::mpsc::channel(1);
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook = Arc::new(PendingHook {
        entered,
        drops: drops.clone(),
    });
    let (address, shutdown, task, _, _) =
        start(Config::default(), TlsPolicy::Baseline, Some(hook)).await;
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&hello("outside.example")).await.unwrap();
    timeout(Duration::from_secs(2), receive.recv())
        .await
        .unwrap()
        .unwrap();
    stop(shutdown, task).await;
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(2), client.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

struct PanicHook;
impl Passthrough for PanicHook {
    fn relay(&self, _name: String, _stream: PrefixedStream) -> PassthroughFuture {
        panic!("injected passthrough panic");
    }
}
#[tokio::test]
async fn passthrough_panic_is_reported_without_stopping_local_admission() {
    let (address, shutdown, task, _, mut rejects) = start(
        Config::default(),
        TlsPolicy::Baseline,
        Some(Arc::new(PanicHook)),
    )
    .await;
    let mut peer = TcpStream::connect(address).await.unwrap();
    peer.write_all(&hello("outside.example")).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), rejects.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        Failure::Panic
    );
    let _healthy = connect(address, "first.example", false, true)
        .await
        .unwrap();
    stop(shutdown, task).await;
}
