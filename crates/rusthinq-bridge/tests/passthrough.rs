use rusthinq_bridge::passthrough::{Config, ConnectFuture, Connector, Relay};
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    Server,
    certificates::Authority,
    tls::{Config as TlsConfig, Failure, FrontDoor},
};
use serde_json::json;
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, watch},
    task::JoinHandle,
    time::timeout,
};

struct Destination {
    address: Option<SocketAddr>,
    calls: AtomicUsize,
}
impl Connector for Destination {
    fn connect(&self, name: String) -> ConnectFuture {
        assert_eq!(name, "cdn.example");
        self.calls.fetch_add(1, Ordering::SeqCst);
        let address = self.address;
        Box::pin(async move {
            match address {
                Some(address) => TcpStream::connect(address).await,
                None => std::future::pending().await,
            }
        })
    }
}
struct Harness {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
    rejects: broadcast::Receiver<rusthinq_server::tls::Rejection>,
}
impl Harness {
    async fn start(relay: Relay) -> Self {
        static CA: OnceLock<Authority> = OnceLock::new();
        let ca = CA.get_or_init(|| Authority::generate("relay-tests.example", 2048).unwrap());
        let identity = ca
            .server_identity("local.example", TlsPolicy::Baseline)
            .unwrap();
        let front =
            FrontDoor::new(TlsConfig::default(), vec![identity], Some(Arc::new(relay))).unwrap();
        let rejects = front.subscribe();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = watch::channel(false);
        let server = Server::new(Default::default()).unwrap();
        let task = tokio::spawn(front.serve(listener, server, stopped));
        Self {
            address,
            stop,
            task,
            rejects,
        }
    }
    async fn connect(&self, host: &str) -> TcpStream {
        let mut peer = TcpStream::connect(self.address).await.unwrap();
        peer.write_all(&hello(host)).await.unwrap();
        peer
    }
    async fn rejected(&mut self) {
        assert_eq!(
            timeout(Duration::from_secs(2), self.rejects.recv())
                .await
                .unwrap()
                .unwrap()
                .reason,
            Failure::Io
        );
    }
    async fn shutdown(self) {
        self.stop.send_replace(true);
        timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
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
    for part in handshake.chunks(7) {
        result.extend_from_slice(&[22, 3, 1]);
        result.extend_from_slice(&(part.len() as u16).to_be_bytes());
        result.extend_from_slice(part);
    }
    result
}
fn destination(address: Option<SocketAddr>) -> Arc<Destination> {
    Arc::new(Destination {
        address,
        calls: AtomicUsize::new(0),
    })
}

#[test]
fn invalid_configuration_is_rejected() {
    for config in [
        Config {
            host_capacity: 0,
            ..Config::default()
        },
        Config {
            max_connections: 0,
            ..Config::default()
        },
        Config {
            connect_timeout: Duration::ZERO,
            ..Config::default()
        },
        Config {
            transfer_timeout: Some(Duration::ZERO),
            ..Config::default()
        },
    ] {
        assert!(Relay::new(config, destination(None)).is_err());
    }
}

#[tokio::test]
async fn shutdown_cancels_pending_dial_and_configured_local_names_do_not_relay() {
    let connector = destination(None);
    let relay = Relay::new(Config::default(), connector.clone()).unwrap();
    relay
        .learn_command(&json!([
            "https://local.example/fw",
            "https://cdn.example/fw"
        ]))
        .unwrap();
    let harness = Harness::start(relay).await;
    let mut local = harness.connect("local.example").await;
    // EOF forces the synthetic local handshake to finish without a relay dial.
    local.shutdown().await.unwrap();
    let _peer = harness.connect("cdn.example").await;
    timeout(Duration::from_secs(2), async {
        while connector.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    harness.shutdown().await;
    assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fragmented_client_hello_and_bytes_relay_exactly_with_half_close() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connector = destination(Some(upstream.local_addr().unwrap()));
    let relay = Relay::new(Config::default(), connector.clone()).unwrap();
    relay
        .learn_command(&json!({"sota":"https://cdn.example/fw"}))
        .unwrap();
    let harness = Harness::start(relay).await;
    let mut peer = TcpStream::connect(harness.address).await.unwrap();
    let hello = hello("cdn.example");
    for part in hello.chunks(3) {
        peer.write_all(part).await.unwrap();
    }
    peer.write_all(b"after hello").await.unwrap();
    peer.shutdown().await.unwrap();
    let (mut remote, _) = timeout(Duration::from_secs(2), upstream.accept())
        .await
        .unwrap()
        .unwrap();
    let mut received = Vec::new();
    timeout(Duration::from_secs(2), remote.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    let mut expected = hello;
    expected.extend_from_slice(b"after hello");
    assert_eq!(received, expected);
    remote.write_all(b"opaque TLS response").await.unwrap();
    remote.shutdown().await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(2), peer.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, b"opaque TLS response");
    assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
    harness.shutdown().await;
}

#[tokio::test]
async fn unlearned_and_protected_hosts_never_dial() {
    let connector = destination(None);
    let relay = Relay::new(Config::default(), connector.clone()).unwrap();
    let mut harness = Harness::start(relay.clone()).await;
    let _peer = harness.connect("cdn.example").await;
    harness.rejected().await;
    relay
        .learn_command(&json!("https://cdn.example/fw"))
        .unwrap();
    relay.confirm_local("cdn.example").unwrap();
    let _protected = harness.connect("cdn.example").await;
    harness.rejected().await;
    assert_eq!(connector.calls.load(Ordering::SeqCst), 0);
    harness.shutdown().await;
}

#[tokio::test]
async fn capacity_rejection_and_shutdown_close_both_sides() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connector = destination(Some(upstream.local_addr().unwrap()));
    let relay = Relay::new(
        Config {
            max_connections: 1,
            ..Config::default()
        },
        connector.clone(),
    )
    .unwrap();
    relay
        .learn_command(&json!("https://cdn.example/fw"))
        .unwrap();
    let mut harness = Harness::start(relay).await;
    let mut peer = harness.connect("cdn.example").await;
    let (mut remote, _) = timeout(Duration::from_secs(2), upstream.accept())
        .await
        .unwrap()
        .unwrap();
    let mut prefix = vec![0; hello("cdn.example").len()];
    remote.read_exact(&mut prefix).await.unwrap();
    let _extra = harness.connect("cdn.example").await;
    harness.rejected().await;
    assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
    harness.shutdown().await;
    assert_eq!(
        timeout(Duration::from_secs(2), remote.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        timeout(Duration::from_secs(2), peer.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn connection_and_transfer_timeouts_release_capacity() {
    let connector = destination(None);
    let relay = Relay::new(
        Config {
            connect_timeout: Duration::from_millis(50),
            ..Config::default()
        },
        connector.clone(),
    )
    .unwrap();
    relay
        .learn_command(&json!("https://cdn.example/fw"))
        .unwrap();
    let mut harness = Harness::start(relay).await;
    let _peer = harness.connect("cdn.example").await;
    harness.rejected().await;
    harness.shutdown().await;
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connector = destination(Some(upstream.local_addr().unwrap()));
    let relay = Relay::new(
        Config {
            max_connections: 1,
            transfer_timeout: Some(Duration::from_millis(100)),
            ..Config::default()
        },
        connector.clone(),
    )
    .unwrap();
    relay
        .learn_command(&json!("https://cdn.example/fw"))
        .unwrap();
    let mut harness = Harness::start(relay).await;
    let _peer = harness.connect("cdn.example").await;
    let (_remote, _) = upstream.accept().await.unwrap();
    harness.rejected().await;
    let _second = harness.connect("cdn.example").await;
    let (_remote2, _) = upstream.accept().await.unwrap();
    harness.rejected().await;
    assert_eq!(connector.calls.load(Ordering::SeqCst), 2);
    harness.shutdown().await;
}
