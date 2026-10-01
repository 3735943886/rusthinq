mod support;
use openssl::{
    ssl::{SslConnector, SslMethod},
    x509::X509,
};
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    certificates::{Config as SignConfig, Signer, SigningHandle},
    provisioning::{Config, Service},
    tls::{Config as TlsConfig, FrontDoor},
};
use serde_json::{Value, json};
use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};
use tokio_openssl::SslStream;
struct Harness {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    front: JoinHandle<std::io::Result<()>>,
    signer: JoinHandle<()>,
    handle: SigningHandle,
    metadata: tokio::sync::mpsc::Receiver<rusthinq_server::thinq1_http::Metadata>,
}
impl Harness {
    async fn start(config: Config, custom: Option<&[u8]>) -> Self {
        let ca = support::authority();
        let (owner, handle) = Signer::new(ca.clone(), SignConfig::default()).unwrap();
        let provisioning = Service::new(config, &ca, handle.clone(), custom).unwrap();
        let (thin, metadata) = rusthinq_server::thinq1_http::Service::new(
            Default::default(),
            Arc::new(rusthinq_server::mqtt::SystemClock),
        )
        .unwrap();
        let service = rusthinq_server::https::Service::new(
            thin,
            provisioning,
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap();
        let identity = ca
            .server_identity("local.example", TlsPolicy::Baseline)
            .unwrap();
        let front = FrontDoor::new(TlsConfig::default(), vec![identity], None).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = watch::channel(false);
        let signer = tokio::spawn(owner.run(stopped.clone()));
        let front = tokio::spawn(front.serve_service(listener, Arc::new(service), stopped));
        Self {
            address,
            stop,
            front,
            signer,
            handle,
            metadata,
        }
    }
    async fn connect(&self) -> SslStream<TcpStream> {
        let mut connector = SslConnector::builder(SslMethod::tls_client()).unwrap();
        connector
            .cert_store_mut()
            .add_cert(X509::from_pem(support::authority().certificate_pem().as_bytes()).unwrap())
            .unwrap();
        let ssl = connector
            .build()
            .configure()
            .unwrap()
            .into_ssl("local.example")
            .unwrap();
        let mut stream =
            SslStream::new(ssl, TcpStream::connect(self.address).await.unwrap()).unwrap();
        timeout(Duration::from_secs(3), Pin::new(&mut stream).connect())
            .await
            .unwrap()
            .unwrap();
        stream
    }
    async fn request(&self, method: &str, path: &str, host: &str, body: &[u8]) -> (u16, Value) {
        let mut stream = self.connect().await;
        let header = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        response(&mut stream).await
    }
    async fn shutdown(self) {
        self.stop.send_replace(true);
        timeout(Duration::from_secs(3), self.front)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(3), self.signer)
            .await
            .unwrap()
            .unwrap();
    }
}
async fn response(stream: &mut SslStream<TcpStream>) -> (u16, Value) {
    timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        let header = loop {
            bytes.push(stream.read_u8().await.unwrap());
            assert!(bytes.len() <= 8192);
            if bytes.ends_with(b"\r\n\r\n") {
                break String::from_utf8(bytes).unwrap();
            }
        };
        let status = header.split_whitespace().nth(1).unwrap().parse().unwrap();
        let length: usize = header
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "0".into())
            .parse()
            .unwrap();
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn https_routes_advertise_default_and_explicit_ports_and_root_list() {
    let harness = Harness::start(Config::new("local.example".into()), None).await;
    let (status, body) = harness
        .request("GET", "/route", "other.example:443", b"")
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!({"resultCode":"0000","result":{"apiServer":"https://local.example:443","mqttServer":"ssl://local.example:8883"}})
    );
    let (_, body) = harness
        .request("GET", "/route/certificate", "local.example", b"")
        .await;
    assert_eq!(body["result"], json!(["common-server", "aws-iot"]));
    let (_, body) = harness
        .request(
            "GET",
            "/route/certificate?name=common-server",
            "local.example",
            b"",
        )
        .await;
    assert_eq!(
        body["result"]["certificatePem"],
        support::authority().certificate_pem()
    );
    assert_eq!(
        harness
            .request("POST", "/route", "local.example", b"")
            .await
            .0,
        405
    );
    assert_eq!(
        harness
            .request("GET", "/unknown", "local.example", b"")
            .await,
        (200, Value::Null)
    );
    harness.shutdown().await;
    let mut config = Config::new("local.example".into());
    config.advertise_requested_host = true;
    config.https_port = 9443;
    config.mqtts_port = 9883;
    let harness = Harness::start(config, None).await;
    let (_, body) = harness
        .request("GET", "/route", "redirect.example:443", b"")
        .await;
    assert_eq!(body["result"]["apiServer"], "https://redirect.example:9443");
    assert_eq!(body["result"]["mqttServer"], "ssl://redirect.example:9883");
    for host in ["127.0.0.1:443", "[::1]:443", "redirect.example:garbage"] {
        let (_, body) = harness.request("GET", "/route", host, b"").await;
        assert_eq!(body["result"]["apiServer"], "https://local.example:9443");
    }
    harness.shutdown().await;
}
#[tokio::test]
async fn real_csr_success_failure_dedupe_and_custom_root_keep_signing_authority() {
    let custom =
        rusthinq_server::certificates::Authority::generate("custom.example", 2048).unwrap();
    let harness = Harness::start(
        Config::new("local.example".into()),
        Some(custom.certificate_pem().as_bytes()),
    )
    .await;
    let (_, body) = harness
        .request(
            "GET",
            "/route/certificate?name=aws-iot",
            "local.example",
            b"",
        )
        .await;
    assert_eq!(body["result"]["certificatePem"], custom.certificate_pem());
    let (csr, key) = support::csr();
    let request =
        serde_json::to_vec(&json!({"csr":String::from_utf8(csr.clone()).unwrap()})).unwrap();
    let (status, body) = harness
        .request(
            "POST",
            "/device/dev-1/certificate",
            "local.example",
            &request,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["resultCode"], "0000");
    let pem = body["result"]["certificatePem"].as_str().unwrap();
    let leaf = X509::from_pem(pem.as_bytes()).unwrap();
    let root = X509::from_pem(support::authority().certificate_pem().as_bytes()).unwrap();
    assert!(leaf.verify(&root.public_key().unwrap()).unwrap());
    assert!(leaf.public_key().unwrap().public_eq(&key));
    assert!(leaf.subject_alt_names().is_none());
    assert_eq!(
        harness
            .request(
                "POST",
                "/device/dev-1/certificate",
                "local.example",
                &request
            )
            .await
            .1,
        body
    );
    assert_eq!(
        harness
            .request(
                "POST",
                "/device/dev-1/certificate",
                "local.example",
                br#"{"csr":"garbage"}"#
            )
            .await,
        (200, json!({"resultCode":"9999"}))
    );
    assert_eq!(
        harness
            .request("POST", "/device/dev-1/certificate", "local.example", b"{}")
            .await
            .0,
        422
    );
    assert_eq!(
        harness
            .request(
                "POST",
                "/device/dev-1/certificate",
                "local.example",
                b"bad json"
            )
            .await
            .0,
        400
    );
    for path in [
        "/device/certificate",
        "/device//certificate",
        "/device/dev%2Fother/certificate",
    ] {
        assert_eq!(
            harness
                .request("POST", path, "local.example", b"{}")
                .await
                .0,
            400
        );
    }
    harness.shutdown().await;
}
#[tokio::test]
async fn oversized_chunked_and_slow_bodies_are_bounded() {
    let mut config = Config::new("local.example".into());
    config.max_body = 256;
    config.request_timeout = Duration::from_millis(200);
    let harness = Harness::start(config, None).await;
    assert_eq!(
        harness
            .request(
                "POST",
                "/device/dev/certificate",
                "local.example",
                &vec![b'x'; 257]
            )
            .await
            .0,
        413
    );
    let mut chunked = harness.connect().await;
    chunked.write_all(b"POST /device/dev/certificate HTTP/1.1\r\nHost: local.example\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n101\r\n").await.unwrap();
    chunked.write_all(&vec![b'x'; 257]).await.unwrap();
    chunked.write_all(b"\r\n0\r\n\r\n").await.unwrap();
    assert_eq!(response(&mut chunked).await.0, 413);
    let mut slow = harness.connect().await;
    slow.write_all(b"POST /device/dev/certificate HTTP/1.1\r\nHost: local.example\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n").await.unwrap();
    assert_eq!(response(&mut slow).await.0, 408);
    harness.shutdown().await;
}
#[tokio::test]
async fn shutdown_cancels_pending_http_and_joins_signing_worker() {
    let harness = Harness::start(Config::new("local.example".into()), None).await;
    let mut peer = harness.connect().await;
    peer.write_all(b"POST /device/dev/certificate HTTP/1.1\r\nHost: local.example\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n").await.unwrap();
    let handle = harness.handle.clone();
    harness.shutdown().await;
    assert_eq!(
        handle.sign("device", &support::csr().0).await,
        Err(rusthinq_server::certificates::SignError::Stopped)
    );
}

#[tokio::test]
async fn shared_https_routes_both_protocols_and_metadata() {
    let mut harness = Harness::start(Config::new("local.example".into()), None).await;
    let mut peer = harness.connect().await;
    let body = b"<lgedmRoot><modelName>M</modelName></lgedmRoot>";
    let header = format!(
        "POST /lgehadm/api/Device/TotalDeviceInfoSvc HTTP/1.1\r\nHost: local.example\r\nx-lgedm-deviceid: d\r\nx-lgedm-devicetype: 1\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    peer.write_all(header.as_bytes()).await.unwrap();
    peer.write_all(body).await.unwrap();
    assert_eq!(response(&mut peer).await.0, 200);
    let observed = timeout(Duration::from_secs(2), harness.metadata.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.device_id, "d");
    assert_eq!(observed.model_name, "M");
    assert_eq!(
        harness
            .request("GET", "/route", "local.example", b"")
            .await
            .0,
        200
    );
    harness.shutdown().await;
}
