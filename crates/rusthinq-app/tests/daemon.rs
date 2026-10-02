use openssl::{
    ssl::{SslConnector, SslMethod},
    x509::X509,
};
use rusthinq_app::daemon::{Config, Daemon};
use rusthinq_server::certificates::Authority;
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_openssl::SslStream;

#[tokio::test]
async fn composed_runtime_serves_shared_https_and_joins_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Arc::new(Authority::generate("daemon-tests.example", 2048).unwrap());
    // Load the same CA that supplies the configured listeners.
    let cert_path = dir.path().join("ca.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, ca.certificate_pem()).unwrap();
    std::fs::write(&key_path, ca.private_key_pem().unwrap()).unwrap();
    let config = Config {
        thinq1_bind: "127.0.0.1:0".parse().unwrap(),
        mqtt_bind: "127.0.0.1:0".parse().unwrap(),
        https_bind: "127.0.0.1:0".parse().unwrap(),
        hostname: "local.example".into(),
        ca_certificate: cert_path,
        ca_key: key_path,
        device_ledger: dir.path().join("devices.json"),
        legacy_tls: false,
        management: Some(rusthinq_app::management::Config {
            bind: "127.0.0.1:0".parse().unwrap(),
            gui: true,
            credentials: None,
            raw_inject: false,
        }),
        drivers: None,
        external_mqtt: None,
    };
    let daemon = Daemon::prepare(config).await.unwrap();
    let (_, mqtt, http) = daemon.endpoints();
    let management = daemon.management_endpoint().unwrap();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(daemon.serve(stopped));
    let mut dashboard = TcpStream::connect(management).await.unwrap();
    dashboard
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut page = Vec::new();
    timeout(Duration::from_secs(3), dashboard.read_to_end(&mut page))
        .await
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8(page)
            .unwrap()
            .contains("management panel")
    );
    let mut connector = SslConnector::builder(SslMethod::tls_client()).unwrap();
    connector
        .cert_store_mut()
        .add_cert(X509::from_pem(ca.certificate_pem().as_bytes()).unwrap())
        .unwrap();
    let ssl = connector
        .build()
        .configure()
        .unwrap()
        .into_ssl("local.example")
        .unwrap();
    let mut peer = SslStream::new(ssl, TcpStream::connect(http).await.unwrap()).unwrap();
    timeout(Duration::from_secs(3), Pin::new(&mut peer).connect())
        .await
        .unwrap()
        .unwrap();
    peer.write_all(b"GET /route HTTP/1.1\r\nHost: local.example\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), async {
        while !bytes.ends_with(b"\r\n\r\n") {
            bytes.push(peer.read_u8().await.unwrap());
        }
        let headers = String::from_utf8_lossy(&bytes);
        let length: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .unwrap()
            .parse()
            .unwrap();
        let mut body = vec![0; length];
        peer.read_exact(&mut body).await.unwrap();
        let route: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            route["result"]["apiServer"],
            format!("https://local.example:{}", http.port())
        );
        assert_eq!(
            route["result"]["mqttServer"],
            format!("ssl://local.example:{}", mqtt.port())
        );
    })
    .await
    .unwrap();
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn configuration_resolves_paths_and_rejects_typographical_fields() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut value = serde_json::json!({"thinq1_bind":"127.0.0.1:0","mqtt_bind":"127.0.0.1:0","https_bind":"127.0.0.1:0","hostname":"local.example","ca_certificate":"ca.pem","ca_key":"key.pem","device_ledger":"devices.json"});
    std::fs::write(&path, value.to_string()).unwrap();
    assert_eq!(
        Config::load(&path).unwrap().ca_certificate,
        dir.path().join("ca.pem")
    );
    value["mqtt_bnid"] = "127.0.0.1:0".into();
    std::fs::write(&path, value.to_string()).unwrap();
    assert!(Config::load(&path).is_err());
}

#[tokio::test]
async fn bind_failure_does_not_create_lifecycle_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ledger = dir.path().join("devices.json");
    let config = Config {
        thinq1_bind: "127.0.0.1:0".parse().unwrap(),
        mqtt_bind: occupied.local_addr().unwrap(),
        https_bind: "127.0.0.1:0".parse().unwrap(),
        hostname: "local.example".into(),
        ca_certificate: dir.path().join("not-loaded.pem"),
        ca_key: dir.path().join("not-loaded-key.pem"),
        device_ledger: ledger.clone(),
        legacy_tls: false,
        management: None,
        drivers: None,
        external_mqtt: None,
    };
    assert!(
        matches!(Daemon::prepare(config).await, Err(error) if error.kind() == std::io::ErrorKind::AddrInUse)
    );
    assert!(!ledger.exists());
}
