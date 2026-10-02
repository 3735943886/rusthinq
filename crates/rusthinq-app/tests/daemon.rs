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
            gui: cfg!(feature = "gui"),
            credentials: None,
            raw_inject: false,
        }),
        drivers: None,
        external_mqtt: None,
        cloud_account: cfg!(feature = "bridge").then(|| dir.path().join("account.json")),
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
    let page = String::from_utf8(page).unwrap();
    if cfg!(feature = "gui") {
        assert!(page.contains("management panel"));
    } else {
        assert!(page.starts_with("HTTP/1.1 404"));
    }
    let mut account = TcpStream::connect(management).await.unwrap();
    account
        .write_all(b"GET /api/cloud HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut status = Vec::new();
    timeout(Duration::from_secs(3), account.read_to_end(&mut status))
        .await
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8(status)
            .unwrap()
            .contains(&format!("\"enabled\":{}", cfg!(feature = "bridge")))
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
    let path = dir.path().join("config.toml");
    std::fs::write(&path, r#"{"password":"do-not-echo"}"#).unwrap();
    let error = Config::load(&path).unwrap_err().to_string();
    assert!(!error.contains("do-not-echo"));
    std::fs::write(
        &path,
        "hostname = \"one.example\"\nhostname = \"two.example\"\n",
    )
    .unwrap();
    assert!(Config::load(&path).is_err());
    let mut value = serde_json::json!({"thinq1_bind":"127.0.0.1:0","mqtt_bind":"127.0.0.1:0","https_bind":"127.0.0.1:0","hostname":"local.example","ca_certificate":"ca.pem","ca_key":"key.pem","device_ledger":"devices.json"});
    std::fs::write(&path, toml::to_string(&value).unwrap()).unwrap();
    assert_eq!(
        Config::load(&path).unwrap().ca_certificate,
        dir.path().join("ca.pem")
    );
    value["drivers"] = serde_json::json!({"directory":".","watch":true});
    std::fs::write(&path, toml::to_string(&value).unwrap()).unwrap();
    if cfg!(feature = "scripting") {
        assert!(Config::load(&path).unwrap().drivers.unwrap().watch);
    } else {
        assert!(Config::load(&path).is_err());
    }
    value["drivers"]["watch"] = serde_json::json!("true");
    std::fs::write(&path, toml::to_string(&value).unwrap()).unwrap();
    assert!(Config::load(&path).is_err());
    value.as_object_mut().unwrap().remove("drivers");
    value["management"] = serde_json::json!({"bind":"127.0.0.1:0"});
    std::fs::write(&path, toml::to_string(&value).unwrap()).unwrap();
    assert_eq!(
        Config::load(&path).unwrap().management.unwrap().gui,
        cfg!(feature = "gui")
    );
    value["management"]["gui"] = true.into();
    std::fs::write(&path, toml::to_string(&value).unwrap()).unwrap();
    assert_eq!(Config::load(&path).is_ok(), cfg!(feature = "gui"));
    value.as_object_mut().unwrap().remove("management");
    value["mqtt_bnid"] = "127.0.0.1:0".into();
    std::fs::write(&path, toml::to_string(&value).unwrap()).unwrap();
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
        cloud_account: None,
    };
    assert!(
        matches!(Daemon::prepare(config).await, Err(error) if error.kind() == std::io::ErrorKind::AddrInUse)
    );
    assert!(!ledger.exists());
}

#[cfg(feature = "scripting")]
#[tokio::test]
async fn daemon_keeps_mqtt_alive_until_terminal_publication_is_confirmed() {
    use rusthinq_app::scripts::Callbacks;
    use rusthinq_scripting::{Compiled, Limits, worker};
    let dir = tempfile::tempdir().unwrap();
    let ca = Authority::generate("terminal-tests.example", 2048).unwrap();
    let certificate = dir.path().join("ca.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&certificate, ca.certificate_pem()).unwrap();
    std::fs::write(&key, ca.private_key_pem().unwrap()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let inventory = dir.path().join("retained.json");
    let broker_port = listener.local_addr().unwrap().port();
    let (initial, initial_seen) = tokio::sync::oneshot::channel();
    let (terminal, terminal_seen) = tokio::sync::oneshot::channel();
    let (acknowledge, acknowledged) = tokio::sync::oneshot::channel();
    let saved = inventory.clone();
    let remote = tokio::spawn(async move {
        async fn packet(peer: &mut TcpStream) -> (u8, Vec<u8>) {
            let header = peer.read_u8().await.unwrap();
            let mut len = 0;
            let mut shift = 0;
            loop {
                let byte = peer.read_u8().await.unwrap();
                len |= usize::from(byte & 127) << shift;
                if byte & 128 == 0 {
                    break;
                }
                shift += 7;
                assert!(shift <= 21);
            }
            let mut body = vec![0; len];
            peer.read_exact(&mut body).await.unwrap();
            (header, body)
        }
        let mut publisher = None;
        let mut subscriber = None;
        for _ in 0..2 {
            let (mut peer, _) = listener.accept().await.unwrap();
            let (header, connect) = packet(&mut peer).await;
            assert_eq!(header, 0x10);
            peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            if &connect[12..] == b"terminal" {
                publisher = Some(peer);
            } else {
                assert_eq!(packet(&mut peer).await.0, 0x82);
                peer.write_all(&[0x90, 3, 0, 1, 1]).await.unwrap();
                subscriber = Some(peer);
            }
        }
        let mut peer = publisher.unwrap();
        let mut initial = Some(initial);
        let mut terminal = Some(terminal);
        let mut acknowledged = Some(acknowledged);
        for expected in ["true", "false"] {
            let (header, body) = packet(&mut peer).await;
            assert_eq!(header, 0x33);
            let topic = usize::from(u16::from_be_bytes([body[0], body[1]]));
            assert_eq!(&body[2..2 + topic], b"test/d/available");
            assert_eq!(&body[4 + topic..], expected.as_bytes());
            let state: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&saved).unwrap()).unwrap();
            assert_eq!(state["pending"][0]["topic"], "test/d/available");
            if expected == "false" {
                terminal.take().unwrap().send(()).unwrap();
                acknowledged.take().unwrap().await.unwrap();
            }
            peer.write_all(&[0x40, 2, body[2 + topic], body[3 + topic]])
                .await
                .unwrap();
            if expected == "true" {
                initial.take().unwrap().send(()).unwrap();
            }
        }
        assert!(peer.read_u8().await.is_err());
        assert!(subscriber.unwrap().read_u8().await.is_err());
    });
    let daemon = Daemon::prepare(Config {
        thinq1_bind: "127.0.0.1:0".parse().unwrap(),
        mqtt_bind: "127.0.0.1:0".parse().unwrap(),
        https_bind: "127.0.0.1:0".parse().unwrap(),
        hostname: "local.example".into(),
        ca_certificate: certificate,
        ca_key: key,
        device_ledger: dir.path().join("devices.json"),
        legacy_tls: false,
        management: None,
        drivers: Some(rusthinq_app::drivers::Config {
            directory: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/drivers"),
            topic_prefix: "test".into(),
            bindings: Default::default(),
            watch: false,
        }),
        cloud_account: None,
        external_mqtt: Some(rusthinq_app::external_mqtt::Config {
            host: "127.0.0.1".into(),
            port: broker_port,
            tls: false,
            ca: None,
            client: "terminal".into(),
            username: None,
            password: None,
            inventory: inventory.clone(),
        }),
    })
    .await
    .unwrap();
    let endpoint = daemon.endpoints().0;
    let handle = daemon.handle();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(daemon.serve(stopped));
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
    let mut peer = SslStream::new(ssl, TcpStream::connect(endpoint).await.unwrap()).unwrap();
    Pin::new(&mut peer).connect().await.unwrap();
    peer.write_all(
        &rusthinq_protocol::thinq1::encode(
            br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#,
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let len = peer.read_u32().await.unwrap();
    let mut frame = vec![0; len as usize];
    peer.read_exact(&mut frame).await.unwrap();
    timeout(Duration::from_secs(3), async {
        while handle.snapshot().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let session = handle.snapshot()[0].session.unwrap();
    let source = r#"fn live(v){publish(`{"topic":"test/d/available","payload":"true","retain":true}`);} fn bye(v){publish(`{"topic":"test/d/available","payload":"false","retain":true}`);}"#;
    handle
        .attach_script(
            "d".into(),
            session,
            Compiled::new(source, Limits::default(), true).unwrap(),
            worker::Config::default(),
            Callbacks {
                shutdown: Some("bye".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle
        .invoke_script("d".into(), session, 1, "live".into(), String::new())
        .await
        .unwrap();
    timeout(Duration::from_secs(5), initial_seen)
        .await
        .unwrap()
        .unwrap();
    stop.send_replace(true);
    timeout(Duration::from_secs(5), terminal_seen)
        .await
        .unwrap()
        .unwrap();
    assert!(!task.is_finished());
    acknowledge.send(()).unwrap();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    remote.await.unwrap();
    let ledger = rusthinq_app::retained_cleanup::Ledger::open(&inventory, 4).unwrap();
    assert_eq!(ledger.pending().len(), 1);
    assert!(ledger.requested().is_empty());
}
