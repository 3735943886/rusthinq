use openssl::{
    ssl::{SslConnector, SslMethod},
    x509::X509,
};
use rusthinq_app::{lifecycle_storage::Storage, runtime::Event, tls_runtime::Service};
use rusthinq_protocol::{lg_compat::TlsPolicy, mqtt, thinq1};
use rusthinq_server::{
    Config,
    certificates::Authority,
    mqtt::SystemClock,
    tls::{Config as TlsConfig, FrontDoor},
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
    time::timeout,
};
use tokio_openssl::SslStream;

fn authority() -> Arc<Authority> {
    static CA: OnceLock<Arc<Authority>> = OnceLock::new();
    CA.get_or_init(|| Arc::new(Authority::generate("runtime-test-ca", 2048).unwrap()))
        .clone()
}
async fn connect(address: SocketAddr, name: &str) -> SslStream<TcpStream> {
    let mut connector = SslConnector::builder(SslMethod::tls_client()).unwrap();
    connector
        .cert_store_mut()
        .add_cert(X509::from_pem(authority().certificate_pem().as_bytes()).unwrap())
        .unwrap();
    let ssl = connector
        .build()
        .configure()
        .unwrap()
        .into_ssl(name)
        .unwrap();
    let mut stream = SslStream::new(ssl, TcpStream::connect(address).await.unwrap()).unwrap();
    timeout(Duration::from_secs(5), Pin::new(&mut stream).connect())
        .await
        .unwrap()
        .unwrap();
    stream
}
async fn until(events: &mut broadcast::Receiver<Event>, predicate: impl Fn(&Event) -> bool) {
    timeout(Duration::from_secs(5), async {
        loop {
            if predicate(&events.recv().await.unwrap()) {
                return;
            }
        }
    })
    .await
    .unwrap();
}
async fn packet(peer: &mut SslStream<TcpStream>) -> Vec<u8> {
    let mut bytes = vec![peer.read_u8().await.unwrap()];
    loop {
        if let Some(total) = mqtt::length(&bytes, 8192).unwrap() {
            let offset = bytes.len();
            bytes.resize(total, 0);
            peer.read_exact(&mut bytes[offset..]).await.unwrap();
            return bytes;
        }
        bytes.push(peer.read_u8().await.unwrap());
    }
}

#[tokio::test]
async fn tls_mixed_lifecycle_and_partial_handshake_shutdown_release_storage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let mut storage = Storage::open(&path, 8).unwrap();
    let block = storage.reserve_generations(64).unwrap();
    let ca = authority();
    let thin = FrontDoor::new(
        TlsConfig::default(),
        vec![
            ca.server_identity("thin.example", TlsPolicy::Baseline)
                .unwrap(),
        ],
        None,
    )
    .unwrap();
    let mqtt_front = FrontDoor::new(
        TlsConfig::default(),
        vec![
            ca.server_identity("mqtt.example", TlsPolicy::Baseline)
                .unwrap(),
        ],
        None,
    )
    .unwrap();
    let thin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let thin_address = thin_listener.local_addr().unwrap();
    let mqtt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mqtt_address = mqtt_listener.local_addr().unwrap();
    let service = Service::new(
        storage,
        Config {
            generation_floor: block.floor,
            generation_ceiling: block.ceiling,
            ..Config::default()
        },
        Arc::new(SystemClock),
        Duration::ZERO,
        128,
        None,
    )
    .unwrap();
    let handle = service.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task =
        tokio::spawn(service.serve((thin, thin_listener), (mqtt_front, mqtt_listener), stopped));
    let mut thin_peer = connect(thin_address, "thin.example").await;
    let payload = br#"{"Header":{"x-lgedm-deviceId":"thin"},"Body":{"Cmd":"Mon"}}"#;
    thin_peer
        .write_all(&thinq1::encode(payload, 8192).unwrap())
        .await
        .unwrap();
    let length = thin_peer.read_u32().await.unwrap();
    let mut ack = vec![0; length as usize];
    thin_peer.read_exact(&mut ack).await.unwrap();
    let mut mqtt_peer = connect(mqtt_address, "mqtt.example").await;
    mqtt_peer
        .write_all(
            &mqtt::frame(
                0x10,
                &[0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'x'],
                8192,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(packet(&mut mqtt_peer).await, [0x20, 2, 0, 0]);
    let topic = b"lime/devices/mqtt";
    let mut body = vec![0, 1, 0, topic.len() as u8];
    body.extend_from_slice(topic);
    body.push(0);
    mqtt_peer
        .write_all(&mqtt::frame(0x82, &body, 8192).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&mut mqtt_peer).await, [0x90, 3, 0, 1, 0]);
    for (topic, value) in [
        (
            "clip/provisioning/devices/mqtt",
            serde_json::json!({"did":"mqtt","cmd":"deploy","kind":"model","data":{}}),
        ),
        (
            "clip/message/devices/mqtt",
            serde_json::json!({"did":"mqtt","cmd":"completeProvisioning_ack"}),
        ),
    ] {
        mqtt_peer
            .write_all(&mqtt::publish(topic, value.to_string().as_bytes(), 8192).unwrap())
            .await
            .unwrap();
        if topic.contains("provisioning") {
            assert!(matches!(
                mqtt::decode(&packet(&mut mqtt_peer).await, 8192).unwrap(),
                mqtt::Packet::Publish { .. }
            ));
        }
    }
    until(&mut events,|event|matches!(event,Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.entry.id=="mqtt" && device.online)).await;
    assert_eq!(handle.snapshot().len(), 2);
    assert_ne!(
        handle.snapshot()[0].entry.last_generation,
        handle.snapshot()[1].entry.last_generation
    );
    let mut pending = TcpStream::connect(thin_address).await.unwrap();
    pending.write_all(&[22, 3]).await.unwrap();
    stop.send_replace(true);
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        handle
            .snapshot()
            .iter()
            .all(|device| device.session.is_none())
    );
    let storage = Storage::open(&path, 8).unwrap();
    assert_eq!(storage.state().ledger.entries.len(), 2);
    assert!(TcpStream::connect(thin_address).await.is_err());
    assert!(TcpStream::connect(mqtt_address).await.is_err());
}

#[tokio::test]
async fn already_stopped_tls_service_joins_tasks_and_releases_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    let ca = authority();
    let thin = FrontDoor::new(
        TlsConfig::default(),
        vec![
            ca.server_identity("thin.example", TlsPolicy::Baseline)
                .unwrap(),
        ],
        None,
    )
    .unwrap();
    let mqtt = FrontDoor::new(
        TlsConfig::default(),
        vec![
            ca.server_identity("mqtt.example", TlsPolicy::Baseline)
                .unwrap(),
        ],
        None,
    )
    .unwrap();
    let thin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mqtt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = Service::new(
        storage,
        Config::default(),
        Arc::new(SystemClock),
        Duration::ZERO,
        128,
        None,
    )
    .unwrap();
    let (_stop, stopped) = watch::channel(true);
    timeout(
        Duration::from_secs(5),
        service.serve((thin, thin_listener), (mqtt, mqtt_listener), stopped),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(Storage::open(&path, 8).is_ok());
}

#[tokio::test]
async fn supervised_https_metadata_and_provisioning_reach_application_and_advertise_mqtt_port() {
    use rusthinq_server::{certificates::Signer, provisioning, thinq1_http};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 8).unwrap();
    let ca = authority();
    let front = || {
        FrontDoor::new(
            TlsConfig::default(),
            vec![
                ca.server_identity("thin.example", TlsPolicy::Baseline)
                    .unwrap(),
            ],
            None,
        )
        .unwrap()
    };
    let thin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mqtt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mqtt_port = mqtt_listener.local_addr().unwrap().port();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let provisioning_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provisioning_address = provisioning_listener.local_addr().unwrap();
    let (http, metadata) =
        thinq1_http::Service::new(Default::default(), Arc::new(SystemClock)).unwrap();
    let (signer, signing) = Signer::new(ca.clone(), Default::default()).unwrap();
    let (signer_stop, signer_stopped) = watch::channel(false);
    let signing_task = tokio::spawn(signer.run(signer_stopped));
    let mut provisioning_config = provisioning::Config::new("thin.example".into());
    provisioning_config.https_port = provisioning_address.port();
    provisioning_config.mqtts_port = mqtt_port;
    let provisioning = provisioning::Service::new(provisioning_config, &ca, signing, None).unwrap();
    let service = Service::new(
        storage,
        Config::default(),
        Arc::new(SystemClock),
        Duration::ZERO,
        128,
        None,
    )
    .unwrap()
    .with_metadata(metadata)
    .with_local_service(front(), http_listener, Arc::new(http))
    .unwrap()
    .with_local_service(front(), provisioning_listener, Arc::new(provisioning))
    .unwrap();
    let handle = service.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task =
        tokio::spawn(service.serve((front(), thin_listener), (front(), mqtt_listener), stopped));
    let mut peer = connect(http_address, "thin.example").await;
    let body = "<lgedmRoot><modelName>Model&amp;1</modelName></lgedmRoot>";
    let request = format!(
        "POST /lgehadm/api/Device/TotalDeviceInfoSvc HTTP/1.1\r\nHost: thin.example\r\nx-lgedm-deviceid: d\r\nx-lgedm-devicetype: purifier\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    peer.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(5), peer.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200"));
    until(
        &mut events,
        |event| matches!(event,Event::Metadata(metadata) if metadata.device_id=="d"),
    )
    .await;
    assert_eq!(handle.metadata_snapshot()[0].model_name, "Model&1");
    assert_eq!(handle.metadata_snapshot()[0].device_type, "purifier");
    assert!(handle.snapshot().is_empty());
    let mut peer = connect(provisioning_address, "thin.example").await;
    peer.write_all(b"GET /route HTTP/1.1\r\nHost: thin.example\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(5), peer.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains(&format!("ssl://thin.example:{mqtt_port}")));
    stop.send_replace(true);
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    signer_stop.send_replace(true);
    signing_task.await.unwrap();
    assert!(Storage::open(&path, 8).is_ok());
    assert!(TcpStream::connect(http_address).await.is_err());
    assert!(TcpStream::connect(provisioning_address).await.is_err());
}
