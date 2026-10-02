#![cfg(feature = "bridge")]
//! Composed bridge path: a local ThinQ2 appliance through the daemon's TLS front door, the
//! cloud owner and a fake LG service (HTTP account API and TLS MQTT). No real LG endpoint.
use openssl::{
    asn1::Asn1Time,
    bn::BigNum,
    ec::{EcGroup, EcKey},
    hash::MessageDigest,
    nid::Nid,
    pkey::PKey,
    ssl::{Ssl, SslAcceptor, SslConnector, SslMethod},
    x509::{X509, X509NameBuilder, extension::BasicConstraints},
};
use rusthinq_app::{
    lifecycle_storage::Storage,
    pairing_storage::{Owner, Store},
    runtime::Event,
    tls_runtime::Service,
};
use rusthinq_bridge::{cloud::Client, pairing::Material, passthrough};
use rusthinq_lifecycle::{Entry, Ledger};
use rusthinq_protocol::{lg_compat::TlsPolicy, mqtt};
use rusthinq_server::{
    Config,
    certificates::Authority,
    mqtt::SystemClock,
    tls::{Config as TlsConfig, FrontDoor},
};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, watch},
    time::timeout,
};
use tokio_openssl::SslStream;

fn authority() -> Arc<Authority> {
    static CA: OnceLock<Arc<Authority>> = OnceLock::new();
    CA.get_or_init(|| Arc::new(Authority::generate("bridge-test-ca", 2048).unwrap()))
        .clone()
}
fn acceptor(name: &str) -> SslAcceptor {
    let identity = authority()
        .server_identity(name, TlsPolicy::Baseline)
        .unwrap();
    let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls_server()).unwrap();
    acceptor
        .set_certificate(&X509::from_pem(&identity.certificate_chain_pem).unwrap())
        .unwrap();
    acceptor
        .set_private_key(&PKey::private_key_from_pem(&identity.private_key_pem).unwrap())
        .unwrap();
    acceptor.build()
}
async fn accept_tls(listener: &TcpListener, acceptor: &SslAcceptor) -> SslStream<TcpStream> {
    let (tcp, _) = listener.accept().await.unwrap();
    let mut stream = SslStream::new(Ssl::new(acceptor.context()).unwrap(), tcp).unwrap();
    Pin::new(&mut stream).accept().await.unwrap();
    stream
}
fn client_identity() -> (String, String) {
    let key = PKey::from_ec_key(
        EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap(),
    )
    .unwrap();
    let mut name = X509NameBuilder::new().unwrap();
    name.append_entry_by_text("CN", "device").unwrap();
    let name = name.build();
    let mut certificate = X509::builder().unwrap();
    certificate.set_version(2).unwrap();
    certificate
        .set_serial_number(&BigNum::from_u32(7).unwrap().to_asn1_integer().unwrap())
        .unwrap();
    certificate.set_subject_name(&name).unwrap();
    certificate.set_issuer_name(&name).unwrap();
    certificate.set_pubkey(&key).unwrap();
    certificate
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    certificate
        .set_not_after(&Asn1Time::days_from_now(1).unwrap())
        .unwrap();
    certificate
        .append_extension(BasicConstraints::new().critical().build().unwrap())
        .unwrap();
    certificate.sign(&key, MessageDigest::sha256()).unwrap();
    (
        String::from_utf8(certificate.build().to_pem().unwrap()).unwrap(),
        String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap(),
    )
}

/// The account API calls `Client::authenticate` makes, answered like LG does.
async fn lg_http(listener: TcpListener) {
    let acceptor = acceptor("lg.test");
    loop {
        let mut stream = accept_tls(&listener, &acceptor).await;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.unwrap());
        }
        let head = String::from_utf8(request).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        let path = head.split(' ').nth(1).unwrap().split('?').next().unwrap();
        let response = match path {
            "/gateway" => json!({"resultCode":"0000","result":{
                "uris":{"empFrontBaseUri2":"https://lg.test/web","empOauthBaseUri":"https://lg.test/auth"},
                "thinq2Uri":"https://lg.test/api"}}),
            "/auth/oauth/1.0/oauth2/token" => json!({"access_token":"access","expires_in":3600}),
            "/auth/users/profile" => json!({"status":1,"account":{"userNo":"user-1"}}),
            "/api/service/users/client" => json!({"resultCode":"0000","result":{}}),
            "/api/service/homes" => {
                json!({"resultCode":"0000","result":{"item":[{"homeId":"h","currentHomeYn":"Y"}]}})
            }
            other => panic!("unexpected LG request {other}"),
        }
        .to_string();
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let _ = stream.shutdown().await;
    }
}

async fn packet<S: AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    timeout(Duration::from_secs(5), async {
        let mut bytes = vec![stream.read_u8().await.unwrap()];
        loop {
            if let Some(total) = mqtt::length(&bytes, 1_004_096).unwrap() {
                let offset = bytes.len();
                bytes.resize(total, 0);
                stream.read_exact(&mut bytes[offset..]).await.unwrap();
                return bytes;
            }
            bytes.push(stream.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap()
}
fn published(bytes: &[u8]) -> (String, Vec<u8>, Option<u16>) {
    let mqtt::Packet::Publish {
        topic, payload, id, ..
    } = mqtt::decode(bytes, 1_004_096).unwrap()
    else {
        panic!("expected PUBLISH, got {bytes:02x?}")
    };
    (topic.to_string(), payload.to_vec(), id)
}
async fn publish<S: AsyncWrite + Unpin>(stream: &mut S, topic: &str, payload: &[u8]) {
    stream
        .write_all(&mqtt::publish(topic, payload, 8192).unwrap())
        .await
        .unwrap();
}
/// A PINGREQ answered by the very next packet proves nothing else (an ACK) was queued.
async fn nothing_pending<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) {
    stream.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(packet(stream).await, [0xd0, 0]);
}

/// One fake LG MQTT session up to Ready (the client's provisioning handshake).
async fn cloud_ready(stream: &mut SslStream<TcpStream>) {
    assert_eq!(packet(stream).await[0], 0x10);
    stream.write_all(&[0x20, 2, 0, 0]).await.unwrap();
    let subscribe = packet(stream).await;
    assert_eq!(subscribe[0], 0x82);
    stream
        .write_all(&[0x90, 3, subscribe[2], subscribe[3], 1])
        .await
        .unwrap();
    let (topic, payload, id) = published(&packet(stream).await);
    assert_eq!(topic, "lg/provision");
    let pre: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(pre["cmd"], "preDeploy");
    let id = id.unwrap().to_be_bytes();
    stream.write_all(&[0x40, 2, id[0], id[1]]).await.unwrap();
    publish(
        stream,
        "lg/down",
        br#"{"did":"d","mid":8,"cmd":"completeProvisioning"}"#,
    )
    .await;
    assert_eq!(packet(stream).await[0] & 0xf0, 0x30);
}

async fn until(events: &mut broadcast::Receiver<Event>, predicate: impl Fn(&Event) -> bool) {
    timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await {
                Ok(event) if predicate(&event) => return,
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bridged_thinq2_device_relays_through_fake_lg_cloud_and_falls_back_to_local_acks() {
    let directory = tempfile::tempdir().unwrap();
    // The device is known as incarnation 1, and its cloud registration was adopted earlier.
    let mut storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    storage
        .save(&Ledger {
            revision: 1,
            next_incarnation: 2,
            entries: vec![Entry {
                id: "d".into(),
                incarnation: 1,
                last_generation: 0,
            }],
        })
        .unwrap();
    let block = storage.reserve_generations(64).unwrap();
    let cloud_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (certificate, private_key) = client_identity();
    let material = Material::ThinQ2 {
        country: "KR".into(),
        api_server: "https://lg.test/api".into(),
        mqtt_server: format!(
            "ssl://localhost:{}",
            cloud_listener.local_addr().unwrap().port()
        ),
        ca_certificate: authority().certificate_pem().into(),
        private_key,
        certificate,
        pub_topic: "lg/up".into(),
        prov_topic: "lg/provision".into(),
        sub_topic: "lg/down".into(),
    };
    let pairings = directory.path().join("pairings.json");
    Store::open(&pairings, 8)
        .unwrap()
        .adopt(
            Owner {
                device: "d".into(),
                incarnation: 1,
                account: "user-1".into(),
            },
            material,
        )
        .unwrap();
    std::fs::write(
        directory.path().join("account.json"),
        json!({"schema":1,"credentials":{"country":"KR","refresh":"refresh"}}).to_string(),
    )
    .unwrap();

    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address: SocketAddr = http_listener.local_addr().unwrap();
    let lg = tokio::spawn(lg_http(http_listener));
    let (account, account_runtime) =
        rusthinq_app::cloud_account::open(directory.path().join("account.json"))
            .await
            .unwrap();
    let account_runtime = account_runtime.with_client_factory(move |country| {
        Client::with_test_service(
            country,
            "https://lg.test/gateway",
            authority().certificate_pem(),
            &["lg.test"],
            http_address,
        )
    });

    let thin = FrontDoor::new(
        TlsConfig::default(),
        vec![
            authority()
                .server_identity("thin.example", TlsPolicy::Baseline)
                .unwrap(),
        ],
        None,
    )
    .unwrap();
    let local = FrontDoor::new(
        TlsConfig::default(),
        vec![
            authority()
                .server_identity("mqtt.example", TlsPolicy::Baseline)
                .unwrap(),
        ],
        None,
    )
    .unwrap();
    let thin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_address = local_listener.local_addr().unwrap();
    let relay =
        passthrough::Relay::new(Default::default(), Arc::new(passthrough::HttpsConnector)).unwrap();
    let service = Service::new(
        storage,
        Config {
            generation_floor: block.floor,
            generation_ceiling: block.ceiling,
            ..Config::default()
        },
        Arc::new(SystemClock),
        Duration::ZERO,
        256,
        None,
    )
    .unwrap()
    .with_firmware(relay.clone());
    service
        .handle()
        .attach_cloud_account(account.clone())
        .unwrap();
    let (service, cloud) = service
        .with_cloud_devices(pairings, account.clone(), relay)
        .await
        .unwrap();
    let handle = service.handle();
    let app = rusthinq_app::api::AppHandle::from(handle.clone());
    let connected = |app: &rusthinq_app::api::AppHandle| {
        app.cloud_devices()["devices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["device"] == "d" && s["connected"] == true)
    };
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let account_task = tokio::spawn(account_runtime.run(stopped.clone()));
    let cloud_task = tokio::spawn(cloud.run(stopped.clone()));
    let task = tokio::spawn(service.serve((thin, thin_listener), (local, local_listener), stopped));

    // The local appliance connects and provisions as on a real network.
    let mut connector = SslConnector::builder(SslMethod::tls_client()).unwrap();
    connector
        .cert_store_mut()
        .add_cert(X509::from_pem(authority().certificate_pem().as_bytes()).unwrap())
        .unwrap();
    let ssl = connector
        .build()
        .configure()
        .unwrap()
        .into_ssl("mqtt.example")
        .unwrap();
    let mut device = SslStream::new(ssl, TcpStream::connect(local_address).await.unwrap()).unwrap();
    Pin::new(&mut device).connect().await.unwrap();
    device
        .write_all(
            &mqtt::frame(
                0x10,
                &[0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'd'],
                8192,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(packet(&mut device).await, [0x20, 2, 0, 0]);
    let topic = b"lime/devices/d";
    let mut body = vec![0, 1, 0, topic.len() as u8];
    body.extend_from_slice(topic);
    body.push(0);
    device
        .write_all(&mqtt::frame(0x82, &body, 8192).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&mut device).await, [0x90, 3, 0, 1, 0]);
    publish(
        &mut device,
        "clip/provisioning/devices/d",
        json!({"did":"d","cmd":"deploy","kind":"MODEL","data":{"appInfo":{"protocolVer":"7"},"platformInfo":{"version":"real"}}})
            .to_string()
            .as_bytes(),
    )
    .await;
    published(&packet(&mut device).await);
    publish(
        &mut device,
        "clip/message/devices/d",
        br#"{"did":"d","cmd":"completeProvisioning_ack"}"#,
    )
    .await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(rusthinq_lifecycle::Action::Online { .. })
        )
    })
    .await;
    let frame = json!({"did":"d","cmd":"device_packet","data":rusthinq_protocol::thinq2::encode_hex(&rusthinq_protocol::aabb::wrap(&[0xf0,1,4]).unwrap())}).to_string();
    // Before bridging, the local ACK owner answers the appliance.
    publish(&mut device, "clip/message/devices/d", frame.as_bytes()).await;
    let (_, ack, _) = published(&packet(&mut device).await);
    assert_eq!(serde_json::from_slice::<Value>(&ack).unwrap()["cmd"], "ack");

    timeout(Duration::from_secs(5), async {
        while !account.status().logged_in {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    timeout(
        Duration::from_secs(5),
        app.cloud_device("d".into(), 1, "enable", json!({})),
    )
    .await
    .unwrap()
    .unwrap();

    let lg_acceptor = acceptor("localhost");
    let mut previous_end = None;
    for session in 0..3 {
        let Ok(mut lg_mqtt) = timeout(
            Duration::from_secs(5),
            accept_tls(&cloud_listener, &lg_acceptor),
        )
        .await
        else {
            panic!(
                "session {session}: no reconnect; cloud status {}",
                app.cloud_devices()
            );
        };
        if let Some(ended) = previous_end {
            // A session that reached Ready resets the backoff: every reconnect waits ~1 s,
            // not 1, 2, 4 s.
            let waited = Instant::now().duration_since(ended);
            assert!(
                waited < Duration::from_millis(1800),
                "session {session}: {waited:?}"
            );
        }
        cloud_ready(&mut lg_mqtt).await;
        until(
            &mut events,
            |event| matches!(event, Event::CloudChanged { device } if device == "d"),
        )
        .await;
        timeout(Duration::from_secs(5), async {
            while !connected(&app) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        // Uplink: the appliance's bytes reach LG unchanged, and no local ACK is sent.
        publish(&mut device, "clip/message/devices/d", frame.as_bytes()).await;
        let (topic, uplink, _) = published(&packet(&mut lg_mqtt).await);
        assert_eq!(topic, "lg/up");
        // As 0.1's sendClip (rethink Connection.sendClip): the packet is kept, mid/kind are
        // restamped by the cloud session.
        let uplink: Value = serde_json::from_slice(&uplink).unwrap();
        let sent: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(
            (&uplink["cmd"], &uplink["data"], &uplink["did"]),
            (&sent["cmd"], &sent["data"], &sent["did"])
        );
        assert_eq!(uplink["kind"], "MODEL");
        assert!(uplink["mid"].is_u64());
        nothing_pending(&mut device).await;

        // Downlink: an LG command reaches the appliance byte for byte.
        let command = br#"{"did":"d","mid":999,"cmd":"reqUniversalCtrl","data":{"messageId":"m"}}"#;
        publish(&mut lg_mqtt, "lg/down", command).await;
        let (topic, delivered, _) = published(&packet(&mut device).await);
        assert_eq!(topic, "lime/devices/d");
        assert_eq!(delivered.strip_suffix(&[0]).unwrap_or(&delivered), command);

        // LG drops the connection: ACK ownership returns to the local owner.
        drop(lg_mqtt);
        timeout(Duration::from_secs(5), async {
            while connected(&app) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        previous_end = Some(Instant::now());
        timeout(Duration::from_secs(5), async {
            loop {
                publish(&mut device, "clip/message/devices/d", frame.as_bytes()).await;
                device.write_all(&[0xc0, 0]).await.unwrap();
                let first = packet(&mut device).await;
                if first == [0xd0, 0] {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue; // bridge state not released yet
                }
                let (_, ack, _) = published(&first);
                assert_eq!(serde_json::from_slice::<Value>(&ack).unwrap()["cmd"], "ack");
                assert_eq!(packet(&mut device).await, [0xd0, 0]);
                break;
            }
        })
        .await
        .unwrap();
    }

    // Logout ends cloud access: no further LG connection is attempted.
    account.logout().await.unwrap();
    assert!(
        timeout(Duration::from_millis(2500), cloud_listener.accept())
            .await
            .is_err()
    );

    stop.send_replace(true);
    for task in [account_task, cloud_task] {
        timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    lg.abort();
}
