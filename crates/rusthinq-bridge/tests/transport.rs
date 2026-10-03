use openssl::{
    asn1::Asn1Time,
    bn::BigNum,
    ec::{EcGroup, EcKey},
    hash::MessageDigest,
    nid::Nid,
    pkey::{PKey, Private},
    ssl::{SslAcceptor, SslMethod, SslVerifyMode},
    x509::{
        X509, X509NameBuilder,
        extension::{BasicConstraints, ExtendedKeyUsage},
        store::X509StoreBuilder,
    },
};
use rusthinq_bridge::{
    pairing::Material,
    transport::{Config, Connector, Protocol},
};
use rusthinq_server::certificates::Authority;
use std::{io, pin::Pin, sync::OnceLock, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tokio_openssl::SslStream;

struct Fixture {
    server_root: String,
    server_cert: X509,
    server_key: PKey<Private>,
    client_root: X509,
    client_issuer: PKey<Private>,
    client_cert: String,
    client_key: String,
}
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let server_ca = Authority::generate("server-ca.example", 2048).unwrap();
        let identity = server_ca
            .server_identity("localhost", Default::default())
            .unwrap();
        let client_ca = Authority::generate("client-ca.example", 2048).unwrap();
        let client_root = X509::from_pem(client_ca.certificate_pem().as_bytes()).unwrap();
        let issuer = PKey::private_key_from_pem(&client_ca.private_key_pem().unwrap()).unwrap();
        let key = PKey::from_ec_key(
            EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap(),
        )
        .unwrap();
        let mut subject = X509NameBuilder::new().unwrap();
        subject.append_entry_by_text("CN", "device").unwrap();
        let mut certificate = X509::builder().unwrap();
        certificate.set_version(2).unwrap();
        certificate
            .set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap())
            .unwrap();
        certificate.set_subject_name(&subject.build()).unwrap();
        certificate
            .set_issuer_name(client_root.subject_name())
            .unwrap();
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
        certificate
            .append_extension(ExtendedKeyUsage::new().client_auth().build().unwrap())
            .unwrap();
        certificate.sign(&issuer, MessageDigest::sha256()).unwrap();
        Fixture {
            server_root: server_ca.certificate_pem().into(),
            server_cert: X509::from_pem(&identity.certificate_chain_pem).unwrap(),
            server_key: PKey::private_key_from_pem(&identity.private_key_pem).unwrap(),
            client_root,
            client_issuer: issuer,
            client_cert: String::from_utf8(certificate.build().to_pem().unwrap()).unwrap(),
            client_key: String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap(),
        }
    })
}
fn material(port: u16, thinq2: bool) -> Material {
    let f = fixture();
    if thinq2 {
        Material::ThinQ2 {
            country: "KR".into(),
            api_server: "https://api.example".into(),
            mqtt_server: format!("ssl://localhost:{port}"),
            ca_certificate: f.server_root.clone(),
            private_key: f.client_key.clone(),
            certificate: f.client_cert.clone(),
            pub_topic: "device/up".into(),
            prov_topic: "device/provision".into(),
            sub_topic: "device/down/#".into(),
        }
    } else {
        Material::ThinQ1 {
            http_server: "https://api.example".into(),
            rti_server: format!("localhost:{port}"),
        }
    }
}
fn config(thinq2: bool) -> Config {
    Config {
        timeout: Duration::from_secs(3),
        thinq1_ca: (!thinq2).then(|| fixture().server_root.clone()),
    }
}
fn acceptor(mutual: bool) -> SslAcceptor {
    let f = fixture();
    let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls_server()).unwrap();
    acceptor.set_certificate(&f.server_cert).unwrap();
    acceptor.set_private_key(&f.server_key).unwrap();
    if mutual {
        let mut trust = X509StoreBuilder::new().unwrap();
        trust.add_cert(f.client_root.clone()).unwrap();
        acceptor.set_cert_store(trust.build());
        acceptor.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    }
    acceptor.build()
}
async fn round_trip(thinq2: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = acceptor(thinq2);
    let peer = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let ssl = openssl::ssl::Ssl::new(acceptor.context()).unwrap();
        let mut stream = SslStream::new(ssl, tcp).unwrap();
        timeout(Duration::from_secs(3), Pin::new(&mut stream).accept())
            .await
            .unwrap()
            .unwrap();
        if thinq2 {
            assert_eq!(
                stream.ssl().peer_certificate().unwrap().to_der().unwrap(),
                X509::from_pem(fixture().client_cert.as_bytes())
                    .unwrap()
                    .to_der()
                    .unwrap()
            );
        } else {
            assert!(stream.ssl().peer_certificate().is_none());
        }
        let mut bytes = [0; 5];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"hello");
        stream.write_all(b"world").await.unwrap();
    });
    let connector = Connector::new(&material(port, thinq2), config(thinq2)).unwrap();
    assert_eq!(
        connector.protocol(),
        if thinq2 {
            Protocol::ThinQ2
        } else {
            Protocol::ThinQ1
        }
    );
    let mut stream = connector.connect().await.unwrap();
    stream.write_all(b"hello").await.unwrap();
    let mut bytes = [0; 5];
    timeout(Duration::from_secs(3), stream.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, b"world");
    peer.await.unwrap();
}
#[tokio::test]
async fn thinq1_validates_server_and_carries_owned_bytes() {
    round_trip(false).await;
}
#[tokio::test]
async fn thinq2_uses_distinct_server_root_and_client_issuer() {
    round_trip(true).await;
}

#[tokio::test]
async fn bad_trust_and_wrong_hostname_never_produce_a_cloud_connection() {
    for (thinq2, wrong_hostname) in [(false, false), (false, true), (true, false), (true, true)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = acceptor(thinq2);
        let peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut stream =
                SslStream::new(openssl::ssl::Ssl::new(acceptor.context()).unwrap(), tcp).unwrap();
            let _ = timeout(Duration::from_secs(3), Pin::new(&mut stream).accept())
                .await
                .unwrap();
        });
        let mut material = material(port, thinq2);
        let mut config = config(thinq2);
        let other_root = String::from_utf8(fixture().client_root.to_pem().unwrap()).unwrap();
        match &mut material {
            Material::ThinQ1 { rti_server, .. } => {
                if wrong_hostname {
                    *rti_server = format!("ssl://127.0.0.1:{port}");
                } else {
                    config.thinq1_ca = Some(other_root);
                }
            }
            Material::ThinQ2 {
                mqtt_server,
                ca_certificate,
                ..
            } => {
                if wrong_hostname {
                    *mqtt_server = format!("ssl://127.0.0.1:{port}");
                } else {
                    *ca_certificate = other_root;
                }
            }
        }
        let connector = Connector::new(&material, config).unwrap();
        let error = connector.connect().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn cancellation_and_timeout_close_stalled_handshakes_without_retries() {
    for cancelled in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (started, ready) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            tcp.read_u8().await.unwrap();
            started.send(()).unwrap();
            let mut bytes = Vec::new();
            let closed = timeout(
                Duration::from_secs(3),
                tcp.take(65536).read_to_end(&mut bytes),
            )
            .await
            .unwrap();
            assert!(closed.is_ok() || closed.unwrap_err().kind() == io::ErrorKind::ConnectionReset);
        });
        let mut config = config(false);
        config.timeout = Duration::from_millis(250);
        let connector = Connector::new(&material(port, false), config).unwrap();
        let task = tokio::spawn(async move { connector.connect().await });
        ready.await.unwrap();
        if cancelled {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(
                task.await.unwrap().unwrap_err().kind(),
                io::ErrorKind::TimedOut
            );
        }
        peer.await.unwrap();
    }
}
#[test]
fn invalid_identity_endpoint_budget_and_trust_are_rejected_before_network() {
    let f = fixture();
    let material = material(8883, true);
    let connector = Connector::new(&material, config(true)).unwrap();
    assert!(!format!("{connector:?}").contains(&f.client_key));
    assert!(!format!("{material:?}").contains(&f.client_key));
    for timeout in [Duration::ZERO, Duration::from_secs(61)] {
        assert!(
            Connector::new(
                &material,
                Config {
                    timeout,
                    thinq1_ca: None
                }
            )
            .is_err()
        );
    }
    assert!(Connector::new(&material, config(false)).is_err());
    let mut mismatch = material.clone();
    let Material::ThinQ2 { private_key, .. } = &mut mismatch else {
        unreachable!()
    };
    *private_key = String::from_utf8(f.server_key.private_key_to_pem_pkcs8().unwrap()).unwrap();
    assert!(Connector::new(&mismatch, config(true)).is_err());
    let no_port = Material::ThinQ1 {
        http_server: "https://api.example".into(),
        rti_server: "ssl://localhost".into(),
    };
    assert!(Connector::new(&no_port, config(false)).is_err());
    let leaf = X509::from_pem(f.client_cert.as_bytes()).unwrap();
    let mut expired = X509::builder().unwrap();
    expired.set_version(2).unwrap();
    expired.set_serial_number(leaf.serial_number()).unwrap();
    expired.set_subject_name(leaf.subject_name()).unwrap();
    expired.set_issuer_name(leaf.issuer_name()).unwrap();
    expired.set_pubkey(&leaf.public_key().unwrap()).unwrap();
    expired
        .set_not_before(&Asn1Time::from_unix(0).unwrap())
        .unwrap();
    expired
        .set_not_after(&Asn1Time::from_unix(1).unwrap())
        .unwrap();
    expired
        .sign(&f.client_issuer, MessageDigest::sha256())
        .unwrap();
    let mut expired_material = material.clone();
    let Material::ThinQ2 { certificate, .. } = &mut expired_material else {
        unreachable!()
    };
    *certificate = String::from_utf8(expired.build().to_pem().unwrap()).unwrap();
    expired_material.validate_stored().unwrap();
    assert!(Connector::new(&expired_material, config(true)).is_err());
    let valid = Material::ThinQ1 {
        http_server: "https://api.example".into(),
        rti_server: "ssl://localhost:5222".into(),
    };
    for ca in ["broken PEM".to_string(), "x".repeat(65537)] {
        assert!(
            Connector::new(
                &valid,
                Config {
                    timeout: Duration::from_secs(3),
                    thinq1_ca: Some(ca)
                }
            )
            .is_err()
        );
    }
    let ipv6 = Material::ThinQ1 {
        http_server: "https://api.example".into(),
        rti_server: "ssl://[::1]:5222".into(),
    };
    assert!(Connector::new(&ipv6, config(false)).is_ok());
}

#[test]
fn legacy_pairing_aliases_preserve_keys_and_expired_material_stays_cleanup_only() {
    let original = material(8883, true);
    let mut archive = serde_json::to_value(&original).unwrap();
    let object = archive.as_object_mut().unwrap();
    object.remove("platform");
    let country = object.remove("country").unwrap();
    object.insert("countryCode".into(), country);
    object.insert(
        "deployAppInfo".into(),
        serde_json::json!({"protocolVer":"7"}),
    );
    let mut no_country = archive.clone();
    no_country.as_object_mut().unwrap().remove("countryCode");
    assert!(Material::from_legacy(no_country.clone()).is_err());
    assert!(Material::from_legacy_in_country(no_country, "KR").is_ok());
    assert!(Material::from_legacy_in_country(archive.clone(), "US").is_err());
    let restored = Material::from_legacy(archive.clone()).unwrap();
    restored.validate().unwrap();
    assert_eq!(
        serde_json::to_value(restored).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    let mut ambiguous = archive.clone();
    ambiguous["country_code"] = serde_json::json!("US");
    assert!(Material::from_legacy(ambiguous).is_err());
    let current = X509::from_pem(fixture().client_cert.as_bytes()).unwrap();
    let mut expired = X509::builder().unwrap();
    expired.set_version(2).unwrap();
    expired
        .set_serial_number(&BigNum::from_u32(9).unwrap().to_asn1_integer().unwrap())
        .unwrap();
    expired.set_subject_name(current.subject_name()).unwrap();
    expired
        .set_issuer_name(fixture().client_root.subject_name())
        .unwrap();
    expired.set_pubkey(&current.public_key().unwrap()).unwrap();
    expired
        .set_not_before(&Asn1Time::from_unix(1).unwrap())
        .unwrap();
    expired
        .set_not_after(&Asn1Time::from_unix(2).unwrap())
        .unwrap();
    expired
        .sign(&fixture().client_issuer, MessageDigest::sha256())
        .unwrap();
    archive["certificate"] =
        serde_json::json!(String::from_utf8(expired.build().to_pem().unwrap()).unwrap());
    let restored = Material::from_legacy(archive).unwrap();
    restored.validate_stored().unwrap();
    assert!(restored.validate().is_err());
    assert!(Connector::new(&restored, config(true)).is_err());
    let t1 = Material::from_legacy(
        serde_json::json!({"httpServer":"https://api.example","rtiServer":"cloud.example:5222","platform":"thinq1"}),
    )
    .unwrap();
    t1.validate().unwrap();
    assert!(Material::from_legacy(serde_json::json!({"httpServer":"https://api.example","rtiServer":"cloud.example:5222","platform":"thinq2"})).is_err());
    // Unused keys are ignored, as 0.1 read these files (real archives carry `httpsServer`).
    assert!(Material::from_legacy(serde_json::json!({"httpServer":"https://api.example","rtiServer":"cloud.example:5222","privateKey":"secret"})).is_ok());
}
