mod support;
#[test]
fn generated_server_identity_passes_strict_chain_validation() {
    let ca = rusthinq_server::certificates::Authority::generate("strict-ca.example", 2048).unwrap();
    let identity = ca
        .server_identity(
            "local.example",
            rusthinq_protocol::lg_compat::TlsPolicy::Baseline,
        )
        .unwrap();
    let certificates =
        openssl::x509::X509::stack_from_pem(&identity.certificate_chain_pem).unwrap();
    let mut store = openssl::x509::store::X509StoreBuilder::new().unwrap();
    store.add_cert(certificates[1].clone()).unwrap();
    store
        .set_flags(openssl::x509::verify::X509VerifyFlags::X509_STRICT)
        .unwrap();
    let mut context = openssl::x509::X509StoreContext::new().unwrap();
    let chain = openssl::stack::Stack::new().unwrap();
    assert!(
        context
            .init(&store.build(), &certificates[0], &chain, |context| context
                .verify_cert())
            .unwrap(),
        "{}",
        context.error()
    );
}
use openssl::{
    stack::Stack,
    x509::{X509, X509StoreContext, store::X509StoreBuilder},
};
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    certificates::{Authority, Config, SignError, Signer},
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};
use tokio::{sync::watch, time::timeout};

#[test]
fn ca_loading_and_server_leaf_validate_material_and_chain() {
    let ca = support::authority();
    let key = ca.private_key_pem().unwrap();
    let loaded = Authority::from_pem(ca.certificate_pem().as_bytes(), &key).unwrap();
    assert_eq!(loaded.certificate_pem(), ca.certificate_pem());
    let identity = loaded
        .server_identity("root.example", TlsPolicy::Baseline)
        .unwrap();
    let chain = X509::stack_from_pem(&identity.certificate_chain_pem).unwrap();
    assert_eq!(chain.len(), 2);
    assert!(chain[0].verify(&chain[1].public_key().unwrap()).unwrap());
    assert_ne!(
        chain[0].subject_name().to_der().unwrap(),
        chain[1].subject_name().to_der().unwrap()
    );
    let mut store = X509StoreBuilder::new().unwrap();
    store.add_cert(chain[1].clone()).unwrap();
    let mut context = X509StoreContext::new().unwrap();
    assert!(
        context
            .init(
                &store.build(),
                &chain[0],
                &Stack::new().unwrap(),
                |context| context.verify_cert()
            )
            .unwrap()
    );
    assert!(Authority::from_pem(&chain[0].to_pem().unwrap(), &identity.private_key_pem).is_err());
    FrontDoor::new(TlsConfig::default(), vec![identity], None).unwrap();
    let (_, wrong_key) = support::csr();
    assert!(
        Authority::from_pem(
            ca.certificate_pem().as_bytes(),
            &wrong_key.private_key_to_pem_pkcs8().unwrap()
        )
        .is_err()
    );
    assert!(Authority::from_pem(b"broken", &key).is_err());
    assert!(Authority::generate("bad name", 2048).is_err());
    assert!(Authority::generate("root.example", 1024).is_err());
    assert!(
        loaded
            .server_identity("*.example", TlsPolicy::Baseline)
            .is_err()
    );
}
#[tokio::test]
async fn device_signing_drops_requested_extensions_and_dedupes_with_bounded_eviction() {
    let ca = support::authority();
    let (owner, handle) = Signer::new(
        ca.clone(),
        Config {
            cache_capacity: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(owner.run(stopped));
    let (csr, key) = support::csr();
    let first = handle.sign("device-a", &csr).await.unwrap();
    let leaf = X509::from_pem(first.as_bytes()).unwrap();
    let root = X509::from_pem(ca.certificate_pem().as_bytes()).unwrap();
    assert!(leaf.verify(&root.public_key().unwrap()).unwrap());
    assert!(leaf.public_key().unwrap().public_eq(&key));
    assert!(leaf.subject_alt_names().is_none());
    let text = String::from_utf8(leaf.to_text().unwrap()).unwrap();
    assert!(!text.contains("CA:TRUE"));
    assert_ne!(first, ca.certificate_pem());
    assert_eq!(first, handle.sign("device-a", &csr).await.unwrap());
    let other = handle.sign("device-b", &csr).await.unwrap();
    assert_ne!(other, first);
    assert_ne!(handle.sign("device-a", &csr).await.unwrap(), first);
    assert_eq!(
        handle.sign("device-a", b"bogus CSR").await,
        Err(SignError::InvalidCsr)
    );
    let parsed = openssl::x509::X509Req::from_pem(&csr).unwrap();
    let mut tampered = parsed.to_der().unwrap();
    *tampered.last_mut().unwrap() ^= 1;
    let tampered = openssl::x509::X509Req::from_der(&tampered)
        .unwrap()
        .to_pem()
        .unwrap();
    assert_eq!(
        handle.sign("tampered", &tampered).await,
        Err(SignError::InvalidCsr)
    );
    stop.send_replace(true);
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(handle.sign("device-a", &csr).await, Err(SignError::Stopped));
}
#[tokio::test]
async fn queue_is_bounded_and_shutdown_rejects_queued_requests() {
    let (owner, handle) = Signer::new(
        support::authority(),
        Config {
            queue_capacity: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let (csr, _) = support::csr();
    let mut queued = Box::pin(handle.sign("queued", &csr));
    poll_fn(|cx| {
        assert!(queued.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(handle.sign("excess", &csr).await, Err(SignError::Busy));
    assert_eq!(
        handle.sign("invalid/id", &csr).await,
        Err(SignError::InvalidDevice)
    );
    assert_eq!(
        handle.sign("large", &vec![0; 16385]).await,
        Err(SignError::TooLarge)
    );
    let (_stop, stopped) = watch::channel(true);
    timeout(Duration::from_secs(2), owner.run(stopped))
        .await
        .unwrap();
    assert_eq!(queued.await, Err(SignError::Stopped));
}
#[tokio::test]
async fn cache_expiry_reissues_without_reusing_a_previous_serial() {
    let (owner, handle) = Signer::new(
        support::authority(),
        Config {
            cache_ttl: Duration::from_millis(20),
            ..Config::default()
        },
    )
    .unwrap();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(owner.run(stopped));
    let (csr, _) = support::csr();
    let first = handle.sign("device", &csr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_ne!(first, handle.sign("device", &csr).await.unwrap());
    stop.send_replace(true);
    task.await.unwrap();
}

#[test]
fn ca_loading_rejects_expired_material_and_invalid_self_signature() {
    use openssl::{
        asn1::Asn1Time, hash::MessageDigest, pkey::PKey, x509::extension::BasicConstraints,
    };
    let ca = support::authority();
    let root = X509::from_pem(ca.certificate_pem().as_bytes()).unwrap();
    let key_pem = ca.private_key_pem().unwrap();
    let key = PKey::private_key_from_pem(&key_pem).unwrap();
    let (_, wrong_key) = support::csr();
    for expired in [true, false] {
        let mut cert = X509::builder().unwrap();
        cert.set_version(2).unwrap();
        cert.set_serial_number(root.serial_number()).unwrap();
        cert.set_subject_name(root.subject_name()).unwrap();
        cert.set_issuer_name(root.subject_name()).unwrap();
        cert.set_pubkey(&key).unwrap();
        cert.set_not_before(&Asn1Time::from_unix(1).unwrap())
            .unwrap();
        let after = if expired {
            Asn1Time::from_unix(2).unwrap()
        } else {
            Asn1Time::days_from_now(1).unwrap()
        };
        cert.set_not_after(&after).unwrap();
        cert.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
            .unwrap();
        cert.sign(
            if expired { &key } else { &wrong_key },
            MessageDigest::sha256(),
        )
        .unwrap();
        assert!(Authority::from_pem(&cert.build().to_pem().unwrap(), &key_pem).is_err());
    }
}
