use rusthinq_bridge::firmware::{Error, Evidence, Hosts};
use serde_json::json;

#[test]
fn nested_cloud_urls_are_confirmed_without_fixed_fields() {
    let mut hosts = Hosts::new(8).unwrap();
    hosts
        .learn_command(
            &json!({"cmd":"osp_command", "data":{"arbitrary":[
                "HTTPS://CDN.Example.:8443/fw.bin", {"sota":"http://second.example/app"},
                "ssl://mqtt.example", "ftp://ftp.example", "https://user:pass@bad.example/x",
                "https://127.0.0.1/x", "not a url"
            ]}}),
            0,
        )
        .unwrap();
    assert_eq!(hosts.snapshot(0).unwrap().len(), 2);
    assert!(hosts.route("CDN.EXAMPLE.", 1_000_000).unwrap());
    assert!(hosts.route("second.example", 1_000_000).unwrap());
    assert!(!hosts.route("mqtt.example", 1_000_000).unwrap());
}

#[test]
fn suspected_leases_expire_and_live_hits_renew_without_downgrade() {
    let mut hosts = Hosts::new(4).unwrap();
    hosts.suspect("https://cdn.example/fw", 0).unwrap();
    assert!(hosts.route("cdn.example", 59_999).unwrap());
    assert_eq!(
        hosts.snapshot(60_000).unwrap()["cdn.example"],
        Evidence::Suspected {
            expires_at: 659_999
        }
    );
    assert!(!hosts.route("cdn.example", 659_999).unwrap());
    hosts
        .learn_command(&json!("https://cdn.example/fw"), 660_000)
        .unwrap();
    hosts.suspect("https://cdn.example/fw", 660_001).unwrap();
    assert_eq!(
        hosts.snapshot(660_001).unwrap()["cdn.example"],
        Evidence::Command
    );
    assert!(hosts.route("cdn.example", 9_000_000).unwrap());
}

#[test]
fn local_proof_overrides_every_tier_and_protects_ssl_endpoints() {
    let mut hosts = Hosts::new(4).unwrap();
    hosts.suspect("https://api.example/", 0).unwrap();
    hosts.confirm_local("API.EXAMPLE.", 1).unwrap();
    hosts
        .protect_local_endpoints(
            &json!({"mqtt-server":"ssl://mqtt.example:8883", "api":"https://api.example"}),
            2,
        )
        .unwrap();
    hosts
        .learn_command(
            &json!(["https://api.example/fw", "https://mqtt.example/fw"]),
            3,
        )
        .unwrap();
    hosts.suspect("https://api.example/fw", 4).unwrap();
    assert!(!hosts.route("api.example", 5).unwrap());
    assert!(!hosts.route("mqtt.example", 5).unwrap());
    assert_eq!(hosts.snapshot(5).unwrap().len(), 2);
}

#[test]
fn capacity_and_payload_limits_are_atomic_and_local_proof_is_never_evicted() {
    let mut hosts = Hosts::new(2).unwrap();
    hosts.confirm_local("local.example", 0).unwrap();
    assert_eq!(
        hosts.learn_command(&json!(["https://a.example", "https://b.example"]), 1),
        Err(Error::Capacity)
    );
    assert_eq!(hosts.snapshot(1).unwrap().len(), 1);
    hosts.suspect("https://a.example", 2).unwrap();
    assert_eq!(hosts.suspect("https://b.example", 3), Err(Error::Capacity));
    hosts.suspect("https://b.example", 60_002).unwrap();
    assert_eq!(
        hosts.snapshot(60_002).unwrap()["local.example"],
        Evidence::Local
    );
    let mut deep = json!("https://c.example");
    for _ in 0..65 {
        deep = json!([deep]);
    }
    assert_eq!(
        hosts.learn_command(&deep, 60_003),
        Err(Error::PayloadExceeded)
    );
    let wide = json!(vec![0; 4097]);
    assert_eq!(
        hosts.learn_command(&wide, 60_003),
        Err(Error::PayloadExceeded)
    );
    assert_eq!(
        hosts.learn_command(&json!("x".repeat(4097)), 60_003),
        Err(Error::PayloadExceeded)
    );
    assert_eq!(hosts.snapshot(60_003).unwrap().len(), 2);
}

#[test]
fn invalid_clock_and_host_do_not_change_evidence_or_renew_leases() {
    assert!(matches!(Hosts::new(0), Err(Error::Capacity)));
    let mut hosts = Hosts::new(2).unwrap();
    hosts.suspect("https://cdn.example", 100).unwrap();
    assert_eq!(hosts.route("cdn.example", 99), Err(Error::Clock));
    assert_eq!(hosts.route("cdn.example", u64::MAX), Err(Error::Clock));
    assert_eq!(
        hosts.confirm_local("bad/host", 100),
        Err(Error::InvalidHost)
    );
    assert_eq!(hosts.route("127.0.0.1", 100), Err(Error::InvalidHost));
    assert_eq!(
        hosts.snapshot(100).unwrap()["cdn.example"],
        Evidence::Suspected { expires_at: 60_100 }
    );
    assert!(!Hosts::new(2).unwrap().route("cdn.example", 100).unwrap());
}
