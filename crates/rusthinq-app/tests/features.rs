use axum::{
    body::Body,
    http::{Request, StatusCode},
};
#[cfg(any(
    not(feature = "bridge"),
    not(feature = "scripting"),
    not(feature = "gui")
))]
use rusthinq_app::daemon::Daemon;
use rusthinq_app::{daemon::Config, lifecycle_storage::Storage, management, runtime::Runtime};
use rusthinq_server::Server;
use std::time::Duration;
use tokio::sync::watch;
use tower::ServiceExt;

fn configuration(directory: &std::path::Path, extra: &str) -> std::path::PathBuf {
    let path = directory.join("config.toml");
    std::fs::write(
        &path,
        format!(
            r#"
thinq1_bind = "127.0.0.1:0"
mqtt_bind = "127.0.0.1:0"
https_bind = "127.0.0.1:0"
hostname = "local.example"
ca_certificate = "missing-ca.pem"
ca_key = "missing-key.pem"
device_ledger = "devices.json"
{extra}
"#
        ),
    )
    .unwrap();
    path
}

#[test]
fn optional_configuration_is_rejected_or_resolved_by_the_actual_build() {
    let directory = tempfile::tempdir().unwrap();
    let path = configuration(directory.path(), "cloud_account = \"account.json\"");
    let result = Config::load(&path);
    if cfg!(feature = "bridge") {
        assert_eq!(
            result.unwrap().cloud_account.unwrap(),
            directory.path().join("account.json")
        );
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("bridge feature is disabled")
        );
    }
    let path = configuration(
        directory.path(),
        "[drivers]\ndirectory = \".\"\nwatch = true",
    );
    let result = Config::load(&path);
    if cfg!(feature = "scripting") {
        assert!(result.unwrap().drivers.unwrap().watch);
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("scripting feature is disabled")
        );
    }
    let path = configuration(
        directory.path(),
        "[management]\nbind = \"127.0.0.1:0\"\ngui = true",
    );
    assert_eq!(Config::load(&path).is_ok(), cfg!(feature = "gui"));
    assert!(!directory.path().join("devices.json").exists());
    assert!(!directory.path().join("account.json.lock").exists());
}

#[tokio::test]
async fn management_keeps_health_and_explicit_disabled_results_without_optional_services() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let server = Server::new(Default::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 16).unwrap();
    let (_stop, stopped) = watch::channel(false);
    let routes = management::router(
        runtime.handle(),
        management::Config {
            bind: "127.0.0.1:0".parse().unwrap(),
            gui: false,
            credentials: None,
            raw_inject: false,
        },
        stopped,
    )
    .unwrap();
    let health = routes
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let cloud = routes
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/cloud/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"country":"KR"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cloud.status(), StatusCode::CONFLICT);
    if !cfg!(feature = "scripting") {
        let result = routes.oneshot(Request::builder().method("POST").uri("/api/devices/d/invoke")
            .header("content-type", "application/json").body(Body::from(r#"{"incarnation":"1","generation":"1","script_generation":"1","function":"__command","input":"x"}"#)).unwrap()).await.unwrap();
        assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[cfg(any(
    not(feature = "bridge"),
    not(feature = "scripting"),
    not(feature = "gui")
))]
#[tokio::test]
async fn embedding_rejects_disabled_capabilities_before_loading_ca_or_writing_checkpoints() {
    let directory = tempfile::tempdir().unwrap();
    let path = configuration(directory.path(), "");
    let base = Config::load(&path).unwrap();
    #[cfg(not(feature = "bridge"))]
    {
        let mut config = base.clone();
        config.cloud_account = Some(directory.path().join("account.json"));
        let error = match Daemon::prepare(config).await {
            Err(error) => error,
            Ok(_) => panic!("disabled bridge accepted"),
        };
        assert!(error.to_string().contains("bridge feature is disabled"));
    }
    #[cfg(not(feature = "scripting"))]
    {
        let mut config = base.clone();
        config.drivers = Some(rusthinq_app::drivers::Config {
            directory: directory.path().into(),
            watch: false,
            topic_prefix: "rusthinq".into(),
            bindings: Default::default(),
        });
        let error = match Daemon::prepare(config).await {
            Err(error) => error,
            Ok(_) => panic!("disabled scripting accepted"),
        };
        assert!(error.to_string().contains("scripting feature is disabled"));
    }
    #[cfg(not(feature = "gui"))]
    {
        let mut config = base;
        config.management = Some(management::Config {
            bind: "127.0.0.1:0".parse().unwrap(),
            gui: true,
            credentials: None,
            raw_inject: false,
        });
        let error = match Daemon::prepare(config).await {
            Err(error) => error,
            Ok(_) => panic!("disabled GUI accepted"),
        };
        assert!(error.to_string().contains("GUI feature is disabled"));
    }
    assert!(!directory.path().join("devices.json.lock").exists());
    assert!(!directory.path().join("account.json.lock").exists());
}
