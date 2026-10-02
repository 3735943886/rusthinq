use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use futures_util::StreamExt;
use rusthinq_app::{
    lifecycle_storage::Storage,
    management::{self, Config, Credentials},
    runtime::{Event, Runtime},
};
use rusthinq_lifecycle::Action;
use rusthinq_protocol::thinq1;
use rusthinq_server::Server;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, watch},
    time::timeout,
};
use tower::ServiceExt;

fn config(gui: bool) -> Config {
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        gui,
        credentials: None,
        raw_inject: false,
    }
}
fn request(path: &str, method: &str, body: Value) -> Request<Body> {
    Request::builder()
        .uri(path)
        .method(method)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}
async fn until(events: &mut broadcast::Receiver<Event>, predicate: impl Fn(&Event) -> bool) {
    timeout(Duration::from_secs(3), async {
        loop {
            if predicate(&events.recv().await.unwrap()) {
                break;
            }
        }
    })
    .await
    .unwrap();
}
async fn identify(server: &mut Server) -> tokio::io::DuplexStream {
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    let value = json!({"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}});
    peer.write_all(&thinq1::encode(value.to_string().as_bytes(), 8192).unwrap())
        .await
        .unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut bytes = vec![0; length as usize];
    peer.read_exact(&mut bytes).await.unwrap();
    peer
}

#[tokio::test]
async fn authentication_origin_and_api_only_routes_do_not_require_mqtt() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(&dir.path().join("devices.json"), 8).unwrap();
    let server = Server::new(Default::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let (_, stop) = watch::channel(false);
    let mut options = config(false);
    options.credentials = Some(Credentials {
        user: "admin".into(),
        password: "secret".into(),
    });
    assert!(!format!("{options:?}").contains("secret"));
    let app = management::router(runtime.handle(), options, stop).unwrap();
    let response = app
        .clone()
        .oneshot(request("/api/devices", "GET", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let auth = |mut req: Request<Body>| {
        req.headers_mut()
            .insert("authorization", "Basic YWRtaW46c2VjcmV0".parse().unwrap());
        req
    };
    let response = app
        .clone()
        .oneshot(auth(request("/api/devices", "GET", json!({}))))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(value["devices"], json!({}));
    assert_eq!(
        app.clone()
            .oneshot(auth(request("/", "GET", json!({}))))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let mut cross = auth(request("/forget/d", "POST", json!({"incarnation":"1"})));
    cross
        .headers_mut()
        .insert("host", "localhost:8080".parse().unwrap());
    cross
        .headers_mut()
        .insert("origin", "https://foreign.example".parse().unwrap());
    assert_eq!(
        app.oneshot(cross).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let mut public = config(true);
    public.bind = "0.0.0.0:8080".parse().unwrap();
    assert!(public.validate().is_err());
}

#[tokio::test]
async fn sends_and_forgets_are_scoped_and_removal_is_admission_then_durable_completion() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(&dir.path().join("devices.json"), 8).unwrap();
    let mut server = Server::new(Default::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let app = management::router(handle.clone(), config(true), stopped.clone()).unwrap();
    let task = tokio::spawn(runtime.run(stopped));
    let mut peer = identify(&mut server).await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    let session = handle.snapshot()[0].session.unwrap();
    let body = json!({"incarnation":session.incarnation.to_string(),"generation":session.generation.to_string(),"payload":r#"{"Body":{"Cmd":"custom"}}"#});
    let response = app
        .clone()
        .oneshot(request("/api/devices/d/send", "POST", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let length = peer.read_u32().await.unwrap();
    let mut bytes = vec![0; length as usize];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, br#"{"Body":{"Cmd":"custom"}}"#);
    let response = app
        .clone()
        .oneshot(request(
            "/forget/d",
            "POST",
            json!({"incarnation":session.incarnation+1}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = app
        .clone()
        .oneshot(request(
            "/forget/d",
            "POST",
            json!({"incarnation":session.incarnation.to_string()}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    until(&mut events, |event| {
        matches!(event, Event::Lifecycle(Action::Removed { .. }))
    })
    .await;
    let _successor = identify(&mut server).await;
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::LedgerResult { result: Ok(()), .. })
        )
    })
    .await;
    let response = app
        .oneshot(request(
            "/forget/d",
            "POST",
            json!({"incarnation":session.incarnation}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(handle.snapshot()[0].entry.incarnation > session.incarnation);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert!(matches!(
        handle.forget_scoped("d".into(), session.incarnation).await,
        Err(rusthinq_server::Reject::Stopped)
    ));
}

#[tokio::test]
async fn sockets_get_initial_snapshot_and_shutdown_joins_idle_http_and_upgrades() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(&dir.path().join("devices.json"), 8).unwrap();
    let server = Server::new(Default::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(management::serve(
        listener,
        runtime.handle(),
        config(true),
        stopped,
    ));
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
        .await
        .unwrap();
    let initial = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let snapshot: Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
    assert_eq!(snapshot["devices"], json!({}));
    let mut idle = tokio::net::TcpStream::connect(address).await.unwrap();
    idle.write_all(b"GET /api/health HTTP/1.1\r\n")
        .await
        .unwrap();
    stop.send_replace(true);
    timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(timeout(Duration::from_secs(1), socket.next()).await.is_ok());
}

#[tokio::test]
async fn cloud_account_api_reports_status_and_durable_logout_without_broker() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("account.json");
    let (cloud, account) = rusthinq_app::cloud_account::open(path.clone())
        .await
        .unwrap();
    let server = Server::new(Default::default()).unwrap();
    let runtime = Runtime::new(
        Storage::open(&dir.path().join("devices.json"), 4).unwrap(),
        server.handle(),
        Duration::ZERO,
        32,
    )
    .unwrap();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(account.run(stopped.clone()));
    let app = management::router_with_cloud(runtime.handle(), config(false), stopped, Some(cloud))
        .unwrap();
    let response = app
        .clone()
        .oneshot(request("/api/cloud", "GET", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let status: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(status["enabled"], true);
    assert_eq!(status["account"]["stored"], false);
    assert!(status["account"].get("refresh").is_none());
    let response = app
        .clone()
        .oneshot(request(
            "/api/cloud/login",
            "POST",
            json!({"country":"invalid"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = app
        .clone()
        .oneshot(request(
            "/api/cloud/login/complete",
            "POST",
            json!({"url":"https://evil.example/?code=secret"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = app
        .clone()
        .oneshot(request("/api/cloud/logout", "POST", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved["credentials"], Value::Null);
    let mut cross = request("/api/cloud/logout", "POST", json!({}));
    cross
        .headers_mut()
        .insert("origin", "https://evil.example".parse().unwrap());
    cross
        .headers_mut()
        .insert("host", "localhost".parse().unwrap());
    assert_eq!(
        app.oneshot(cross).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}
