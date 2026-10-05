//! Broker-independent management API and existing dashboard projection.
use crate::api::{AppHandle as Handle, CloudError, Delivery, Reject};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine;
use rusthinq_lifecycle::SessionKey;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, broadcast, watch},
    task::JoinSet,
    time::timeout,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: SocketAddr,
    pub gui: bool,
    pub credentials: Option<Credentials>,
    /// Allows raw injection to be switched on at runtime (`/api/raw-inject`).
    pub raw_inject_toggle: bool,
    /// Runtime injection state, shared with the MQTT `$raw` routes. Always starts off
    /// and is never persisted, so a restart returns to the safe state.
    pub raw_inject: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl Config {
    fn injection(&self) -> bool {
        self.raw_inject.load(std::sync::atomic::Ordering::Relaxed)
    }
}
#[derive(Clone)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credentials(<redacted>)")
    }
}
impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if self.gui && !cfg!(feature = "gui") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GUI feature is disabled",
            ));
        }
        if let Some(auth) = &self.credentials
            && (auth.user.is_empty()
                || auth.user.contains(':')
                || auth.password.is_empty()
                || auth.user.len() > 256
                || auth.password.len() > 1024)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid management credentials",
            ));
        }
        Ok(())
    }
}
/// LG account aliases by device id, read from the account inventory (0.1 showed them).
struct NameCache {
    names: std::sync::Mutex<std::collections::HashMap<String, String>>,
    wanted: tokio::sync::Notify,
    last_query: std::sync::Mutex<Option<tokio::time::Instant>>,
    forced: std::sync::atomic::AtomicBool,
    changed: watch::Sender<u64>,
}
impl Default for NameCache {
    fn default() -> Self {
        Self {
            names: Default::default(),
            wanted: Default::default(),
            last_query: Default::default(),
            forced: Default::default(),
            changed: watch::channel(0).0,
        }
    }
}
impl NameCache {
    fn begin_query(&self, force: bool) -> bool {
        let mut last = self.last_query.lock().unwrap_or_else(|e| e.into_inner());
        if !force && last.is_some_and(|at| at.elapsed() < Duration::from_secs(60)) {
            return false;
        }
        *last = Some(tokio::time::Instant::now());
        true
    }
    fn query_finished(&self) {
        *self.last_query.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(tokio::time::Instant::now());
    }
    fn force_refresh(&self) {
        self.forced
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.refresh();
    }
    fn replace(&self, names: std::collections::HashMap<String, String>) {
        let mut current = self.lock();
        if *current != names {
            *current = names;
            self.changed
                .send_modify(|version| *version = version.wrapping_add(1));
        }
    }
    fn inventory(&self, inventory: &Value) {
        self.replace(
            inventory
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|d| {
                    let id = d["deviceId"].as_str()?;
                    let alias = d["alias"].as_str().filter(|a| !a.trim().is_empty())?;
                    Some((id.to_owned(), alias.to_owned()))
                })
                .collect(),
        );
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, String>> {
        self.names.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Coalesced: requests while a read is running produce one more read.
    fn refresh(&self) {
        self.wanted.notify_one();
    }
}
type Names = Arc<NameCache>;
#[derive(Clone)]
struct App {
    handle: Handle,
    names: Names,
    config: Config,
    stop: watch::Receiver<bool>,
    sockets: Arc<Semaphore>,
    analysis: Arc<Semaphore>,
    observer: crate::cloud_observer::Observer,
}
pub fn router(
    handle: impl Into<Handle>,
    config: Config,
    stop: watch::Receiver<bool>,
) -> io::Result<Router> {
    let handle = handle.into();
    config.validate()?;
    let observer = crate::cloud_observer::Observer::new(
        cfg!(feature = "bridge") && handle.cloud_status()["enabled"] == true,
    );
    #[cfg(feature = "scripting")]
    let observer = observer.with_scripts(handle.clone());
    Ok(build(App {
        observer,
        handle,
        names: Names::default(),
        config,
        stop,
        sockets: Arc::new(Semaphore::new(64)),
        analysis: Arc::new(Semaphore::new(2)),
    }))
}
fn build(app: App) -> Router {
    #[allow(unused_mut)]
    let mut routes = Router::new()
        .route("/api/health", get(health))
        .route("/api/diagnostics", get(diagnostics))
        .route("/api/devices/{id}/presentation", get(presentation))
        .route(
            "/api/cloud/notifications",
            get(notifications).post(notification_control),
        )
        .route("/api/cloud/notifications/ws", get(notification_socket))
        .route("/api/packets/decode", post(decode_packet))
        .route("/api/tlv/catalog", get(tlv_catalog))
        .route("/api/cloud/devices", get(cloud_devices))
        .route("/api/cloud/inventory", get(cloud_inventory))
        .route("/api/devices/{id}/bridge/{action}", post(cloud_device))
        .route("/api/cloud", get(cloud_status))
        .route("/api/cloud/login", post(cloud_login))
        .route("/api/cloud/login/complete", post(cloud_complete))
        .route("/api/cloud/logout", post(cloud_logout))
        .route("/api/cloud/refresh", post(cloud_refresh))
        .route("/api/mqtt", get(mqtt_status))
        .route("/api/mqtt/retained/delete", post(delete_retained))
        .route("/api/devices", get(devices))
        .route("/api/devices/{id}/forget", post(forget))
        .route("/api/devices/{id}/send", post(send))
        .route("/api/devices/{id}/invoke", post(invoke))
        .route("/api/devices/{id}/reload", post(reload_driver))
        .route("/api/devices/{id}/inject", post(inject))
        .route(
            "/api/raw-inject",
            get(raw_inject_status).post(raw_inject_set),
        )
        .route("/api/events", get(event_socket))
        .route("/ws", get(panel))
        .route("/device", get(monitor))
        .route("/forget/{id}", post(forget));
    #[cfg(feature = "gui")]
    if app.config.gui {
        routes = routes
            .route(
                "/",
                get(|| async {
                    (
                        [("content-type", "text/html; charset=utf-8")],
                        include_str!("../assets/index.html"),
                    )
                }),
            )
            .route(
                "/cloud-feed.js",
                get(|| async {
                    (
                        [("content-type", "text/javascript; charset=utf-8")],
                        include_str!("../assets/cloud-feed.js"),
                    )
                }),
            )
            .route(
                "/controls.js",
                get(|| async {
                    (
                        [("content-type", "text/javascript; charset=utf-8")],
                        include_str!("../assets/controls.js"),
                    )
                }),
            )
            .route(
                "/ui.js",
                get(|| async {
                    (
                        [("content-type", "text/javascript; charset=utf-8")],
                        include_str!("../assets/ui.js"),
                    )
                }),
            )
            .route(
                "/panel.js",
                get(|| async {
                    (
                        [("content-type", "text/javascript; charset=utf-8")],
                        include_str!("../assets/panel.js"),
                    )
                }),
            )
            .route(
                "/monitor",
                get(|| async {
                    (
                        [("content-type", "text/html; charset=utf-8")],
                        include_str!("../assets/monitor.html"),
                    )
                }),
            )
            .route(
                "/monitor.js",
                get(|| async {
                    (
                        [("content-type", "text/javascript; charset=utf-8")],
                        include_str!("../assets/monitor.js"),
                    )
                }),
            )
            .route(
                "/dark.css",
                get(|| async {
                    (
                        [("content-type", "text/css; charset=utf-8")],
                        include_str!("../assets/dark.css"),
                    )
                }),
            )
            .route(
                "/logo.svg",
                get(|| async {
                    (
                        [("content-type", "image/svg+xml")],
                        include_str!("../assets/logo.svg"),
                    )
                }),
            )
            .route(
                "/favicon.png",
                get(|| async {
                    (
                        [("content-type", "image/png")],
                        include_bytes!("../assets/favicon.png").as_slice(),
                    )
                }),
            );
    }
    routes
        .layer(DefaultBodyLimit::max(1_000_000))
        .layer(middleware::from_fn_with_state(app.clone(), authorize))
        .with_state(app)
}
async fn authorize(State(app): State<App>, request: Request, next: Next) -> Response {
    if *app.stop.borrow() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "stopped");
    }
    if let Some(auth) = &app.config.credentials {
        let matches = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Basic "))
            .and_then(|v| base64::engine::general_purpose::STANDARD.decode(v).ok())
            .is_some_and(|bytes| {
                bytes
                    .ct_eq(format!("{}:{}", auth.user, auth.password).as_bytes())
                    .into()
            });
        if !matches {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Basic realm=\"rusthinq\"")],
                "authentication required",
            )
                .into_response();
        }
    }
    // Browser Basic credentials are ambient: deny cross-origin mutations and upgrades.
    if (request.method() != axum::http::Method::GET
        || request.headers().contains_key(header::UPGRADE))
        && let Some(origin) = request.headers().get(header::ORIGIN)
    {
        let origin = origin.to_str().ok().and_then(|origin| {
            origin
                .strip_prefix("http://")
                .or_else(|| origin.strip_prefix("https://"))
        });
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|v| v.to_str().ok());
        if origin.is_none() || origin != host {
            return error(StatusCode::FORBIDDEN, "cross-origin operation");
        }
    }
    next.run(request).await
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PacketDecode {
    hex: String,
    direction: Option<String>,
    model_id: Option<String>,
}
async fn decode_packet(State(app): State<App>, Json(input): Json<PacketDecode>) -> Response {
    let Ok(permit) = app.analysis.clone().try_acquire_owned() else {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "packet analysis capacity exceeded",
        );
    };
    if input.hex.len() > 131072
        || input
            .model_id
            .as_ref()
            .is_some_and(|v| v.len() > 256 || v.chars().any(char::is_control))
        || input
            .direction
            .as_deref()
            .is_some_and(|v| !matches!(v, "fromDevice" | "toDevice"))
    {
        return error(StatusCode::BAD_REQUEST, "invalid packet analysis input");
    }
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut decoded =
            rusthinq_protocol::decode::decode_hex_payload(&input.hex, input.direction.as_deref())?;
        decoded["exportText"] = json!(rusthinq_protocol::decode::re_export_text(
            &decoded,
            input.model_id.as_deref(),
            input.direction.as_deref(),
        ));
        Ok::<_, String>(decoded)
    });
    match task.await {
        Ok(Ok(decoded)) => Json(decoded).into_response(),
        Ok(Err(reason)) => error(StatusCode::BAD_REQUEST, &reason),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "packet analysis failed"),
    }
}
async fn tlv_catalog() -> Json<Value> {
    Json(rusthinq_protocol::decode::tlv_catalog_json())
}
async fn mqtt_status(State(app): State<App>) -> Json<Value> {
    let Some(handle) = app.handle.external_mqtt() else {
        return Json(json!({"status":"Disabled"}));
    };
    let status = format!("{:?}", *handle.status().borrow());
    Json(json!({"status":status,"droppedTransient":handle.dropped_transient()}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedDelete {
    scope: String,
    owner: Option<String>,
    topic: Option<String>,
}
async fn delete_retained(State(app): State<App>, Json(request): Json<RetainedDelete>) -> Response {
    let Some(adapter) = app.handle.external_mqtt() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "external MQTT disabled");
    };
    let operation = async {
        match (request.scope.as_str(), request.owner, request.topic) {
            ("topic", Some(owner), Some(topic)) => adapter.delete_topic(owner, topic).await,
            ("owner", Some(owner), None) => adapter.delete_owner(owner).await,
            ("all", None, None) => adapter.delete_all().await,
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scope requires topic+owner, owner, or all",
            )),
        }
    };
    match timeout(Duration::from_secs(30), operation).await {
        Ok(Ok(count)) => Json(json!({"confirmed":count})).into_response(),
        Ok(Err(reason)) => error(StatusCode::BAD_GATEWAY, &reason.to_string()),
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "cleanup outcome unknown; inspect adapter status and durable inventory",
        ),
    }
}
#[derive(Deserialize)]
struct PresentationScope {
    incarnation: u64,
    generation: u64,
    script_generation: u64,
}
async fn presentation(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(scope): Query<PresentationScope>,
) -> Response {
    let session = SessionKey {
        incarnation: scope.incarnation,
        generation: scope.generation,
    };
    if !app
        .handle
        .snapshot()
        .iter()
        .any(|d| d.entry.id == id && d.online && d.session == Some(session))
        || !app
            .handle
            .script_states()
            .get(&id)
            .is_some_and(|(current, generation, _)| {
                *current == session && *generation == scope.script_generation
            })
    {
        return error(StatusCode::CONFLICT, "device presentation scope changed");
    }
    Json(json!({"incarnation":scope.incarnation.to_string(),"generation":scope.generation.to_string(),"scriptGeneration":scope.script_generation.to_string(),"publications":app.handle.publications(&id,session,scope.script_generation)})).into_response()
}
async fn diagnostics(State(app): State<App>) -> Json<Value> {
    Json(
        json!({"version":env!("CARGO_PKG_VERSION"),"runtime":app.handle.diagnostics(),"devices":snapshot(&app.handle,&app.names)["devices"],"cloud":{"enabled":app.handle.cloud_status()["enabled"],"loggedIn":app.handle.cloud_status()["account"]["loggedIn"]},"notifications":app.observer.snapshot(0,0,None),"mqtt":app.handle.external_mqtt().map(|m|json!({"status":format!("{:?}",*m.status().borrow()),"droppedTransient":m.dropped_transient()})),"privacy":"No credentials, certificates or wire payloads are included."}),
    )
}
#[derive(Default, Deserialize)]
struct NotificationQuery {
    #[serde(default)]
    cursor: u64,
    limit: Option<usize>,
    device: Option<String>,
}
async fn notifications(
    State(app): State<App>,
    Query(query): Query<NotificationQuery>,
) -> Json<Value> {
    Json(app.observer.snapshot(
        query.cursor,
        query.limit.unwrap_or(100),
        query.device.as_deref(),
    ))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NotificationControl {
    enabled: Option<bool>,
    #[serde(default)]
    clear: bool,
}
async fn notification_control(
    State(app): State<App>,
    Json(input): Json<NotificationControl>,
) -> Response {
    if input.enabled == Some(true) && app.handle.cloud_status()["account"]["loggedIn"] != true {
        return error(
            StatusCode::CONFLICT,
            "Sign in to LG before enabling notifications",
        );
    }
    if let Some(enabled) = input.enabled
        && let Err(reason) = app.observer.set_enabled(enabled)
    {
        return error(StatusCode::SERVICE_UNAVAILABLE, reason);
    }
    if input.clear {
        app.observer.clear();
    }
    Json(app.observer.snapshot(0, 0, None)).into_response()
}
async fn notification_socket(
    State(app): State<App>,
    Query(query): Query<NotificationQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Ok(permit) = app.sockets.clone().try_acquire_owned() else {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "management socket capacity exceeded",
        );
    };
    let mut events = app.observer.subscribe();
    upgrade.max_message_size(4096).on_upgrade(move |mut socket| async move {
        let _permit = permit;
        let mut stop = app.stop.clone();
        if *stop.borrow() { return; }
        let initial = app.observer.snapshot(query.cursor, 0, None);
        let reset = initial["reset"] == true;
        let boundary = initial["cursor"].as_str().and_then(|v|v.parse::<u64>().ok()).unwrap_or(0);
        let mut cursor = if reset { 0 } else { query.cursor };
        let mut first = true;
        loop {
            let mut snapshot = app.observer.snapshot(cursor, 200, query.device.as_deref());
            let next = snapshot["nextCursor"].as_str().and_then(|v|v.parse::<u64>().ok()).unwrap_or(cursor);
            snapshot["cursor"] = json!(next.to_string());
            snapshot["reset"] = json!(reset && first);
            first = false;
            if !write(&mut socket, json!({"type":"cloudSnapshot","snapshot":snapshot})).await { return; }
            if next == cursor || next >= boundary { break; }
            cursor = next;
            if *stop.borrow() { return; }
        }
        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => break,
                message = socket.recv() => {
                    if matches!(message,None|Some(Err(_))|Some(Ok(Message::Close(_)))) { break; }
                },
                value = events.recv() => {
                    let value = match value {
                        Ok(value) => value,
                        Err(broadcast::error::RecvError::Lagged(events)) => json!({"type":"cloudLoss","events":events,"t":crate::observability::now_ms()}),
                        Err(_) => break,
                    };
                    if value["type"] == "cloudNotification" && query.device.as_deref().is_some_and(|id| !crate::cloud_observer::matches_device(&value, id)) {
                        continue;
                    }
                    if !write(&mut socket, value).await { break; }
                }
            }
        }
    }).into_response()
}
async fn health(State(app): State<App>) -> Json<Value> {
    Json(
        json!({"running":true,"version":env!("CARGO_PKG_VERSION"),"retainedCleanup":format!("{:?}", &*app.handle.cleanup_status().borrow()),"diagnostics":app.handle.diagnostics()}),
    )
}
/// Device state and account changes only request reads; one owner performs LG I/O.
fn name_refresh_scope(handle: &Handle) -> std::collections::BTreeMap<String, (u64, bool)> {
    handle
        .snapshot()
        .into_iter()
        .map(|device| (device.entry.id, (device.entry.incarnation, device.online)))
        .collect()
}
async fn refresh_names(handle: Handle, names: Names, mut stop: watch::Receiver<bool>) {
    let mut events = handle.adapter_events();
    #[cfg(feature = "bridge")]
    let mut account_changes = handle
        .account_handle()
        .map(|account| account.status_updates());
    #[cfg(feature = "bridge")]
    let account = handle.account_handle();
    #[cfg(feature = "bridge")]
    let mut inventory_changes = account.as_ref().map(|account| account.inventory_updates());
    #[cfg(feature = "bridge")]
    if let Some(inventory) = account
        .as_ref()
        .and_then(|account| account.inventory_snapshot())
    {
        names.inventory(&inventory);
        names.query_finished();
    }
    loop {
        if *stop.borrow() {
            return;
        }
        let scope = name_refresh_scope(&handle);
        let logged_in = handle.cloud_status()["account"]["loggedIn"] == true;
        let force = names
            .forced
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        if !logged_in {
            names.replace(Default::default());
            *names.last_query.lock().unwrap_or_else(|e| e.into_inner()) = None;
        } else if names.begin_query(force) {
            let inventory = tokio::select! {
                _ = stop.changed() => return,
                result = handle.cloud_inventory() => result,
            };
            names.query_finished();
            if handle.cloud_status()["account"]["loggedIn"] != true {
                names.replace(Default::default());
            } else if let Ok(inventory) = inventory {
                names.inventory(&inventory);
            }
        }
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                _ = names.wanted.notified() => break,
                _ = async {
                    #[cfg(feature = "bridge")]
                    if let Some(changes) = inventory_changes.as_mut()
                        && changes.changed().await.is_ok()
                    {
                        return;
                    }
                    std::future::pending::<()>().await;
                } => {
                    #[cfg(feature = "bridge")]
                    if let Some(inventory) = account.as_ref().and_then(|account| account.inventory_snapshot()) {
                        names.inventory(&inventory);
                        names.query_finished();
                    }
                },
                _ = async {
                    #[cfg(feature = "bridge")]
                    if let Some(changes) = account_changes.as_mut()
                        && changes.changed().await.is_ok()
                    {
                        return;
                    }
                    std::future::pending::<()>().await;
                } => break,
                event = events.recv() => {
                    match event {
                        Err(broadcast::error::RecvError::Closed) => return,
                        Ok(event) if event["type"] != "stateChanged" => continue,
                        _ => {}
                    }
                    if name_refresh_scope(&handle) != scope { break; }
                },
            }
        }
    }
}

fn snapshot(handle: &Handle, names: &Names) -> Value {
    let names = names.lock().clone();
    let metadata = handle.metadata_snapshot();
    let models = handle.driver_models();
    let persisted_models = handle.persisted_models();
    let states = handle.script_states();
    let cloud = handle.cloud_devices();
    let mut devices = serde_json::Map::new();
    for device in handle.snapshot() {
        let meta = metadata
            .iter()
            .find(|meta| meta.device_id == device.entry.id);
        let saved = persisted_models
            .get(&device.entry.id)
            .filter(|meta| meta.incarnation == device.entry.incarnation);
        let model = models.get(&device.entry.id);
        let script = states
            .get(&device.entry.id)
            .filter(|(session, _, _)| Some(*session) == device.session);
        let bridge = cloud["devices"].as_array().and_then(|ds| {
            ds.iter().find(|d| {
                d["device"] == device.entry.id
                    && d["incarnation"]
                        .as_str()
                        .and_then(|s| s.parse::<u64>().ok())
                        == Some(device.entry.incarnation)
            })
        });
        devices.insert(
            device.entry.id.clone(),
            json!({
                "online":device.online,
                "incarnation":device.entry.incarnation.to_string(),
                "generation":device.session.map(|s|s.generation.to_string()),
                "model":model.map(|(_,model,_)|model.as_str()).or_else(||meta.map(|m|m.model_name.as_str())).unwrap_or(""),
                "modelId":saved.map(|m|m.model_id.as_str()).filter(|v| !v.is_empty()).or_else(||model.map(|(_,model,_)|model.as_str())),
                "modelName":saved.map(|m|m.model_name.as_str()),
                "swVersion":saved.map(|m|m.sw_version.as_str()),
                "lastSeenUnix":saved.map(|m|m.last_seen_unix),
                "deviceType":meta.map(|m|m.device_type.as_str()).filter(|v| !v.is_empty()).or_else(||saved.map(|m|m.device_type.as_str())),
                "platform":model.map(|(_,_,t2)|if *t2 {"ThinQ2"} else {"ThinQ1"}).unwrap_or(if meta.is_some() {"ThinQ1"} else {""}),
                "modelPersisted":persisted_models.get(&device.entry.id).is_some_and(|persisted|persisted.incarnation==device.entry.incarnation && model.is_some_and(|(_,name,t2)|(if persisted.model_id.is_empty(){&persisted.model_name}else{&persisted.model_id})==name && persisted.thinq2==*t2)),
                "driverReloadable":handle.driver_reload_configured() && script.is_some() && model.is_some() && (!handle.driver_watch() || script.is_some_and(|(_,_,faulted)|*faulted)),"mapped":script.is_some(),"scriptGeneration":script.map(|(_,generation,_)|generation.to_string()),"scriptFaulted":script.is_some_and(|(_,_,faulted)|*faulted),"bridgePaired":bridge.is_some_and(|b|b["paired"]==true),"bridgeEnabled":bridge.is_some_and(|b|b["enabled"]==true),"bridged":bridge.is_some_and(|b|b["connected"]==true),"bridgePending":bridge.is_some_and(|b|b["paired"]==false),"bridgeError":bridge.map(|b|b["error"].clone()),
                "removal":device.removal.map(|r|format!("{r:?}")),
                "name":names.get(&device.entry.id)
            }),
        );
    }
    json!({"devices":devices,"version":env!("CARGO_PKG_VERSION"),"features":{"scripting":cfg!(feature="scripting"),"bridge":cloud["enabled"]},"mqtt":null,"guiMqtt":handle.external_mqtt().map(|mqtt|matches!(*mqtt.status().borrow(),crate::external_mqtt::Status::Connected)),"management":true})
}
async fn devices(State(app): State<App>) -> Json<Value> {
    Json(snapshot(&app.handle, &app.names))
}
fn number(value: &Value, key: &str) -> Result<u64, &'static str> {
    value[key]
        .as_str()
        .and_then(|n| n.parse().ok())
        .or_else(|| value[key].as_u64())
        .filter(|n| *n > 0)
        .ok_or("missing/invalid generation or incarnation")
}
async fn forget(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let incarnation = match number(&body, "incarnation") {
        Ok(v) => v,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    match timeout(
        Duration::from_secs(5),
        app.handle.adapter_forget(id, incarnation),
    )
    .await
    {
        Ok(Ok(())) => (
            StatusCode::ACCEPTED,
            Json(json!({"status":"accepted","incarnation":incarnation.to_string()})),
        )
            .into_response(),
        Ok(Err(reject)) => rejected(reject),
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "admission unknown; inspect device state before retrying",
        ),
    }
}
async fn send(State(app): State<App>, Path(id): Path<String>, Json(body): Json<Value>) -> Response {
    let incarnation = match number(&body, "incarnation") {
        Ok(v) => v,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let generation = match number(&body, "generation") {
        Ok(v) => v,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let Some(payload) = body["payload"].as_str() else {
        return error(StatusCode::BAD_REQUEST, "payload must be a JSON string");
    };
    match timeout(
        Duration::from_secs(30),
        app.handle.adapter_send(
            id,
            SessionKey {
                incarnation,
                generation,
            },
            payload.as_bytes().to_vec(),
        ),
    )
    .await
    {
        Ok(Ok(delivery)) => delivery_response(delivery),
        Ok(Err(reject)) => rejected(reject),
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "delivery unknown; do not automatically retry",
        ),
    }
}
fn delivery_response(delivery: Delivery) -> Response {
    let status = match delivery {
        Delivery::Sent => StatusCode::OK,
        Delivery::Failed => StatusCode::BAD_GATEWAY,
        Delivery::Unknown => StatusCode::GATEWAY_TIMEOUT,
    };
    (
        status,
        Json(json!({"delivery":format!("{delivery:?}"),"deviceAcknowledged":false})),
    )
        .into_response()
}
async fn invoke(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let incarnation = match number(&body, "incarnation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let generation = match number(&body, "generation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let script_generation = match number(&body, "script_generation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let (Some(function), Some(input)) = (body["function"].as_str(), body["input"].as_str()) else {
        return error(
            StatusCode::BAD_REQUEST,
            "function and input strings required",
        );
    };
    match timeout(
        Duration::from_secs(5),
        app.handle.adapter_invoke(
            id,
            SessionKey {
                incarnation,
                generation,
            },
            script_generation,
            function.into(),
            input.into(),
        ),
    )
    .await
    {
        Ok(Ok(sequence)) => (
            StatusCode::ACCEPTED,
            Json(json!({"status":"accepted","sequence":sequence.to_string(),"executed":false})),
        )
            .into_response(),
        Ok(Err(reject)) => error(
            match reject {
                Reject::StaleSession => StatusCode::CONFLICT,
                Reject::Busy => StatusCode::TOO_MANY_REQUESTS,
                Reject::Stopped | Reject::Disabled => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::BAD_REQUEST,
            },
            &format!("{reject:?}"),
        ),
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "admission unknown; do not automatically retry",
        ),
    }
}
async fn reload_driver(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let incarnation = match number(&body, "incarnation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let generation = match number(&body, "generation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let script_generation = match number(&body, "script_generation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    match timeout(Duration::from_secs(5), app.handle.adapter_reload_driver(
        id, SessionKey {incarnation,generation},script_generation,
    )).await {
        Ok(Ok(generation)) => (StatusCode::OK,Json(json!({"status":"reloaded","scriptGeneration":generation.to_string(),"initialized":false}))).into_response(),
        Ok(Err(reject)) => error(match reject {
            Reject::StaleSession => StatusCode::CONFLICT,
            Reject::Busy => StatusCode::TOO_MANY_REQUESTS,
            Reject::Disabled | Reject::Stopped => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::BAD_REQUEST,
        }, &format!("driver reload: {reject:?}")),
        Err(_) => error(StatusCode::GATEWAY_TIMEOUT,"reload outcome unknown; inspect scriptGeneration before retrying"),
    }
}
fn raw_inject_state(app: &App) -> Value {
    json!({"enabled":app.config.injection(),"toggle":app.config.raw_inject_toggle})
}
async fn raw_inject_status(State(app): State<App>) -> Response {
    Json(raw_inject_state(&app)).into_response()
}
async fn raw_inject_set(State(app): State<App>, Json(body): Json<Value>) -> Response {
    if !app.config.raw_inject_toggle {
        return error(StatusCode::FORBIDDEN, "raw injection toggle disabled");
    }
    let Some(enabled) = body["enabled"].as_bool() else {
        return error(StatusCode::BAD_REQUEST, "enabled must be boolean");
    };
    app.config
        .raw_inject
        .store(enabled, std::sync::atomic::Ordering::Relaxed);
    Json(raw_inject_state(&app)).into_response()
}
async fn inject(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if !app.config.injection() {
        return error(StatusCode::FORBIDDEN, "raw injection disabled");
    }
    let incarnation = match number(&body, "incarnation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let generation = match number(&body, "generation") {
        Ok(n) => n,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let (Some(hex), Some(direction)) = (body["hex"].as_str(), body["direction"].as_str()) else {
        return error(StatusCode::BAD_REQUEST, "hex and direction required");
    };
    let to_device = match direction {
        "toDevice" => true,
        "fromDevice" => false,
        _ => return error(StatusCode::BAD_REQUEST, "invalid direction"),
    };
    let data = match rusthinq_protocol::hex::decode(hex) {
        Ok(data) => data,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid hex"),
    };
    match timeout(
        Duration::from_secs(30),
        app.handle.adapter_inject(
            id,
            SessionKey {
                incarnation,
                generation,
            },
            data,
            to_device,
        ),
    )
    .await
    {
        Ok(Ok(Some(delivery))) => delivery_response(delivery),
        Ok(Ok(None)) => (
            StatusCode::ACCEPTED,
            Json(json!({"injected":true,"deviceAcknowledged":false})),
        )
            .into_response(),
        Ok(Err(reject)) => rejected(reject),
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "injection outcome unknown; do not automatically retry",
        ),
    }
}
async fn monitor_message(app: &App, id: &str, session: Option<SessionKey>, text: &str) -> Value {
    if !app.config.injection() {
        return json!({"error":"raw injection disabled"});
    }
    let Some(session) = session else {
        return json!({"error":"offline; reopen monitor after reconnect"});
    };
    let body: Value = match serde_json::from_str(text) {
        Ok(body) => body,
        Err(_) => return json!({"error":"invalid JSON"}),
    };
    let (hex, to_device) = match (
        body["sendToDevice"].as_str(),
        body["sendFromDevice"].as_str(),
    ) {
        (Some(hex), None) => (hex, true),
        (None, Some(hex)) => (hex, false),
        _ => return json!({"error":"one injection direction required"}),
    };
    let data = match rusthinq_protocol::hex::decode(hex) {
        Ok(data) => data,
        Err(_) => return json!({"error":"invalid hex"}),
    };
    match app
        .handle
        .adapter_inject(id.into(), session, data, to_device)
        .await
    {
        Ok(Some(delivery)) => {
            json!({"delivery":format!("{:?}",delivery),"deviceAcknowledged":false})
        }
        Ok(None) => json!({"injected":true}),
        Err(reject) => json!({"error":format!("{reject:?}")}),
    }
}
async fn event_socket(State(app): State<App>, upgrade: WebSocketUpgrade) -> Response {
    let Ok(permit) = app.sockets.clone().try_acquire_owned() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "socket capacity exceeded");
    };
    let mut events = app.handle.adapter_events();
    upgrade.max_message_size(1_000_000).on_upgrade(move |mut socket|async move {
        let _permit=permit;let mut stop=app.stop.clone();
        if *stop.borrow() || !write(&mut socket,json!({"type":"snapshot","state":snapshot(&app.handle, &app.names)})).await {return;}
        loop {tokio::select! {
            _=stop.changed()=>break,
            message=socket.recv()=>if matches!(message,None|Some(Err(_))|Some(Ok(Message::Close(_)))) {break;},
            event=events.recv()=> {
                let value=match event {Ok(event)=>event,Err(broadcast::error::RecvError::Lagged(count))=>json!({"type":"lost","events":count,"state":snapshot(&app.handle, &app.names)}),Err(broadcast::error::RecvError::Closed)=>break};
                if !write(&mut socket,value).await {break;}
            }
        }}
    }).into_response()
}
fn rejected(reject: Reject) -> Response {
    let status = match reject {
        Reject::StaleSession => StatusCode::CONFLICT,
        Reject::Busy => StatusCode::TOO_MANY_REQUESTS,
        Reject::Stopped | Reject::Disabled => StatusCode::SERVICE_UNAVAILABLE,
        Reject::PayloadExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::BAD_REQUEST,
    };
    error(status, &format!("{reject:?}"))
}
fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error":message}))).into_response()
}
async fn panel(State(app): State<App>, upgrade: WebSocketUpgrade) -> Response {
    let Ok(permit) = app.sockets.clone().try_acquire_owned() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "socket capacity exceeded");
    };
    app.names.refresh();
    let mut names_changed = app.names.changed.subscribe();
    let mut events = app.handle.adapter_events(); // subscribe before snapshot
    upgrade.max_message_size(1_000_000).on_upgrade(move |mut socket| async move {
        let _permit = permit;
        let mut stop = app.stop.clone();
        if *stop.borrow() {return;}
        if !write(&mut socket,snapshot(&app.handle, &app.names)).await {return;}
        loop {tokio::select! {
            _=stop.changed()=>break,
            changed=names_changed.changed()=> {
                if changed.is_err() || !write(&mut socket,snapshot(&app.handle, &app.names)).await {break;}
            },
            message=socket.recv()=>if matches!(message,None|Some(Err(_))|Some(Ok(Message::Close(_)))) {break;},
            event=events.recv()=> {
                let lost = match event {Ok(value) if value["type"]=="lost"=>value["events"].as_u64().unwrap_or(0), Err(broadcast::error::RecvError::Lagged(count))=>count,Err(broadcast::error::RecvError::Closed)=>break,_=>0};
                let mut value = snapshot(&app.handle, &app.names);
                if lost > 0 {value["lostEvents"]=json!(lost);}
                if !write(&mut socket,value).await {break;}
            }
        }}
    }).into_response()
}
#[derive(Deserialize)]
struct DeviceQuery {
    id: String,
}
async fn monitor(
    State(app): State<App>,
    Query(query): Query<DeviceQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    if query.id.is_empty() || query.id.len() > 256 {
        return error(StatusCode::BAD_REQUEST, "invalid device ID");
    }
    let Ok(permit) = app.sockets.clone().try_acquire_owned() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "socket capacity exceeded");
    };
    app.names.refresh();
    let mut names_changed = app.names.changed.subscribe();
    let mut events = app.handle.adapter_events();
    upgrade.max_message_size(1_000_000).on_upgrade(move |mut socket| async move {
        let _permit = permit;
        let mut stop = app.stop.clone();
        if *stop.borrow() {return;}
        let captured=app.handle.snapshot().iter().find(|d|d.entry.id==query.id && d.online).and_then(|d|d.session);
        let status = |handle:&Handle| {
            let value = snapshot(handle, &app.names);
            let device = &value["devices"][&query.id];
            json!({"status":if device["online"]==true {"online"} else {"offline"},"sessionChanged":app.handle.snapshot().iter().find(|d|d.entry.id==query.id).and_then(|d|d.session)!=captured,"injectionEnabled":app.config.injection(),"injectionToggle":app.config.raw_inject_toggle,"meta":{"modelId":device["modelId"],"modelName":device["modelName"],"name":device["name"],"swVersion":device["swVersion"]}})
        };
        if !write(&mut socket,status(&app.handle)).await {return;}
        loop {tokio::select! {
            _=stop.changed()=>break,
            changed=names_changed.changed()=> {
                if changed.is_err() || !write(&mut socket,status(&app.handle)).await {break;}
            },
            message=socket.recv()=>match message {
                Some(Ok(Message::Text(text)))=>{
                    let result=tokio::select! {_=stop.changed()=>break,value=monitor_message(&app,&query.id,captured,&text)=>value};
                    if !write(&mut socket,result).await {break;}
                },
                None|Some(Err(_))|Some(Ok(Message::Close(_)))=>break,_=>{},
            },
            event=events.recv()=> {
                let value = match event {
                    Ok(value) if value["type"]=="data" && value["device"]==query.id=>json!({"rx":value["hex"].as_str().unwrap_or_default().to_ascii_uppercase()}),
                    Ok(value) if value["type"]=="sent" && value["device"]==query.id=>json!({"tx":value["hex"],"deviceAcknowledged":false}),
                    Ok(value) if value["type"]=="injected" && value["device"]==query.id=>if value["toDevice"]==true {json!({"tx":value["hex"],"injected":true})} else {json!({"rx":value["hex"],"injected":true})},
                    Ok(value) if value["type"]=="stateChanged"=>status(&app.handle),
                    Ok(value) if value["type"]=="lost"=>json!({"lostEvents":value["events"]}),
                    Err(broadcast::error::RecvError::Lagged(count))=>json!({"lostEvents":count}),
                    Err(broadcast::error::RecvError::Closed)=>break,_=>continue,
                };
                if !write(&mut socket,value).await {break;}
            }
        }}
    }).into_response()
}
async fn write(socket: &mut WebSocket, value: Value) -> bool {
    matches!(
        timeout(
            Duration::from_secs(2),
            socket.send(Message::Text(value.to_string().into()))
        )
        .await,
        Ok(Ok(()))
    )
}

/// Own HTTP tasks and drain upgraded sockets through their stop receiver/permits.
async fn cloud_inventory(State(app): State<App>) -> Response {
    app.names.begin_query(true); // Explicit user read bypasses the automatic refractory period.
    let result = app.handle.cloud_inventory().await;
    app.names.query_finished();
    if let Ok(inventory) = &result {
        app.names.inventory(inventory);
    }
    cloud_result(result)
}
async fn cloud_devices(State(app): State<App>) -> Json<Value> {
    Json(app.handle.cloud_devices())
}
async fn cloud_device(
    State(app): State<App>,
    Path((id, action)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Response {
    let incarnation = match number(&body, "incarnation") {
        Ok(n) => n,
        Err(e) => return error(StatusCode::BAD_REQUEST, e),
    };
    let result = app
        .handle
        .cloud_device(id, incarnation, &action, body)
        .await;
    if matches!(action.as_str(), "pair" | "adopt" | "unpair") {
        app.names.refresh();
    }
    cloud_result(result)
}
async fn cloud_status(State(app): State<App>) -> Json<Value> {
    Json(app.handle.cloud_status())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CloudLogin {
    country: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CloudComplete {
    url: String,
}
fn cloud_result(result: Result<Value, CloudError>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => {
            let status = match error {
                CloudError::InvalidInput => StatusCode::BAD_REQUEST,
                CloudError::Busy => StatusCode::TOO_MANY_REQUESTS,
                CloudError::Unavailable | CloudError::Cancelled | CloudError::Rejected => {
                    StatusCode::CONFLICT
                }
                CloudError::Authentication => StatusCode::UNAUTHORIZED,
                _ => StatusCode::SERVICE_UNAVAILABLE,
            };
            (status, Json(json!({"error":error.to_string()}))).into_response()
        }
    }
}
async fn cloud_login(State(app): State<App>, Json(input): Json<CloudLogin>) -> Response {
    cloud_result(app.handle.cloud_login(input.country).await)
}
async fn cloud_complete(State(app): State<App>, Json(input): Json<CloudComplete>) -> Response {
    let result = app.handle.cloud_complete(input.url).await;
    if result.is_ok() {
        app.names.force_refresh();
    }
    cloud_result(result)
}
async fn cloud_logout(State(app): State<App>) -> Response {
    let result = app.handle.cloud_logout().await;
    app.names.refresh();
    cloud_result(result)
}
async fn cloud_refresh(State(app): State<App>) -> Response {
    let result = app.handle.cloud_refresh().await;
    if result.is_ok() {
        app.names.force_refresh();
    }
    cloud_result(result)
}
pub async fn serve(
    listener: TcpListener,
    handle: impl Into<Handle>,
    config: Config,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let handle = handle.into();
    config.validate()?;
    let (owned_stop, owned_stopped) = crate::task::Shutdown::new(*stop.borrow());
    let sockets = Arc::new(Semaphore::new(64));
    let names = Names::default();
    let refresher = crate::task::OwnedTask::spawn(refresh_names(
        handle.clone(),
        names.clone(),
        owned_stopped.clone(),
    ));
    let observer = crate::cloud_observer::Observer::new(
        cfg!(feature = "bridge") && handle.cloud_status()["enabled"] == true,
    );
    #[cfg(feature = "scripting")]
    let observer = observer.with_scripts(handle.clone());
    #[cfg(feature = "bridge")]
    let observer_task = crate::task::OwnedTask::spawn(
        observer
            .clone()
            .run(handle.account_handle(), owned_stopped.clone()),
    );
    let app = build(App {
        observer,
        handle,
        names,
        config,
        stop: owned_stopped.clone(),
        sockets: sockets.clone(),
        analysis: Arc::new(Semaphore::new(2)),
    });
    let mut tasks = JoinSet::new();
    let mut failure = None;
    while !*stop.borrow() {
        tokio::select! {
            _=stop.changed()=>break,
            joined=tasks.join_next(),if !tasks.is_empty()=>{if let Some(Err(error))=joined {failure=Some(io::Error::other(error));break;}},
            accepted=listener.accept(),if tasks.len()<128=> {
                let (stream,_) = match accepted {Ok(pair)=>pair,Err(error)=>{failure=Some(error);break;}};
                let app = app.clone();
                let mut stopped = owned_stopped.clone();
                tasks.spawn(async move {
                    let service = hyper_util::service::TowerToHyperService::new(app);
                    let connection = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream),service).with_upgrades();
                    tokio::pin!(connection);
                    if *stopped.borrow() {connection.as_mut().graceful_shutdown();}
                    tokio::select! {
                        _=&mut connection=>{},
                        _=stopped.changed()=>{connection.as_mut().graceful_shutdown();let _=timeout(Duration::from_secs(2),&mut connection).await;},
                        _=tokio::time::sleep(Duration::from_secs(300))=>{},
                    }
                });
            }
        }
    }
    owned_stop.stop();
    let _ = refresher.await;
    #[cfg(feature = "bridge")]
    let _ = observer_task.await;
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            failure.get_or_insert_with(|| io::Error::other(error));
        }
    }
    // Upgraded futures release their permits on stop; await ownership, not a detached tail.
    let _ = sockets.acquire_many(64).await;
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(all(test, feature = "bridge"))]
mod tests {
    use super::*;
    use crate::{lifecycle_storage::Storage, runtime::Runtime};
    use futures_util::StreamExt;

    #[tokio::test(start_paused = true)]
    async fn names_do_not_refresh_merely_because_fifteen_minutes_passed() {
        let directory = tempfile::tempdir().unwrap();
        let server = rusthinq_server::Server::new(Default::default()).unwrap();
        let runtime = Runtime::new(
            Storage::open(&directory.path().join("devices.json"), 4).unwrap(),
            server.handle(),
            Duration::ZERO,
            128,
        )
        .unwrap();
        let names = Names::default();
        names.inventory(&json!([{"deviceId":"d","alias":"Initial"}]));
        let mut changes = names.changed.subscribe();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(refresh_names(
            runtime.handle().into(),
            names.clone(),
            stopped,
        ));
        changes.changed().await.unwrap();
        names.inventory(&json!([{"deviceId":"d","alias":"No timer refresh"}]));
        changes.borrow_and_update();
        tokio::time::advance(Duration::from_secs(901)).await;
        tokio::task::yield_now().await;
        assert!(!changes.has_changed().unwrap());
        names.refresh();
        changes.changed().await.unwrap();
        assert!(names.lock().is_empty());
        stop.send_replace(true);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn automatic_name_reads_have_a_refractory_period_explicit_reads_bypass_it() {
        let names = NameCache::default();
        assert!(names.begin_query(false));
        tokio::time::advance(Duration::from_secs(10)).await;
        names.query_finished();
        assert!(!names.begin_query(false));
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(!names.begin_query(false));
        assert!(names.begin_query(true));
        names.query_finished();
        assert!(!names.begin_query(false));
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(names.begin_query(false));
    }

    #[tokio::test]
    async fn names_refresh_on_start_online_and_offline_but_not_packet_traffic() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let directory = tempfile::tempdir().unwrap();
        let mut server = rusthinq_server::Server::new(Default::default()).unwrap();
        let runtime = Runtime::new(
            Storage::open(&directory.path().join("devices.json"), 4).unwrap(),
            server.handle(),
            Duration::ZERO,
            128,
        )
        .unwrap();
        let handle: Handle = runtime.handle().into();
        let names = Names::default();
        names.inventory(&json!([{"deviceId":"d","alias":"Previous account"}]));
        let mut changes = names.changed.subscribe();
        let (stop, stopped) = watch::channel(false);
        let runtime_task = tokio::spawn(runtime.run(stopped.clone()));
        let refresh_task = tokio::spawn(refresh_names(handle.clone(), names.clone(), stopped));
        timeout(Duration::from_secs(3), changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(names.lock().is_empty()); // Startup clears names while logged out.
        names.inventory(&json!([{"deviceId":"d","alias":"Pending online refresh"}]));
        changes.borrow_and_update();
        let (stream, mut peer) = tokio::io::duplex(8192);
        server.admit(stream).unwrap();
        let payload = json!({"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}).to_string();
        let frame = rusthinq_protocol::thinq1::encode(payload.as_bytes(), 8192).unwrap();
        peer.write_all(&frame).await.unwrap();
        let size = timeout(Duration::from_secs(3), peer.read_u32())
            .await
            .unwrap()
            .unwrap();
        peer.read_exact(&mut vec![0; size as usize]).await.unwrap();
        timeout(Duration::from_secs(3), changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(names.lock().is_empty());
        assert!(handle.snapshot()[0].online);
        names.inventory(&json!([{"deviceId":"d","alias":"Pending offline refresh"}]));
        changes.borrow_and_update();
        peer.write_all(&frame).await.unwrap();
        assert!(
            timeout(Duration::from_millis(1500), changes.changed())
                .await
                .is_err()
        );
        drop(peer);
        timeout(Duration::from_secs(3), changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(names.lock().is_empty());
        assert!(!handle.snapshot()[0].online);
        stop.send_replace(true);
        timeout(Duration::from_secs(3), refresh_task)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(3), runtime_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.shutdown().await;
    }

    #[tokio::test]
    async fn alias_updates_push_dashboard_without_device_traffic() {
        use futures_util::StreamExt;
        let directory = tempfile::tempdir().unwrap();
        let server = rusthinq_server::Server::new(Default::default()).unwrap();
        let runtime = Runtime::new(
            Storage::open(&directory.path().join("devices.json"), 4).unwrap(),
            server.handle(),
            Duration::ZERO,
            2,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = watch::channel(false);
        let names = Names::default();
        let routes = build(App {
            handle: runtime.handle().into(),
            names: names.clone(),
            config: Config {
                bind: address,
                gui: true,
                credentials: None,
                raw_inject_toggle: false,
                raw_inject: Default::default(),
            },
            stop: stopped.clone(),
            sockets: Arc::new(Semaphore::new(64)),
            analysis: Arc::new(Semaphore::new(2)),
            observer: crate::cloud_observer::Observer::new(false),
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, routes)
                .with_graceful_shutdown(async move {
                    let mut stopped = stopped;
                    let _ = stopped.changed().await;
                })
                .await
                .unwrap();
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
            .await
            .unwrap();
        let initial = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            serde_json::from_str::<Value>(initial.to_text().unwrap()).unwrap()["devices"]
                .is_object()
        );
        names.inventory(
            &json!([{"deviceId":"d","alias":"Laundry room"},{"deviceId":"blank","alias":" "}]),
        );
        assert_eq!(names.lock().get("d").unwrap(), "Laundry room");
        assert!(!names.lock().contains_key("blank"));
        let update = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            serde_json::from_str::<Value>(update.to_text().unwrap()).unwrap()["devices"]
                .is_object()
        );
        let mut changes = names.changed.subscribe();
        names.inventory(&json!([{"deviceId":"d","alias":"Laundry room"}]));
        assert!(!changes.has_changed().unwrap());
        names.replace(Default::default());
        changes.changed().await.unwrap();
        assert!(names.lock().is_empty());
        stop.send_replace(true);
        drop(socket);
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn websocket_consumers_report_loss_then_resume_and_join_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let server = rusthinq_server::Server::new(Default::default()).unwrap();
        let runtime = Runtime::new(
            Storage::open(&directory.path().join("devices.json"), 4).unwrap(),
            server.handle(),
            Duration::ZERO,
            2,
        )
        .unwrap();
        let handle = runtime.handle();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(serve(
            listener,
            handle.clone(),
            Config {
                bind: address,
                gui: cfg!(feature = "gui"),
                credentials: None,
                raw_inject_toggle: false,
                raw_inject: Default::default(),
            },
            stopped,
        ));
        let mut paths = vec!["/api/events", "/device?id=d"];
        if cfg!(feature = "gui") {
            paths.push("/ws");
        }
        for path in paths {
            let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}{path}"))
                .await
                .unwrap();
            let initial = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let initial: Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
            // No await in this burst: the subscribed socket cannot drain its bounded
            // event queue until all twenty application events have been published.
            for _ in 0..20 {
                handle.cloud_changed("d".into());
            }
            let lost = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let lost: Value = serde_json::from_str(lost.to_text().unwrap()).unwrap();
            if path == "/api/events" {
                assert_eq!(initial["type"], "snapshot");
                assert_eq!(lost["type"], "lost");
                assert_eq!(lost["events"], 18);
                assert_eq!(lost["state"], initial["state"]);
            } else {
                assert_eq!(lost["lostEvents"], 18);
                if path == "/ws" {
                    assert_eq!(lost["devices"], initial["devices"]);
                }
            }
            // Drain the two retained events, then verify a later event still arrives.
            for _ in 0..2 {
                timeout(Duration::from_secs(2), socket.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            }
            handle.cloud_changed("d".into());
            let resumed = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let resumed: Value = serde_json::from_str(resumed.to_text().unwrap()).unwrap();
            assert!(resumed.get("lostEvents").is_none());
            if path == "/api/events" {
                assert_eq!(resumed["type"], "stateChanged");
            } else if path == "/ws" {
                assert_eq!(resumed["devices"], initial["devices"]);
            } else {
                assert_eq!(resumed["status"], "offline");
            }
            socket.close(None).await.unwrap();
        }
        stop.send_replace(true);
        timeout(Duration::from_secs(4), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
