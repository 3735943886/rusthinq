//! Broker-independent management API and existing dashboard projection.
use crate::runtime::{Event, Handle};
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
use rusthinq_server::{Delivery, Reject};
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
    pub raw_inject: bool,
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
        if let Some(auth) = &self.credentials {
            if auth.user.is_empty()
                || auth.user.contains(':')
                || auth.password.is_empty()
                || auth.user.len() > 256
                || auth.password.len() > 1024
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid management credentials",
                ));
            }
        } else if !self.bind.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "non-loopback management requires authentication",
            ));
        }
        Ok(())
    }
}
#[derive(Clone)]
struct App {
    handle: Handle,
    config: Config,
    stop: watch::Receiver<bool>,
    sockets: Arc<Semaphore>,
}
pub fn router(handle: Handle, config: Config, stop: watch::Receiver<bool>) -> io::Result<Router> {
    config.validate()?;
    Ok(build(App {
        handle,
        config,
        stop,
        sockets: Arc::new(Semaphore::new(64)),
    }))
}
fn build(app: App) -> Router {
    let mut routes = Router::new()
        .route("/api/health", get(health))
        .route("/api/mqtt", get(mqtt_status))
        .route("/api/mqtt/retained/delete", post(delete_retained))
        .route("/api/devices", get(devices))
        .route("/api/devices/{id}/forget", post(forget))
        .route("/api/devices/{id}/send", post(send))
        .route("/api/devices/{id}/invoke", post(invoke))
        .route("/api/devices/{id}/inject", post(inject))
        .route("/api/events", get(event_socket))
        .route("/ws", get(panel))
        .route("/device", get(monitor))
        .route("/forget/{id}", post(forget));
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
async fn mqtt_status(State(app): State<App>) -> Json<Value> {
    let status = app
        .handle
        .external_mqtt()
        .map(|handle| format!("{:?}", *handle.status().borrow()))
        .unwrap_or_else(|| "Disabled".into());
    Json(json!({"status":status}))
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
async fn health(State(app): State<App>) -> Json<Value> {
    Json(
        json!({"running":true,"version":env!("CARGO_PKG_VERSION"),"retainedCleanup":format!("{:?}", &*app.handle.cleanup_status().borrow())}),
    )
}
fn snapshot(handle: &Handle) -> Value {
    let metadata = handle.metadata_snapshot();
    let models = handle.driver_models();
    let states = handle.script_states();
    let mut devices = serde_json::Map::new();
    for device in handle.snapshot() {
        let meta = metadata
            .iter()
            .find(|meta| meta.device_id == device.entry.id);
        let model = models.get(&device.entry.id);
        let script = states
            .get(&device.entry.id)
            .filter(|(session, _, _)| Some(*session) == device.session);
        devices.insert(
            device.entry.id,
            json!({
                "online":device.online,
                "incarnation":device.entry.incarnation.to_string(),
                "generation":device.session.map(|s|s.generation.to_string()),
                "model":model.map(|(_,model,_)|model.as_str()).or_else(||meta.map(|m|m.model_name.as_str())).unwrap_or(""),
                "deviceType":meta.map(|m|m.device_type.as_str()),
                "platform":model.map(|(_,_,t2)|if *t2 {"ThinQ2"} else {"ThinQ1"}).unwrap_or(if meta.is_some() {"ThinQ1"} else {""}),
                "mapped":script.is_some(),"scriptGeneration":script.map(|(_,generation,_)|generation.to_string()),"scriptFaulted":script.is_some_and(|(_,_,faulted)|*faulted),"bridgePaired":false,
                "removal":device.removal.map(|r|format!("{r:?}"))
            }),
        );
    }
    json!({"devices":devices,"version":env!("CARGO_PKG_VERSION"),"features":{"scripting":!states.is_empty(),"bridge":false},"mqtt":null,"guiMqtt":null,"management":true})
}
async fn devices(State(app): State<App>) -> Json<Value> {
    Json(snapshot(&app.handle))
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
        app.handle.forget_scoped(id, incarnation),
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
    match timeout(Duration::from_secs(30), async {
        let receipt = app
            .handle
            .send(
                id,
                SessionKey {
                    incarnation,
                    generation,
                },
                payload.as_bytes().to_vec(),
            )
            .await?;
        Ok::<_, Reject>(receipt.wait().await)
    })
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
        app.handle.invoke_script(
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
                rusthinq_scripting::Error::Stale => StatusCode::CONFLICT,
                rusthinq_scripting::Error::Busy => StatusCode::TOO_MANY_REQUESTS,
                rusthinq_scripting::Error::Stopped => StatusCode::SERVICE_UNAVAILABLE,
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
fn context(context: &crate::scripts::Context) -> Value {
    json!({"device":context.device,"incarnation":context.session.incarnation.to_string(),"generation":context.session.generation.to_string(),"scriptGeneration":context.generation.to_string()})
}
async fn inject(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if !app.config.raw_inject {
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
    match timeout(Duration::from_secs(30), async {
        let receipt = app
            .handle
            .inject(
                id,
                SessionKey {
                    incarnation,
                    generation,
                },
                data,
                to_device,
            )
            .await?;
        Ok::<_, Reject>(match receipt {
            Some(receipt) => Some(receipt.wait().await),
            None => None,
        })
    })
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
    if !app.config.raw_inject {
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
    match app.handle.inject(id.into(), session, data, to_device).await {
        Ok(Some(receipt)) => {
            json!({"delivery":format!("{:?}",receipt.wait().await),"deviceAcknowledged":false})
        }
        Ok(None) => json!({"injected":true}),
        Err(reject) => json!({"error":format!("{reject:?}")}),
    }
}
fn event_value(event: Event) -> Value {
    match event {
        Event::Injected {
            session,
            data,
            to_device,
        } => {
            json!({"type":"injected","device":session.device,"generation":session.generation.to_string(),"hex":rusthinq_protocol::hex::encode(data),"toDevice":to_device})
        }
        Event::ScriptExecuted {
            sequence,
            context: scope,
            error,
        } => {
            json!({"type":"scriptExecuted","sequence":sequence.to_string(),"context":context(&scope),"error":error})
        }
        Event::ScriptOutput {
            context: scope,
            payload,
        } => json!({"type":"scriptOutput","context":context(&scope),"payload":payload}),
        Event::ScriptDelivery {
            context: scope,
            delivery,
        } => {
            json!({"type":"scriptDelivery","context":context(&scope),"delivery":format!("{delivery:?}"),"deviceAcknowledged":false})
        }
        Event::Transport(rusthinq_server::Event::Data(id, bytes)) => {
            json!({"type":"data","device":id.device,"generation":id.generation.to_string(),"hex":rusthinq_protocol::hex::encode(bytes)})
        }
        Event::Lost { transport_events } => json!({"type":"lost","events":transport_events}),
        Event::Rejected { device, reason } => {
            json!({"type":"rejected","device":device,"reason":reason})
        }
        Event::Metadata(metadata) => {
            json!({"type":"metadata","device":metadata.device_id,"model":metadata.model_name,"deviceType":metadata.device_type})
        }
        other => json!({"type":"stateChanged","detail":format!("{other:?}")}),
    }
}
async fn event_socket(State(app): State<App>, upgrade: WebSocketUpgrade) -> Response {
    let Ok(permit) = app.sockets.clone().try_acquire_owned() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "socket capacity exceeded");
    };
    let mut events = app.handle.subscribe();
    upgrade.max_message_size(1_000_000).on_upgrade(move |mut socket|async move {
        let _permit=permit;let mut stop=app.stop.clone();
        if *stop.borrow() || !write(&mut socket,json!({"type":"snapshot","state":snapshot(&app.handle)})).await {return;}
        loop {tokio::select! {
            _=stop.changed()=>break,
            message=socket.recv()=>if matches!(message,None|Some(Err(_))|Some(Ok(Message::Close(_)))) {break;},
            event=events.recv()=> {
                let value=match event {Ok(event)=>event_value(event),Err(broadcast::error::RecvError::Lagged(count))=>json!({"type":"lost","events":count,"state":snapshot(&app.handle)}),Err(broadcast::error::RecvError::Closed)=>break};
                if !write(&mut socket,value).await {break;}
            }
        }}
    }).into_response()
}
fn rejected(reject: Reject) -> Response {
    let status = match reject {
        Reject::StaleSession => StatusCode::CONFLICT,
        Reject::Busy => StatusCode::TOO_MANY_REQUESTS,
        Reject::Stopped => StatusCode::SERVICE_UNAVAILABLE,
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
    let mut events = app.handle.subscribe(); // subscribe before snapshot
    upgrade.max_message_size(1_000_000).on_upgrade(move |mut socket| async move {
        let _permit = permit;
        let mut stop = app.stop.clone();
        if *stop.borrow() {return;}
        if !write(&mut socket,snapshot(&app.handle)).await {return;}
        loop {tokio::select! {
            _=stop.changed()=>break,
            message=socket.recv()=>if matches!(message,None|Some(Err(_))|Some(Ok(Message::Close(_)))) {break;},
            event=events.recv()=> {
                let lost = match event {Ok(Event::Lost {transport_events})=>transport_events, Err(broadcast::error::RecvError::Lagged(count))=>count,Err(broadcast::error::RecvError::Closed)=>break,_=>0};
                let mut value = snapshot(&app.handle);
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
    let mut events = app.handle.subscribe();
    upgrade.max_message_size(1_000_000).on_upgrade(move |mut socket| async move {
        let _permit = permit;
        let mut stop = app.stop.clone();
        if *stop.borrow() {return;}
        let captured=app.handle.snapshot().iter().find(|d|d.entry.id==query.id && d.online).and_then(|d|d.session);
        let status = |handle:&Handle| {
            let value = snapshot(handle);
            let device = &value["devices"][&query.id];
            json!({"status":if device["online"]==true {"online"} else {"offline"},"injectionEnabled":app.config.raw_inject,"meta":{"modelId":device["model"]}})
        };
        if !write(&mut socket,status(&app.handle)).await {return;}
        loop {tokio::select! {
            _=stop.changed()=>break,
            message=socket.recv()=>match message {
                Some(Ok(Message::Text(text)))=>{
                    let result=tokio::select! {_=stop.changed()=>break,value=monitor_message(&app,&query.id,captured,&text)=>value};
                    if !write(&mut socket,result).await {break;}
                },
                None|Some(Err(_))|Some(Ok(Message::Close(_)))=>break,_=>{},
            },
            event=events.recv()=> {
                let value = match event {
                    Ok(Event::Transport(rusthinq_server::Event::Data(id,data))) if id.device == query.id=>json!({"rx":rusthinq_protocol::thinq2::encode_hex(&data)}),
                    Ok(Event::Injected {session,data,to_device}) if session.device==query.id=>if to_device {json!({"tx":rusthinq_protocol::hex::encode(data),"injected":true})} else {json!({"rx":rusthinq_protocol::hex::encode(data),"injected":true})},
                    Ok(Event::Lifecycle(_))=>status(&app.handle),
                    Ok(Event::Lost {transport_events})=>json!({"lostEvents":transport_events}),
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
pub async fn serve(
    listener: TcpListener,
    handle: Handle,
    config: Config,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    config.validate()?;
    let (owned_stop, owned_stopped) = watch::channel(*stop.borrow());
    let sockets = Arc::new(Semaphore::new(64));
    let app = build(App {
        handle,
        config,
        stop: owned_stopped.clone(),
        sockets: sockets.clone(),
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
    owned_stop.send_replace(true);
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
