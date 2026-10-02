//! L6 pairing checkpoints and owned cloud task lifetimes; L4 owns network sessions.
use crate::{
    pairing_storage::{Owner, Record, Store},
    runtime::{Event as AppEvent, Handle as App},
};
use rusthinq_bridge::{
    account,
    cloud::{Client, NewDevice},
    devices::{BridgeHandle, DeregisterFuture, Deregistration, Registration},
    pairing::Material,
    session, transport,
};
use rusthinq_server::{ServerHandle, SessionId, mqtt};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub device: String,
    pub incarnation: u64,
    pub attempt: u64,
    pub paired: bool,
    pub enabled: bool,
    pub connected: bool,
    pub error: Option<String>,
}
#[derive(Clone, Debug)]
pub struct Pair {
    pub device: String,
    pub incarnation: u64,
    pub alias: String,
    pub device_type: String,
    pub model: String,
    pub thinq2: bool,
}
#[derive(Clone, Debug)]
pub enum Operation {
    Pair(Pair),
    Enable {
        device: String,
        incarnation: u64,
        enabled: bool,
    },
    Unpair {
        device: String,
        incarnation: u64,
    },
}
struct Command {
    operation: Operation,
    result: oneshot::Sender<io::Result<()>>,
    registration: Option<Registration>,
}
#[derive(Clone)]
pub struct Handle {
    commands: mpsc::Sender<Command>,
    status: Arc<Mutex<BTreeMap<String, Status>>>,
}
impl Handle {
    pub fn snapshot(&self) -> Vec<Status> {
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
    pub async fn operate(&self, operation: Operation) -> io::Result<()> {
        self.request(operation, None).await
    }
    async fn request(
        &self,
        operation: Operation,
        registration: Option<Registration>,
    ) -> io::Result<()> {
        let (result, reply) = oneshot::channel();
        self.commands
            .try_send(Command {
                operation,
                result,
                registration,
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "cloud management busy or stopped",
                )
            })?;
        reply
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "cloud management stopped"))?
    }
}
impl Deregistration for Handle {
    fn deregister(&self, registration: Registration) -> DeregisterFuture {
        let handle = self.clone();
        Box::pin(async move {
            handle
                .request(
                    Operation::Unpair {
                        device: registration.device.clone(),
                        incarnation: registration.incarnation,
                    },
                    Some(registration),
                )
                .await
        })
    }
}
struct Task {
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}
pub struct Runtime {
    store: Arc<Mutex<Store>>,
    commands: mpsc::Receiver<Command>,
    handle: Handle,
    account: account::Handle,
    app: App,
    server: ServerHandle,
    broker: mqtt::Handle,
    bridge: BridgeHandle,
    firmware: rusthinq_bridge::passthrough::Relay,
    generation: Arc<AtomicU64>,
    tasks: BTreeMap<String, Task>,
}
impl Runtime {
    pub async fn open(
        path: PathBuf,
        account: account::Handle,
        app: App,
        server: ServerHandle,
        broker: mqtt::Handle,
        firmware: rusthinq_bridge::passthrough::Relay,
    ) -> io::Result<(Handle, Self, BridgeHandle)> {
        let store = tokio::task::spawn_blocking(move || Store::open(&path, 256))
            .await
            .map_err(io::Error::other)??;
        let bridge = BridgeHandle::new(256, 1_000_000, 256)
            .map_err(|_| io::Error::other("invalid bridge limits"))?;
        let (commands, received) = mpsc::channel(16);
        let handle = Handle {
            commands,
            status: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let runtime = Self {
            store: Arc::new(Mutex::new(store)),
            commands: received,
            handle: handle.clone(),
            account,
            app,
            server,
            broker,
            bridge: bridge.clone(),
            firmware,
            generation: Arc::new(AtomicU64::new(2)),
            tasks: BTreeMap::new(),
        };
        runtime.project();
        Ok((handle, runtime, bridge))
    }
    fn records(&self) -> Vec<Record> {
        self.store
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot()
    }
    fn project(&self) {
        let mut statuses = self.handle.status.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::mem::take(&mut *statuses);
        for record in self.records() {
            let owner = &record.attempt.owner;
            let prior = previous.get(&owner.device);
            statuses.insert(
                owner.device.clone(),
                Status {
                    device: owner.device.clone(),
                    incarnation: owner.incarnation,
                    attempt: record.attempt.sequence,
                    paired: record.material.is_some(),
                    enabled: record.enabled,
                    connected: prior.is_some_and(|s| s.connected),
                    error: prior.and_then(|s| s.error.clone()),
                },
            );
        }
    }
    async fn storage<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Store) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            operation(&mut store.lock().unwrap_or_else(|e| e.into_inner()))
        })
        .await
        .map_err(io::Error::other)?
    }
    async fn stop_device(&mut self, id: &str) -> io::Result<()> {
        if let Some(task) = self.tasks.remove(id) {
            task.stop.send_replace(true);
            task.task.await.map_err(io::Error::other)??;
        }
        Ok(())
    }
    fn current(&self, id: &str, incarnation: u64) -> bool {
        self.app
            .snapshot()
            .iter()
            .any(|d| d.entry.id == id && d.entry.incarnation == incarnation && d.removal.is_none())
    }
    async fn command(
        &mut self,
        command: Command,
        stop: &mut watch::Receiver<bool>,
    ) -> io::Result<()> {
        match command.operation {
            Operation::Pair(pair) => {
                if !self.current(&pair.device, pair.incarnation) {
                    return Err(stale());
                }
                for text in [&pair.device, &pair.model, &pair.alias, &pair.device_type] {
                    if text.is_empty() || text.len() > 256 || text.chars().any(char::is_control) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid pairing input",
                        ));
                    }
                }
                let client = self
                    .account
                    .authenticated_client()
                    .map_err(io::Error::other)?;
                let identity = client.account_identity().ok_or_else(stale)?.to_string();
                let owner = Owner {
                    device: pair.device.clone(),
                    incarnation: pair.incarnation,
                    account: identity.clone(),
                };
                let attempt = self.storage(move |store| store.begin(owner)).await?;
                self.project();
                let mut cancelled = self.account.cancellation();
                let epoch = *cancelled.borrow_and_update();
                // Intent is already durable. Unknown remote outcomes stay owned and are never retried.
                let remote = async {
                    client
                        .pair_device(NewDevice {
                            id: &pair.device,
                            alias: &pair.alias,
                            model: &pair.model,
                            device_type: &pair.device_type,
                            platform: if pair.thinq2 { "thinq2" } else { "thinq1" },
                            ciphertext: None,
                        })
                        .await
                        .map_err(io::Error::other)
                };
                let material = tokio::select! {biased;_=stop.changed()=>return Err(stale()),_=cancelled.changed()=>return Err(stale()),result=timeout(Duration::from_secs(90),remote)=>result.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"pairing outcome unknown"))??};
                if *cancelled.borrow() != epoch {
                    return Err(stale());
                }
                let saved = attempt.clone();
                self.storage(move |store| store.complete(&saved, material))
                    .await?;
                self.project();
                if !self.current(&pair.device, pair.incarnation) {
                    return Err(stale());
                }
                let registration = self
                    .bridge
                    .registered(pair.device, pair.incarnation)
                    .map_err(io::Error::other)?;
                self.bridge
                    .disable(&registration)
                    .map_err(io::Error::other)?;
                Ok(())
            }
            Operation::Enable {
                device,
                incarnation,
                enabled,
            } => {
                if !self.current(&device, incarnation) {
                    return Err(stale());
                }
                let record = self.record(&device, incarnation)?;
                if enabled {
                    let client = self
                        .account
                        .authenticated_client()
                        .map_err(io::Error::other)?;
                    if client.account_identity() != Some(record.attempt.owner.account.as_str()) {
                        return Err(stale());
                    }
                    record
                        .material
                        .as_ref()
                        .ok_or_else(stale)?
                        .validate()
                        .map_err(io::Error::other)?;
                }
                let attempt = record.attempt.clone();
                self.storage(move |store| store.set_enabled(&attempt, enabled))
                    .await?;
                self.stop_device(&device).await?;
                let registration = self
                    .bridge
                    .registered(device.clone(), incarnation)
                    .map_err(io::Error::other)?;
                if enabled {
                    self.start(record, registration)?;
                } else {
                    self.bridge
                        .disable(&registration)
                        .map_err(io::Error::other)?;
                }
                self.project();
                Ok(())
            }
            Operation::Unpair {
                device,
                incarnation,
            } => {
                let record = self.record(&device, incarnation)?;
                let client = self
                    .account
                    .authenticated_client()
                    .map_err(io::Error::other)?;
                if client.account_identity() != Some(record.attempt.owner.account.as_str()) {
                    return Err(stale());
                }
                if let Some(registration) = &command.registration {
                    if !self
                        .bridge
                        .snapshot()
                        .iter()
                        .any(|s| s.registration == *registration)
                    {
                        return Err(stale());
                    }
                }
                let attempt = record.attempt.clone();
                self.storage(move |store| store.set_enabled(&attempt, false))
                    .await?;
                self.stop_device(&device).await?;
                if let Some(status) = self
                    .bridge
                    .snapshot()
                    .iter()
                    .find(|s| s.registration.device == device)
                {
                    self.bridge
                        .disable(&status.registration)
                        .map_err(io::Error::other)?;
                }
                let mut cancelled = self.account.cancellation();
                cancelled.borrow_and_update();
                tokio::select! {biased;_=stop.changed()=>return Err(stale()),_=cancelled.changed()=>return Err(stale()),result=client.remove_device(&device)=>result.map_err(io::Error::other)?};
                let attempt = record.attempt.clone();
                self.storage(move |store| store.remove_confirmed(&attempt, true))
                    .await?;
                if command.registration.is_none()
                    && let Some(status) = self
                        .bridge
                        .snapshot()
                        .iter()
                        .find(|s| s.registration.device == device)
                {
                    self.bridge
                        .deregistered(&status.registration)
                        .map_err(io::Error::other)?;
                }
                self.project();
                Ok(())
            }
        }
    }
    fn record(&self, id: &str, incarnation: u64) -> io::Result<Record> {
        self.records()
            .into_iter()
            .find(|r| r.attempt.owner.device == id && r.attempt.owner.incarnation == incarnation)
            .ok_or_else(stale)
    }
    fn start(&mut self, record: Record, registration: Registration) -> io::Result<()> {
        let (stop, stopped) = watch::channel(false);
        let context = Context {
            app: self.app.clone(),
            account: self.account.clone(),
            server: self.server.clone(),
            broker: self.broker.clone(),
            bridge: self.bridge.clone(),
            firmware: self.firmware.clone(),
            generation: self.generation.clone(),
            statuses: self.handle.status.clone(),
        };
        let device = record.attempt.owner.device.clone();
        let task = tokio::spawn(supervise(context, record, registration, stopped));
        self.tasks.insert(device, Task { stop, task });
        Ok(())
    }
    pub async fn run(mut self, mut stop: watch::Receiver<bool>) -> io::Result<()> {
        // Restore registration ownership even without authentication or an active socket.
        for record in self.records() {
            if record.material.is_some() {
                let registration = self
                    .bridge
                    .registered(
                        record.attempt.owner.device.clone(),
                        record.attempt.owner.incarnation,
                    )
                    .map_err(io::Error::other)?;
                self.bridge
                    .disable(&registration)
                    .map_err(io::Error::other)?;
            }
        }
        while !*stop.borrow() {
            let command = tokio::select! {biased;_=stop.changed()=>break,command=self.commands.recv()=>match command{Some(c)=>c,None=>break}};
            if command.result.is_closed() {
                continue;
            }
            let result = self
                .command(
                    Command {
                        operation: command.operation,
                        result: oneshot::channel().0,
                        registration: command.registration,
                    },
                    &mut stop,
                )
                .await;
            if let Err(error) = &result {
                self.project();
                let mut statuses = self.handle.status.lock().unwrap_or_else(|e| e.into_inner());
                for status in statuses.values_mut() {
                    status.error = Some(error.to_string());
                }
            }
            let _ = command.result.send(result);
        }
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            let _ = command.result.send(Err(stale()));
        }
        let ids: Vec<_> = self.tasks.keys().cloned().collect();
        let mut failure = None;
        for id in ids {
            if let Err(e) = self.stop_device(&id).await {
                failure.get_or_insert(e);
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
fn stale() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "cloud owner unavailable or stale",
    )
}
struct Context {
    app: App,
    account: account::Handle,
    server: ServerHandle,
    broker: mqtt::Handle,
    bridge: BridgeHandle,
    firmware: rusthinq_bridge::passthrough::Relay,
    generation: Arc<AtomicU64>,
    statuses: Arc<Mutex<BTreeMap<String, Status>>>,
}
async fn supervise(
    context: Context,
    record: Record,
    registration: Registration,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let device = record.attempt.owner.device.clone();
    let account = record.attempt.owner.account.clone();
    let mut clients = context.account.clients();
    let mut cancelled = context.account.cancellation();
    cancelled.borrow_and_update();
    let material = record.material.ok_or_else(stale)?;
    let prepared = material.clone();
    let connector = tokio::task::spawn_blocking(move || {
        transport::Connector::new(&prepared, Default::default())
    })
    .await
    .map_err(io::Error::other)??;
    let mut backoff = 1;
    let mut events = context.app.subscribe();
    loop {
        if *stop.borrow() {
            break;
        }
        let client = clients
            .borrow()
            .clone()
            .filter(|c| c.authenticated() && c.account_identity() == Some(account.as_str()));
        let current = context.app.snapshot().into_iter().find(|d| {
            d.entry.id == device
                && d.entry.incarnation == registration.incarnation
                && d.removal.is_none()
        });
        if current.is_none() {
            break;
        }
        let local = current
            .filter(|d| d.online)
            .and_then(|d| d.session)
            .map(|s| SessionId {
                device: device.clone(),
                generation: s.generation,
            });
        let Some(local) = local.filter(|_| client.is_some()) else {
            tokio::select! {biased;_=stop.changed()=>break,_=cancelled.changed()=>break,_=clients.changed()=>{},_=events.recv()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            continue;
        };
        context
            .bridge
            .registered(device.clone(), registration.incarnation)
            .map_err(io::Error::other)?;
        context
            .bridge
            .bind(&registration, local.clone())
            .map_err(io::Error::other)?;
        let generation = context
            .generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(2))
            .map_err(|_| stale())?;
        let result = connected(
            &context,
            &connector,
            &material,
            &registration,
            &local,
            generation,
            &mut events,
            &mut stop,
            &mut cancelled,
            &mut clients,
            &account,
        )
        .await;
        if matches!(material, Material::ThinQ2 { .. }) {
            if let Ok(receipt) = context.broker.bridge_state(&local, generation + 1, false) {
                let _ = timeout(Duration::from_secs(15), receipt.wait()).await;
            }
        }
        let _ = context.bridge.unbind(&registration, &local);
        if let Some(status) = context
            .statuses
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&device)
        {
            status.connected = false;
            status.error = result.as_ref().err().map(ToString::to_string);
        }
        if *stop.borrow() || cancelled.has_changed().unwrap_or(true) {
            break;
        }
        if let Err(error) = &result
            && matches!(
                error.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied
            )
        {
            break;
        }
        tokio::select! {biased;_=stop.changed()=>break,_=cancelled.changed()=>break,_=tokio::time::sleep(Duration::from_secs(backoff))=>{}}
        backoff = (backoff * 2).min(60);
    }
    context
        .bridge
        .disable(&registration)
        .map_err(io::Error::other)?;
    Ok(())
}
#[allow(clippy::too_many_arguments)]
async fn connected(
    context: &Context,
    connector: &transport::Connector,
    material: &Material,
    registration: &Registration,
    local: &SessionId,
    generation: u64,
    events: &mut tokio::sync::broadcast::Receiver<AppEvent>,
    stop: &mut watch::Receiver<bool>,
    cancelled: &mut watch::Receiver<u64>,
    clients: &mut watch::Receiver<Option<Arc<Client>>>,
    account: &str,
) -> io::Result<()> {
    let stream = tokio::select! {biased;_=stop.changed()=>return Ok(()),_=cancelled.changed()=>return Ok(()),result=connector.connect()=>result?};
    let identity = session::Identity {
        device: local.device.clone(),
        model: context
            .app
            .driver_models()
            .get(&local.device)
            .map(|m| m.1.clone())
            .or_else(|| {
                context
                    .app
                    .persisted_models()
                    .get(&local.device)
                    .map(|m| m.model_name.clone())
            })
            .ok_or_else(stale)?,
    };
    let (uplink, received) = mpsc::channel(32);
    let (output, mut input) = mpsc::channel(32);
    let (_stop, stopped) = watch::channel(false);
    let operation = async {
        match material {
            Material::ThinQ1 { .. } => {
                session::thinq1(stream, identity, received, output, stopped).await
            }
            Material::ThinQ2 {
                pub_topic,
                prov_topic,
                sub_topic,
                ..
            } => {
                let deploy = context
                    .app
                    .cloud_deploy(&local.device, local.generation)
                    .ok_or_else(stale)?;
                session::thinq2(
                    stream,
                    session::MqttConfig {
                        identity,
                        publish: pub_topic.clone(),
                        provisioning: prov_topic.clone(),
                        subscribe: sub_topic.clone(),
                        deploy,
                    },
                    received,
                    output,
                    stopped,
                )
                .await
            }
        }
    };
    tokio::pin!(operation);
    let mut ready = false;
    loop {
        tokio::select! {biased;
            _=stop.changed()=>return Ok(()),_=cancelled.changed()=>return Ok(()),
            _=clients.changed()=>{if !clients.borrow().as_ref().is_some_and(|c|c.authenticated() && c.account_identity()==Some(account)){return Ok(());}},
            result=&mut operation=>return result,
            event=input.recv()=>match event{
                Some(session::Event::Ready)=>{
                    if matches!(material,Material::ThinQ2{..}) {if context.broker.bridge_state(local,generation,true).map_err(io::Error::other)?.wait().await!=rusthinq_server::Delivery::Sent{return Err(stale());}}
                    ready=true;if let Some(status)=context.statuses.lock().unwrap_or_else(|e|e.into_inner()).get_mut(&local.device){status.connected=true;status.error=None;}
                },
                Some(session::Event::Downlink{payload,result})=>{
                    let receipt=if ready && matches!(material,Material::ThinQ1{..}){context.bridge.downlink(registration,local,&payload,&context.server,Some(&context.firmware)).map_err(io::Error::other)}else if ready{
                        let value:serde_json::Value=serde_json::from_slice(payload.strip_suffix(&[0]).unwrap_or(&payload)).map_err(io::Error::other)?;context.firmware.learn_command(&value)?;context.broker.cloud(local,generation,&payload).map_err(io::Error::other)
                    }else{Err(stale())};
                    let delivered=match receipt{Ok(receipt)=>timeout(Duration::from_secs(15),receipt.wait()).await==Ok(rusthinq_server::Delivery::Sent),Err(_)=>false};let _=result.send(delivered);
                },None=>return Err(stale())
            },
            event=events.recv()=>{
                let payload=match event {
                    Ok(AppEvent::Transport(rusthinq_server::Event::Data(id,data))) if id==*local && matches!(material,Material::ThinQ1{..})=>Some(data),
                    Ok(AppEvent::Transport(rusthinq_server::Event::BridgedCloudBound(id,g,bytes))) if id==*local && g==generation=>Some(bytes),
                    Ok(AppEvent::Transport(rusthinq_server::Event::Down(id,_))) if id==*local=>return Ok(()),
                    Err(_)|Ok(AppEvent::Lost{..})=>return Err(io::Error::other("cloud source events lost")),_=>None,
                };
                if !context.app.snapshot().iter().any(|d|d.entry.id==local.device && d.entry.incarnation==registration.incarnation && d.session.is_some_and(|s|s.generation==local.generation) && d.online && d.removal.is_none()){return Ok(());}
                if ready && let Some(payload)=payload {let (result,_)=oneshot::channel();uplink.try_send(session::Uplink{payload,result}).map_err(|_|io::Error::new(io::ErrorKind::WouldBlock,"cloud uplink capacity exceeded"))?;}
            }
        }
    }
}
