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
use serde_json::Value;
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
#[derive(Clone)]
pub enum Operation {
    Pair(Pair),
    Adopt {
        device: String,
        incarnation: u64,
        archive: Value,
    },
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
            .map_err(|e| io::Error::other(format!("{e:?}")))??;
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
        drop(statuses);
        self.app.cloud_changed(String::new());
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
        .map_err(|e| io::Error::other(format!("{e:?}")))?
    }
    async fn stop_device(&mut self, id: &str) -> io::Result<()> {
        if let Some(task) = self.tasks.remove(id) {
            task.stop.send_replace(true);
            // A terminated session already reports its error in device status.
            // Disabling or retrying must still finish its cleanup.
            let _ = task
                .task
                .await
                .map_err(|e| io::Error::other(format!("{e:?}")))?;
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
        operation: Operation,
        registration: Option<Registration>,
        stop: &mut watch::Receiver<bool>,
    ) -> io::Result<()> {
        match operation {
            Operation::Adopt {
                device,
                incarnation,
                archive,
            } => {
                if !self.current(&device, incarnation) {
                    return Err(stale());
                }
                let epoch = *self.account.cancellation().borrow();
                let client = self.account.authenticated_client().map_err(|_| stale())?;
                let country = client.country().to_owned();
                let material = self
                    .storage(move |_| {
                        Material::from_legacy_in_country(archive, &country).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "invalid archived pairing material",
                            )
                        })
                    })
                    .await?;
                if let Some(meta) = self.app.persisted_models().get(&device)
                    && meta.incarnation == incarnation
                    && meta.thinq2 != matches!(material, Material::ThinQ2 { .. })
                {
                    return Err(stale());
                }
                let identity = client.account_identity().ok_or_else(stale)?.to_owned();
                if let Material::ThinQ2 { country, .. } = &material
                    && country != client.country()
                {
                    return Err(stale());
                }
                let devices = owned_remote(&self.account, &identity, epoch, stop, async {
                    timeout(Duration::from_secs(30), client.list_devices())
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "cloud inventory timed out")
                        })?
                        .map_err(|_| io::Error::other("cloud inventory unavailable"))
                })
                .await?;
                if !devices.iter().any(|entry| entry["deviceId"] == device) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "registration absent from authenticated account",
                    ));
                }
                if !self.current(&device, incarnation) {
                    return Err(stale());
                }
                let owner = Owner {
                    device: device.clone(),
                    incarnation,
                    account: identity.clone(),
                };
                self.storage(move |store| store.adopt(owner, material))
                    .await?;
                self.project();
                if !self.current(&device, incarnation)
                    || *self.account.cancellation().borrow() != epoch
                    || !self.account.authenticated_client().is_ok_and(|c| {
                        c.account_identity() == Some(identity.as_str())
                            && c.country() == client.country()
                    })
                {
                    return Err(stale());
                }
                let registration = self
                    .bridge
                    .registered(device, incarnation)
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                self.bridge
                    .disable(&registration)
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                Ok(())
            }
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
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                let identity = client.account_identity().ok_or_else(stale)?.to_string();
                let mut cancelled = self.account.cancellation();
                let epoch = *cancelled.borrow_and_update();
                let owner = Owner {
                    device: pair.device.clone(),
                    incarnation: pair.incarnation,
                    account: identity.clone(),
                };
                let attempt = self.storage(move |store| store.begin(owner)).await?;
                self.project();
                if !self.current(&pair.device, pair.incarnation)
                    || *cancelled.borrow() != epoch
                    || !self
                        .account
                        .authenticated_client()
                        .is_ok_and(|c| c.account_identity() == Some(identity.as_str()))
                {
                    return Err(stale());
                }
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
                        .map_err(|e| io::Error::other(format!("{e:?}")))
                };
                let material = owned_remote(&self.account, &identity, epoch, stop, async {
                    timeout(Duration::from_secs(90), remote)
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "pairing outcome unknown")
                        })?
                })
                .await?;
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
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                self.bridge
                    .disable(&registration)
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
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
                if record.material.is_none() {
                    return Err(stale());
                }
                if enabled {
                    let client = self
                        .account
                        .authenticated_client()
                        .map_err(|e| io::Error::other(format!("{e:?}")))?;
                    if client.account_identity() != Some(record.attempt.owner.account.as_str()) {
                        return Err(stale());
                    }
                    record
                        .material
                        .as_ref()
                        .ok_or_else(stale)?
                        .validate()
                        .map_err(|e| io::Error::other(format!("{e:?}")))?;
                }
                let attempt = record.attempt.clone();
                self.storage(move |store| store.set_enabled(&attempt, enabled))
                    .await?;
                self.stop_device(&device).await?;
                let registration = self
                    .bridge
                    .registered(device.clone(), incarnation)
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                if enabled {
                    self.start(record, registration)?;
                } else {
                    self.bridge
                        .disable(&registration)
                        .map_err(|e| io::Error::other(format!("{e:?}")))?;
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
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                if client.account_identity() != Some(record.attempt.owner.account.as_str()) {
                    return Err(stale());
                }
                if let Some(registration) = &registration
                    && !self
                        .bridge
                        .snapshot()
                        .iter()
                        .any(|s| s.registration == *registration)
                {
                    return Err(stale());
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
                        .map_err(|e| io::Error::other(format!("{e:?}")))?;
                }
                let mut cancelled = self.account.cancellation();
                cancelled.borrow_and_update();
                let epoch = *cancelled.borrow();
                owned_remote(
                    &self.account,
                    &record.attempt.owner.account,
                    epoch,
                    stop,
                    async {
                        client
                            .remove_device(&device)
                            .await
                            .map_err(|e| io::Error::other(format!("{e:?}")))
                    },
                )
                .await?;
                let attempt = record.attempt.clone();
                self.storage(move |store| store.remove_confirmed(&attempt, true))
                    .await?;
                if registration.is_none()
                    && let Some(status) = self
                        .bridge
                        .snapshot()
                        .iter()
                        .find(|s| s.registration.device == device)
                {
                    self.bridge
                        .deregistered(&status.registration)
                        .map_err(|e| io::Error::other(format!("{e:?}")))?;
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
    /// Read-only account reconciliation never deletes registration material.
    /// A suspicious empty response is confirmed on a separate poll before pausing.
    async fn reconcile_account(
        &mut self,
        empty: &mut Option<String>,
        stop: &mut watch::Receiver<bool>,
    ) -> io::Result<bool> {
        let client = match self.account.authenticated_client() {
            Ok(client) => client,
            Err(_) => {
                *empty = None;
                return Ok(false);
            }
        };
        let identity = client.account_identity().ok_or_else(stale)?.to_owned();
        let epoch = *self.account.cancellation().borrow();
        let inventory = owned_remote(&self.account, &identity, epoch, stop, async {
            timeout(Duration::from_secs(30), client.list_devices())
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "account reconciliation timed out")
                })?
                .map_err(|_| io::Error::other("account inventory unavailable"))
        })
        .await?;
        if inventory.is_empty() && empty.as_ref() != Some(&identity) {
            *empty = Some(identity);
            return Ok(true);
        }
        *empty = None;
        for record in self.records() {
            let owner = &record.attempt.owner;
            if owner.account != identity
                || !record.enabled
                || record.material.is_none()
                || !self.current(&owner.device, owner.incarnation)
                || inventory
                    .iter()
                    .any(|device| device["deviceId"] == owner.device)
            {
                continue;
            }
            let attempt = record.attempt.clone();
            self.storage(move |store| store.set_enabled(&attempt, false))
                .await?;
            self.stop_device(&owner.device).await?;
            if let Some(status) = self.bridge.snapshot().iter().find(|status| {
                status.registration.device == owner.device
                    && status.registration.incarnation == owner.incarnation
            }) {
                self.bridge
                    .disable(&status.registration)
                    .map_err(|_| stale())?;
            }
            self.project();
            if let Some(status) = self
                .handle
                .status
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get_mut(&owner.device)
            {
                status.connected = false;
                status.error = Some("Registration absent from the LG account; bridge paused, saved material preserved".into());
            }
            self.app.cloud_changed(owner.device.clone());
        }
        Ok(false)
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
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                self.bridge
                    .disable(&registration)
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                if record.enabled {
                    self.start(record, registration)?;
                }
            }
        }
        let mut accounts = self.account.clients();
        let mut empty_inventory = None;
        let mut reconcile_at = tokio::time::Instant::now();
        while !*stop.borrow() {
            let command = tokio::select! {
                biased;
                _=stop.changed()=>break,
                command=self.commands.recv()=>match command{Some(c)=>c,None=>break},
                changed=accounts.changed()=>{
                    if changed.is_err() { break; }
                    accounts.borrow_and_update();
                    empty_inventory = None;
                    reconcile_at = tokio::time::Instant::now();
                    continue;
                },
                _=tokio::time::sleep_until(reconcile_at)=>{
                    let recheck = self.reconcile_account(&mut empty_inventory, &mut stop).await.unwrap_or(false);
                    reconcile_at = tokio::time::Instant::now() + Duration::from_secs(if recheck {60} else {900});
                    continue;
                }
            };
            if command.result.is_closed() {
                continue;
            }
            let target = match &command.operation {
                Operation::Pair(p) => p.device.clone(),
                Operation::Adopt { device, .. }
                | Operation::Enable { device, .. }
                | Operation::Unpair { device, .. } => device.clone(),
            };
            let result = self
                .command(command.operation, command.registration, &mut stop)
                .await;
            if let Err(error) = &result {
                self.project();
                let mut statuses = self.handle.status.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(status) = statuses.get_mut(&target) {
                    status.error = Some(error.to_string());
                }
                drop(statuses);
                self.app.cloud_changed(target);
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
    .map_err(|e| io::Error::other(format!("{e:?}")))??;
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
            tokio::select! {biased;_=stop.changed()=>break,_=cancelled.changed()=>{cancelled.borrow_and_update();},_=clients.changed()=>{},_=events.recv()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            continue;
        };
        context
            .bridge
            .registered(device.clone(), registration.incarnation)
            .map_err(|e| io::Error::other(format!("{e:?}")))?;
        // Stale here means the local session was replaced or closed since the snapshot:
        // re-evaluate rather than ending the supervisor.
        if let Err(error) = context.bridge.bind(&registration, local.clone()) {
            if error != rusthinq_bridge::devices::Error::Stale {
                return Err(io::Error::other(format!("{error:?}")));
            }
            tokio::select! {biased;_=stop.changed()=>break,_=events.recv()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            continue;
        }
        let generation = context
            .generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(2))
            .map_err(|_| stale())?;
        let mut owned_stop = stop.clone();
        let epoch = *cancelled.borrow();
        let result = owned_remote(
            &context.account,
            &account,
            epoch,
            &mut owned_stop,
            connected(
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
            ),
        )
        .await;
        if matches!(material, Material::ThinQ2 { .. })
            && let Ok(receipt) = context.broker.bridge_state(&local, generation + 1, false)
        {
            let _ = timeout(Duration::from_secs(15), receipt.wait()).await;
        }
        // Only the cloud session may have ended. Unbind means the *local* session is
        // finished, after which it can never be bound again; keep the binding while that
        // local session is still current so the next cloud session can reuse it.
        let local_current = context.app.snapshot().iter().any(|d| {
            d.entry.id == device
                && d.entry.incarnation == registration.incarnation
                && d.online
                && d.removal.is_none()
                && d.session.is_some_and(|s| s.generation == local.generation)
        });
        if !local_current {
            let _ = context.bridge.unbind(&registration, &local);
        }
        if let Some(status) = context
            .statuses
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&device)
        {
            // A session that reached Ready was healthy: reconnect promptly, as 0.1 did.
            if status.connected {
                backoff = 1;
            }
            status.connected = false;
            status.error = result.as_ref().err().map(ToString::to_string);
        }
        context.app.cloud_changed(device.clone());
        if *stop.borrow() {
            break;
        }
        if cancelled.has_changed().unwrap_or(true) {
            cancelled.borrow_and_update();
            continue;
        }
        if let Err(error) = &result
            && matches!(
                error.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied
            )
        {
            break;
        }
        tokio::select! {biased;_=stop.changed()=>break,_=cancelled.changed()=>{cancelled.borrow_and_update();},_=tokio::time::sleep(Duration::from_secs(backoff))=>{}}
        backoff = (backoff * 2).min(60);
    }
    context
        .bridge
        .disable(&registration)
        .map_err(|e| io::Error::other(format!("{e:?}")))?;
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
    if matches!(material, Material::ThinQ1 { .. }) {
        let device_type = context
            .app
            .persisted_models()
            .get(&local.device)
            .map(|meta| meta.device_type.clone())
            .unwrap_or_else(|| "201".into());
        transport::prepare_thinq1(material, &local.device, &identity.model, &device_type).await?;
    }
    let stream = tokio::select! {biased;_=stop.changed()=>return Ok(()),_=cancelled.changed()=>return Ok(()),result=connector.connect()=>result?};
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
        let expires = clients
            .borrow()
            .as_ref()
            .and_then(|c| c.expires_at())
            .unwrap_or_else(tokio::time::Instant::now);
        tokio::select! {biased;
        _=tokio::time::sleep_until(expires)=>return Ok(()),
            _=stop.changed()=>return Ok(()),_=cancelled.changed()=>return Ok(()),
            _=clients.changed()=>{if !clients.borrow().as_ref().is_some_and(|c|c.authenticated() && c.account_identity()==Some(account)){return Ok(());}},
            result=&mut operation=>return result,
            event=input.recv()=>match event{
                Some(session::Event::Ready)=>{
                    if matches!(material,Material::ThinQ2{..}) && timeout(Duration::from_secs(15),context.broker.bridge_state(local,generation,true).map_err(|e|io::Error::other(format!("{e:?}")))?.wait()).await!=Ok(rusthinq_server::Delivery::Sent){return Err(stale());}
                    ready=true;if let Some(status)=context.statuses.lock().unwrap_or_else(|e|e.into_inner()).get_mut(&local.device){status.connected=true;status.error=None;}
                context.app.cloud_changed(local.device.clone());
                },
                Some(session::Event::Downlink{payload,result})=>{
                    let receipt=if ready && matches!(material,Material::ThinQ1{..}){context.bridge.downlink(registration,local,&payload,&context.server,Some(&context.firmware)).map_err(|e|io::Error::other(format!("{e:?}")))}else if ready{
                        let value:serde_json::Value=serde_json::from_slice(payload.strip_suffix(&[0]).unwrap_or(&payload)).map_err(|e|io::Error::other(format!("{e:?}")))?;context.firmware.learn_command(&value)?;context.broker.cloud(local,generation,&payload).map_err(|e|io::Error::other(format!("{e:?}")))
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

async fn owned_remote<T>(
    account: &account::Handle,
    identity: &str,
    epoch: u64,
    stop: &mut watch::Receiver<bool>,
    operation: impl std::future::Future<Output = io::Result<T>>,
) -> io::Result<T> {
    let mut cancelled = account.cancellation();
    let mut clients = account.clients();
    tokio::pin!(operation);
    loop {
        if *stop.borrow() || *cancelled.borrow() != epoch {
            return Err(stale());
        }
        let expires = clients
            .borrow()
            .as_ref()
            .filter(|c| c.authenticated() && c.account_identity() == Some(identity))
            .and_then(|c| c.expires_at())
            .ok_or_else(stale)?;
        tokio::select! {biased;
            _=stop.changed()=>return Err(stale()),_=cancelled.changed()=>return Err(stale()),_=tokio::time::sleep_until(expires)=>return Err(stale()),
            result=&mut operation=>return result,_=clients.changed()=>{},
        }
    }
}
