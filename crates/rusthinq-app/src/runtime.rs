//! Owned L6 session/lifecycle loop and serialized off-thread persistence.
use crate::lifecycle_storage::Storage;
use rusthinq_bridge::devices::{BridgeHandle, Deregistration, Registration};
use rusthinq_lifecycle::{Action, Device, Input, Model, SessionKey, Step};
use rusthinq_server::{Event as TransportEvent, ServerHandle, SessionId};
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Metadata(rusthinq_server::thinq1_http::Metadata),
    GenerationExtended { ceiling: u64 },
    GenerationRefillFailed { reason: String },
    CleanupFailed { reason: String },
    Lifecycle(Action),
    Transport(TransportEvent),
    Lost { transport_events: u64 },
    Rejected { device: String, reason: String },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CleanupStatus {
    Disabled,
    Running,
    Failed(String),
}
type CleanupState = watch::Receiver<(Vec<Device>, bool)>;
type CleanupStart = Box<dyn FnOnce(CleanupState) -> JoinHandle<io::Result<()>> + Send>;
struct AttachCleanup {
    start: CleanupStart,
    result: oneshot::Sender<io::Result<()>>,
}
struct Shared {
    metadata: Mutex<BTreeMap<String, rusthinq_server::thinq1_http::Metadata>>,
    devices: Mutex<Vec<Device>>,
    events: broadcast::Sender<Event>,
    commands: mpsc::Sender<String>,
    attach: mpsc::Sender<AttachCleanup>,
    cleanup_status: watch::Sender<CleanupStatus>,
}
#[derive(Clone)]
pub struct Handle(Arc<Shared>);
impl Handle {
    pub fn metadata_snapshot(&self) -> Vec<rusthinq_server::thinq1_http::Metadata> {
        self.0
            .metadata
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
    /// Supply a fresh session and reopened inventory after cleanup failure.
    /// A running worker is never replaced or aborted by this operation.
    pub async fn attach_retained_cleanup<S>(
        &self,
        session: crate::cleanup_mqtt::Session<S>,
        ledger: crate::retained_cleanup::Ledger,
    ) -> io::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (result, received) = oneshot::channel();
        self.0
            .attach
            .send(AttachCleanup {
                start: cleanup_start(session, ledger),
                result,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "runtime stopped"))?;
        received
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "runtime stopped"))?
    }
    /// Latest health survives observer broadcast loss.
    pub fn cleanup_status(&self) -> watch::Receiver<CleanupStatus> {
        self.0.cleanup_status.subscribe()
    }
    /// Request removal; configured bridge registrations are deregistered first.
    pub fn forget(&self, id: String) -> Result<(), mpsc::error::TrySendError<String>> {
        self.0.commands.try_send(id)
    }
    /// Subscribe before snapshot; receivers must handle Lagged and resnapshot.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.0.events.subscribe()
    }
    pub fn snapshot(&self) -> Vec<Device> {
        self.0
            .devices
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}
enum WriteResult {
    Lifecycle(Input),
    Reservation(Result<crate::lifecycle_storage::GenerationBlock, String>),
}
type WriteTask = JoinHandle<(Storage, WriteResult)>;
#[derive(Clone)]
enum TransportHandle {
    ThinQ1(ServerHandle),
    ThinQ2(rusthinq_server::mqtt::Handle),
}
impl TransportHandle {
    fn subscribe(&self) -> broadcast::Receiver<TransportEvent> {
        match self {
            Self::ThinQ1(handle) => handle.subscribe(),
            Self::ThinQ2(handle) => handle.subscribe(),
        }
    }
    fn snapshot(&self) -> Vec<SessionId> {
        match self {
            Self::ThinQ1(handle) => handle.snapshot(),
            Self::ThinQ2(handle) => handle.snapshot(),
        }
    }
    fn close(&self, session: &SessionId) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) => handle.close(session),
            Self::ThinQ2(handle) => handle.close(session),
        }
    }
    async fn close_and_wait(&self, session: &SessionId) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) => handle.close_and_wait(session).await,
            Self::ThinQ2(handle) => handle.close_and_wait(session).await,
        }
    }
    fn generation_budget(&self) -> (u64, u64) {
        match self {
            Self::ThinQ1(handle) => handle.generation_budget(),
            Self::ThinQ2(handle) => handle.generation_budget(),
        }
    }
    fn extend_generations(&self, floor: u64, ceiling: u64) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) => handle.extend_generations(floor, ceiling),
            Self::ThinQ2(handle) => handle.extend_generations(floor, ceiling),
        }
    }
}
pub struct Runtime {
    scripts: Option<crate::scripts::Owner>,
    model: Model,
    storage: Option<Storage>,
    write: Option<WriteTask>,
    server: TransportHandle,
    transport: broadcast::Receiver<TransportEvent>,
    shared: Arc<Shared>,
    capacity: usize,
    epoch: Instant,
    deadline: Option<Duration>,
    commands: mpsc::Receiver<String>,
    closes: JoinSet<Input>,
    cleanup: Option<JoinHandle<io::Result<()>>>,
    cleanup_state: watch::Sender<(Vec<Device>, bool)>,
    attach: mpsc::Receiver<AttachCleanup>,
    bridge: Option<Bridge>,
    removal_registrations: BTreeMap<String, Registration>,
    refill: Option<(u64, u64)>,
    deferred_storage: Option<Action>,
    metadata: Option<mpsc::Receiver<rusthinq_server::thinq1_http::Metadata>>,
}
struct Bridge {
    handle: BridgeHandle,
    deregistration: Arc<dyn Deregistration>,
    deadline: Duration,
}
impl Runtime {
    /// Build L3 using the saved generation floor, or a durably reserved block's
    /// floor and ceiling, before constructing this owner.
    pub fn new(
        storage: Storage,
        server: ServerHandle,
        grace: Duration,
        event_capacity: usize,
    ) -> io::Result<Self> {
        Self::from_transport(
            storage,
            TransportHandle::ThinQ1(server),
            grace,
            event_capacity,
        )
    }
    /// Local MQTT transport tasks remain owned and joined by the caller/front door.
    pub fn new_mqtt(
        storage: Storage,
        broker: rusthinq_server::mqtt::Handle,
        grace: Duration,
        event_capacity: usize,
    ) -> io::Result<Self> {
        Self::from_transport(
            storage,
            TransportHandle::ThinQ2(broker),
            grace,
            event_capacity,
        )
    }
    fn from_transport(
        storage: Storage,
        server: TransportHandle,
        grace: Duration,
        event_capacity: usize,
    ) -> io::Result<Self> {
        if event_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero app event capacity",
            ));
        }
        let model = Model::new(storage.state().ledger.clone(), grace, Duration::ZERO)
            .map_err(io::Error::other)?;
        let capacity = storage.capacity();
        let transport = server.subscribe();
        let (events, _) = broadcast::channel(event_capacity);
        let (commands, receiver) = mpsc::channel(event_capacity);
        let (cleanup_state, _) = watch::channel((model.devices(), false));
        let (attach, attachments) = mpsc::channel(1);
        let (cleanup_status, _) = watch::channel(CleanupStatus::Disabled);
        let shared = Arc::new(Shared {
            metadata: Mutex::new(BTreeMap::new()),
            devices: Mutex::new(model.devices()),
            events,
            commands,
            attach,
            cleanup_status,
        });
        Ok(Self {
            scripts: None,
            model,
            storage: Some(storage),
            write: None,
            server,
            transport,
            shared,
            capacity,
            epoch: Instant::now(),
            deadline: None,
            commands: receiver,
            closes: JoinSet::new(),
            cleanup: None,
            cleanup_state,
            attach: attachments,
            bridge: None,
            removal_registrations: BTreeMap::new(),
            refill: None,
            deferred_storage: None,
            metadata: None,
        })
    }
    pub fn handle(&self) -> Handle {
        Handle(self.shared.clone())
    }
    /// Own preconfigured workers; lifecycle changes invalidate their session bindings.
    /// Automatic driver selection and transport callback dispatch remain separate.
    pub fn with_scripts(mut self, mut owner: crate::scripts::Owner) -> Self {
        assert!(self.scripts.is_none(), "one script worker owner");
        owner.reconcile(&self.model.devices());
        self.scripts = Some(owner);
        self
    }
    pub fn with_metadata(
        mut self,
        receiver: mpsc::Receiver<rusthinq_server::thinq1_http::Metadata>,
    ) -> Self {
        assert!(self.metadata.is_none(), "one metadata source");
        self.metadata = Some(receiver);
        self
    }
    fn observe_metadata(&self, metadata: rusthinq_server::thinq1_http::Metadata) {
        let mut entries = self
            .shared
            .metadata
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !entries.contains_key(&metadata.device_id) && entries.len() >= self.capacity {
            drop(entries);
            self.emit(Event::Rejected {
                device: metadata.device_id,
                reason: "metadata inventory capacity exceeded".into(),
            });
            return;
        }
        entries.insert(metadata.device_id.clone(), metadata.clone());
        drop(entries);
        self.emit(Event::Metadata(metadata));
    }
    /// Refill a reserved ThinQ1 generation budget on the serialized storage worker.
    pub fn with_generation_refill(mut self, count: u64, low_water: u64) -> io::Result<Self> {
        let (_, ceiling) = self.server.generation_budget();
        if count == 0
            || low_water >= count
            || ceiling
                != self
                    .storage
                    .as_ref()
                    .expect("before run")
                    .state()
                    .generation_floor
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid reserved generation refill",
            ));
        }
        self.refill = Some((count, low_water));
        Ok(self)
    }
    fn write_action(&mut self, action: Action) {
        let Some(mut storage) = self.storage.take() else {
            assert!(self.deferred_storage.is_none(), "one L2 storage effect");
            self.deferred_storage = Some(action);
            return;
        };
        self.write = Some(tokio::task::spawn_blocking(move || {
            let result = storage.execute(&action).expect("storage effect");
            (storage, WriteResult::Lifecycle(result))
        }));
    }
    /// Supply a confirmed-registration inventory and cancellation-safe cloud adapter.
    pub fn with_bridge(
        mut self,
        handle: BridgeHandle,
        deregistration: Arc<dyn Deregistration>,
        deadline: Duration,
    ) -> io::Result<Self> {
        if deadline.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero deregistration deadline",
            ));
        }
        self.bridge = Some(Bridge {
            handle,
            deregistration,
            deadline,
        });
        Ok(self)
    }
    fn forget(&mut self, id: String) {
        if let Some(bridge) = &self.bridge
            && let Some(status) = bridge
                .handle
                .snapshot()
                .into_iter()
                .find(|status| status.registration.device == id)
        {
            if !self.model.devices().iter().any(|device| {
                device.entry.id == id && device.entry.incarnation == status.registration.incarnation
            }) {
                self.emit(Event::Rejected {
                    device: id,
                    reason: "bridge incarnation conflicts with lifecycle".into(),
                });
                return;
            }
            if self
                .removal_registrations
                .get(&id)
                .is_some_and(|captured| *captured != status.registration)
            {
                self.emit(Event::Rejected {
                    device: id,
                    reason: "bridge registration changed during removal".into(),
                });
                return;
            }
            // Disabled registrations still require remote deregistration.
            if let Err(error) = bridge.handle.disable(&status.registration) {
                self.emit(Event::Rejected {
                    device: id,
                    reason: format!("bridge quiesce: {error:?}"),
                });
                return;
            }
            self.removal_registrations
                .entry(id.clone())
                .or_insert(status.registration);
        }
        let bridge_active = self.removal_registrations.contains_key(&id);
        self.input(Input::Forget { id, bridge_active });
    }
    /// Attach an exclusive connected MQTT session and its durable inventory before run.
    /// Startup recovers orphan device owners; failures require explicit reconnect/reopen.
    pub fn with_retained_cleanup<S>(
        mut self,
        session: crate::cleanup_mqtt::Session<S>,
        ledger: crate::retained_cleanup::Ledger,
    ) -> Self
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        assert!(self.cleanup.is_none(), "one retained inventory owner");
        self.cleanup = Some(cleanup_start(session, ledger)(
            self.cleanup_state.subscribe(),
        ));
        self.shared
            .cleanup_status
            .send_replace(CleanupStatus::Running);
        self
    }
    fn emit(&self, event: Event) {
        let _ = self.shared.events.send(event);
    }
    fn input(&mut self, input: Input) {
        let outcome = self.model.input(input, self.epoch.elapsed());
        self.deadline = outcome.next_deadline;
        if let Some(scripts) = &mut self.scripts {
            scripts.reconcile(&self.model.devices());
        }
        *self
            .shared
            .devices
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = self.model.devices();
        self.cleanup_state
            .send_replace((self.model.devices(), false));
        if let Some(error) = outcome.error {
            self.emit(Event::Rejected {
                device: String::new(),
                reason: error.to_string(),
            });
        }
        for action in outcome.actions {
            self.emit(Event::Lifecycle(action.clone()));
            match action {
                Action::Removed { id, .. } => {
                    self.removal_registrations.remove(&id);
                    self.shared
                        .metadata
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&id);
                }
                Action::ForgetStep {
                    id,
                    incarnation,
                    operation,
                    step: Step::Deregister,
                    ..
                } => {
                    let bridge = self.bridge.as_ref().expect("captured bridge registration");
                    let registration = self
                        .removal_registrations
                        .get(&id)
                        .expect("captured registration")
                        .clone();
                    let handle = bridge.handle.clone();
                    let adapter = bridge.deregistration.clone();
                    let deadline = bridge.deadline;
                    self.closes.spawn(async move {
                        let result = match tokio::time::timeout(
                            deadline,
                            adapter.deregister(registration.clone()),
                        )
                        .await
                        {
                            Ok(Ok(())) => handle
                                .deregistered(&registration)
                                .map_err(|error| format!("deregistration confirmation: {error:?}")),
                            Ok(Err(error)) => Err(error.to_string()),
                            Err(_) => {
                                Err("deregistration timed out; remote outcome unknown".into())
                            }
                        };
                        Input::StepResult {
                            id,
                            incarnation,
                            operation,
                            step: Step::Deregister,
                            result,
                        }
                    });
                }
                Action::PersistLedger(_)
                | Action::ForgetStep {
                    step: Step::PersistRemoval,
                    ..
                } => {
                    // L2 permits exactly one storage effect in flight and coalesces updates.
                    self.write_action(action);
                }
                Action::CloseSuperseded { id, session } => {
                    // L3 close is generation-targeted; a stale old target is already superseded.
                    let _ = self.server.close(&SessionId {
                        device: id,
                        generation: session.generation,
                    });
                }
                Action::ForgetStep {
                    id,
                    incarnation,
                    operation,
                    step: Step::Close,
                    session,
                    ..
                } => {
                    let server = self.server.clone();
                    let generation = session.map(|session| session.generation).or_else(|| {
                        self.model
                            .devices()
                            .iter()
                            .find(|device| device.entry.id == id)
                            .map(|device| device.entry.last_generation)
                    });
                    self.closes.spawn(async move {
                        let result = match generation {
                            Some(generation) => server
                                .close_and_wait(&SessionId {
                                    device: id.clone(),
                                    generation,
                                })
                                .await
                                .map_err(|error| format!("close completion: {error:?}")),
                            None => Ok(()),
                        };
                        Input::StepResult {
                            id,
                            incarnation,
                            operation,
                            step: Step::Close,
                            result,
                        }
                    });
                }
                _ => {}
            }
        }
    }
    fn up(&mut self, id: SessionId) {
        let devices = self.model.devices();
        if let Some(device) = devices.iter().find(|device| device.entry.id == id.device) {
            if device.removal.is_some() {
                let _ = self.server.close(&id);
                self.emit(Event::Rejected {
                    device: id.device,
                    reason: "device removal in progress; retry after removal".into(),
                });
                return;
            }
            if device
                .session
                .is_some_and(|session| session.generation == id.generation)
            {
                return;
            }
            if id.generation <= device.entry.last_generation {
                let _ = self.server.close(&id);
                self.emit(Event::Rejected {
                    device: id.device,
                    reason: "stale generation; configure saved generation floor".into(),
                });
                return;
            }
        } else if devices.len() >= self.capacity {
            let _ = self.server.close(&id);
            self.emit(Event::Rejected {
                device: id.device,
                reason: "known device capacity exceeded".into(),
            });
            return;
        }
        self.input(Input::SessionUp {
            id: id.device,
            generation: id.generation,
        });
    }
    fn down(&mut self, id: SessionId) {
        if let Some(device) = self
            .model
            .devices()
            .iter()
            .find(|device| device.entry.id == id.device)
        {
            self.input(Input::SessionDown {
                id: id.device,
                session: SessionKey {
                    incarnation: device.entry.incarnation,
                    generation: id.generation,
                },
            });
        }
    }
    fn reconcile(&mut self) {
        let current = self.server.snapshot();
        for device in self.model.devices() {
            if let Some(session) = device.session
                && !current
                    .iter()
                    .any(|id| id.device == device.entry.id && id.generation == session.generation)
            {
                self.down(SessionId {
                    device: device.entry.id,
                    generation: session.generation,
                });
            }
        }
        for id in current {
            self.up(id);
        }
    }
    /// Shutdown L3 first, then signal this loop; it reconciles final session state
    /// and drains/join all started storage work, including coalesced final updates.
    pub async fn run(mut self, mut stop: watch::Receiver<bool>) -> io::Result<()> {
        self.reconcile();
        let mut stopping = *stop.borrow();
        loop {
            if !stopping
                && self.write.is_none()
                && let Some((count, low_water)) = self.refill
            {
                let (generation, ceiling) = self.server.generation_budget();
                if ceiling.saturating_sub(generation) <= low_water {
                    let mut storage = self.storage.take().expect("idle storage worker");
                    self.write = Some(tokio::task::spawn_blocking(move || {
                        let result = storage
                            .reserve_generations(count)
                            .map_err(|error| error.to_string());
                        (storage, WriteResult::Reservation(result))
                    }));
                }
            }
            if stopping && self.write.is_none() && self.closes.is_empty() {
                if let Some(scripts) = self.scripts.take()
                    && let Err(error) = scripts.shutdown().await
                {
                    self.emit(Event::Rejected {
                        device: String::new(),
                        reason: format!("script worker shutdown: {error:?}"),
                    });
                }
                if let Some(mut receiver) = self.metadata.take() {
                    while let Ok(metadata) = receiver.try_recv() {
                        self.observe_metadata(metadata);
                    }
                }
                self.cleanup_state
                    .send_replace((self.model.devices(), true));
                if self.cleanup.is_none() {
                    return Ok(());
                }
            }
            let deadline = self.deadline.map(|value| self.epoch + value);
            tokio::select! {
                biased;
                result = async {self.cleanup.as_mut().expect("cleanup present").await}, if self.cleanup.is_some() => {
                    self.cleanup = None;
                    let error = match result {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(error.to_string()),
                        Err(error) => Some(error.to_string()),
                    };
                    if let Some(reason) = error {
                        self.shared.cleanup_status.send_replace(CleanupStatus::Failed(reason.clone()));
                        self.emit(Event::CleanupFailed {reason});
                    } else {
                        self.shared.cleanup_status.send_replace(CleanupStatus::Disabled);
                    }
                }
                _ = stop.changed(), if !stopping => {
                    if *stop.borrow() || stop.has_changed().is_err() {stopping = true; self.reconcile();}
                }
                Some(attachment) = self.attach.recv(), if !stopping => {
                    if self.cleanup.is_some() {
                        let _ = attachment.result.send(Err(io::Error::new(io::ErrorKind::AlreadyExists, "cleanup worker already running")));
                    } else if !attachment.result.is_closed() {
                        self.cleanup = Some((attachment.start)(self.cleanup_state.subscribe()));
                        self.shared.cleanup_status.send_replace(CleanupStatus::Running);
                        let _ = attachment.result.send(Ok(()));
                    }
                }
                result = async {self.write.as_mut().expect("write present").await}, if self.write.is_some() => {
                    self.write = None;
                    let (storage,result) = result.map_err(io::Error::other)?;
                    self.storage = Some(storage);
                    match result {
                        WriteResult::Lifecycle(input) => self.input(input),
                        WriteResult::Reservation(result) => {
                            let result = result.and_then(|block| self.server.extend_generations(block.floor, block.ceiling).map(|()| block.ceiling).map_err(|error| format!("generation extension: {error:?}")));
                            if let Err(reason) = result {
                                self.refill = None;
                                self.emit(Event::GenerationRefillFailed {reason});
                            } else if let Ok(ceiling) = result {
                                self.emit(Event::GenerationExtended {ceiling});
                            }
                            if let Some(action) = self.deferred_storage.take() {self.write_action(action);}
                        }
                    }
                }
                result = self.closes.join_next(), if !self.closes.is_empty() => {
                    self.input(result.expect("close task present").map_err(io::Error::other)?);
                }
                Some(id) = self.commands.recv(), if !stopping => {
                    self.forget(id);
                }
                metadata = async {self.metadata.as_mut().expect("metadata source").recv().await}, if !stopping && self.metadata.is_some() => {
                    match metadata {Some(metadata)=>self.observe_metadata(metadata),None=>self.metadata=None}
                }
                _ = tokio::time::sleep(Duration::from_millis(100)), if !stopping && self.refill.is_some() => {},
                event = self.transport.recv(), if !stopping => match event {
                    Ok(TransportEvent::Up(id)) => self.up(id),
                    Ok(TransportEvent::Down(id,_)) => self.down(id),
                    Ok(event) => {
                        let session = match &event {
                            TransportEvent::Data(id,_) | TransportEvent::Response(id,_) |
                            TransportEvent::Ready(id,_) | TransportEvent::CloudBound(id,_) => Some(id),
                            TransportEvent::Will {session,..} => session.as_ref(),
                            _ => None,
                        };
                        if session.is_none_or(|id| self.model.devices().iter().any(|device| device.entry.id == id.device && device.session.is_some_and(|session| session.generation == id.generation))) {
                            self.emit(Event::Transport(event));
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        self.emit(Event::Lost {transport_events:count});
                        self.reconcile();
                    }
                    Err(broadcast::error::RecvError::Closed) => {stopping = true; self.reconcile();}
                },
                _ = async {
                    match deadline {Some(deadline) => tokio::time::sleep_until(deadline).await,None => std::future::pending().await}
                }, if !stopping => self.input(Input::Tick),
            }
        }
    }
}

fn cleanup_start<S>(
    session: crate::cleanup_mqtt::Session<S>,
    ledger: crate::retained_cleanup::Ledger,
) -> CleanupStart
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    Box::new(move |state| tokio::spawn(crate::lifecycle_cleanup::run(session, ledger, state)))
}
