//! Owned L6 session/lifecycle loop and serialized off-thread persistence.
use crate::lifecycle_storage::Storage;
use rusthinq_bridge::devices::{BridgeHandle, Deregistration, Registration};
use rusthinq_lifecycle::{Action, Device, Input, Model, SessionKey, Step};
use rusthinq_server::{Event as TransportEvent, ServerHandle, SessionId};
use std::{
    collections::{BTreeMap, VecDeque},
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
    Injected {
        session: SessionId,
        data: Vec<u8>,
        to_device: bool,
    },
    ScriptOutput {
        context: crate::scripts::Context,
        payload: String,
    },
    ScriptExecuted {
        sequence: u64,
        context: crate::scripts::Context,
        error: Option<String>,
    },
    ScriptDelivery {
        context: crate::scripts::Context,
        delivery: rusthinq_server::Delivery,
    },
    Metadata(rusthinq_server::thinq1_http::Metadata),
    GenerationExtended {
        ceiling: u64,
    },
    GenerationRefillFailed {
        reason: String,
    },
    CleanupFailed {
        reason: String,
    },
    Lifecycle(Action),
    Transport(TransportEvent),
    Lost {
        transport_events: u64,
    },
    Rejected {
        device: String,
        reason: String,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CleanupStatus {
    Disabled,
    Running,
    Failed(String),
}
type CleanupState = watch::Receiver<(Vec<Device>, bool)>;
type CleanupStart = Box<dyn FnOnce(CleanupState) -> JoinHandle<io::Result<()>> + Send>;
struct ApplicationSink(
    broadcast::Sender<Event>,
    Option<crate::external_mqtt::Handle>,
);
impl crate::scripts::PublishSink for ApplicationSink {
    fn try_publish(
        &self,
        context: &crate::scripts::Context,
        payload: String,
    ) -> Result<(), String> {
        if let Some(external) = &self.1 {
            external.try_publish(context, payload.clone())?;
        }
        let _ = self.0.send(Event::ScriptOutput {
            context: context.clone(),
            payload,
        });
        Ok(())
    }
}
struct AttachCleanup {
    start: CleanupStart,
    result: oneshot::Sender<io::Result<()>>,
}
struct Shared {
    external_mqtt: Mutex<Option<crate::external_mqtt::Handle>>,
    durable_devices: Mutex<Vec<rusthinq_lifecycle::Entry>>,
    message_id: std::sync::atomic::AtomicU64,
    driver_models: Mutex<BTreeMap<String, (SessionKey, String, bool)>>,
    script_states: Mutex<BTreeMap<String, (SessionKey, u64, bool)>>,
    scripts: mpsc::Sender<ScriptCommand>,
    metadata: Mutex<BTreeMap<String, rusthinq_server::thinq1_http::Metadata>>,
    devices: Mutex<Vec<Device>>,
    events: broadcast::Sender<Event>,
    commands: mpsc::Sender<ManagementCommand>,
    attach: mpsc::Sender<AttachCleanup>,
    cleanup_status: watch::Sender<CleanupStatus>,
}
#[derive(Clone)]
pub struct Handle(Arc<Shared>);
impl Handle {
    pub fn external_mqtt(&self) -> Option<crate::external_mqtt::Handle> {
        self.0
            .external_mqtt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn durable_devices(&self) -> Vec<rusthinq_lifecycle::Entry> {
        self.0
            .durable_devices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub async fn inject(
        &self,
        id: String,
        session: SessionKey,
        data: Vec<u8>,
        to_device: bool,
    ) -> Result<Option<rusthinq_server::Receipt>, rusthinq_server::Reject> {
        if data.len() > 1_000_000 {
            return Err(rusthinq_server::Reject::PayloadExceeded);
        }
        let (result, received) = oneshot::channel();
        self.management(ManagementCommand::Inject {
            id,
            session,
            data,
            to_device,
            result,
        })?;
        received
            .await
            .unwrap_or(Err(rusthinq_server::Reject::Stopped))
    }
    pub fn driver_models(&self) -> BTreeMap<String, (SessionKey, String, bool)> {
        self.0
            .driver_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn script_states(&self) -> BTreeMap<String, (SessionKey, u64, bool)> {
        self.0
            .script_states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// Compile before admission; the captured incarnation/session must still be current.
    pub async fn attach_script(
        &self,
        device: String,
        session: SessionKey,
        compiled: rusthinq_scripting::Compiled,
        config: rusthinq_scripting::worker::Config,
        callbacks: crate::scripts::Callbacks,
    ) -> Result<(), rusthinq_scripting::Error> {
        if [
            &callbacks.response,
            &callbacks.data,
            &callbacks.ready,
            &callbacks.timer,
        ]
        .iter()
        .any(|name| {
            name.as_ref()
                .is_some_and(|name| name.is_empty() || name.len() > 256)
        }) {
            return Err(rusthinq_scripting::Error::InvalidConfig);
        }
        let (result, received) = oneshot::channel();
        self.0
            .scripts
            .try_send(ScriptCommand::Attach(Box::new(AttachScript {
                device,
                session,
                compiled,
                config,
                callbacks,
                result,
            })))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_scripting::Error::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_scripting::Error::Stopped,
            })?;
        received
            .await
            .map_err(|_| rusthinq_scripting::Error::Stopped)?
    }
    pub fn metadata_snapshot(&self) -> Vec<rusthinq_server::thinq1_http::Metadata> {
        self.0
            .metadata
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
    /// Returns ordered execution admission; ScriptExecuted reports the eventual outcome.
    pub async fn invoke_script(
        &self,
        device: String,
        session: SessionKey,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<u64, rusthinq_scripting::Error> {
        if input.len() > 1_000_000 || function.is_empty() || function.len() > 256 {
            return Err(rusthinq_scripting::Error::InputExceeded);
        }
        let (result, received) = oneshot::channel();
        self.0
            .scripts
            .try_send(ScriptCommand::Invoke {
                device,
                session,
                generation,
                function,
                input,
                result,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_scripting::Error::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_scripting::Error::Stopped,
            })?;
        received
            .await
            .map_err(|_| rusthinq_scripting::Error::Stopped)?
    }
    /// Supply a successfully compiled replacement; admission fences session and generation.
    /// Callback names/encoding are kept. A lost reply must not trigger automatic retry.
    pub async fn reload_script(
        &self,
        device: String,
        session: SessionKey,
        generation: u64,
        compiled: rusthinq_scripting::Compiled,
    ) -> Result<u64, rusthinq_scripting::Error> {
        let (result, received) = oneshot::channel();
        self.0
            .scripts
            .try_send(ScriptCommand::Reload(Box::new(ReloadScript {
                device,
                session,
                generation,
                compiled,
                result,
            })))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_scripting::Error::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_scripting::Error::Stopped,
            })?;
        received
            .await
            .map_err(|_| rusthinq_scripting::Error::Stopped)?
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
        self.0
            .commands
            .try_send(ManagementCommand::Forget(id))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(ManagementCommand::Forget(id)) => {
                    mpsc::error::TrySendError::Full(id)
                }
                mpsc::error::TrySendError::Closed(ManagementCommand::Forget(id)) => {
                    mpsc::error::TrySendError::Closed(id)
                }
                _ => unreachable!("submitted forget command"),
            })
    }
    /// Admission only; durable removal success/failure is observed in lifecycle events.
    pub async fn forget_scoped(
        &self,
        id: String,
        incarnation: u64,
    ) -> Result<(), rusthinq_server::Reject> {
        let (result, received) = oneshot::channel();
        self.management(ManagementCommand::ForgetScoped {
            id,
            incarnation,
            result,
        })?;
        received
            .await
            .unwrap_or(Err(rusthinq_server::Reject::Stopped))
    }
    /// Raw JSON transport send, fenced again by the lifecycle actor and L3 registry.
    pub async fn send(
        &self,
        id: String,
        session: SessionKey,
        payload: Vec<u8>,
    ) -> Result<rusthinq_server::Receipt, rusthinq_server::Reject> {
        if payload.len() > 1_000_000 {
            return Err(rusthinq_server::Reject::PayloadExceeded);
        }
        let (result, received) = oneshot::channel();
        self.management(ManagementCommand::Send {
            id,
            session,
            payload,
            result,
        })?;
        received
            .await
            .unwrap_or(Err(rusthinq_server::Reject::Stopped))
    }
    fn management(&self, command: ManagementCommand) -> Result<(), rusthinq_server::Reject> {
        self.0
            .commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_server::Reject::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_server::Reject::Stopped,
            })
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
enum ManagementCommand {
    Inject {
        id: String,
        session: SessionKey,
        data: Vec<u8>,
        to_device: bool,
        result: oneshot::Sender<Result<Option<rusthinq_server::Receipt>, rusthinq_server::Reject>>,
    },
    Forget(String),
    ForgetScoped {
        id: String,
        incarnation: u64,
        result: oneshot::Sender<Result<(), rusthinq_server::Reject>>,
    },
    Send {
        id: String,
        session: SessionKey,
        payload: Vec<u8>,
        result: oneshot::Sender<Result<rusthinq_server::Receipt, rusthinq_server::Reject>>,
    },
}
impl ManagementCommand {
    fn cancel(self) {
        match self {
            Self::Forget(_) => {}
            Self::ForgetScoped { result, .. } => {
                let _ = result.send(Err(rusthinq_server::Reject::Stopped));
            }
            Self::Send { result, .. } => {
                let _ = result.send(Err(rusthinq_server::Reject::Stopped));
            }
            Self::Inject { result, .. } => {
                let _ = result.send(Err(rusthinq_server::Reject::Stopped));
            }
        }
    }
}
enum WriteResult {
    Lifecycle(Input),
    Reservation(Result<crate::lifecycle_storage::GenerationBlock, String>),
}
struct AttachScript {
    device: String,
    session: SessionKey,
    compiled: rusthinq_scripting::Compiled,
    config: rusthinq_scripting::worker::Config,
    callbacks: crate::scripts::Callbacks,
    result: oneshot::Sender<Result<(), rusthinq_scripting::Error>>,
}
struct ReloadScript {
    device: String,
    session: SessionKey,
    generation: u64,
    compiled: rusthinq_scripting::Compiled,
    result: oneshot::Sender<Result<u64, rusthinq_scripting::Error>>,
}
enum ScriptCommand {
    Attach(Box<AttachScript>),
    Reload(Box<ReloadScript>),
    Invoke {
        device: String,
        session: SessionKey,
        generation: u64,
        function: String,
        input: String,
        result: oneshot::Sender<Result<u64, rusthinq_scripting::Error>>,
    },
}
impl ScriptCommand {
    fn cancel(self) {
        match self {
            Self::Attach(request) => {
                let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
            }
            Self::Reload(request) => {
                let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
            }
            Self::Invoke { result, .. } => {
                let _ = result.send(Err(rusthinq_scripting::Error::Stopped));
            }
        }
    }
}
type WriteTask = JoinHandle<(Storage, WriteResult)>;
#[derive(Clone)]
enum TransportHandle {
    ThinQ1(ServerHandle),
    ThinQ2(rusthinq_server::mqtt::Handle),
    Mixed {
        server: ServerHandle,
        broker: rusthinq_server::mqtt::Handle,
    },
}
impl TransportHandle {
    fn protocol(
        &self,
        id: &SessionId,
    ) -> Result<rusthinq_server::Protocol, rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(server) | Self::Mixed { server, .. } => server.protocol(id),
            Self::ThinQ2(broker) => {
                if broker.snapshot().contains(id) {
                    Ok(rusthinq_server::Protocol::ThinQ2)
                } else {
                    Err(rusthinq_server::Reject::StaleSession)
                }
            }
        }
    }
    fn send(
        &self,
        session: &SessionId,
        payload: &[u8],
    ) -> Result<rusthinq_server::Receipt, rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) => handle.send(session, payload),
            Self::ThinQ2(handle) => handle.send(session, payload),
            Self::Mixed { server, broker } => match server.protocol(session)? {
                rusthinq_server::Protocol::ThinQ1 => server.send(session, payload),
                rusthinq_server::Protocol::ThinQ2 => broker.send(session, payload),
            },
        }
    }
    fn subscribe(&self) -> broadcast::Receiver<TransportEvent> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.subscribe(),
            Self::ThinQ2(handle) => handle.subscribe(),
        }
    }
    fn snapshot(&self) -> Vec<SessionId> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.snapshot(),
            Self::ThinQ2(handle) => handle.snapshot(),
        }
    }
    fn close(&self, session: &SessionId) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.close(session),
            Self::ThinQ2(handle) => handle.close(session),
        }
    }
    async fn close_and_wait(&self, session: &SessionId) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => {
                handle.close_and_wait(session).await
            }
            Self::ThinQ2(handle) => handle.close_and_wait(session).await,
        }
    }
    fn generation_budget(&self) -> (u64, u64) {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.generation_budget(),
            Self::ThinQ2(handle) => handle.generation_budget(),
        }
    }
    fn extend_generations(&self, floor: u64, ceiling: u64) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => {
                handle.extend_generations(floor, ceiling)
            }
            Self::ThinQ2(handle) => handle.extend_generations(floor, ceiling),
        }
    }
}
pub struct Runtime {
    drivers: Option<crate::drivers::Config>,
    driver_preparation: JoinSet<(
        String,
        SessionKey,
        String,
        Result<rusthinq_scripting::Compiled, rusthinq_scripting::Error>,
    )>,
    driver_attempts: BTreeMap<String, (SessionKey, String)>,
    driver_buffers: BTreeMap<String, (SessionKey, VecDeque<Vec<u8>>)>,
    script_timers: BTreeMap<(String, String), (crate::scripts::Context, Instant)>,
    script_attach: mpsc::Receiver<ScriptCommand>,
    script_callbacks: BTreeMap<String, crate::scripts::Callbacks>,
    script_results: JoinSet<(
        u64,
        String,
        Result<crate::scripts::Completion, rusthinq_scripting::Error>,
    )>,
    script_sequence: u64,
    script_order: BTreeMap<String, VecDeque<u64>>,
    script_buffer: BTreeMap<
        u64,
        (
            String,
            Result<crate::scripts::Completion, rusthinq_scripting::Error>,
        ),
    >,
    script_deliveries: JoinSet<(crate::scripts::Context, rusthinq_server::Delivery)>,
    script_limit: usize,
    script_sink: Option<Arc<dyn crate::scripts::PublishSink>>,
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
    commands: mpsc::Receiver<ManagementCommand>,
    closes: JoinSet<Input>,
    cleanup: Option<JoinHandle<io::Result<()>>>,
    cleanup_state: watch::Sender<(Vec<Device>, bool)>,
    attach: mpsc::Receiver<AttachCleanup>,
    bridge: Option<Bridge>,
    firmware: Option<rusthinq_bridge::passthrough::Relay>,
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
    /// Both handles must share the exact L3 registry; outbound codec follows captured session.
    pub fn new_mixed(
        storage: Storage,
        server: ServerHandle,
        broker: rusthinq_server::mqtt::Handle,
        grace: Duration,
        event_capacity: usize,
    ) -> io::Result<Self> {
        if !broker.shares_registry(&server) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mixed transports must share one registry",
            ));
        }
        Self::from_transport(
            storage,
            TransportHandle::Mixed { server, broker },
            grace,
            event_capacity,
        )
    }
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
        let (script_sender, script_attach) = mpsc::channel(1);
        let (cleanup_state, _) = watch::channel((model.devices(), false));
        let (attach, attachments) = mpsc::channel(1);
        let (cleanup_status, _) = watch::channel(CleanupStatus::Disabled);
        let shared = Arc::new(Shared {
            durable_devices: Mutex::new(storage.state().ledger.entries.clone()),
            message_id: std::sync::atomic::AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(io::Error::other)?
                    .as_millis()
                    .try_into()
                    .map_err(io::Error::other)?,
            ),
            driver_models: Mutex::new(BTreeMap::new()),
            external_mqtt: Mutex::new(None),
            script_states: Mutex::new(BTreeMap::new()),
            scripts: script_sender,
            metadata: Mutex::new(BTreeMap::new()),
            devices: Mutex::new(model.devices()),
            events,
            commands,
            attach,
            cleanup_status,
        });
        Ok(Self {
            drivers: None,
            driver_preparation: JoinSet::new(),
            driver_attempts: BTreeMap::new(),
            driver_buffers: BTreeMap::new(),
            script_timers: BTreeMap::new(),
            script_attach,
            script_callbacks: BTreeMap::new(),
            script_results: JoinSet::new(),
            script_sequence: 0,
            script_order: BTreeMap::new(),
            script_buffer: BTreeMap::new(),
            script_deliveries: JoinSet::new(),
            script_limit: event_capacity,
            script_sink: None,
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
            firmware: None,
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
    pub fn with_script_sink(mut self, sink: Arc<dyn crate::scripts::PublishSink>) -> Self {
        assert!(self.script_sink.is_none(), "one script publication sink");
        self.script_sink = Some(sink);
        self
    }
    pub fn with_external_mqtt(mut self, handle: crate::external_mqtt::Handle) -> Self {
        *self
            .shared
            .external_mqtt
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(handle.clone());
        self.script_sink = Some(Arc::new(ApplicationSink(
            self.shared.events.clone(),
            Some(handle),
        )));
        self
    }
    pub fn with_drivers(mut self, config: crate::drivers::Config) -> io::Result<Self> {
        if self.scripts.is_none() {
            self.scripts = Some(
                crate::scripts::Owner::new(self.capacity)
                    .map_err(|e| io::Error::other(format!("{e:?}")))?,
            );
        }
        if self.script_sink.is_none() {
            self.script_sink = Some(Arc::new(ApplicationSink(self.shared.events.clone(), None)));
        }
        self.drivers = Some(config);
        Ok(self)
    }
    /// Share the exact relay used by the TLS front door. Only completed current
    /// provisioning contributes local proof; pending device packets never do.
    pub fn with_firmware(mut self, relay: rusthinq_bridge::passthrough::Relay) -> Self {
        self.firmware = Some(relay);
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
    fn observe_metadata(&mut self, metadata: rusthinq_server::thinq1_http::Metadata) {
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
        let id = metadata.device_id.clone();
        if let Some(session) = self
            .model
            .devices()
            .iter()
            .find(|d| d.entry.id == id)
            .and_then(|d| d.session)
        {
            self.shared
                .driver_models
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id.clone(), (session, metadata.model_name.clone(), false));
        }
        self.emit(Event::Metadata(metadata));
        self.prepare_driver(&id);
    }
    fn prepare_driver(&mut self, id: &str) {
        let Some(config) = self.drivers.as_ref() else {
            return;
        };
        if self.driver_preparation.len() >= 2
            || self
                .scripts
                .as_ref()
                .and_then(|owner| owner.generation(id))
                .is_some()
        {
            return;
        }
        let Some(device) = self
            .model
            .devices()
            .into_iter()
            .find(|d| d.entry.id == id && d.online && d.removal.is_none())
        else {
            return;
        };
        let Some(session) = device.session else {
            return;
        };
        let model = self
            .shared
            .driver_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .filter(|(current, _, _)| *current == session)
            .map(|(_, model, t2)| (model.clone(), *t2));
        let (model, thinq2) = if let Some((model, thinq2)) = model {
            (model, thinq2)
        } else if let Some(model) = config.bindings.get(id) {
            let thinq2 = match &self.server {
                TransportHandle::ThinQ1(_) => false,
                TransportHandle::ThinQ2(_) => true,
                TransportHandle::Mixed { server, .. } => {
                    server.protocol(&SessionId {
                        device: id.into(),
                        generation: session.generation,
                    }) == Ok(rusthinq_server::Protocol::ThinQ2)
                }
            };
            (model.clone(), thinq2)
        } else {
            return;
        };
        if self.driver_attempts.get(id) == Some(&(session, model.clone())) {
            return;
        }
        self.driver_attempts
            .insert(id.into(), (session, model.clone()));
        self.shared
            .driver_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.into(), (session, model.clone(), thinq2));
        let config = config.clone();
        let id = id.to_string();
        self.driver_preparation.spawn_blocking(move || {
            let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                config.prepare(&id, &model, thinq2, true)
            }))
            .unwrap_or_else(|_| {
                Err(rusthinq_scripting::Error::Compile(
                    "driver preparation panic".into(),
                ))
            });
            (id, session, model, prepared)
        });
    }
    fn buffer_driver(&mut self, id: &SessionId, data: &[u8]) {
        if self.drivers.is_none()
            || self
                .scripts
                .as_ref()
                .and_then(|owner| owner.generation(&id.device))
                .is_some()
        {
            return;
        }
        let Some(session) = self
            .model
            .devices()
            .iter()
            .find(|d| d.entry.id == id.device)
            .and_then(|d| d.session)
        else {
            return;
        };
        let total: usize = self
            .driver_buffers
            .values()
            .flat_map(|(_, queue)| queue.iter())
            .map(Vec::len)
            .sum();
        if data.len() > 65536 || total.saturating_add(data.len()) > 1_048_576 {
            self.script_rejected(
                id.device.clone(),
                "driver input preparation budget exceeded",
            );
            return;
        }
        let buffered = self
            .driver_buffers
            .entry(id.device.clone())
            .or_insert_with(|| (session, VecDeque::new()));
        if buffered.0 != session {
            *buffered = (session, VecDeque::new());
        }
        if buffered.1.len() >= 16 {
            self.script_rejected(id.device.clone(), "driver input preparation queue exceeded");
            return;
        }
        buffered.1.push_back(data.to_vec());
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
    fn script_rejected(&self, device: String, reason: impl std::fmt::Debug) {
        self.emit(Event::Rejected {
            device,
            reason: format!("script: {reason:?}"),
        });
    }
    fn script_transport(&mut self, event: &TransportEvent) {
        let (id, function, input) = match event {
            TransportEvent::Response(id, body) => (
                id,
                self.script_callbacks
                    .get(&id.device)
                    .and_then(|c| c.response.clone()),
                Ok(body.to_string()),
            ),
            TransportEvent::Ready(id, body) => (
                id,
                self.script_callbacks
                    .get(&id.device)
                    .and_then(|c| c.ready.clone()),
                Ok(body.to_string()),
            ),
            TransportEvent::Data(id, data) => (
                id,
                self.script_callbacks
                    .get(&id.device)
                    .and_then(|c| c.data.clone()),
                self.script_callbacks.get(&id.device).map_or_else(
                    || Ok(String::new()),
                    |callbacks| callbacks.data_encoding.encode(data),
                ),
            ),
            _ => return,
        };
        let Some(function) = function else {
            return;
        };
        let Some(owner) = self.scripts.as_mut() else {
            return;
        };
        let Some(generation) = owner.generation(&id.device) else {
            return;
        };
        if self.script_results.len() + self.script_buffer.len() >= self.script_limit {
            self.script_rejected(id.device.clone(), "callback capacity exceeded");
            return;
        }
        let input = match input {
            Ok(input) => input,
            Err(error) => {
                self.script_rejected(id.device.clone(), error);
                return;
            }
        };
        let Some(sequence) = self.script_sequence.checked_add(1) else {
            self.script_rejected(id.device.clone(), "callback sequence exhausted");
            return;
        };
        match owner.invoke(
            &self.model.devices(),
            &id.device,
            generation,
            function,
            input,
        ) {
            Ok(call) => {
                let device = id.device.clone();
                self.script_sequence = sequence;
                self.script_order
                    .entry(device.clone())
                    .or_default()
                    .push_back(sequence);
                self.script_results
                    .spawn(async move { (sequence, device, call.wait().await) });
            }
            Err(error) => self.script_rejected(id.device.clone(), error),
        }
    }
    fn invoke_script(
        &mut self,
        device: String,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<u64, rusthinq_scripting::Error> {
        let owner = self
            .scripts
            .as_mut()
            .ok_or(rusthinq_scripting::Error::InvalidConfig)?;
        if self.script_results.len() + self.script_buffer.len() >= self.script_limit {
            return Err(rusthinq_scripting::Error::Busy);
        }
        let sequence = self
            .script_sequence
            .checked_add(1)
            .ok_or(rusthinq_scripting::Error::GenerationExhausted)?;
        let call = owner.invoke(&self.model.devices(), &device, generation, function, input)?;
        self.script_sequence = sequence;
        self.script_order
            .entry(device.clone())
            .or_default()
            .push_back(sequence);
        self.script_results
            .spawn(async move { (sequence, device, call.wait().await) });
        Ok(sequence)
    }
    fn fire_timers(&mut self) {
        let due: Vec<_> = self
            .script_timers
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= Instant::now())
            .map(|(key, _)| key.clone())
            .collect();
        for (device, name) in due {
            let (context, _) = self
                .script_timers
                .remove(&(device.clone(), name.clone()))
                .expect("due timer");
            if !self.model.devices().iter().any(|d| {
                d.entry.id == device
                    && d.session == Some(context.session)
                    && d.online
                    && d.removal.is_none()
            }) {
                continue;
            }
            if self
                .scripts
                .as_ref()
                .and_then(|owner| owner.generation(&device))
                != Some(context.generation)
            {
                continue;
            }
            if let Some(function) = self
                .script_callbacks
                .get(&device)
                .and_then(|c| c.timer.clone())
                && let Err(error) =
                    self.invoke_script(device.clone(), context.generation, function, name)
            {
                self.script_rejected(device, error);
            }
        }
    }
    fn script_result(
        &mut self,
        sequence: u64,
        device: String,
        completion: Result<crate::scripts::Completion, rusthinq_scripting::Error>,
        dispatch: bool,
    ) {
        self.script_buffer
            .insert(sequence, (device.clone(), completion));
        while let Some(next) = self
            .script_order
            .get(&device)
            .and_then(|queue| queue.front())
            .copied()
        {
            let Some((id, completion)) = self.script_buffer.remove(&next) else {
                break;
            };
            self.script_order
                .get_mut(&device)
                .expect("callback queue")
                .pop_front();
            if dispatch {
                self.script_complete(next, id, completion);
            } else {
                self.script_rejected(id, rusthinq_scripting::Error::Stopped);
            }
        }
        if self
            .script_order
            .get(&device)
            .is_some_and(|queue| queue.is_empty())
        {
            self.script_order.remove(&device);
        }
    }
    fn script_complete(
        &mut self,
        sequence: u64,
        device: String,
        completion: Result<crate::scripts::Completion, rusthinq_scripting::Error>,
    ) {
        // L3 may already have closed/replaced a session before its broadcast is consumed.
        self.reconcile();
        let completion = match completion {
            Ok(completion) => completion,
            Err(error) => {
                self.script_rejected(device, error);
                return;
            }
        };
        let context = completion.context();
        let Some(owner) = self.scripts.as_mut() else {
            return;
        };
        let outcome = match owner.accept(&self.model.devices(), completion) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.script_rejected(device, error);
                return;
            }
        };
        self.emit(Event::ScriptExecuted {
            sequence,
            context: context.clone(),
            error: outcome.error.as_ref().map(|e| format!("{e:?}")),
        });
        self.shared
            .script_states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                device.clone(),
                (context.session, context.generation, outcome.error.is_some()),
            );
        for output in outcome.outputs {
            let result = match output {
                rusthinq_scripting::Output::Publish(payload) => self
                    .script_sink
                    .as_ref()
                    .ok_or_else(|| "publication sink disabled".to_string())
                    .and_then(|sink| sink.try_publish(&context, payload)),
                rusthinq_scripting::Output::Send(payload) => {
                    if self.script_deliveries.len() >= self.script_limit {
                        Err("delivery capacity exceeded".into())
                    } else {
                        match self.server.send(
                            &SessionId {
                                device: device.clone(),
                                generation: context.session.generation,
                            },
                            payload.as_bytes(),
                        ) {
                            Ok(receipt) => {
                                let context = context.clone();
                                self.script_deliveries
                                    .spawn(async move { (context, receipt.wait().await) });
                                Ok(())
                            }
                            Err(error) => Err(format!("send: {error:?}")),
                        }
                    }
                }
                rusthinq_scripting::Output::Timer { name, after_ms } => {
                    let key = (device.clone(), name);
                    if let Some(after_ms) = after_ms {
                        if self
                            .script_callbacks
                            .get(&device)
                            .and_then(|c| c.timer.as_ref())
                            .is_none()
                        {
                            Err("timer callback disabled".into())
                        } else if !self.script_timers.contains_key(&key)
                            && self
                                .script_timers
                                .keys()
                                .filter(|(id, _)| id == &device)
                                .count()
                                >= 64
                        {
                            Err("timer capacity exceeded".into())
                        } else if let Some(deadline) =
                            Instant::now().checked_add(Duration::from_millis(after_ms))
                        {
                            self.script_timers.insert(key, (context.clone(), deadline));
                            Ok(())
                        } else {
                            Err("timer deadline exceeded".into())
                        }
                    } else {
                        self.script_timers.remove(&key);
                        Ok(())
                    }
                }
            };
            if let Err(reason) = result {
                self.script_rejected(device.clone(), reason);
                break;
            }
        }
        if let Some(error) = outcome.error {
            self.script_rejected(device, error);
        }
    }
    fn input(&mut self, input: Input) {
        let outcome = self.model.input(input, self.epoch.elapsed());
        self.deadline = outcome.next_deadline;
        if let Some(scripts) = &mut self.scripts {
            scripts.reconcile(&self.model.devices());
        }
        let devices = self.model.devices();
        self.script_timers.retain(|_, (context, _)| {
            devices.iter().any(|device| {
                device.entry.id == context.device
                    && device.session == Some(context.session)
                    && device.online
                    && device.removal.is_none()
            })
        });
        self.driver_buffers.retain(|id, (session, _)| {
            devices
                .iter()
                .any(|d| d.entry.id == *id && d.session == Some(*session) && d.removal.is_none())
        });
        self.driver_attempts.retain(|id, (session, _)| {
            devices
                .iter()
                .any(|d| d.entry.id == *id && d.session == Some(*session) && d.removal.is_none())
        });
        self.shared
            .script_states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|id, (session, _, _)| {
                devices.iter().any(|d| {
                    d.entry.id == *id && d.session == Some(*session) && d.removal.is_none()
                })
            });
        self.shared
            .driver_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|id, _| devices.iter().any(|d| d.entry.id == *id));
        self.script_callbacks.retain(|id, _| {
            self.model.devices().iter().any(|device| {
                &device.entry.id == id && device.session.is_some() && device.removal.is_none()
            })
        });
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
            id: id.device.clone(),
            generation: id.generation,
        });
        if let Some(metadata) = self
            .handle()
            .metadata_snapshot()
            .into_iter()
            .find(|metadata| metadata.device_id == id.device)
            && let Some(session) = self
                .model
                .devices()
                .iter()
                .find(|d| d.entry.id == id.device)
                .and_then(|d| d.session)
        {
            self.shared
                .driver_models
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id.device.clone(), (session, metadata.model_name, false));
        }
        self.prepare_driver(&id.device);
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
            if !stopping && (*stop.borrow() || stop.has_changed().is_err()) {
                stopping = true;
                self.reconcile();
            }
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
                while let Some(result) = self.driver_preparation.join_next().await {
                    let _ = result.map_err(io::Error::other)?;
                }
                if let Some(scripts) = self.scripts.take()
                    && let Err(error) = scripts.shutdown().await
                {
                    self.emit(Event::Rejected {
                        device: String::new(),
                        reason: format!("script worker shutdown: {error:?}"),
                    });
                }
                self.script_attach.close();
                self.commands.close();
                while let Ok(command) = self.commands.try_recv() {
                    command.cancel();
                }
                while let Ok(request) = self.script_attach.try_recv() {
                    request.cancel();
                }
                while let Some(result) = self.script_results.join_next().await {
                    let (sequence, device, completion) = result.map_err(io::Error::other)?;
                    self.script_result(sequence, device, completion, false);
                }
                while let Some(result) = self.script_deliveries.join_next().await {
                    let (context, delivery) = result.map_err(io::Error::other)?;
                    self.emit(Event::ScriptDelivery { context, delivery });
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
            let deadline = self
                .deadline
                .map(|value| self.epoch + value)
                .into_iter()
                .chain(self.script_timers.values().map(|(_, deadline)| *deadline))
                .min();
            tokio::select! {
                biased;
                Some(result)=self.driver_preparation.join_next(), if !self.driver_preparation.is_empty() && !stopping=>{
                    let (id,session,model,prepared)=result.map_err(io::Error::other)?;
                    self.reconcile();
                    let current=self.model.devices().iter().any(|d|d.entry.id==id && d.session==Some(session) && d.online && d.removal.is_none()) && self.driver_attempts.get(&id)==Some(&(session,model));
                    if current {
                        let result=match prepared {
                            Err(error)=>Err(error),
                            Ok(mut compiled)=>{
                                compiled.set_consumer_enabled(self.script_sink.is_some());
                                let reaped=self.scripts.as_mut().expect("driver owner").reap().await;
                                self.reconcile();
                                let current=self.model.devices().iter().any(|d|d.entry.id==id && d.session==Some(session) && d.online && d.removal.is_none());
                                if current && !*stop.borrow() {reaped.and_then(|()|self.scripts.as_mut().expect("driver owner").attach(&self.model.devices(),id.clone(),session,compiled,rusthinq_scripting::worker::Config {capacity:16,input_bytes:131072,source_bytes:524288}))} else {Err(rusthinq_scripting::Error::Stale)}
                            }
                        };
                        if let Err(error)=result {self.script_rejected(id.clone(),error);}
                        else {
                            self.script_callbacks.insert(id.clone(),crate::scripts::Callbacks {response:Some("__response".into()),data:Some("__data".into()),ready:None,timer:Some("__timer".into()),data_encoding:crate::scripts::DataEncoding::Hex});
                            self.shared.script_states.lock().unwrap_or_else(|e|e.into_inner()).insert(id.clone(),(session,1,false));
                            if let Err(error)=self.invoke_script(id.clone(),1,"__init".into(),String::new()) {self.script_rejected(id.clone(),error);}
                            if let Some((buffer_session,queue))=self.driver_buffers.remove(&id) && buffer_session==session {
                                for data in queue {if let Err(error)=self.invoke_script(id.clone(),1,"__data".into(),rusthinq_protocol::hex::encode(data)) {self.script_rejected(id.clone(),error);break;}}
                            }
                        }
                    }
                    for device in self.model.devices() {self.prepare_driver(&device.entry.id);}
                },
                Some(result) = self.script_deliveries.join_next(), if !self.script_deliveries.is_empty() => {
                    let (context, delivery) = result.map_err(io::Error::other)?;
                    self.emit(Event::ScriptDelivery {context, delivery});
                }
                Some(result) = self.script_results.join_next(), if !self.script_results.is_empty() => {
                    let (sequence, device, completion) = result.map_err(io::Error::other)?;
                    self.script_result(sequence, device, completion, !stopping && !*stop.borrow() && stop.has_changed().is_ok());
                }
                Some(command) = self.script_attach.recv(), if !stopping => match command {
                ScriptCommand::Invoke {device,session,generation,function,input,result}=>{
                    if !result.is_closed() {
                        self.reconcile();
                        let current=self.model.devices().iter().any(|d|d.entry.id==device && d.session==Some(session) && d.online && d.removal.is_none());
                        let outcome=if current {self.invoke_script(device,generation,function,input)} else {Err(rusthinq_scripting::Error::Stale)};
                        let _=result.send(outcome);
                    }
                },
                ScriptCommand::Attach(request) => {
                    let mut request = *request;
                    if !request.result.is_closed() {
                        self.reconcile();
                        request.compiled.set_consumer_enabled(self.script_sink.is_some());
                        let reaped = if let Some(owner) = self.scripts.as_mut() {
                            owner.reconcile(&self.model.devices());
                            owner.reap().await
                        } else { Err(rusthinq_scripting::Error::InvalidConfig) };
                        self.reconcile();
                        let result = if *stop.borrow() || stop.has_changed().is_err() {
                            Err(rusthinq_scripting::Error::Stopped)
                        } else {reaped.and_then(|()| self.scripts.as_mut().expect("script owner").attach(&self.model.devices(), request.device.clone(), request.session, request.compiled, request.config))};
                        if result.is_ok() {self.shared.script_states.lock().unwrap_or_else(|e|e.into_inner()).insert(request.device.clone(),(request.session,1,false));self.script_timers.retain(|(id,_),_|id!=&request.device);self.script_callbacks.insert(request.device, request.callbacks);}
                        let _ = request.result.send(result);
                    }
                },
                ScriptCommand::Reload(request) => {
                    let mut request = *request;
                    if !request.result.is_closed() {
                        self.reconcile();
                        request.compiled.set_consumer_enabled(self.script_sink.is_some());
                        let current = self.model.devices().iter().any(|device|
                            device.entry.id == request.device && device.session == Some(request.session) && device.removal.is_none());
                        let result = if !current {Err(rusthinq_scripting::Error::Stale)}
                        else if let Some(owner) = self.scripts.as_mut() {
                            match owner.reload(&self.model.devices(), &request.device, request.generation, request.compiled) {
                                Ok(reload) => reload.wait().await,
                                Err(error) => Err(error),
                            }
                        } else {Err(rusthinq_scripting::Error::InvalidConfig)};
                        self.reconcile();
                        let current = self.model.devices().iter().any(|device|
                            device.entry.id == request.device && device.session == Some(request.session) && device.removal.is_none());
                        let result = if *stop.borrow() || stop.has_changed().is_err() {Err(rusthinq_scripting::Error::Stopped)}
                            else if !current {Err(rusthinq_scripting::Error::Stale)} else {result};
                        if let Ok(generation)=result {self.shared.script_states.lock().unwrap_or_else(|e|e.into_inner()).insert(request.device.clone(),(request.session,generation,false));self.script_timers.retain(|(id,_),_|id!=&request.device);}
                        let _ = request.result.send(result);
                    }
                },
                },
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
                    *self.shared.durable_devices.lock().unwrap_or_else(|e|e.into_inner())=storage.state().ledger.entries.clone();
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
                Some(command) = self.commands.recv(), if !stopping => {
                    self.reconcile();
                    match command {
                        ManagementCommand::Forget(id) => self.forget(id),
                        ManagementCommand::ForgetScoped {id, incarnation, result} => {
                            if !result.is_closed() {
                                if self.model.devices().iter().any(|device| device.entry.id == id && device.entry.incarnation == incarnation) {
                                    self.forget(id);
                                    let _ = result.send(Ok(()));
                                } else {let _ = result.send(Err(rusthinq_server::Reject::StaleSession));}
                            }
                        }
                        ManagementCommand::Send {id, session, payload, result} => {
                            if !result.is_closed() {
                                let current = self.model.devices().iter().any(|device| device.entry.id == id && device.session == Some(session) && device.online && device.removal.is_none());
                                let sent = if current {self.server.send(&SessionId {device:id,generation:session.generation}, &payload)} else {Err(rusthinq_server::Reject::StaleSession)};
                                let _ = result.send(sent);
                            }
                        }
                        ManagementCommand::Inject {id,session,data,to_device,result}=>{
                            if !result.is_closed() {
                                let target=SessionId {device:id.clone(),generation:session.generation};
                                let current=self.model.devices().iter().any(|d|d.entry.id==id && d.session==Some(session) && d.online && d.removal.is_none()) && self.server.snapshot().contains(&target);
                                let outcome=if !current {Err(rusthinq_server::Reject::StaleSession)} else {
                                    match self.server.protocol(&target) {
                                        Err(error)=>Err(error),
                                        Ok(protocol)=>{
                                            if to_device {
                                                let payload=if protocol==rusthinq_server::Protocol::ThinQ2 {
                                                    self.shared.message_id.fetch_update(std::sync::atomic::Ordering::Relaxed,std::sync::atomic::Ordering::Relaxed,|id|id.checked_add(1)).map(|mid|serde_json::json!({"did":id,"mid":mid,"cmd":"packet","type":1,"data":rusthinq_protocol::hex::encode_upper(&data)}).to_string().into_bytes()).map_err(|_|rusthinq_server::Reject::Stopped)
                                                } else {Ok(data.clone())};
                                                payload.and_then(|payload|self.server.send(&target,&payload)).map(Some)
                                            } else {
                                                let input=if protocol==rusthinq_server::Protocol::ThinQ1 {use base64::Engine;serde_json::json!({"Header":{"x-lgedm-deviceId":id},"Body":{"Cmd":"Mon","Format":"B64","Data":base64::engine::general_purpose::STANDARD.encode(&data)}}).to_string().into_bytes()} else {data.clone()};
                                                self.script_transport(&TransportEvent::Data(target.clone(),input));Ok(None)
                                            }
                                        }
                                    }
                                };
                                if outcome.is_ok() {self.emit(Event::Injected {session:target,data,to_device});}
                                let _=result.send(outcome);
                            }
                        }
                    }
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
                            TransportEvent::Ready(id,_) | TransportEvent::CloudBound(id,_) |
                            TransportEvent::BridgedCloudBound(id,_,_) | TransportEvent::BridgeChanged(id,_,_) => Some(id),
                            TransportEvent::Will {session,..} => session.as_ref(),
                            _ => None,
                        };
                        if session.is_none_or(|id| self.model.devices().iter().any(|device| device.entry.id == id.device && device.session.is_some_and(|session| session.generation == id.generation))) {
                            if let TransportEvent::Ready(id, deploy) = &event
                                && self.server.snapshot().contains(id)
                                && let Some(firmware) = &self.firmware
                                && let Err(error) = firmware.protect_local_endpoints(deploy)
                            {
                                self.emit(Event::Rejected {device:id.device.clone(), reason:format!("local endpoint protection: {error}")});
                            }
                            self.script_transport(&event);
                            if let TransportEvent::Ready(id,deploy)=&event
                                && let Some(session)=self.model.devices().iter().find(|d|d.entry.id==id.device).and_then(|d|d.session)
                                && let Some(model)=deploy["kind"].as_str() {
                                self.shared.driver_models.lock().unwrap_or_else(|e|e.into_inner()).insert(id.device.clone(),(session,model.to_string(),true));
                                self.prepare_driver(&id.device);
                            }
                            if let TransportEvent::Data(id,data)=&event {self.buffer_driver(id,data);}
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
                }, if !stopping => {self.input(Input::Tick);self.fire_timers();},
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
