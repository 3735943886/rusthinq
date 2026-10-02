//! Owned L6 session/lifecycle loop and serialized off-thread persistence.
use crate::lifecycle_storage::Storage;
#[cfg(feature = "bridge")]
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
    Injected {
        session: SessionId,
        data: Vec<u8>,
        to_device: bool,
    },
    ScriptOutput {
        context: crate::scripts::Context,
        payload: String,
    },
    ScriptStopped {
        context: crate::scripts::Context,
        error: Option<String>,
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
#[cfg(feature = "scripting")]
struct ApplicationSink(
    broadcast::Sender<Event>,
    Option<crate::external_mqtt::Handle>,
);
#[cfg(feature = "scripting")]
impl crate::scripts::PublishSink for ApplicationSink {
    fn try_publish_retired(
        &self,
        context: &crate::scripts::Context,
        payload: String,
    ) -> Result<(), String> {
        if let Some(external) = &self.1 {
            external.try_publish_retired(context, payload.clone())?;
        }
        let _ = self.0.send(Event::ScriptOutput {
            context: context.clone(),
            payload,
        });
        Ok(())
    }
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
    driver_reload_configured: std::sync::atomic::AtomicBool,
    cloud: Mutex<Option<crate::cloud_account::Handle>>,
    #[cfg(feature = "bridge")]
    cloud_devices: Mutex<Option<crate::cloud_devices::Handle>>,
    #[cfg(feature = "bridge")]
    cloud_deploy: Mutex<BTreeMap<String, (u64, serde_json::Value)>>,
    retired_scripts: Mutex<BTreeMap<String, crate::scripts::Context>>,
    external_mqtt: Mutex<Option<crate::external_mqtt::Handle>>,
    persisted_models: Mutex<BTreeMap<String, crate::lifecycle_storage::DeviceMetadata>>,
    durable_devices: Mutex<Vec<rusthinq_lifecycle::Entry>>,
    message_id: std::sync::atomic::AtomicU64,
    driver_models: Mutex<BTreeMap<String, (SessionKey, String, bool)>>,
    script_states: Mutex<BTreeMap<String, (SessionKey, u64, bool)>>,
    #[cfg(feature = "scripting")]
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
mod handle;

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
    Metadata(Result<(), String>),
}
#[cfg(feature = "scripting")]
struct AttachScript {
    device: String,
    session: SessionKey,
    compiled: rusthinq_scripting::Compiled,
    config: rusthinq_scripting::worker::Config,
    callbacks: crate::scripts::Callbacks,
    result: oneshot::Sender<Result<(), rusthinq_scripting::Error>>,
}
#[cfg(feature = "scripting")]
struct ReloadScript {
    initialize: bool,
    device: String,
    session: SessionKey,
    generation: u64,
    compiled: rusthinq_scripting::Compiled,
    result: oneshot::Sender<Result<u64, rusthinq_scripting::Error>>,
}
#[cfg(feature = "scripting")]
struct PrepareReload {
    device: String,
    session: SessionKey,
    generation: u64,
    result: oneshot::Sender<Result<u64, rusthinq_scripting::Error>>,
}
#[cfg(feature = "scripting")]
enum ScriptCommand {
    Attach(Box<AttachScript>),
    Reload(Box<ReloadScript>),
    PrepareReload(PrepareReload),
    Invoke {
        device: String,
        session: SessionKey,
        generation: u64,
        function: String,
        input: String,
        result: oneshot::Sender<Result<u64, rusthinq_scripting::Error>>,
    },
}
#[cfg(feature = "scripting")]
impl ScriptCommand {
    fn cancel(self) {
        match self {
            Self::Attach(request) => {
                let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
            }
            Self::Reload(request) => {
                let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
            }
            Self::PrepareReload(request) => {
                let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
            }
            Self::Invoke { result, .. } => {
                let _ = result.send(Err(rusthinq_scripting::Error::Stopped));
            }
        }
    }
}
type WriteTask = JoinHandle<(Storage, WriteResult)>;
#[cfg(feature = "scripting")]
mod scripting;
#[cfg(feature = "scripting")]
use scripting::runtime_select;
#[cfg(not(feature = "scripting"))]
macro_rules! runtime_select { ($runtime:ident,$stop:ident,$stopping:ident; $($branches:tt)*) => {tokio::select! { biased; $($branches)* }}; }
mod transport;
use transport::TransportHandle;

pub struct Runtime {
    #[cfg(feature = "scripting")]
    pending_attach: Option<Box<AttachScript>>,
    pending_metadata: BTreeMap<String, crate::lifecycle_storage::DeviceMetadata>,
    #[cfg(feature = "scripting")]
    drivers: Option<crate::drivers::Config>,
    #[cfg(feature = "scripting")]
    preparation_policy: rusthinq_scripting::preparation::Preparation<SessionKey>,
    #[cfg(feature = "scripting")]
    driver_reload_preparation: JoinSet<(
        PrepareReload,
        String,
        Result<rusthinq_scripting::Compiled, rusthinq_scripting::Error>,
    )>,
    #[cfg(feature = "scripting")]
    driver_preparation: JoinSet<(
        String,
        SessionKey,
        String,
        Result<rusthinq_scripting::Compiled, rusthinq_scripting::Error>,
    )>,

    #[cfg(feature = "scripting")]
    script_attach: mpsc::Receiver<ScriptCommand>,
    #[cfg(feature = "scripting")]
    script_callbacks: BTreeMap<String, crate::scripts::Callbacks>,
    #[cfg(feature = "scripting")]
    script_schedule: rusthinq_scripting::scheduling::Scheduler<
        crate::scripts::Context,
        Result<crate::scripts::Completion, rusthinq_scripting::Error>,
    >,
    #[cfg(feature = "scripting")]
    script_results: JoinSet<(
        u64,
        String,
        Result<crate::scripts::Completion, rusthinq_scripting::Error>,
    )>,

    #[cfg(feature = "scripting")]
    script_deliveries: JoinSet<(crate::scripts::Context, rusthinq_server::Delivery)>,

    #[cfg(feature = "scripting")]
    script_sink: Option<Arc<dyn crate::scripts::PublishSink>>,
    #[cfg(feature = "scripting")]
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
    #[cfg(feature = "bridge")]
    bridge: Option<Bridge>,
    #[cfg(feature = "bridge")]
    firmware: Option<rusthinq_bridge::passthrough::Relay>,
    #[cfg(feature = "bridge")]
    removal_registrations: BTreeMap<String, Registration>,
    refill: Option<(u64, u64)>,
    deferred_storage: Option<Action>,
    metadata: Option<mpsc::Receiver<rusthinq_server::thinq1_http::Metadata>>,
}
#[cfg(feature = "bridge")]
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
        #[cfg(feature = "scripting")]
        let (script_sender, script_attach) = mpsc::channel(1);
        let (cleanup_state, _) = watch::channel((model.devices(), false));
        let (attach, attachments) = mpsc::channel(1);
        let (cleanup_status, _) = watch::channel(CleanupStatus::Disabled);
        let shared = Arc::new(Shared {
            driver_reload_configured: std::sync::atomic::AtomicBool::new(false),
            cloud: Mutex::new(None),
            #[cfg(feature = "bridge")]
            cloud_devices: Mutex::new(None),
            #[cfg(feature = "bridge")]
            cloud_deploy: Mutex::new(BTreeMap::new()),
            persisted_models: Mutex::new(storage.state().metadata.clone()),
            durable_devices: Mutex::new(storage.state().ledger.entries.clone()),
            message_id: std::sync::atomic::AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(io::Error::other)?
                    .as_millis()
                    .try_into()
                    .map_err(io::Error::other)?,
            ),
            driver_models: Mutex::new(
                storage
                    .state()
                    .metadata
                    .iter()
                    .map(|(id, meta)| {
                        let generation = storage
                            .state()
                            .ledger
                            .entries
                            .iter()
                            .find(|entry| entry.id == *id)
                            .expect("validated model owner")
                            .last_generation;
                        (
                            id.clone(),
                            (
                                SessionKey {
                                    incarnation: meta.incarnation,
                                    generation,
                                },
                                meta.model_name.clone(),
                                meta.thinq2,
                            ),
                        )
                    })
                    .collect(),
            ),
            retired_scripts: Mutex::new(BTreeMap::new()),
            external_mqtt: Mutex::new(None),
            script_states: Mutex::new(BTreeMap::new()),
            #[cfg(feature = "scripting")]
            scripts: script_sender,
            metadata: Mutex::new(
                storage
                    .state()
                    .metadata
                    .iter()
                    .filter(|(_, meta)| !meta.thinq2)
                    .map(|(id, meta)| {
                        (
                            id.clone(),
                            rusthinq_server::thinq1_http::Metadata {
                                device_id: id.clone(),
                                model_name: meta.model_name.clone(),
                                device_type: meta.device_type.clone(),
                            },
                        )
                    })
                    .collect(),
            ),
            devices: Mutex::new(model.devices()),
            events,
            commands,
            attach,
            cleanup_status,
        });
        Ok(Self {
            #[cfg(feature = "scripting")]
            pending_attach: None,
            pending_metadata: BTreeMap::new(),
            #[cfg(feature = "scripting")]
            drivers: None,
            #[cfg(feature = "scripting")]
            preparation_policy: rusthinq_scripting::preparation::Preparation::new(capacity),
            #[cfg(feature = "scripting")]
            driver_preparation: JoinSet::new(),
            #[cfg(feature = "scripting")]
            driver_reload_preparation: JoinSet::new(),

            #[cfg(feature = "scripting")]
            script_attach,
            #[cfg(feature = "scripting")]
            script_callbacks: BTreeMap::new(),
            #[cfg(feature = "scripting")]
            script_schedule: rusthinq_scripting::scheduling::Scheduler::new(event_capacity)
                .expect("validated event capacity"),
            #[cfg(feature = "scripting")]
            script_results: JoinSet::new(),

            #[cfg(feature = "scripting")]
            script_deliveries: JoinSet::new(),

            #[cfg(feature = "scripting")]
            script_sink: None,
            #[cfg(feature = "scripting")]
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
            #[cfg(feature = "bridge")]
            bridge: None,
            #[cfg(feature = "bridge")]
            firmware: None,
            #[cfg(feature = "bridge")]
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
    #[cfg(feature = "scripting")]
    pub fn with_scripts(mut self, mut owner: crate::scripts::Owner) -> Self {
        assert!(self.scripts.is_none(), "one script worker owner");
        owner.reconcile(&self.model.devices());
        self.scripts = Some(owner);
        self
    }
    #[cfg(feature = "scripting")]
    pub fn with_script_sink(mut self, sink: Arc<dyn crate::scripts::PublishSink>) -> Self {
        assert!(self.script_sink.is_none(), "one script publication sink");
        self.script_sink = Some(sink);
        self
    }
    #[allow(unused_mut)]
    pub fn with_external_mqtt(mut self, handle: crate::external_mqtt::Handle) -> Self {
        *self
            .shared
            .external_mqtt
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(handle.clone());
        #[cfg(feature = "scripting")]
        {
            self.script_sink = Some(Arc::new(ApplicationSink(
                self.shared.events.clone(),
                Some(handle),
            )));
        }
        self
    }
    #[cfg(feature = "scripting")]
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
        self.shared
            .driver_reload_configured
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(self)
    }
    /// Share the exact relay used by the TLS front door. Only completed current
    /// provisioning contributes local proof; pending device packets never do.
    #[cfg(feature = "bridge")]
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
    fn stage_model(&mut self, id: &str, session: SessionKey, model: &str, thinq2: bool) {
        let Some(device) = self.model.devices().into_iter().find(|device| {
            device.entry.id == id && device.session == Some(session) && device.removal.is_none()
        }) else {
            return;
        };
        let device_type = if thinq2 {
            String::new()
        } else {
            self.handle()
                .metadata_snapshot()
                .into_iter()
                .find(|meta| meta.device_id == id)
                .map(|meta| meta.device_type)
                .unwrap_or_default()
        };
        let record = crate::lifecycle_storage::DeviceMetadata {
            incarnation: device.entry.incarnation,
            model_name: model.into(),
            device_type,
            thinq2,
        };
        if self
            .shared
            .persisted_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            != Some(&record)
        {
            self.pending_metadata.insert(id.into(), record);
        }
    }
    fn write_metadata(&mut self) {
        if self.write.is_some() {
            return;
        }
        let devices = self.model.devices();
        self.pending_metadata.retain(|id, meta| {
            devices.iter().any(|device| {
                device.entry.id == *id
                    && device.entry.incarnation == meta.incarnation
                    && device.removal.is_none()
            })
        });
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        let eligible: Vec<_> = self
            .pending_metadata
            .iter()
            .filter(|(id, meta)| {
                storage
                    .state()
                    .ledger
                    .entries
                    .iter()
                    .any(|entry| entry.id == **id && entry.incarnation == meta.incarnation)
            })
            .map(|(id, _)| id.clone())
            .collect();
        if eligible.is_empty() {
            if !self.pending_metadata.is_empty() && self.deferred_storage.is_none() {
                self.pending_metadata.clear();
                self.emit(Event::Rejected {
                    device: String::new(),
                    reason: "model metadata requires durable device ownership".into(),
                });
            }
            return;
        }
        let batch = eligible
            .into_iter()
            .map(|id| {
                let meta = self.pending_metadata.remove(&id).expect("pending model");
                (id, meta)
            })
            .collect::<BTreeMap<_, _>>();
        let mut storage = self.storage.take().expect("idle model storage");
        self.write = Some(tokio::task::spawn_blocking(move || {
            let result = storage
                .save_metadata(&batch)
                .map_err(|error| error.to_string());
            (storage, WriteResult::Metadata(result))
        }));
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
            && self.server.protocol(&SessionId {
                device: id.clone(),
                generation: session.generation,
            }) == Ok(rusthinq_server::Protocol::ThinQ1)
        {
            self.stage_model(&id, session, &metadata.model_name, false);
            self.shared
                .driver_models
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id.clone(), (session, metadata.model_name.clone(), false));
        }
        self.emit(Event::Metadata(metadata));
        self.prepare_driver(&id);
    }
    #[cfg(feature = "scripting")]
    fn prepare_driver(&mut self, id: &str) {
        let Some(config) = self.drivers.as_ref() else {
            return;
        };
        if !rusthinq_scripting::preparation::Preparation::<SessionKey>::can_start(
            self.driver_preparation.len() + self.driver_reload_preparation.len(),
            self.scripts
                .as_ref()
                .and_then(|owner| owner.generation(id))
                .is_some(),
        ) {
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
        if !self.preparation_policy.begin(id, session, &model) {
            return;
        }
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
    #[cfg(feature = "scripting")]
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
        if let Err(error) = self.preparation_policy.buffer(&id.device, session, data) {
            self.script_rejected(id.device.clone(), error);
        }
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
    #[cfg(feature = "bridge")]
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
        #[cfg(feature = "bridge")]
        if self.handle().cloud_devices().is_some_and(|handle| {
            handle
                .snapshot()
                .iter()
                .any(|s| s.device == id && !s.paired)
        }) {
            self.emit(Event::Rejected {
                device: id,
                reason: "pending pairing outcome; unpair before forget".into(),
            });
            return;
        }
        #[cfg(feature = "bridge")]
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
        #[cfg(feature = "bridge")]
        let bridge_active = self.removal_registrations.contains_key(&id);
        #[cfg(not(feature = "bridge"))]
        let bridge_active = false;
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
        let devices = self.model.devices();
        self.reconcile_scripts(&devices);
        self.shared
            .driver_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|id, _| devices.iter().any(|d| d.entry.id == *id));
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
                    #[cfg(feature = "bridge")]
                    self.removal_registrations.remove(&id);
                    self.shared
                        .metadata
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&id);
                }
                #[cfg(feature = "bridge")]
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
            && self.server.protocol(&id) == Ok(rusthinq_server::Protocol::ThinQ1)
            && let Some(session) = self
                .model
                .devices()
                .iter()
                .find(|d| d.entry.id == id.device)
                .and_then(|d| d.session)
        {
            self.stage_model(&id.device, session, &metadata.model_name, false);
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
            self.write_metadata();
            self.prepare_scripts_turn(stopping);
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
                self.stop_scripts().await?;
                self.commands.close();
                while let Ok(command) = self.commands.try_recv() {
                    command.cancel();
                }
                self.drain_script_results().await?;
                if let Some(mut receiver) = self.metadata.take() {
                    while let Ok(metadata) = receiver.try_recv() {
                        self.observe_metadata(metadata);
                    }
                }
                if !self.pending_metadata.is_empty() {
                    continue;
                }
                self.cleanup_state
                    .send_replace((self.model.devices(), true));
                if self.cleanup.is_none() {
                    return Ok(());
                }
            }
            #[allow(unused_mut)]
            let mut deadline = self.deadline.map(|value| self.epoch + value);
            #[cfg(feature = "scripting")]
            {
                deadline = deadline
                    .into_iter()
                    .chain(self.script_schedule.next_deadline())
                    .min();
            }
            runtime_select! {self,stop,stopping;
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
                    *self.shared.persisted_models.lock().unwrap_or_else(|e|e.into_inner())=storage.state().metadata.clone();
                    self.storage = Some(storage);
                    match result {
                        WriteResult::Lifecycle(input) => self.input(input),
                        WriteResult::Metadata(result)=>{
                            if let Err(reason)=result {self.emit(Event::Rejected{device:String::new(),reason:format!("model metadata persistence: {reason}")});}
                            if let Some(action)=self.deferred_storage.take() {self.write_action(action);}
                        },
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
                            #[cfg(feature = "bridge")]
                            if let TransportEvent::Ready(id, deploy) = &event
                                && self.server.snapshot().contains(id)
                                && let Some(firmware) = &self.firmware
                                && let Err(error) = firmware.protect_local_endpoints(deploy)
                            {
                                self.emit(Event::Rejected {device:id.device.clone(), reason:format!("local endpoint protection: {error}")});
                            }
                            self.script_transport(&event);
                            #[cfg(feature="bridge")]
                            if let TransportEvent::Ready(id,deploy)=&event {self.shared.cloud_deploy.lock().unwrap_or_else(|e|e.into_inner()).insert(id.device.clone(),(id.generation,deploy.clone()));}
                            if let TransportEvent::Ready(id,deploy)=&event
                                && let Some(session)=self.model.devices().iter().find(|d|d.entry.id==id.device).and_then(|d|d.session)
                                && let Some(model)=deploy["kind"].as_str() {
                                self.stage_model(&id.device,session,model,true);
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

#[cfg(not(feature = "scripting"))]
impl Runtime {
    fn prepare_scripts_turn(&mut self, _stopping: bool) {}
    async fn stop_scripts(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn drain_script_results(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn reconcile_scripts(&mut self, _devices: &[Device]) {}
    fn prepare_driver(&mut self, _id: &str) {}
    fn buffer_driver(&mut self, _id: &SessionId, _data: &[u8]) {}
    fn script_transport(&mut self, _event: &TransportEvent) {}
    fn fire_timers(&mut self) {}
}
