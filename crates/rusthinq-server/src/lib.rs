//! L3 ThinQ1 runtime. Admission accepts an already established transport (TCP or TLS).
//! No lifecycle, persistence, scripts, or cloud ownership lives here.
pub mod certificates;
pub mod https;
pub mod mqtt;
pub mod provisioning;
pub mod retained;
pub mod thinq1_http;
pub mod tls;
use rusthinq_protocol::thinq1::{self, Action, Input};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinSet,
    time::Instant,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_payload: usize,
    pub read_chunk: usize,
    pub outbound_capacity: usize,
    pub event_capacity: usize,
    pub max_connections: usize,
    pub idle_timeout: Duration,
    pub write_timeout: Duration,
    /// Must be at least the loaded lifecycle ledger's generation high-water mark.
    pub generation_floor: u64,
    /// Inclusive durable reservation bound; MAX preserves unreserved transport use.
    pub generation_ceiling: u64,
}
impl Config {
    fn validate(&self) -> Result<(), Reject> {
        if self.max_payload == 0
            || self.max_payload > i32::MAX as usize
            || self.read_chunk == 0
            || self.outbound_capacity == 0
            || self.event_capacity == 0
            || self.max_connections == 0
            || self.idle_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.generation_floor > self.generation_ceiling
        {
            return Err(Reject::InvalidConfig);
        }
        Ok(())
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_payload: 1_000_000,
            read_chunk: 8192,
            outbound_capacity: 16,
            event_capacity: 256,
            max_connections: 256,
            idle_timeout: Duration::from_secs(90),
            write_timeout: Duration::from_secs(10),
            generation_floor: 0,
            generation_ceiling: u64::MAX,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionId {
    pub device: String,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Up(SessionId),
    Data(SessionId, Vec<u8>),
    /// Original application payload after a complete device-facing frame write.
    Sent(SessionId, Vec<u8>),
    Response(SessionId, serde_json::Value),
    Down(SessionId, Disconnect),
    Ready(SessionId, serde_json::Value),
    CloudBound(SessionId, Vec<u8>),
    BridgedCloudBound(SessionId, u64, Vec<u8>),
    BridgeChanged(SessionId, u64, bool),
    /// Raw MQTT Last Will observation; L6 owns publication and retained storage.
    Will {
        generation: u64,
        session: Option<SessionId>,
        message: rusthinq_protocol::mqtt::Will,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disconnect {
    Eof,
    Closed,
    Io,
    WriteTimeout,
    Protocol(thinq1::Error),
    ThinQ2(rusthinq_protocol::thinq2::Error),
    Mqtt,
    IdleTimeout,
    Panic,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Reject {
    WrongTransport,
    InvalidConfig,
    Busy,
    StaleSession,
    PayloadExceeded,
    InvalidJson,
    Stopped,
    GenerationExhausted,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    Failed,
    Unknown,
}
/// A queued receipt resolves when the complete frame has been written, not device-ACKed.
pub struct Receipt(oneshot::Receiver<Delivery>);
impl Receipt {
    pub async fn wait(self) -> Delivery {
        self.0.await.unwrap_or(Delivery::Unknown)
    }
}
struct Command {
    frame: Vec<u8>,
    payload: Vec<u8>,
    result: oneshot::Sender<Delivery>,
}
struct Entry {
    protocol: Protocol,
    id: SessionId,
    commands: mpsc::Sender<Command>,
    bridge: Option<mpsc::Sender<mqtt::BridgeCommand>>,
    close: watch::Sender<bool>,
    ready: bool,
}
struct State {
    entries: HashMap<String, Entry>,
    generation: u64,
    generation_ceiling: u64,
    active: BTreeSet<u64>,
    identified: HashMap<String, u64>,
    stopped: bool,
}
struct Shared {
    stop: watch::Sender<bool>,
    state: Mutex<State>,
    completions: Mutex<HashMap<u64, watch::Receiver<bool>>>,
    events: broadcast::Sender<Event>,
    config: Config,
}
impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // A device panic must not poison shared transport ownership.
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}
#[derive(Clone)]
pub struct ServerHandle(Arc<Shared>);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    ThinQ1,
    ThinQ2,
}
impl ServerHandle {
    pub fn protocol(&self, session: &SessionId) -> Result<Protocol, Reject> {
        self.0
            .lock()
            .entries
            .get(&session.device)
            .filter(|entry| entry.id == *session && entry.ready)
            .map(|entry| entry.protocol)
            .ok_or(Reject::StaleSession)
    }
    /// Apply only a durably committed contiguous extension from the owner.
    pub fn extend_generations(&self, floor: u64, ceiling: u64) -> Result<(), Reject> {
        let mut state = self.0.lock();
        if state.stopped {
            return Err(Reject::Stopped);
        }
        if floor != state.generation_ceiling || ceiling <= floor {
            return Err(Reject::InvalidConfig);
        }
        state.generation_ceiling = ceiling;
        Ok(())
    }
    pub fn generation_budget(&self) -> (u64, u64) {
        let state = self.0.lock();
        (state.generation, state.generation_ceiling)
    }
    /// Close the exact ThinQ1 generation and await transport-future completion.
    /// A Down event is emitted earlier and is not this completion barrier.
    pub async fn close_and_wait(&self, session: &SessionId) -> Result<(), Reject> {
        let completion = self
            .0
            .completions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&session.generation)
            .cloned();
        let Some(mut completion) = completion else {
            // A completed ThinQ1 task may have been pruned; an active unsupported
            // transport cannot be treated as completed.
            return if self.0.lock().active.contains(&session.generation) {
                Err(Reject::StaleSession)
            } else {
                Ok(())
            };
        };
        match self.close(session) {
            Ok(()) | Err(Reject::StaleSession) => {}
            Err(error) => return Err(error),
        }
        while !*completion.borrow() {
            completion.changed().await.map_err(|_| Reject::Stopped)?;
        }
        Ok(())
    }
    /// Broadcast receivers report Lagged explicitly. Recover via snapshot; events carry generations.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.0.events.subscribe()
    }
    pub fn snapshot(&self) -> Vec<SessionId> {
        self.0
            .lock()
            .entries
            .values()
            .filter(|entry| entry.ready)
            .map(|entry| entry.id.clone())
            .collect()
    }
    pub fn send(&self, session: &SessionId, payload: &[u8]) -> Result<Receipt, Reject> {
        if payload.len() > self.0.config.max_payload {
            return Err(Reject::PayloadExceeded);
        }
        let state = self.0.lock();
        if state.stopped {
            return Err(Reject::Stopped);
        }
        let entry = state
            .entries
            .get(&session.device)
            .filter(|entry| entry.id == *session && entry.ready)
            .ok_or(Reject::StaleSession)?;
        if entry.protocol != Protocol::ThinQ1 {
            return Err(Reject::WrongTransport);
        }
        let _: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| Reject::InvalidJson)?;
        let frame = thinq1::encode(payload, self.0.config.max_payload)
            .map_err(|_| Reject::PayloadExceeded)?;
        let (result, receipt) = oneshot::channel();
        entry
            .commands
            .try_send(Command {
                frame,
                payload: payload.to_vec(),
                result,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Reject::Busy,
                mpsc::error::TrySendError::Closed(_) => Reject::StaleSession,
            })?;
        Ok(Receipt(receipt))
    }
    /// Generation-targeted close cannot accidentally close a replacement connection.
    pub fn close(&self, session: &SessionId) -> Result<(), Reject> {
        let mut state = self.0.lock();
        let entry = state
            .entries
            .get(&session.device)
            .filter(|entry| entry.id == *session && entry.ready)
            .ok_or(Reject::StaleSession)?;
        entry.close.send_replace(true);
        let ready = entry.ready;
        state.entries.remove(&session.device);
        if ready {
            let _ = self
                .0
                .events
                .send(Event::Down(session.clone(), Disconnect::Closed));
        }
        Ok(())
    }
}
/// Owns all connection tasks, including unidentified peers. Drop cancels; shutdown cancels and joins.
pub struct Server {
    handle: ServerHandle,
    tasks: JoinSet<()>,
    stop: watch::Sender<bool>,
}
impl Server {
    pub fn new(config: Config) -> Result<Self, Reject> {
        config.validate()?;
        let (events, _) = broadcast::channel(config.event_capacity);
        let (stop, _) = watch::channel(false);
        let state = State {
            entries: HashMap::new(),
            generation: config.generation_floor,
            generation_ceiling: config.generation_ceiling,
            active: BTreeSet::new(),
            identified: HashMap::new(),
            stopped: false,
        };
        let handle = ServerHandle(Arc::new(Shared {
            stop: stop.clone(),
            state: Mutex::new(state),
            completions: Mutex::new(HashMap::new()),
            events,
            config,
        }));
        Ok(Self {
            handle,
            tasks: JoinSet::new(),
            stop,
        })
    }
    pub fn handle(&self) -> ServerHandle {
        self.handle.clone()
    }
    pub fn admit<S>(&mut self, stream: S) -> Result<u64, Reject>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        while self.tasks.try_join_next().is_some() {}
        if self.tasks.len() >= self.handle.0.config.max_connections {
            return Err(Reject::Busy);
        }
        let generation = {
            let mut state = self.handle.0.lock();
            if state.stopped {
                return Err(Reject::Stopped);
            }
            if state.active.len() >= self.handle.0.config.max_connections {
                return Err(Reject::Busy);
            }
            state.generation = state
                .generation
                .checked_add(1)
                .filter(|next| *next <= state.generation_ceiling)
                .ok_or(Reject::GenerationExhausted)?;
            let generation = state.generation;
            state.active.insert(generation);
            generation
        };
        let shared = self.handle.0.clone();
        let stop = self.stop.subscribe();
        let (completed, completion) = watch::channel(false);
        {
            let mut completions = shared.completions.lock().unwrap_or_else(|e| e.into_inner());
            completions.retain(|_, receiver| !*receiver.borrow() && receiver.has_changed().is_ok());
            completions.insert(generation, completion);
        }
        self.tasks.spawn(async move {
            run(stream, shared, generation, stop).await;
            completed.send_replace(true);
        });
        Ok(generation)
    }
    pub async fn shutdown(mut self) {
        self.stop();
        while self.tasks.join_next().await.is_some() {}
    }
    fn stop(&self) {
        self.handle.0.lock().stopped = true;
        self.stop.send_replace(true);
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Guard {
    shared: Arc<Shared>,
    id: Option<SessionId>,
    reason: Disconnect,
    generation: u64,
}
impl Guard {
    fn current(&self) -> bool {
        let state = self.shared.lock();
        !state.stopped
            && self.id.as_ref().is_none_or(|id| {
                state
                    .entries
                    .get(&id.device)
                    .is_some_and(|entry| entry.id == *id)
            })
    }
    fn publish(&self, event: Event) -> bool {
        let state = self.shared.lock();
        if state.stopped
            || !self.id.as_ref().is_some_and(|id| {
                state
                    .entries
                    .get(&id.device)
                    .is_some_and(|entry| entry.id == *id)
            })
        {
            return false;
        }
        let _ = self.shared.events.send(event);
        true
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        if let Some(id) = &self.id
            && state
                .entries
                .get(&id.device)
                .is_some_and(|entry| entry.id == *id)
        {
            let ready = state
                .entries
                .remove(&id.device)
                .is_some_and(|entry| entry.ready);
            if ready {
                let _ = self
                    .shared
                    .events
                    .send(Event::Down(id.clone(), self.reason.clone()));
            }
        }
        state.active.remove(&self.generation);
        let oldest = state.active.first().copied();
        state
            .identified
            .retain(|_, latest| oldest.is_some_and(|oldest| oldest <= *latest));
    }
}
async fn run<S>(
    mut stream: S,
    shared: Arc<Shared>,
    generation: u64,
    mut stop: watch::Receiver<bool>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut guard = Guard {
        shared: shared.clone(),
        id: None,
        reason: Disconnect::Panic,
        generation,
    };
    let config = &shared.config;
    let start = Instant::now();
    let mut model = thinq1::Session::new(config.max_payload, config.idle_timeout, Duration::ZERO);
    let mut deadline = config.idle_timeout;
    let mut buffer = vec![0; config.read_chunk];
    let (commands, mut outbound) = mpsc::channel::<Command>(config.outbound_capacity);
    let (close, mut closing) = watch::channel(false);
    let reason = 'connection: loop {
        if *stop.borrow() || *closing.borrow() || !guard.current() {
            break Disconnect::Closed;
        }
        let outcome = tokio::select! {
            _ = stop.changed() => break Disconnect::Closed,
            _ = closing.changed() => break Disconnect::Closed,
            _ = tokio::time::sleep_until(start + deadline) => model.input(Input::Tick, start.elapsed()),
            command = outbound.recv() => {
                if let Some(command) = command {
                    if !guard.current() { let _ = command.result.send(Delivery::Failed); break Disconnect::Closed; }
                    let result = write_frame(&mut stream, &command.frame, config.write_timeout, &mut stop, &mut closing).await;
                    let delivery = match &result { Ok(()) => Delivery::Sent, Err((_, 0)) => Delivery::Failed, Err(_) => Delivery::Unknown };
                    if delivery==Delivery::Sent && let Some(id)=&guard.id {guard.publish(Event::Sent(id.clone(),command.payload));}
                    let _ = command.result.send(delivery);
                    if let Err((reason, _)) = result { break reason; }
                }
                continue;
            }
            read = stream.read(&mut buffer) => match read {
                Ok(0) => model.input(Input::End, start.elapsed()),
                Ok(count) => model.input(Input::Bytes(&buffer[..count]), start.elapsed()),
                Err(_) => break Disconnect::Io,
            }
        };
        for action in outcome.actions {
            if !guard.current() {
                break 'connection Disconnect::Closed;
            }
            match action {
                Action::Identified(device) => {
                    let id = SessionId { device, generation };
                    let mut state = shared.lock();
                    if state.stopped {
                        break 'connection Disconnect::Closed;
                    }
                    // Admission order fences a slow older handshake, too.
                    if state
                        .identified
                        .get(&id.device)
                        .is_some_and(|latest| *latest >= generation)
                    {
                        break 'connection Disconnect::Closed;
                    }
                    // An older unidentified peer can pin history; refuse new identities
                    // rather than retaining unbounded generation tombstones.
                    if !state.identified.contains_key(&id.device)
                        && state.identified.len() >= config.max_connections
                    {
                        break 'connection Disconnect::Closed;
                    }
                    state.identified.insert(id.device.clone(), generation);
                    if let Some(old) = state.entries.insert(
                        id.device.clone(),
                        Entry {
                            protocol: Protocol::ThinQ1,
                            bridge: None,
                            id: id.clone(),
                            commands: commands.clone(),
                            close: close.clone(),
                            ready: true,
                        },
                    ) {
                        old.close.send_replace(true);
                    }
                    guard.id = Some(id.clone());
                    let _ = shared.events.send(Event::Up(id));
                }
                Action::Send(frame) => {
                    if let Err((reason, _)) = write_frame(
                        &mut stream,
                        &frame,
                        config.write_timeout,
                        &mut stop,
                        &mut closing,
                    )
                    .await
                    {
                        break 'connection reason;
                    }
                    if let Some(id) = &guard.id {
                        guard.publish(Event::Sent(id.clone(), frame[4..].to_vec()));
                    }
                }
                Action::Data(payload) => {
                    if !guard.publish(Event::Data(
                        guard.id.clone().expect("identified data"),
                        payload,
                    )) {
                        break 'connection Disconnect::Closed;
                    }
                }
                Action::Response(body) => {
                    if !guard.publish(Event::Response(
                        guard.id.clone().expect("identified response"),
                        body,
                    )) {
                        break 'connection Disconnect::Closed;
                    }
                }
            }
        }
        if let Some(error) = outcome.error {
            break Disconnect::Protocol(error);
        }
        match outcome.next_deadline {
            Some(next) => deadline = next,
            None => break Disconnect::Eof,
        }
    };
    guard.reason = reason;
    // Remove the usable session before reporting queued failures.
    drop(stream);
    drop(guard);
    outbound.close();
    while let Some(command) = outbound.recv().await {
        let _ = command.result.send(Delivery::Failed);
    }
}
async fn write_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    frame: &[u8],
    timeout: Duration,
    stop: &mut watch::Receiver<bool>,
    close: &mut watch::Receiver<bool>,
) -> Result<(), (Disconnect, usize)> {
    let deadline = Instant::now() + timeout;
    let mut written = 0;
    while written < frame.len() {
        if *stop.borrow() || *close.borrow() {
            return Err((Disconnect::Closed, written));
        }
        let count = tokio::select! {
            biased;
            _ = stop.changed() => return Err((Disconnect::Closed, written)),
            _ = close.changed() => return Err((Disconnect::Closed, written)),
            _ = tokio::time::sleep_until(deadline) => return Err((Disconnect::WriteTimeout, written)),
            result = stream.write(&frame[written..]) => result.map_err(|_| (Disconnect::Io, written))?,
        };
        if count == 0 {
            return Err((Disconnect::Io, written));
        }
        written += count;
    }
    // TLS transports may buffer plaintext; Sent requires flushing those bytes too.
    tokio::select! {
        biased;
        _ = stop.changed() => Err((Disconnect::Closed, written)),
        _ = close.changed() => Err((Disconnect::Closed, written)),
        _ = tokio::time::sleep_until(deadline) => Err((Disconnect::WriteTimeout, written)),
        result = stream.flush() => result.map_err(|_| (Disconnect::Io, written)),
    }
}
