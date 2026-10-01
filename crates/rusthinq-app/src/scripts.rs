//! L6 ownership and fencing for device script workers. No output sink is implicit.
use rusthinq_lifecycle::{Device, SessionKey};
use rusthinq_scripting::{
    Compiled, Error, Outcome,
    worker::{Config, Invocation, Worker},
};
use std::{collections::BTreeMap, sync::Arc};

struct Owned {
    binding: u64,
    session: SessionKey,
    worker: Worker,
}
pub struct Call {
    owner: Arc<()>,
    binding: u64,
    device: String,
    session: SessionKey,
    generation: u64,
    invocation: Invocation,
}
pub struct Completion {
    owner: Arc<()>,
    binding: u64,
    device: String,
    session: SessionKey,
    generation: u64,
    outcome: Outcome,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub device: String,
    pub session: SessionKey,
    pub generation: u64,
}
impl Completion {
    pub fn context(&self) -> Context {
        Context {
            device: self.device.clone(),
            session: self.session,
            generation: self.generation,
        }
    }
}
/// Called synchronously after fencing. Implementations must be bounded/nonblocking;
/// retained MQTT adapters must durably inventory ownership before publication.
pub trait PublishSink: Send + Sync {
    fn try_publish(&self, context: &Context, payload: String) -> Result<(), String>;
}
#[derive(Clone, Debug, Default)]
pub struct Callbacks {
    pub response: Option<String>,
    pub data: Option<String>,
    pub ready: Option<String>,
    pub data_encoding: DataEncoding,
}
#[derive(Clone, Debug, Default)]
pub enum DataEncoding {
    #[default]
    Utf8,
    /// Lowercase hex preserves every byte without interpreting IL or model framing.
    Hex,
}
impl DataEncoding {
    pub(crate) fn encode(&self, data: &[u8]) -> Result<String, String> {
        match self {
            Self::Utf8 => String::from_utf8(data.to_vec()).map_err(|error| error.to_string()),
            Self::Hex => {
                let mut result = String::with_capacity(data.len().saturating_mul(2));
                const HEX: &[u8; 16] = b"0123456789abcdef";
                for byte in data {
                    result.push(HEX[usize::from(byte >> 4)] as char);
                    result.push(HEX[usize::from(byte & 15)] as char);
                }
                Ok(result)
            }
        }
    }
}
impl Call {
    pub async fn wait(self) -> Result<Completion, Error> {
        Ok(Completion {
            owner: self.owner,
            binding: self.binding,
            device: self.device,
            session: self.session,
            generation: self.generation,
            outcome: self.invocation.wait().await?,
        })
    }
}
/// A finite worker budget for one runtime. Retired workers occupy a slot until joined.
pub struct Owner {
    identity: Arc<()>,
    next_binding: u64,
    capacity: usize,
    workers: BTreeMap<String, Owned>,
    retiring: Vec<Worker>,
    stopped: bool,
}
impl Owner {
    pub fn generation(&self, id: &str) -> Option<u64> {
        self.workers
            .get(id)
            .and_then(|owned| match *owned.worker.handle().status().borrow() {
                rusthinq_scripting::worker::Status::Running { generation }
                | rusthinq_scripting::worker::Status::Faulted { generation, .. } => {
                    Some(generation)
                }
                rusthinq_scripting::worker::Status::Stopped { .. } => None,
            })
    }
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            identity: Arc::new(()),
            next_binding: 1,
            capacity,
            workers: BTreeMap::new(),
            retiring: Vec::new(),
            stopped: false,
        })
    }
    fn current(devices: &[Device], id: &str, session: SessionKey) -> bool {
        devices.iter().any(|device| {
            device.entry.id == id
                && device.entry.incarnation == session.incarnation
                && device.session == Some(session)
                && device.removal.is_none()
        })
    }
    /// Caller supplies the current L2 snapshot; no offline/quiesced attachment.
    pub fn attach(
        &mut self,
        devices: &[Device],
        id: String,
        session: SessionKey,
        compiled: Compiled,
        config: Config,
    ) -> Result<(), Error> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        self.reconcile(devices);
        if !Self::current(devices, &id, session) {
            return Err(Error::Stale);
        }
        if compiled.context_device().is_some_and(|device| device != id) {
            return Err(Error::InvalidConfig);
        }
        if self.workers.contains_key(&id)
            || self.workers.len() + self.retiring.len() >= self.capacity
        {
            return Err(Error::Busy);
        }
        let next = self
            .next_binding
            .checked_add(1)
            .ok_or(Error::GenerationExhausted)?;
        let worker = Worker::spawn(compiled, config)?;
        self.workers.insert(
            id,
            Owned {
                binding: self.next_binding,
                session,
                worker,
            },
        );
        self.next_binding = next;
        Ok(())
    }
    /// Invalidate before waiting for workers; accepted calls can finish but cannot dispatch.
    pub fn reconcile(&mut self, devices: &[Device]) {
        let stale: Vec<_> = self
            .workers
            .iter()
            .filter(|(id, owned)| !Self::current(devices, id, owned.session))
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            let owned = self.workers.remove(&id).expect("selected worker");
            owned.worker.handle().request_stop();
            self.retiring.push(owned.worker);
        }
    }
    pub fn invoke(
        &mut self,
        devices: &[Device],
        id: &str,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<Call, Error> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        self.reconcile(devices);
        let owned = self.workers.get(id).ok_or(Error::Stale)?;
        let invocation = owned.worker.handle().invoke(generation, function, input)?;
        Ok(Call {
            owner: self.identity.clone(),
            binding: owned.binding,
            device: id.into(),
            session: owned.session,
            generation,
            invocation,
        })
    }
    pub fn reload(
        &mut self,
        devices: &[Device],
        id: &str,
        generation: u64,
        compiled: Compiled,
    ) -> Result<rusthinq_scripting::worker::Reload, Error> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        if compiled.context_device().is_some_and(|device| device != id) {
            return Err(Error::InvalidConfig);
        }
        self.reconcile(devices);
        self.workers
            .get(id)
            .ok_or(Error::Stale)?
            .worker
            .handle()
            .reload(generation, compiled)
    }
    /// Check immediately before effects in the same L6 actor turn. The caller owns delivery.
    /// Opaque strings are neither parsed nor published to MQTT by this owner.
    pub fn accept(&mut self, devices: &[Device], completion: Completion) -> Result<Outcome, Error> {
        if !Arc::ptr_eq(&self.identity, &completion.owner) {
            return Err(Error::Stale);
        }
        if self.stopped {
            return Err(Error::Stopped);
        }
        self.reconcile(devices);
        let owned = self.workers.get(&completion.device).ok_or(Error::Stale)?;
        let generation = match *owned.worker.handle().status().borrow() {
            rusthinq_scripting::worker::Status::Running { generation }
            | rusthinq_scripting::worker::Status::Faulted { generation, .. } => generation,
            rusthinq_scripting::worker::Status::Stopped { .. } => return Err(Error::Stopped),
        };
        if owned.binding != completion.binding
            || owned.session != completion.session
            || completion.generation != generation
            || completion.outcome.generation != generation
        {
            return Err(Error::Stale);
        }
        Ok(completion.outcome)
    }
    /// Join retired workers before making their budget available to successors.
    pub async fn reap(&mut self) -> Result<(), Error> {
        let mut failure = None;
        while let Some(worker) = self.retiring.pop() {
            if let Err(error) = worker.shutdown().await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.stopped = true;
        for (_, owned) in std::mem::take(&mut self.workers) {
            owned.worker.handle().request_stop();
            self.retiring.push(owned.worker);
        }
        self.reap().await
    }
}
