//! L4 registration ownership and generation-fenced ThinQ1 downlink dispatch.
//! Cloud adapters must supply authenticated downlinks and confirmed deregistration.
use crate::passthrough::Relay;
use rusthinq_server::{Receipt, Reject, ServerHandle, SessionId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

pub type DeregisterFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>;
/// Cloud adapter operation. Success must mean confirmed remote deregistration.
/// Dropping the future must cancel its work; no detached retries are allowed.
pub trait Deregistration: Send + Sync {
    fn deregister(&self, registration: Registration) -> DeregisterFuture;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    pub device: String,
    pub incarnation: u64,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub registration: Registration,
    pub enabled: bool,
    pub local: Option<SessionId>,
    pub last_local_generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Changed(Status),
    Deregistered(Registration),
}
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    InvalidIdentity,
    Capacity,
    GenerationExhausted,
    Stale,
    Disabled,
    Offline,
    IncarnationConflict,
    InvalidPayload,
    HostEvidence(String),
    Transport(Reject),
}
struct State {
    entries: BTreeMap<String, Status>,
    next: u64,
}
struct Shared {
    state: Mutex<State>,
    capacity: usize,
    max_payload: usize,
    events: broadcast::Sender<Event>,
}
#[derive(Clone)]
pub struct BridgeHandle(Arc<Shared>);
impl BridgeHandle {
    pub fn new(capacity: usize, max_payload: usize, event_capacity: usize) -> Result<Self, Error> {
        if capacity == 0
            || max_payload == 0
            || max_payload > i32::MAX as usize
            || event_capacity == 0
        {
            return Err(Error::InvalidConfig);
        }
        let (events, _) = broadcast::channel(event_capacity);
        Ok(Self(Arc::new(Shared {
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                next: 0,
            }),
            capacity,
            max_payload,
            events,
        })))
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.0.events.subscribe()
    }
    pub fn snapshot(&self) -> Vec<Status> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .values()
            .cloned()
            .collect()
    }
    /// Record a registration established by the cloud adapter. Re-enabling the
    /// same incarnation preserves its registration; local reconnect never dials.
    pub fn registered(&self, device: String, incarnation: u64) -> Result<Registration, Error> {
        if device.is_empty()
            || device.len() > 256
            || device.chars().any(char::is_control)
            || incarnation == 0
        {
            return Err(Error::InvalidIdentity);
        }
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(status) = state.entries.get_mut(&device) {
            if status.registration.incarnation != incarnation {
                return Err(Error::IncarnationConflict);
            }
            status.enabled = true;
            let _ = self.0.events.send(Event::Changed(status.clone()));
            return Ok(status.registration.clone());
        }
        if state.entries.len() >= self.0.capacity {
            return Err(Error::Capacity);
        }
        state.next = state
            .next
            .checked_add(1)
            .ok_or(Error::GenerationExhausted)?;
        let registration = Registration {
            device: device.clone(),
            incarnation,
            generation: state.next,
        };
        let status = Status {
            registration: registration.clone(),
            enabled: true,
            local: None,
            last_local_generation: 0,
        };
        state.entries.insert(device, status.clone());
        let _ = self.0.events.send(Event::Changed(status));
        Ok(registration)
    }
    fn update(
        &self,
        registration: &Registration,
        update: impl FnOnce(&mut Status) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let status = state
            .entries
            .get_mut(&registration.device)
            .filter(|status| status.registration == *registration)
            .ok_or(Error::Stale)?;
        update(status)?;
        let _ = self.0.events.send(Event::Changed(status.clone()));
        Ok(())
    }
    pub fn bind(&self, registration: &Registration, local: SessionId) -> Result<(), Error> {
        if local.device != registration.device || local.generation == 0 {
            return Err(Error::InvalidIdentity);
        }
        self.update(registration, |status| {
            if local.generation < status.last_local_generation
                || (local.generation == status.last_local_generation
                    && status.local.as_ref() != Some(&local))
            {
                return Err(Error::Stale);
            }
            status.last_local_generation = local.generation;
            status.local = Some(local);
            Ok(())
        })
    }
    pub fn unbind(&self, registration: &Registration, local: &SessionId) -> Result<(), Error> {
        self.update(registration, |status| {
            if status.local.as_ref() != Some(local) {
                return Err(Error::Stale);
            }
            status.local = None;
            Ok(())
        })
    }
    pub fn disable(&self, registration: &Registration) -> Result<(), Error> {
        self.update(registration, |status| {
            status.enabled = false;
            Ok(())
        })
    }
    /// Call only after the cloud adapter confirms deregistration. Failure leaves
    /// registration intact; delayed results cannot remove a successor.
    pub fn deregistered(&self, registration: &Registration) -> Result<(), Error> {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state
            .entries
            .get(&registration.device)
            .is_some_and(|status| status.registration == *registration)
        {
            return Err(Error::Stale);
        }
        state.entries.remove(&registration.device);
        let _ = self
            .0
            .events
            .send(Event::Deregistered(registration.clone()));
        Ok(())
    }
    /// Token and exact local target must come from this cloud connection's
    /// captured context. Never substitute a latest token for delayed traffic.
    /// No offline buffering, replay, byte retry, or cloud dial occurs here.
    pub fn downlink(
        &self,
        registration: &Registration,
        local: &SessionId,
        payload: &[u8],
        server: &ServerHandle,
        firmware: Option<&Relay>,
    ) -> Result<Receipt, Error> {
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let status = state
            .entries
            .get(&registration.device)
            .filter(|status| status.registration == *registration)
            .ok_or(Error::Stale)?;
        if !status.enabled {
            return Err(Error::Disabled);
        }
        if status.local.as_ref() != Some(local) {
            return Err(Error::Stale);
        }
        if !server.snapshot().contains(local) {
            return Err(Error::Offline);
        }
        if server.protocol(local) != Ok(rusthinq_server::Protocol::ThinQ1) {
            return Err(Error::Transport(Reject::WrongTransport));
        }
        if payload.len() > self.0.max_payload {
            return Err(Error::InvalidPayload);
        }
        let json: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| Error::InvalidPayload)?;
        if !json.is_object()
            || json
                .get("Header")
                .and_then(|header| header.get("x-lgedm-deviceId"))
                .is_some_and(|id| id.as_str() != Some(registration.device.as_str()))
        {
            return Err(Error::InvalidPayload);
        }
        // Evidence is from a validated cloud-adapter command, never local data.
        if let Some(firmware) = firmware {
            firmware
                .learn_command(&json)
                .map_err(|error| Error::HostEvidence(error.to_string()))?;
        }
        server.send(local, payload).map_err(Error::Transport)
    }
}
