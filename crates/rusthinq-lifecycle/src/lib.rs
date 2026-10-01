//! L2: process-wide lifecycle decisions with caller-supplied monotonic time.
//! No protocol, transport, storage, clocks, or executor dependencies.
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub id: String,
    pub incarnation: u64,
    pub last_generation: u64,
}

/// Persist the allocation high-water mark even after the last device is removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    pub revision: u64,
    pub next_incarnation: u64,
    pub entries: Vec<Entry>,
}
impl Default for Ledger {
    fn default() -> Self {
        Self {
            revision: 0,
            next_incarnation: 1,
            entries: vec![],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionKey {
    pub incarnation: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Close,
    Deregister,
    PersistRemoval,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Removal {
    Active { operation: u64, step: Step },
    Failed { step: Step, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    pub entry: Entry,
    /// None means unusable immediately, even while online grace is pending.
    pub session: Option<SessionKey>,
    pub online: bool,
    pub offline_deadline: Option<Duration>,
    pub removal: Option<Removal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Changed(Device),
    Online {
        id: String,
        incarnation: u64,
    },
    Offline {
        id: String,
        incarnation: u64,
    },
    CloseSuperseded {
        id: String,
        session: SessionKey,
    },
    PersistLedger(Ledger),
    LedgerResult {
        revision: u64,
        result: Result<(), String>,
    },
    ForgetStep {
        id: String,
        incarnation: u64,
        operation: u64,
        step: Step,
        /// The close target is retained through retries, never a successor session.
        session: Option<SessionKey>,
        /// Present only for PersistRemoval; excludes the quiesced device.
        ledger: Option<Ledger>,
    },
    ForgetFailed {
        id: String,
        incarnation: u64,
        step: Step,
        reason: String,
    },
    Removed {
        id: String,
        incarnation: u64,
    },
}

pub enum Input {
    SessionUp {
        id: String,
        generation: u64,
    },
    SessionDown {
        id: String,
        session: SessionKey,
    },
    Forget {
        id: String,
        bridge_active: bool,
    },
    StepResult {
        id: String,
        incarnation: u64,
        operation: u64,
        step: Step,
        result: Result<(), String>,
    },
    LedgerResult {
        revision: u64,
        result: Result<(), String>,
    },
    Tick,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidLedger,
    InvalidIdentity,
    UnknownDevice,
    Busy,
    Quiesced,
    StaleGeneration,
    TimeWentBackwards,
    CounterExhausted,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lifecycle error: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    pub actions: Vec<Action>,
    pub error: Option<Error>,
    pub next_deadline: Option<Duration>,
}

struct State {
    device: Device,
    bridge_active: bool,
    close_target: Option<SessionKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Storage {
    Ledger(u64),
    Removal {
        id: String,
        incarnation: u64,
        operation: u64,
    },
}

pub struct Model {
    devices: BTreeMap<String, State>,
    grace: Duration,
    last_now: Duration,
    next_incarnation: u64,
    revision: u64,
    next_operation: u64,
    storage: Option<Storage>,
    ledger_dirty: bool,
    removal_carried_updates: bool,
}

impl Model {
    /// Loaded devices start offline with no live sessions. Generations must be
    /// monotonic across runtime restarts (the ledger records their high-water mark).
    pub fn new(ledger: Ledger, grace: Duration, now: Duration) -> Result<Self, Error> {
        let mut devices = BTreeMap::new();
        let mut incarnations = BTreeSet::new();
        if ledger.next_incarnation == 0 {
            return Err(Error::InvalidLedger);
        }
        for entry in ledger.entries {
            if entry.id.is_empty()
                || entry.incarnation == 0
                || entry.incarnation >= ledger.next_incarnation
                || !incarnations.insert(entry.incarnation)
                || devices.contains_key(&entry.id)
            {
                return Err(Error::InvalidLedger);
            }
            devices.insert(
                entry.id.clone(),
                State {
                    device: Device {
                        entry,
                        session: None,
                        online: false,
                        offline_deadline: None,
                        removal: None,
                    },
                    bridge_active: false,
                    close_target: None,
                },
            );
        }
        Ok(Self {
            devices,
            grace,
            last_now: now,
            next_incarnation: ledger.next_incarnation,
            revision: ledger.revision,
            next_operation: 1,
            storage: None,
            ledger_dirty: false,
            removal_carried_updates: false,
        })
    }

    pub fn devices(&self) -> Vec<Device> {
        self.devices.values().map(|s| s.device.clone()).collect()
    }

    pub fn input(&mut self, input: Input, now: Duration) -> Outcome {
        if now < self.last_now {
            return Outcome {
                actions: vec![],
                error: Some(Error::TimeWentBackwards),
                next_deadline: self.deadline(),
            };
        }
        self.last_now = now;
        let mut actions = vec![];
        // At the exact grace deadline, offline is published before the next input.
        self.expire(now, &mut actions);
        let mut result = match input {
            Input::SessionUp { id, generation } => self.up(id, generation, &mut actions),
            Input::SessionDown { id, session } => {
                if let Some(state) = self.devices.get_mut(&id)
                    && state.device.session == Some(session)
                {
                    state.device.session = None;
                    state.device.offline_deadline = Some(now.saturating_add(self.grace));
                    actions.push(Action::Changed(state.device.clone()));
                }
                Ok(())
            }
            Input::Forget { id, bridge_active } => self.forget(id, bridge_active, &mut actions),
            Input::StepResult {
                id,
                incarnation,
                operation,
                step,
                result,
            } => self.step_result(id, incarnation, operation, step, result, &mut actions),
            Input::LedgerResult { revision, result } => {
                if self.storage == Some(Storage::Ledger(revision)) {
                    self.storage = None;
                    actions.push(Action::LedgerResult { revision, result });
                }
                Ok(())
            }
            Input::Tick => Ok(()),
        };
        self.expire(now, &mut actions);
        if let Err(error) = self.dispatch_storage(&mut actions) {
            result = Err(error);
        }
        Outcome {
            actions,
            error: result.err(),
            next_deadline: self.deadline(),
        }
    }

    // One storage effect in flight. Waiting removals live in per-device state;
    // ordinary updates coalesce into one dirty flag, never an unbounded queue.
    fn dispatch_storage(&mut self, actions: &mut Vec<Action>) -> Result<(), Error> {
        if self.storage.is_some() {
            return Ok(());
        }
        let waiting = self
            .devices
            .iter()
            .filter_map(|(id, state)| {
                if let Some(Removal::Active {
                    operation,
                    step: Step::PersistRemoval,
                }) = state.device.removal
                {
                    Some((
                        operation,
                        id.clone(),
                        state.device.entry.incarnation,
                        state.close_target,
                    ))
                } else {
                    None
                }
            })
            .min_by_key(|entry| entry.0);
        if let Some((operation, id, incarnation, session)) = waiting {
            self.revision = self
                .revision
                .checked_add(1)
                .ok_or(Error::CounterExhausted)?;
            self.storage = Some(Storage::Removal {
                id: id.clone(),
                incarnation,
                operation,
            });
            self.removal_carried_updates = self.ledger_dirty;
            self.ledger_dirty = false;
            actions.push(Action::ForgetStep {
                ledger: Some(self.ledger(Some(&id))),
                id,
                incarnation,
                operation,
                step: Step::PersistRemoval,
                session,
            });
        } else if self.ledger_dirty {
            self.revision = self
                .revision
                .checked_add(1)
                .ok_or(Error::CounterExhausted)?;
            self.storage = Some(Storage::Ledger(self.revision));
            self.ledger_dirty = false;
            actions.push(Action::PersistLedger(self.ledger(None)));
        }
        Ok(())
    }

    fn deadline(&self) -> Option<Duration> {
        self.devices
            .values()
            .filter_map(|s| s.device.offline_deadline)
            .min()
    }

    fn expire(&mut self, now: Duration, actions: &mut Vec<Action>) {
        for state in self.devices.values_mut() {
            if state
                .device
                .offline_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                state.device.offline_deadline = None;
                state.device.online = false;
                actions.push(Action::Offline {
                    id: state.device.entry.id.clone(),
                    incarnation: state.device.entry.incarnation,
                });
                actions.push(Action::Changed(state.device.clone()));
            }
        }
    }

    fn ledger(&self, excluded: Option<&str>) -> Ledger {
        Ledger {
            revision: self.revision,
            next_incarnation: self.next_incarnation,
            entries: self
                .devices
                .iter()
                .filter(|(id, _)| Some(id.as_str()) != excluded)
                .map(|(_, s)| s.device.entry.clone())
                .collect(),
        }
    }

    fn up(&mut self, id: String, generation: u64, actions: &mut Vec<Action>) -> Result<(), Error> {
        if id.is_empty() {
            return Err(Error::InvalidIdentity);
        }
        if let Some(state) = self.devices.get(&id) {
            if state.device.removal.is_some() {
                actions.push(Action::CloseSuperseded {
                    id,
                    session: SessionKey {
                        incarnation: state.device.entry.incarnation,
                        generation,
                    },
                });
                return Err(Error::Quiesced);
            }
            if generation <= state.device.entry.last_generation {
                return Err(Error::StaleGeneration);
            }
        }
        self.revision
            .checked_add(1)
            .ok_or(Error::CounterExhausted)?;
        if !self.devices.contains_key(&id) {
            let incarnation = self.next_incarnation;
            self.next_incarnation = incarnation.checked_add(1).ok_or(Error::CounterExhausted)?;
            self.devices.insert(
                id.clone(),
                State {
                    device: Device {
                        entry: Entry {
                            id: id.clone(),
                            incarnation,
                            last_generation: generation,
                        },
                        session: None,
                        online: false,
                        offline_deadline: None,
                        removal: None,
                    },
                    bridge_active: false,
                    close_target: None,
                },
            );
        }
        let state = self.devices.get_mut(&id).expect("inserted device");
        if let Some(old) = state.device.session {
            actions.push(Action::CloseSuperseded {
                id: id.clone(),
                session: old,
            });
        }
        state.device.entry.last_generation = generation;
        state.device.session = Some(SessionKey {
            incarnation: state.device.entry.incarnation,
            generation,
        });
        state.device.offline_deadline = None;
        if !state.device.online {
            state.device.online = true;
            actions.push(Action::Online {
                id: id.clone(),
                incarnation: state.device.entry.incarnation,
            });
        }
        actions.push(Action::Changed(state.device.clone()));
        self.ledger_dirty = true;
        Ok(())
    }

    fn forget(
        &mut self,
        id: String,
        bridge_active: bool,
        actions: &mut Vec<Action>,
    ) -> Result<(), Error> {
        let state = self.devices.get(&id).ok_or(Error::UnknownDevice)?;
        let step = match &state.device.removal {
            Some(Removal::Active { .. }) => return Err(Error::Busy),
            Some(Removal::Failed { step, .. }) => *step,
            None => Step::Close,
        };
        // Check resources before quiescing so counter exhaustion cannot strand an operation.
        if self.next_operation == u64::MAX || self.revision == u64::MAX {
            return Err(Error::CounterExhausted);
        }
        let state = self.devices.get_mut(&id).expect("device checked");
        if state.device.removal.is_none() {
            state.bridge_active = bridge_active;
            state.close_target = state.device.session.take();
            state.device.offline_deadline = None;
            if state.device.online {
                state.device.online = false;
                actions.push(Action::Offline {
                    id: id.clone(),
                    incarnation: state.device.entry.incarnation,
                });
            }
        }
        self.begin_step(&id, step, actions)
    }

    fn begin_step(&mut self, id: &str, step: Step, actions: &mut Vec<Action>) -> Result<(), Error> {
        let operation = self.next_operation;
        let next_operation = operation.checked_add(1).ok_or(Error::CounterExhausted)?;
        self.next_operation = next_operation;
        let state = self.devices.get_mut(id).expect("active removal device");
        state.device.removal = Some(Removal::Active { operation, step });
        actions.push(Action::Changed(state.device.clone()));
        if step == Step::PersistRemoval {
            return Ok(());
        }
        actions.push(Action::ForgetStep {
            id: id.to_owned(),
            incarnation: state.device.entry.incarnation,
            operation,
            step,
            session: state.close_target,
            ledger: None,
        });
        Ok(())
    }

    fn step_result(
        &mut self,
        id: String,
        incarnation: u64,
        operation: u64,
        step: Step,
        result: Result<(), String>,
        actions: &mut Vec<Action>,
    ) -> Result<(), Error> {
        let Some(state) = self.devices.get_mut(&id) else {
            return Ok(());
        };
        if state.device.entry.incarnation != incarnation
            || state.device.removal != Some(Removal::Active { operation, step })
        {
            return Ok(());
        }
        if step == Step::PersistRemoval {
            let expected = Storage::Removal {
                id: id.clone(),
                incarnation,
                operation,
            };
            if self.storage != Some(expected) {
                return Ok(());
            }
            self.storage = None;
            if result.is_err() {
                self.ledger_dirty |= self.removal_carried_updates;
            }
            self.removal_carried_updates = false;
        }
        if let Err(reason) = result {
            state.device.removal = Some(Removal::Failed {
                step,
                reason: reason.clone(),
            });
            actions.push(Action::ForgetFailed {
                id,
                incarnation,
                step,
                reason,
            });
            actions.push(Action::Changed(state.device.clone()));
            return Ok(());
        }
        if step == Step::PersistRemoval {
            self.devices.remove(&id);
            actions.push(Action::Removed { id, incarnation });
            return Ok(());
        }
        let next = if step == Step::Close && state.bridge_active {
            Step::Deregister
        } else {
            Step::PersistRemoval
        };
        // A resource failure is also a reported failure, never a stuck active step.
        if let Err(error) = self.begin_step(&id, next, actions) {
            let reason = error.to_string();
            let state = self.devices.get_mut(&id).expect("removal device");
            state.device.removal = Some(Removal::Failed {
                step: next,
                reason: reason.clone(),
            });
            actions.push(Action::ForgetFailed {
                id,
                incarnation,
                step: next,
                reason,
            });
            actions.push(Action::Changed(state.device.clone()));
            return Err(error);
        }
        Ok(())
    }
}
