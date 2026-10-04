//! Durable L2 device ledger and process-wide L3 generation high-water mark.
use rusthinq_lifecycle::{Action, Entry, Input, Ledger, Model, Step};
use serde_json::{Value, json};
use std::{io, path::Path, time::Duration};

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceMetadata {
    pub incarnation: u64,
    pub model_name: String,
    pub device_type: String,
    pub thinq2: bool,
    #[serde(default)]
    pub model_id: String,
    #[serde(default)]
    pub sw_version: String,
    #[serde(default)]
    pub last_seen_unix: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    pub metadata: std::collections::BTreeMap<String, DeviceMetadata>,
    pub ledger: Ledger,
    /// Persisted high-water mark, including unused reservations. Without a new
    /// reservation use it as L3's floor; a new block supplies its own floor/ceiling.
    pub generation_floor: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationBlock {
    pub floor: u64,
    pub ceiling: u64,
}

pub struct Storage {
    file: crate::checkpoint::Checkpoint,
    capacity: usize,
    state: State,
}
impl Storage {
    /// A missing file initializes empty state. Corrupt or incompatible files fail
    /// explicitly and are preserved; no automatic migration from 0.1 is attempted.
    pub fn open(path: &Path, capacity: usize) -> io::Result<Self> {
        if capacity == 0 || path.file_name().is_none() {
            return Err(invalid("invalid lifecycle storage configuration"));
        }
        let maximum = capacity
            .checked_mul(8192)
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| invalid("lifecycle capacity overflow"))?;
        let file = crate::checkpoint::Checkpoint::open(path)?;
        let state = match file.read(maximum)? {
            Some(bytes) => decode(&bytes, capacity)?,
            None => State {
                ledger: Ledger::default(),
                generation_floor: 0,
                metadata: Default::default(),
            },
        };
        Ok(Self {
            file,
            capacity,
            state,
        })
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn requires_reopen(&self) -> bool {
        self.file.requires_reopen()
    }
    /// Reserve before transport admission. Unused numbers are intentionally lost
    /// after restart; no generation in this range is issued by a later process.
    /// Run on the application-owned blocking worker.
    pub fn reserve_generations(&mut self, count: u64) -> io::Result<GenerationBlock> {
        self.file
            .ensure_ready("lifecycle checkpoint uncertain; reopen before reservation")?;
        if count == 0 {
            return Err(invalid("zero generation reservation"));
        }
        let floor = self.state.generation_floor;
        let ceiling = floor
            .checked_add(count)
            .ok_or_else(|| invalid("generation reservation exhausted"))?;
        let candidate = State {
            ledger: self.state.ledger.clone(),
            generation_floor: ceiling,
            metadata: self.state.metadata.clone(),
        };
        self.commit(candidate)?;
        Ok(GenerationBlock { floor, ceiling })
    }

    /// Execute only L2 storage effects. Caller feeds the result back to its model.
    /// Run on the application-owned blocking worker; other effects have other owners.
    pub fn execute(&mut self, action: &Action) -> Option<Input> {
        match action {
            Action::PersistLedger(ledger) => Some(Input::LedgerResult {
                revision: ledger.revision,
                result: self.save(ledger).map_err(|error| error.to_string()),
            }),
            Action::ForgetStep {
                id,
                incarnation,
                operation,
                step: Step::PersistRemoval,
                ledger,
                ..
            } => Some(Input::StepResult {
                id: id.clone(),
                incarnation: *incarnation,
                operation: *operation,
                step: Step::PersistRemoval,
                result: ledger
                    .as_ref()
                    .ok_or_else(|| invalid("removal ledger missing"))
                    .and_then(|ledger| self.save(ledger))
                    .map_err(|error| error.to_string()),
            }),
            _ => None,
        }
    }

    /// Acknowledge L2 persistence effects only after this returns success.
    /// Same-revision identical retries are permitted; conflicting/stale writes fail.
    pub fn save(&mut self, ledger: &Ledger) -> io::Result<()> {
        self.file
            .ensure_ready("lifecycle checkpoint uncertain; reopen before retry")?;
        validate(ledger, self.capacity)?;
        if ledger.revision < self.state.ledger.revision
            || ledger.next_incarnation < self.state.ledger.next_incarnation
            || ledger.revision == self.state.ledger.revision && ledger != &self.state.ledger
        {
            return Err(invalid("stale or conflicting lifecycle checkpoint"));
        }
        let floor = ledger
            .entries
            .iter()
            .map(|entry| entry.last_generation)
            .max()
            .unwrap_or(0)
            .max(self.state.generation_floor);
        let candidate = State {
            ledger: ledger.clone(),
            generation_floor: floor,
            metadata: self
                .state
                .metadata
                .iter()
                .filter(|(id, meta)| {
                    ledger
                        .entries
                        .iter()
                        .any(|entry| entry.id == **id && entry.incarnation == meta.incarnation)
                })
                .map(|(id, meta)| (id.clone(), meta.clone()))
                .collect(),
        };
        self.commit(candidate)
    }
    /// Model data shares the ledger's exclusive serialized writer and incarnation fence.
    pub fn save_metadata(
        &mut self,
        updates: &std::collections::BTreeMap<String, DeviceMetadata>,
    ) -> io::Result<()> {
        self.file
            .ensure_ready("lifecycle checkpoint uncertain; reopen before metadata write")?;
        let mut candidate = self.state.clone();
        for (id, meta) in updates {
            if !candidate
                .ledger
                .entries
                .iter()
                .any(|entry| entry.id == *id && entry.incarnation == meta.incarnation)
            {
                return Err(invalid("stale model incarnation"));
            }
            candidate.metadata.insert(id.clone(), meta.clone());
        }
        validate_metadata(&candidate)?;
        self.commit(candidate)
    }
    fn commit(&mut self, candidate: State) -> io::Result<()> {
        let bytes = encode(&candidate);
        self.file.replace(
            &bytes,
            ".lifecycle-",
            "lifecycle checkpoint uncertain; reopen before retry",
        )?;
        self.state = candidate;
        Ok(())
    }
}

fn validate(ledger: &Ledger, capacity: usize) -> io::Result<()> {
    if ledger.entries.len() > capacity || ledger.entries.iter().any(|entry| entry.id.len() > 256) {
        return Err(invalid("lifecycle device capacity or identity exceeded"));
    }
    Model::new(ledger.clone(), Duration::ZERO, Duration::ZERO)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(())
}
fn encode(state: &State) -> Vec<u8> {
    serde_json::to_vec(&json!({"version":2,"metadata":state.metadata,"generation_floor":state.generation_floor,"revision":state.ledger.revision,"next_incarnation":state.ledger.next_incarnation,
        "entries":state.ledger.entries.iter().map(|entry| json!({"id":entry.id,"incarnation":entry.incarnation,"last_generation":entry.last_generation})).collect::<Vec<_>>()})).expect("JSON values")
}
fn decode(bytes: &[u8], capacity: usize) -> io::Result<State> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| invalid("invalid lifecycle JSON"))?;
    if !matches!(value["version"].as_u64(), Some(1 | 2)) {
        return Err(invalid("unsupported lifecycle version"));
    }
    let entries = value["entries"]
        .as_array()
        .ok_or_else(|| invalid("missing lifecycle entries"))?;
    if entries.len() > capacity {
        return Err(invalid("lifecycle capacity exceeded"));
    }
    let mut ledger = Ledger {
        revision: number(&value, "revision")?,
        next_incarnation: number(&value, "next_incarnation")?,
        entries: Vec::with_capacity(entries.len()),
    };
    for entry in entries {
        let id = entry["id"]
            .as_str()
            .ok_or_else(|| invalid("invalid lifecycle identity"))?;
        ledger.entries.push(Entry {
            id: id.into(),
            incarnation: number(entry, "incarnation")?,
            last_generation: number(entry, "last_generation")?,
        });
    }
    validate(&ledger, capacity)?;
    let generation_floor = number(&value, "generation_floor")?;
    if ledger
        .entries
        .iter()
        .any(|entry| entry.last_generation > generation_floor)
    {
        return Err(invalid("generation floor below saved session"));
    }
    let metadata = if value["version"].as_u64() == Some(2) {
        serde_json::from_value(value["metadata"].clone())
            .map_err(|_| invalid("invalid model metadata"))?
    } else {
        Default::default()
    };
    let state = State {
        ledger,
        generation_floor,
        metadata,
    };
    validate_metadata(&state)?;
    Ok(state)
}
fn validate_metadata(state: &State) -> io::Result<()> {
    if state.metadata.len() > state.ledger.entries.len()
        || state.metadata.iter().any(|(id, meta)| {
            meta.model_name.is_empty()
                || meta.model_name.len() > 256
                || meta.model_name.chars().any(char::is_control)
                || meta.model_id.len() > 256
                || meta.model_id.chars().any(char::is_control)
                || meta.sw_version.len() > 256
                || meta.sw_version.chars().any(char::is_control)
                || meta.device_type.len() > 128
                || meta.device_type.chars().any(char::is_control)
                || !state
                    .ledger
                    .entries
                    .iter()
                    .any(|entry| entry.id == *id && entry.incarnation == meta.incarnation)
        })
    {
        return Err(invalid("invalid model metadata identity or bounds"));
    }
    Ok(())
}
fn number(value: &Value, field: &str) -> io::Result<u64> {
    value[field]
        .as_u64()
        .ok_or_else(|| invalid("missing or invalid lifecycle counter"))
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
