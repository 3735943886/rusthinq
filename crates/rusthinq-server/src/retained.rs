//! Owned retained records and explicit cleanup manifests. No network publication.
use std::collections::{BTreeMap, BTreeSet};

/// Includes the device ID plus incarnation and namespace metadata.
pub const MAX_OWNER_BYTES: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Capacity,
    OwnerConflict,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub owner: String,
    pub topic: String,
    pub payload: Vec<u8>,
}
/// Publish this empty payload with RETAIN=1 to remove a remote retained record.
/// Local removal alone does not confirm that a remote broker has deleted it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tombstone {
    pub owner: String,
    pub topic: String,
}
impl Tombstone {
    pub fn frame(&self, maximum: usize) -> Result<Vec<u8>, rusthinq_protocol::mqtt::Error> {
        let mut frame = rusthinq_protocol::mqtt::publish(&self.topic, &[], maximum)?;
        frame[0] |= 1;
        Ok(frame)
    }
}

pub struct Store {
    records: BTreeMap<String, Record>,
    max_records: usize,
    max_bytes: usize,
    bytes: usize,
}
impl Store {
    pub fn new(max_records: usize, max_bytes: usize) -> Result<Self, Error> {
        if max_records == 0 || max_bytes == 0 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            records: BTreeMap::new(),
            max_records,
            max_bytes,
            bytes: 0,
        })
    }
    /// Every record has an owner, making device/adapter removal enumerable.
    /// Empty retained payload deletes; empty values are never stored for replay.
    pub fn put(
        &mut self,
        owner: &str,
        topic: &str,
        payload: &[u8],
    ) -> Result<Option<Tombstone>, Error> {
        if owner.is_empty()
            || owner.len() > MAX_OWNER_BYTES
            || owner.chars().any(char::is_control)
            || topic.is_empty()
            || topic.len() > 1024
            || topic.contains(['+', '#'])
            || topic.chars().any(char::is_control)
        {
            return Err(Error::Invalid);
        }
        if self
            .records
            .get(topic)
            .is_some_and(|record| record.owner != owner)
        {
            return Err(Error::OwnerConflict);
        }
        if payload.is_empty() {
            // Return a deletion even if no local record exists: remote state may survive restart.
            let _ = self.remove(owner, topic)?;
            return Ok(Some(Tombstone {
                owner: owner.into(),
                topic: topic.into(),
            }));
        }
        let previous = self.records.get(topic).map_or(0, size);
        let added = owner
            .len()
            .checked_add(topic.len())
            .and_then(|n| n.checked_add(payload.len()))
            .ok_or(Error::Capacity)?;
        let next = (self.bytes - previous)
            .checked_add(added)
            .ok_or(Error::Capacity)?;
        if next > self.max_bytes
            || !self.records.contains_key(topic) && self.records.len() >= self.max_records
        {
            return Err(Error::Capacity);
        }
        self.records.insert(
            topic.into(),
            Record {
                owner: owner.into(),
                topic: topic.into(),
                payload: payload.into(),
            },
        );
        self.bytes = next;
        Ok(None)
    }
    pub fn remove(&mut self, owner: &str, topic: &str) -> Result<Option<Tombstone>, Error> {
        if self
            .records
            .get(topic)
            .is_some_and(|record| record.owner != owner)
        {
            return Err(Error::OwnerConflict);
        }
        Ok(self.records.remove(topic).map(|record| {
            self.bytes -= size(&record);
            Tombstone {
                owner: record.owner,
                topic: record.topic,
            }
        }))
    }
    pub fn remove_owner(&mut self, owner: &str) -> Vec<Tombstone> {
        let topics: Vec<_> = self
            .records
            .values()
            .filter(|record| record.owner == owner)
            .map(|record| record.topic.clone())
            .collect();
        topics
            .into_iter()
            .filter_map(|topic| self.remove(owner, &topic).expect("matching owner"))
            .collect()
    }
    pub fn clear(&mut self) -> Vec<Tombstone> {
        self.bytes = 0;
        std::mem::take(&mut self.records)
            .into_values()
            .map(|record| Tombstone {
                owner: record.owner,
                topic: record.topic,
            })
            .collect()
    }
    /// Owner-scoped replay prevents wildcard subscriptions crossing device ownership.
    pub fn replay(&self, owner: &str, filters: &[String]) -> Vec<Record> {
        self.records
            .values()
            .filter(|record| {
                record.owner == owner
                    && filters
                        .iter()
                        .any(|filter| rusthinq_protocol::mqtt::matches(filter, &record.topic))
            })
            .cloned()
            .collect()
    }
    pub fn snapshot(&self) -> Vec<Record> {
        self.records.values().cloned().collect()
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}
fn size(record: &Record) -> usize {
    record.owner.len() + record.topic.len() + record.payload.len()
}

/// Recoverable deletion work. The application must durably commit checkpoints
/// before sending; this pure ledger deliberately does not own filesystem writes.
#[derive(Clone)]
pub struct Cleanup {
    capacity: usize,
    next: u64,
    pending: BTreeMap<String, (Tombstone, Option<u64>)>,
    deleting: BTreeSet<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attempt {
    pub token: u64,
    pub deletion: Tombstone,
}
impl Cleanup {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            capacity,
            next: 0,
            pending: BTreeMap::new(),
            deleting: BTreeSet::new(),
        })
    }
    /// Atomic batch admission; replacement invalidates any older send result.
    pub fn enqueue(&mut self, deletions: &[Tombstone]) -> Result<(), Error> {
        if deletions.len() > self.capacity {
            return Err(Error::Capacity);
        }
        let mut staged = self.pending.clone();
        for deletion in deletions {
            let mut validation = Store::new(1, 1)?;
            validation.put(&deletion.owner, &deletion.topic, &[])?;
            if staged
                .get(&deletion.topic)
                .is_some_and(|(old, _)| old.owner != deletion.owner)
            {
                return Err(Error::OwnerConflict);
            }
            staged.insert(deletion.topic.clone(), (deletion.clone(), None));
            if staged.len() > self.capacity {
                return Err(Error::Capacity);
            }
        }
        self.pending = staged;
        Ok(())
    }
    /// Hand an inventoried topic from `from` to a new owner. Refused while a deletion of
    /// the topic is requested or in flight, or when `from` does not own it.
    pub fn transfer(&mut self, from: &str, to: Tombstone) -> Result<(), Error> {
        let mut validation = Store::new(1, 1)?;
        validation.put(&to.owner, &to.topic, &[])?;
        if self.deleting.contains(&to.topic)
            || self
                .pending
                .get(&to.topic)
                .is_none_or(|(owned, _)| owned.owner != from)
        {
            return Err(Error::OwnerConflict);
        }
        self.pending.insert(to.topic.clone(), (to, None));
        Ok(())
    }
    pub fn owner(&self, topic: &str) -> Option<&str> {
        self.pending
            .get(topic)
            .map(|(deletion, _)| deletion.owner.as_str())
    }
    /// Allocate a fresh token, including retries. Commit checkpoint before send.
    pub fn begin(&mut self, topic: &str) -> Result<Attempt, Error> {
        let (deletion, attempt) = self.pending.get_mut(topic).ok_or(Error::Invalid)?;
        let next = self.next.checked_add(1).ok_or(Error::Capacity)?;
        self.next = next;
        self.deleting.insert(topic.into());
        *attempt = Some(next);
        Ok(Attempt {
            token: next,
            deletion: deletion.clone(),
        })
    }
    /// Only broker-confirmed completion removes work. Local QoS0 Sent is insufficient.
    /// A failure/unknown outcome leaves the deletion queued for a fresh attempt.
    pub fn complete(&mut self, attempt: &Attempt, broker_confirmed: bool) -> bool {
        let matches = self
            .pending
            .get(&attempt.deletion.topic)
            .is_some_and(|(deletion, token)| {
                deletion == &attempt.deletion && *token == Some(attempt.token)
            });
        if !matches {
            return false;
        }
        if broker_confirmed {
            self.pending.remove(&attempt.deletion.topic);
            self.deleting.remove(&attempt.deletion.topic);
        } else if let Some((_, token)) = self.pending.get_mut(&attempt.deletion.topic) {
            *token = None;
        }
        true
    }
    pub fn pending(&self) -> Vec<Tombstone> {
        self.pending
            .values()
            .map(|(deletion, _)| deletion.clone())
            .collect()
    }
    /// Persist the complete deletion intent before sending any member of a batch.
    pub fn request_delete(&mut self, deletions: &[Tombstone]) -> Result<(), Error> {
        if deletions.iter().any(|deletion| {
            self.pending
                .get(&deletion.topic)
                .is_none_or(|(owned, _)| owned != deletion)
        }) {
            return Err(Error::OwnerConflict);
        }
        for deletion in deletions {
            self.deleting.insert(deletion.topic.clone());
            self.pending
                .get_mut(&deletion.topic)
                .expect("validated owner")
                .1 = None;
        }
        Ok(())
    }
    pub fn requested(&self) -> Vec<Tombstone> {
        self.deleting
            .iter()
            .filter_map(|topic| {
                self.pending
                    .get(topic)
                    .map(|(deletion, _)| deletion.clone())
            })
            .collect()
    }
    pub fn checkpoint(&self) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"version":2,"next":self.next,"pending":self.pending.values().map(|(deletion, _)| serde_json::json!({"owner":deletion.owner,"topic":deletion.topic})).collect::<Vec<_>>(),"deleting":self.deleting})).expect("JSON values")
    }
    /// Reject malformed/unbounded recovery data; interrupted attempts are never restored.
    pub fn restore(capacity: usize, bytes: &[u8]) -> Result<Self, Error> {
        let maximum = capacity
            .checked_mul(8192)
            .and_then(|n| n.checked_add(128))
            .ok_or(Error::Capacity)?;
        if bytes.len() > maximum {
            return Err(Error::Capacity);
        }
        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| Error::Invalid)?;
        let version = value["version"].as_u64();
        if !matches!(version, Some(1 | 2)) {
            return Err(Error::Invalid);
        }
        let mut ledger = Self::new(capacity)?;
        ledger.next = value["next"].as_u64().ok_or(Error::Invalid)?;
        let records = value["pending"].as_array().ok_or(Error::Invalid)?;
        if records.len() > capacity {
            return Err(Error::Capacity);
        }
        for record in records {
            let deletion = Tombstone {
                owner: record["owner"].as_str().ok_or(Error::Invalid)?.into(),
                topic: record["topic"].as_str().ok_or(Error::Invalid)?.into(),
            };
            if ledger.pending.contains_key(&deletion.topic) {
                return Err(Error::Invalid);
            }
            ledger.enqueue(&[deletion])?;
        }
        if version == Some(2) {
            let deleting = value["deleting"].as_array().ok_or(Error::Invalid)?;
            if deleting.len() > capacity {
                return Err(Error::Capacity);
            }
            for topic in deleting {
                let topic = topic.as_str().ok_or(Error::Invalid)?;
                if !ledger.pending.contains_key(topic) || !ledger.deleting.insert(topic.into()) {
                    return Err(Error::Invalid);
                }
            }
        }
        Ok(ledger)
    }
}
