//! Durable pairing intent inventory. Callers run filesystem operations off I/O tasks.
use rusthinq_bridge::pairing::Material;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Owner {
    pub device: String,
    pub incarnation: u64,
    pub account: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub owner: Owner,
    pub sequence: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub attempt: Attempt,
    pub material: Option<Material>,
    #[serde(default)]
    pub enabled: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    schema: u8,
    next: u64,
    records: BTreeMap<String, Record>,
}
pub struct Store {
    path: PathBuf,
    parent: PathBuf,
    _lock: File,
    checkpoint: Checkpoint,
    capacity: usize,
    uncertain: bool,
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid pairing ownership checkpoint",
    )
}
fn owner(owner: &Owner) -> io::Result<()> {
    if owner.incarnation == 0
        || [&owner.device, &owner.account].iter().any(|value| {
            value.is_empty() || value.len() > 256 || value.chars().any(char::is_control)
        })
    {
        return Err(invalid());
    }
    Ok(())
}
impl Store {
    pub fn open(path: &Path, capacity: usize) -> io::Result<Self> {
        if capacity == 0 || capacity > 256 || path.file_name().is_none() {
            return Err(invalid());
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(PathBuf::from(lock_path))?;
        lock.try_lock().map_err(io::Error::other)?;
        let maximum = capacity * 262144 + 1024;
        let checkpoint = match File::open(path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take((maximum + 1) as u64).read_to_end(&mut bytes)?;
                if bytes.len() > maximum {
                    return Err(invalid());
                }
                let saved: Checkpoint = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if saved.schema != 1 || saved.records.len() > capacity {
                    return Err(invalid());
                }
                for (id, record) in &saved.records {
                    owner(&record.attempt.owner)?;
                    if id != &record.attempt.owner.device
                        || record.attempt.sequence == 0
                        || record.attempt.sequence > saved.next
                    {
                        return Err(invalid());
                    }
                    if let Some(material) = &record.material {
                        material.validate_stored().map_err(|_| invalid())?;
                    }
                }
                saved
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Checkpoint {
                schema: 1,
                next: 0,
                records: BTreeMap::new(),
            },
            Err(error) => return Err(error),
        };
        Ok(Self {
            path: path.into(),
            parent,
            _lock: lock,
            checkpoint,
            capacity,
            uncertain: false,
        })
    }
    pub fn snapshot(&self) -> Vec<Record> {
        self.checkpoint.records.values().cloned().collect()
    }
    /// Persist before the first remote pairing mutation. Unknown outcomes remain owned.
    /// An existing intent must be reconciled explicitly; it is never retried implicitly.
    pub fn begin(&mut self, ownership: Owner) -> io::Result<Attempt> {
        owner(&ownership)?;
        if self.checkpoint.records.contains_key(&ownership.device) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "pairing intent already owned",
            ));
        }
        if self.checkpoint.records.len() == self.capacity {
            return Err(invalid());
        }
        let mut next = self.checkpoint.clone();
        next.next = next.next.checked_add(1).ok_or_else(invalid)?;
        let attempt = Attempt {
            owner: ownership,
            sequence: next.next,
        };
        next.records.insert(
            attempt.owner.device.clone(),
            Record {
                attempt: attempt.clone(),
                material: None,
                enabled: false,
            },
        );
        self.commit(next)?;
        Ok(attempt)
    }
    /// Account inventory has confirmed this registration. Import in one durable
    /// write, always disabled; an existing intent must never be overwritten.
    pub fn adopt(&mut self, ownership: Owner, material: Material) -> io::Result<()> {
        owner(&ownership)?;
        material.validate_stored().map_err(|_| invalid())?;
        if self.checkpoint.records.contains_key(&ownership.device) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "pairing intent already owned",
            ));
        }
        if self.checkpoint.records.len() == self.capacity {
            return Err(invalid());
        }
        let mut next = self.checkpoint.clone();
        next.next = next.next.checked_add(1).ok_or_else(invalid)?;
        let attempt = Attempt {
            owner: ownership,
            sequence: next.next,
        };
        next.records.insert(
            attempt.owner.device.clone(),
            Record {
                attempt,
                material: Some(material),
                enabled: false,
            },
        );
        self.commit(next)
    }
    pub fn complete(&mut self, attempt: &Attempt, material: Material) -> io::Result<()> {
        material.validate().map_err(|_| invalid())?;
        self.current(attempt)?;
        if self.checkpoint.records[&attempt.owner.device]
            .material
            .is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "pairing attempt already completed",
            ));
        }
        let mut next = self.checkpoint.clone();
        next.records
            .get_mut(&attempt.owner.device)
            .ok_or_else(invalid)?
            .material = Some(material);
        self.commit(next)
    }
    /// Durable enable intent is separate from remote registration ownership.
    pub fn set_enabled(&mut self, attempt: &Attempt, enabled: bool) -> io::Result<()> {
        self.current(attempt)?;
        if enabled
            && self.checkpoint.records[&attempt.owner.device]
                .material
                .is_none()
        {
            return Err(invalid());
        }
        let mut next = self.checkpoint.clone();
        next.records
            .get_mut(&attempt.owner.device)
            .ok_or_else(invalid)?
            .enabled = enabled;
        self.commit(next)
    }
    /// Call only after a successful cloud removal; failure preserves cleanup ownership.
    pub fn remove_confirmed(&mut self, attempt: &Attempt, confirmed: bool) -> io::Result<()> {
        self.current(attempt)?;
        if !confirmed {
            return Err(io::Error::other("remote pairing removal unconfirmed"));
        }
        let mut next = self.checkpoint.clone();
        next.records.remove(&attempt.owner.device);
        self.commit(next)
    }
    fn current(&self, attempt: &Attempt) -> io::Result<()> {
        if !self
            .checkpoint
            .records
            .get(&attempt.owner.device)
            .is_some_and(|record| record.attempt == *attempt)
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn commit(&mut self, next: Checkpoint) -> io::Result<()> {
        if self.uncertain {
            return Err(io::Error::other(
                "pairing checkpoint uncertain; reopen required",
            ));
        }
        let bytes = serde_json::to_vec(&next).map_err(|_| invalid())?;
        if bytes.len() > self.capacity * 262144 + 1024 {
            return Err(invalid());
        }
        let result = (|| {
            let mut file = tempfile::Builder::new()
                .prefix(".pairing-")
                .tempfile_in(&self.parent)?;
            file.write_all(&bytes)?;
            file.as_file().sync_all()?;
            file.persist(&self.path).map_err(|error| error.error)?;
            File::open(&self.parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.uncertain = true;
        } else {
            self.checkpoint = next;
        }
        result
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn ownership(incarnation: u64) -> Owner {
        Owner {
            device: "d".into(),
            incarnation,
            account: "account".into(),
        }
    }
    fn material() -> Material {
        Material::ThinQ1 {
            http_server: "https://cloud.example".into(),
            rti_server: "rti.example:5222".into(),
        }
    }
    #[test]
    fn pending_intents_survive_restart_and_removal_requires_exact_confirmation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pairing.json");
        let mut store = Store::open(&path, 4).unwrap();
        assert!(Store::open(&path, 4).is_err());
        let first = store.begin(ownership(1)).unwrap();
        drop(store);
        let mut store = Store::open(&path, 4).unwrap();
        assert!(store.snapshot()[0].material.is_none());
        assert!(store.begin(ownership(2)).is_err());
        store.complete(&first, material()).unwrap();
        assert!(store.complete(&first, material()).is_err());
        assert!(store.remove_confirmed(&first, false).is_err());
        drop(store);
        let mut store = Store::open(&path, 4).unwrap();
        assert!(store.snapshot()[0].material.is_some());
        store.remove_confirmed(&first, true).unwrap();
        let second = store.begin(ownership(2)).unwrap();
        assert!(second.sequence > first.sequence);
        assert!(store.complete(&first, material()).is_err());
        assert!(store.remove_confirmed(&first, true).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    #[test]
    fn adopted_registration_is_atomic_disabled_and_never_replaces_existing_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("adopted.json");
        let mut store = Store::open(&path, 4).unwrap();
        store.adopt(ownership(1), material()).unwrap();
        let record = store.snapshot().remove(0);
        assert!(!record.enabled);
        assert!(record.material.is_some());
        assert!(store.adopt(ownership(2), material()).is_err());
        drop(store);
        let mut restored = Store::open(&path, 4).unwrap();
        assert_eq!(restored.snapshot()[0].attempt, record.attempt);
        assert!(!restored.snapshot()[0].enabled);
        restored.set_enabled(&record.attempt, true).unwrap();
        drop(restored);
        let mut restored = Store::open(&path, 4).unwrap();
        assert!(restored.snapshot()[0].enabled);
        let mut wrong = record.attempt.clone();
        wrong.owner.account = "different-account".into();
        assert!(restored.set_enabled(&wrong, false).is_err());
        assert!(restored.snapshot()[0].enabled);
    }
    #[test]
    fn corrupt_checkpoints_are_preserved_and_write_failure_blocks_retries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pairing.json");
        std::fs::write(&path, b"unknown").unwrap();
        assert!(Store::open(&path, 1).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"unknown");
        std::fs::remove_file(&path).unwrap();
        let mut store = Store::open(&path, 1).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(store.begin(ownership(1)).is_err());
        std::fs::remove_dir(&path).unwrap();
        assert!(store.begin(ownership(1)).is_err());
        assert!(!path.exists());
    }
}
