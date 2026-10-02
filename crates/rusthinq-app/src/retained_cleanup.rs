//! Durable retained deletion ledger. Run blocking filesystem calls off async I/O tasks.
use rusthinq_server::retained::{Attempt, Cleanup, Error, Tombstone};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub struct Ledger {
    path: PathBuf,
    parent: PathBuf,
    _lock: File,
    cleanup: Cleanup,
    uncertain: bool,
}
impl Ledger {
    /// Requires an existing parent directory. An invalid checkpoint is never overwritten.
    /// The sidecar lock is held for this owner's lifetime and must not be unlinked.
    pub fn open(path: &Path, capacity: usize) -> io::Result<Self> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
        if path.file_name().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint filename required",
            ));
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(PathBuf::from(lock_path))?;
        lock.try_lock().map_err(io::Error::other)?;
        let maximum = capacity
            .checked_mul(8192)
            .and_then(|n| n.checked_add(128))
            .ok_or_else(|| policy(Error::Capacity))?;
        let cleanup = match File::open(path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
                Cleanup::restore(capacity, &bytes).map_err(policy)?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Cleanup::new(capacity).map_err(policy)?
            }
            Err(error) => return Err(error),
        };
        Ok(Self {
            path: path.into(),
            parent,
            _lock: lock,
            cleanup,
            uncertain: false,
        })
    }
    fn commit(&mut self, candidate: Cleanup) -> io::Result<()> {
        if self.uncertain {
            return Err(io::Error::other(
                "checkpoint outcome uncertain; reopen before retry",
            ));
        }
        let result = (|| {
            let mut temporary = tempfile::Builder::new()
                .prefix(".retained-cleanup-")
                .tempfile_in(&self.parent)?;
            temporary.write_all(&candidate.checkpoint())?;
            temporary.as_file().sync_all()?;
            temporary.persist(&self.path).map_err(|error| error.error)?;
            File::open(&self.parent)?.sync_all()?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.cleanup = candidate;
                Ok(())
            }
            Err(error) => {
                self.uncertain = true;
                Err(error)
            }
        }
    }
    /// Record publication ownership without exposing the lower-layer cleanup model.
    pub fn inventory_topic(&mut self, owner: String, topic: String) -> io::Result<()> {
        self.enqueue(&[Tombstone { owner, topic }])
    }
    pub fn enqueue(&mut self, deletions: &[Tombstone]) -> io::Result<()> {
        let mut candidate = self.cleanup.clone();
        candidate.enqueue(deletions).map_err(policy)?;
        self.commit(candidate)
    }
    /// Returns a send token only after work and its high-water mark are durable.
    pub fn begin(&mut self, topic: &str) -> io::Result<Attempt> {
        let mut candidate = self.cleanup.clone();
        let attempt = candidate.begin(topic).map_err(policy)?;
        self.commit(candidate)?;
        Ok(attempt)
    }
    pub fn complete(&mut self, attempt: &Attempt, broker_confirmed: bool) -> io::Result<bool> {
        if self.uncertain {
            return Err(io::Error::other(
                "checkpoint outcome uncertain; reopen before completion",
            ));
        }
        let mut candidate = self.cleanup.clone();
        if !candidate.complete(attempt, broker_confirmed) {
            return Ok(false);
        }
        self.commit(candidate)?;
        Ok(true)
    }
    pub fn pending(&self) -> Vec<Tombstone> {
        self.cleanup.pending()
    }
    pub fn request_delete(&mut self, deletions: &[Tombstone]) -> io::Result<()> {
        let mut candidate = self.cleanup.clone();
        candidate.request_delete(deletions).map_err(policy)?;
        self.commit(candidate)
    }
    pub fn requested(&self) -> Vec<Tombstone> {
        self.cleanup.requested()
    }
    pub fn requires_reopen(&self) -> bool {
        self.uncertain
    }
}
fn policy(error: Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("cleanup ledger: {error:?}"),
    )
}
