//! Durable retained deletion ledger. Run blocking filesystem calls off async I/O tasks.
use rusthinq_server::retained::{Attempt, Cleanup, Error, Tombstone};
use std::{io, path::Path};

pub struct Ledger {
    file: crate::checkpoint::Checkpoint,
    cleanup: Cleanup,
}
impl Ledger {
    /// Requires an existing parent directory. An invalid checkpoint is never overwritten.
    /// The sidecar lock is held for this owner's lifetime and must not be unlinked.
    pub fn open(path: &Path, capacity: usize) -> io::Result<Self> {
        let file = crate::checkpoint::Checkpoint::open(path)?;
        let maximum = capacity
            .checked_mul(8192)
            .and_then(|n| n.checked_add(128))
            .ok_or_else(|| policy(Error::Capacity))?;
        let cleanup = match file.read(maximum)? {
            Some(bytes) => Cleanup::restore(capacity, &bytes).map_err(policy)?,
            None => Cleanup::new(capacity).map_err(policy)?,
        };
        Ok(Self { file, cleanup })
    }
    fn commit(&mut self, candidate: Cleanup) -> io::Result<()> {
        self.file.replace(
            &candidate.checkpoint(),
            ".retained-cleanup-",
            "checkpoint outcome uncertain; reopen before retry",
        )?;
        self.cleanup = candidate;
        Ok(())
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
        self.file
            .ensure_ready("checkpoint outcome uncertain; reopen before completion")?;
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
        self.file.requires_reopen()
    }
}
fn policy(error: Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("cleanup ledger: {error:?}"),
    )
}
