//! Exclusive checkpoint ownership and durable atomic replacement.
//! Filesystem work belongs on an application-owned blocking worker.
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub(crate) struct Checkpoint {
    path: PathBuf,
    parent: PathBuf,
    // The sidecar must stay linked: replacing/unlinking it would split ownership.
    _lock: File,
    uncertain: bool,
}

impl Checkpoint {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        if path.file_name().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint filename required",
            ));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_owned();
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(PathBuf::from(lock_path))?;
        lock.try_lock().map_err(io::Error::other)?;
        Ok(Self {
            path: path.into(),
            parent,
            _lock: lock,
            uncertain: false,
        })
    }

    pub(crate) fn read(&self, maximum: usize) -> io::Result<Option<Vec<u8>>> {
        let bound = u64::try_from(maximum)
            .ok()
            .and_then(|v| v.checked_add(1))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "checkpoint capacity overflow")
            })?;
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        file.take(bound).read_to_end(&mut bytes)?;
        if bytes.len() > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checkpoint capacity exceeded",
            ));
        }
        Ok(Some(bytes))
    }

    pub(crate) fn requires_reopen(&self) -> bool {
        self.uncertain
    }

    pub(crate) fn ensure_ready(&self, message: &'static str) -> io::Result<()> {
        if self.uncertain {
            Err(io::Error::other(message))
        } else {
            Ok(())
        }
    }

    /// A failed write may have replaced the file before directory sync failed.
    /// Block all further writes until reopened; publish in-memory state only on success.
    pub(crate) fn replace(
        &mut self,
        bytes: &[u8],
        prefix: &str,
        message: &'static str,
    ) -> io::Result<()> {
        self.ensure_ready(message)?;
        let result = (|| {
            let mut temporary = tempfile::Builder::new()
                .prefix(prefix)
                .tempfile_in(&self.parent)?;
            temporary.write_all(bytes)?;
            temporary.as_file().sync_all()?;
            temporary.persist(&self.path).map_err(|error| error.error)?;
            File::open(&self.parent)?.sync_all()?;
            Ok(())
        })();
        self.uncertain = result.is_err();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lock_and_temporary_files_follow_owner_lifetime() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        let mut checkpoint = Checkpoint::open(&path).unwrap();
        assert!(Checkpoint::open(&path).is_err());
        assert!(checkpoint.read(4).unwrap().is_none());
        checkpoint
            .replace(b"1234", ".test-", "reopen required")
            .unwrap();
        assert_eq!(checkpoint.read(4).unwrap().unwrap(), b"1234");
        assert_eq!(
            checkpoint.read(3).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(checkpoint);
        assert!(Checkpoint::open(&path).is_ok());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
    }
    #[test]
    fn failed_replace_preserves_state_and_blocks_retry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        std::fs::create_dir(&path).unwrap();
        let mut checkpoint = Checkpoint::open(&path).unwrap();
        assert!(
            checkpoint
                .replace(b"data", ".test-", "reopen required")
                .is_err()
        );
        assert!(checkpoint.requires_reopen());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
        std::fs::remove_dir(&path).unwrap();
        assert!(
            checkpoint
                .replace(b"retry", ".test-", "reopen required")
                .is_err()
        );
        assert!(!path.exists());
    }
}
