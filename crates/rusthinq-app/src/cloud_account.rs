//! One account owner, bounded commands and a locked atomic credential checkpoint.
use rusthinq_bridge::account::{CredentialStore, Credentials};
pub use rusthinq_bridge::account::{Error, Handle, Runtime, Status};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    schema: u8,
    credentials: Option<Credentials>,
}
struct Store {
    path: PathBuf,
    parent: PathBuf,
    _lock: File,
    credentials: Option<Credentials>,
    uncertain: bool,
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid cloud account checkpoint",
    )
}
impl Store {
    fn open(path: &Path) -> io::Result<Self> {
        if path.file_name().is_none() {
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
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(PathBuf::from(lock_path))?;
        lock.try_lock().map_err(io::Error::other)?;
        let credentials = match File::open(path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(32769).read_to_end(&mut bytes)?;
                if bytes.len() > 32768 {
                    return Err(invalid());
                }
                let saved: Checkpoint = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if saved.schema != 1 {
                    return Err(invalid());
                }
                if let Some(credentials) = &saved.credentials {
                    validate(credentials)?;
                }
                saved.credentials
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            path: path.into(),
            parent,
            _lock: lock,
            credentials,
            uncertain: false,
        })
    }
    fn save(&mut self, credentials: Option<Credentials>) -> io::Result<()> {
        if self.uncertain {
            return Err(io::Error::other(
                "cloud checkpoint uncertain; restart required",
            ));
        }
        if let Some(credentials) = &credentials {
            validate(credentials)?;
        }
        let bytes = serde_json::to_vec(&Checkpoint {
            schema: 1,
            credentials: credentials.clone(),
        })
        .map_err(|_| invalid())?;
        let result = (|| {
            let mut temporary = tempfile::Builder::new()
                .prefix(".cloud-account-")
                .tempfile_in(&self.parent)?;
            temporary.write_all(&bytes)?;
            temporary.as_file().sync_all()?;
            temporary.persist(&self.path).map_err(|error| error.error)?;
            File::open(&self.parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.uncertain = true;
        } else {
            self.credentials = credentials;
        }
        result
    }
}
fn validate(credentials: &Credentials) -> io::Result<()> {
    if credentials.country.len() != 2
        || !credentials.country.bytes().all(|b| b.is_ascii_uppercase())
        || credentials.refresh.is_empty()
        || credentials.refresh.len() > 8192
        || credentials.refresh.chars().any(char::is_control)
    {
        return Err(invalid());
    }
    Ok(())
}
struct Persistence(Arc<Mutex<Store>>);
impl CredentialStore for Persistence {
    fn credentials(&self) -> Option<Credentials> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .credentials
            .clone()
    }
    fn save(
        &self,
        credentials: Option<Credentials>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        let store = self.0.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .save(credentials)
            })
            .await
            .map_err(io::Error::other)?
        })
    }
}
pub async fn open(path: PathBuf) -> io::Result<(Handle, Runtime)> {
    let store = tokio::task::spawn_blocking(move || Store::open(&path))
        .await
        .map_err(io::Error::other)??;
    Ok(rusthinq_bridge::account::new(Arc::new(Persistence(
        Arc::new(Mutex::new(store)),
    ))))
}
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;
    #[test]
    fn credential_checkpoint_is_locked_private_restartable_and_logout_is_durable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("account.json");
        let mut store = Store::open(&path).unwrap();
        assert!(Store::open(&path).is_err());
        store
            .save(Some(Credentials {
                country: "KR".into(),
                refresh: "private-token".into(),
            }))
            .unwrap();
        drop(store);
        let mut store = Store::open(&path).unwrap();
        assert_eq!(store.credentials.as_ref().unwrap().refresh, "private-token");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        store.save(None).unwrap();
        drop(store);
        assert!(Store::open(&path).unwrap().credentials.is_none());
    }
    #[test]
    fn corrupt_or_failed_checkpoints_are_preserved_and_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("account.json");
        std::fs::write(
            &path,
            br#"{"schema":2,"credentials":{"country":"KR","refresh":"secret"}}"#,
        )
        .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        std::fs::remove_file(&path).unwrap();
        let mut store = Store::open(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(store.save(None).is_err());
        std::fs::remove_dir(&path).unwrap();
        assert!(store.save(None).is_err());
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn actor_logout_status_and_owned_shutdown_require_no_lg_connection() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("account.json");
        let (handle, runtime) = open(path.clone()).await.unwrap();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(runtime.run(stopped));
        assert!(matches!(
            handle.login("invalid".into()).await,
            Err(Error::InvalidInput)
        ));
        assert!(matches!(
            handle.complete("https://evil.example/?code=x".into()).await,
            Err(Error::InvalidInput)
        ));
        assert!(matches!(handle.refresh().await, Err(Error::Unavailable)));
        handle.logout().await.unwrap();
        assert!(!handle.status().stored);
        stop.send_replace(true);
        task.await.unwrap().unwrap();
        assert!(matches!(handle.logout().await, Err(Error::Stopped)));
        assert!(Store::open(&path).unwrap().credentials.is_none());
    }
    #[tokio::test]
    async fn already_stopped_owner_preserves_saved_credentials_without_network() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("account.json");
        let mut store = Store::open(&path).unwrap();
        store
            .save(Some(Credentials {
                country: "KR".into(),
                refresh: "secret".into(),
            }))
            .unwrap();
        drop(store);
        let (handle, runtime) = open(path.clone()).await.unwrap();
        let (_, stopped) = watch::channel(true);
        runtime.run(stopped).await.unwrap();
        assert!(handle.status().stored);
        assert!(!handle.status().logged_in);
        assert!(Store::open(&path).unwrap().credentials.is_some());
    }
}
