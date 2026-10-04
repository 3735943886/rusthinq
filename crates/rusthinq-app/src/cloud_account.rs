//! One account owner, bounded commands and a locked atomic credential checkpoint.
use rusthinq_bridge::account::{CredentialStore, Credentials};
pub use rusthinq_bridge::account::{Error, Handle, Runtime, Status};
use serde::{Deserialize, Serialize};
use std::{
    io,
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
    file: crate::checkpoint::Checkpoint,
    credentials: Option<Credentials>,
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
        let file = crate::checkpoint::Checkpoint::open(path)?;
        let credentials = match file.read(32768)? {
            Some(bytes) => {
                let saved: Checkpoint = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if saved.schema != 1 {
                    return Err(invalid());
                }
                if let Some(credentials) = &saved.credentials {
                    validate(credentials)?;
                }
                saved.credentials
            }
            None => None,
        };
        Ok(Self { file, credentials })
    }
    fn save(&mut self, credentials: Option<Credentials>) -> io::Result<()> {
        self.file
            .ensure_ready("cloud checkpoint uncertain; restart required")?;
        if let Some(credentials) = &credentials {
            validate(credentials)?;
        }
        let bytes = serde_json::to_vec(&Checkpoint {
            schema: 1,
            credentials: credentials.clone(),
        })
        .map_err(|_| invalid())?;
        self.file.replace(
            &bytes,
            ".cloud-account-",
            "cloud checkpoint uncertain; restart required",
        )?;
        self.credentials = credentials;
        Ok(())
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
