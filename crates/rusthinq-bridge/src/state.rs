//! Bridge state storage (JSON files under a base path).

use serde::{Serialize, de::DeserializeOwned};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Environment {
    pub country_code: String,
    #[serde(default)]
    pub language_code: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials {
    pub refresh_token: String,
    pub env: Environment,
}

pub trait BridgeState: Send + Sync {
    fn get_credentials(&self) -> Option<Credentials>;
    fn set_credentials(&self, credentials: Option<Credentials>);
    fn get_device_state_json(&self, id: &str) -> Option<serde_json::Value>;
    fn set_device_state_json(&self, id: &str, state: Option<serde_json::Value>);
    /// Every device id with a stored state file, regardless of whether this
    /// process has ever seen it live this run — the only way to discover a
    /// `device_<id>.json` that's just been sitting there since before this
    /// process started. Used once, by `Bridge::reconcile_storage_with_account`'s
    /// first poll (see its doc comment).
    fn list_device_ids(&self) -> Vec<String>;
}

/// File-backed storage matching TypeScript `JSONStorage`.
pub struct JsonStorage {
    base_path: PathBuf,
}

impl JsonStorage {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    fn oauth2_path(&self) -> PathBuf {
        self.base_path.join("oauth2.json")
    }

    fn device_path(&self, id: &str) -> PathBuf {
        self.base_path.join(format!("device_{id}.json"))
    }

    fn read_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
        let data = match fs::read_to_string(path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                rusthinq_core::logging::log(
                    "bridge",
                    &[&format!("Failed to read {}: {e} (treating as absent)", path.display())],
                );
                return None;
            }
        };
        serde_json::from_str(&data)
            .inspect_err(|e| {
                rusthinq_core::logging::log(
                    "bridge",
                    &[&format!(
                        "{} is corrupt, treating as absent: {e}",
                        path.display()
                    )],
                );
            })
            .ok()
    }

    fn write_json<T: Serialize>(path: &Path, value: &T) {
        if let Some(parent) = path.parent()
            && let Err(e) = fs::create_dir_all(parent)
        {
            rusthinq_core::logging::log(
                "bridge",
                &[&format!("Failed to create {}: {e}", parent.display())],
            );
            return;
        }
        match serde_json::to_string(value) {
            Ok(s) => {
                if let Err(e) = rusthinq_core::atomic_file::write(path, s.as_bytes()) {
                    rusthinq_core::logging::log(
                        "bridge",
                        &[&format!("Failed to persist {}: {e}", path.display())],
                    );
                }
            }
            Err(e) => {
                rusthinq_core::logging::log(
                    "bridge",
                    &[&format!("Failed to serialize state for {}: {e}", path.display())],
                );
            }
        }
    }
}

impl BridgeState for JsonStorage {
    fn get_credentials(&self) -> Option<Credentials> {
        Self::read_json(&self.oauth2_path())
    }

    fn set_credentials(&self, credentials: Option<Credentials>) {
        let path = self.oauth2_path();
        match credentials {
            Some(c) => Self::write_json(&path, &c),
            None => {
                let _ = fs::remove_file(path);
            }
        }
    }

    fn get_device_state_json(&self, id: &str) -> Option<serde_json::Value> {
        Self::read_json(&self.device_path(id))
    }

    fn set_device_state_json(&self, id: &str, state: Option<serde_json::Value>) {
        let path = self.device_path(id);
        match state {
            Some(s) => Self::write_json(&path, &s),
            None => {
                let _ = fs::remove_file(path);
            }
        }
    }

    fn list_device_ids(&self) -> Vec<String> {
        let Ok(entries) = fs::read_dir(&self.base_path) else {
            return Vec::new();
        };
        entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                name.to_str()?
                    .strip_prefix("device_")?
                    .strip_suffix(".json")
                    .map(str::to_string)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An `oauth2.json` as the TypeScript bridge (`bridge/state.ts`'s
    /// `JSONStorage`) actually writes it: `Environment` there only ever has
    /// `countryCode`, camelCase, no `languageCode`. A state directory carried
    /// over from that bridge must still parse as a valid login here.
    #[test]
    fn reads_an_oauth2_json_shaped_like_the_typescript_bridge_wrote_it() {
        let dir =
            std::env::temp_dir().join(format!("rusthinq-bridge-state-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("oauth2.json"),
            r#"{"refreshToken":"abc123","env":{"countryCode":"KR"}}"#,
        )
        .unwrap();

        let storage = JsonStorage::new(&dir);
        let creds = storage
            .get_credentials()
            .expect("must parse the TypeScript-shaped oauth2.json");
        assert_eq!(creds.refresh_token, "abc123");
        assert_eq!(creds.env.country_code, "KR");

        let _ = fs::remove_dir_all(&dir);
    }

    /// A truncated/corrupt state file (the shape a crash mid-write used to be able
    /// to leave behind before `write_json` switched to `atomic_file::write`) must
    /// read back as "no credentials" rather than panicking — and a write afterward
    /// must fully replace it, not merge with the garbage.
    #[test]
    fn corrupt_oauth2_json_reads_as_absent_and_a_write_replaces_it_cleanly() {
        let dir = std::env::temp_dir().join(format!(
            "rusthinq-bridge-state-corrupt-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("oauth2.json"), b"{\"refreshToken\":\"abc123\",\"e").unwrap();

        let storage = JsonStorage::new(&dir);
        assert!(storage.get_credentials().is_none());

        storage.set_credentials(Some(Credentials {
            refresh_token: "new-token".to_string(),
            env: Environment {
                country_code: "KR".to_string(),
                language_code: None,
            },
        }));
        let creds = storage
            .get_credentials()
            .expect("write after corruption must be readable");
        assert_eq!(creds.refresh_token, "new-token");

        let _ = fs::remove_dir_all(&dir);
    }
}
