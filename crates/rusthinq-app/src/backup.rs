//! Backup archive of the configuration and the state files it names (`GET /api/backup`).
//!
//! A plain ustar archive: `config.toml` as written, every state file at its path relative
//! to the configuration's directory (by file name when it lies elsewhere), and
//! `backup.json`, which records where each file came from so `thinqctl restore` can put it
//! back. Each state file is replaced atomically by rename, so every file read is a whole
//! version; the files are read one after another.
use serde_json::json;
use std::{
    collections::BTreeSet,
    fs::File,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const FORMAT: u64 = 1;
const CONFIG: &str = "config.toml";
const MANIFEST: &str = "backup.json";
/// Bound on everything read into one archive.
const MAXIMUM: usize = 64 << 20;

#[derive(Clone, Debug)]
pub struct Source {
    config: PathBuf,
    directory: PathBuf,
    files: Vec<(String, PathBuf)>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_owned())
}

impl Source {
    /// `files` are the state files the configuration at `config` names.
    pub fn new(config: &Path, files: impl IntoIterator<Item = PathBuf>) -> io::Result<Self> {
        let config = std::path::absolute(config)?;
        let directory = config
            .parent()
            .ok_or_else(|| invalid("configuration directory required"))?
            .to_owned();
        let mut names = BTreeSet::from([CONFIG.to_owned(), MANIFEST.to_owned()]);
        let mut entries = Vec::new();
        for file in files {
            let file = std::path::absolute(&file)?;
            let name = file
                .strip_prefix(&directory)
                .ok()
                .filter(|relative| {
                    relative
                        .components()
                        .all(|c| matches!(c, Component::Normal(_)))
                })
                .or_else(|| file.file_name().map(Path::new))
                .and_then(Path::to_str)
                .filter(|name| !name.is_empty() && name.len() <= 100)
                .ok_or_else(|| invalid("state file path cannot be archived"))?
                .to_owned();
            if !names.insert(name.clone()) {
                return Err(invalid("two state files share an archive name"));
            }
            entries.push((name, file));
        }
        Ok(Self {
            config,
            directory,
            files: entries,
        })
    }

    /// Reads the files and returns the archive. Blocking. A state file not created yet
    /// (no LG account, no pairing) is left out.
    pub fn archive(&self, version: &str) -> io::Result<Vec<u8>> {
        let mut total = 0;
        let mut read = |path: &Path| -> io::Result<Option<Vec<u8>>> {
            let file = match File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            let mut bytes = Vec::new();
            file.take((MAXIMUM - total) as u64 + 1)
                .read_to_end(&mut bytes)?;
            total += bytes.len();
            if total > MAXIMUM {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "backup exceeds 64 MiB",
                ));
            }
            Ok(Some(bytes))
        };
        let created = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|v| v.as_secs())
            .unwrap_or(0);
        let config = read(&self.config)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "configuration file is gone"))?;
        let mut entries = vec![(CONFIG.to_owned(), config)];
        let mut listed = Vec::new();
        for (name, path) in &self.files {
            if let Some(bytes) = read(path)? {
                listed.push(json!({"name": name, "path": path}));
                entries.push((name.clone(), bytes));
            }
        }
        let manifest = json!({
            "format": FORMAT,
            "rusthinq": version,
            "created": created,
            "config": self.config,
            "directory": self.directory,
            "files": listed,
        });
        entries.insert(
            1,
            (MANIFEST.to_owned(), serde_json::to_vec_pretty(&manifest)?),
        );
        tar(&entries, created)
    }
}

/// A ustar archive of regular files, owner-only readable.
fn tar(entries: &[(String, Vec<u8>)], mtime: u64) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    for (name, bytes) in entries {
        if name.len() > 100 {
            return Err(invalid("archive name too long"));
        }
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        let octal = |field: &mut [u8], value: u64| {
            let text = format!("{value:0width$o}", width = field.len() - 1);
            field[..text.len()].copy_from_slice(text.as_bytes());
        };
        octal(&mut header[100..108], 0o600);
        octal(&mut header[108..116], 0);
        octal(&mut header[116..124], 0);
        octal(&mut header[124..136], bytes.len() as u64);
        octal(&mut header[136..148], mtime);
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        header[148..156].fill(b' ');
        let sum: u64 = header.iter().map(|&b| u64::from(b)).sum();
        let text = format!("{sum:06o}\0 ");
        header[148..156].copy_from_slice(text.as_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(bytes);
        out.resize(out.len().next_multiple_of(512), 0);
    }
    out.resize(out.len() + 1024, 0);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_holds_config_manifest_and_present_files() {
        let directory = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let config = directory.path().join("config.toml");
        std::fs::write(&config, "hostname = \"x\"\n").unwrap();
        std::fs::create_dir(directory.path().join("state")).unwrap();
        std::fs::write(directory.path().join("ca.key"), "KEY").unwrap();
        std::fs::write(directory.path().join("state/devices.json"), "{}").unwrap();
        std::fs::write(elsewhere.path().join("account.json"), "A").unwrap();
        let source = Source::new(
            &config,
            [
                directory.path().join("ca.key"),
                directory.path().join("state/devices.json"),
                elsewhere.path().join("account.json"),
                directory.path().join("account.json.pairings"),
            ],
        )
        .unwrap();
        let archive = source.archive("0.0.0-test").unwrap();
        assert_eq!(archive.len() % 512, 0);
        let mut names = vec![];
        let mut offset = 0;
        while archive[offset] != 0 {
            let header = &archive[offset..offset + 512];
            let name = std::str::from_utf8(&header[..100])
                .unwrap()
                .trim_end_matches('\0');
            let size = u64::from_str_radix(std::str::from_utf8(&header[124..135]).unwrap(), 8)
                .unwrap() as usize;
            let stored =
                u64::from_str_radix(std::str::from_utf8(&header[148..154]).unwrap(), 8).unwrap();
            let mut blank = header.to_vec();
            blank[148..156].fill(b' ');
            assert_eq!(stored, blank.iter().map(|&b| u64::from(b)).sum::<u64>());
            let body = &archive[offset + 512..offset + 512 + size];
            if name == "backup.json" {
                let manifest: serde_json::Value = serde_json::from_slice(body).unwrap();
                assert_eq!(manifest["format"], 1);
                assert_eq!(manifest["files"].as_array().unwrap().len(), 3);
                assert_eq!(
                    manifest["files"][2]["path"],
                    elsewhere.path().join("account.json").to_str().unwrap()
                );
            }
            names.push(name.to_owned());
            offset += 512 + size.next_multiple_of(512);
        }
        assert_eq!(
            names,
            [
                "config.toml",
                "backup.json",
                "ca.key",
                "state/devices.json",
                "account.json"
            ]
        );
    }

    #[test]
    fn names_must_not_collide() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let config = a.path().join("config.toml");
        assert!(Source::new(&config, [a.path().join("ca.key"), b.path().join("ca.key")]).is_err());
        assert!(Source::new(&config, [b.path().join("config.toml")]).is_err());
    }
}
