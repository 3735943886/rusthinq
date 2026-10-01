//! Atomic same-filesystem file writes for persisted state.
//!
//! A plain `fs::write` truncates the destination before writing the new content, so
//! a crash or full disk mid-write can leave a state file half-written. Every reader
//! of these files treats a parse failure as "absent" (no prior state) rather than
//! surfacing corruption, which turns a truncated write into silent data loss —
//! credentials, known-device history, retained MQTT topics — on the next boot.
//!
//! Writing to a sibling temp file and renaming it into place avoids that: `rename`
//! within the same directory is atomic on the filesystems this project targets, so a
//! reader always sees either the fully-old or the fully-new content, never a partial
//! one.

use std::io;
use std::path::Path;

/// Write `contents` to `path` atomically.
pub fn write(path: &Path, contents: &[u8]) -> io::Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let dir = dir.unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;

    let tmp_path = dir.join(format!(
        ".{}.tmp-{}",
        file_name.to_string_lossy(),
        std::process::id()
    ));

    let result =
        std::fs::write(&tmp_path, contents).and_then(|()| std::fs::rename(&tmp_path, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_read_round_trips() {
        let dir =
            std::env::temp_dir().join(format!("rusthinq-atomic-write-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");

        write(&path, b"{\"a\":1}").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"a\":1}");

        // Overwrite: must fully replace, not append or merge.
        write(&path, b"{\"a\":2}").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"a\":2}");

        // No leftover temp file after a successful write.
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "temp file left behind: {leftovers:?}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_a_path_with_no_file_name() {
        assert!(write(Path::new("/"), b"x").is_err());
    }
}
