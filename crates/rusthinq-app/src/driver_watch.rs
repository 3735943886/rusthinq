//! One owned polling task; changed source is prepared before generation replacement.
use crate::{drivers::Config, runtime::Handle};
use std::{collections::BTreeMap, io};
use tokio::sync::watch;
pub async fn run(config: Config, app: Handle, mut stop: watch::Receiver<bool>) -> io::Result<()> {
    let mut revisions = BTreeMap::new();
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let models = app.driver_models();
        revisions.retain(|id, _| models.contains_key(id));
        for (id, (session, model, thinq2)) in models {
            if *stop.borrow() {
                return Ok(());
            }
            let Some((attached, generation, _)) = app.script_states().get(&id).copied() else {
                continue;
            };
            if attached != session {
                continue;
            }
            let scan = config.clone();
            let model_scan = model.clone();
            let device_scan = id.clone();
            let fingerprint = tokio::task::spawn_blocking(move || {
                scan.source_revision(&device_scan, &model_scan)
            })
            .await
            .map_err(io::Error::other)?;
            let fingerprint = match fingerprint {
                Ok(value) => value,
                Err(error) => {
                    app.driver_error(id, error.to_string());
                    continue;
                }
            };
            let key = (session, fingerprint);
            let previous = revisions.insert(id.clone(), key);
            if previous.is_none_or(|previous| previous == key || previous.0 != session) {
                continue;
            }
            let prepare = config.clone();
            let device = id.clone();
            let compiled = tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    prepare.prepare(&device, &model, thinq2, true)
                }))
                .unwrap_or_else(|_| {
                    Err(rusthinq_scripting::Error::Compile(
                        "driver reload preparation panic".into(),
                    ))
                })
            })
            .await
            .map_err(io::Error::other)?;
            if *stop.borrow() {
                return Ok(());
            }
            let result = match compiled {
                Ok(compiled) => {
                    app.reload_driver(id.clone(), session, generation, compiled)
                        .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(_) => {}
                Err(rusthinq_scripting::Error::Busy) => {
                    // Capacity pressure defers this edit; it does not discard it.
                    if let Some(previous) = previous {
                        revisions.insert(id.clone(), previous);
                    }
                    app.driver_error(id, "driver reload deferred: capacity exceeded".into());
                }
                Err(error) => app.driver_error(id, format!("driver reload: {error:?}")),
            }
        }
        tokio::select! {_=stop.changed()=>return Ok(()),_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(directory: &std::path::Path) -> Config {
        Config {
            directory: directory.into(),
            topic_prefix: "test".into(),
            bindings: Default::default(),
            watch: true,
        }
    }
    #[test]
    fn revision_tracks_contents_module_changes_and_file_removal() {
        let directory = tempfile::tempdir().unwrap();
        let config = config(directory.path());
        let model = directory.path().join("model.rhai");
        std::fs::write(directory.path().join("common.rhai"), "fn b(){}").unwrap();
        std::fs::write(&model, "import \"common\" as c; fn a(){}").unwrap();
        let first = config.source_revision("d", "model").unwrap();
        std::fs::write(directory.path().join("common.rhai"), "fn b(){}").unwrap();
        std::fs::write(&model, "import \"common\" as c; fn a(){}").unwrap();
        assert_eq!(config.source_revision("d", "model").unwrap(), first);
        std::fs::write(directory.path().join("common.rhai"), "fn changed(){}").unwrap();
        let module = config.source_revision("d", "model").unwrap();
        assert_ne!(module, first);
        std::fs::remove_file(&model).unwrap();
        assert!(config.source_revision("d", "model").is_err());
    }
    #[test]
    fn revision_rejects_oversized_source_and_escaping_symlink() {
        let directory = tempfile::tempdir().unwrap();
        let config = config(directory.path());
        let model = directory.path().join("model.rhai");
        std::fs::write(&model, vec![b' '; 524289]).unwrap();
        assert!(config.source_revision("d", "model").is_err());
        std::fs::remove_file(&model).unwrap();
        #[cfg(unix)]
        {
            let outside = tempfile::tempdir().unwrap();
            let file = outside.path().join("outside.rhai");
            std::fs::write(&file, "fn a(){}").unwrap();
            std::os::unix::fs::symlink(file, model).unwrap();
            assert!(config.source_revision("d", "model").is_err());
        }
    }
}
