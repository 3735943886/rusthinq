//! Native filesystem notifications; changed source is prepared before replacement.
use crate::{drivers::Config, runtime::Handle};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{collections::BTreeMap, io, path::Path, time::Duration};
use tokio::sync::watch;
const DEBOUNCE: Duration = Duration::from_millis(200);
const CAPACITY_RETRY: Duration = Duration::from_millis(250);

fn relevant(event: &notify::Event, root: &Path) -> bool {
    !matches!(event.kind, EventKind::Access(_))
        && (event.need_rescan()
            || event.paths.is_empty()
            || event.paths.iter().any(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "rhai")
                    || path == root
                    || path == &root.join("drivers")
                    || path == &root.join("modules")
            }))
}

struct Changes {
    // Own the native watcher for exactly as long as the reload task.
    _watcher: RecommendedWatcher,
    changed: watch::Receiver<Option<String>>,
    deadline: Option<tokio::time::Instant>,
}
impl Changes {
    fn new(directory: &Path) -> io::Result<Self> {
        let (changed, receiver) = watch::channel(None);
        let root = directory.to_owned();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                match event {
                    // Reading sources must never trigger another reload scan.
                    Ok(event) if relevant(&event, &root) => {
                        changed.send_modify(|_| {});
                    }
                    Ok(_) => {}
                    Err(error) => {
                        changed.send_replace(Some(error.to_string()));
                    }
                }
            })
            .map_err(io::Error::other)?;
        watcher
            .watch(directory, RecursiveMode::Recursive)
            .map_err(io::Error::other)?;
        Ok(Self {
            _watcher: watcher,
            changed: receiver,
            deadline: None,
        })
    }
    async fn next(&mut self) -> io::Result<()> {
        if self.deadline.is_none() {
            self.changed.changed().await.map_err(io::Error::other)?;
            self.deadline = Some(tokio::time::Instant::now() + DEBOUNCE);
        }
        if let Some(error) = self.changed.borrow().as_ref() {
            return Err(io::Error::other(error.clone()));
        }
        // Keep the deadline across select cancellation by a lifecycle event.
        // Events during scanning remain pending for the next bounded window.
        tokio::time::sleep_until(self.deadline.expect("pending filesystem event")).await;
        if let Some(error) = self.changed.borrow_and_update().as_ref() {
            return Err(io::Error::other(error.clone()));
        }
        self.deadline = None;
        Ok(())
    }
}

// Shared only within one scan: edits and dependency removals are checked again on
// the next change. Bindings may select different sources for the same device model.
async fn source_revision(
    config: &Config,
    id: &str,
    model: &str,
    scanned: &mut BTreeMap<String, u64>,
) -> io::Result<u64> {
    let source = config.bindings.get(id).map(String::as_str).unwrap_or(model);
    if let Some(revision) = scanned.get(source) {
        return Ok(*revision);
    }
    let scan = config.clone();
    let device = id.to_owned();
    let model = model.to_owned();
    let revision = tokio::task::spawn_blocking(move || scan.source_revision(&device, &model))
        .await
        .map_err(io::Error::other)??;
    scanned.insert(source.to_owned(), revision);
    Ok(revision)
}

pub async fn run(config: Config, app: Handle, mut stop: watch::Receiver<bool>) -> io::Result<()> {
    if *stop.borrow() {
        return Ok(());
    }
    let mut changes = Changes::new(&config.directory.canonicalize()?)?;
    let mut states = app.state_changes();
    let mut scopes = BTreeMap::new();
    let mut revisions = BTreeMap::new();
    let mut initial = true;
    let mut retry = None;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        if !initial {
            tokio::select! {
                biased;
                _ = stop.changed() => return Ok(()),
                result = changes.next() => result?,
                result = states.changed() => {
                    if result.is_err() { return Ok(()); }
                    let current: BTreeMap<_, _> = app.script_states().into_iter()
                        .map(|(id, (session, generation, _))| (id, (session, generation))).collect();
                    if current == scopes { continue; }
                },
                _ = tokio::time::sleep_until(retry.unwrap_or_else(tokio::time::Instant::now)), if retry.is_some() => {},
            }
        }
        retry = None;
        let models = app.driver_models();
        revisions.retain(|id, _| models.contains_key(id));
        let mut scanned = BTreeMap::new();
        let script_states = app.script_states();
        scopes = script_states
            .iter()
            .map(|(id, (session, generation, _))| (id.clone(), (*session, *generation)))
            .collect();
        for (id, (session, model, thinq2)) in models {
            if *stop.borrow() {
                return Ok(());
            }
            let Some((attached, generation, _)) = script_states.get(&id).copied() else {
                continue;
            };
            if attached != session {
                continue;
            }
            let Some(loaded) = app.driver_revision(&id, session, generation) else {
                // Explicitly attached generic scripts have no filesystem bundle.
                continue;
            };
            let fingerprint = source_revision(&config, &id, &model, &mut scanned).await;
            let fingerprint = match fingerprint {
                Ok(value) => value,
                Err(error) => {
                    app.driver_error(id, format!("driver reload source scan: {error}"));
                    continue;
                }
            };
            let key = (session, generation, fingerprint);
            let previous = revisions.insert(id.clone(), key);
            if loaded == fingerprint || previous == Some(key) {
                continue;
            }
            let prepare = config.clone();
            let device = id.clone();
            let metadata = app.persisted_models().get(&id).cloned();
            let compiled = tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    prepare.prepare_with_metadata(
                        &device,
                        &model,
                        thinq2,
                        true,
                        metadata
                            .as_ref()
                            .map(|m| (m.model_name.as_str(), m.sw_version.as_str())),
                    )
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
                    retry = Some(tokio::time::Instant::now() + CAPACITY_RETRY);
                    if let Some(previous) = previous {
                        revisions.insert(id.clone(), previous);
                    } else {
                        revisions.remove(&id);
                    }
                    app.driver_error(id, "driver reload deferred: capacity exceeded".into());
                }
                Err(error) => app.driver_error(id, format!("driver reload: {error:?}")),
            }
        }
        initial = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(directory: &std::path::Path) -> Config {
        Config {
            directory: directory.into(),
            topic_prefix: "test".into(),
            limits: Default::default(),
            bindings: Default::default(),
            watch: true,
        }
    }
    #[tokio::test]
    async fn native_events_cover_atomic_saves_modules_and_removal_without_read_wakeups() {
        use tokio::time::timeout;
        let directory = tempfile::tempdir().unwrap();
        let modules = directory.path().join("modules");
        std::fs::create_dir(&modules).unwrap();
        let model = directory.path().join("M.rhai");
        std::fs::write(&model, "fn old() {}").unwrap();
        let mut changes = Changes::new(directory.path()).unwrap();
        // Idle watching, source reads, and unrelated output files must not wake it.
        std::fs::read_to_string(&model).unwrap();
        std::fs::write(directory.path().join("output.log"), "log").unwrap();
        assert!(
            timeout(Duration::from_millis(1100), changes.next())
                .await
                .is_err()
        );
        let temporary = directory.path().join("M.tmp");
        std::fs::write(&temporary, "fn new() {}").unwrap();
        std::fs::rename(&temporary, &model).unwrap();
        timeout(Duration::from_secs(5), changes.next())
            .await
            .unwrap()
            .unwrap();
        let module = modules.join("common.rhai");
        std::fs::write(&module, "fn value() {}").unwrap();
        timeout(Duration::from_secs(5), changes.next())
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_file(module).unwrap();
        timeout(Duration::from_secs(5), changes.next())
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_file(model).unwrap();
        timeout(Duration::from_secs(5), changes.next())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn file_events_reload_a_later_attachment_and_preserve_it_on_compile_failure() {
        use crate::{
            lifecycle_storage::Storage,
            runtime::{Event, Runtime},
        };
        use rusthinq_server::{Server, thinq1_http::Metadata};
        use std::{
            future::{Future, poll_fn},
            task::Poll,
        };
        use tokio::{io::AsyncWriteExt, sync::mpsc, time::timeout};
        let directory = tempfile::tempdir().unwrap();
        let module = directory.path().join("common.rhai");
        std::fs::write(&module, "fn value() {\"old\"}").unwrap();
        std::fs::write(directory.path().join("Echo.rhai"),
            "import \"common\" as common; fn start(ctx) {ctx.publish_raw(\"test/state\",common::value(),false);}").unwrap();
        let mut options = config(directory.path());
        options.limits.source_bytes = 600000;
        options.limits.worker_capacity = 3;
        options.limits.input_bytes = 8192;
        let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
        let mut server = Server::new(Default::default()).unwrap();
        let (metadata, receiver) = mpsc::channel(4);
        let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 64)
            .unwrap()
            .with_metadata(receiver)
            .with_drivers(options.clone())
            .unwrap();
        let app = runtime.handle();
        let mut events = app.subscribe();
        let (stop, stopped) = watch::channel(false);
        // Poll to the event wait before attaching any device. This verifies that
        // first edits after a later attachment are not mistaken for a baseline.
        let mut watcher = Box::pin(run(options.clone(), app.clone(), stopped.clone()));
        assert!(poll_fn(|cx| Poll::Ready(watcher.as_mut().poll(cx).is_pending())).await);
        let watcher = tokio::spawn(watcher);
        let runtime = tokio::spawn(runtime.run(stopped));
        metadata
            .send(Metadata {
                device_id: "d".into(),
                model_name: "Echo".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let (stream, mut peer) = tokio::io::duplex(8192);
        server.admit(stream).unwrap();
        peer.write_all(
            &rusthinq_protocol::thinq1::encode(
                br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#,
                8192,
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let old = timeout(Duration::from_secs(5), async {
            loop {
                if let Event::ScriptOutput { context, payload } = events.recv().await.unwrap()
                    && serde_json::from_str::<serde_json::Value>(&payload).unwrap()["payload"]
                        == "old"
                {
                    break context;
                }
            }
        })
        .await
        .unwrap();
        std::fs::write(directory.path().join("Other.rhai"), "fn start(ctx) {}").unwrap();
        assert!(
            timeout(Duration::from_millis(800), async {
                loop {
                    if matches!(events.recv().await.unwrap(), Event::ScriptOutput { .. }) {
                        break;
                    }
                }
            })
            .await
            .is_err(),
            "unrelated model must not initialize the late attachment"
        );
        assert_eq!(app.script_states()["d"].1, old.generation);
        std::fs::write(&module, "fn broken(").unwrap();
        timeout(Duration::from_secs(5), async {
            while !matches!(events.recv().await.unwrap(), Event::Rejected {reason,..} if reason.contains("driver reload")) {}
        }).await.unwrap();
        assert_eq!(app.script_states()["d"].1, old.generation);
        let temporary = directory.path().join("common.tmp");
        std::fs::write(&temporary, "fn value() {\"new\"}").unwrap();
        std::fs::rename(temporary, module).unwrap();
        let new = timeout(Duration::from_secs(5), async {
            loop {
                if let Event::ScriptOutput { context, payload } = events.recv().await.unwrap()
                    && serde_json::from_str::<serde_json::Value>(&payload).unwrap()["payload"]
                        == "new"
                {
                    break context;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(new.session, old.session);
        assert_eq!(new.generation, old.generation + 1);
        // Prepare the old bundle, then edit while no watcher can consume an
        // event. Manual reload must commit the compiled fingerprint, not reread
        // the newer files and incorrectly declare them loaded.
        watcher.abort();
        assert!(watcher.await.unwrap_err().is_cancelled());
        let prepared = options.prepare("d", "Echo", false, true).unwrap();
        std::fs::write(
            directory.path().join("common.rhai"),
            "fn value() {\"latest\"}",
        )
        .unwrap();
        let manual = app
            .reload_driver("d".into(), new.session, new.generation, prepared)
            .await
            .unwrap();
        assert_eq!(manual, new.generation + 1);
        let watcher = tokio::spawn(run(options, app.clone(), stop.subscribe()));
        let latest = timeout(Duration::from_secs(5), async {
            loop {
                if let Event::ScriptOutput { context, payload } = events.recv().await.unwrap()
                    && serde_json::from_str::<serde_json::Value>(&payload).unwrap()["payload"]
                        == "latest"
                {
                    break context;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(latest.generation, manual + 1);
        drop(peer);
        timeout(Duration::from_secs(5), async {
            while app.script_states().contains_key("d") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (stream, mut peer) = tokio::io::duplex(8192);
        server.admit(stream).unwrap();
        peer.write_all(
            &rusthinq_protocol::thinq1::encode(
                br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#,
                8192,
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let reconnected = timeout(Duration::from_secs(5), async {
            loop {
                if let Event::ScriptOutput { context, payload } = events.recv().await.unwrap()
                    && serde_json::from_str::<serde_json::Value>(&payload).unwrap()["payload"]
                        == "latest"
                {
                    break context;
                }
            }
        })
        .await
        .unwrap();
        assert_ne!(reconnected.session, latest.session);
        assert_eq!(reconnected.generation, 1);
        std::fs::write(directory.path().join("Other.rhai"), "fn another() {}").unwrap();
        assert!(
            timeout(Duration::from_millis(800), async {
                loop {
                    if matches!(events.recv().await.unwrap(), Event::ScriptOutput { .. }) {
                        break;
                    }
                }
            })
            .await
            .is_err()
        );
        assert_eq!(app.script_states()["d"].1, 1);

        stop.send_replace(true);
        timeout(Duration::from_secs(3), watcher)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.shutdown().await;
        runtime.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn lifecycle_wakeup_during_debounce_does_not_consume_the_file_event() {
        use std::{
            future::{Future, poll_fn},
            task::Poll,
        };
        let directory = tempfile::tempdir().unwrap();
        let mut changes = Changes::new(directory.path()).unwrap();
        std::fs::write(directory.path().join("M.rhai"), "fn f() {}").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while changes.deadline.is_none() {
                let mut next = Box::pin(changes.next());
                assert!(poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx).is_pending())).await);
                drop(next);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), changes.next())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stopped_watcher_does_not_require_a_valid_directory() {
        let directory = tempfile::tempdir().unwrap();
        let storage =
            crate::lifecycle_storage::Storage::open(&directory.path().join("devices.json"), 4)
                .unwrap();
        let server = rusthinq_server::Server::new(Default::default()).unwrap();
        let runtime =
            crate::runtime::Runtime::new(storage, server.handle(), Duration::ZERO, 16).unwrap();
        let (_, stopped) = watch::channel(true);
        let mut options = config(directory.path());
        options.directory = directory.path().join("missing");
        run(options, runtime.handle(), stopped).await.unwrap();
    }

    #[tokio::test]
    async fn scan_shares_sources_respects_bindings_and_refreshes_next_pass() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config(directory.path());
        std::fs::write(directory.path().join("M.rhai"), "fn first() {}").unwrap();
        std::fs::write(
            directory.path().join("Override.rhai"),
            "fn override_driver() {}",
        )
        .unwrap();
        config.bindings.insert("bound".into(), "Override".into());
        let mut scanned = BTreeMap::new();
        let first = source_revision(&config, "a", "M", &mut scanned)
            .await
            .unwrap();
        std::fs::write(directory.path().join("M.rhai"), "fn edited() {}").unwrap();
        assert_eq!(
            source_revision(&config, "b", "M", &mut scanned)
                .await
                .unwrap(),
            first
        );
        let bound = source_revision(&config, "bound", "M", &mut scanned)
            .await
            .unwrap();
        assert_ne!(bound, first);
        assert_eq!(scanned.len(), 2);
        scanned.clear();
        assert_ne!(
            source_revision(&config, "a", "M", &mut scanned)
                .await
                .unwrap(),
            first
        );
        std::fs::remove_file(directory.path().join("M.rhai")).unwrap();
        scanned.clear();
        assert!(
            source_revision(&config, "a", "M", &mut scanned)
                .await
                .is_err()
        );
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
