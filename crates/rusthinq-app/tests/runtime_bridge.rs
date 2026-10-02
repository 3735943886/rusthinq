#![cfg(feature = "bridge")]
use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Runtime},
};
use rusthinq_bridge::devices::{BridgeHandle, DeregisterFuture, Deregistration, Registration};
use rusthinq_lifecycle::{Action, Entry, Ledger, Step};
use rusthinq_server::{Config, Server};
use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Semaphore, broadcast, watch},
    time::timeout,
};

struct Adapter<F>(F);
impl<F: Fn(Registration) -> DeregisterFuture + Send + Sync> Deregistration for Adapter<F> {
    fn deregister(&self, registration: Registration) -> DeregisterFuture {
        (self.0)(registration)
    }
}
fn setup(
    path: &Path,
    bridge: BridgeHandle,
    adapter: Arc<dyn Deregistration>,
    deadline: Duration,
) -> (Runtime, Server) {
    let mut storage = Storage::open(path, 8).unwrap();
    storage
        .save(&Ledger {
            revision: 1,
            next_incarnation: 2,
            entries: vec![Entry {
                id: "d".into(),
                incarnation: 1,
                last_generation: 1,
            }],
        })
        .unwrap();
    let server = Server::new(Config {
        generation_floor: 1,
        ..Config::default()
    })
    .unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 128)
        .unwrap()
        .with_bridge(bridge, adapter, deadline)
        .unwrap();
    (runtime, server)
}
async fn until(events: &mut broadcast::Receiver<Event>, predicate: impl Fn(&Event) -> bool) {
    timeout(Duration::from_secs(3), async {
        loop {
            if predicate(&events.recv().await.unwrap()) {
                return;
            }
        }
    })
    .await
    .unwrap();
}
fn saved_entries(path: &Path) -> usize {
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    value["entries"].as_array().unwrap().len()
}

#[tokio::test]
async fn deregistration_failure_preserves_device_and_explicit_retry_commits_removal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let bridge = BridgeHandle::new(8, 8192, 16).unwrap();
    let registration = bridge.registered("d".into(), 1).unwrap();
    bridge.disable(&registration).unwrap(); // disabled still owns registration
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let checkpoint = path.clone();
    let adapter = Arc::new(Adapter(move |target: Registration| -> DeregisterFuture {
        assert_eq!(target, registration);
        assert_eq!(saved_entries(&checkpoint), 1);
        let first = count.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            if first {
                Err(io::Error::other("remote unavailable"))
            } else {
                Ok(())
            }
        })
    }));
    let (runtime, server) = setup(&path, bridge.clone(), adapter, Duration::from_secs(1));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::ForgetFailed {
                step: Step::Deregister,
                ..
            })
        )
    })
    .await;
    assert_eq!(saved_entries(&path), 1);
    assert_eq!(handle.snapshot().len(), 1);
    assert!(!bridge.snapshot()[0].enabled);
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| {
        matches!(event, Event::Lifecycle(Action::Removed { .. }))
    })
    .await;
    assert!(bridge.snapshot().is_empty());
    assert_eq!(saved_entries(&path), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn deregistration_timeout_cancels_future_and_never_confirms_removal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let bridge = BridgeHandle::new(8, 8192, 16).unwrap();
    bridge.registered("d".into(), 1).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let observer = dropped.clone();
    let adapter = Arc::new(Adapter(move |_: Registration| -> DeregisterFuture {
        let dropped = observer.clone();
        Box::pin(async move {
            let _guard = Dropped(dropped);
            std::future::pending::<io::Result<()>>().await
        })
    }));
    let (runtime, server) = setup(&path, bridge.clone(), adapter, Duration::from_millis(20));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::ForgetFailed {
                step: Step::Deregister,
                ..
            })
        )
    })
    .await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(saved_entries(&path), 1);
    assert_eq!(bridge.snapshot().len(), 1);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn delayed_confirmation_cannot_remove_or_disable_successor_registration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let bridge = BridgeHandle::new(8, 8192, 16).unwrap();
    let first = bridge.registered("d".into(), 1).unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let waiting = release.clone();
    let started = entered.clone();
    let adapter = Arc::new(Adapter(move |_: Registration| -> DeregisterFuture {
        let (entered, release) = (started.clone(), waiting.clone());
        Box::pin(async move {
            entered.add_permits(1);
            release.acquire().await.unwrap().forget();
            Ok(())
        })
    }));
    let (runtime, server) = setup(&path, bridge.clone(), adapter, Duration::from_secs(2));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    handle.forget("d".into()).unwrap();
    timeout(Duration::from_secs(1), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    bridge.deregistered(&first).unwrap();
    let second = bridge.registered("d".into(), 2).unwrap();
    release.add_permits(1);
    until(&mut events, |event| {
        matches!(
            event,
            Event::Lifecycle(Action::ForgetFailed {
                step: Step::Deregister,
                ..
            })
        )
    })
    .await;
    handle.forget("d".into()).unwrap();
    until(&mut events, |event| matches!(event, Event::Rejected { .. })).await;
    assert_eq!(bridge.snapshot()[0].registration, second);
    assert!(bridge.snapshot()[0].enabled);
    assert_eq!(saved_entries(&path), 1);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}
