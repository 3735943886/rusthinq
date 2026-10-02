#![cfg(feature = "scripting")]
use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Handle, Runtime},
    scripts::{Callbacks, Context, Owner, PublishSink},
};
use rusthinq_lifecycle::{Action, SessionKey};
use rusthinq_protocol::thinq1;
use rusthinq_scripting::{Compiled, Error, Limits, worker};
use rusthinq_server::{Config, Server};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, mpsc, watch},
    time::timeout,
};

struct Sink(mpsc::Sender<(Context, String)>);
impl PublishSink for Sink {
    fn try_publish(&self, context: &Context, payload: String) -> Result<(), String> {
        self.0
            .try_send((context.clone(), payload))
            .map_err(|error| error.to_string())
    }
}
fn compiled(source: &str) -> Compiled {
    Compiled::new(source, Limits::default(), true).unwrap()
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
async fn identify(server: &mut Server) -> tokio::io::DuplexStream {
    let (stream, mut peer) = tokio::io::duplex(8192);
    server.admit(stream).unwrap();
    peer.write_all(
        &thinq1::encode(
            br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#,
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let size = timeout(Duration::from_secs(3), peer.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut ack = vec![0; size as usize];
    peer.read_exact(&mut ack).await.unwrap();
    peer
}
async fn response(peer: &mut tokio::io::DuplexStream) {
    peer.write_all(
        &thinq1::encode(
            br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"ReturnCode":"OK"}}"#,
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
}
async fn publication(receiver: &mut mpsc::Receiver<(Context, String)>) -> (Context, String) {
    timeout(Duration::from_secs(3), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn attach(handle: &Handle, source: &str) -> SessionKey {
    let session = handle.snapshot()[0].session.unwrap();
    handle
        .attach_script(
            "d".into(),
            session,
            compiled(source),
            worker::Config::default(),
            Callbacks {
                response: Some("on_response".into()),
                ..Callbacks::default()
            },
        )
        .await
        .unwrap();
    session
}

#[tokio::test]
async fn live_reload_preserves_old_scope_on_compile_failure_and_resets_on_success() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut outputs) = mpsc::channel(8);
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_script_sink(Arc::new(Sink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let mut peer = identify(&mut server).await;
    until(&mut events, |e| {
        matches!(e, Event::Lifecycle(Action::Online { .. }))
    })
    .await;
    let session = attach(
        &handle,
        "let count=0; fn on_response(v){count+=1;publish(\"old\"+count.to_string());}",
    )
    .await;
    response(&mut peer).await;
    assert_eq!(publication(&mut outputs).await.1, "old1");
    assert!(Compiled::new("fn broken(", Limits::default(), true).is_err());
    response(&mut peer).await;
    assert_eq!(publication(&mut outputs).await.1, "old2");
    assert_eq!(
        handle
            .reload_script(
                "d".into(),
                session,
                9,
                compiled("fn on_response(v){publish(\"bad\");}")
            )
            .await,
        Err(Error::Stale)
    );
    assert_eq!(
        handle
            .reload_script(
                "d".into(),
                session,
                1,
                compiled(
                    "let count=0; fn on_response(v){count+=1;publish(\"new\"+count.to_string());}"
                )
            )
            .await,
        Ok(2)
    );
    response(&mut peer).await;
    let (context, value) = publication(&mut outputs).await;
    assert_eq!(context.generation, 2);
    assert_eq!(value, "new1");
    assert_eq!(
        handle
            .reload_script(
                "d".into(),
                session,
                1,
                compiled("fn on_response(v){publish(\"stale\");}")
            )
            .await,
        Err(Error::Stale)
    );
    response(&mut peer).await;
    assert_eq!(publication(&mut outputs).await.1, "new2");
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert_eq!(
        handle
            .reload_script("d".into(), session, 2, compiled("fn on_response(v){}"))
            .await,
        Err(Error::Stopped)
    );
}

#[tokio::test]
async fn live_reload_explicitly_recovers_fault_without_replaying_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut outputs) = mpsc::channel(8);
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_script_sink(Arc::new(Sink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let mut peer = identify(&mut server).await;
    until(&mut events, |e| {
        matches!(e, Event::Lifecycle(Action::Online { .. }))
    })
    .await;
    let session = attach(
        &handle,
        "fn on_response(v){publish(\"prefix\");throw \"fault\";}",
    )
    .await;
    response(&mut peer).await;
    assert_eq!(publication(&mut outputs).await.1, "prefix");
    until(
        &mut events,
        |e| matches!(e,Event::Rejected{reason,..} if reason.contains("Execution")),
    )
    .await;
    response(&mut peer).await;
    until(
        &mut events,
        |e| matches!(e,Event::Rejected{reason,..} if reason.contains("Faulted")),
    )
    .await;
    assert!(outputs.try_recv().is_err());
    assert_eq!(
        handle
            .reload_script(
                "d".into(),
                session,
                1,
                compiled("fn on_response(v){publish(\"recovered\");}")
            )
            .await,
        Ok(2)
    );
    response(&mut peer).await;
    let (context, value) = publication(&mut outputs).await;
    assert_eq!(context.generation, 2);
    assert_eq!(value, "recovered");
    assert!(outputs.try_recv().is_err());
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn old_session_reload_cannot_replace_successor_with_same_script_generation() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut outputs) = mpsc::channel(8);
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_script_sink(Arc::new(Sink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let _old_peer = identify(&mut server).await;
    until(&mut events, |e| {
        matches!(e, Event::Lifecycle(Action::Online { .. }))
    })
    .await;
    let old = attach(&handle, "fn on_response(v){publish(\"old\");}").await;
    let mut peer = identify(&mut server).await;
    until(&mut events,|e|matches!(e,Event::Lifecycle(Action::Changed(d)) if d.session.is_some_and(|s|s.generation>old.generation))).await;
    let new = attach(&handle, "fn on_response(v){publish(\"successor\");}").await;
    assert_eq!(
        handle
            .reload_script(
                "d".into(),
                old,
                1,
                compiled("fn on_response(v){publish(\"wrong\");}")
            )
            .await,
        Err(Error::Stale)
    );
    response(&mut peer).await;
    let (context, value) = publication(&mut outputs).await;
    assert_eq!(context.session, new);
    assert_eq!(context.generation, 1);
    assert_eq!(value, "successor");
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn attach_and_reload_share_bounded_admission_and_shutdown_cancels_queued_reload() {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let server = Server::new(Config::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap());
    let handle = runtime.handle();
    let session = SessionKey {
        incarnation: 1,
        generation: 1,
    };
    let queued = handle.reload_script("d".into(), session, 1, compiled("fn on_response(v){}"));
    tokio::pin!(queued);
    // Poll once to admit the command without running its owner or relying on sleeps.
    assert!(poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx).is_pending())).await);
    assert_eq!(
        handle
            .attach_script(
                "d".into(),
                session,
                compiled("fn on_response(v){}"),
                worker::Config::default(),
                Callbacks::default()
            )
            .await,
        Err(Error::Busy)
    );
    assert_eq!(
        handle
            .reload_script("d".into(), session, 1, compiled("fn on_response(v){}"))
            .await,
        Err(Error::Busy)
    );
    let (_stop, stopped) = watch::channel(true);
    server.shutdown().await;
    runtime.run(stopped).await.unwrap();
    assert_eq!(queued.await, Err(Error::Stopped));
}
