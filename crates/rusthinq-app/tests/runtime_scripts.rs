use rusthinq_app::{
    lifecycle_storage::Storage,
    runtime::{Event, Handle, Runtime},
    scripts::{Callbacks, Context, Owner, PublishSink},
};
use rusthinq_protocol::thinq1;
use rusthinq_scripting::{Compiled, Error, Limits, worker};
use rusthinq_server::{Config, Delivery, Server};
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
            .map_err(|e| e.to_string())
    }
}
async fn until(
    events: &mut broadcast::Receiver<Event>,
    predicate: impl Fn(&Event) -> bool,
) -> Event {
    timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.unwrap();
            if predicate(&event) {
                return event;
            }
        }
    })
    .await
    .unwrap()
}
async fn frame(peer: &mut tokio::io::DuplexStream) -> Vec<u8> {
    timeout(Duration::from_secs(3), async {
        let size = peer.read_u32().await.unwrap();
        let mut bytes = vec![0; size as usize];
        peer.read_exact(&mut bytes).await.unwrap();
        bytes
    })
    .await
    .unwrap()
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
    frame(&mut peer).await;
    peer
}
async fn response(peer: &mut tokio::io::DuplexStream) {
    peer.write_all(
        &thinq1::encode(
            br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"ReturnCode":"OK","value":7}}"#,
            8192,
        )
        .unwrap(),
    )
    .await
    .unwrap();
}
async fn attach(handle: &Handle, source: &str) -> Result<(), Error> {
    let session = handle.snapshot()[0].session.unwrap();
    handle
        .attach_script(
            "d".into(),
            session,
            Compiled::new(source, Limits::default(), true).unwrap(),
            worker::Config::default(),
            Callbacks {
                response: Some("on_response".into()),
                ..Callbacks::default()
            },
        )
        .await
}

#[tokio::test]
async fn real_response_invokes_script_publishes_opaque_value_and_sends_exact_json() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut publications) = mpsc::channel(4);
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
        matches!(
            e,
            Event::Lifecycle(rusthinq_lifecycle::Action::Online { .. })
        )
    })
    .await;
    attach(
        &handle,
        r#"fn on_response(v){publish("opaque non-JSON");send("{ \"Body\": {\"Cmd\":\"Get\"} }");}"#,
    )
    .await
    .unwrap();
    response(&mut peer).await;
    let (context, payload) = timeout(Duration::from_secs(3), publications.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload, "opaque non-JSON");
    assert_eq!(context.device, "d");
    assert_eq!(context.session, handle.snapshot()[0].session.unwrap());
    assert_eq!(frame(&mut peer).await, br#"{ "Body": {"Cmd":"Get"} }"#);
    until(&mut events, |e| {
        matches!(
            e,
            Event::ScriptDelivery {
                delivery: Delivery::Sent,
                ..
            }
        )
    })
    .await;
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert!(Storage::open(&path, 4).is_ok());
    assert_eq!(
        handle
            .attach_script(
                "d".into(),
                context.session,
                Compiled::new("fn on_response(v){}", Limits::default(), true).unwrap(),
                worker::Config::default(),
                Callbacks::default()
            )
            .await,
        Err(Error::Stopped)
    );
}

#[tokio::test]
async fn missing_sink_faults_before_send_and_stale_live_attachment_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap());
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let mut peer = identify(&mut server).await;
    until(&mut events, |e| {
        matches!(
            e,
            Event::Lifecycle(rusthinq_lifecycle::Action::Online { .. })
        )
    })
    .await;
    let mut stale = handle.snapshot()[0].session.unwrap();
    stale.generation += 1;
    assert_eq!(
        handle
            .attach_script(
                "d".into(),
                stale,
                Compiled::new("fn on_response(v){}", Limits::default(), true).unwrap(),
                worker::Config::default(),
                Callbacks::default()
            )
            .await,
        Err(Error::Stale)
    );
    attach(&handle, r#"fn on_response(v){publish(v);send("{}");}"#)
        .await
        .unwrap();
    response(&mut peer).await;
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
    assert!(
        timeout(Duration::from_millis(50), peer.read_u32())
            .await
            .is_err()
    );
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn sink_overload_stops_remaining_outputs_and_fault_prefix_is_not_replayed() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut publications) = mpsc::channel(1);
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
        matches!(
            e,
            Event::Lifecycle(rusthinq_lifecycle::Action::Online { .. })
        )
    })
    .await;
    attach(
        &handle,
        r#"fn on_response(v){publish("prefix");publish("overflow");send("{}");throw "fault";}"#,
    )
    .await
    .unwrap();
    response(&mut peer).await;
    until(
        &mut events,
        |e| matches!(e,Event::Rejected{reason,..} if reason.contains("Execution")),
    )
    .await;
    assert_eq!(publications.try_recv().unwrap().1, "prefix");
    assert!(publications.try_recv().is_err());
    assert!(
        timeout(Duration::from_millis(50), peer.read_u32())
            .await
            .is_err()
    );
    response(&mut peer).await;
    until(
        &mut events,
        |e| matches!(e,Event::Rejected{reason,..} if reason.contains("Faulted")),
    )
    .await;
    assert!(publications.try_recv().is_err());
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconnect_reaps_old_worker_before_attaching_successor() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut publications) = mpsc::channel(4);
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
        matches!(
            e,
            Event::Lifecycle(rusthinq_lifecycle::Action::Online { .. })
        )
    })
    .await;
    let old = handle.snapshot()[0].session.unwrap();
    attach(&handle, "fn on_response(v){publish(v);}")
        .await
        .unwrap();
    let mut peer = identify(&mut server).await;
    until(&mut events, |e| matches!(e, Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.session.is_some_and(|session| session.generation > old.generation))).await;
    assert_eq!(
        handle
            .attach_script(
                "d".into(),
                old,
                Compiled::new("fn on_response(v){}", Limits::default(), true).unwrap(),
                worker::Config::default(),
                Callbacks::default()
            )
            .await,
        Err(Error::Stale)
    );
    attach(&handle, "fn on_response(v){publish(v);}")
        .await
        .unwrap();
    response(&mut peer).await;
    let (context, body) = timeout(Duration::from_secs(3), publications.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(context.session.generation > old.generation);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["value"],
        7
    );
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn callback_burst_preserves_device_scope_and_publication_order() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut publications) = mpsc::channel(16);
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
        matches!(
            e,
            Event::Lifecycle(rusthinq_lifecycle::Action::Online { .. })
        )
    })
    .await;
    attach(
        &handle,
        "let count=0; fn on_response(v){count+=1;publish(count.to_string());}",
    )
    .await
    .unwrap();
    for _ in 0..8 {
        response(&mut peer).await;
    }
    for count in 1..=8 {
        let (_, payload) = timeout(Duration::from_secs(3), publications.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(payload, count.to_string());
    }
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn context_callbacks_bind_device_state_and_reload_without_il_interpretation() {
    use rusthinq_scripting::context::Config as ContextConfig;
    let source = r#"import "output_helper" as h; fn on_response(ctx,body){
        let count=ctx.state_get("count");if count==(){count=0;}
        count+=1;ctx.state_set("count",count);
        h::publish_count(ctx,count);
        ctx.send_json("{ \"Body\": {\"Cmd\":\"Get\"} }");
    }"#;
    let compiled = |id: &str| {
        Compiled::with_context(
            source,
            Limits::default(),
            true,
            ContextConfig::new(id.into(), "model".into()),
        )
        .unwrap()
        .with_modules(vec![rusthinq_scripting::modules::Source {
            name: "output_helper".into(),
            source: r#"fn publish_count(ctx,count){ctx.publish(ctx.id()+":"+ctx.model_id()+":"+count.to_string());}"#.into(),
        }]).unwrap()
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("devices.json");
    let storage = Storage::open(&path, 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut publications) = mpsc::channel(8);
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
        matches!(
            e,
            Event::Lifecycle(rusthinq_lifecycle::Action::Online { .. })
        )
    })
    .await;
    let session = handle.snapshot()[0].session.unwrap();
    let callbacks = Callbacks {
        response: Some("on_response".into()),
        ..Callbacks::default()
    };
    assert_eq!(
        handle
            .attach_script(
                "d".into(),
                session,
                compiled("wrong"),
                worker::Config::default(),
                callbacks.clone()
            )
            .await,
        Err(Error::InvalidConfig)
    );
    handle
        .attach_script(
            "d".into(),
            session,
            compiled("d"),
            worker::Config::default(),
            callbacks,
        )
        .await
        .unwrap();
    for count in 1..=2 {
        response(&mut peer).await;
        let (context, value) = timeout(Duration::from_secs(3), publications.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.generation, 1);
        assert_eq!(value, format!("d:model:{count}"));
        assert_eq!(frame(&mut peer).await, br#"{ "Body": {"Cmd":"Get"} }"#);
    }
    assert_eq!(
        handle
            .reload_script("d".into(), session, 1, compiled("wrong"))
            .await,
        Err(Error::InvalidConfig)
    );
    assert_eq!(
        handle
            .reload_script("d".into(), session, 1, compiled("d"))
            .await,
        Ok(2)
    );
    response(&mut peer).await;
    let (context, value) = timeout(Duration::from_secs(3), publications.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.generation, 2);
    assert_eq!(value, "d:model:1");
    assert_eq!(frame(&mut peer).await, br#"{ "Body": {"Cmd":"Get"} }"#);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_callback_publishes_once_offline_and_cannot_send_to_device() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut publications) = mpsc::channel(4);
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_script_sink(Arc::new(Sink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let peer = identify(&mut server).await;
    until(&mut events,|event|matches!(event,Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.online)).await;
    let session = handle.snapshot()[0].session.unwrap();
    handle
        .attach_script(
            "d".into(),
            session,
            Compiled::new(
                "fn bye(v){publish(\"offline\");send(\"{}\");publish(\"late\");}",
                Limits::default(),
                true,
            )
            .unwrap(),
            worker::Config::default(),
            Callbacks {
                shutdown: Some("bye".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    drop(peer);
    until(&mut events,|event|matches!(event,Event::ScriptStopped{context,error:None} if context.session==session)).await;
    assert_eq!(
        timeout(Duration::from_secs(3), publications.recv())
            .await
            .unwrap()
            .unwrap(),
        (
            Context {
                device: "d".into(),
                session,
                generation: 1
            },
            "offline".into()
        )
    );
    until(&mut events,|event|matches!(event,Event::Rejected{reason,..} if reason.contains("shutdown callback cannot send"))).await;
    assert!(!handle.snapshot()[0].online);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert!(publications.try_recv().is_err());
}

#[tokio::test]
async fn shutdown_output_from_replaced_session_never_reaches_successor() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let (sink, mut publications) = mpsc::channel(4);
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_script_sink(Arc::new(Sink(sink)));
    let handle = runtime.handle();
    let mut events = handle.subscribe();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(runtime.run(stopped));
    let old = identify(&mut server).await;
    until(&mut events,|event|matches!(event,Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.online)).await;
    let session = handle.snapshot()[0].session.unwrap();
    handle
        .attach_script(
            "d".into(),
            session,
            Compiled::new(
                "fn bye(v){publish(\"old-offline\");}",
                Limits::default(),
                true,
            )
            .unwrap(),
            worker::Config::default(),
            Callbacks {
                shutdown: Some("bye".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let new = identify(&mut server).await;
    until(
        &mut events,
        |event| matches!(event,Event::ScriptStopped{context,..} if context.session==session),
    )
    .await;
    assert!(handle.snapshot()[0].session.unwrap().generation > session.generation);
    assert!(publications.try_recv().is_err());
    drop(old);
    drop(new);
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert!(publications.try_recv().is_err());
}
