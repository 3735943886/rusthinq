#![cfg(feature = "scripting")]
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
async fn context_callbacks_bind_device_state_and_reload_without_interpretation() {
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
async fn timer_from_replaced_generation_never_fires_and_successor_timer_does() {
    use rusthinq_scripting::context::Config as ContextConfig;
    let compiled = |tag: &str| {
        Compiled::with_context(
            &format!(
                r#"fn on_response(ctx,body){{ctx.set_timer("t",150);ctx.publish("{tag}:armed");}}
                fn on_timer(ctx,name){{ctx.publish("{tag}:"+name);}}"#
            ),
            Limits::default(),
            true,
            ContextConfig::new("d".into(), "model".into()),
        )
        .unwrap()
    };
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
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
    handle
        .attach_script(
            "d".into(),
            session,
            compiled("old"),
            worker::Config::default(),
            Callbacks {
                response: Some("on_response".into()),
                timer: Some("on_timer".into()),
                ..Callbacks::default()
            },
        )
        .await
        .unwrap();
    let mut next = async || {
        timeout(Duration::from_secs(3), publications.recv())
            .await
            .unwrap()
            .unwrap()
    };
    response(&mut peer).await;
    assert_eq!(next().await.1, "old:armed");
    assert_eq!(
        handle
            .reload_script("d".into(), session, 1, compiled("new"))
            .await,
        Ok(2)
    );
    // The old timer is due during this window; nothing may be delivered.
    tokio::time::sleep(Duration::from_millis(400)).await;
    response(&mut peer).await;
    let (context, value) = next().await;
    assert_eq!((context.generation, value.as_str()), (2, "new:armed"));
    let (context, value) = next().await;
    assert_eq!((context.generation, value.as_str()), (2, "new:t"));
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}

// Captured from the fixed 0.1 archive: the same body sent through its ThinQ1
// DeviceAcceptor (`SendToDevice::T1Json`) and read off the socket. 0.1 names
// commands `n-<uuid>`; 0.2 uses `n-<counter>`, so only that value is substituted.
const LEGACY_CONTROL: &str = r#"{"Body":{"Cmd":"Control","CmdOpt":"Operation","CmdWId":"n-3a3473d6-65dc-4341-a963-429532618484","Data":"8CQEAQA=","Format":"B64"},"Header":{"x-lgedm-deviceId":"d"}}"#;

#[tokio::test]
async fn driver_thinq1_command_matches_legacy_wire_frame() {
    use rusthinq_scripting::context::Config as ContextConfig;
    let mut context = ContextConfig::new("d".into(), "model".into());
    context.driver_api = true;
    let compiled = Compiled::with_context(
        r#"fn on_response(ctx,body){ctx.send_json("{\"Cmd\":\"Control\",\"CmdOpt\":\"Operation\",\"Format\":\"B64\",\"Data\":\"8CQEAQA=\"}");}"#,
        Limits::default(),
        true,
        context,
    )
    .unwrap();
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
    let session = handle.snapshot()[0].session.unwrap();
    handle
        .attach_script(
            "d".into(),
            session,
            compiled,
            worker::Config::default(),
            Callbacks {
                response: Some("on_response".into()),
                ..Callbacks::default()
            },
        )
        .await
        .unwrap();
    response(&mut peer).await;
    let sent = frame(&mut peer).await;
    let value: serde_json::Value = serde_json::from_slice(&sent).unwrap();
    let id = value["Body"]["CmdWId"].as_str().unwrap();
    assert!(id.strip_prefix("n-").unwrap().parse::<u64>().is_ok());
    let expected = LEGACY_CONTROL.replace("n-3a3473d6-65dc-4341-a963-429532618484", id);
    assert_eq!(String::from_utf8(sent).unwrap(), expected);
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

#[tokio::test]
async fn reload_removes_only_retained_topics_the_new_generation_does_not_publish() {
    use rusthinq_scripting::context::Config as ContextConfig;
    let compiled = |topics: &[&str]| {
        let body: String = topics
            .iter()
            .map(|t| format!(r#"ctx.publish_raw("rusthinq/d/{t}","v",true);"#))
            .collect();
        Compiled::with_context(
            &format!("fn on_response(ctx,body){{{body}}}"),
            Limits::default(),
            true,
            ContextConfig::new("d".into(), "model".into()),
        )
        .unwrap()
    };
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 4).unwrap();
    let mut server = Server::new(Config::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sink, adapter) = rusthinq_app::external_mqtt::new(rusthinq_app::external_mqtt::Config {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
        tls: false,
        ca: None,
        client: "reload".into(),
        username: None,
        password: None,
        inventory: directory.path().join("retained.json"),
    })
    .unwrap();
    let runtime = Runtime::new(storage, server.handle(), Duration::ZERO, 32)
        .unwrap()
        .with_scripts(Owner::new(1).unwrap())
        .with_external_mqtt(sink.clone());
    let handle = runtime.handle();
    let (adapter_stop, adapter_stopped) = watch::channel(false);
    let adapter = tokio::spawn(adapter.run(handle.clone(), adapter_stopped));
    let (seen, mut published) = mpsc::channel::<(String, String)>(32);
    let broker = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        let read = async |peer: &mut tokio::net::TcpStream| -> Option<(u8, Vec<u8>)> {
            let header = peer.read_u8().await.ok()?;
            let mut length = 0usize;
            let mut shift = 0;
            loop {
                let byte = peer.read_u8().await.ok()?;
                length |= usize::from(byte & 127) << shift;
                if byte & 128 == 0 {
                    break;
                }
                shift += 7;
            }
            let mut body = vec![0; length];
            peer.read_exact(&mut body).await.ok()?;
            Some((header, body))
        };
        assert_eq!(read(&mut peer).await.unwrap().0, 0x10);
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        while let Some((header, body)) = read(&mut peer).await {
            match header {
                0xe0 => break,
                0xc0 => peer.write_all(&[0xd0, 0]).await.unwrap(),
                _ => {
                    let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
                    let topic = String::from_utf8(body[2..2 + length].to_vec()).unwrap();
                    let value = String::from_utf8(body[4 + length..].to_vec()).unwrap();
                    peer.write_all(&[0x40, 2, body[2 + length], body[3 + length]])
                        .await
                        .unwrap();
                    seen.send((topic, value)).await.unwrap();
                }
            }
        }
    });
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
    handle
        .attach_script(
            "d".into(),
            session,
            compiled(&["a", "b"]),
            worker::Config::default(),
            Callbacks {
                response: Some("on_response".into()),
                ..Callbacks::default()
            },
        )
        .await
        .unwrap();
    let mut next = async || {
        timeout(Duration::from_secs(3), published.recv())
            .await
            .unwrap()
            .unwrap()
    };
    response(&mut peer).await;
    let mut first = vec![next().await, next().await];
    first.sort();
    assert_eq!(
        first,
        [
            ("rusthinq/d/a".into(), "v".into()),
            ("rusthinq/d/b".into(), "v".into())
        ]
    );
    assert_eq!(
        handle
            .reload_script("d".into(), session, 1, compiled(&["a"]))
            .await,
        Ok(2)
    );
    response(&mut peer).await;
    // b is cleared; a is republished and never cleared.
    let mut second = vec![next().await, next().await];
    second.sort();
    assert_eq!(
        second,
        [
            ("rusthinq/d/a".into(), "v".into()),
            ("rusthinq/d/b".into(), String::new())
        ]
    );
    timeout(Duration::from_secs(3), sink.flush())
        .await
        .unwrap()
        .unwrap();
    assert!(published.try_recv().is_err());
    server.shutdown().await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    adapter_stop.send_replace(true);
    adapter.await.unwrap().unwrap();
    broker.abort();
}
