use rusthinq_protocol::thinq1;
use rusthinq_server::{Config, Delivery, Disconnect, Event, Reject, Server, SessionId};
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
    time::timeout,
};

fn packet(id: &str, body: serde_json::Value) -> Vec<u8> {
    thinq1::encode(
        &serde_json::to_vec(
            &serde_json::json!({"Header":{"x-lgedm-deviceId":id,"unknown":7},"Body":body}),
        )
        .unwrap(),
        1_000_000,
    )
    .unwrap()
}
async fn next(events: &mut broadcast::Receiver<Event>) -> Event {
    timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn frame<S: AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    let size = timeout(Duration::from_secs(2), stream.read_u32())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![0; size as usize];
    timeout(Duration::from_secs(2), stream.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    bytes
}
async fn identify<S: AsyncWrite + Unpin>(
    stream: &mut S,
    events: &mut broadcast::Receiver<Event>,
    device: &str,
) -> SessionId {
    stream
        .write_all(&packet(device, serde_json::json!({"Cmd":"Status"})))
        .await
        .unwrap();
    let Event::Up(id) = next(events).await else {
        panic!("expected up")
    };
    assert!(matches!(next(events).await, Event::Data(ref current, _) if current == &id));
    id
}
#[tokio::test]
async fn loopback_ack_command_response_and_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let mut server = Server::new(Config {
        generation_floor: 50,
        ..Config::default()
    })
    .unwrap();
    let handle = server.handle();
    let mut events = handle.subscribe();
    assert_eq!(server.admit(stream), Ok(51));
    let input = packet("device", serde_json::json!({"Cmd":"Mon","Data":"opaque"}));
    for byte in &input {
        client.write_all(&[*byte]).await.unwrap();
    }
    let Event::Up(id) = next(&mut events).await else {
        panic!("expected up")
    };
    let ack: serde_json::Value = serde_json::from_slice(&frame(&mut client).await).unwrap();
    assert_eq!(ack["Header"]["unknown"], 7);
    assert_eq!(ack["Body"]["Return"], "OK");
    assert_eq!(
        next(&mut events).await,
        Event::Data(id.clone(), input[4..].to_vec())
    );
    let payload = br#"{"Body":{"Cmd":"Set"}}"#;
    let receipt = handle.send(&id, payload).unwrap();
    assert_eq!(frame(&mut client).await, payload);
    assert_eq!(receipt.wait().await, Delivery::Sent);
    client
        .write_all(&packet(
            "device",
            serde_json::json!({"ReturnCode":"0000","Cmd":"Mon"}),
        ))
        .await
        .unwrap();
    assert!(matches!(next(&mut events).await, Event::Response(ref current, _) if current == &id));
    assert!(matches!(next(&mut events).await, Event::Data(_, _)));
    drop(client);
    assert_eq!(next(&mut events).await, Event::Down(id, Disconnect::Eof));
    assert!(handle.snapshot().is_empty());
    server.shutdown().await;
}
#[tokio::test]
async fn replacement_fences_old_events_and_generation_targeted_commands() {
    let mut server = Server::new(Config::default()).unwrap();
    let handle = server.handle();
    let mut events = handle.subscribe();
    let (a, mut old) = tokio::io::duplex(4096);
    server.admit(a).unwrap();
    let previous = identify(&mut old, &mut events, "same").await;
    let (b, mut new) = tokio::io::duplex(4096);
    server.admit(b).unwrap();
    let current = identify(&mut new, &mut events, "same").await;
    assert!(current.generation > previous.generation);
    assert_eq!(handle.close(&previous), Err(Reject::StaleSession));
    assert!(matches!(
        handle.send(&previous, b"{}"),
        Err(Reject::StaleSession)
    ));
    assert_eq!(handle.snapshot(), vec![current.clone()]);
    handle.close(&current).unwrap();
    assert_eq!(
        next(&mut events).await,
        Event::Down(current, Disconnect::Closed)
    );
    timeout(Duration::from_secs(2), server.shutdown())
        .await
        .unwrap();
    assert!(events.try_recv().is_err());
}
#[tokio::test]
async fn bounded_queue_stalled_partial_write_and_other_device_progress() {
    let mut server = Server::new(Config {
        outbound_capacity: 1,
        write_timeout: Duration::from_millis(80),
        ..Config::default()
    })
    .unwrap();
    let handle = server.handle();
    let mut events = handle.subscribe();
    let (a, mut stalled) = tokio::io::duplex(128);
    server.admit(a).unwrap();
    let id = identify(&mut stalled, &mut events, "stalled").await;
    // No await between sends: the worker cannot drain the one-slot queue yet.
    let payload = serde_json::to_vec(&"x".repeat(4096)).unwrap();
    let receipt = handle.send(&id, &payload).unwrap();
    assert!(matches!(handle.send(&id, b"{}"), Err(Reject::Busy)));
    let (b, mut healthy) = tokio::io::duplex(4096);
    server.admit(b).unwrap();
    let healthy_id = identify(&mut healthy, &mut events, "healthy").await;
    let sent = handle.send(&healthy_id, b"{}").unwrap();
    assert_eq!(frame(&mut healthy).await, b"{}");
    assert_eq!(sent.wait().await, Delivery::Sent);
    assert_eq!(receipt.wait().await, Delivery::Unknown);
    assert_eq!(
        next(&mut events).await,
        Event::Down(id, Disconnect::WriteTimeout)
    );
    server.shutdown().await;
}
#[tokio::test]
async fn malformed_idle_and_unidentified_shutdown() {
    let mut server = Server::new(Config {
        idle_timeout: Duration::from_millis(50),
        ..Config::default()
    })
    .unwrap();
    let handle = server.handle();
    let mut events = handle.subscribe();
    let (a, mut client) = tokio::io::duplex(4096);
    server.admit(a).unwrap();
    let id = identify(&mut client, &mut events, "bad").await;
    client.write_all(&(-1i32).to_be_bytes()).await.unwrap();
    assert_eq!(
        next(&mut events).await,
        Event::Down(id, Disconnect::Protocol(thinq1::Error::NegativeLength))
    );
    let (b, mut client) = tokio::io::duplex(4096);
    server.admit(b).unwrap();
    let id = identify(&mut client, &mut events, "idle").await;
    assert_eq!(
        next(&mut events).await,
        Event::Down(id, Disconnect::Protocol(thinq1::Error::IdleTimeout))
    );
    let (c, _unidentified) = tokio::io::duplex(1);
    server.admit(c).unwrap();
    timeout(Duration::from_secs(2), server.shutdown())
        .await
        .unwrap();
    assert!(handle.snapshot().is_empty());
    assert!(matches!(
        handle.send(
            &SessionId {
                device: "idle".into(),
                generation: 1
            },
            b"{}"
        ),
        Err(Reject::Stopped)
    ));
}
#[tokio::test]
async fn admission_bound_generation_overflow_and_event_loss_are_explicit() {
    let mut server = Server::new(Config {
        max_connections: 1,
        ..Config::default()
    })
    .unwrap();
    let (a, _peer) = tokio::io::duplex(1);
    server.admit(a).unwrap();
    let (b, _peer) = tokio::io::duplex(1);
    assert_eq!(server.admit(b), Err(Reject::Busy));
    server.shutdown().await;
    let mut server = Server::new(Config {
        generation_floor: u64::MAX,
        ..Config::default()
    })
    .unwrap();
    let (a, _peer) = tokio::io::duplex(1);
    assert_eq!(server.admit(a), Err(Reject::GenerationExhausted));
    server.shutdown().await;
    let mut server = Server::new(Config {
        event_capacity: 1,
        ..Config::default()
    })
    .unwrap();
    let handle = server.handle();
    let mut slow = handle.subscribe();
    let mut fast = handle.subscribe();
    let (a, mut peer) = tokio::io::duplex(4096);
    server.admit(a).unwrap();
    peer.write_all(&packet("lag", serde_json::json!({"Cmd":"Status"})))
        .await
        .unwrap();
    // The two ordered events overrun capacity one, so recover the identity from snapshot.
    loop {
        match timeout(Duration::from_secs(2), fast.recv()).await.unwrap() {
            Ok(Event::Data(_, _)) => break,
            Err(broadcast::error::RecvError::Lagged(_)) | Ok(_) => {}
            Err(error) => panic!("{error}"),
        }
    }
    assert!(matches!(
        slow.recv().await,
        Err(broadcast::error::RecvError::Lagged(_))
    ));
    assert_eq!(handle.snapshot().len(), 1);
    server.shutdown().await;
}

struct PanicRead {
    inner: tokio::io::DuplexStream,
    panic: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl AsyncRead for PanicRead {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        assert!(
            !this.panic.load(std::sync::atomic::Ordering::SeqCst),
            "injected device handler panic"
        );
        std::pin::Pin::new(&mut this.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for PanicRead {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn device_panic_cleans_ownership_without_restarting_or_stopping_other_devices() {
    let mut server = Server::new(Config::default()).unwrap();
    let handle = server.handle();
    let mut events = handle.subscribe();
    let (inner, mut peer) = tokio::io::duplex(4096);
    let panic = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    server
        .admit(PanicRead {
            inner,
            panic: panic.clone(),
        })
        .unwrap();
    let id = identify(&mut peer, &mut events, "panic").await;
    panic.store(true, std::sync::atomic::Ordering::SeqCst);
    peer.write_all(b"wake").await.unwrap();
    assert_eq!(next(&mut events).await, Event::Down(id, Disconnect::Panic));
    assert!(handle.snapshot().is_empty());
    let (a, mut peer) = tokio::io::duplex(4096);
    server.admit(a).unwrap();
    let id = identify(&mut peer, &mut events, "healthy").await;
    let sent = handle.send(&id, b"{}").unwrap();
    assert_eq!(frame(&mut peer).await, b"{}");
    assert_eq!(sent.wait().await, Delivery::Sent);
    server.shutdown().await;
}
#[tokio::test]
async fn full_queue_cannot_delay_shutdown_and_queued_frames_are_not_replayed() {
    let mut server = Server::new(Config {
        outbound_capacity: 1,
        write_timeout: Duration::from_secs(60),
        ..Config::default()
    })
    .unwrap();
    let handle = server.handle();
    let mut events = handle.subscribe();
    let (a, mut peer) = tokio::io::duplex(128);
    server.admit(a).unwrap();
    let id = identify(&mut peer, &mut events, "shutdown").await;
    let first = handle
        .send(&id, &serde_json::to_vec(&"x".repeat(4096)).unwrap())
        .unwrap();
    // Reading one byte proves the first write started; never drain the rest of that frame.
    let mut byte = [0];
    timeout(Duration::from_secs(2), peer.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    let queued = handle.send(&id, b"{}").unwrap();
    assert!(matches!(handle.send(&id, b"{}"), Err(Reject::Busy)));
    timeout(Duration::from_secs(2), server.shutdown())
        .await
        .unwrap();
    assert_eq!(first.wait().await, Delivery::Unknown);
    assert_eq!(queued.wait().await, Delivery::Failed);
    assert!(handle.snapshot().is_empty());
}

#[tokio::test]
async fn delayed_old_identity_cannot_resurrect_a_closed_newer_session() {
    let mut server = Server::new(Config::default()).unwrap();
    let handle = server.handle();
    let mut events = handle.subscribe();
    let (a, mut old) = tokio::io::duplex(4096);
    server.admit(a).unwrap();
    let (b, mut new) = tokio::io::duplex(4096);
    server.admit(b).unwrap();
    let current = identify(&mut new, &mut events, "same").await;
    drop(new);
    assert_eq!(
        next(&mut events).await,
        Event::Down(current, Disconnect::Eof)
    );
    old.write_all(&packet("same", serde_json::json!({"Cmd":"Status"})))
        .await
        .unwrap();
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(2), old.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(handle.snapshot().is_empty());
    assert!(events.try_recv().is_err());
    server.shutdown().await;
}
