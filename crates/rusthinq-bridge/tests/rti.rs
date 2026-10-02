use rusthinq_bridge::rti::Session;
use std::{io::ErrorKind, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

#[tokio::test]
async fn fragmented_frames_preserve_bytes_and_do_not_ack() {
    let (client, mut server) = duplex(128);
    let mut session = Session::new(client, 128, Duration::from_secs(1)).unwrap();
    let payload = br#"{ "Body": {"CmdWId":"1"}, "unknown":true }"#;
    let frame = rusthinq_protocol::thinq1::encode(payload, 128).unwrap();
    let peer = tokio::spawn(async move {
        for byte in frame {
            server.write_all(&[byte]).await.unwrap();
        }
        let mut received = vec![0; payload.len() + 4];
        server.read_exact(&mut received).await.unwrap();
        assert_eq!(&received[4..], payload);
    });
    assert_eq!(session.receive().await.unwrap().unwrap(), payload);
    session.send(payload).await.unwrap();
    peer.await.unwrap();
    assert!(session.receive().await.unwrap().is_none());
    assert_eq!(
        session.receive().await.unwrap_err().kind(),
        ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn malformed_truncated_and_oversized_frames_are_terminal() {
    for bytes in [
        vec![255; 4],
        129_i32.to_be_bytes().to_vec(),
        vec![0, 0],
        vec![0, 0, 0, 2, b'{'],
        vec![0, 0, 0, 2, b'[', b']'],
    ] {
        let (client, mut peer) = duplex(128);
        let mut session = Session::new(client, 128, Duration::from_secs(1)).unwrap();
        peer.write_all(&bytes).await.unwrap();
        drop(peer);
        assert!(session.receive().await.is_err());
        assert_eq!(
            session.send(b"{}").await.unwrap_err().kind(),
            ErrorKind::NotConnected
        );
    }
}

#[tokio::test(start_paused = true)]
async fn timeouts_and_cancellation_fence_partial_operations() {
    let (client, mut peer) = duplex(8);
    let mut session = Session::new(client, 128, Duration::from_secs(1)).unwrap();
    peer.write_all(&[0, 0]).await.unwrap();
    assert_eq!(
        session.receive().await.unwrap_err().kind(),
        ErrorKind::TimedOut
    );
    assert_eq!(
        session.receive().await.unwrap_err().kind(),
        ErrorKind::NotConnected
    );

    let (client, _peer) = duplex(8);
    let mut session = Session::new(client, 128, Duration::from_secs(10)).unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            session.send(br#"{"long":"payload"}"#)
        )
        .await
        .is_err()
    );
    assert_eq!(
        session.send(b"{}").await.unwrap_err().kind(),
        ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn rejected_local_input_does_not_poison_session() {
    let (client, mut peer) = duplex(128);
    let mut session = Session::new(client, 8, Duration::from_secs(1)).unwrap();
    assert!(session.send(b"[]").await.is_err());
    assert!(session.send(br#"{"too":"large"}"#).await.is_err());
    session.send(b"{}").await.unwrap();
    let mut frame = [0; 6];
    peer.read_exact(&mut frame).await.unwrap();
    assert_eq!(&frame, b"\0\0\0\x02{}");
}

#[tokio::test(start_paused = true)]
async fn cancelled_read_and_stalled_write_are_terminal() {
    let (client, mut peer) = duplex(8);
    let mut session = Session::new(client, 128, Duration::from_secs(10)).unwrap();
    peer.write_all(&[0, 0, 0, 8, b'{']).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), session.receive())
            .await
            .is_err()
    );
    assert_eq!(
        session.receive().await.unwrap_err().kind(),
        ErrorKind::NotConnected
    );

    let (client, _peer) = duplex(8);
    let mut session = Session::new(client, 128, Duration::from_secs(1)).unwrap();
    assert_eq!(
        session
            .send(br#"{"long":"payload"}"#)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::TimedOut
    );
    assert_eq!(
        session.send(b"{}").await.unwrap_err().kind(),
        ErrorKind::NotConnected
    );
}
