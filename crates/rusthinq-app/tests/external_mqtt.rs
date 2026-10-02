use rusthinq_app::{
    external_mqtt::{self, Config},
    lifecycle_storage::Storage,
    retained_cleanup::Ledger,
    runtime::Runtime,
};
use rusthinq_server::{Server, retained::Tombstone};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::timeout,
};
async fn packet(peer: &mut TcpStream) -> (u8, Vec<u8>) {
    let header = peer.read_u8().await.unwrap();
    let mut length = 0usize;
    let mut shift = 0;
    loop {
        let byte = peer.read_u8().await.unwrap();
        length |= usize::from(byte & 127) << shift;
        if byte & 128 == 0 {
            break;
        }
        shift += 7;
        assert!(shift <= 21);
    }
    let mut body = vec![0; length];
    peer.read_exact(&mut body).await.unwrap();
    (header, body)
}
#[tokio::test]
async fn recovered_intent_and_all_cleanup_are_confirmed_without_scripts() {
    let directory = tempfile::tempdir().unwrap();
    let inventory = directory.path().join("retained.json");
    let mut ledger = Ledger::open(&inventory, 4).unwrap();
    let items: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|topic| Tombstone {
            owner: "adapter".into(),
            topic: topic.into(),
        })
        .collect();
    ledger.enqueue(&items).unwrap();
    ledger.request_delete(&items[..2]).unwrap();
    drop(ledger);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
        tls: false,
        ca: None,
        client: "test".into(),
        username: Some("user".into()),
        password: Some("secret".into()),
        inventory: inventory.clone(),
    };
    assert!(!format!("{config:?}").contains("secret"));
    let (adapter, service) = external_mqtt::new(config).unwrap();
    let storage = Storage::open(&directory.path().join("devices.json"), 8).unwrap();
    let server = Server::new(Default::default()).unwrap();
    let app = Runtime::new(storage, server.handle(), Duration::ZERO, 128).unwrap();
    let (stop, stopped) = watch::channel(false);
    let run = tokio::spawn(service.run(app.handle(), stopped));
    let (recovered, ready) = tokio::sync::oneshot::channel();
    let (recovery_done, recovery_ready) = tokio::sync::oneshot::channel();
    let broker = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        let (header, body) = packet(&mut peer).await;
        assert_eq!(header, 0x10);
        assert_eq!(body[7] & 0xc0, 0xc0);
        peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        let mut recovery_done = Some(recovery_done);
        for (index, topic) in (*b"abc").into_iter().enumerate() {
            let (header, body) = packet(&mut peer).await;
            assert_eq!(header, 0x33);
            assert_eq!(&body[..3], &[0, 1, topic]);
            assert_eq!(body.len(), 5);
            let checkpoint: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&inventory).unwrap()).unwrap();
            assert!(
                checkpoint["deleting"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|value| value.as_str() == Some(std::str::from_utf8(&[topic]).unwrap()))
            );
            peer.write_all(&[0x40, 2, body[3], body[4]]).await.unwrap();
            if index == 1 {
                let _ = recovery_done.take().unwrap().send(());
            }
        }
        let _ = recovered.send(());
        // Keep the connection alive until the owner shuts down.
        let _ = peer.read_u8().await;
    });
    timeout(Duration::from_secs(5), recovery_ready)
        .await
        .unwrap()
        .unwrap();
    // The explicit request also covers remaining inventory, independent of script ownership.
    assert_eq!(
        timeout(Duration::from_secs(5), adapter.delete_all())
            .await
            .unwrap()
            .unwrap(),
        1
    );
    timeout(Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    stop.send_replace(true);
    timeout(Duration::from_secs(5), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    broker.await.unwrap();
    assert!(
        Ledger::open(&directory.path().join("retained.json"), 4)
            .unwrap()
            .pending()
            .is_empty()
    );
}
