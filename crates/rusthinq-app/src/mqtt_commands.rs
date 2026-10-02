//! Owned command subscriber; clean sessions never replay retained instructions.
use crate::{external_mqtt::Config, runtime::Handle};
use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    io,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    sync::watch,
};

const MAXIMUM: usize = 1_000_000;
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid MQTT command packet")
}
async fn packet<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Vec<u8>> {
    let mut bytes = vec![stream.read_u8().await?];
    loop {
        bytes.push(stream.read_u8().await?);
        match rusthinq_protocol::mqtt::length(&bytes, MAXIMUM).map_err(|_| invalid())? {
            Some(size) => {
                bytes.resize(size, 0);
                let header = bytes[1..]
                    .iter()
                    .position(|byte| byte & 128 == 0)
                    .ok_or_else(invalid)?
                    + 2;
                stream.read_exact(&mut bytes[header..]).await?;
                return Ok(bytes);
            }
            None => continue,
        }
    }
}
fn frame(header: u8, body: &[u8]) -> Vec<u8> {
    let mut result = vec![header];
    let mut n = body.len();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n != 0 {
            byte |= 128;
        }
        result.push(byte);
        if n == 0 {
            break;
        }
    }
    result.extend_from_slice(body);
    result
}
fn property<'a>(prefix: &str, id: &str, topic: &'a str) -> Option<&'a str> {
    let property = topic
        .strip_prefix(&format!("{prefix}/{id}/"))?
        .strip_suffix("/set")?;
    (!property.is_empty() && !property.contains('/')).then_some(property)
}
async fn connected(
    mut stream: crate::external_mqtt::Transport,
    prefix: &str,
    app: &Handle,
) -> io::Result<()> {
    let filter = format!("{prefix}/#");
    let mut body = vec![0, 1];
    body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    body.extend_from_slice(filter.as_bytes());
    body.push(1);
    stream.write_all(&frame(0x82, &body)).await?;
    let ack = tokio::time::timeout(Duration::from_secs(10), packet(&mut stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "MQTT subscription timed out"))??;
    if ack != [0x90, 3, 0, 1, 1] && ack != [0x90, 3, 0, 1, 0] {
        return Err(invalid());
    }
    let mut acknowledged = BTreeMap::new();
    loop {
        let mut bytes = packet(&mut stream).await?;
        let retained = bytes[0] & 1 != 0;
        // The device codec deliberately forbids retain; preserve that contract by
        // checking and clearing the bit only in this broker-facing adapter.
        bytes[0] &= !1;
        let rusthinq_protocol::mqtt::Packet::Publish {
            topic,
            payload,
            id,
            duplicate,
            qos,
        } = rusthinq_protocol::mqtt::decode(&bytes, MAXIMUM).map_err(|_| invalid())?
        else {
            return Err(invalid());
        };
        if qos > 1 {
            return Err(invalid());
        }
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        topic.hash(&mut hash);
        payload.hash(&mut hash);
        let fingerprint = hash.finish();
        let repeated =
            duplicate && id.is_some_and(|id| acknowledged.get(&id) == Some(&fingerprint));
        if !retained
            && !repeated
            && topic.ends_with("/set")
            && let Ok(value) = String::from_utf8(payload)
        {
            // No queue survives this connection. Capture both identities before
            // admission; the application reconciles them again before execution.
            for (device, (session, generation, faulted)) in app.script_states() {
                if !faulted && let Some(prop) = property(prefix, &device, &topic) {
                    let input = serde_json::json!({"prop":prop,"value":value}).to_string();
                    if let Err(error) = app
                        .invoke_script(
                            device.clone(),
                            session,
                            generation,
                            "__command".into(),
                            input,
                        )
                        .await
                    {
                        app.driver_error(
                            device,
                            format!("MQTT command admission rejected: {error:?}"),
                        );
                    }
                    break;
                }
            }
        }
        if let Some(id) = id {
            acknowledged.insert(id, fingerprint);
            stream
                .write_all(&[0x40, 2, (id >> 8) as u8, id as u8])
                .await?;
        }
    }
}
pub(crate) async fn run(
    mut config: Config,
    prefix: String,
    app: Handle,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    // Independent client ID preserves the publisher's exclusive PUBACK stream.
    let mut client_hash = std::collections::hash_map::DefaultHasher::new();
    config.client.hash(&mut client_hash);
    config.client = format!("rusthinq-commands-{:016x}", client_hash.finish());
    let mut delay = 1;
    while !*stop.borrow() {
        // Await TLS preparation to completion: its owned blocking job must join.
        if let Ok(session) = config.connect().await {
            if *stop.borrow() {
                break;
            }
            tokio::select! { biased; _ = stop.changed() => break, _ = connected(session.into_stream(), &prefix, &app) => {} }
        }
        tokio::select! { biased; _ = stop.changed() => break, _ = tokio::time::sleep(Duration::from_secs(delay)) => {} }
        delay = (delay * 2).min(30);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn broker_commands_are_fenced_retained_and_duplicates_discarded() {
        use crate::{lifecycle_storage::Storage, runtime::Runtime, scripts::Callbacks};
        use rusthinq_scripting::{Compiled, Limits, worker};
        use rusthinq_server::{Config, Server};
        let directory = tempfile::tempdir().unwrap();
        let mut server = Server::new(Config::default()).unwrap();
        struct Sink(tokio::sync::mpsc::Sender<String>);
        impl crate::scripts::PublishSink for Sink {
            fn try_publish(
                &self,
                _: &crate::scripts::Context,
                payload: String,
            ) -> Result<(), String> {
                self.0.try_send(payload).map_err(|error| error.to_string())
            }
        }
        let (sent, mut received) = tokio::sync::mpsc::channel(4);
        let runtime = Runtime::new(
            Storage::open(&directory.path().join("devices.json"), 4).unwrap(),
            server.handle(),
            Duration::ZERO,
            32,
        )
        .unwrap()
        .with_scripts(crate::scripts::Owner::new(1).unwrap())
        .with_script_sink(std::sync::Arc::new(Sink(sent)));
        let app = runtime.handle();
        let (stop, stopped) = watch::channel(false);
        let runtime = tokio::spawn(runtime.run(stopped.clone()));
        let (stream, mut device) = tokio::io::duplex(8192);
        server.admit(stream).unwrap();
        device
            .write_all(
                &rusthinq_protocol::thinq1::encode(
                    br#"{"Header":{"x-lgedm-deviceId":"d"},"Body":{"Cmd":"Mon"}}"#,
                    8192,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while app.snapshot().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let session = app.snapshot()[0].session.unwrap();
        app.attach_script(
            "d".into(),
            session,
            Compiled::new(
                "fn __command(input) { publish(input); }",
                Limits::default(),
                true,
            )
            .unwrap(),
            worker::Config::default(),
            Callbacks::default(),
        )
        .await
        .unwrap();
        let (subscriber, mut broker) = tokio::io::duplex(8192);
        let route = tokio::spawn(async move { connected(Box::new(subscriber), "lg", &app).await });
        assert_eq!(packet(&mut broker).await.unwrap()[0], 0x82);
        broker.write_all(&[0x90, 3, 0, 1, 1]).await.unwrap();
        let mut body = vec![0, 14];
        body.extend_from_slice(b"lg/d/power/set");
        // topic is thirteen bytes; build lengths explicitly to catch framing errors.
        body[1] = b"lg/d/power/set".len() as u8;
        body.extend_from_slice(&[0, 7]);
        body.extend_from_slice(b"on");
        broker.write_all(&frame(0x33, &body)).await.unwrap();
        assert_eq!(packet(&mut broker).await.unwrap(), [0x40, 2, 0, 7]);
        broker.write_all(&frame(0x32, &body)).await.unwrap();
        assert_eq!(packet(&mut broker).await.unwrap(), [0x40, 2, 0, 7]);
        let payload = tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload).unwrap(),
            serde_json::json!({"prop":"power","value":"on"})
        );
        broker.write_all(&frame(0x3a, &body)).await.unwrap();
        assert_eq!(packet(&mut broker).await.unwrap(), [0x40, 2, 0, 7]);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), received.recv())
                .await
                .is_err()
        );
        drop(broker);
        assert!(route.await.unwrap().is_err());
        server.shutdown().await;
        stop.send_replace(true);
        runtime.await.unwrap().unwrap();
    }
    #[test]
    fn exact_property_route() {
        assert_eq!(
            property("home/lg", "a/b", "home/lg/a/b/power/set"),
            Some("power")
        );
        for topic in [
            "home/lg/a/b/raw/inject/set",
            "home/lg/a/b//set",
            "home/lg/a/b/power",
            "home/lg/a/bb/power/set",
        ] {
            assert_eq!(property("home/lg", "a/b", topic), None);
        }
    }
    #[tokio::test]
    async fn fragmented_frame_and_bounds() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let expected = frame(0x32, &[0; 130]);
        let sent = expected.clone();
        let task = tokio::spawn(async move {
            for byte in sent {
                writer.write_all(&[byte]).await.unwrap();
            }
        });
        assert_eq!(packet(&mut reader).await.unwrap(), expected);
        task.await.unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&[0x30, 0xff, 0xff, 0xff, 0x7f])
            .await
            .unwrap();
        assert!(packet(&mut reader).await.is_err());
    }
}
