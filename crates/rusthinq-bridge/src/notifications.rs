//! Read-only, caller-owned account notification subscription. Never provisions appliances.
use crate::{cloud::Error, pairing::Material};
use openssl::{
    hash::MessageDigest,
    pkey::PKey,
    rsa::Rsa,
    x509::{X509NameBuilder, X509Req},
};
use rusthinq_protocol::mqtt;
use serde_json::{Value, json};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, watch},
    time::{interval, timeout},
};
pub struct Subscription {
    pub client_id: String,
    pub filters: Vec<String>,
    pub material: Material,
}
impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NotificationSubscription(<redacted>)")
    }
}
pub fn identity() -> Result<(String, String), Error> {
    let key = PKey::from_rsa(Rsa::generate(2048).map_err(|_| Error::Crypto)?)
        .map_err(|_| Error::Crypto)?;
    let mut name = X509NameBuilder::new().map_err(|_| Error::Crypto)?;
    name.append_entry_by_text("CN", "AWS IoT Certificate")
        .map_err(|_| Error::Crypto)?;
    name.append_entry_by_text("O", "Amazon")
        .map_err(|_| Error::Crypto)?;
    let mut csr = X509Req::builder().map_err(|_| Error::Crypto)?;
    csr.set_subject_name(&name.build())
        .map_err(|_| Error::Crypto)?;
    csr.set_pubkey(&key).map_err(|_| Error::Crypto)?;
    csr.sign(&key, MessageDigest::sha256())
        .map_err(|_| Error::Crypto)?;
    Ok((
        String::from_utf8(key.private_key_to_pem_pkcs8().map_err(|_| Error::Crypto)?)
            .map_err(|_| Error::Crypto)?,
        String::from_utf8(csr.build().to_pem().map_err(|_| Error::Crypto)?)
            .map_err(|_| Error::Crypto)?,
    ))
}
const MAX: usize = 65536;
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid notification MQTT response",
    )
}
async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Vec<u8>> {
    let mut bytes = vec![stream.read_u8().await?];
    let length = loop {
        bytes.push(stream.read_u8().await?);
        if let Some(length) = mqtt::length(&bytes, MAX).map_err(|_| invalid())? {
            break length;
        }
    };
    let prefix = bytes.len();
    bytes.resize(length, 0);
    stream.read_exact(&mut bytes[prefix..]).await?;
    Ok(bytes)
}
async fn write<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> io::Result<()> {
    timeout(Duration::from_secs(10), stream.write_all(bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "notification write timed out"))?
}
fn string(body: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let length: u16 = value.len().try_into().map_err(|_| invalid())?;
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(value.as_bytes());
    Ok(())
}
/// Bounded handshake, QoS0/1 notification delivery and keepalive. Owner cancels/joins.
pub async fn run<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    client_id: &str,
    filters: &[String],
    events: mpsc::Sender<Value>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    if *stop.borrow() {
        return Ok(());
    }
    if client_id.is_empty()
        || client_id.len() > 128
        || filters.is_empty()
        || filters.len() > 64
        || filters.iter().any(|s| !mqtt::valid_filter(s))
    {
        return Err(invalid());
    }
    let mut connect = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60];
    string(&mut connect, client_id)?;
    let handshake = async {
        write(
            &mut stream,
            &mqtt::frame(0x10, &connect, MAX).map_err(|_| invalid())?,
        )
        .await?;
        if read(&mut stream).await? != [0x20, 2, 0, 0] {
            return Err(invalid());
        }
        let mut body = vec![0, 1];
        for filter in filters {
            string(&mut body, filter)?;
            body.push(1);
        }
        write(
            &mut stream,
            &mqtt::frame(0x82, &body, MAX).map_err(|_| invalid())?,
        )
        .await?;
        let response = read(&mut stream).await?;
        let offset = 1
            + response[1..]
                .iter()
                .position(|b| b & 128 == 0)
                .ok_or_else(invalid)?
            + 1;
        if response[0] != 0x90
            || response.get(offset..offset + 2) != Some(&[0, 1])
            || response.len() != offset + 2 + filters.len()
            || response[offset + 2..].iter().any(|q| *q > 1)
        {
            return Err(invalid());
        }
        Ok(())
    };
    tokio::select! {_=stop.changed()=>return Ok(()),result=timeout(Duration::from_secs(15),handshake)=>result.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"notification handshake timed out"))??};
    events
        .try_send(json!({"type":"ready"}))
        .map_err(|_| invalid())?;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut ping = interval(Duration::from_secs(25));
    ping.tick().await;
    while !*stop.borrow() {
        let reading = timeout(Duration::from_secs(60), read(&mut reader));
        tokio::pin!(reading);
        let packet = loop {
            tokio::select! {biased;_ = stop.changed()=>return Ok(()),result=&mut reading=>break result.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"notification read timed out"))??,_=ping.tick()=>write(&mut writer,&[0xc0,0]).await?}
        };
        if packet == [0xd0, 0] {
            continue;
        }
        let mqtt::Packet::Publish {
            topic,
            payload,
            id,
            qos,
            duplicate,
        } = mqtt::decode(&packet, MAX).map_err(|_| invalid())?
        else {
            return Err(invalid());
        };
        if qos > 1 || !filters.iter().any(|filter| mqtt::matches(filter, &topic)) {
            return Err(invalid());
        }
        let raw = String::from_utf8(payload).map_err(|_| invalid())?;
        let value = serde_json::from_str::<Value>(&raw).ok();
        events
            .try_send(json!({"topic":topic,"raw":raw,"payload":value,"duplicate":duplicate}))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::WouldBlock, "notification consumer exceeded")
            })?;
        if let Some(id) = id {
            let [a, b] = id.to_be_bytes();
            write(&mut writer, &[0x40, 2, a, b]).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn observer_subscribes_without_provisioning_and_acknowledges_notifications() {
        let (client, mut server) = tokio::io::duplex(8192);
        let (stop, stopped) = watch::channel(false);
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(async move {
            run(
                client,
                "independent-observer",
                &["account/+/state".into()],
                tx,
                stopped,
            )
            .await
        });
        assert!(
            matches!(mqtt::decode(&read(&mut server).await.unwrap(),MAX).unwrap(),mqtt::Packet::Connect{client,..} if client=="independent-observer")
        );
        server.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        assert!(
            matches!(mqtt::decode(&read(&mut server).await.unwrap(),MAX).unwrap(),mqtt::Packet::Subscribe{id:1,filters} if filters==["account/+/state"])
        );
        server.write_all(&[0x90, 3, 0, 1, 1]).await.unwrap();
        assert_eq!(rx.recv().await.unwrap()["type"], "ready");
        let mut body = Vec::new();
        string(&mut body, "account/d/state").unwrap();
        body.extend_from_slice(&7u16.to_be_bytes());
        body.extend_from_slice(br#"{"deviceId":"d","power":true}"#);
        server
            .write_all(&mqtt::frame(0x32, &body, MAX).unwrap())
            .await
            .unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event["payload"]["deviceId"], "d");
        assert_eq!(event["topic"], "account/d/state");
        assert_eq!(read(&mut server).await.unwrap(), [0x40, 2, 0, 7]);
        stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            server.read_u8().await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
    #[tokio::test]
    async fn refused_subscription_is_an_error_and_never_reports_ready() {
        let (client, mut server) = tokio::io::duplex(8192);
        let (_stop, stopped) = watch::channel(false);
        let (tx, mut rx) = mpsc::channel(8);
        let task =
            tokio::spawn(
                async move { run(client, "observer", &["feed/#".into()], tx, stopped).await },
            );
        read(&mut server).await.unwrap();
        server.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        read(&mut server).await.unwrap();
        server.write_all(&[0x90, 3, 0, 1, 0x80]).await.unwrap();
        assert!(task.await.unwrap().is_err());
        assert!(rx.recv().await.is_none());
    }
}
