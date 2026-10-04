//! Correlated capture and live observation; management transport stays in Client.
use crate::{Client, EventStream, invalid, segment};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use rusthinq_protocol::hex;
use serde_json::{Value, json};
use std::{
    io,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

pub fn capture_event(event: &Value, id: &str) -> Option<Value> {
    if event["type"] == "lost" {
        return Some(json!({"k":"lost","t":now_ms(),"events":event["events"]}));
    }
    if event["device"] != id {
        return None;
    }
    let (direction, payload) = match event["type"].as_str() {
        Some("data") => ("rx", event["hex"].as_str()?),
        Some("sent") => ("tx", event["hex"].as_str()?),
        Some("injected") if event["toDevice"] == false => ("rx", event["hex"].as_str()?),
        _ => return None,
    };
    let bytes = hex::decode(payload).ok()?;
    let text = std::str::from_utf8(&bytes).ok();
    let value = text.and_then(|t| serde_json::from_str::<Value>(t).ok());
    let (kind, payload) = match value.as_ref() {
        Some(value) if value["cmd"] == "ack" => ("ack", value["data"].as_str()?.to_string()),
        Some(value) if value["Body"]["Format"] == "B64" => {
            let data = STANDARD.decode(value["Body"]["Data"].as_str()?).ok()?;
            ("packet", hex::encode(data))
        }
        Some(_) => ("clip", text?.to_string()),
        None => ("packet", payload.to_string()),
    };
    Some(json!({"k":direction,"t":now_ms(),"type":kind,"hex":payload}))
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
pub async fn capture(client: Client, id: String, path: &Path) -> io::Result<()> {
    capture_with_cloud(client, id, path, false).await
}
pub async fn cloud_events(client: &Client, device: Option<&str>) -> io::Result<EventStream> {
    client
        .request("api/cloud/notifications", Some(&json!({"enabled":true})))
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let status = loop {
        let status = client
            .request("api/cloud/notifications?limit=0", None)
            .await?;
        if status["status"] == "connected" {
            break status;
        }
        if !status["enabled"].as_bool().unwrap_or(false) || tokio::time::Instant::now() >= deadline
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "LG notification feed did not connect; sign in and check account/network status",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let mut path = format!(
        "api/cloud/notifications/ws?cursor={}",
        status["cursor"].as_str().unwrap_or("0")
    );
    if let Some(device) = device {
        path.push_str("&device=");
        path.push_str(&segment(device));
    }
    client.events_at(&path).await
}
pub fn notification_capture(event: &Value) -> Option<Value> {
    match event["type"].as_str()? {
        "cloudNotification" => {
            let mut value = event.clone();
            value["observedAt"] = value["t"].clone();
            value["t"] = json!(now_ms());
            value["k"] = json!("cloud");
            Some(value)
        }
        "cloudLoss" => {
            Some(json!({"k":"lost","source":"cloud","t":now_ms(),"events":event["events"]}))
        }
        "cloudStatus" => Some(
            json!({"k":"note","source":"cloud","t":now_ms(),"text":format!("LG notification feed: {}",event["status"].as_str().unwrap_or("unknown"))}),
        ),
        "cloudReset" => Some(
            json!({"k":"note","source":"cloud","t":now_ms(),"text":"LG account context changed; correlation interrupted"}),
        ),
        _ => None,
    }
}
fn message_value(message: Message) -> io::Result<Option<Value>> {
    match message {
        Message::Text(text) => serde_json::from_str(&text).map(Some).map_err(|_| invalid()),
        Message::Close(_) => Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "observation stream closed",
        )),
        _ => Ok(None),
    }
}

async fn close(stream: &mut EventStream) {
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.close(None)).await;
}

pub async fn watch_cloud(client: Client) -> io::Result<()> {
    let mut events = cloud_events(&client, None).await?;
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { signal?; close(&mut events).await; return Ok(()); }
            event = events.next() => {
                let event = event.ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "LG observation stream closed"))?.map_err(|_| invalid())?;
                if let Some(event) = message_value(event)? && let Some(value) = notification_capture(&event) { println!("{value}"); }
            }
        }
    }
}

struct CaptureWriter<W>(W);
impl<W: AsyncWrite + Unpin> CaptureWriter<W> {
    async fn record(&mut self, value: &Value) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(value).map_err(|_| invalid())?;
        bytes.push(b'\n');
        self.0.write_all(&bytes).await?;
        self.0.flush().await
    }
    async fn lost_cloud(&mut self, reason: &str) -> io::Result<()> {
        self.record(
            &json!({"k":"lost","source":"cloud","t":now_ms(),"events":null,"reason":reason}),
        )
        .await?;
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "LG feed interrupted; capture correlation is incomplete",
        ))
    }
    async fn cloud(&mut self, event: &Value) -> io::Result<()> {
        let records = if event["type"] == "cloudSnapshot" {
            event["snapshot"]["events"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
        } else {
            std::slice::from_ref(event)
        };
        for record in records {
            if let Some(value) = notification_capture(record) {
                self.record(&value).await?;
            }
        }
        if cloud_interrupted(event) {
            self.lost_cloud("LG correlation interrupted").await?;
        }
        Ok(())
    }
}

fn cloud_interrupted(event: &Value) -> bool {
    match event["type"].as_str() {
        Some("cloudReset" | "cloudLoss") => true,
        Some("cloudSnapshot") => {
            event["snapshot"]["lost"] == true || event["snapshot"]["status"] != "connected"
        }
        Some("cloudStatus") => event["status"] != "connected",
        _ => false,
    }
}

async fn next_cloud(
    stream: &mut Option<EventStream>,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    match stream {
        Some(stream) => stream.next().await,
        None => std::future::pending().await,
    }
}

pub async fn capture_with_cloud(
    client: Client,
    id: String,
    path: &Path,
    with_cloud: bool,
) -> io::Result<()> {
    // Require both feeds before touching the output file; do not silently degrade --cloud.
    let mut cloud = if with_cloud {
        Some(cloud_events(&client, Some(&id)).await?)
    } else {
        None
    };
    let mut events = client.events().await?;
    let mut output = CaptureWriter(
        tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await?,
    );
    output.record(&json!({"k":"session","t":now_ms(),"device":id,"cloud":with_cloud,"correlation":"time-aligned observations, not proof of causality"})).await?;
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut note = Vec::new();
    let mut notes = true;
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                close(&mut events).await;
                if let Some(stream) = cloud.as_mut() { close(stream).await; }
                return Ok(());
            }
            result = stdin.fill_buf(), if notes => {
                let bytes = result?;
                if bytes.is_empty() { notes = false; continue; }
                let end = bytes.iter().position(|byte| *byte == b'\n').map(|n| n + 1);
                let count = end.unwrap_or(bytes.len());
                if note.len() + count > 65536 { return Err(invalid()); }
                note.extend_from_slice(&bytes[..count]);
                stdin.consume(count);
                if end.is_some() {
                    let text = std::str::from_utf8(&note).map_err(|_| invalid())?;
                    output.record(&json!({"k":"note","t":now_ms(),"text":text.trim_end()})).await?;
                    note.clear();
                }
            }
            notification = next_cloud(&mut cloud) => {
                let value = match notification {
                    Some(Ok(message)) => message_value(message),
                    _ => Err(io::Error::new(io::ErrorKind::NotConnected, "LG observation stream closed")),
                };
                match value {
                    Ok(Some(event)) => output.cloud(&event).await?,
                    Ok(None) => {},
                    Err(_) => return output.lost_cloud("LG observation stream closed or invalid").await,
                }
            }
            event = events.next() => {
                let Some(event) = event else { return Ok(()); };
                let event = event.map_err(|_| invalid())?;
                if let Message::Text(text) = event {
                    let event: Value = serde_json::from_str(&text).map_err(|_| invalid())?;
                    if let Some(value) = capture_event(&event, &id) { output.record(&value).await?; }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn cloud_output(event: Value) -> (io::Result<()>, Vec<Value>) {
        let (writer, mut reader) = tokio::io::duplex(8192);
        let mut output = CaptureWriter(writer);
        let result = output.cloud(&event).await;
        drop(output);
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
            .await
            .unwrap();
        let records = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        (result, records)
    }
    #[tokio::test]
    async fn lost_snapshot_keeps_observations_then_flushes_loss_before_error() {
        let (result, records) = cloud_output(json!({"type":"cloudSnapshot","snapshot":{"status":"connected","lost":true,"events":[{"type":"cloudNotification","t":123,"sequence":"9007199254740993","payload":{}}]}})).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected);
        assert_eq!(records[0]["k"], "cloud");
        assert_eq!(records[0]["observedAt"], 123);
        assert_eq!(records[0]["sequence"], "9007199254740993");
        assert_eq!(records[1]["k"], "lost");
    }
    #[tokio::test]
    async fn connected_snapshot_stays_live_but_reset_requires_new_capture() {
        let (result, records) = cloud_output(
            json!({"type":"cloudSnapshot","snapshot":{"status":"connected","events":[]}}),
        )
        .await;
        assert!(result.is_ok());
        assert!(records.is_empty());
        let (result, records) = cloud_output(json!({"type":"cloudReset"})).await;
        assert!(result.is_err());
        assert_eq!(records[0]["k"], "note");
        assert_eq!(records[1]["k"], "lost");
    }
}
