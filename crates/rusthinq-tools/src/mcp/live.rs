//! MCP-owned observations: no device control or automatic reconnect.
use crate::Client;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
const MAX_DEVICES: usize = 8;
const MAX_EVENTS: usize = 512;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_EVENT: usize = 256 * 1024;
#[derive(Default)]
pub(super) struct Captures {
    entries: BTreeMap<String, Capture>,
    sequence: Arc<AtomicU64>,
}
struct Capture {
    buffer: Arc<Mutex<Buffer>>,
    task: Option<JoinHandle<()>>,
}
impl Drop for Capture {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
#[derive(Default)]
struct Buffer {
    events: VecDeque<(u64, usize, Value)>,
    bytes: usize,
    evicted: u64,
    cursor: u64,
    active: bool,
}
impl Buffer {
    fn push(&mut self, mut event: Value, sequence: &AtomicU64) {
        let seq = sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        event["sequence"] = json!(seq.to_string());
        event["ts"] = json!(timestamp);
        if event.to_string().len() > MAX_EVENT {
            event = json!({"type":"lost","reason":"event capacity exceeded","sequence":seq.to_string(),"ts":timestamp});
        }
        let size = event.to_string().len();
        self.cursor = seq;
        self.bytes += size;
        self.events.push_back((seq, size, event));
        while self.events.len() > MAX_EVENTS || self.bytes > MAX_BYTES {
            let (seq, size, _) = self.events.pop_front().unwrap();
            self.bytes -= size;
            self.evicted = seq;
        }
    }
    fn read(&self, cursor: u64, limit: usize, direction: Option<&str>) -> Value {
        let reset = cursor > self.cursor;
        let cursor = if reset { 0 } else { cursor };
        let mut next = cursor;
        let mut events = Vec::new();
        let mut bytes = 0;
        for (seq, size, event) in &self.events {
            if *seq <= cursor {
                continue;
            }
            if direction.is_none_or(|d| event["record"]["k"] == d || event["record"].is_null()) {
                if events.len() >= limit || bytes + size > MAX_EVENT {
                    break;
                }
                bytes += size;
                events.push(event.clone());
            }
            next = *seq;
        }
        json!({"active":self.active,"events":events,"nextCursor":next.to_string(),"cursor":self.cursor.to_string(),"lost":cursor < self.evicted,"reset":reset})
    }
}
fn device(args: &Value) -> io::Result<&str> {
    args["device_id"]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 256)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "device_id required"))
}
impl Captures {
    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }
    pub(super) async fn start(&mut self, client: &Client, args: &Value) -> io::Result<Value> {
        let id = device(args)?.to_owned();
        if let Some(capture) = self.entries.get(&id) {
            let buffer = capture.buffer.lock().unwrap();
            if buffer.active {
                return Ok(
                    json!({"device_id":id,"active":true,"cursor":buffer.cursor.to_string()}),
                );
            }
        }
        if !self.entries.contains_key(&id) && self.entries.len() >= MAX_DEVICES {
            return Err(io::Error::other(
                "live capture capacity exceeded; stop with clear to release history",
            ));
        }
        // Subscribe before reading scope; the initial stream snapshot is authoritative.
        let mut stream = client.events().await?;
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .map_err(|_| io::Error::other("live snapshot timed out"))?
            .ok_or_else(|| io::Error::other("live stream closed"))?
            .map_err(|_| io::Error::other("live stream failed"))?;
        let snapshot: Value = serde_json::from_str(
            first
                .to_text()
                .map_err(|_| io::Error::other("invalid live snapshot"))?,
        )
        .map_err(|_| io::Error::other("invalid live snapshot"))?;
        if snapshot["type"] != "snapshot" || !snapshot["state"]["devices"][&id].is_object() {
            return Err(io::Error::other("device missing from live snapshot"));
        }
        let buffer = self
            .entries
            .get(&id)
            .map(|c| c.buffer.clone())
            .unwrap_or_default();
        {
            let mut b = buffer.lock().unwrap();
            b.active = true;
            b.push(
                json!({"type":"session","device":id,"state":snapshot["state"]["devices"][&id]}),
                &self.sequence,
            );
        }
        let sequence = self.sequence.clone();
        let owned = buffer.clone();
        let device_id = id.clone();
        let task = tokio::spawn(async move {
            let reason = loop {
                match stream.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let Ok(mut event) = serde_json::from_str::<Value>(&text) else {
                            break "invalid event";
                        };
                        if event["device"] == device_id || event["type"] == "lost" {
                            if let Some(record) = crate::capture_event(&event, &device_id) {
                                event["record"] = record.clone();
                                if record["type"] == "packet" {
                                    event["decoded"] = crate::decode(&json!({"hex":record["hex"],"direction":if record["k"]=="tx" {"toDevice"} else {"fromDevice"}})).unwrap_or(Value::Null);
                                }
                            }
                            owned.lock().unwrap().push(event, &sequence);
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break "stream closed",
                    _ => break "stream failed",
                }
            };
            let mut b = owned.lock().unwrap();
            b.active = false;
            b.push(json!({"type":"lost","reason":reason}), &sequence);
        });
        self.entries.insert(
            id.clone(),
            Capture {
                buffer,
                task: Some(task),
            },
        );
        Ok(json!({"device_id":id,"active":true}))
    }
    pub(super) async fn stop(&mut self, args: &Value) -> io::Result<Value> {
        let id = device(args)?;
        if args["clear"] == true {
            if let Some(mut capture) = self.entries.remove(id)
                && let Some(task) = capture.task.take()
            {
                task.abort();
                let _ = task.await;
            }
        } else if let Some(capture) = self.entries.get_mut(id) {
            if let Some(task) = capture.task.take() {
                task.abort();
                let _ = task.await;
            }
            let mut b = capture.buffer.lock().unwrap();
            if b.active {
                b.active = false;
                b.push(json!({"type":"stopped"}), &self.sequence);
            }
        }
        Ok(json!({"device_id":id,"active":false,"cleared":args["clear"]==true}))
    }
    pub(super) fn read(&self, args: &Value) -> io::Result<Value> {
        let id = device(args)?;
        let cursor = match args.get("cursor") {
            None => 0,
            Some(v) => v
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| io::Error::other("cursor must be a decimal u64 string"))?,
        };
        let direction = args["direction"].as_str();
        if direction.is_some_and(|d| !matches!(d, "rx" | "tx")) {
            return Err(io::Error::other("invalid direction"));
        }
        let limit = args["limit"].as_u64().unwrap_or(100).clamp(1, 200) as usize;
        let capture = self
            .entries
            .get(id)
            .ok_or_else(|| io::Error::other("start live capture first"))?;
        Ok(capture
            .buffer
            .lock()
            .unwrap()
            .read(cursor, limit, direction))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    #[test]
    fn eviction_and_filtering_preserve_cursor_and_loss() {
        let sequence = AtomicU64::new(0);
        let mut b = Buffer::default();
        for _ in 0..MAX_EVENTS + 1 {
            b.push(json!({"record":{"k":"rx"}}), &sequence);
        }
        let page = b.read(0, 2, Some("tx"));
        assert_eq!(page["lost"], true);
        assert_eq!(page["nextCursor"], "513");
        assert!(page["events"].as_array().unwrap().is_empty());
        b.push(
            json!({"type":"data","hex":"x".repeat(MAX_EVENT)}),
            &sequence,
        );
        let page = b.read(513, 2, Some("tx"));
        assert_eq!(page["events"][0]["type"], "lost");
        assert!(b.bytes <= MAX_BYTES);
        let reset = b.read(1000, 1, None);
        assert_eq!(reset["reset"], true);
        assert_eq!(reset["events"].as_array().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn stream_history_survives_close_and_restart_without_cursor_reuse() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::new(
            &format!("http://{}/", listener.local_addr().unwrap()),
            None,
            None,
        )
        .unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
                for event in [
                    json!({"type":"snapshot","state":{"devices":{"d":{"generation":"9007199254740993"}}}}),
                    json!({"type":"data","device":"other","hex":"00"}),
                    json!({"type":"sent","device":"d","hex":"0011"}),
                ] {
                    socket
                        .send(Message::Text(event.to_string().into()))
                        .await
                        .unwrap();
                }
                socket.close(None).await.unwrap();
            }
        });
        let mut captures = Captures::default();
        let args = json!({"device_id":"d"});
        captures.start(&client, &args).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while captures.entries["d"].buffer.lock().unwrap().active {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let page = captures.read(&args).unwrap();
        assert_eq!(page["events"].as_array().unwrap().len(), 3);
        assert_eq!(page["events"][0]["state"]["generation"], "9007199254740993");
        assert_eq!(page["events"][1]["record"]["k"], "tx");
        assert_eq!(page["events"][2]["type"], "lost");
        let cursor = page["nextCursor"].clone();
        captures.start(&client, &args).await.unwrap();
        assert_eq!(
            captures
                .read(&json!({"device_id":"d","cursor":cursor}))
                .unwrap()["events"][0]["type"],
            "session"
        );
        captures.stop(&args).await.unwrap();
        assert_eq!(captures.read(&args).unwrap()["active"], false);
        captures
            .stop(&json!({"device_id":"d","clear":true}))
            .await
            .unwrap();
        assert!(captures.read(&args).is_err());
        server.await.unwrap();
    }
}
