//! Management-owned account observer. Ephemeral opt-in; bounded history, joined shutdown.
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::{broadcast, watch};
const MAX_EVENT_BYTES: usize = 524288;
const MAX_HISTORY_BYTES: usize = 8 * 1024 * 1024;
const MAX_HISTORY_EVENTS: usize = 1000;
const MAX_PAGE_EVENTS: usize = 200;

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "camelCase")]
enum Status {
    Disabled,
    #[cfg(feature = "bridge")]
    WaitingForLogin,
    #[cfg(feature = "bridge")]
    Connecting,
    #[cfg(feature = "bridge")]
    IdentityFailed,
    #[cfg(feature = "bridge")]
    Subscribing,
    #[cfg(feature = "bridge")]
    Connected,
    #[cfg(feature = "bridge")]
    Reconnecting,
}

struct Recorded {
    sequence: u64,
    bytes: usize,
    value: Value,
}
struct State {
    status: Status,
    sequence: u64,
    evicted: u64,
    bytes: usize,
    events: VecDeque<Recorded>,
}
#[derive(Clone)]
pub struct Observer {
    state: Arc<Mutex<State>>,
    wanted: watch::Sender<bool>,
    events: broadcast::Sender<Value>,
    available: bool,
    #[cfg(feature = "scripting")]
    scripts: Option<crate::api::AppHandle>,
}
impl Observer {
    pub fn new(available: bool) -> Self {
        let (wanted, _) = watch::channel(false);
        let (events, _) = broadcast::channel(256);
        Self {
            state: Arc::new(Mutex::new(State {
                status: Status::Disabled,
                sequence: 0,
                evicted: 0,
                bytes: 0,
                events: VecDeque::new(),
            })),
            wanted,
            events,
            available,
            #[cfg(feature = "scripting")]
            scripts: None,
        }
    }
    #[cfg(feature = "scripting")]
    pub fn with_scripts(mut self, handle: impl Into<crate::api::AppHandle>) -> Self {
        self.scripts = Some(handle.into());
        self
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }
    pub fn enabled(&self) -> bool {
        *self.wanted.borrow()
    }
    pub fn set_enabled(&self, enabled: bool) -> Result<(), &'static str> {
        if enabled && !self.available {
            return Err("LG account service unavailable");
        }
        self.wanted.send_replace(enabled);
        if !enabled {
            self.status(Status::Disabled);
        }
        Ok(())
    }
    fn status(&self, status: Status) {
        let mut state = self.lock();
        state.status = status;
        let _ = self
            .events
            .send(json!({"type":"cloudStatus","status":status,"enabled":self.enabled()}));
    }
    pub fn clear(&self) {
        let mut state = self.lock();
        state.events.clear();
        state.bytes = 0;
        state.evicted = 0;
        // Serialize reset with record + broadcast, so an old record cannot follow it.
        let _ = self.events.send(json!({"type":"cloudReset"}));
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    pub fn snapshot(&self, cursor: u64, limit: usize, device: Option<&str>) -> Value {
        let state = self.lock();
        let first = state
            .events
            .front()
            .map(|event| event.sequence)
            .unwrap_or(state.sequence.saturating_add(1));
        let mut bytes = 0;
        let mut next = cursor;
        let mut selected = Vec::new();
        // Advance over scanned records even if device filtering removes them.
        for event in state
            .events
            .iter()
            .filter(|event| event.sequence > cursor)
            .take(limit.min(MAX_PAGE_EVENTS))
        {
            if bytes > 0 && bytes + event.bytes > MAX_EVENT_BYTES {
                break;
            }
            bytes += event.bytes;
            next = event.sequence;
            if device.is_none_or(|id| matches_device(&event.value, id)) {
                selected.push(&event.value);
            }
        }
        json!({
            "available": self.available,
            "enabled": self.enabled(),
            "status": state.status,
            "events": selected,
            "nextCursor": if cursor > state.sequence { "0".into() } else { next.to_string() },
            "cursor": state.sequence.to_string(),
            "evicted": state.evicted,
            "reset": cursor > state.sequence,
            "lost": cursor != 0 && cursor.saturating_add(1) < first,
            "buffered": state.events.len(),
        })
    }
    pub fn record(&self, mut value: Value) {
        if !value.is_object() || value.to_string().len() > MAX_EVENT_BYTES {
            return;
        }
        let mut ids = Vec::new();
        collect_ids(&value["payload"], 0, &mut ids);
        let correlation = if ids.is_empty() { "account" } else { "device" };
        let mut state = self.lock();
        let Some(sequence) = state.sequence.checked_add(1) else {
            return;
        };
        value["type"] = json!("cloudNotification");
        value["k"] = json!("cloud");
        value["t"] = json!(crate::observability::now_ms());
        value["sequence"] = json!(sequence.to_string());
        value["devices"] = json!(ids);
        value["correlation"] = json!(correlation);
        let bytes = value.to_string().len();
        if bytes > MAX_EVENT_BYTES {
            return;
        }
        state.sequence = sequence;
        state.bytes += bytes;
        state.events.push_back(Recorded {
            sequence,
            bytes,
            value: value.clone(),
        });
        while state.events.len() > MAX_HISTORY_EVENTS || state.bytes > MAX_HISTORY_BYTES {
            if let Some(old) = state.events.pop_front() {
                state.bytes -= old.bytes;
                state.evicted = state.evicted.saturating_add(1);
            }
        }
        // Queue under the same lock as clear(): delivery order follows history order.
        #[cfg(feature = "scripting")]
        if self.enabled()
            && let Some(handle) = &self.scripts
        {
            handle.cloud_notification(&value);
        }
        let _ = self.events.send(value);
    }
    #[cfg(feature = "bridge")]
    pub async fn run(
        self,
        account: Option<crate::cloud_account::Handle>,
        mut stop: watch::Receiver<bool>,
    ) {
        use std::time::Duration;
        let Some(account) = account else {
            return;
        };
        let mut wanted = self.wanted.subscribe();
        let mut clients = account.clients();
        let mut epoch = account.cancellation();
        while !*stop.borrow() {
            if !self.enabled() {
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = wanted.changed() => {},
                    _ = epoch.changed() => self.clear(),
                };
                continue;
            }
            let client = clients.borrow_and_update().clone();
            let Some(client) = client else {
                self.status(Status::WaitingForLogin);
                self.clear();
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = wanted.changed() => {},
                    _ = clients.changed() => {},
                    _ = epoch.changed() => self.clear(),
                };
                continue;
            };
            epoch.borrow_and_update();
            self.status(Status::Connecting);
            // Await pure key generation before cancellation so shutdown owns its worker.
            let identity =
                tokio::task::spawn_blocking(rusthinq_bridge::notifications::identity).await;
            let Ok(Ok((key, csr))) = identity else {
                self.status(Status::IdentityFailed);
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = wanted.changed() => {},
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                };
                continue;
            };
            if *stop.borrow() || !self.enabled() {
                continue;
            }
            let connection = async {
                let subscription = client
                    .notification_subscription(key, csr)
                    .await
                    .map_err(|_| std::io::Error::other("notification subscription unavailable"))?;
                let material = subscription.material;
                let connector = rusthinq_bridge::transport::Connector::notifications(
                    &material,
                    Default::default(),
                )?;
                let stream = connector.connect().await?;
                Ok::<_, std::io::Error>((stream, subscription.client_id, subscription.filters))
            };
            let ready = tokio::select! {
                biased;
                _ = stop.changed() => break,
                _ = wanted.changed() => continue,
                _ = epoch.changed() => { self.clear(); continue; },
                _ = clients.changed() => { self.clear(); continue; },
                value = connection => value,
            };
            if let Ok((stream, id, filters)) = ready {
                let (tx, mut rx) = tokio::sync::mpsc::channel(64);
                let session =
                    rusthinq_bridge::notifications::run(stream, &id, &filters, tx, stop.clone());
                tokio::pin!(session);
                // Connected transport; subscription completion is reported by the first event below.
                self.status(Status::Subscribing);
                loop {
                    tokio::select! {
                        biased;
                        _ = stop.changed() => return,
                        _ = wanted.changed() => break,
                        _ = epoch.changed() => { self.clear(); break; },
                        _ = clients.changed() => { self.clear(); break; },
                        _ = &mut session => break,
                        value = rx.recv() => {
                            let Some(value) = value else { break; };
                            if value["type"] == "ready" { self.status(Status::Connected); }
                            else { self.record(value); }
                        },
                    }
                }
            }
            if !self.enabled() {
                self.status(Status::Disabled);
                continue;
            }
            self.status(Status::Reconnecting);
            tokio::select! {
                _ = stop.changed() => break,
                _ = wanted.changed() => {},
                _ = epoch.changed() => self.clear(),
                _ = clients.changed() => self.clear(),
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            }
        }
        self.status(Status::Disabled);
    }
}
pub(crate) fn matches_device(value: &Value, device: &str) -> bool {
    value["devices"]
        .as_array()
        .is_some_and(|ids| ids.is_empty() || ids.iter().any(|id| id == device))
}

fn collect_ids(value: &Value, depth: usize, ids: &mut Vec<String>) {
    if depth > 8 || ids.len() >= 64 {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if ids.len() >= 64 {
                    break;
                }
                if matches!(key.as_str(), "deviceId" | "device_id" | "did") {
                    if let Some(id) = value
                        .as_str()
                        .filter(|id| !id.is_empty() && id.len() <= 256)
                        && !ids.iter().any(|v| v == id)
                    {
                        ids.push(id.into());
                    }
                } else {
                    collect_ids(value, depth + 1, ids);
                }
            }
        }
        Value::Array(values) => {
            for value in values.iter().take(64) {
                collect_ids(value, depth + 1, ids);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_or_oversized_envelopes_do_not_allocate_a_sequence() {
        let observer = Observer::new(true);
        observer.record(json!("invalid"));
        observer.record(json!({"raw":"x".repeat(MAX_EVENT_BYTES - 15)}));
        assert_eq!(observer.snapshot(0, 200, None)["cursor"], "0");
        let payload: Vec<_> = (0..100).map(|id| json!({"did":id.to_string()})).collect();
        observer.record(json!({"payload":payload}));
        assert_eq!(
            observer.snapshot(0, 200, None)["events"][0]["devices"]
                .as_array()
                .unwrap()
                .len(),
            64
        );
    }
    #[test]
    fn reset_and_notifications_are_broadcast_in_history_order() {
        let observer = Observer::new(true);
        let mut messages = observer.subscribe();
        observer.record(json!({"payload":{"did":"before"}}));
        observer.clear();
        observer.record(json!({"payload":{"did":"after"}}));
        assert_eq!(messages.try_recv().unwrap()["sequence"], "1");
        assert_eq!(messages.try_recv().unwrap()["type"], "cloudReset");
        assert_eq!(messages.try_recv().unwrap()["sequence"], "2");
        let snapshot = observer.snapshot(0, 200, None);
        assert_eq!(snapshot["events"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["events"][0]["sequence"], "2");
    }
    #[test]
    fn bounded_history_exposes_gaps_filters_exact_ids_and_preserves_account_events() {
        let observer = Observer::new(true);
        assert!(!observer.enabled());
        for id in 0..1005 {
            observer.record(json!({"payload":{"deviceId":if id%2==0{"d"}else{"other"}},"raw":"{}","topic":"feed"}));
        }
        let snapshot = observer.snapshot(1, 200, Some("d"));
        assert_eq!(snapshot["buffered"], 1000);
        assert_eq!(snapshot["evicted"], 5);
        assert_eq!(snapshot["lost"], true);
        assert!(
            snapshot["events"]
                .as_array()
                .unwrap()
                .iter()
                .all(|value| value["devices"] == json!(["d"]))
        );
        let next: u64 = snapshot["nextCursor"].as_str().unwrap().parse().unwrap();
        assert!(next > 1);
        observer.record(json!({"payload":{"event":"account"},"raw":"{}","topic":"feed"}));
        let tail = observer.snapshot(1005, 200, Some("d"));
        assert_eq!(tail["events"][0]["correlation"], "account");
        let sequence = tail["cursor"].clone();
        observer.clear();
        let reset = observer.snapshot(0, 200, None);
        assert_eq!(reset["buffered"], 0);
        assert_eq!(reset["cursor"], sequence);
    }
    #[test]
    fn pagination_remains_bounded_by_bytes_and_cursor_advances_over_filtered_events() {
        let observer = Observer::new(true);
        for _ in 0..30 {
            observer
                .record(json!({"payload":{"did":"other"},"raw":"x".repeat(60000),"topic":"feed"}));
        }
        let page = observer.snapshot(0, 200, Some("d"));
        assert_eq!(page["events"], json!([]));
        assert!(page["nextCursor"].as_str().unwrap().parse::<u64>().unwrap() > 0);
        assert!(page.to_string().len() < 1048576);
        let page = observer.snapshot(0, 200, None);
        assert!(page.to_string().len() < 1048576);
        assert!(page["events"].as_array().unwrap().len() < 30);
    }
}
