//! Management-owned account observer. Ephemeral opt-in; bounded history, joined shutdown.
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::{broadcast, watch};
struct State {
    status: &'static str,
    sequence: u64,
    evicted: u64,
    bytes: usize,
    events: VecDeque<Value>,
}
#[derive(Clone)]
pub struct Observer {
    state: Arc<Mutex<State>>,
    wanted: watch::Sender<bool>,
    events: broadcast::Sender<Value>,
    available: bool,
}
impl Observer {
    pub fn new(available: bool) -> Self {
        let (wanted, _) = watch::channel(false);
        let (events, _) = broadcast::channel(256);
        Self {
            state: Arc::new(Mutex::new(State {
                status: "disabled",
                sequence: 0,
                evicted: 0,
                bytes: 0,
                events: VecDeque::new(),
            })),
            wanted,
            events,
            available,
        }
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
            self.status("disabled");
        }
        Ok(())
    }
    fn status(&self, status: &'static str) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).status = status;
        let _ = self
            .events
            .send(json!({"type":"cloudStatus","status":status,"enabled":self.enabled()}));
    }
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.events.clear();
        state.bytes = 0;
        state.evicted = 0;
        drop(state);
        let _ = self.events.send(json!({"type":"cloudReset"}));
    }
    pub fn snapshot(&self, cursor: u64, limit: usize, device: Option<&str>) -> Value {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let first = state
            .events
            .front()
            .and_then(|v| v["sequence"].as_str())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(state.sequence.saturating_add(1));
        let mut bytes = 0;
        let selected: Vec<_> = state
            .events
            .iter()
            .filter(|v| {
                v["sequence"]
                    .as_str()
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|s| s > cursor)
            })
            .take(limit.min(200))
            .take_while(|value| {
                let size = value.to_string().len();
                if bytes > 0 && bytes + size > 524288 {
                    return false;
                }
                bytes += size;
                true
            })
            .cloned()
            .collect();
        let next = selected
            .last()
            .and_then(|v| v["sequence"].as_str())
            .unwrap_or("")
            .to_string();
        let selected: Vec<_> = selected
            .into_iter()
            .filter(|v| {
                device.is_none_or(|id| {
                    v["devices"]
                        .as_array()
                        .is_some_and(|ids| ids.is_empty() || ids.iter().any(|v| v == id))
                })
            })
            .collect();
        json!({"available":self.available,"enabled":self.enabled(),"status":state.status,"events":selected,"nextCursor":if cursor>state.sequence{"0".into()}else if next.is_empty(){cursor.to_string()}else{next},"cursor":state.sequence.to_string(),"evicted":state.evicted,"reset":cursor>state.sequence,"lost":cursor!=0&&cursor.saturating_add(1)<first,"buffered":state.events.len()})
    }
    pub fn record(&self, mut value: Value) {
        let bytes = value.to_string().len();
        if bytes > 524288 {
            return;
        }
        let mut ids = Vec::new();
        collect_ids(&value["payload"], 0, &mut ids);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(sequence) = state.sequence.checked_add(1) else {
            return;
        };
        state.sequence = sequence;
        value["type"] = json!("cloudNotification");
        value["k"] = json!("cloud");
        value["t"] = json!(crate::observability::now_ms());
        value["sequence"] = json!(sequence.to_string());
        value["devices"] = json!(ids);
        value["correlation"] = json!(
            if value["devices"].as_array().is_some_and(|v| v.is_empty()) {
                "account"
            } else {
                "device"
            }
        );
        let bytes = value.to_string().len();
        state.bytes += bytes;
        state.events.push_back(value.clone());
        while state.events.len() > 1000 || state.bytes > 8 * 1024 * 1024 {
            if let Some(old) = state.events.pop_front() {
                state.bytes = state.bytes.saturating_sub(old.to_string().len());
                state.evicted = state.evicted.saturating_add(1);
            }
        }
        drop(state);
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
                tokio::select! {_=stop.changed()=>break,_=wanted.changed()=>{},_ = epoch.changed()=>{self.clear();}};
                continue;
            }
            let client = clients.borrow_and_update().clone();
            let Some(client) = client else {
                self.status("waitingForLogin");
                self.clear();
                tokio::select! {_=stop.changed()=>break,_=wanted.changed()=>{},_=clients.changed()=>{},_=epoch.changed()=>{self.clear();}};
                continue;
            };
            epoch.borrow_and_update();
            self.status("connecting");
            // Await pure key generation before cancellation so shutdown owns its worker.
            let identity =
                tokio::task::spawn_blocking(rusthinq_bridge::notifications::identity).await;
            let Ok(Ok((key, csr))) = identity else {
                self.status("identityFailed");
                tokio::select! {_=stop.changed()=>break,_=wanted.changed()=>{},_=tokio::time::sleep(Duration::from_secs(5))=>{}};
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
            let ready = tokio::select! {biased;_=stop.changed()=>break,_=wanted.changed()=>continue,_=epoch.changed()=>{self.clear();continue;},_=clients.changed()=>{self.clear();continue;},value=connection=>value};
            if let Ok((stream, id, filters)) = ready {
                let (tx, mut rx) = tokio::sync::mpsc::channel(64);
                let session =
                    rusthinq_bridge::notifications::run(stream, &id, &filters, tx, stop.clone());
                tokio::pin!(session);
                // Connected transport; subscription completion is reported by the first event below.
                self.status("subscribing");
                loop {
                    tokio::select! {biased;_=stop.changed()=>return,_=wanted.changed()=>break,_=epoch.changed()=>{self.clear();break;},_=clients.changed()=>{self.clear();break;},_=&mut session=>break,value=rx.recv()=>if let Some(value)=value {if value["type"]=="ready" {self.status("connected");}else {self.record(value);}}else{break;}}
                }
            }
            if !self.enabled() {
                self.status("disabled");
                continue;
            }
            self.status("reconnecting");
            tokio::select! {_=stop.changed()=>break,_=wanted.changed()=>{},_=epoch.changed()=>{self.clear();},_=clients.changed()=>{self.clear();},_=tokio::time::sleep(Duration::from_secs(5))=>{}}
        }
        self.status("disabled");
    }
}
fn collect_ids(value: &Value, depth: usize, ids: &mut Vec<String>) {
    if depth > 8 || ids.len() >= 64 {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, value) in map {
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
