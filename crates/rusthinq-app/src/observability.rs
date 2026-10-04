//! Bounded, opaque publication and diagnostic projections. No device semantics here.
use crate::runtime::Event;
use rusthinq_lifecycle::SessionKey;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
    time::Instant,
};
const MAX_DEVICES: usize = 256;
const MAX_PUBLICATIONS: usize = 256;
const MAX_DEVICE_BYTES: usize = 512 * 1024;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECENT: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Scope {
    session: SessionKey,
    generation: u64,
}
struct Publication {
    value: Value,
    bytes: usize,
}
struct Publications {
    scope: Scope,
    values: BTreeMap<String, Publication>,
    bytes: usize,
}
impl Publications {
    fn new(scope: Scope) -> Self {
        Self {
            scope,
            values: BTreeMap::new(),
            bytes: 0,
        }
    }
}
#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Signals {
    received: u64,
    sent: u64,
    connections: u64,
    last_received_ms: Option<u64>,
    last_sent_ms: Option<u64>,
    last_connection_ms: Option<u64>,
    #[serde(skip)]
    session: Option<u64>,
}
#[derive(Default)]
struct State {
    signals: BTreeMap<String, Signals>,
    publications: BTreeMap<String, Publications>,
    counters: BTreeMap<&'static str, u64>,
    recent: VecDeque<Value>,
    publication_bytes: usize,
}
impl State {
    fn remove_publications(&mut self, id: &str) {
        if let Some(cache) = self.publications.remove(id) {
            self.publication_bytes -= cache.bytes;
        }
    }
}
pub struct Observability {
    started: Instant,
    state: Mutex<State>,
}
impl Default for Observability {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            state: Mutex::new(State::default()),
        }
    }
}
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
impl Observability {
    pub fn publish(&self, id: &str, session: SessionKey, generation: u64, payload: &str) {
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        if value["retain"] != true {
            return;
        }
        let Some(topic) = value["topic"].as_str().filter(|s| s.len() <= 1024) else {
            return;
        };
        let Some(body) = value["payload"].as_str().filter(|s| s.len() <= 65536) else {
            return;
        };
        let scope = Scope {
            session,
            generation,
        };
        let mut state = self.lock();
        if state
            .publications
            .get(id)
            .is_some_and(|cache| cache.scope != scope)
        {
            state.remove_publications(id);
        }
        if !state.publications.contains_key(id) && state.publications.len() >= MAX_DEVICES {
            return;
        }
        let cache = state
            .publications
            .entry(id.into())
            .or_insert_with(|| Publications::new(scope));
        let previous = cache.values.get(topic).map_or(0, |entry| entry.bytes);
        if body.is_empty() {
            cache.values.remove(topic);
            cache.bytes -= previous;
            state.publication_bytes -= previous;
            return;
        }
        let bytes = value.to_string().len();
        let next_device_bytes = cache.bytes - previous + bytes;
        if (cache.values.len() >= MAX_PUBLICATIONS && !cache.values.contains_key(topic))
            || next_device_bytes > MAX_DEVICE_BYTES
        {
            return;
        }
        let next_total_bytes = state.publication_bytes - previous + bytes;
        if next_total_bytes > MAX_TOTAL_BYTES {
            state.remove_publications(id);
            return;
        }
        let cache = state
            .publications
            .get_mut(id)
            .expect("admitted publication cache");
        cache.bytes = next_device_bytes;
        cache
            .values
            .insert(topic.into(), Publication { value, bytes });
        state.publication_bytes = next_total_bytes;
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    pub fn publications(&self, id: &str, session: SessionKey, generation: u64) -> Value {
        let state = self.lock();
        let scope = Scope {
            session,
            generation,
        };
        let values: Vec<_> = state
            .publications
            .get(id)
            .filter(|cache| cache.scope == scope)
            .map(|cache| cache.values.values().map(|entry| &entry.value).collect())
            .unwrap_or_default();
        json!(values)
    }
    pub fn observe(&self, event: &Event) {
        let mut state = self.lock();
        match event {
            Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.online => {
                if state.signals.contains_key(&device.entry.id) || state.signals.len() < MAX_DEVICES
                {
                    let signals = state.signals.entry(device.entry.id.clone()).or_default();
                    let generation = device.session.map(|v| v.generation);
                    if generation.is_some() && signals.session != generation {
                        signals.connections = signals.connections.saturating_add(1);
                        signals.last_connection_ms = Some(now_ms());
                        signals.session = generation;
                    }
                }
            }
            Event::Transport(rusthinq_server::Event::Data(id, _))
            | Event::Transport(rusthinq_server::Event::Sent(id, _)) => {
                if state.signals.contains_key(&id.device) || state.signals.len() < MAX_DEVICES {
                    let signals = state.signals.entry(id.device.clone()).or_default();
                    if matches!(event, Event::Transport(rusthinq_server::Event::Data(..))) {
                        signals.received = signals.received.saturating_add(1);
                        signals.last_received_ms = Some(now_ms());
                    } else {
                        signals.sent = signals.sent.saturating_add(1);
                        signals.last_sent_ms = Some(now_ms());
                    }
                }
            }
            Event::Lifecycle(rusthinq_lifecycle::Action::Removed { id, .. }) => {
                state.signals.remove(id);
                state.remove_publications(id);
            }
            _ => {}
        }
        let (kind, device, reason) = match event {
            Event::Transport(rusthinq_server::Event::Data(id, _)) => {
                ("received", Some(id.device.as_str()), None)
            }
            Event::Transport(rusthinq_server::Event::Sent(id, _)) => {
                ("sent", Some(id.device.as_str()), None)
            }
            Event::Rejected { device, .. } => (
                "rejected",
                Some(device.as_str()),
                Some("operation rejected"),
            ),
            Event::ScriptExecuted { context, error, .. } => (
                if error.is_some() {
                    "scriptFault"
                } else {
                    "scriptExecuted"
                },
                Some(context.device.as_str()),
                error.as_ref().map(|_| "script execution failed"),
            ),
            Event::Lost { .. } => ("transportLoss", None, Some("transport events missed")),
            Event::Lifecycle(rusthinq_lifecycle::Action::Removed { id, .. }) => {
                ("forgotten", Some(id.as_str()), None)
            }
            _ => return,
        };
        let count = state.counters.entry(kind).or_default();
        *count = count.saturating_add(1);
        if reason.is_some() {
            state
                .recent
                .push_back(json!({"t":now_ms(),"kind":kind,"device":device,"reason":reason}));
            while state.recent.len() > MAX_RECENT {
                state.recent.pop_front();
            }
        }
    }
    pub fn diagnostics(&self) -> Value {
        let state = self.lock();
        json!({"uptimeSeconds":self.started.elapsed().as_secs(),"counters":state.counters,"devices":state.signals,"recent":state.recent,"limits":{"recent":MAX_RECENT,"devices":MAX_DEVICES,"publicationsPerDevice":MAX_PUBLICATIONS,"publicationBytesPerDevice":MAX_DEVICE_BYTES,"publicationBytesTotal":MAX_TOTAL_BYTES}})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_cache_accepts_same_size_replacements_and_releases_deleted_or_retired_values() {
        let observer = Observability::default();
        let session = SessionKey {
            incarnation: 1,
            generation: 1,
        };
        let body = "x".repeat(60000);
        for device in 0..35 {
            for topic in 0..if device == 34 { 7 } else { 8 } {
                observer.publish(
                    &device.to_string(),
                    session,
                    1,
                    &json!({"topic":topic.to_string(),"payload":body,"retain":true}).to_string(),
                );
            }
        }
        // Total usage is less than one publication below the global limit.
        observer.publish(
            "0",
            session,
            1,
            &json!({"topic":"0","payload":"y".repeat(60000),"retain":true}).to_string(),
        );
        assert_eq!(
            observer
                .publications("0", session, 1)
                .as_array()
                .unwrap()
                .len(),
            8
        );
        observer.publish(
            "0",
            session,
            1,
            &json!({"topic":"0","payload":"","retain":true}).to_string(),
        );
        assert_eq!(
            observer
                .publications("0", session, 1)
                .as_array()
                .unwrap()
                .len(),
            7
        );
        observer.publish(
            "1",
            session,
            2,
            &json!({"topic":"next","payload":"new scope","retain":true}).to_string(),
        );
        assert_eq!(
            observer
                .publications("1", session, 2)
                .as_array()
                .unwrap()
                .len(),
            1
        );
        observer.observe(&Event::Lifecycle(rusthinq_lifecycle::Action::Removed {
            id: "2".into(),
            incarnation: 1,
        }));
        assert_eq!(observer.publications("2", session, 1), json!([]));
        let state = observer.lock();
        assert_eq!(
            state.publication_bytes,
            state
                .publications
                .values()
                .map(|cache| cache.bytes)
                .sum::<usize>()
        );
    }
    #[test]
    fn publication_projection_is_fenced_and_never_includes_transient_events() {
        let observer = Observability::default();
        let scope = SessionKey {
            incarnation: 1,
            generation: 2,
        };
        observer.publish(
            "d",
            scope,
            1,
            &json!({"topic":"il/d","payload":"descriptor","retain":true}).to_string(),
        );
        observer.publish(
            "d",
            scope,
            1,
            &json!({"topic":"d/reject","payload":"transient","retain":false}).to_string(),
        );
        assert_eq!(
            observer
                .publications("d", scope, 1)
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            observer.publications(
                "d",
                SessionKey {
                    incarnation: 2,
                    ..scope
                },
                1
            ),
            json!([])
        );
        assert_eq!(observer.publications("d", scope, 2), json!([]));
        observer.publish(
            "d",
            scope,
            2,
            &json!({"topic":"d/power","payload":"on","retain":true}).to_string(),
        );
        assert_eq!(observer.publications("d", scope, 1), json!([]));
        assert_eq!(observer.publications("d", scope, 2)[0]["topic"], "d/power");
        observer.publish(
            "d",
            scope,
            2,
            &json!({"topic":"d/power","payload":"","retain":true}).to_string(),
        );
        assert_eq!(observer.publications("d", scope, 2), json!([]));
    }
}
