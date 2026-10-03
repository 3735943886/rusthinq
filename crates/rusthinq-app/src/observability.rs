//! Bounded, opaque publication and diagnostic projections. No device semantics here.
use crate::runtime::Event;
use rusthinq_lifecycle::SessionKey;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
    time::Instant,
};
#[derive(Default)]
struct Publications {
    session: Option<SessionKey>,
    generation: u64,
    values: BTreeMap<String, Value>,
    bytes: usize,
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
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.publications.contains_key(id) && state.publications.len() >= 256 {
            return;
        }
        let total: usize = state.publications.values().map(|v| v.bytes).sum();
        if total.saturating_add(payload.len()) > 16 * 1024 * 1024 {
            state.publications.remove(id);
            return;
        }
        let cache = state.publications.entry(id.into()).or_default();
        if cache.session != Some(session) || cache.generation != generation {
            *cache = Publications {
                session: Some(session),
                generation,
                ..Default::default()
            };
        }
        let previous = cache
            .values
            .get(topic)
            .map(|v| v.to_string().len())
            .unwrap_or(0);
        let bytes = value.to_string().len();
        if body.is_empty() {
            cache.values.remove(topic);
            cache.bytes = cache.bytes.saturating_sub(previous);
            return;
        }
        if (cache.values.len() >= 256 && !cache.values.contains_key(topic))
            || cache.bytes.saturating_sub(previous) + bytes > 524288
        {
            return;
        }
        cache.bytes = cache.bytes.saturating_sub(previous) + bytes;
        cache.values.insert(topic.into(), value);
    }
    pub fn publications(&self, id: &str, session: SessionKey, generation: u64) -> Value {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        json!(
            state
                .publications
                .get(id)
                .filter(|v| v.session == Some(session) && v.generation == generation)
                .map(|v| v.values.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        )
    }
    pub fn observe(&self, event: &Event) {
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            match event {
                Event::Lifecycle(rusthinq_lifecycle::Action::Changed(device)) if device.online => {
                    if state.signals.contains_key(&device.entry.id) || state.signals.len() < 256 {
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
                    if state.signals.contains_key(&id.device) || state.signals.len() < 256 {
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
                }
                _ => {}
            }
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
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .publications
                    .remove(id);
                ("forgotten", Some(id.as_str()), None)
            }
            _ => return,
        };
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let count = state.counters.entry(kind).or_default();
        *count = count.saturating_add(1);
        if reason.is_some() {
            state
                .recent
                .push_back(json!({"t":now_ms(),"kind":kind,"device":device,"reason":reason}));
            while state.recent.len() > 100 {
                state.recent.pop_front();
            }
        }
    }
    pub fn diagnostics(&self) -> Value {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        json!({"uptimeSeconds":self.started.elapsed().as_secs(),"counters":state.counters,"devices":state.signals,"recent":state.recent,"limits":{"recent":100,"devices":256,"publicationsPerDevice":256,"publicationBytesPerDevice":524288,"publicationBytesTotal":16777216}})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
