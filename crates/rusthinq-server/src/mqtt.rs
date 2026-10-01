//! Local ThinQ2 device MQTT service. No external broker, cloud dial, or IL semantics.
use crate::{
    Command, Config, Delivery, Disconnect, Entry, Event, Guard, Protocol, Receipt, Reject,
    ServerHandle, SessionId, Shared, State,
    tls::{LocalFuture, LocalService, Transport},
    write_frame,
};
use base64::Engine;
use chrono::{Datelike, Timelike};
use rusthinq_protocol::{
    mqtt::{self, Packet},
    thinq2::{self, Action, Input},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap},
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite},
    sync::{broadcast, mpsc, oneshot, watch},
    time::Instant,
};

pub struct Sample {
    pub mid: u64,
    pub calendar: [u8; 7],
}
pub trait Clock: Send + Sync {
    fn sample(&self) -> io::Result<Sample>;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn sample(&self) -> io::Result<Sample> {
        let now = chrono::Utc::now();
        Ok(Sample {
            mid: now
                .timestamp_millis()
                .try_into()
                .map_err(io::Error::other)?,
            calendar: [
                now.year().rem_euclid(100) as u8,
                now.month0() as u8,
                now.day() as u8,
                now.hour() as u8,
                now.minute() as u8,
                now.second() as u8,
                now.weekday().num_days_from_sunday() as u8,
            ],
        })
    }
}
#[derive(Clone)]
pub struct Broker {
    shared: Arc<Shared>,
    clock: Arc<dyn Clock>,
    stop: watch::Sender<bool>,
}
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}
impl Handle {
    pub fn generation_budget(&self) -> (u64, u64) {
        ServerHandle(self.shared.clone()).generation_budget()
    }
    pub async fn close_and_wait(&self, session: &SessionId) -> Result<(), Reject> {
        ServerHandle(self.shared.clone())
            .close_and_wait(session)
            .await
    }
    pub fn extend_generations(&self, floor: u64, ceiling: u64) -> Result<(), Reject> {
        ServerHandle(self.shared.clone()).extend_generations(floor, ceiling)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.shared.events.subscribe()
    }
    pub fn snapshot(&self) -> Vec<SessionId> {
        ServerHandle(self.shared.clone()).snapshot()
    }
    pub fn close(&self, id: &SessionId) -> Result<(), Reject> {
        ServerHandle(self.shared.clone()).close(id)
    }
    pub fn send(&self, id: &SessionId, payload: &[u8]) -> Result<Receipt, Reject> {
        if payload.len() > self.shared.config.max_payload {
            return Err(Reject::PayloadExceeded);
        }
        let _: Value = serde_json::from_slice(payload).map_err(|_| Reject::InvalidJson)?;
        let frame = encode_publish(
            &format!("lime/devices/{}", id.device),
            payload,
            &self.shared.config,
        )
        .map_err(|_| Reject::PayloadExceeded)?;
        let state = self.shared.lock();
        if state.stopped {
            return Err(Reject::Stopped);
        }
        let entry = state
            .entries
            .get(&id.device)
            .filter(|entry| entry.id == *id && entry.ready)
            .ok_or(Reject::StaleSession)?;
        if entry.protocol != Protocol::ThinQ2 {
            return Err(Reject::WrongTransport);
        }
        let (result, receive) = oneshot::channel();
        entry
            .commands
            .try_send(Command { frame, result })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Reject::Busy,
                mpsc::error::TrySendError::Closed(_) => Reject::StaleSession,
            })?;
        Ok(Receipt(receive))
    }
}
impl Broker {
    pub fn new(config: Config, clock: Arc<dyn Clock>) -> Result<Self, Reject> {
        config.validate()?;
        let (events, _) = broadcast::channel(config.event_capacity);
        let (stop, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            stop: stop.clone(),
            completions: Mutex::new(HashMap::new()),
            config: config.clone(),
            events,
            state: Mutex::new(State {
                entries: HashMap::new(),
                generation: config.generation_floor,
                generation_ceiling: config.generation_ceiling,
                active: BTreeSet::new(),
                identified: HashMap::new(),
                stopped: false,
            }),
        });
        Ok(Self {
            shared,
            clock,
            stop,
        })
    }
    pub fn handle(&self) -> Handle {
        Handle {
            shared: self.shared.clone(),
        }
    }
    /// Share the ThinQ1 owner's sessions, allocator, admission bound and stop signal.
    /// MQTT transport tasks remain caller/front-door owned.
    pub fn sharing(server: ServerHandle, clock: Arc<dyn Clock>) -> Self {
        Self {
            stop: server.0.stop.clone(),
            shared: server.0,
            clock,
        }
    }
    /// Signals all connections separately from data queues. Their caller/front door owns joins.
    pub fn stop(&self) {
        self.shared.lock().stopped = true;
        self.stop.send_replace(true);
    }
    pub async fn run<S: AsyncRead + AsyncWrite + Unpin + Send>(&self, stream: S) -> io::Result<()> {
        let generation = {
            let mut state = self.shared.lock();
            if state.stopped || state.active.len() >= self.shared.config.max_connections {
                return Err(io::Error::other("broker stopped or full"));
            }
            state.generation = state
                .generation
                .checked_add(1)
                .filter(|next| *next <= state.generation_ceiling)
                .ok_or_else(|| io::Error::other("generation exhausted"))?;
            let generation = state.generation;
            state.active.insert(generation);
            generation
        };
        let guard = Guard {
            shared: self.shared.clone(),
            id: None,
            reason: Disconnect::Panic,
            generation,
        };
        let (completed, completion) = watch::channel(false);
        {
            let mut completions = self
                .shared
                .completions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            completions.retain(|_, receiver| !*receiver.borrow() && receiver.has_changed().is_ok());
            completions.insert(generation, completion);
        }
        connection(stream, self, guard).await;
        completed.send_replace(true);
        Ok(())
    }
}
impl LocalService for Broker {
    fn serve(&self, stream: Transport) -> LocalFuture {
        let broker = self.clone();
        Box::pin(async move { broker.run(stream).await })
    }
}
fn encode_publish(topic: &str, payload: &[u8], config: &Config) -> Result<Vec<u8>, mqtt::Error> {
    if payload.len() > config.max_payload {
        return Err(mqtt::Error::Exceeded);
    }
    mqtt::publish(topic, payload, maximum(config))
}
fn maximum(config: &Config) -> usize {
    config.max_payload.saturating_add(4096)
}
async fn read_packet<S: AsyncRead + Unpin>(
    stream: &mut S,
    maximum: usize,
) -> Result<Packet, Disconnect> {
    let mut bytes = vec![stream.read_u8().await.map_err(|_| Disconnect::Eof)?];
    loop {
        match mqtt::length(&bytes, maximum).map_err(|_| Disconnect::Mqtt)? {
            Some(length) => {
                let header = bytes.len();
                bytes.resize(length, 0);
                stream
                    .read_exact(&mut bytes[header..])
                    .await
                    .map_err(|_| Disconnect::Mqtt)?;
                return mqtt::decode(&bytes, maximum).map_err(|_| Disconnect::Mqtt);
            }
            None => bytes.push(stream.read_u8().await.map_err(|_| Disconnect::Mqtt)?),
        }
    }
}
async fn connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    broker: &Broker,
    mut guard: Guard,
) {
    let config = &broker.shared.config;
    let start = Instant::now();
    let mut model = thinq2::Session::new(config.max_payload, false, Duration::ZERO);
    let mut stop = broker.stop.subscribe();
    let (close, mut closing) = watch::channel(false);
    let (commands, mut outbound) = mpsc::channel::<Command>(config.outbound_capacity);
    let mut connected = false;
    let mut will = None;
    let mut clean_disconnect = false;
    let mut idle = config.idle_timeout;
    let mut deadline = Instant::now() + idle;
    let mut filters: Vec<String> = Vec::new();
    let mut qos2: HashMap<u16, (u64, String, Vec<u8>)> = HashMap::new();
    // Reads persist across outbound command selection; cancellation never loses partial packets.
    let (mut reader, mut writer) = tokio::io::split(&mut stream);
    let reason = 'connection: loop {
        if *stop.borrow() || *closing.borrow() || !guard.current() {
            break Disconnect::Closed;
        }
        let read = read_packet(&mut reader, maximum(config));
        tokio::pin!(read);
        let packet = loop {
            if Instant::now() >= deadline {
                break 'connection Disconnect::IdleTimeout;
            }
            tokio::select! {
                _ = stop.changed() => break 'connection Disconnect::Closed,
                _ = closing.changed() => break 'connection Disconnect::Closed,
                _ = tokio::time::sleep_until(deadline) => break 'connection Disconnect::IdleTimeout,
                command = outbound.recv(), if connected => {
                    if let Some(command) = command {
                        if !guard.current() { let _ = command.result.send(Delivery::Failed); break 'connection Disconnect::Closed; }
                        let topic = format!("lime/devices/{}", guard.id.as_ref().expect("registered command").device);
                        if !filters.iter().any(|filter| mqtt::matches(filter, &topic)) { let _ = command.result.send(Delivery::Failed); continue; }
                        let result = write_frame(&mut writer, &command.frame, config.write_timeout, &mut stop, &mut closing).await;
                        let delivery = match &result { Ok(()) => Delivery::Sent, Err((_, 0)) => Delivery::Failed, Err(_) => Delivery::Unknown };
                        let _ = command.result.send(delivery);
                        if let Err((reason, _)) = result { break 'connection reason; }
                    }
                }
                packet = &mut read => break match packet { Ok(packet) => packet, Err(reason) => break 'connection reason },
            }
        };
        if Instant::now() >= deadline {
            break Disconnect::IdleTimeout;
        }
        deadline = Instant::now() + idle;
        let mut actions = Vec::new();
        let mut replies = Vec::new();
        match packet {
            Packet::Connect {
                keep_alive,
                will: message,
                ..
            } if !connected => {
                will = message;
                connected = true;
                // Preserve the appliance-tested grace floor from the 0.1 broker.
                idle = mqtt_idle_timeout(keep_alive, config.idle_timeout);
                deadline = Instant::now() + idle;
                replies.push(vec![0x20, 2, 0, 0]);
            }
            _ if !connected => break Disconnect::Mqtt,
            Packet::Connect { .. } => break Disconnect::Mqtt,
            Packet::Subscribe {
                id,
                filters: requested,
            } => {
                let mut next = filters.clone();
                for filter in &requested {
                    if !next.contains(filter) {
                        next.push(filter.clone());
                    }
                }
                if next.len() > 16 {
                    break Disconnect::Mqtt;
                }
                filters = next;
                let mut body = id.to_be_bytes().to_vec();
                body.resize(2 + requested.len(), 0); // grant QoS 0
                replies.push(mqtt::frame(0x90, &body, maximum(config)).expect("bounded SUBACK"));
            }
            Packet::Unsubscribe {
                id,
                filters: removed,
            } => {
                filters.retain(|filter| !removed.contains(filter));
                replies.push(vec![0xb0, 2, (id >> 8) as u8, id as u8]);
            }
            Packet::Ping => replies.push(vec![0xd0, 0]),
            Packet::Disconnect => {
                clean_disconnect = true;
                break Disconnect::Eof;
            }
            Packet::Publish {
                topic,
                payload,
                id: Some(id),
                qos: 2,
                ..
            } => {
                if payload.len() > config.max_payload {
                    break Disconnect::Mqtt;
                }
                if let Some((_, previous_topic, previous_payload)) = qos2.get(&id) {
                    if previous_topic != &topic || previous_payload != &payload {
                        break Disconnect::Mqtt;
                    }
                } else {
                    if qos2.len() >= config.outbound_capacity {
                        break Disconnect::Mqtt;
                    }
                    qos2.insert(id, (guard.generation, topic, payload));
                }
                replies.push(vec![0x50, 2, (id >> 8) as u8, id as u8]);
            }
            Packet::PubRel { id } => {
                // Unknown/repeated PUBREL still completes without replaying device input.
                if let Some((generation, topic, payload)) = qos2.remove(&id)
                    && generation == guard.generation
                {
                    let sample = match broker.clock.sample() {
                        Ok(sample) => sample,
                        Err(_) => break Disconnect::Io,
                    };
                    let outcome = model.input(
                        Input::Device {
                            topic: &topic,
                            payload: &payload,
                            outbound_mid: sample.mid,
                        },
                        start.elapsed(),
                    );
                    if let Some(error) = outcome.error {
                        break Disconnect::ThinQ2(error);
                    }
                    actions = outcome.actions;
                }
                replies.push(vec![0x70, 2, (id >> 8) as u8, id as u8]);
            }
            Packet::Publish {
                topic,
                payload,
                id,
                duplicate: _,
                qos: _,
            } => {
                // QoS1 is at-least-once. DUP can also be the first delivery seen here.
                // Packet identifiers are reusable after PUBACK, so do not infer
                // application-level duplication from an identifier or DUP alone.
                let sample = match broker.clock.sample() {
                    Ok(sample) => sample,
                    Err(_) => break Disconnect::Io,
                };
                let outcome = model.input(
                    Input::Device {
                        topic: &topic,
                        payload: &payload,
                        outbound_mid: sample.mid,
                    },
                    start.elapsed(),
                );
                if let Some(error) = outcome.error {
                    break Disconnect::ThinQ2(error);
                }
                actions = outcome.actions;
                if let Some(id) = id {
                    replies.push(vec![0x40, 2, (id >> 8) as u8, id as u8]);
                }
            }
        }
        for reply in replies {
            if let Err((reason, _)) = write_frame(
                &mut writer,
                &reply,
                config.write_timeout,
                &mut stop,
                &mut closing,
            )
            .await
            {
                break 'connection reason;
            }
        }
        let mut index = 0;
        while index < actions.len() {
            if !guard.current() {
                break 'connection Disconnect::Closed;
            }
            let action = actions[index].clone();
            index += 1;
            match action {
                Action::Provision { operation, request } => {
                    let device = request["did"].as_str().expect("L1 deploy ID").to_owned();
                    if device.len() > 256 {
                        break 'connection Disconnect::Mqtt;
                    }
                    {
                        let mut state = broker.shared.lock();
                        if let Some(previous) = guard.id.as_ref() {
                            if state.stopped
                                || !state
                                    .entries
                                    .get(&previous.device)
                                    .is_some_and(|entry| entry.id == *previous)
                            {
                                break 'connection Disconnect::Closed;
                            }
                            let Some(next) = state
                                .generation
                                .checked_add(1)
                                .filter(|next| *next <= state.generation_ceiling)
                            else {
                                break 'connection Disconnect::ThinQ2(
                                    thinq2::Error::CounterExhausted,
                                );
                            };
                            state.generation = next;
                            state.active.remove(&guard.generation);
                            state.active.insert(next);
                            let previous_generation = guard.generation;
                            guard.generation = next;
                            {
                                let mut completions = broker
                                    .shared
                                    .completions
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner());
                                if let Some(completion) = completions.remove(&previous_generation) {
                                    completions.insert(next, completion);
                                }
                            }
                            if let Some(old) = state.entries.remove(&previous.device)
                                && old.ready
                            {
                                let _ = broker
                                    .shared
                                    .events
                                    .send(Event::Down(old.id, Disconnect::Closed));
                            }
                            // All queued sends belong to the previous logical incarnation.
                            while let Ok(command) = outbound.try_recv() {
                                let _ = command.result.send(Delivery::Failed);
                            }
                        }
                        let id = SessionId {
                            device,
                            generation: guard.generation,
                        };
                        if state.stopped
                            || state
                                .identified
                                .get(&id.device)
                                .is_some_and(|latest| *latest >= id.generation)
                        {
                            break 'connection Disconnect::Closed;
                        }
                        if !state.identified.contains_key(&id.device)
                            && state.identified.len() >= config.max_connections
                        {
                            break 'connection Disconnect::Closed;
                        }
                        state.identified.insert(id.device.clone(), id.generation);
                        if let Some(old) = state.entries.insert(
                            id.device.clone(),
                            Entry {
                                protocol: Protocol::ThinQ2,
                                id: id.clone(),
                                commands: commands.clone(),
                                close: close.clone(),
                                ready: false,
                            },
                        ) {
                            old.close.send_replace(true);
                            if old.ready {
                                let _ = broker
                                    .shared
                                    .events
                                    .send(Event::Down(old.id, Disconnect::Closed));
                            }
                        }
                        guard.id = Some(id);
                    }
                    let sample = match broker.clock.sample() {
                        Ok(sample) => sample,
                        Err(_) => break 'connection Disconnect::Io,
                    };
                    let did = request["did"].as_str().expect("L1 deploy ID");
                    let response = json!({"did":did,"mid":sample.mid,"cmd":"completeProvisioning","type":0,"data":{"result":0,"host":"message","appInfo":{"host":"message","publication":{"message":format!("clip/message/devices/{did}"),"provisioning":format!("clip/provisioning/devices/{did}")}},"provisioningType":request["cmd"],"deployInterval":600}});
                    let topic = format!("lime/devices/{did}");
                    if !filters.iter().any(|filter| mqtt::matches(filter, &topic)) {
                        let _ = model.input(
                            Input::ProvisionResult {
                                operation,
                                sent: false,
                            },
                            start.elapsed(),
                        );
                        break 'connection Disconnect::ThinQ2(thinq2::Error::ProvisionFailed);
                    }
                    let frame =
                        match encode_publish(&topic, response.to_string().as_bytes(), config) {
                            Ok(frame) => frame,
                            Err(_) => {
                                let _ = model.input(
                                    Input::ProvisionResult {
                                        operation,
                                        sent: false,
                                    },
                                    start.elapsed(),
                                );
                                break 'connection Disconnect::ThinQ2(
                                    thinq2::Error::ProvisionFailed,
                                );
                            }
                        };
                    if let Err((reason, _)) = write_frame(
                        &mut writer,
                        &frame,
                        config.write_timeout,
                        &mut stop,
                        &mut closing,
                    )
                    .await
                    {
                        let _ = model.input(
                            Input::ProvisionResult {
                                operation,
                                sent: false,
                            },
                            start.elapsed(),
                        );
                        break 'connection reason;
                    }
                    let outcome = model.input(
                        Input::ProvisionResult {
                            operation,
                            sent: true,
                        },
                        start.elapsed(),
                    );
                    if let Some(error) = outcome.error {
                        break 'connection Disconnect::ThinQ2(error);
                    }
                    actions.extend(outcome.actions);
                }
                Action::Ready { device_id, deploy } => {
                    let id = guard.id.clone().expect("provisioned identity");
                    if id.device != device_id {
                        break 'connection Disconnect::Mqtt;
                    }
                    {
                        let mut state = broker.shared.lock();
                        if state.stopped {
                            break 'connection Disconnect::Closed;
                        }
                        let Some(entry) = state
                            .entries
                            .get_mut(&id.device)
                            .filter(|entry| entry.id == id)
                        else {
                            break 'connection Disconnect::Closed;
                        };
                        entry.ready = true;
                        let _ = broker.shared.events.send(Event::Up(id.clone()));
                        let _ = broker.shared.events.send(Event::Ready(id, deploy));
                    }
                }
                Action::Send {
                    topic,
                    payload,
                    bridge_generation: None,
                } => {
                    if !filters.iter().any(|filter| mqtt::matches(filter, &topic)) {
                        break 'connection Disconnect::Mqtt;
                    }
                    let frame = match encode_publish(&topic, &payload, config) {
                        Ok(frame) => frame,
                        Err(_) => break 'connection Disconnect::Mqtt,
                    };
                    if let Err((reason, _)) = write_frame(
                        &mut writer,
                        &frame,
                        config.write_timeout,
                        &mut stop,
                        &mut closing,
                    )
                    .await
                    {
                        break 'connection reason;
                    }
                }
                Action::TimeSyncRequested => {
                    let Some(did) = model.device_id() else {
                        break 'connection Disconnect::Mqtt;
                    };
                    let sample = match broker.clock.sample() {
                        Ok(sample) => sample,
                        Err(_) => break 'connection Disconnect::Io,
                    };
                    let payload = json!({"did":did,"mid":sample.mid,"cmd":"resp_timesync","type":1,"data":base64::engine::general_purpose::STANDARD.encode(sample.calendar)}).to_string();
                    actions.push(Action::Send {
                        topic: format!("lime/devices/{did}"),
                        payload: payload.into_bytes(),
                        bridge_generation: None,
                    });
                }
                Action::Data(payload) => {
                    guard.publish(Event::Data(
                        guard.id.clone().expect("L1 ready data"),
                        payload,
                    ));
                }
                Action::CloudBound {
                    payload,
                    bridge_generation: None,
                } => {
                    guard.publish(Event::CloudBound(
                        guard.id.clone().expect("L1 ready relay"),
                        payload,
                    ));
                }
                _ => break 'connection Disconnect::Mqtt, // No bridge input/queue is exposed by this local-only service.
            }
        }
    };
    if !clean_disconnect
        && !matches!(reason, Disconnect::Closed)
        && let Some(message) = will
    {
        let state = broker.shared.lock();
        if !state.stopped
            && guard.id.as_ref().is_none_or(|id| {
                state
                    .entries
                    .get(&id.device)
                    .is_some_and(|entry| entry.id == *id)
            })
        {
            let _ = broker.shared.events.send(Event::Will {
                generation: guard.generation,
                session: guard.id.clone(),
                message,
            });
        }
    }
    guard.reason = reason;
    drop(reader);
    drop(writer);
    drop(stream);
    drop(guard);
    outbound.close();
    while let Some(command) = outbound.recv().await {
        let _ = command.result.send(Delivery::Failed);
    }
}

fn mqtt_idle_timeout(keep_alive: u16, configured: Duration) -> Duration {
    let floor = configured.max(Duration::from_secs(300));
    floor.max(Duration::from_millis(u64::from(keep_alive) * 1500))
}

#[cfg(test)]
mod idle_tests {
    use super::*;
    #[test]
    fn appliance_grace_never_shortens_configured_or_declared_interval() {
        for keep_alive in [0, 1, 30, 200] {
            assert_eq!(
                mqtt_idle_timeout(keep_alive, Duration::from_secs(90)),
                Duration::from_secs(300)
            );
        }
        assert_eq!(
            mqtt_idle_timeout(600, Duration::from_secs(90)),
            Duration::from_secs(900)
        );
        assert_eq!(
            mqtt_idle_timeout(0, Duration::from_secs(600)),
            Duration::from_secs(600)
        );
    }
}
