//! Per-MQTT-connection CLIP interpretation. The broker, provisioning response
//! service, time source, TLS, and device replacement belong to the runtime.
use crate::{
    aabb,
    lg_compat::{self, AckOwner},
};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    PayloadExceeded,
    InvalidJson,
    InvalidEnvelope,
    InvalidTopic,
    DeviceIdChanged,
    NotProvisioned,
    BridgeDisabled,
    InvalidHex,
    TimeWentBackwards,
    Closed,
    ProvisionFailed,
    CounterExhausted,
    Busy,
    StaleBridge,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ThinQ2 protocol error: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// L3's provisioning service constructs the actual response.
    Provision {
        operation: u64,
        request: Value,
    },
    Ready {
        device_id: String,
        deploy: Value,
    },
    Send {
        topic: String,
        payload: Vec<u8>,
        /// Some for cloud-origin writes; the runtime must fence stale generations.
        bridge_generation: Option<u64>,
    },
    Data(Vec<u8>),
    /// Unknown fields/messages remain opaque and can be relayed unchanged.
    CloudBound {
        payload: Vec<u8>,
        bridge_generation: Option<u64>,
    },
    BridgeChanged {
        generation: u64,
        active: bool,
    },
    TimeSyncRequested,
}

#[derive(Debug, PartialEq)]
pub struct Outcome {
    pub actions: Vec<Action>,
    pub error: Option<Error>,
    /// Device protocol errors are terminal; rejected cloud input is not.
    pub closed: bool,
    /// MQTT keepalive is owned by the broker, not a second protocol timer.
    pub next_deadline: Option<Duration>,
}

pub enum Input<'a> {
    BridgeState {
        generation: u64,
        active: bool,
    },
    ProvisionResult {
        operation: u64,
        sent: bool,
    },
    Device {
        topic: &'a str,
        payload: &'a [u8],
        outbound_mid: u64,
    },
    Cloud {
        generation: u64,
        payload: &'a [u8],
    },
    End,
}

pub struct Session {
    max_payload: usize,
    bridge_active: bool,
    bridge_generation: u64,
    last_now: Duration,
    device_id: Option<String>,
    deploy: Option<Value>,
    ready: bool,
    provision_operation: u64,
    provision_sent: bool,
    completion_received: bool,
    closed: bool,
}

impl Session {
    /// Initial relay generation is 1. Runtime bridge transitions carry strictly
    /// increasing generations, fencing delayed cloud inputs and queued writes.
    pub fn new(max_payload: usize, bridge_active: bool, now: Duration) -> Self {
        Self {
            max_payload,
            bridge_active,
            bridge_generation: 1,
            last_now: now,
            device_id: None,
            deploy: None,
            ready: false,
            provision_operation: 0,
            provision_sent: false,
            completion_received: false,
            closed: false,
        }
    }

    pub fn bridge_generation(&self) -> u64 {
        self.bridge_generation
    }

    /// Check immediately before starting a cloud-origin write or upstream relay.
    /// An already partially written frame cannot be replayed after a transition.
    pub fn accepts_bridge_generation(&self, generation: u64) -> bool {
        !self.closed && self.bridge_active && self.bridge_generation == generation
    }

    pub fn device_id(&self) -> Option<&str> {
        self.device_id.as_deref()
    }

    pub fn input(&mut self, input: Input<'_>, now: Duration) -> Outcome {
        let cloud_input = matches!(input, Input::Cloud { .. });
        let result = if self.closed {
            Err(Error::Closed)
        } else if now < self.last_now {
            Err(Error::TimeWentBackwards)
        } else {
            self.last_now = now;
            match input {
                Input::BridgeState { generation, active } => {
                    if generation <= self.bridge_generation {
                        Err(Error::StaleBridge)
                    } else {
                        self.bridge_generation = generation;
                        self.bridge_active = active;
                        Ok(vec![Action::BridgeChanged { generation, active }])
                    }
                }
                Input::ProvisionResult { operation, sent } => {
                    if operation != self.provision_operation
                        || self.provision_sent
                        || self.deploy.is_none()
                    {
                        Ok(vec![])
                    } else if !sent {
                        Err(Error::ProvisionFailed)
                    } else {
                        self.provision_sent = true;
                        let mut actions = vec![];
                        if self.completion_received {
                            self.make_ready(&mut actions);
                        }
                        Ok(actions)
                    }
                }
                Input::End => {
                    self.closed = true;
                    Ok(vec![])
                }
                Input::Device {
                    topic,
                    payload,
                    outbound_mid,
                } => self.device(topic, payload, outbound_mid),
                Input::Cloud {
                    generation,
                    payload,
                } => self.cloud(generation, payload),
            }
        };
        match result {
            Ok(actions) => Outcome {
                actions,
                error: None,
                closed: self.closed,
                next_deadline: None,
            },
            Err(error) => {
                if (!cloud_input && !matches!(error, Error::Busy | Error::StaleBridge))
                    || matches!(error, Error::Closed | Error::TimeWentBackwards)
                {
                    self.closed = true;
                    self.deploy = None;
                }
                Outcome {
                    actions: vec![],
                    error: Some(error),
                    closed: self.closed,
                    next_deadline: None,
                }
            }
        }
    }

    fn parse(&self, payload: &[u8]) -> Result<Value, Error> {
        if payload.len() > self.max_payload {
            return Err(Error::PayloadExceeded);
        }
        // LG-003: one trailing NUL occurs on CLIP JSON from device firmware.
        let bytes = payload.strip_suffix(&[0]).unwrap_or(payload);
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::InvalidJson)?;
        let id = value
            .get("did")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(Error::InvalidEnvelope)?;
        if id.contains(['/', '+', '#', '\0']) {
            return Err(Error::InvalidEnvelope);
        }
        value
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|cmd| !cmd.is_empty())
            .ok_or(Error::InvalidEnvelope)?;
        if let Some(current) = &self.device_id
            && current != id
        {
            return Err(Error::DeviceIdChanged);
        }
        Ok(value)
    }

    fn policy(&self) -> lg_compat::Policy {
        lg_compat::select(
            self.deploy
                .as_ref()
                .and_then(|v| v.get("kind"))
                .and_then(Value::as_str),
            self.deploy
                .as_ref()
                .and_then(|v| v.pointer("/data/appInfo/softVer"))
                .and_then(Value::as_str),
            self.bridge_active,
        )
    }

    fn device(&mut self, topic: &str, payload: &[u8], mid: u64) -> Result<Vec<Action>, Error> {
        // Bound topics too; do not allocate an attacker-sized normalized topic.
        if topic.len() > self.max_payload {
            return Err(Error::PayloadExceeded);
        }
        let value = self.parse(payload)?;
        let id = value["did"].as_str().ok_or(Error::InvalidEnvelope)?;
        let cmd = value["cmd"].as_str().ok_or(Error::InvalidEnvelope)?;
        let topic = normalize_clip_topic(topic);
        if topic == format!("clip/provisioning/devices/{id}")
            && matches!(cmd, "deploy" | "preDeploy")
        {
            self.provision_operation = self
                .provision_operation
                .checked_add(1)
                .ok_or(Error::CounterExhausted)?;
            self.provision_sent = false;
            self.completion_received = false;
            self.ready = false;
            self.device_id = Some(id.to_owned());
            self.deploy = Some(value.clone());
            return Ok(vec![Action::Provision {
                operation: self.provision_operation,
                request: value,
            }]);
        }
        if topic != format!("clip/message/devices/{id}") {
            return Err(Error::InvalidTopic);
        }
        if self.deploy.is_none() {
            return Err(Error::NotProvisioned);
        }
        let mut actions = vec![];
        match cmd {
            "completeProvisioning_ack" => {
                self.completion_received = true;
                if self.provision_sent {
                    self.make_ready(&mut actions);
                }
            }
            "device_packet" => {
                if !self.provision_sent {
                    return Err(Error::Busy);
                }
                let data = decode_hex(
                    value
                        .get("data")
                        .and_then(Value::as_str)
                        .ok_or(Error::InvalidHex)?,
                )?;
                if !self.ready {
                    if !self.policy().first_packet_completes_provisioning {
                        return Err(Error::NotProvisioned);
                    }
                    self.make_ready(&mut actions);
                }
                if self.policy().ack_owner == AckOwner::Local
                    && let Some(ack) = aabb::cloud_ack(&data)
                {
                    let message =
                        json!({"did":id,"mid":mid,"cmd":"ack","type":1,"data":encode_hex(&ack)});
                    actions.push(self.send(
                        id,
                        &serde_json::to_vec(&message).map_err(|_| Error::InvalidJson)?,
                    )?);
                }
                actions.push(Action::Data(data));
                actions.push(Action::CloudBound {
                    payload: payload.to_vec(),
                    bridge_generation: self.bridge_active.then_some(self.bridge_generation),
                });
            }
            "req_timesync" => actions.push(Action::TimeSyncRequested),
            _ => {
                if !self.ready {
                    return Err(Error::NotProvisioned);
                }
                actions.push(Action::CloudBound {
                    payload: payload.to_vec(),
                    bridge_generation: self.bridge_active.then_some(self.bridge_generation),
                });
            }
        }
        Ok(actions)
    }

    fn make_ready(&mut self, actions: &mut Vec<Action>) {
        if !self.ready {
            self.ready = true;
            actions.push(Action::Ready {
                device_id: self.device_id.clone().expect("deploy identity"),
                deploy: self.deploy.clone().expect("deploy checked"),
            });
        }
    }

    fn cloud(&self, generation: u64, payload: &[u8]) -> Result<Vec<Action>, Error> {
        if generation != self.bridge_generation {
            return Err(Error::StaleBridge);
        }
        let value = self.parse(payload)?;
        if !self.bridge_active {
            return Err(Error::BridgeDisabled);
        }
        if !self.ready {
            return Err(Error::NotProvisioned);
        }
        let id = value["did"].as_str().ok_or(Error::InvalidEnvelope)?;
        let mut action = self.send(id, payload)?;
        if let Action::Send {
            bridge_generation, ..
        } = &mut action
        {
            *bridge_generation = Some(generation);
        }
        Ok(vec![action])
    }

    fn send(&self, id: &str, payload: &[u8]) -> Result<Action, Error> {
        if payload.len() > self.max_payload {
            return Err(Error::PayloadExceeded);
        }
        Ok(Action::Send {
            topic: format!("lime/devices/{id}"),
            payload: payload.to_vec(),
            bridge_generation: None,
        })
    }
}

/// LG-004: AWS republish paths pivot on the final /clip segment.
pub fn normalize_clip_topic(topic: &str) -> &str {
    if let Some(index) = topic.rfind("/clip/") {
        &topic[index + 1..]
    } else {
        topic
    }
}

pub fn decode_hex(data: &str) -> Result<Vec<u8>, Error> {
    if !data.len().is_multiple_of(2) {
        return Err(Error::InvalidHex);
    }
    data.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).ok_or(Error::InvalidHex)?;
            let low = (pair[1] as char).to_digit(16).ok_or(Error::InvalidHex)?;
            Ok(((high << 4) | low) as u8)
        })
        .collect()
}

pub fn encode_hex(data: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(data.len() * 2);
    for byte in data {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}
