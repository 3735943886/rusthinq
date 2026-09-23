//! Minimal MQTT 3.1.1 broker for ThinQ device connections (port of cloud/mqtt-broker.ts).

use bytes::{Buf, BufMut, BytesMut};
use rusthinq_util::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};

/// Idle timeout used until a client's CONNECT packet has been seen (so there is no
/// declared keep-alive yet to honor), and as the floor/fallback once one has -- see
/// `idle_timeout_for`. A client that sends nothing at all for this long is dropped.
const CLIENT_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// How long to wait after last activity before dropping a client, given the keep-alive
/// (seconds) it declared in its CONNECT packet.
///
/// This used to be a flat `CLIENT_IDLE_TIMEOUT` for every client regardless of what it
/// asked for -- the CONNECT packet's keep-alive field was parsed and discarded. That is
/// exactly backwards for MQTT: the keep-alive is the client telling the broker how
/// often to expect a PINGREQ if there's nothing else to send, and the broker is
/// supposed to allow "1.5 times the keep alive time period" (MQTT 3.1.1 §3.1.2.10)
/// before treating it as dead. A device whose own keep-alive is longer than 5 minutes
/// (real-world appliances have been observed well past that, e.g. an idle cooktop with
/// nothing to report) was getting dropped by this broker mid-keep-alive, forcing a full
/// reconnect/re-provisioning cycle it never needed against LG's real cloud.
///
/// `keep_alive == 0` means the client explicitly opted out of keep-alive enforcement
/// (MQTT 3.1.1 §3.1.2.10) -- this broker still reaps a connection whose TCP session died
/// without a clean close, rather than holding it forever, by falling back to the flat
/// default in that case.
fn idle_timeout_for(keep_alive: u16) -> std::time::Duration {
    if keep_alive == 0 {
        return CLIENT_IDLE_TIMEOUT;
    }
    std::time::Duration::from_secs(keep_alive as u64)
        .mul_f64(1.5)
        .max(CLIENT_IDLE_TIMEOUT)
}

#[derive(Debug, Clone)]
pub struct PublishPacket {
    pub topic: String,
    pub payload: Vec<u8>,
    pub retain: bool,
    pub qos: u8,
    pub dup: bool,
}

impl PublishPacket {
    /// QoS 0, not retained, not a dup redelivery -- the common case for a fresh publish.
    pub fn new(topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            retain: false,
            qos: 0,
            dup: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
}

struct Subscription {
    pattern: String,
    re_parts: Vec<String>,
}

impl Subscription {
    fn new(topic_pattern: &str) -> Self {
        // Convert MQTT filter to simple matcher: # at end → prefix, + → one level
        let re = format!(
            "^{}$",
            topic_pattern
                .replace('#', "\x00HASH\x00")
                .replace('+', "\x00PLUS\x00")
        );
        // Store original; match manually
        let _ = re;
        Self {
            pattern: topic_pattern.to_string(),
            re_parts: topic_pattern.split('/').map(|s| s.to_string()).collect(),
        }
    }

    fn match_topic(&self, topic: &str) -> bool {
        // Port of TS: '^' + pattern.replace(/#$/, '.*').replace(/\+/g, '[^/]*') + '$'
        // Approximate with split logic for correctness on common cases.
        let pat = &self.pattern;
        if pat.ends_with('#') {
            let prefix = pat.trim_end_matches('#').trim_end_matches('/');
            if prefix.is_empty() {
                return true;
            }
            return topic == prefix || topic.starts_with(&format!("{prefix}/"));
        }
        let tparts: Vec<&str> = topic.split('/').collect();
        if tparts.len() != self.re_parts.len() {
            return false;
        }
        for (p, t) in self.re_parts.iter().zip(tparts.iter()) {
            if p == "+" {
                continue;
            }
            if p != t {
                return false;
            }
        }
        true
    }
}

type ClientId = u64;

struct ClientInner {
    subscriptions: HashMap<String, Subscription>,
    will: Option<Will>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

pub type PublishHandler = Arc<dyn Fn(PublishPacket, Option<ClientId>) + Send + Sync>;
pub type DisconnectHandler = Arc<dyn Fn(ClientId) + Send + Sync>;
pub type OutgoingHandler = Arc<dyn Fn(&PublishPacket) + Send + Sync>;

struct BrokerState {
    clients: HashMap<ClientId, ClientInner>,
    retain_map: HashMap<String, PublishPacket>,
    next_id: ClientId,
    on_publish: Option<PublishHandler>,
    on_disconnect: Option<DisconnectHandler>,
    /// Fires for every server-originated publish (`publish(.., None)`) — e.g. a CLIP
    /// response to `lime/devices/<id>`. Separate from `on_publish` (which only fires for
    /// client→broker traffic and is already claimed by `DeviceAcceptor`) so a second
    /// observer — the device simulator, see `sim_device.rs` — can tap replies without
    /// needing a real client subscription.
    on_outgoing: Option<OutgoingHandler>,
    /// Extra data for ThinQ2 acceptor: client_id → deploy JSON / device flag
    client_meta: HashMap<ClientId, ClientMeta>,
}

#[derive(Default, Clone)]
pub struct ClientMeta {
    pub deploy_msg: Option<serde_json::Value>,
    pub has_device: bool,
    pub device_id: Option<String>,
}

#[derive(Clone)]
pub struct Broker {
    state: Arc<Mutex<BrokerState>>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(BrokerState {
                clients: HashMap::new(),
                retain_map: HashMap::new(),
                next_id: 1,
                on_publish: None,
                on_disconnect: None,
                on_outgoing: None,
                client_meta: HashMap::new(),
            })),
        }
    }

    pub fn on_publish(&self, h: PublishHandler) {
        self.state.lock().on_publish = Some(h);
    }

    pub fn on_disconnect(&self, h: DisconnectHandler) {
        self.state.lock().on_disconnect = Some(h);
    }

    pub fn on_outgoing(&self, h: OutgoingHandler) {
        self.state.lock().on_outgoing = Some(h);
    }

    pub fn client_meta(&self, id: ClientId) -> ClientMeta {
        self.state
            .lock()
            .client_meta
            .get(&id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn set_client_meta(&self, id: ClientId, meta: ClientMeta) {
        self.state.lock().client_meta.insert(id, meta);
    }

    pub fn destroy_client(&self, id: ClientId) {
        let mut st = self.state.lock();
        if let Some(c) = st.clients.remove(&id) {
            let _ = c.tx.send(Vec::new()); // empty = close signal
        }
        st.client_meta.remove(&id);
    }

    pub fn publish(&self, packet: PublishPacket, from: Option<ClientId>) {
        let (handler, outgoing, clients) = {
            let mut st = self.state.lock();
            if packet.retain {
                if packet.payload.is_empty() {
                    st.retain_map.remove(&packet.topic);
                } else {
                    st.retain_map.insert(packet.topic.clone(), packet.clone());
                }
            }
            let handler = st.on_publish.clone();
            let outgoing = if from.is_none() {
                st.on_outgoing.clone()
            } else {
                None
            };
            let mut targets = Vec::new();
            for c in st.clients.values() {
                for sub in c.subscriptions.values() {
                    if sub.match_topic(&packet.topic) {
                        targets.push(c.tx.clone());
                        break;
                    }
                }
            }
            (handler, outgoing, targets)
        };
        if let Some(h) = handler {
            h(packet.clone(), from);
        }
        if let Some(h) = outgoing {
            h(&packet);
        }
        let wire = encode_publish(&packet);
        for tx in clients {
            let _ = tx.send(wire.clone());
        }
    }

    /// Accept a plain (no TLS) TCP stream as an MQTT client. Nothing in production
    /// binds a listener to this anymore (see main.rs's removal of the old 1884 debug
    /// port) — real devices always arrive via `accept_tls`. Kept for tests, which
    /// drive the broker over a real loopback socket instead of hand-building wire
    /// bytes; see this module's own `#[cfg(test)] mod tests`.
    #[allow(dead_code)]
    pub async fn accept_tcp(self: &Arc<Self>, stream: TcpStream) {
        self.handle_connection(stream).await;
    }

    /// Accept any TLS-wrapped stream (rustls or OpenSSL) as an MQTT client.
    pub async fn accept_tls<S>(self: &Arc<Self>, stream: S)
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
    {
        self.handle_connection(stream).await;
    }

    pub async fn handle_connection<S>(self: &Arc<Self>, stream: S)
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
    {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let client_id = {
            let mut st = self.state.lock();
            let id = st.next_id;
            st.next_id += 1;
            st.clients.insert(
                id,
                ClientInner {
                    subscriptions: HashMap::new(),
                    will: None,
                    tx: tx.clone(),
                },
            );
            st.client_meta.insert(id, ClientMeta::default());
            id
        };

        // A write reaching this client (e.g. another client's publish relayed to it,
        // or a bridged cloud command) is activity too, same as MQTT-spec-compliant
        // brokers treat it -- a client that is otherwise silent because it's simply
        // waiting on a server push must not have that push racing its own idle
        // timeout. `write_task` runs independently of the read loop below (it also
        // drains publishes queued by *other* connections' dispatch), so it signals
        // activity back over `activity` rather than touching the idle timer directly.
        let activity = Arc::new(Notify::new());
        let activity_writer = activity.clone();
        let write_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if msg.is_empty() {
                    break;
                }
                if writer.write_all(&msg).await.is_err() {
                    break;
                }
                let _ = writer.flush().await;
                activity_writer.notify_one();
            }
            // Graceful close (TLS close_notify, then FIN) instead of just dropping the
            // socket -- an abrupt/unclean close leaves an otherwise-idle client with no
            // way to notice it was disconnected until its own next read/write attempt
            // or keep-alive check, which can be a long time away.
            let _ = writer.shutdown().await;
        });

        let mut buf = BytesMut::with_capacity(4096);
        let mut tmp = [0u8; 4096];
        // Placeholder until CONNECT is seen and tells us the client's actual keep-alive
        // -- see `idle_timeout_for`.
        let mut idle_timeout = CLIENT_IDLE_TIMEOUT;
        let idle = tokio::time::sleep(idle_timeout);
        tokio::pin!(idle);

        // Distinguishes, in the logs, a connection this broker gave up on (idle-timeout)
        // from one the client itself ended -- needed to tell apart "we dropped a
        // legitimately-quiet device" from "the device's own TCP session died on the
        // wire", which look identical from here on out (both end in the same cleanup
        // below) but have very different causes upstream.
        let mut connect_seen = false;
        loop {
            tokio::select! {
                () = &mut idle => {
                    tracing::warn!(
                        client_id,
                        connect_seen,
                        idle_timeout_secs = idle_timeout.as_secs(),
                        "mqtt client idle-timeout: no activity for the configured window"
                    );
                    break;
                }
                _ = activity.notified() => {
                    idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
                }
                n = reader.read(&mut tmp) => {
                    match n {
                        Ok(0) => {
                            tracing::info!(client_id, connect_seen, "mqtt client closed (EOF)");
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(client_id, connect_seen, error = %e, "mqtt client closed (read error)");
                            break;
                        }
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            while let Some(packet) = try_decode_packet(&mut buf) {
                                if let MqttPacket::Connect { keep_alive, .. } = &packet {
                                    idle_timeout = idle_timeout_for(*keep_alive);
                                    connect_seen = true;
                                    tracing::info!(
                                        client_id,
                                        keep_alive,
                                        idle_timeout_secs = idle_timeout.as_secs(),
                                        "mqtt client CONNECT"
                                    );
                                }
                                if !self.dispatch(client_id, packet, &tx).await {
                                    break;
                                }
                            }
                            idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
                        }
                    }
                }
            }
        }

        // LWT + cleanup
        let (will, disc) = {
            let mut st = self.state.lock();
            let will = st.clients.get(&client_id).and_then(|c| c.will.clone());
            st.clients.remove(&client_id);
            let disc = st.on_disconnect.clone();
            st.client_meta.remove(&client_id);
            (will, disc)
        };
        if let Some(w) = will {
            self.publish(PublishPacket::new(w.topic, w.payload), Some(client_id));
        }
        if let Some(h) = disc {
            h(client_id);
        }
        let _ = tx.send(Vec::new());
        let _ = write_task.await;
    }

    async fn dispatch(
        &self,
        client_id: ClientId,
        packet: MqttPacket,
        tx: &mpsc::UnboundedSender<Vec<u8>>,
    ) -> bool {
        match packet {
            MqttPacket::Connect { will, .. } => {
                {
                    let mut st = self.state.lock();
                    if let Some(c) = st.clients.get_mut(&client_id) {
                        c.will = will;
                    }
                }
                let _ = tx.send(vec![0x20, 0x02, 0x00, 0x00]); // CONNACK success
                true
            }
            MqttPacket::Publish(p) => {
                if p.qos > 0 {
                    // PUBACK
                    let mut pkt = vec![0x40, 0x02];
                    pkt.put_u16(p.message_id.unwrap_or(0));
                    let _ = tx.send(pkt);
                }
                self.publish(
                    PublishPacket {
                        topic: p.topic,
                        payload: p.payload,
                        retain: p.retain,
                        qos: p.qos,
                        dup: p.dup,
                    },
                    Some(client_id),
                );
                true
            }
            MqttPacket::Subscribe { message_id, topics } => {
                let mut granted = Vec::new();
                let new_subs: Vec<Subscription> = topics
                    .iter()
                    .map(|t| {
                        granted.push(0u8); // QoS 0
                        Subscription::new(t)
                    })
                    .collect();

                let retained_to_send = {
                    let mut st = self.state.lock();
                    let retain_map = st.retain_map.clone();
                    let client = st.clients.get_mut(&client_id);
                    let mut unseen: HashSet<String> = retain_map.keys().cloned().collect();
                    if let Some(c) = client.as_ref() {
                        for t in retain_map.keys() {
                            for s in c.subscriptions.values() {
                                if s.match_topic(t) {
                                    unseen.remove(t);
                                    break;
                                }
                            }
                        }
                    }
                    if let Some(c) = st.clients.get_mut(&client_id) {
                        for (i, t) in topics.iter().enumerate() {
                            c.subscriptions.insert(t.clone(), new_subs[i].clone_sub());
                        }
                    }
                    let mut out = Vec::new();
                    for t in unseen {
                        for s in &new_subs {
                            if s.match_topic(&t) {
                                if let Some(p) = retain_map.get(&t) {
                                    out.push(p.clone());
                                }
                                break;
                            }
                        }
                    }
                    out
                };

                let mut suback = vec![0x90]; // SUBACK
                let mut body = BytesMut::new();
                body.put_u16(message_id);
                body.extend_from_slice(&granted);
                encode_remaining_length(&mut suback, body.len());
                suback.extend_from_slice(&body);
                let _ = tx.send(suback);

                for p in retained_to_send {
                    let _ = tx.send(encode_publish(&p));
                }
                true
            }
            MqttPacket::Unsubscribe { message_id, topics } => {
                {
                    let mut st = self.state.lock();
                    if let Some(c) = st.clients.get_mut(&client_id) {
                        for t in topics {
                            c.subscriptions.remove(&t);
                        }
                    }
                }
                let mut unsuback = vec![0xb0, 0x02];
                unsuback.put_u16(message_id);
                let _ = tx.send(unsuback);
                true
            }
            MqttPacket::PingReq => {
                let _ = tx.send(vec![0xd0, 0x00]);
                true
            }
            MqttPacket::Disconnect => {
                // A clean DISCONNECT must suppress the Will (MQTT 3.1.1 §3.14) — only an
                // unclean loss of connection (read EOF/error) should publish it. Clearing it
                // here reuses the same post-loop cleanup path for both cases instead of
                // duplicating it.
                if let Some(c) = self.state.lock().clients.get_mut(&client_id) {
                    c.will = None;
                }
                false
            }
            MqttPacket::Unknown => true,
        }
    }
}

impl Subscription {
    fn clone_sub(&self) -> Self {
        Self {
            pattern: self.pattern.clone(),
            re_parts: self.re_parts.clone(),
        }
    }
}

enum MqttPacket {
    Connect {
        will: Option<Will>,
        keep_alive: u16,
    },
    Publish(PublishIn),
    Subscribe {
        message_id: u16,
        topics: Vec<String>,
    },
    Unsubscribe {
        message_id: u16,
        topics: Vec<String>,
    },
    PingReq,
    Disconnect,
    Unknown,
}

struct PublishIn {
    topic: String,
    payload: Vec<u8>,
    retain: bool,
    qos: u8,
    dup: bool,
    message_id: Option<u16>,
}

fn try_decode_packet(buf: &mut BytesMut) -> Option<MqttPacket> {
    if buf.len() < 2 {
        return None;
    }
    let first = buf[0];
    let (rem_len, rl_bytes) = decode_remaining_length(&buf[1..])?;
    let header_len = 1 + rl_bytes;
    if buf.len() < header_len + rem_len {
        return None;
    }
    let _ = buf.split_to(header_len);
    let mut body = buf.split_to(rem_len);
    let packet_type = first >> 4;
    let flags = first & 0x0f;

    Some(match packet_type {
        1 => decode_connect(&mut body),
        3 => {
            let dup = flags & 0x08 != 0;
            let qos = (flags >> 1) & 0x03;
            let retain = flags & 0x01 != 0;
            if body.remaining() < 2 {
                return Some(MqttPacket::Unknown);
            }
            let tlen = body.get_u16() as usize;
            if body.remaining() < tlen {
                return Some(MqttPacket::Unknown);
            }
            let topic = String::from_utf8_lossy(&body.copy_to_bytes(tlen)).into_owned();
            let message_id = if qos > 0 {
                if body.remaining() < 2 {
                    return Some(MqttPacket::Unknown);
                }
                Some(body.get_u16())
            } else {
                None
            };
            let payload = body.to_vec();
            MqttPacket::Publish(PublishIn {
                topic,
                payload,
                retain,
                qos,
                dup,
                message_id,
            })
        }
        8 => {
            if body.remaining() < 2 {
                return Some(MqttPacket::Unknown);
            }
            let message_id = body.get_u16();
            let mut topics = Vec::new();
            while body.remaining() >= 2 {
                let tlen = body.get_u16() as usize;
                if body.remaining() < tlen + 1 {
                    break;
                }
                let topic = String::from_utf8_lossy(&body.copy_to_bytes(tlen)).into_owned();
                let _qos = body.get_u8();
                topics.push(topic);
            }
            MqttPacket::Subscribe { message_id, topics }
        }
        10 => {
            if body.remaining() < 2 {
                return Some(MqttPacket::Unknown);
            }
            let message_id = body.get_u16();
            let mut topics = Vec::new();
            while body.remaining() >= 2 {
                let tlen = body.get_u16() as usize;
                if body.remaining() < tlen {
                    break;
                }
                topics.push(String::from_utf8_lossy(&body.copy_to_bytes(tlen)).into_owned());
            }
            MqttPacket::Unsubscribe { message_id, topics }
        }
        12 => MqttPacket::PingReq,
        14 => MqttPacket::Disconnect,
        _ => MqttPacket::Unknown,
    })
}

fn decode_connect(body: &mut BytesMut) -> MqttPacket {
    // protocol name
    if body.remaining() < 2 {
        return MqttPacket::Unknown;
    }
    let nlen = body.get_u16() as usize;
    if body.remaining() < nlen + 4 {
        return MqttPacket::Unknown;
    }
    let _ = body.copy_to_bytes(nlen); // MQTT
    let _proto_level = body.get_u8();
    let connect_flags = body.get_u8();
    let keep_alive = body.get_u16();
    // client id
    if body.remaining() < 2 {
        return MqttPacket::Unknown;
    }
    let cid_len = body.get_u16() as usize;
    if body.remaining() < cid_len {
        return MqttPacket::Unknown;
    }
    let _ = body.copy_to_bytes(cid_len); // client id (unused: no consumer needs it)

    let will = if connect_flags & 0x04 != 0 {
        // will topic + payload
        if body.remaining() < 2 {
            return MqttPacket::Connect {
                will: None,
                keep_alive,
            };
        }
        let wt_len = body.get_u16() as usize;
        if body.remaining() < wt_len + 2 {
            return MqttPacket::Connect {
                will: None,
                keep_alive,
            };
        }
        let topic = String::from_utf8_lossy(&body.copy_to_bytes(wt_len)).into_owned();
        let wp_len = body.get_u16() as usize;
        if body.remaining() < wp_len {
            return MqttPacket::Connect {
                will: None,
                keep_alive,
            };
        }
        let payload = body.copy_to_bytes(wp_len).to_vec();
        Some(Will { topic, payload })
    } else {
        None
    };

    // skip username/password if present
    let _ = connect_flags;

    MqttPacket::Connect { will, keep_alive }
}

fn decode_remaining_length(data: &[u8]) -> Option<(usize, usize)> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    for (i, &b) in data.iter().enumerate() {
        value += (b as usize & 127) * multiplier;
        multiplier *= 128;
        if b & 128 == 0 {
            return Some((value, i + 1));
        }
        if i >= 3 {
            return None;
        }
    }
    None
}

fn encode_remaining_length(out: &mut Vec<u8>, mut len: usize) {
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 128;
        }
        out.push(b);
        if len == 0 {
            break;
        }
    }
}

fn encode_publish(p: &PublishPacket) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u16(p.topic.len() as u16);
    body.extend_from_slice(p.topic.as_bytes());
    // qos 0 — no message id
    body.extend_from_slice(&p.payload);
    let mut flags = 0u8;
    if p.retain {
        flags |= 0x01;
    }
    if p.dup {
        flags |= 0x08;
    }
    flags |= (p.qos & 0x03) << 1;
    let mut out = vec![0x30 | flags];
    encode_remaining_length(&mut out, body.len());
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, PublishOptions, QoS};
    use tokio::net::TcpListener;

    /// Real `rumqttc` clients over a real loopback socket — no wire protocol is
    /// hand-built here; the broker's own decode/encode is what gets exercised, same as
    /// a physical appliance would drive it. `accept_tcp` is otherwise unreachable in
    /// production (no listener binds it — see main.rs), so this is its only coverage.
    async fn listening_broker() -> (Arc<Broker>, u16) {
        let broker = Arc::new(Broker::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let b = broker.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let b = b.clone();
                tokio::spawn(async move { b.accept_tcp(stream).await });
            }
        });
        (broker, port)
    }

    async fn connect(
        port: u16,
        client_id: &str,
    ) -> (AsyncClient, mpsc::UnboundedReceiver<(String, Vec<u8>)>) {
        let mut opts = MqttOptions::new(client_id, ("127.0.0.1", port));
        opts.set_keep_alive(30);
        let (client, mut eventloop) = AsyncClient::builder(opts).capacity(32).build();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let topic = String::from_utf8_lossy(&p.topic).to_string();
                        let _ = tx.send((topic, p.payload.to_vec()));
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        (client, rx)
    }

    async fn wait_for<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed")
    }

    #[tokio::test]
    async fn publish_reaches_a_subscriber_of_the_exact_topic() {
        let (_broker, port) = listening_broker().await;
        let (sub, mut rx) = connect(port, "sub").await;
        sub.subscribe("device/1/state", QoS::AtMostOnce)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (pub_client, _) = connect(port, "pub").await;
        pub_client
            .publish(
                "device/1/state",
                b"on".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();

        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/state");
        assert_eq!(payload, b"on");
    }

    #[tokio::test]
    async fn hash_wildcard_matches_the_whole_subtree_but_not_a_sibling() {
        let (_broker, port) = listening_broker().await;
        let (sub, mut rx) = connect(port, "sub").await;
        sub.subscribe("device/1/#", QoS::AtMostOnce).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (pub_client, _) = connect(port, "pub").await;
        pub_client
            .publish(
                "device/1/x/y",
                b"a".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();
        pub_client
            .publish("device/2/x", b"b".to_vec(), PublishOptions::at_most_once())
            .await
            .unwrap();
        pub_client
            .publish(
                "device/1/x/y",
                b"c".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();

        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/x/y");
        assert_eq!(payload, b"a");
        // the device/2 publish must never arrive — confirm the *next* one is the second
        // device/1 publish, not the sibling.
        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/x/y");
        assert_eq!(payload, b"c");
    }

    #[tokio::test]
    async fn plus_wildcard_matches_exactly_one_level() {
        let (_broker, port) = listening_broker().await;
        let (sub, mut rx) = connect(port, "sub").await;
        sub.subscribe("device/+/state", QoS::AtMostOnce)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (pub_client, _) = connect(port, "pub").await;
        pub_client
            .publish(
                "device/1/state/extra",
                b"nope".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();
        pub_client
            .publish(
                "device/1/state",
                b"yes".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();

        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/state");
        assert_eq!(payload, b"yes");
    }

    #[tokio::test]
    async fn a_retained_message_reaches_a_subscriber_that_joins_later() {
        let (_broker, port) = listening_broker().await;
        let (pub_client, _) = connect(port, "pub").await;
        pub_client
            .publish(
                "device/1/config",
                b"cfg".to_vec(),
                PublishOptions::at_most_once().retained(),
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (late_sub, mut rx) = connect(port, "late").await;
        late_sub
            .subscribe("device/1/config", QoS::AtMostOnce)
            .await
            .unwrap();

        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/config");
        assert_eq!(payload, b"cfg");
    }

    #[tokio::test]
    async fn a_retained_message_with_an_empty_payload_clears_it() {
        let (_broker, port) = listening_broker().await;
        let (pub_client, _) = connect(port, "pub").await;
        pub_client
            .publish(
                "device/1/config",
                b"cfg".to_vec(),
                PublishOptions::at_most_once().retained(),
            )
            .await
            .unwrap();
        pub_client
            .publish(
                "device/1/config",
                Vec::new(),
                PublishOptions::at_most_once().retained(),
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (late_sub, mut rx) = connect(port, "late").await;
        late_sub
            .subscribe("device/1/other", QoS::AtMostOnce)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // Nothing retained under device/1/config anymore, so publishing something new
        // under a sibling topic must be the first (and only) thing this subscriber sees.
        pub_client
            .publish(
                "device/1/other",
                b"only-this".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();
        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/other");
        assert_eq!(payload, b"only-this");
    }

    #[tokio::test]
    async fn will_is_published_on_an_unclean_disconnect() {
        let (broker, port) = listening_broker().await;
        let (sub, mut rx) = connect(port, "sub").await;
        sub.subscribe("device/1/lwt", QoS::AtMostOnce)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut opts = MqttOptions::new("will-client", ("127.0.0.1", port));
        opts.set_last_will(rumqttc::LastWill::new(
            "device/1/lwt",
            b"offline".to_vec(),
            QoS::AtMostOnce,
            false,
        ));
        let (client, mut eventloop) = AsyncClient::builder(opts).capacity(32).build();
        let poll_task = tokio::spawn(async move {
            loop {
                if eventloop.poll().await.is_err() {
                    break;
                }
            }
        });
        // Give the CONNECT a moment to land, then kill the connection without a clean
        // DISCONNECT — that's what should fire the will. Aborting the task that owns
        // `eventloop` drops the TCP socket at a known instant; merely dropping `client`
        // (the request-sender handle) isn't reliable here — rumqttc's eventloop, still
        // being polled, may notice the channel close and send a clean DISCONNECT of its
        // own before the socket actually goes away, which is exactly the case the
        // sibling test below covers and must NOT fire the will.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        poll_task.abort();
        drop(client);

        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/lwt");
        assert_eq!(payload, b"offline");
        let _ = broker;
    }

    #[tokio::test]
    async fn a_clean_disconnect_does_not_publish_the_will() {
        let (_broker, port) = listening_broker().await;
        let (sub, mut rx) = connect(port, "sub").await;
        sub.subscribe("device/1/lwt", QoS::AtMostOnce)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut opts = MqttOptions::new("will-client-2", ("127.0.0.1", port));
        opts.set_last_will(rumqttc::LastWill::new(
            "device/1/lwt",
            b"offline".to_vec(),
            QoS::AtMostOnce,
            false,
        ));
        let (client, mut eventloop) = AsyncClient::builder(opts).capacity(32).build();
        tokio::spawn(async move {
            loop {
                if eventloop.poll().await.is_err() {
                    break;
                }
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        client.disconnect().await.unwrap();

        // Prove the will did NOT fire by publishing something else and confirming that's
        // what the subscriber sees first (rather than racing an absence, which a slow CI
        // box could flake on).
        let (pub_client, _) = connect(port, "pub").await;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        pub_client
            .publish(
                "device/1/lwt",
                b"canary".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();
        let (topic, payload) = wait_for(&mut rx).await;
        assert_eq!(topic, "device/1/lwt");
        assert_eq!(payload, b"canary");
    }

    #[tokio::test]
    async fn on_outgoing_fires_for_server_originated_publishes_only() {
        let (broker, port) = listening_broker().await;
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        broker.on_outgoing(Arc::new(move |p: &PublishPacket| {
            seen2.lock().push(p.topic.clone());
        }));

        let (client, _rx) = connect(port, "c").await;
        client
            .subscribe("lime/devices/x", QoS::AtMostOnce)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // A client->broker publish must NOT fire on_outgoing.
        client
            .publish(
                "client/topic",
                b"a".to_vec(),
                PublishOptions::at_most_once(),
            )
            .await
            .unwrap();
        // A server-originated publish (from=None, e.g. a CLIP response) must.
        broker.publish(
            PublishPacket::new("lime/devices/x", b"reply".to_vec()),
            None,
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let seen = seen.lock().clone();
        assert_eq!(seen, vec!["lime/devices/x".to_string()]);
    }

    /// Root cause of a real intermittent-disconnect report: a device with a
    /// keep-alive longer than the old flat `CLIENT_IDLE_TIMEOUT` (5 minutes) was
    /// being force-dropped by this broker well within its own legitimate keep-alive
    /// budget, even though it was otherwise perfectly healthy (pings fine, no error
    /// on the wire) -- something LG's real cloud never did to it, since AWS IoT
    /// honors the client's declared keep-alive instead of enforcing a flat cutoff.
    #[test]
    fn idle_timeout_honors_a_keep_alive_longer_than_the_old_flat_default() {
        // MQTT 3.1.1 §3.1.2.10: broker should allow 1.5x the declared keep-alive.
        assert_eq!(
            idle_timeout_for(600),
            std::time::Duration::from_secs(900),
            "a 10 minute keep-alive must survive past the old flat 5 minute cutoff"
        );
    }

    #[test]
    fn idle_timeout_floors_at_the_flat_default_for_a_short_keep_alive() {
        // A client asking for a very short keep-alive (so 1.5x it would be well
        // under the old flat default) must not be dropped more aggressively than
        // before -- this preserves prior behavior as a floor, not a ceiling.
        assert_eq!(idle_timeout_for(10), CLIENT_IDLE_TIMEOUT);
    }

    #[test]
    fn idle_timeout_falls_back_to_the_flat_default_when_keep_alive_is_disabled() {
        // keep_alive == 0 means the client opted out of keep-alive enforcement
        // entirely (MQTT 3.1.1 §3.1.2.10) -- this broker still reaps a connection
        // whose TCP session died without a clean close, rather than holding it
        // forever, by falling back to the flat default in that case.
        assert_eq!(idle_timeout_for(0), CLIENT_IDLE_TIMEOUT);
    }
}
