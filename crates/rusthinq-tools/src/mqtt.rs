//! Minimal synchronous MQTT helpers for rusthinq-tools' CLI binaries
//! (rusthinq-mcp, rusthinq-capture): short one-shot or session-length connections
//! to rusthinq-cloud's MQTT bus — not a long-running managed client.
//!
//! rumqttc's synchronous `Client` only actually talks to the network while its
//! paired `Connection` is being iterated, so every helper here pairs a `Client`
//! with a background thread draining its `Connection` ([`spawn_pump`]).

use anyhow::{Result, anyhow};
use rumqttc::{Client, Connection, Event, Incoming, MqttOptions, Publish, QoS};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Parse `"host"` or `"host:port"` into `(host, port)`, defaulting the port to
/// 1883 (plain MQTT — these tools are meant for a local/trusted broker).
pub fn parse_host_port(host_port: &str) -> (String, u16) {
    match host_port
        .rsplit_once(':')
        .and_then(|(h, p)| p.parse::<u16>().ok().map(|p| (h.to_string(), p)))
    {
        Some(hp) => hp,
        None => (host_port.to_string(), 1883),
    }
}

pub fn connect(client_id: &str, host_port: &str) -> (Client, Connection) {
    let (host, port) = parse_host_port(host_port);
    let mut opts = MqttOptions::new(client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(5));
    Client::new(opts, 16)
}

/// Drive `connection` on a background thread, forwarding every incoming
/// `Publish` to `tx` until the connection ends (e.g. after the paired
/// `Client::disconnect()`) or the receiver is dropped.
pub fn spawn_pump(mut connection: Connection, tx: mpsc::Sender<Publish>) {
    std::thread::spawn(move || {
        for notification in connection.iter() {
            match notification {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if tx.send(p).is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
}

/// Subscribe to `topic` and wait up to `timeout` for one message on it exactly
/// (retained snapshots arrive right after the subscribe ack).
pub fn fetch_one(
    client_id: &str,
    host_port: &str,
    topic: &str,
    timeout: Duration,
) -> Result<Option<Vec<u8>>> {
    let (client, connection) = connect(client_id, host_port);
    let (tx, rx) = mpsc::channel();
    spawn_pump(connection, tx);
    client
        .subscribe(topic, QoS::AtLeastOnce)
        .map_err(|e| anyhow!("subscribe {topic}: {e}"))?;

    let deadline = Instant::now() + timeout;
    let result = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break None;
        }
        match rx.recv_timeout(remaining) {
            Ok(p) if p.topic == topic => break Some(p.payload.to_vec()),
            Ok(_) => continue,
            Err(_) => break None,
        }
    };
    let _ = client.disconnect();
    Ok(result)
}

/// Publish `payload` to `topic`, giving the background pump a short window to
/// flush it over the wire before disconnecting.
pub fn publish(client_id: &str, host_port: &str, topic: &str, payload: &[u8]) -> Result<()> {
    let (client, connection) = connect(client_id, host_port);
    let (tx, _rx) = mpsc::channel();
    spawn_pump(connection, tx);
    client
        .publish(topic, QoS::AtLeastOnce, false, payload)
        .map_err(|e| anyhow!("publish {topic}: {e}"))?;
    std::thread::sleep(Duration::from_millis(300));
    let _ = client.disconnect();
    Ok(())
}

/// Subscribe to `topic` (may use MQTT wildcards) and hand every `Publish` on it
/// to the returned receiver for as long as the caller keeps receiving. Returns
/// the still-live `Client` too, so the caller can `disconnect()` it to stop the
/// background pump.
pub fn subscribe_stream(
    client_id: &str,
    host_port: &str,
    topic: &str,
) -> Result<(Client, mpsc::Receiver<Publish>)> {
    let (client, connection) = connect(client_id, host_port);
    let (tx, rx) = mpsc::channel();
    spawn_pump(connection, tx);
    client
        .subscribe(topic, QoS::AtLeastOnce)
        .map_err(|e| anyhow!("subscribe {topic}: {e}"))?;
    Ok((client, rx))
}

/// Subscribe to `topic_filter` (typically a wildcard, e.g. `"rusthinq/+/+"`) and
/// collect every message the broker delivers with its RETAIN flag set, for as
/// long as `window` -- i.e. a one-shot snapshot of whatever's currently retained
/// under that filter, not a live stream. A short `window` after subscribing is
/// enough since retained messages are delivered immediately on subscribe ack;
/// anything arriving with `retain: false` (live, non-retained traffic on an
/// overlapping topic) is filtered out rather than mistaken for retained state.
pub fn collect_retained(
    client_id: &str,
    host_port: &str,
    topic_filter: &str,
    window: Duration,
) -> Result<Vec<Publish>> {
    let (client, connection) = connect(client_id, host_port);
    let (tx, rx) = mpsc::channel();
    spawn_pump(connection, tx);
    client
        .subscribe(topic_filter, QoS::AtLeastOnce)
        .map_err(|e| anyhow!("subscribe {topic_filter}: {e}"))?;

    let deadline = Instant::now() + window;
    let mut out = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match rx.recv_timeout(remaining) {
            Ok(p) if p.retain => out.push(p),
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    let _ = client.disconnect();
    Ok(out)
}

/// Delete each of `topics` from the broker's retained store by publishing an
/// empty retained payload to it -- the standard MQTT way to clear a retained
/// message (mirrors `MqttSink::clear_retained`'s own empty-payload republish).
pub fn clear_retained_topics(client_id: &str, host_port: &str, topics: &[String]) -> Result<()> {
    let (client, connection) = connect(client_id, host_port);
    let (tx, _rx) = mpsc::channel();
    spawn_pump(connection, tx);
    for topic in topics {
        client
            .publish(topic, QoS::AtLeastOnce, true, Vec::<u8>::new())
            .map_err(|e| anyhow!("clear {topic}: {e}"))?;
    }
    std::thread::sleep(Duration::from_millis(300));
    let _ = client.disconnect();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestBroker;

    #[test]
    fn parse_host_port_defaults_and_explicit() {
        assert_eq!(
            parse_host_port("127.0.0.1"),
            ("127.0.0.1".to_string(), 1883)
        );
        assert_eq!(
            parse_host_port("127.0.0.1:1884"),
            ("127.0.0.1".to_string(), 1884)
        );
        assert_eq!(
            parse_host_port("broker.local:8883"),
            ("broker.local".to_string(), 8883)
        );
    }

    #[test]
    fn publish_retained_then_fetch_one_round_trips() {
        let Some(broker) = TestBroker::start() else {
            return;
        };
        let topic = "rusthinq-mqtt-test/retained";

        // Seed a retained message directly (mirrors rusthinq-cloud's retained
        // <prefix>/devices snapshot) rather than via publish(), which always
        // sends non-retained (that's all the real CLI callers need).
        let (client, connection) = connect("seed", &broker.addr());
        let (tx, _rx) = mpsc::channel();
        spawn_pump(connection, tx);
        client
            .publish(topic, QoS::AtLeastOnce, true, b"hello-retained".to_vec())
            .unwrap();
        std::thread::sleep(Duration::from_millis(300));
        let _ = client.disconnect();

        let got = fetch_one("fetcher", &broker.addr(), topic, Duration::from_secs(3)).unwrap();
        assert_eq!(got, Some(b"hello-retained".to_vec()));
    }

    #[test]
    fn fetch_one_times_out_when_nothing_published() {
        let Some(broker) = TestBroker::start() else {
            return;
        };
        let got = fetch_one(
            "fetcher-empty",
            &broker.addr(),
            "rusthinq-mqtt-test/nothing",
            Duration::from_millis(500),
        )
        .unwrap();
        assert_eq!(got, None);
    }

    #[test]
    fn subscribe_stream_receives_live_publishes() {
        let Some(broker) = TestBroker::start() else {
            return;
        };
        let (client, rx) =
            subscribe_stream("streamer", &broker.addr(), "rusthinq-mqtt-test/live/+").unwrap();

        // Give the subscribe ack a moment to land before publishing live (non-retained).
        std::thread::sleep(Duration::from_millis(200));
        publish(
            "publisher",
            &broker.addr(),
            "rusthinq-mqtt-test/live/a",
            b"payload-a",
        )
        .unwrap();

        let got = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(got.topic, "rusthinq-mqtt-test/live/a");
        assert_eq!(got.payload.as_ref(), b"payload-a");
        let _ = client.disconnect();
    }

    fn seed_retained(broker: &TestBroker, topic: &str, payload: &[u8]) {
        let (client, connection) = connect("seeder", &broker.addr());
        let (tx, _rx) = mpsc::channel();
        spawn_pump(connection, tx);
        client
            .publish(topic, QoS::AtLeastOnce, true, payload.to_vec())
            .unwrap();
        std::thread::sleep(Duration::from_millis(300));
        let _ = client.disconnect();
    }

    #[test]
    fn collect_retained_snapshots_matching_topics_and_ignores_others() {
        let Some(broker) = TestBroker::start() else {
            return;
        };
        seed_retained(&broker, "rusthinq-gc-test/dev-1/power", b"on");
        seed_retained(&broker, "rusthinq-gc-test/dev-2/power", b"off");
        seed_retained(&broker, "rusthinq-gc-test/other", b"unrelated");

        let mut got = collect_retained(
            "collector",
            &broker.addr(),
            "rusthinq-gc-test/+/+",
            Duration::from_secs(2),
        )
        .unwrap();
        got.sort_by(|a, b| a.topic.cmp(&b.topic));
        let topics: Vec<&str> = got.iter().map(|p| p.topic.as_str()).collect();
        assert_eq!(
            topics,
            vec![
                "rusthinq-gc-test/dev-1/power",
                "rusthinq-gc-test/dev-2/power"
            ]
        );
        assert!(got.iter().all(|p| p.retain));
    }

    #[test]
    fn collect_retained_ignores_live_non_retained_publishes() {
        let Some(broker) = TestBroker::start() else {
            return;
        };
        let (client, rx) = subscribe_stream(
            "collector-live-setup",
            &broker.addr(),
            "rusthinq-gc-live-test/+/+",
        )
        .unwrap();
        // Just used to confirm the topic filter is live before the real collector
        // subscribes; drop it immediately so it doesn't steal the message below.
        drop(rx);
        let _ = client.disconnect();

        publish(
            "publisher",
            &broker.addr(),
            "rusthinq-gc-live-test/dev-1/power",
            b"on",
        )
        .unwrap();

        let got = collect_retained(
            "collector-live",
            &broker.addr(),
            "rusthinq-gc-live-test/+/+",
            Duration::from_millis(500),
        )
        .unwrap();
        assert!(got.is_empty(), "non-retained publish must not be collected");
    }

    #[test]
    fn clear_retained_topics_removes_them_from_the_broker() {
        let Some(broker) = TestBroker::start() else {
            return;
        };
        seed_retained(&broker, "rusthinq-gc-clear-test/dev-1/power", b"on");

        clear_retained_topics(
            "clearer",
            &broker.addr(),
            &["rusthinq-gc-clear-test/dev-1/power".to_string()],
        )
        .unwrap();

        let got = fetch_one(
            "verify",
            &broker.addr(),
            "rusthinq-gc-clear-test/dev-1/power",
            Duration::from_millis(500),
        )
        .unwrap();
        assert_eq!(got, None, "topic must no longer be retained on the broker");
    }
}
