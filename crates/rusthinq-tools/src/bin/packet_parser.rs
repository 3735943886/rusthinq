//! packet-parser: interpret TLV packets from hex or live MQTT feed.

use anyhow::{Result, bail};
use rusthinq_util::crc16::crc16;
use rusthinq_util::tlv;
use std::env;

fn print_tlv(buf: &[u8], raw: bool) {
    let crc_slice = if raw { buf } else { &buf[2.min(buf.len())..] };
    if crc16(crc_slice) != 0 {
        eprintln!("CRC16 mismatch!");
    }
    let start = if raw { 8 } else { 10 };
    if buf.len() <= start {
        eprintln!("packet too short");
        return;
    }
    let len = buf[start] as usize;
    let end = (start + 1 + len).min(buf.len());
    for el in tlv::parse(&buf[start + 1..end]) {
        println!(
            "t=0x{:x} l={} v=0x{:x} ({})",
            el.t,
            el.l.map(|l| l.to_string()).unwrap_or_else(|| "?".into()),
            el.v,
            el.v
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 && (args[1] == "-message" || args[1] == "-message-raw") {
        let raw = args[1] == "-message-raw";
        let buf = rusthinq_util::hex::decode(
            args[2]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )?;
        print_tlv(&buf, raw);
        return Ok(());
    }

    if args.len() != 3 {
        eprintln!(
            "Usage:
\tpacket-parser mqtt-hostname[:port] device-uuid
\tpacket-parser [-message|-message-raw] HEX-STRING
"
        );
        return Ok(());
    }

    let mqtt_hostname = &args[1];
    let device_id = &args[2];
    let url = if mqtt_hostname.contains("://") {
        mqtt_hostname.clone()
    } else {
        format!("mqtt://{mqtt_hostname}")
    };

    // Live MQTT mode uses rumqttc if available via rusthinq-core path; keep as thin client.
    use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
    let host_port: Vec<&str> = url
        .trim_start_matches("mqtt://")
        .trim_start_matches("mqtts://")
        .split(':')
        .collect();
    let host = host_port[0];
    let port: u16 = host_port
        .get(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);

    let mut opts = MqttOptions::new("packet-parser", host, port);
    opts.set_keep_alive(rusthinq_util::MQTT_KEEP_ALIVE);
    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    let topic = format!("clip/message/devices/{device_id}");
    client.subscribe(&topic, QoS::AtMostOnce).await?;

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                if p.topic != topic {
                    continue;
                }
                let mut payload = p.payload.to_vec();
                if payload.last() == Some(&0) {
                    payload.pop();
                }
                let text = String::from_utf8_lossy(&payload);
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
                    && v.get("cmd").and_then(|c| c.as_str()) == Some("device_packet")
                    && let Some(data) = v.get("data").and_then(|d| d.as_str())
                {
                    println!("{} {}", chrono_like_now(), data);
                    match rusthinq_util::hex::decode(data) {
                        Ok(buf) => print_tlv(&buf, false),
                        Err(e) => println!("{e}"),
                    }
                }
            }
            Ok(_) => {}
            Err(e) => bail!("mqtt error: {e}"),
        }
    }
}

fn chrono_like_now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
