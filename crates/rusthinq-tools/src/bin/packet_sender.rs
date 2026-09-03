//! packet-sender: build a TLV toDevice packet and publish via MQTT.

use anyhow::{Context, Result, bail};
use rusthinq_util::crc16::crc16;
use rusthinq_util::tlv::{self, Tlv};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 9 {
        eprintln!(
            "Usage:
\tpacket-sender mqtt-hostname[:port] device-uuid a_value s_value byte5 byte6 byte7 [t1 v1] [t2 v2] [...]

\ta_value, s_value - see https://github.com/anszom/rethink/wiki/CloudProtocol#packet
\tbyte5, byte6, byte7 - see https://github.com/anszom/rethink/wiki/UartProtocol#framing-format
\ttX,vX - TLV attributes
"
        );
        return Ok(());
    }

    let mqtt_hostname = &args[1];
    let device_id = &args[2];
    let numbers: Vec<u32> = args[3..]
        .iter()
        .map(|s| s.parse::<u32>())
        .collect::<Result<Vec<_>, _>>()
        .context("numeric args")?;
    let b0 = numbers[0] as u8;
    let b1 = numbers[1] as u8;
    let b2 = numbers[2] as u8;
    let b3 = numbers[3] as u8;
    let b4 = numbers[4] as u8;

    let mut tlvs = Vec::new();
    let mut i = 5;
    while i + 1 < numbers.len() {
        tlvs.push(Tlv::new(numbers[i] as u16, numbers[i + 1]));
        i += 2;
    }
    let tlv_array = tlv::build(&tlvs);
    if tlv_array.len() > 255 {
        bail!("TLV payload too large");
    }
    let mut buf = vec![
        0x04,
        0x00,
        0x00,
        0x00,
        0x65,
        b2,
        b3,
        b4,
        tlv_array.len() as u8,
    ];
    buf.extend_from_slice(&tlv_array);
    let result = crc16(&buf);
    let mut out = vec![b0, b1];
    out.extend_from_slice(&buf);
    out.push((result >> 8) as u8);
    out.push((result & 0xff) as u8);

    let mid = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let messagestr = serde_json::json!({
        "did": device_id,
        "mid": mid,
        "cmd": "packet",
        "type": 1,
        // Some firmware only accepts uppercase hex in the packet payload and
        // silently ignores lowercase (matches cloud's real send path).
        "data": rusthinq_util::hex::encode_upper(&out),
    })
    .to_string();
    println!("{messagestr}");

    let url = if mqtt_hostname.contains("://") {
        mqtt_hostname.clone()
    } else {
        format!("mqtt://{mqtt_hostname}")
    };
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

    use rumqttc::{AsyncClient, MqttOptions, QoS};
    let mut opts = MqttOptions::new("packet-sender", host, port);
    opts.set_keep_alive(rusthinq_util::MQTT_KEEP_ALIVE);
    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    let topic = format!("lime/devices/{device_id}");
    // trailing space matches TS packet-sender
    client
        .publish(&topic, QoS::AtMostOnce, false, format!("{messagestr} "))
        .await?;

    // Drive eventloop until publish is flushed
    for _ in 0..20 {
        let _ = eventloop.poll().await;
    }
    Ok(())
}
