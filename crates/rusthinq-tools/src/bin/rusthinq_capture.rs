//! Capture device wire traffic from rusthinq-cloud's raw MQTT bus to JSONL.
//! Replaces tools/rusthinq-capture.ts (device path; use bridge mode for cloud
//! correlation). Subscribes to `<prefix>/<device_id>/raw/rx`, `.../raw/tx` and `.../raw/clip/tx`
//! (see rusthinq-cloud's raw_bus.rs) instead of the old management WebSocket,
//! which no longer exists — MQTT is the only external control surface now.
//!
//! Usage:
//!   rusthinq-capture <mqtt-host[:port]> <device-uuid> [out.jsonl]
//!
//! Env: RUSTHINQ_PREFIX (default "rusthinq") -- despite the name, this is
//!      rusthinq-cloud's `[mqtt] raw_prefix`, NOT `rusthinq_prefix`: the raw
//!      bus this tool reads always lives under raw_prefix (see raw_bus.rs),
//!      and the two commonly differ. Get this wrong and the tool subscribes
//!      to a topic nothing publishes and silently captures nothing.
//!
//! Stdin lines become `{"k":"note","t":…,"text":…}` annotations.

use anyhow::{Context, Result};
use rusthinq_util::packet_codec::{Decoded, decode_packet};
use serde_json::{Value, json};
use std::env;
use std::fs::OpenOptions;
use std::io::{BufRead, Write};
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `payload` is hex for a wire frame, but T2Clip/T1Json sends on `raw/clip/tx` are a
/// JSON-stringified command instead — only decode when it actually looks like
/// one of ours (even length, hex digits only).
fn decode_summary(payload: &str) -> Option<Value> {
    let looks_like_hex = !payload.is_empty()
        && payload.len().is_multiple_of(2)
        && payload.chars().all(|c| c.is_ascii_hexdigit());
    if !looks_like_hex {
        return None;
    }
    Some(match decode_packet(payload) {
        Decoded::Tlv(t) => json!({
            "protocol": "Tlv",
            "direction": format!("{:?}", t.direction),
            "crc_ok": t.crc_ok,
            "tlv": t.tlv.iter().map(|e| json!({"t": e.t, "v": e.v})).collect::<Vec<_>>(),
        }),
        Decoded::Aabb(a) => json!({
            "protocol": "Aabb",
            "checksum_ok": a.checksum_ok,
            "body": a.body,
        }),
        Decoded::Unknown(u) => json!({
            "protocol": "Unknown",
            "reason": u.reason,
        }),
    })
}

enum Ev {
    Note(String),
    Wire { dir: &'static str, payload: String },
    Closed,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 {
        eprintln!("Usage: rusthinq-capture <mqtt-host[:port]> <device-uuid> [out.jsonl]");
        std::process::exit(2);
    }
    let host = &args[0];
    let device_id = &args[1];
    let out_path = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| format!("{device_id}.jsonl"));
    let prefix = env::var("RUSTHINQ_PREFIX").unwrap_or_else(|_| "rusthinq".into());

    let mut out = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .with_context(|| format!("open {out_path}"))?;

    let write_ev = |out: &mut std::fs::File, ev: Value| -> Result<()> {
        writeln!(out, "{ev}")?;
        out.flush()?;
        Ok(())
    };

    write_ev(
        &mut out,
        json!({
            "k": "session",
            "t": now_ms(),
            "device_id": device_id,
            "mqtt": host,
        }),
    )?;

    // `raw/#`, not `raw/+`: CLIP commands sent to the device are on `raw/clip/tx`.
    let topic_filter = format!("{prefix}/{device_id}/raw/#");
    eprintln!("[rusthinq-capture] subscribing {topic_filter} on {host} → {out_path}");
    let client_id = format!("rusthinq-capture-{}", std::process::id());
    let (mqtt_client, mqtt_rx) =
        rusthinq_tools::mqtt::subscribe_stream(&client_id, host, &topic_filter)?;

    let (tx, rx) = mpsc::channel::<Ev>();
    let tx_notes = tx.clone();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            if tx_notes.send(Ev::Note(line)).is_err() {
                break;
            }
        }
    });
    let tx_wire = tx.clone();
    thread::spawn(move || {
        while let Ok(p) = mqtt_rx.recv() {
            let dir = if p.topic.ends_with(b"/raw/rx") {
                "rx"
            } else if p.topic.ends_with(b"/raw/tx") || p.topic.ends_with(b"/raw/clip/tx") {
                "tx"
            } else {
                continue;
            };
            let payload = String::from_utf8_lossy(&p.payload).to_string();
            if tx_wire.send(Ev::Wire { dir, payload }).is_err() {
                break;
            }
        }
        let _ = tx_wire.send(Ev::Closed);
    });

    while let Ok(ev) = rx.recv() {
        match ev {
            Ev::Note(note) => {
                if note.trim().is_empty() {
                    continue;
                }
                write_ev(&mut out, json!({"k": "note", "t": now_ms(), "text": note}))?;
                eprintln!("[note] {note}");
            }
            Ev::Wire { dir, payload } => {
                let t = now_ms();
                let mut ev = json!({"k": dir, "t": t, "hex": payload});
                if let Some(decode) = decode_summary(&payload) {
                    ev["decode"] = decode;
                }
                write_ev(&mut out, ev)?;
                eprintln!("[{dir}] {}", &payload[..payload.len().min(32)]);
            }
            Ev::Closed => {
                eprintln!("[rusthinq-capture] MQTT connection closed");
                break;
            }
        }
    }
    let _ = mqtt_client.disconnect();
    Ok(())
}
