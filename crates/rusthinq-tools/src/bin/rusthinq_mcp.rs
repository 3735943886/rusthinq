//! Minimal MCP server (stdio, newline-delimited JSON-RPC 2.0) for RE workflows.
//! Replaces tools/mcp-server.ts — talks to rusthinq-cloud over MQTT (its only
//! external control surface now) plus pure codecs.
//!
//! Tools: set_mqtt_host, list_devices, health, encode_packet, decode_packet,
//!        read_capture, inject (gated).
//!
//! Run: rusthinq-mcp
//! Env: RUSTHINQ_MQTT=host[:port] (default 127.0.0.1:1883),
//!      RUSTHINQ_PREFIX (default "rusthinq", rusthinq-cloud's `[mqtt] rusthinq_prefix`;
//!      used by list_devices/health),
//!      RUSTHINQ_RAW_PREFIX (default "rusthinq-raw", its `[mqtt] raw_prefix`; the raw
//!      bus `inject` publishes to lives there, see raw_bus.rs).

use anyhow::{Context, Result, anyhow};
use rusthinq_tools::mqtt;
use rusthinq_util::decode::decode_hex_payload;
use rusthinq_util::packet_codec::{
    AabbEncodeInput, Direction, EncodeInput, TlvEncodeInput, encode_packet,
};
use rusthinq_util::sync::Mutex;
use rusthinq_util::tlv::Tlv;
use serde_json::{Value, json};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Duration;

static MQTT_HOST: Mutex<String> = Mutex::new(String::new());
static PREFIX: Mutex<String> = Mutex::new(String::new());
static RAW_PREFIX: Mutex<String> = Mutex::new(String::new());

fn mqtt_host() -> String {
    MQTT_HOST
        .lock()
        .clone()
        .if_empty(|| env::var("RUSTHINQ_MQTT").unwrap_or_else(|_| "127.0.0.1:1883".into()))
}

fn prefix() -> String {
    PREFIX
        .lock()
        .clone()
        .if_empty(|| env::var("RUSTHINQ_PREFIX").unwrap_or_else(|_| "rusthinq".into()))
}

fn raw_prefix() -> String {
    RAW_PREFIX
        .lock()
        .clone()
        .if_empty(|| env::var("RUSTHINQ_RAW_PREFIX").unwrap_or_else(|_| "rusthinq-raw".into()))
}

trait IfEmpty {
    fn if_empty(self, f: impl FnOnce() -> String) -> String;
}
impl IfEmpty for String {
    fn if_empty(self, f: impl FnOnce() -> String) -> String {
        if self.is_empty() { f() } else { self }
    }
}

const FETCH_TIMEOUT: Duration = Duration::from_secs(3);

fn tool_list() -> Value {
    json!([
        {
            "name": "set_mqtt_host",
            "description": "Set the rusthinq-cloud MQTT broker host[:port] (and optionally its rusthinq_prefix and raw_prefix) for subsequent tools",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "host": { "type": "string" },
                    "prefix": { "type": "string" },
                    "raw_prefix": { "type": "string" }
                },
                "required": ["host"]
            }
        },
        {
            "name": "list_devices",
            "description": "Fetch the retained <prefix>/devices snapshot: connected devices plus handler-mapping/bridge status",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "health",
            "description": "Fetch <prefix>/devices and report MQTT/bridge connectivity derived from it",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "decode_packet",
            "description": "Decode ThinQ UART/AABB hex locally via rusthinq-util (no network)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "hex": { "type": "string" },
                    "direction": { "type": "string" },
                    "model_id": { "type": "string" }
                },
                "required": ["hex"]
            }
        },
        {
            "name": "encode_packet",
            "description": "Encode TLV or AABB packet from primitives",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "protocol": { "type": "string", "enum": ["tlv", "aabb"] },
                    "direction": { "type": "string", "enum": ["fromDevice", "toDevice"] },
                    "tlv": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "t": { "type": "integer" },
                                "v": { "type": "integer" }
                            },
                            "required": ["t", "v"]
                        }
                    },
                    "body_hex": { "type": "string" }
                },
                "required": ["protocol"]
            }
        },
        {
            "name": "read_capture",
            "description": "Read a JSONL capture file (from rusthinq-capture). Each rx/tx event has a type: \"packet\", \"ack\" for a delivery ack sent to the device (e.g. AABB f0 00 <type> 04 [<seq16>]), or \"clip\" for another CLIP message. Optional filter by type; paging via offset/limit.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "type": {
                        "type": "string",
                        "enum": ["packet", "ack", "clip"],
                        "description": "rx/tx events only; captures predating the field count as packet"
                    },
                    "offset": { "type": "integer" },
                    "limit": { "type": "integer" }
                },
                "required": ["path"]
            }
        },
        {
            "name": "inject",
            "description": "Publish a hex frame to the device's raw MQTT bus (requires a live device + inject_ok=true)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "device_id": { "type": "string" },
                    "hex": { "type": "string" },
                    "from_device": { "type": "boolean" },
                    "inject_ok": { "type": "boolean" }
                },
                "required": ["device_id", "hex", "inject_ok"]
            }
        }
    ])
}

fn decode_local(hex: &str, direction: Option<&str>) -> Value {
    match decode_hex_payload(hex, direction) {
        Ok(v) => v,
        Err(e) => json!({"ok": false, "error": e}),
    }
}

fn fetch_devices() -> Result<Option<Value>> {
    let topic = format!("{}/devices", prefix());
    let raw = mqtt::fetch_one("rusthinq-mcp-fetch", &mqtt_host(), &topic, FETCH_TIMEOUT)?;
    Ok(raw.map(|bytes| {
        serde_json::from_slice(&bytes).unwrap_or(json!({"raw": String::from_utf8_lossy(&bytes)}))
    }))
}

fn call_tool(name: &str, args: &Value) -> Result<Value> {
    match name {
        "set_mqtt_host" => {
            let host = args
                .get("host")
                .and_then(|h| h.as_str())
                .ok_or_else(|| anyhow!("host required"))?;
            *MQTT_HOST.lock() = host.to_string();
            if let Some(p) = args.get("prefix").and_then(|p| p.as_str()) {
                *PREFIX.lock() = p.to_string();
            }
            if let Some(p) = args.get("raw_prefix").and_then(|p| p.as_str()) {
                *RAW_PREFIX.lock() = p.to_string();
            }
            Ok(json!({"ok": true, "host": host, "prefix": prefix(), "raw_prefix": raw_prefix()}))
        }
        "list_devices" => match fetch_devices()? {
            Some(v) => Ok(v),
            None => Ok(json!({
                "ok": false,
                "error": format!("no retained {}/devices snapshot within {:?} — is rusthinq-cloud running and reachable at {}?", prefix(), FETCH_TIMEOUT, mqtt_host())
            })),
        },
        "health" => match fetch_devices()? {
            Some(v) => Ok(json!({
                "ok": true,
                "mqttConnected": true,
                "cloudMqttConnected": v.get("mqtt").cloned().unwrap_or(Value::Null),
                "bridgeLoggedIn": v.get("bridgeLoggedIn").cloned().unwrap_or(Value::Null),
                "deviceCount": v.get("devices").and_then(|d| d.as_object()).map(|o| o.len()).unwrap_or(0),
            })),
            None => Ok(json!({
                "ok": false,
                "mqttConnected": false,
                "error": format!("no response from {} within {:?}", mqtt_host(), FETCH_TIMEOUT)
            })),
        },
        "decode_packet" => {
            let hex = args
                .get("hex")
                .and_then(|h| h.as_str())
                .ok_or_else(|| anyhow!("hex required"))?;
            Ok(decode_local(
                hex,
                args.get("direction").and_then(|d| d.as_str()),
            ))
        }
        "encode_packet" => {
            let protocol = args
                .get("protocol")
                .and_then(|p| p.as_str())
                .unwrap_or("tlv");
            let input = match protocol {
                "aabb" => {
                    let body_hex = args
                        .get("body_hex")
                        .and_then(|h| h.as_str())
                        .ok_or_else(|| anyhow!("body_hex required for aabb"))?;
                    EncodeInput::Aabb(AabbEncodeInput {
                        body_hex: body_hex.into(),
                        direction: None,
                    })
                }
                _ => {
                    let dir = match args
                        .get("direction")
                        .and_then(|d| d.as_str())
                        .unwrap_or("toDevice")
                    {
                        "fromDevice" => Direction::FromDevice,
                        _ => Direction::ToDevice,
                    };
                    let tlv: Vec<Tlv> = args
                        .get("tlv")
                        .and_then(|a| a.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|e| {
                                    Some(Tlv::new(
                                        e.get("t")?.as_u64()? as u16,
                                        e.get("v")?.as_u64()? as u32,
                                    ))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    EncodeInput::Tlv(TlvEncodeInput {
                        direction: dir,
                        tlv,
                        a: None,
                        s: None,
                        byte5: None,
                        byte6: None,
                        byte7: None,
                    })
                }
            };
            let (hex, _) = encode_packet(&input).map_err(|e| anyhow!("{e}"))?;
            Ok(json!({"hex": hex}))
        }
        "read_capture" => {
            let path = args
                .get("path")
                .and_then(|p| p.as_str())
                .ok_or_else(|| anyhow!("path required"))?;
            let offset = args.get("offset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
            let limit = args.get("limit").and_then(|l| l.as_u64()).unwrap_or(50) as usize;
            let file = File::open(Path::new(path)).with_context(|| format!("open {path}"))?;
            let lines: Vec<String> = BufReader::new(file)
                .lines()
                .collect::<std::io::Result<_>>()?;
            let wire_type = args.get("type").and_then(|t| t.as_str());
            let slice: Vec<Value> = lines
                .into_iter()
                .filter_map(|l| serde_json::from_str::<Value>(&l).ok())
                .filter(|e| {
                    wire_type.is_none_or(|want| {
                        matches!(e.get("k").and_then(Value::as_str), Some("rx" | "tx"))
                            && e.get("type").and_then(Value::as_str).unwrap_or("packet") == want
                    })
                })
                .skip(offset)
                .take(limit)
                .collect();
            Ok(json!({"offset": offset, "count": slice.len(), "events": slice}))
        }
        "inject" => {
            if !args
                .get("inject_ok")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                return Err(anyhow!("inject_ok must be true to allow injection"));
            }
            let device_id = args
                .get("device_id")
                .and_then(|d| d.as_str())
                .ok_or_else(|| anyhow!("device_id required"))?;
            let hex = args
                .get("hex")
                .and_then(|h| h.as_str())
                .ok_or_else(|| anyhow!("hex required"))?;
            let from_device = args
                .get("from_device")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let leaf = if from_device { "emit" } else { "inject" };
            let topic = format!("{}/{device_id}/raw/{leaf}/set", raw_prefix());
            mqtt::publish("rusthinq-mcp-inject", &mqtt_host(), &topic, hex.as_bytes())?;
            Ok(
                json!({"ok": true, "device_id": device_id, "from_device": from_device, "topic": topic}),
            )
        }
        other => Err(anyhow!("unknown tool {other}")),
    }
}

fn respond(id: Value, result: Value) {
    let msg = json!({"jsonrpc": "2.0", "id": id, "result": result});
    let mut out = std::io::stdout().lock();
    writeln!(out, "{msg}").ok();
    out.flush().ok();
}

fn respond_err(id: Value, message: String) {
    let msg = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32000, "message": message }
    });
    let mut out = std::io::stdout().lock();
    writeln!(out, "{msg}").ok();
    out.flush().ok();
}

fn main() {
    // Diagnostics to stderr only
    eprintln!(
        "[rusthinq-mcp] ready mqtt={} prefix={} raw_prefix={}",
        mqtt_host(),
        prefix(),
        raw_prefix()
    );
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[rusthinq-mcp] bad json: {e}");
                continue;
            }
        };
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(json!({}));

        match method {
            "initialize" => {
                respond(
                    id,
                    json!({
                        "protocolVersion": "2024-11-05",
                        "capabilities": { "tools": {} },
                        "serverInfo": { "name": "rusthinq-mcp", "version": env!("CARGO_PKG_VERSION") }
                    }),
                );
            }
            "notifications/initialized" | "initialized" => {
                // no response for notifications without id
            }
            "tools/list" => {
                respond(id, json!({ "tools": tool_list() }));
            }
            "tools/call" => {
                let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
                let args = params.get("arguments").cloned().unwrap_or(json!({}));
                match call_tool(name, &args) {
                    Ok(v) => respond(
                        id,
                        json!({
                            "content": [{ "type": "text", "text": v.to_string() }],
                            "structuredContent": v,
                            "isError": false
                        }),
                    ),
                    Err(e) => respond(
                        id,
                        json!({
                            "content": [{ "type": "text", "text": e.to_string() }],
                            "isError": true
                        }),
                    ),
                }
            }
            "ping" => respond(id, json!({})),
            _ => {
                if !id.is_null() {
                    respond_err(id, format!("method not found: {method}"));
                }
            }
        }
    }
}
