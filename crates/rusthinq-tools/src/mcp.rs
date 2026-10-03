//! Newline JSON-RPC MCP adapter. Notifications do not receive responses.
use crate::Client;
use serde_json::{Value, json};
use std::io;
fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({"name":name,"description":description,"inputSchema":{"type":"object","properties":properties,"required":required}})
}
pub fn tools() -> Value {
    json!([
        tool(
            "set_api_endpoint",
            "Configure the daemon management API; credentials remain in environment variables",
            json!({"endpoint":{"type":"string"}}),
            &["endpoint"]
        ),
        tool(
            "list_devices",
            "Read live/durable devices and script/bridge status without an external broker",
            json!({}),
            &[]
        ),
        tool(
            "cloud_start",
            "Enable the shared, read-only LG notification observer",
            json!({}),
            &[]
        ),
        tool(
            "cloud_stop",
            "Disable the LG observer (shared by GUI and captures)",
            json!({"clear":{"type":"boolean"}}),
            &[]
        ),
        tool(
            "read_cloud",
            "Read bounded time-aligned LG notifications; cursors are decimal strings, reset/lost mark discontinuities",
            json!({"cursor":{"type":"string"},"limit":{"type":"integer"},"device_id":{"type":"string"}}),
            &[]
        ),
        tool("health", "Read daemon health", json!({}), &[]),
        tool(
            "decode_packet",
            "Decode a wire packet locally",
            json!({"hex":{"type":"string"},"direction":{"type":"string"},"model_id":{"type":"string"}}),
            &["hex"]
        ),
        tool(
            "encode_packet",
            "Encode bounded AABB/TLV frames locally",
            json!({"protocol":{"type":"string","enum":["tlv","aabb"]},"direction":{"type":"string","enum":["toDevice","fromDevice"]},"body_hex":{"type":"string"},"tlv":{"type":"array","items":{"type":"object","properties":{"t":{"type":"integer"},"v":{"type":"integer"}},"required":["t","v"]}}}),
            &["protocol"]
        ),
        tool(
            "read_capture",
            "Read bounded legacy/new JSONL captures",
            json!({"path":{"type":"string"},"offset":{"type":"integer"},"limit":{"type":"integer"},"type":{"type":"string"}}),
            &["path"]
        ),
        tool(
            "inject",
            "Inject after explicit local opt-in and server authorization; session identity is captured before admission",
            json!({"device_id":{"type":"string"},"hex":{"type":"string"},"inject_ok":{"type":"boolean"},"from_device":{"type":"boolean"}}),
            &["device_id", "hex", "inject_ok"]
        ),
    ])
}
pub async fn dispatch(client: &mut Option<Client>, request: Value) -> Option<Value> {
    let id = request.get("id")?.clone();
    if request["jsonrpc"] != "2.0" {
        return Some(
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32600,"message":"invalid request"}}),
        );
    }
    let result: io::Result<Value> = match request["method"].as_str() {
        Some("initialize") => Ok(
            json!({"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"rusthinq-mcp","version":env!("CARGO_PKG_VERSION")}}),
        ),
        Some("ping") => Ok(json!({})),
        Some("tools/list") => Ok(json!({"tools":tools()})),
        Some("tools/call") => {
            let args = &request["params"]["arguments"];
            let name = request["params"]["name"].as_str().unwrap_or_default();
            let value: io::Result<Value> = match name {
                "decode_packet" => crate::decode(args),
                "encode_packet" => crate::encode(args),
                "read_capture" => crate::read_capture(args),
                "set_api_endpoint" => match args["endpoint"].as_str() {
                    Some(endpoint) => Client::new(
                        endpoint,
                        std::env::var("RUSTHINQ_USER").ok().as_deref(),
                        std::env::var("RUSTHINQ_PASSWORD").ok().as_deref(),
                    )
                    .map(|value| {
                        *client = Some(value);
                        json!({"configured":true})
                    }),
                    None => Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "endpoint required",
                    )),
                },
                "list_devices" | "health" | "inject" | "cloud_start" | "cloud_stop"
                | "read_cloud" => {
                    if client.is_none() {
                        match Client::environment() {
                            Ok(value) => *client = Some(value),
                            Err(error) => return Some(tool_result(id, Err(error))),
                        }
                    }
                    let client = client.as_ref().expect("configured");
                    match name {
                        "list_devices" => client.request("api/devices", None).await,
                        "health" => client.request("api/health", None).await,
                        "cloud_start" => {
                            client
                                .request("api/cloud/notifications", Some(&json!({"enabled":true})))
                                .await
                        }
                        "cloud_stop" => {
                            client
                                .request(
                                    "api/cloud/notifications",
                                    Some(&json!({"enabled":false,"clear":args["clear"]==true})),
                                )
                                .await
                        }
                        "read_cloud" => {
                            let cursor = args["cursor"].as_str().unwrap_or("0").parse::<u64>();
                            match cursor {
                                Err(_) => Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "cursor must be a decimal u64 string",
                                )),
                                Ok(cursor) => {
                                    let mut path = format!(
                                        "api/cloud/notifications?cursor={cursor}&limit={}",
                                        args["limit"].as_u64().unwrap_or(100).min(200)
                                    );
                                    if let Some(device) = args["device_id"].as_str() {
                                        path.push_str("&device=");
                                        path.push_str(&crate::segment(device));
                                    }
                                    client.request(&path, None).await
                                }
                            }
                        }
                        _ => client.inject(args).await,
                    }
                }
                "set_mqtt_host" => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "0.2 uses set_api_endpoint; MQTT is optional",
                )),
                _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "unknown tool")),
            };
            return Some(tool_result(id, value));
        }
        _ => {
            return Some(
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
            );
        }
    };
    match result {
        Ok(result) => Some(json!({"jsonrpc":"2.0","id":id,"result":result})),
        Err(error) => Some(
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":format!("{error}")}}),
        ),
    }
}
fn tool_result(id: Value, result: io::Result<Value>) -> Value {
    let (text, error) = match result {
        Ok(value) => (value.to_string(), false),
        Err(error) => (error.to_string(), true),
    };
    json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":text}],"isError":error}})
}
pub async fn run() -> io::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    let mut client = None;
    loop {
        let mut bytes = Vec::new();
        let n = (&mut reader)
            .take(1_048_577)
            .read_until(b'\n', &mut bytes)
            .await?;
        if n == 0 {
            break;
        }
        if n > 1_048_576 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP input exceeded",
            ));
        }
        let response = match serde_json::from_slice(&bytes) {
            Ok(value) => dispatch(&mut client, value).await,
            Err(_) => Some(
                json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}),
            ),
        };
        if let Some(response) = response {
            output.write_all(format!("{response}\n").as_bytes()).await?;
            output.flush().await?;
        }
    }
    Ok(())
}
