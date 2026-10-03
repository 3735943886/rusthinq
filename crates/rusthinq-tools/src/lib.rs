//! Thin management API adapters and offline codecs. No device or account ownership.
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use rusthinq_protocol::{
    hex,
    packet_codec::{self, AabbEncodeInput, Decoded, Direction, EncodeInput, TlvEncodeInput},
    tlv::Tlv,
};
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Read},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{client::IntoClientRequest, http::header::AUTHORIZATION},
};

#[derive(Clone)]
pub struct Client {
    base: url::Url,
    http: reqwest::Client,
    authorization: Option<String>,
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid management/tool input")
}
impl Client {
    pub fn new(endpoint: &str, user: Option<&str>, password: Option<&str>) -> io::Result<Self> {
        let base = url::Url::parse(endpoint).map_err(|_| invalid())?;
        if endpoint.len() > 4096
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || !base.path().ends_with('/')
            || !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
        {
            return Err(invalid());
        }
        if base.scheme() == "http"
            && !base.host_str().is_some_and(|h| {
                h == "localhost"
                    || h.parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            })
        {
            return Err(invalid());
        }
        if user.is_some() != password.is_some()
            || user.is_some_and(|u| {
                u.is_empty() || u.contains(':') || u.len() > 256 || u.chars().any(char::is_control)
            })
            || password.is_some_and(|p| p.is_empty() || p.len() > 1024)
        {
            return Err(invalid());
        }
        let authorization = user
            .zip(password)
            .map(|(u, p)| format!("Basic {}", STANDARD.encode(format!("{u}:{p}"))));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(100))
            .build()
            .map_err(|_| invalid())?;
        Ok(Self {
            base,
            http,
            authorization,
        })
    }
    pub fn environment() -> io::Result<Self> {
        let endpoint =
            std::env::var("RUSTHINQ_API").unwrap_or_else(|_| "http://127.0.0.1:8080/".into());
        Self::new(
            &endpoint,
            std::env::var("RUSTHINQ_USER").ok().as_deref(),
            std::env::var("RUSTHINQ_PASSWORD").ok().as_deref(),
        )
    }
    fn url(&self, path: &str) -> io::Result<url::Url> {
        if path.starts_with('/') || path.contains("..") || path.contains("://") {
            return Err(invalid());
        }
        let url = self.base.join(path).map_err(|_| invalid())?;
        if url.origin() != self.base.origin() {
            return Err(invalid());
        }
        Ok(url)
    }
    pub async fn request(&self, path: &str, body: Option<&Value>) -> io::Result<Value> {
        let url = self.url(path)?;
        let mut request = if let Some(body) = body {
            self.http.post(url).json(body)
        } else {
            self.http.get(url)
        };
        if let Some(auth) = &self.authorization {
            let mut header = reqwest::header::HeaderValue::from_str(auth).map_err(|_| invalid())?;
            header.set_sensitive(true);
            request = request.header(AUTHORIZATION, header);
        }
        let mut response = request.send().await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "management request failed; mutation outcome may be unknown",
            )
        })?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| invalid())? {
            if bytes.len() + chunk.len() > 1_048_576 {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if !status.is_success() {
            return Err(io::Error::other(format!(
                "management HTTP {}: {}",
                status.as_u16(),
                value["error"].as_str().unwrap_or("rejected")
            )));
        }
        Ok(value)
    }
    pub async fn events(
        &self,
    ) -> io::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let mut url = self.url("api/events")?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme).map_err(|_| invalid())?;
        let mut request = url.as_str().into_client_request().map_err(|_| invalid())?;
        if let Some(auth) = &self.authorization {
            let mut header = auth
                .parse::<tokio_tungstenite::tungstenite::http::HeaderValue>()
                .map_err(|_| invalid())?;
            header.set_sensitive(true);
            request.headers_mut().insert(AUTHORIZATION, header);
        }
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(1_048_576))
            .max_frame_size(Some(1_048_576));
        let (stream, _) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| invalid())?
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "management event connection failed",
            )
        })?;
        Ok(stream)
    }
    pub async fn scope(&self, id: &str) -> io::Result<Value> {
        let snapshot = self.request("api/devices", None).await?;
        let device = &snapshot["devices"][id];
        if !device.is_object() {
            return Err(invalid());
        }
        Ok(
            json!({"incarnation":device["incarnation"],"generation":device["generation"],"script_generation":device["scriptGeneration"]}),
        )
    }
    pub async fn inject(&self, args: &Value) -> io::Result<Value> {
        if args["inject_ok"] != true {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "inject_ok=true required",
            ));
        }
        let id = args["device_id"].as_str().ok_or_else(invalid)?;
        hex::decode(args["hex"].as_str().ok_or_else(invalid)?).map_err(|_| invalid())?;
        let mut scope = self.scope(id).await?;
        if scope["generation"].is_null() {
            return Err(invalid());
        }
        scope["hex"] = args["hex"].clone();
        scope["direction"] = json!(if args["from_device"] == true {
            "fromDevice"
        } else {
            "toDevice"
        });
        self.request(&format!("api/devices/{}/inject", segment(id)), Some(&scope))
            .await
    }
}
pub fn segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}
pub fn decode(args: &Value) -> io::Result<Value> {
    let text = args["hex"]
        .as_str()
        .filter(|s| s.len() <= 2_000_000)
        .ok_or_else(invalid)?;
    let direction = args["direction"].as_str();
    if direction
        .is_some_and(|v| !matches!(v, "fromDevice" | "toDevice" | "rx" | "tx" | "from" | "to"))
    {
        return Err(invalid());
    }
    let mut decoded = match rusthinq_protocol::decode::decode_hex_payload(text, direction) {
        Ok(decoded) => decoded,
        Err(reason) => return Ok(json!({"protocol":"Unknown","hex":text,"reason":reason})),
    };
    // Preserve the 0.2 baseline fields while restoring the detailed 0.1 analysis.
    match packet_codec::decode_packet(text) {
        Decoded::Aabb(a) => {
            decoded["checksum_ok"] = json!(a.checksum_ok);
            decoded["length"] = json!(a.length);
            decoded["body"] = json!(a.body);
        }
        Decoded::Tlv(t) => {
            decoded["crc_ok"] = json!(t.crc_ok);
            decoded["tlv"] = json!(
                t.tlv
                    .iter()
                    .map(|v| json!({"t":v.t,"l":v.l,"v":v.v}))
                    .collect::<Vec<_>>()
            );
            decoded["frame"] = json!({"kind":t.frame.kind,"byte5":t.frame.byte5,"byte6":t.frame.byte6,"byte7":t.frame.byte7,"len":t.frame.len});
            decoded["a"] = json!(t.a);
            decoded["s"] = json!(t.s);
        }
        Decoded::Unknown(u) => decoded["reason"] = json!(u.reason),
    }
    decoded["exportText"] = json!(rusthinq_protocol::decode::re_export_text(
        &decoded,
        args["model_id"].as_str(),
        direction,
    ));
    Ok(decoded)
}

pub fn encode(args: &Value) -> io::Result<Value> {
    let direction = match args["direction"].as_str().unwrap_or("toDevice") {
        "toDevice" => Direction::ToDevice,
        "fromDevice" => Direction::FromDevice,
        _ => return Err(invalid()),
    };
    let byte = |key: &str| -> io::Result<Option<u8>> {
        args.get(key)
            .map(|v| {
                v.as_u64()
                    .and_then(|v| v.try_into().ok())
                    .ok_or_else(invalid)
            })
            .transpose()
    };
    let input = match args["protocol"].as_str() {
        Some("aabb") => EncodeInput::Aabb(AabbEncodeInput {
            body_hex: args["body_hex"]
                .as_str()
                .filter(|s| s.len() <= 500)
                .ok_or_else(invalid)?
                .into(),
            direction: Some(direction),
        }),
        Some("tlv") => {
            let values = args["tlv"]
                .as_array()
                .filter(|v| v.len() <= 128)
                .ok_or_else(invalid)?;
            let mut tlv = Vec::new();
            for value in values {
                let t = value["t"]
                    .as_u64()
                    .filter(|v| *v <= 1023)
                    .ok_or_else(invalid)? as u16;
                let v = value["v"]
                    .as_u64()
                    .filter(|v| *v <= 0xffffff)
                    .ok_or_else(invalid)? as u32;
                tlv.push(Tlv::new(t, v));
            }
            EncodeInput::Tlv(TlvEncodeInput {
                direction,
                tlv,
                a: byte("a")?,
                s: byte("s")?,
                byte5: byte("byte5")?,
                byte6: byte("byte6")?,
                byte7: byte("byte7")?,
            })
        }
        _ => return Err(invalid()),
    };
    let (hex, bytes) = packet_codec::encode_packet(&input).map_err(io::Error::other)?;
    Ok(json!({"hex":hex,"bytes":bytes}))
}
pub fn read_capture(args: &Value) -> io::Result<Value> {
    let path = args["path"].as_str().ok_or_else(invalid)?;
    let offset = args["offset"].as_u64().unwrap_or(0);
    let limit = args["limit"].as_u64().unwrap_or(50).min(1000);
    let kind = args["type"].as_str();
    let file = std::fs::File::open(Path::new(path))?;
    let mut reader = io::BufReader::new(file.take(64 * 1024 * 1024 + 1));
    let mut count = 0;
    let mut scanned = 0;
    let mut values = Vec::new();
    loop {
        let mut bytes = Vec::new();
        let n = reader
            .by_ref()
            .take(1_048_577)
            .read_until(b'\n', &mut bytes)?;
        if n == 0 {
            break;
        }
        scanned += n;
        if scanned > 64 * 1024 * 1024 || n > 1_048_576 {
            return Err(invalid());
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if kind.is_some_and(|k| {
            !matches!(value["k"].as_str(), Some("rx" | "tx"))
                || value["type"].as_str().unwrap_or("packet") != k
        }) {
            continue;
        }
        if count >= offset && values.len() < (limit as usize) {
            values.push(value);
        }
        count += 1;
        if values.len() == limit as usize {
            break;
        }
    }
    Ok(json!({"events":values,"offset":offset,"limit":limit}))
}
pub fn capture_event(event: &Value, id: &str) -> Option<Value> {
    if event["type"] == "lost" {
        return Some(json!({"k":"lost","t":now_ms(),"events":event["events"]}));
    }
    if event["device"] != id {
        return None;
    }
    let (direction, payload) = match event["type"].as_str() {
        Some("data") => ("rx", event["hex"].as_str()?),
        Some("sent") => ("tx", event["hex"].as_str()?),
        Some("injected") if event["toDevice"] == false => ("rx", event["hex"].as_str()?),
        _ => return None,
    };
    let bytes = hex::decode(payload).ok()?;
    let text = std::str::from_utf8(&bytes).ok();
    let value = text.and_then(|t| serde_json::from_str::<Value>(t).ok());
    let (kind, payload) = match value.as_ref() {
        Some(value) if value["cmd"] == "ack" => ("ack", value["data"].as_str()?.to_string()),
        Some(value) if value["Body"]["Format"] == "B64" => {
            let data = STANDARD.decode(value["Body"]["Data"].as_str()?).ok()?;
            ("packet", hex::encode(data))
        }
        Some(_) => ("clip", text?.to_string()),
        None => ("packet", payload.to_string()),
    };
    Some(json!({"k":direction,"t":now_ms(),"type":kind,"hex":payload}))
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
pub async fn capture(client: Client, id: String, path: &Path) -> io::Result<()> {
    let mut output = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)?;
    let mut events = client.events().await?;
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
    use tokio::io::AsyncBufReadExt;
    let mut note = Vec::new();
    let mut notes = true;
    loop {
        tokio::select! {
            signal=tokio::signal::ctrl_c()=>{signal?;let _=events.close(None).await;break;},
            result=stdin.fill_buf(),if notes=>{let bytes=result?;if bytes.is_empty(){notes=false;}else{let end=bytes.iter().position(|b|*b==b'\n').map(|n|n+1);let n=end.unwrap_or(bytes.len());if note.len()+n>65536{return Err(invalid());}note.extend_from_slice(&bytes[..n]);stdin.consume(n);if end.is_some(){let text=std::str::from_utf8(&note).map_err(|_|invalid())?;use std::io::Write;writeln!(output,"{}",json!({"k":"note","t":now_ms(),"text":text.trim_end()}))?;output.flush()?;note.clear();}}},
            event=events.next()=>{let Some(event)=event else{break;};let event=event.map_err(|_|invalid())?;if let tokio_tungstenite::tungstenite::Message::Text(text)=event{let event:Value=serde_json::from_str(&text).map_err(|_|invalid())?;if let Some(value)=capture_event(&event,&id){use std::io::Write;writeln!(output,"{value}")?;output.flush()?;}}}
        }
    }
    Ok(())
}

pub mod cli;
pub mod mcp;

pub mod softap;

pub mod migration;

pub mod replay;
