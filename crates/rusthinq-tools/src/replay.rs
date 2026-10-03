//! Ordered capture ingress replay. Validate the complete bounded plan before any
//! submission, and never retarget a replacement device or retry an unknown result.
use crate::{Client, segment};
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Read},
    path::Path,
};
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub fn plan(path: &Path) -> io::Result<Vec<String>> {
    let file = std::fs::File::open(path)?;
    let mut reader = io::BufReader::new(file.take(64 * 1024 * 1024 + 1));
    let mut scanned = 0;
    let mut packets = Vec::new();
    loop {
        let mut line = Vec::new();
        let n = reader
            .by_ref()
            .take(1_048_577)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        scanned += n;
        if n > 1_048_576 || scanned > 64 * 1024 * 1024 {
            return Err(invalid("capture replay input exceeded"));
        }
        let value: Value =
            serde_json::from_slice(&line).map_err(|_| invalid("invalid capture JSON"))?;
        if !value.is_object() {
            return Err(invalid("invalid capture record"));
        }
        match value["k"].as_str() {
            Some("lost") => {
                return Err(invalid(
                    "capture contains a loss gap; replay requires a contiguous capture",
                ));
            }
            Some("rx") if value.get("type").is_none() || value["type"] == "packet" => {
                let text = value["hex"]
                    .as_str()
                    .ok_or_else(|| invalid("capture packet hex missing"))?;
                let bytes = rusthinq_protocol::hex::decode(text)
                    .map_err(|_| invalid("invalid capture packet hex"))?;
                if bytes.is_empty() {
                    return Err(invalid("empty capture packet"));
                }
                packets.push(rusthinq_protocol::hex::encode(bytes));
                if packets.len() > 1000 {
                    return Err(invalid("capture replay exceeds 1000 packets"));
                }
            }
            Some("rx" | "tx" | "note" | "session" | "cloud" | "marker") => {}
            _ => return Err(invalid("unknown capture record kind")),
        }
    }
    Ok(packets)
}
pub async fn replay(
    client: &Client,
    device: &str,
    path: &Path,
    inject_ok: bool,
) -> io::Result<Value> {
    if !inject_ok {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "--inject-ok required for replay",
        ));
    }
    let packets = plan(path)?;
    if packets.is_empty() {
        return Ok(json!({"submitted":0}));
    }
    let mut scope = client.scope(device).await?;
    if scope["generation"].is_null() {
        return Err(invalid("replay requires an online device"));
    }
    scope["direction"] = json!("fromDevice");
    for (submitted, packet) in packets.iter().enumerate() {
        scope["hex"] = json!(packet);
        if client
            .request(
                &format!("api/devices/{}/inject", segment(device)),
                Some(&scope),
            )
            .await
            .is_err()
        {
            return Err(io::Error::other(format!(
                "capture replay stopped after {submitted} submissions; latest request was not confirmed; do not automatically retry"
            )));
        }
    }
    Ok(
        json!({"submitted":packets.len(),"direction":"fromDevice","incarnation":scope["incarnation"],"generation":scope["generation"]}),
    )
}
