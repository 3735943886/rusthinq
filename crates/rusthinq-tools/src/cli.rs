use crate::{Client, segment};
use serde_json::{Value, json};
use std::{io, path::Path};

const HELP: &str = "rusthinqctl devices|health|mqtt|cloud|cloud-devices
rusthinqctl forget|reload|enable|disable|unpair DEVICE
rusthinqctl pair DEVICE DEVICE_TYPE [ALIAS]
rusthinqctl adopt DEVICE device_DEVICE.json
rusthinqctl set DEVICE PROPERTY VALUE
rusthinqctl send DEVICE JSON
rusthinqctl inject DEVICE HEX --inject-ok [--from-device]
rusthinqctl raw-inject on|off|status
rusthinqctl retained-delete JSON
rusthinqctl capture DEVICE OUTPUT.jsonl [--cloud]
rusthinqctl replay DEVICE CAPTURE.jsonl --inject-ok
packet-parser [-message|-message-raw] HEX
packet-sender DEVICE HEX --inject-ok [--from-device]
rusthinq-capture DEVICE OUTPUT.jsonl [--cloud]
lgcloud-monitor [--inventory]
rusthinqctl cloud-watch
RUSTHINQ_API=http://127.0.0.1:8080/; RUSTHINQ_USER/RUSTHINQ_PASSWORD for authentication.";

fn required(args: &[String], index: usize) -> io::Result<&str> {
    args.get(index).map(String::as_str).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing command argument; use --help",
        )
    })
}
fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

async fn archive(path: &str, id: &str) -> io::Result<Value> {
    use tokio::io::AsyncReadExt;
    let expected = format!("device_{id}.json");
    if Path::new(path).file_name().and_then(|name| name.to_str()) != Some(expected.as_str()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "archive filename must match device id",
        ));
    }
    let mut bytes = Vec::new();
    tokio::fs::File::open(path)
        .await?
        .take(262145)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 262144 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "archived material exceeded",
        ));
    }
    serde_json::from_slice(&bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid archived material JSON",
        )
    })
}

pub async fn run(args: Vec<String>) -> io::Result<()> {
    let mut arguments = args.into_iter();
    let executable = arguments.next().unwrap_or_default();
    let name = Path::new(&executable)
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("rusthinqctl");
    let mut args: Vec<_> = arguments.collect();
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
        || args.is_empty()
    {
        println!("{HELP}");
        return Ok(());
    }
    match name {
        "packet-parser" => {
            let raw = args.first().is_some_and(|arg| arg == "-message-raw");
            if args.first().is_some_and(|arg| arg.starts_with('-')) {
                args.remove(0);
            }
            let text = args.join("");
            let text = if raw { format!("0000{text}") } else { text };
            println!("{}", crate::decode(&json!({"hex":text}))?);
            return Ok(());
        }
        "packet-sender" => args.insert(0, "inject".into()),
        "lgcloud-monitor" => args.insert(
            0,
            if has_flag(&args, "--inventory") {
                "cloud-inventory"
            } else {
                "cloud-watch"
            }
            .into(),
        ),
        "rusthinq-capture" => args.insert(0, "capture".into()),
        "rusthinq-retained-gc" => args.insert(0, "retained-delete".into()),
        _ => {}
    }
    let client = Client::environment()?;
    let command = args[0].as_str();
    let result = match command {
        "devices" | "health" | "mqtt" | "cloud" => {
            client.request(&format!("api/{command}"), None).await?
        }
        "cloud-devices" => client.request("api/cloud/devices", None).await?,
        "cloud-inventory" => client.request("api/cloud/inventory", None).await?,
        "forget" | "reload" | "enable" | "disable" | "unpair" | "pair" => {
            let id = required(&args, 1)?;
            let mut scope = client.scope(id).await?;
            let action = if matches!(command, "enable" | "disable" | "unpair" | "pair") {
                if command == "pair" {
                    scope["deviceType"] = json!(required(&args, 2)?);
                    if let Some(alias) = args.get(3) {
                        scope["alias"] = json!(alias);
                    }
                }
                format!("bridge/{command}")
            } else {
                command.to_owned()
            };
            client
                .request(
                    &format!("api/devices/{}/{action}", segment(id)),
                    Some(&scope),
                )
                .await?
        }
        "set" => {
            let id = required(&args, 1)?;
            let mut scope = client.scope(id).await?;
            scope["function"] = json!("__command");
            scope["input"] =
                json!(json!({"prop":required(&args, 2)?,"value":required(&args, 3)?}).to_string());
            client
                .request(&format!("api/devices/{}/invoke", segment(id)), Some(&scope))
                .await?
        }
        "send" => {
            let id = required(&args, 1)?;
            let mut scope = client.scope(id).await?;
            scope["payload"] = json!(required(&args, 2)?);
            client
                .request(&format!("api/devices/{}/send", segment(id)), Some(&scope))
                .await?
        }
        "inject" => {
            client
                .inject(&json!({
                    "device_id": required(&args, 1)?,
                    "hex": required(&args, 2)?,
                    "inject_ok": has_flag(&args, "--inject-ok"),
                    "from_device": has_flag(&args, "--from-device"),
                }))
                .await?
        }
        "raw-inject" => match args.get(1).map(String::as_str) {
            None | Some("status") => client.request("api/raw-inject", None).await?,
            Some("on") => {
                client
                    .request("api/raw-inject", Some(&json!({"enabled":true})))
                    .await?
            }
            Some("off") => {
                client
                    .request("api/raw-inject", Some(&json!({"enabled":false})))
                    .await?
            }
            Some(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "raw-inject on|off|status",
                ));
            }
        },
        "retained-delete" => {
            let body: Value =
                serde_json::from_str(required(&args, 1)?).map_err(io::Error::other)?;
            client
                .request("api/mqtt/retained/delete", Some(&body))
                .await?
        }
        "adopt" => {
            let id = required(&args, 1)?;
            let archive = archive(required(&args, 2)?, id).await?;
            let mut scope = client.scope(id).await?;
            scope["archive"] = archive;
            scope["archiveDevice"] = json!(id);
            client
                .request(
                    &format!("api/devices/{}/bridge/adopt", segment(id)),
                    Some(&scope),
                )
                .await?
        }
        "replay" => {
            crate::replay::replay(
                &client,
                required(&args, 1)?,
                Path::new(required(&args, 2)?),
                has_flag(&args, "--inject-ok"),
            )
            .await?
        }
        "cloud-watch" => {
            crate::watch_cloud(client).await?;
            json!({"observed":true})
        }
        "capture" => {
            crate::capture_with_cloud(
                client,
                required(&args, 1)?.into(),
                Path::new(required(&args, 2)?),
                has_flag(&args, "--cloud"),
            )
            .await?;
            json!({"captured":true})
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown command; use --help",
            ));
        }
    };
    println!("{result}");
    Ok(())
}
