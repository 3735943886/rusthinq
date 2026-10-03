use crate::{Client, segment};
use serde_json::{Value, json};
use std::{io, path::Path};
pub async fn run(mut args: Vec<String>) -> io::Result<()> {
    let name = args.remove(0);
    let name = Path::new(&name)
        .file_stem()
        .and_then(|n| n.to_str())
        .unwrap_or("rusthinqctl");
    if args.first().is_some_and(|a| a == "--help" || a == "-h") || args.is_empty() {
        println!(
            "rusthinqctl devices|health|mqtt|cloud|cloud-devices\nrusthinqctl forget|reload|enable|disable|unpair DEVICE\nrusthinqctl pair DEVICE DEVICE_TYPE [ALIAS]\nrusthinqctl adopt DEVICE device_DEVICE.json\nrusthinqctl set DEVICE PROPERTY VALUE\nrusthinqctl send DEVICE JSON\nrusthinqctl inject DEVICE HEX --inject-ok [--from-device]\nrusthinqctl raw-inject on|off|status\nrusthinqctl retained-delete JSON\nrusthinqctl capture DEVICE OUTPUT.jsonl\nrusthinqctl replay DEVICE CAPTURE.jsonl --inject-ok\npacket-parser [-message|-message-raw] HEX\npacket-sender DEVICE HEX --inject-ok [--from-device]\nrusthinq-capture DEVICE OUTPUT.jsonl [--cloud]\nlgcloud-monitor [--inventory]\nrusthinqctl cloud-watch\nRUSTHINQ_API=http://127.0.0.1:8080/; RUSTHINQ_USER/RUSTHINQ_PASSWORD for authentication."
        );
        return Ok(());
    }
    match name {
        "packet-parser" => {
            let raw = args.first().is_some_and(|a| a == "-message-raw");
            if args.first().is_some_and(|a| a.starts_with('-')) {
                args.remove(0);
            }
            let text = args.join("");
            let text = if raw { format!("0000{text}") } else { text };
            println!("{}", crate::decode(&json!({"hex":text}))?);
            return Ok(());
        }
        "packet-sender" => args.insert(0, "inject".into()),
        "rusthinq-capture" => args.insert(0, "capture".into()),
        "lgcloud-monitor" => {
            if args.first().is_some_and(|v| v == "--inventory") {
                args.remove(0);
                args.insert(0, "cloud-inventory".into());
            } else {
                args.insert(0, "cloud-watch".into());
            }
        }
        "rusthinq-retained-gc" => args.insert(0, "retained-delete".into()),
        _ => {}
    }
    let client = Client::environment()?;
    let required = |i: usize| {
        args.get(i).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing command argument; use --help",
            )
        })
    };
    let result=match args[0].as_str(){
        "devices"=>client.request("api/devices",None).await?,"health"=>client.request("api/health",None).await?,"mqtt"=>client.request("api/mqtt",None).await?,"cloud"=>client.request("api/cloud",None).await?,"cloud-devices"=>client.request("api/cloud/devices",None).await?,"cloud-inventory"=>client.request("api/cloud/inventory",None).await?,
        "forget"|"reload"|"enable"|"disable"|"unpair"|"pair"=>{
            let id=required(1)?;let mut scope=client.scope(&id).await?;
            let path=if matches!(args[0].as_str(),"enable"|"disable"|"unpair"|"pair"){if args[0]=="pair" {scope["deviceType"]=json!(required(2)?);if let Some(alias)=args.get(3){scope["alias"]=json!(alias);}}format!("api/devices/{}/bridge/{}",segment(&id),args[0])}else{format!("api/devices/{}/{}",segment(&id),args[0])};
            client.request(&path,Some(&scope)).await?
        },
        "set"=>{let id=required(1)?;let mut scope=client.scope(&id).await?;scope["function"]=json!("__command");scope["input"]=json!(json!({"prop":required(2)?,"value":required(3)?}).to_string());client.request(&format!("api/devices/{}/invoke",segment(&id)),Some(&scope)).await?},
        "send"=>{let id=required(1)?;let mut scope=client.scope(&id).await?;scope["payload"]=json!(required(2)?);client.request(&format!("api/devices/{}/send",segment(&id)),Some(&scope)).await?},
        "inject"=>client.inject(&json!({"device_id":required(1)?,"hex":required(2)?,"inject_ok":args.iter().any(|a|a=="--inject-ok"),"from_device":args.iter().any(|a|a=="--from-device")})).await?,
        "raw-inject"=>match args.get(1).map(String::as_str) {
            None|Some("status")=>client.request("api/raw-inject",None).await?,
            Some("on")=>client.request("api/raw-inject",Some(&json!({"enabled":true}))).await?,
            Some("off")=>client.request("api/raw-inject",Some(&json!({"enabled":false}))).await?,
            Some(_)=>return Err(io::Error::new(io::ErrorKind::InvalidInput,"raw-inject on|off|status")),
        },
        "retained-delete"=>{let body:Value=serde_json::from_str(&required(1)?).map_err(io::Error::other)?;client.request("api/mqtt/retained/delete",Some(&body)).await?},
        "adopt"=>{
            use std::io::Read;
            let id=required(1)?;let path=required(2)?;
            if Path::new(&path).file_name().and_then(|n|n.to_str())!=Some(format!("device_{id}.json").as_str()){return Err(io::Error::new(io::ErrorKind::InvalidInput,"archive filename must match device id"));}
            let mut bytes=Vec::new();std::fs::File::open(&path)?.take(262145).read_to_end(&mut bytes)?;
            if bytes.len()>262144 {return Err(io::Error::new(io::ErrorKind::InvalidInput,"archived material exceeded"));}
            let archive:Value=serde_json::from_slice(&bytes).map_err(|_|io::Error::new(io::ErrorKind::InvalidInput,"invalid archived material JSON"))?;
            let mut scope=client.scope(&id).await?;scope["archive"]=archive;scope["archiveDevice"]=json!(id);
            client.request(&format!("api/devices/{}/bridge/adopt",segment(&id)),Some(&scope)).await?
        },
        "replay"=>crate::replay::replay(&client,&required(1)?,Path::new(&required(2)?),args.iter().any(|a|a=="--inject-ok")).await?,
        "cloud-watch"=>{crate::watch_cloud(client).await?;json!({"observed":true})},
        "capture"=>{crate::capture_with_cloud(client,required(1)?,Path::new(&required(2)?),args.iter().any(|a|a=="--cloud")).await?;json!({"captured":true})},
        _=>return Err(io::Error::new(io::ErrorKind::InvalidInput,"unknown command; use --help")),
    };
    println!("{result}");
    Ok(())
}
