//! One caller-owned cloud session. Bounded channels, original downlinks and no replay.
use base64::{Engine, engine::general_purpose::STANDARD};
use rusthinq_protocol::{mqtt, thinq1};
use serde_json::{Value, json};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout},
};

const MAX: usize = 1_000_000;
const DEADLINE: Duration = Duration::from_secs(15);

pub struct Uplink {
    pub payload: Vec<u8>,
    /// Complete socket write only. This does not report device/cloud acknowledgement.
    pub result: oneshot::Sender<io::Result<()>>,
}
pub enum Event {
    Ready,
    Downlink {
        payload: Vec<u8>,
        result: oneshot::Sender<bool>,
    },
}
#[derive(Clone, Debug)]
pub struct Identity {
    pub device: String,
    pub model: String,
}
impl Identity {
    fn validate(&self) -> io::Result<()> {
        for value in [&self.device, &self.model] {
            if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid cloud session input")
}
async fn write<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> io::Result<()> {
    timeout(DEADLINE, async {
        stream.write_all(bytes).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cloud write timed out"))?
}
async fn downlink(events: &mpsc::Sender<Event>, payload: Vec<u8>) -> io::Result<()> {
    let (result, received) = oneshot::channel();
    events
        .try_send(Event::Downlink { payload, result })
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "cloud downlink capacity exceeded",
            )
        })?;
    match timeout(DEADLINE, received).await {
        Ok(Ok(true)) => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "cloud downlink not delivered",
        )),
    }
}
/// Owns cancellation and drops both socket halves before returning. An admitted
/// partial write is never replayed by this session or its reconnecting owner.
pub async fn thinq1<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    identity: Identity,
    mut uplinks: mpsc::Receiver<Uplink>,
    events: mpsc::Sender<Event>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    identity.validate()?;
    if *stop.borrow() {
        return Ok(());
    }
    let operation = async {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut buffer = Vec::new();
        let mut chunk = [0; 8192];
        let mut partial = None;
        let mut alive = tokio::time::interval(Duration::from_secs(60));
        alive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut counter = 0_u64;
        let mut active = false;
        let mut latest = None;
        events.try_send(Event::Ready).map_err(|_| invalid())?;
        loop {
            tokio::select! {
                _=tokio::time::sleep_until(partial.unwrap_or_else(||Instant::now()+Duration::from_secs(300))),if partial.is_some()=>return Err(io::Error::new(io::ErrorKind::TimedOut,"RTI partial frame timed out")),
                _=alive.tick()=>{
                    counter=counter.checked_add(1).ok_or_else(invalid)?;
                    let payload=json!({"Header":{"x-lgedm-deviceId":identity.device},"Body":{"CmdWId":format!("alive-{counter}"),"Cmd":"Alive"}}).to_string();
                    write(&mut writer,&thinq1::encode(payload.as_bytes(),MAX).map_err(|_|invalid())?).await?;
                },
                uplink=uplinks.recv()=>{
                    let Some(uplink)=uplink else {return Ok(());};
                    let result=(||{
                        if uplink.payload.len()>MAX {return Err(invalid());}
                        let value:Value=serde_json::from_slice(&uplink.payload).map_err(|_|invalid())?;
                        let body=&value["Body"];
                        if body["Format"]!="B64" {return Err(invalid());}
                        let data=STANDARD.decode(body["Data"].as_str().ok_or_else(invalid)?).map_err(|_|invalid())?;
                        Ok(json!({"Header":{"x-lgedm-deviceId":identity.device},"Body":{"CmdWId":format!("n-{}",identity.device),"ReturnCode":"0000","Format":"B64","Data":STANDARD.encode(data)}}).to_string().into_bytes())
                    })();
                    match result {
                        Err(error)=>{let _=uplink.result.send(Err(error));},
                        Ok(payload)=>{
                            latest=Some(payload.clone());
                            if active {
                                let sent=write(&mut writer,&thinq1::encode(&payload,MAX).map_err(|_|invalid())?).await;
                                let failed=sent.is_err();let _=uplink.result.send(sent);if failed{return Err(io::Error::other("RTI uplink failed"));}
                            } else {let _=uplink.result.send(Err(io::Error::new(io::ErrorKind::WouldBlock,"RTI monitoring stopped; latest status saved")));}
                        }
                    }
                },
                read=reader.read(&mut chunk)=>{
                    let count=read?;
                    if count==0{return Err(io::Error::new(if buffer.is_empty(){io::ErrorKind::ConnectionReset}else{io::ErrorKind::UnexpectedEof},"RTI closed"));}
                    if buffer.is_empty(){partial=Some(Instant::now()+DEADLINE);}
                    buffer.extend_from_slice(&chunk[..count]);
                    loop {
                        if buffer.len()<4 {break;}
                        let size=i32::from_be_bytes(buffer[..4].try_into().expect("header"));
                        if size<0 || size as usize>MAX {return Err(invalid());}
                        let size=size as usize+4;if buffer.len()<size {break;}
                        let payload=buffer[4..size].to_vec();buffer.drain(..size);
                        let value:Value=serde_json::from_slice(&payload).map_err(|_|invalid())?;
                        if !value.is_object() || value.get("Header").and_then(|h|h.get("x-lgedm-deviceId")).is_some_and(|id|id.as_str()!=Some(identity.device.as_str())) {return Err(invalid());}
                        match value["Body"]["CmdOpt"].as_str() {
                            Some("Start")=>{active=true;if let Some(payload)=&latest {write(&mut writer,&thinq1::encode(payload,MAX).map_err(|_|invalid())?).await?;}},
                            Some("Stop")=>active=false,
                            _=>{
                                downlink(&events,payload).await?;
                                if value["Body"].get("ReturnCode").is_none() && let Some(id)=value["Body"].get("CmdWId") {
                                    let ack=json!({"Header":{"x-lgedm-deviceId":identity.device},"Body":{"CmdWId":id,"ReturnCode":"0000"}}).to_string();
                                    write(&mut writer,&thinq1::encode(ack.as_bytes(),MAX).map_err(|_|invalid())?).await?;
                                }
                            }
                        }
                        partial=if buffer.is_empty(){None}else{Some(Instant::now()+DEADLINE)};
                    }
                }
            }
        }
    };
    tokio::select! {biased;_=stop.changed()=>Ok(()),result=operation=>result}
}

#[derive(Clone, Debug)]
pub struct MqttConfig {
    pub identity: Identity,
    pub publish: String,
    pub provisioning: String,
    pub subscribe: String,
    /// Actual local deploy data, not guessed firmware/protocol placeholders.
    pub deploy: Value,
}
fn string(body: &mut Vec<u8>, value: &str) -> io::Result<()> {
    if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
        return Err(invalid());
    }
    body.extend_from_slice(&(value.len() as u16).to_be_bytes());
    body.extend_from_slice(value.as_bytes());
    Ok(())
}
fn packet(header: u8, body: &[u8]) -> io::Result<Vec<u8>> {
    mqtt::frame(header, body, MAX + 4096).map_err(|_| invalid())
}
fn publication(topic: &str, payload: &[u8], id: Option<u16>) -> io::Result<Vec<u8>> {
    if topic.contains(['#', '+']) || payload.len() > MAX {
        return Err(invalid());
    }
    let mut body = Vec::new();
    string(&mut body, topic)?;
    if let Some(id) = id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    packet(if id.is_some() { 0x32 } else { 0x30 }, &body)
}
fn relayable(cmd: &str) -> bool {
    !matches!(
        cmd,
        "undeploy" | "deploy" | "preDeploy" | "completeProvisioning" | "completeProvisioning_ack"
    ) && !cmd.is_empty()
}
/// MQTT 3.1.1 clean session. QoS1 subscription and provisioning; transient ordered
/// uplinks use QoS0. Retained downlinks and duplicate command replay are rejected.
/// Provisioning/keepalive/partial-frame deadlines are bounded. No retained output.
pub async fn thinq2<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    config: MqttConfig,
    mut uplinks: mpsc::Receiver<Uplink>,
    events: mpsc::Sender<Event>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    config.identity.validate()?;
    if !mqtt::valid_filter(&config.subscribe)
        || config.deploy.to_string().len() > MAX
        || !config.deploy["data"]["appInfo"].is_object()
        || !config.deploy["data"]["platformInfo"].is_object()
    {
        return Err(invalid());
    }
    let mut connect = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60];
    string(&mut connect, &config.identity.device)?;
    let mut sub = vec![0, 1];
    string(&mut sub, &config.subscribe)?;
    sub.push(1);
    let mut pre = config.deploy.clone();
    pre["cmd"] = json!("preDeploy");
    pre["did"] = json!(config.identity.device);
    pre["kind"] = json!(config.identity.model);
    if *stop.borrow() {
        return Ok(());
    }
    let operation = async {
        timeout(DEADLINE, async {
            write(&mut stream, &packet(0x10, &connect)?).await?;
            let mut ack = [0; 4];
            stream.read_exact(&mut ack).await?;
            if ack != [0x20, 2, 0, 0] {
                return Err(invalid());
            }
            write(&mut stream, &packet(0x82, &sub)?).await?;
            let mut ack = [0; 5];
            stream.read_exact(&mut ack).await?;
            if !matches!(ack, [0x90, 3, 0, 1, 0 | 1]) {
                return Err(invalid());
            }
            write(
                &mut stream,
                &publication(&config.provisioning, pre.to_string().as_bytes(), Some(2))?,
            )
            .await?;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cloud MQTT handshake timed out"))??;
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut buffer = Vec::new();
        let mut chunk = [0; 8192];
        let mut partial = None;
        let mut ping = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut pong = None;
        let mut provision_ack = false;
        let mut completed = false;
        let mut ready = false;
        let setup = Instant::now() + DEADLINE;
        let mut counter = 10000_u64;
        let mut seen = std::collections::VecDeque::new();
        loop {
            tokio::select! {
                _=tokio::time::sleep_until(setup),if !ready=>return Err(io::Error::new(io::ErrorKind::TimedOut,"cloud provisioning timed out")),
                _=tokio::time::sleep_until(partial.unwrap_or(setup)),if partial.is_some()=>return Err(io::Error::new(io::ErrorKind::TimedOut,"MQTT partial frame timed out")),
                _=tokio::time::sleep_until(pong.unwrap_or(setup)),if pong.is_some()=>return Err(io::Error::new(io::ErrorKind::TimedOut,"cloud PINGRESP timed out")),
                _=ping.tick()=>{write(&mut writer,&[0xc0,0]).await?;pong=Some(Instant::now()+DEADLINE);},
                uplink=uplinks.recv(),if ready=>{
                    let Some(uplink)=uplink else{return Ok(());};
                    let result=(||{
                        if uplink.payload.len()>MAX{return Err(invalid());}
                        let mut value:Value=serde_json::from_slice(uplink.payload.strip_suffix(&[0]).unwrap_or(&uplink.payload)).map_err(|_|invalid())?;
                        if value["did"]!=config.identity.device || !relayable(value["cmd"].as_str().unwrap_or_default()){return Err(invalid());}
                        counter=counter.checked_add(1).ok_or_else(invalid)?;
                        value["mid"]=json!(counter);value["kind"]=json!(config.identity.model);
                        publication(&config.publish,value.to_string().as_bytes(),None)
                    })();
                    match result {
                        Err(error)=>{let _=uplink.result.send(Err(error));},
                        Ok(bytes)=>{let result=write(&mut writer,&bytes).await;let failed=result.is_err();let _=uplink.result.send(result);if failed{return Err(io::Error::other("cloud uplink failed"));}}
                    }
                },
                read=reader.read(&mut chunk)=>{
                    let count=read?;if count==0{return Err(io::Error::new(io::ErrorKind::ConnectionReset,"cloud MQTT closed"));}
                    if buffer.is_empty(){partial=Some(Instant::now()+DEADLINE);}buffer.extend_from_slice(&chunk[..count]);
                    while let Some(size)=mqtt::length(&buffer,MAX+4096).map_err(|_|invalid())? {
                        if buffer.len()<size{break;}
                        let bytes:Vec<_>=buffer.drain(..size).collect();
                        match bytes[0] {
                            0x40 if bytes==[0x40,2,0,2]=>provision_ack=true,
                            0xd0 if bytes==[0xd0,0]=>{if pong.take().is_none(){return Err(invalid());}},
                            header if header>>4==3=>{
                                let mqtt::Packet::Publish{topic,payload,id,qos,duplicate}=mqtt::decode(&bytes,MAX+4096).map_err(|_|invalid())? else{return Err(invalid());};
                                if header&1!=0 || qos>1 || payload.len()>MAX || !mqtt::matches(&config.subscribe,&topic){return Err(invalid());}
                                let fingerprint=openssl::sha::sha256(&payload);
                                if duplicate && !seen.contains(&(id,fingerprint)){return Err(invalid());}
                                if !duplicate {
                                    let value:Value=serde_json::from_slice(payload.strip_suffix(&[0]).unwrap_or(&payload)).map_err(|_|invalid())?;
                                    if value["did"]!=config.identity.device{return Err(invalid());}
                                    match value["cmd"].as_str().ok_or_else(invalid)? {
                                        "completeProvisioning"=>{
                                            counter=counter.checked_add(1).ok_or_else(invalid)?;
                                            let ack=json!({"mid":counter,"did":config.identity.device,"kind":config.identity.model,"cmd":"completeProvisioning_ack","rssi":-48,"fs":"idle","data":null,"type":1});
                                            write(&mut writer,&publication(&config.publish,ack.to_string().as_bytes(),None)?).await?;completed=true;
                                        },
                                        cmd if relayable(cmd) && ready=>downlink(&events,payload).await?,
                                        cmd if !relayable(cmd)=>{},
                                        _=>return Err(invalid()),
                                    }
                                }
                                if !duplicate {seen.retain(|(prior,_)|*prior!=id);if seen.len()==64{seen.pop_front();}seen.push_back((id,fingerprint));}
                                if let Some(id)=id{write(&mut writer,&[0x40,2,(id>>8)as u8,id as u8]).await?;}
                            },
                            _=>return Err(invalid()),
                        }
                        if !ready && provision_ack && completed {events.try_send(Event::Ready).map_err(|_|invalid())?;ready=true;}
                        partial=if buffer.is_empty(){None}else{Some(Instant::now()+DEADLINE)};
                    }
                }
            }
        }
    };
    tokio::select! {biased;_=stop.changed()=>Ok(()),result=operation=>result}
}
