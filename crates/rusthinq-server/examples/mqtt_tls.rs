//! Local ThinQ2 MQTT/TLS harness using an explicitly loaded persistent CA.
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    Config,
    certificates::Authority,
    mqtt::{Broker, SystemClock},
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{path::Path, sync::Arc};
use tokio::net::TcpListener;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(args.len() == 4 || args.len() == 5) || args.get(4).is_some_and(|arg| arg != "rtk") {
        return Err("usage: mqtt_tls ADDRESS SNI_NAME CA_CERT_PEM CA_KEY_PEM [rtk]".into());
    }
    let ca = Authority::load(Path::new(&args[2]), Path::new(&args[3]))?;
    let policy = if args.len() == 5 {
        TlsPolicy::RtkRtl8711am
    } else {
        TlsPolicy::Baseline
    };
    let front = FrontDoor::new(
        TlsConfig::default(),
        vec![ca.server_identity(&args[1], policy)?],
        None,
    )?;
    let broker =
        Broker::new(Config::default(), Arc::new(SystemClock)).expect("valid configuration");
    let mut events = broker.handle().subscribe();
    let listener = TcpListener::bind(&args[0]).await?;
    eprintln!(
        "ThinQ2 MQTT/TLS listener: {} (SNI {}; press Enter to stop)",
        listener.local_addr()?,
        args[1]
    );
    let (stop, stopped) = tokio::sync::watch::channel(false);
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        stop.send_replace(true);
    });
    let serving = front.serve_service(listener, Arc::new(broker.clone()), stopped);
    tokio::pin!(serving);
    loop {
        tokio::select! {result=&mut serving=>{broker.stop();return Ok(result?);},event=events.recv()=>if let Ok(event)=event{eprintln!("{event:?}");}}
    }
}
