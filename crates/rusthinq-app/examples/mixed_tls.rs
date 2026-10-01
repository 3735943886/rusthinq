//! Mixed lifecycle TLS harness using an explicitly loaded persistent CA.
use rusthinq_app::{lifecycle_storage::Storage, tls_runtime::Service};
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    Config,
    certificates::Authority,
    mqtt::SystemClock,
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{io, path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 7 {
        return Err("usage: mixed_tls THINQ1_ADDRESS MQTT_ADDRESS THINQ1_SNI MQTT_SNI CA_CERT_PEM CA_KEY_PEM DEVICE_LEDGER_JSON".into());
    }
    let setup = args.clone();
    let (storage, block, thin_identity, mqtt_identity) = tokio::task::spawn_blocking(move || {
        let ca = Authority::load(Path::new(&setup[4]), Path::new(&setup[5]))?;
        let thin_identity = ca.server_identity(&setup[2], TlsPolicy::Baseline)?;
        let mqtt_identity = ca.server_identity(&setup[3], TlsPolicy::Baseline)?;
        let mut storage = Storage::open(Path::new(&setup[6]), 256)?;
        let block = storage.reserve_generations(1_000_000)?;
        Ok::<_, io::Error>((storage, block, thin_identity, mqtt_identity))
    })
    .await??;
    let thin_front = FrontDoor::new(TlsConfig::default(), vec![thin_identity], None)?;
    let mqtt_front = FrontDoor::new(TlsConfig::default(), vec![mqtt_identity], None)?;
    let thin_listener = TcpListener::bind(&args[0]).await?;
    let mqtt_listener = TcpListener::bind(&args[1]).await?;
    eprintln!(
        "ThinQ1 TLS: {}; MQTT TLS: {} (press Enter to stop)",
        thin_listener.local_addr()?,
        mqtt_listener.local_addr()?
    );
    let service = Service::new(
        storage,
        Config {
            generation_floor: block.floor,
            generation_ceiling: block.ceiling,
            ..Config::default()
        },
        Arc::new(SystemClock),
        Duration::from_secs(10),
        256,
        Some((1_000_000, 10_000)),
    )?;
    let mut events = service.handle().subscribe();
    let (stop, stopped) = watch::channel(false);
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        stop.send_replace(true);
    });
    let serving = service.serve(
        (thin_front, thin_listener),
        (mqtt_front, mqtt_listener),
        stopped,
    );
    tokio::pin!(serving);
    loop {
        tokio::select! {result=&mut serving=>return Ok(result?),event=events.recv()=>eprintln!("app: {event:?}")}
    }
}
