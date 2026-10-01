//! ThinQ1 HTTPS harness using an explicitly loaded CA.
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    certificates::Authority,
    mqtt::SystemClock,
    thinq1_http::{Config, Service},
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{path::Path, sync::Arc};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(args.len() == 4 || args.len() == 5) || args.get(4).is_some_and(|arg| arg != "rtk") {
        return Err("usage: thinq1_http ADDRESS SNI_NAME CA_CERT_PEM CA_KEY_PEM [rtk]".into());
    }
    let ca = Authority::load(Path::new(&args[2]), Path::new(&args[3]))?;
    let policy = if args.len() == 5 {
        TlsPolicy::RtkRtl8711am
    } else {
        TlsPolicy::Baseline
    };
    let identity = ca.server_identity(&args[1], policy)?;
    let (service, mut metadata) = Service::new(Config::default(), Arc::new(SystemClock))?;
    let listener = TcpListener::bind(&args[0]).await?;
    eprintln!(
        "ThinQ1 HTTPS listener: {} (SNI {}; press Enter to stop)",
        listener.local_addr()?,
        args[1]
    );
    let front = FrontDoor::new(TlsConfig::default(), vec![identity], None)?;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        stop.send_replace(true);
    });
    let serving = front.serve_service(listener, Arc::new(service), stopped);
    tokio::pin!(serving);
    loop {
        tokio::select! {
            result = &mut serving => return Ok(result?),
            Some(observation) = metadata.recv() => eprintln!("metadata: {observation:?}"),
        }
    }
}
