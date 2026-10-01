//! HTTPS provisioning harness with an explicitly loaded persistent CA.
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    certificates::{Authority, Config as SignConfig, Signer},
    provisioning::{Config, Service},
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{path::Path, sync::Arc};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(args.len() == 4 || args.len() == 5) || args.get(4).is_some_and(|arg| arg != "rtk") {
        return Err("usage: provisioning ADDRESS SNI_NAME CA_CERT_PEM CA_KEY_PEM [rtk]".into());
    }
    let ca = Arc::new(Authority::load(Path::new(&args[2]), Path::new(&args[3]))?);
    let policy = if args.len() == 5 {
        TlsPolicy::RtkRtl8711am
    } else {
        TlsPolicy::Baseline
    };
    let identity = ca.server_identity(&args[1], policy)?;
    let (signer, handle) = Signer::new(ca.clone(), SignConfig::default())?;
    let listener = TcpListener::bind(&args[0]).await?;
    let address = listener.local_addr()?;
    let mut config = Config::new(args[1].clone());
    config.https_port = address.port();
    let service = Arc::new(Service::new(config, &ca, handle, None)?);
    let front = FrontDoor::new(TlsConfig::default(), vec![identity], None)?;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let signer_task = tokio::spawn(signer.run(stopped.clone()));
    let interactive_stop = stop.clone();
    eprintln!(
        "Provisioning HTTPS listener: {address} (SNI {}; press Enter to stop)",
        args[1]
    );
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        interactive_stop.send_replace(true);
    });
    let result = front.serve_service(listener, service, stopped).await;
    stop.send_replace(true);
    signer_task.await?;
    Ok(result?)
}
