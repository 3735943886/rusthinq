//! Local ThinQ1 TLS plus explicitly seeded firmware HTTPS passthrough.
use rusthinq_bridge::passthrough::{Config, HttpsConnector, Relay};
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    Server,
    certificates::Authority,
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{path::Path, sync::Arc};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5 {
        return Err(
            "usage: firmware_tls ADDRESS LOCAL_SNI CA_CERT_PEM CA_KEY_PEM DOWNLOAD_URL".into(),
        );
    }
    let ca = Authority::load(Path::new(&args[2]), Path::new(&args[3]))?;
    let identity = ca.server_identity(&args[1], TlsPolicy::Baseline)?;
    let relay = Relay::new(Config::default(), Arc::new(HttpsConnector))?;
    relay.confirm_local(&args[1])?;
    // Explicit operator seed for this harness; the daemon must use real downlink commands.
    relay.learn_command(&serde_json::json!(args[4]))?;
    let front = FrontDoor::new(TlsConfig::default(), vec![identity], Some(Arc::new(relay)))?;
    let listener = TcpListener::bind(&args[0]).await?;
    eprintln!(
        "TLS listener: {} (press Enter to stop)",
        listener.local_addr()?
    );
    let (stop, stopped) = tokio::sync::watch::channel(false);
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        stop.send_replace(true);
    });
    Ok(front
        .serve(
            listener,
            Server::new(Default::default()).map_err(|error| {
                std::io::Error::other(format!("server configuration: {error:?}"))
            })?,
            stopped,
        )
        .await?)
}
