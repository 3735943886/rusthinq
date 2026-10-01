//! Single-name TLS ThinQ1 harness with externally supplied certificate material.
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    Config as SessionConfig, Server,
    tls::{Config, FrontDoor, Identity},
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(args.len() == 4 || args.len() == 5) || args.get(4).is_some_and(|arg| arg != "rtk") {
        return Err("usage: thinq1_tls ADDRESS SNI_NAME CERT_CHAIN_PEM KEY_PEM [rtk]".into());
    }
    let identity = Identity {
        name: args[1].clone(),
        certificate_chain_pem: std::fs::read(&args[2])?,
        private_key_pem: std::fs::read(&args[3])?,
        policy: if args.len() == 5 {
            TlsPolicy::RtkRtl8711am
        } else {
            TlsPolicy::Baseline
        },
    };
    let front = FrontDoor::new(Config::default(), vec![identity], None)?;
    let listener = TcpListener::bind(&args[0]).await?;
    let server = Server::new(SessionConfig::default()).expect("valid configuration");
    let mut events = server.handle().subscribe();
    let mut rejects = front.subscribe();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    eprintln!(
        "ThinQ1 TLS listener: {} (SNI {}; press Enter to stop)",
        listener.local_addr()?,
        args[1]
    );
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        stop.send_replace(true);
    });
    let running = front.serve(listener, server, stopped);
    tokio::pin!(running);
    loop {
        tokio::select! {
            result = &mut running => return Ok(result?),
            event = events.recv() => if let Ok(event) = event { eprintln!("{event:?}"); },
            rejection = rejects.recv() => if let Ok(rejection) = rejection { eprintln!("{rejection:?}"); },
        }
    }
}
