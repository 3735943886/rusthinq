//! Plain loopback capture/replay harness; production admission must supply TLS.
use rusthinq_server::{Config, Server};
use tokio::net::TcpListener;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:5501".into());
    let listener = TcpListener::bind(&address).await?;
    let mut server = Server::new(Config::default()).expect("valid configuration");
    let mut events = server.handle().subscribe();
    eprintln!(
        "ThinQ1 replay listener: {} (press Enter to stop)",
        listener.local_addr()?
    );
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        let _ = stop.send(());
    });
    loop {
        tokio::select! {
            _ = &mut stopped => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                if let Err(error) = server.admit(stream) { eprintln!("admission: {error:?}"); }
            }
            event = events.recv() => eprintln!("{event:?}"),
        }
    }
    drop(listener);
    server.shutdown().await;
    Ok(())
}
