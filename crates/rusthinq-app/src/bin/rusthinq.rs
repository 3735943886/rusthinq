//! 0.2 device runtime entry point. Stops and joins all services on Ctrl-C.
use rusthinq_app::daemon::{Config, Daemon};
use std::{io, path::PathBuf};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().map(PathBuf::from).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "usage: rusthinq CONFIG.toml")
    })?;
    if args.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: rusthinq CONFIG.toml",
        ));
    }
    let config = tokio::task::spawn_blocking(move || Config::load(&path))
        .await
        .map_err(io::Error::other)??;
    let daemon = Daemon::prepare(config).await?;
    let mut events = daemon.handle().subscribe();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    eprintln!("device endpoints: {:?}", daemon.endpoints());
    if let Some(address) = daemon.management_endpoint() {
        eprintln!("management endpoint: http://{address}");
    }
    let serving = daemon.serve(stopped);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    tokio::pin!(serving);
    loop {
        tokio::select! {
            result = &mut serving => return result,
            signal = &mut shutdown => {
                stop.send_replace(true);
                let result = serving.await;
                signal?;
                return result;
            }
            event = events.recv() => { eprintln!("app: {event:?}"); }
        }
    }
}

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
