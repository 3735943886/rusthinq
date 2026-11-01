//! 0.2 device runtime entry point. Stops and joins all services on Ctrl-C.
use rusthinq_app::{
    daemon::{Config, Daemon},
    logging,
};
use std::{io, path::PathBuf};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // The message, not the Debug form `main` would print for an io::Error.
            eprintln!("rusthinq: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> io::Result<()> {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--version")
    {
        println!("rusthinq {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--backup")
    {
        return backup();
    }
    logging::init()?;
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
    tracing::info!(endpoints = ?daemon.endpoints(), "device listeners started");
    if let Some(address) = daemon.management_endpoint() {
        tracing::info!(%address, "management listener started");
    }
    let serving = daemon.serve(stopped);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    tokio::pin!(serving);
    loop {
        tokio::select! {
            result = &mut serving => {
                if let Err(error) = &result { tracing::error!(%error, "daemon failed"); }
                return result;
            },
            signal = &mut shutdown => {
                tracing::info!("shutdown requested");
                stop.send_replace(true);
                let result = serving.await;
                if let Err(error) = &result { tracing::error!(%error, "shutdown failed"); }
                else { tracing::info!("daemon stopped"); }
                signal?;
                return result;
            }
            event = events.recv() => match event {
                Ok(event) => logging::runtime_event(&event),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(lost)) => tracing::warn!(lost, "application log events lost"),
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return serving.await,
            }
        }
    }
}

/// `rusthinq --backup CONFIG.toml OUTPUT.tar`: the archive `GET /api/backup` serves, read
/// from the files themselves, so it works with the daemon stopped (`thinqctl backup`).
fn backup() -> io::Result<()> {
    let usage = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: rusthinq --backup CONFIG.toml OUTPUT.tar",
        )
    };
    let args: Vec<_> = std::env::args_os().skip(2).collect();
    let [config, output] = args.as_slice() else {
        return Err(usage());
    };
    let archive = Config::backup_source(config.as_ref())?.archive(env!("CARGO_PKG_VERSION"))?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(output).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot create {}: {error}", output.to_string_lossy()),
        )
    })?;
    std::io::Write::write_all(&mut file, &archive)
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
