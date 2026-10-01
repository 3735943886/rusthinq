//! Plain loopback lifecycle/persistence harness; production admission must use TLS.
use rusthinq_app::{lifecycle_storage::Storage, runtime::Runtime};
use rusthinq_server::{Config, Server};
use std::{io, path::Path, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: thinq1_lifecycle ADDRESS DEVICE_LEDGER_JSON".into());
    }
    let path = args[1].clone();
    let (storage, generations) = tokio::task::spawn_blocking(move || {
        let mut storage = Storage::open(Path::new(&path), 256)?;
        let generations = storage.reserve_generations(1_000_000)?;
        Ok::<_, io::Error>((storage, generations))
    })
    .await??;
    let mut server = Server::new(Config {
        generation_floor: generations.floor,
        generation_ceiling: generations.ceiling,
        ..Config::default()
    })
    .map_err(|error| io::Error::other(format!("server config: {error:?}")))?;
    let runtime = Runtime::new(storage, server.handle(), Duration::from_secs(10), 256)?
        .with_generation_refill(1_000_000, 10_000)?;
    let mut events = runtime.handle().subscribe();
    let listener = TcpListener::bind(&args[0]).await?;
    eprintln!(
        "ThinQ1 lifecycle listener: {} (press Enter to stop)",
        listener.local_addr()?
    );
    let (app_stop, stopped) = watch::channel(false);
    let mut task = tokio::spawn(runtime.run(stopped));
    let mut app_result = None;
    let (stop, mut requested) = oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        let _ = stop.send(());
    });
    let result = loop {
        tokio::select! {
            _ = &mut requested => break Ok(()),
            result = &mut task => {
                app_result = Some(result);
                break Err(io::Error::other("lifecycle runtime stopped"));
            },
            accepted = listener.accept() => match accepted {
                Ok((stream,_)) => {if let Err(error) = server.admit(stream) {eprintln!("admission: {error:?}");}},
                Err(error) => break Err(error),
            },
            event = events.recv() => eprintln!("app: {event:?}"),
        }
    };
    drop(listener);
    server.shutdown().await;
    app_stop.send_replace(true);
    match app_result {
        Some(result) => result,
        None => task.await,
    }??;
    Ok(result?)
}
