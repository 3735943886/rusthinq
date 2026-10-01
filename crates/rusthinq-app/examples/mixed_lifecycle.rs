//! Mixed plain-loopback harness. Production admission requires TLS/provisioning.
use rusthinq_app::{lifecycle_storage::Storage, runtime::Runtime};
use rusthinq_server::{
    Config, Server,
    mqtt::{Broker, SystemClock},
};
use std::sync::Arc;
use std::{io, path::Path, time::Duration};
use tokio::task::JoinSet;
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: mixed_lifecycle THINQ1_ADDRESS MQTT_ADDRESS DEVICE_LEDGER_JSON".into());
    }
    let path = args[2].clone();
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
    let broker = Broker::sharing(server.handle(), Arc::new(SystemClock));
    let mut mqtt_tasks = JoinSet::new();
    let runtime = Runtime::new(storage, server.handle(), Duration::from_secs(10), 256)?
        .with_generation_refill(1_000_000, 10_000)?;
    let mut events = runtime.handle().subscribe();
    let listener = TcpListener::bind(&args[0]).await?;
    let mqtt_listener = TcpListener::bind(&args[1]).await?;
    eprintln!("MQTT lifecycle listener: {}", mqtt_listener.local_addr()?);
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
    let mut result = loop {
        tokio::select! {
            _ = &mut requested => break Ok(()),
            result = &mut task => {
                app_result = Some(result);
                break Err(io::Error::other("lifecycle runtime stopped"));
            },
            accepted = mqtt_listener.accept() => match accepted {
                Ok((stream,_)) => {
                    if mqtt_tasks.len() >= 256 {drop(stream); continue;}
                    let broker = broker.clone();
                    mqtt_tasks.spawn(async move {broker.run(stream).await});
                },
                Err(error) => break Err(error),
            },
            completed = mqtt_tasks.join_next(), if !mqtt_tasks.is_empty() => {
                match completed.expect("MQTT task present") {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => eprintln!("MQTT admission/transport: {error}"),
                    Err(error) => break Err(io::Error::other(error)),
                }
            },
            accepted = listener.accept() => match accepted {
                Ok((stream,_)) => {if let Err(error) = server.admit(stream) {eprintln!("admission: {error:?}");}},
                Err(error) => break Err(error),
            },
            event = events.recv() => eprintln!("app: {event:?}"),
        }
    };
    drop(listener);
    drop(mqtt_listener);
    server.shutdown().await;
    while let Some(completed) = mqtt_tasks.join_next().await {
        match completed {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("MQTT shutdown/admission: {error}"),
            Err(error) => {
                result = Err(io::Error::other(error));
            }
        }
    }
    app_stop.send_replace(true);
    match app_result {
        Some(result) => result,
        None => task.await,
    }??;
    Ok(result?)
}
