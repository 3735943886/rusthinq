//! L6 ownership of mixed TLS listeners, transport joins and lifecycle shutdown.
use crate::{
    lifecycle_storage::Storage,
    runtime::{Handle, Runtime},
};
use rusthinq_server::{
    Config, Server,
    mqtt::{Broker, Clock},
    tls::{FrontDoor, LocalService},
};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    task::JoinSet,
};

pub struct Service {
    server: Server,
    broker: Broker,
    runtime: Runtime,
    local: Vec<(FrontDoor, TcpListener, Arc<dyn LocalService>)>,
}
impl Service {
    /// Config must use a previously committed generation block. No CA files are
    /// created or overwritten here. The caller prepares exact TLS identities.
    pub fn new(
        storage: Storage,
        config: Config,
        clock: Arc<dyn Clock>,
        grace: Duration,
        events: usize,
        refill: Option<(u64, u64)>,
    ) -> io::Result<Self> {
        let server = Server::new(config)
            .map_err(|error| io::Error::other(format!("server config: {error:?}")))?;
        let broker = Broker::sharing(server.handle(), clock);
        let mut runtime =
            Runtime::new_mixed(storage, server.handle(), broker.handle(), grace, events)?;
        if let Some((count, low_water)) = refill {
            runtime = runtime.with_generation_refill(count, low_water)?;
        }
        Ok(Self {
            server,
            broker,
            runtime,
            local: Vec::new(),
        })
    }
    #[cfg(feature = "bridge")]
    pub async fn with_cloud_devices(
        mut self,
        path: std::path::PathBuf,
        account: crate::cloud_account::Handle,
        firmware: rusthinq_bridge::passthrough::Relay,
    ) -> io::Result<(Self, crate::cloud_devices::Runtime)> {
        let (handle, cloud, bridge) = crate::cloud_devices::Runtime::open(
            path,
            account,
            self.handle(),
            self.server.handle(),
            self.broker.handle(),
            firmware,
        )
        .await?;
        self.handle().attach_cloud_devices(handle.clone())?;
        self.runtime =
            self.runtime
                .with_bridge(bridge, Arc::new(handle), Duration::from_secs(90))?;
        Ok((self, cloud))
    }
    pub fn handle(&self) -> Handle {
        self.runtime.handle()
    }
    #[cfg(feature = "scripting")]
    pub fn with_scripts(mut self, owner: crate::scripts::Owner) -> Self {
        self.runtime = self.runtime.with_scripts(owner);
        self
    }
    #[cfg(feature = "scripting")]
    pub fn with_script_sink(mut self, sink: Arc<dyn crate::scripts::PublishSink>) -> Self {
        self.runtime = self.runtime.with_script_sink(sink);
        self
    }
    pub fn with_external_mqtt(mut self, handle: crate::external_mqtt::Handle) -> Self {
        self.runtime = self.runtime.with_external_mqtt(handle);
        self
    }
    #[cfg(feature = "scripting")]
    pub fn with_drivers(mut self, config: crate::drivers::Config) -> io::Result<Self> {
        self.runtime = self.runtime.with_drivers(config)?;
        Ok(self)
    }
    /// Add bounded L3 HTTPS services (e.g. ThinQ1 metadata or provisioning).
    pub fn mqtt_diagnostics(
        &self,
    ) -> tokio::sync::broadcast::Receiver<rusthinq_server::MqttDiagnostic> {
        self.server.handle().mqtt_diagnostics()
    }
    pub fn with_local_service(
        mut self,
        front: FrontDoor,
        listener: TcpListener,
        service: Arc<dyn LocalService>,
    ) -> io::Result<Self> {
        if self.local.len() >= 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many local TLS services",
            ));
        }
        self.local.push((front, listener, service));
        Ok(self)
    }
    #[cfg(feature = "bridge")]
    pub fn with_firmware(mut self, relay: rusthinq_bridge::passthrough::Relay) -> Self {
        self.runtime = self.runtime.with_firmware(relay);
        self
    }
    pub fn with_metadata(
        mut self,
        receiver: mpsc::Receiver<rusthinq_server::thinq1_http::Metadata>,
    ) -> Self {
        self.runtime = self.runtime.with_metadata(receiver);
        self
    }
    /// Any listener/runtime termination stops admission on both protocols.
    /// All front-door tasks and lifecycle workers are awaited before returning.
    pub async fn serve(
        self,
        thinq1: (FrontDoor, TcpListener),
        mqtt: (FrontDoor, TcpListener),
        stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        self.serve_listeners(Some(thinq1), Some(mqtt), stop).await
    }
    /// As `serve`; an absent listener is simply not served (0.1 port without `bind`).
    pub async fn serve_listeners(
        self,
        thinq1: Option<(FrontDoor, TcpListener)>,
        mqtt: Option<(FrontDoor, TcpListener)>,
        mut stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let Self {
            server,
            broker,
            runtime,
            local,
        } = self;
        let (front_stop, front_stopped) = crate::task::Shutdown::new(false);
        let (app_stop, app_stopped) = crate::task::Shutdown::new(false);
        let mut app = crate::task::OwnedTask::spawn(runtime.run(app_stopped));
        let mut app_result = None;
        let mut fronts = JoinSet::new();
        // Dropping the transport server stops the state it shares with the broker, so an
        // unserved ThinQ1 listener keeps it alive until the fronts are joined.
        let mut unserved = None;
        if let Some((front, listener)) = thinq1 {
            fronts.spawn(front.serve(listener, server, front_stopped.clone()));
        } else {
            unserved = Some(server);
        }
        if let Some((front, listener)) = mqtt {
            fronts.spawn(front.serve_service(listener, Arc::new(broker.clone()), front_stopped));
        }
        for (front, listener, service) in local {
            fronts.spawn(front.serve_service(listener, service, front_stop.subscribe()));
        }
        let mut failure = None;
        if !*stop.borrow() {
            loop {
                tokio::select! {
                    _=stop.changed()=>{
                        if *stop.borrow() || stop.has_changed().is_err() {break;}
                    }
                    result=&mut app=>{
                        app_result=Some(result);
                        failure=Some(io::Error::other("lifecycle runtime stopped"));
                        break;
                    }
                    Some(result)=fronts.join_next(), if !fronts.is_empty()=>{
                        failure=Some(match result {
                            Ok(Err(error))=>error,
                            Err(error)=>io::Error::other(error),
                            Ok(Ok(()))=>io::Error::other("TLS listener stopped unexpectedly"),
                        });
                        break;
                    }
                }
            }
        }
        broker.stop();
        front_stop.stop();
        while let Some(result) = fronts.join_next().await {
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) => Some(io::Error::other(error)),
            };
            if failure.is_none() {
                failure = error;
            }
        }
        if let Some(server) = unserved {
            server.shutdown().await;
        }
        app_stop.stop();
        let app_result = match app_result {
            Some(result) => result,
            None => app.await,
        };
        match app_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
            Err(error) => {
                if failure.is_none() {
                    failure = Some(io::Error::other(error));
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
