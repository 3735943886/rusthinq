//! Optional MQTT output adapter. Retained ownership precedes every wire publish.
use crate::{
    api::AppHandle as Application,
    cleanup_mqtt::Session,
    lifecycle_cleanup,
    retained_cleanup::Ledger,
    scripts::{Context, PublishSink},
};
use openssl::ssl::{SslConnector, SslMethod};
pub(crate) trait Connection:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send
{
}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Connection for T {}
pub(crate) type Transport = Box<dyn Connection>;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{Notify, mpsc, oneshot, watch},
    time::{Instant, timeout},
};

#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub ca: Option<PathBuf>,
    pub client: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub inventory: PathBuf,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalMqtt")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("credentials", &"<redacted>")
            .field("inventory", &self.inventory)
            .finish()
    }
}
impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if !(rusthinq_protocol::client_hello::hostname(&self.host)
            || self.host.parse::<std::net::IpAddr>().is_ok())
            || self.port == 0
            || self.client.is_empty()
            || self.client.len() > 1024
            || self.client.chars().any(char::is_control)
            || self
                .username
                .as_ref()
                .is_some_and(|name| name.len() > 1024 || name.contains('\0'))
            || self
                .password
                .as_ref()
                .is_some_and(|value| value.len() > 4096)
            || self.password.is_some() && self.username.is_none()
            || self.inventory.file_name().is_none()
            || self.ca.is_some() && !self.tls
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid external MQTT configuration",
            ));
        }
        Ok(())
    }
    pub(crate) async fn connect(&self) -> io::Result<Session<Transport>> {
        let config = self.clone();
        let connector = tokio::task::spawn_blocking(move || -> io::Result<Option<SslConnector>> {
            if !config.tls {
                return Ok(None);
            }
            let mut connector =
                SslConnector::builder(SslMethod::tls_client()).map_err(io::Error::other)?;
            if let Some(ca) = config.ca {
                connector.set_ca_file(ca).map_err(io::Error::other)?;
            } else {
                connector
                    .set_default_verify_paths()
                    .map_err(io::Error::other)?;
            }
            Ok(Some(connector.build()))
        })
        .await
        .map_err(io::Error::other)??;
        timeout(Duration::from_secs(10), async {
            let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
            let stream: Transport = if let Some(connector) = connector {
                let ssl = connector
                    .configure()
                    .map_err(io::Error::other)?
                    .into_ssl(&self.host)
                    .map_err(io::Error::other)?;
                let mut tls =
                    tokio_openssl::SslStream::new(ssl, stream).map_err(io::Error::other)?;
                Pin::new(&mut tls)
                    .connect()
                    .await
                    .map_err(io::Error::other)?;
                Box::new(tls)
            } else {
                Box::new(stream)
            };
            Session::connect_authenticated(
                stream,
                &self.client,
                Duration::from_secs(10),
                self.username.as_deref(),
                self.password.as_deref(),
            )
            .await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "external MQTT connect timed out"))?
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Connecting,
    Connected,
    Failed(String),
    Stopped,
}
#[derive(Clone)]
struct Publication {
    retired: bool,
    context: Context,
    owner: String,
    topic: String,
    payload: String,
    version: u64,
}
struct Cache {
    values: BTreeMap<String, Publication>,
    bytes: usize,
    next: u64,
}
enum Removal {
    Topic {
        owner: String,
        topic: String,
        result: oneshot::Sender<io::Result<usize>>,
    },
    Owner {
        owner: String,
        result: oneshot::Sender<io::Result<usize>>,
    },
    All {
        result: oneshot::Sender<io::Result<usize>>,
    },
}
struct Shared {
    cache: Mutex<Cache>,
    notify: Notify,
    transient: mpsc::Sender<Publication>,
    removals: mpsc::Sender<Removal>,
    status: watch::Sender<Status>,
    flushed: watch::Sender<u64>,
    /// Non-retained publications discarded because no broker connection existed.
    dropped: AtomicU64,
}
#[derive(Clone)]
pub struct Handle(Arc<Shared>);
pub struct Runtime {
    config: Config,
    shared: Arc<Shared>,
    transient: mpsc::Receiver<Publication>,
    removals: mpsc::Receiver<Removal>,
}
pub fn new(config: Config) -> io::Result<(Handle, Runtime)> {
    config.validate()?;
    let (transient, receiver) = mpsc::channel(128);
    let (removals, removal_receiver) = mpsc::channel(16);
    let shared = Arc::new(Shared {
        cache: Mutex::new(Cache {
            values: BTreeMap::new(),
            bytes: 0,
            next: 0,
        }),
        notify: Notify::new(),
        transient,
        removals,
        status: watch::channel(Status::Connecting).0,
        flushed: watch::channel(0).0,
        dropped: AtomicU64::new(0),
    });
    Ok((
        Handle(shared.clone()),
        Runtime {
            config,
            shared,
            transient: receiver,
            removals: removal_receiver,
        },
    ))
}
impl Handle {
    /// Wait for admitted output to be confirmed or fenced away. Caller bounds the wait.
    pub async fn flush(&self) -> io::Result<()> {
        let mut flushed = self.0.flushed.subscribe();
        let mut status = self.status();
        let target = self.0.cache.lock().unwrap_or_else(|e| e.into_inner()).next;
        loop {
            if *flushed.borrow() >= target {
                return Ok(());
            }
            if matches!(*status.borrow(), Status::Stopped) {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "MQTT adapter stopped before flush",
                ));
            }
            tokio::select! {
                result=flushed.changed()=>{result.map_err(io::Error::other)?;},
                result=status.changed()=>{result.map_err(io::Error::other)?;},
            }
        }
    }
    pub fn status(&self) -> watch::Receiver<Status> {
        self.0.status.subscribe()
    }
    /// Non-retained publications discarded while disconnected, since the adapter started.
    pub fn dropped_transient(&self) -> u64 {
        self.0.dropped.load(Ordering::Relaxed)
    }
    pub async fn delete_topic(&self, owner: String, topic: String) -> io::Result<usize> {
        let (result, received) = oneshot::channel();
        self.remove(Removal::Topic {
            owner,
            topic,
            result,
        })?;
        received.await.map_err(io::Error::other)?
    }
    pub async fn delete_owner(&self, owner: String) -> io::Result<usize> {
        let (result, received) = oneshot::channel();
        self.remove(Removal::Owner { owner, result })?;
        received.await.map_err(io::Error::other)?
    }
    pub async fn delete_all(&self) -> io::Result<usize> {
        let (result, received) = oneshot::channel();
        self.remove(Removal::All { result })?;
        received.await.map_err(io::Error::other)?
    }
    fn remove(&self, removal: Removal) -> io::Result<()> {
        self.0
            .removals
            .try_send(removal)
            .map_err(|error| io::Error::new(io::ErrorKind::WouldBlock, error.to_string()))
    }
}
impl Handle {
    fn publish(&self, context: &Context, payload: String, retired: bool) -> Result<(), String> {
        if matches!(*self.0.status.borrow(), Status::Stopped) {
            return Err("MQTT adapter stopped".into());
        }
        let message: Value = serde_json::from_str(&payload).map_err(|error| error.to_string())?;
        let topic = message["topic"]
            .as_str()
            .filter(|topic| {
                !topic.is_empty()
                    && topic.len() <= 1024
                    && !topic.contains(['#', '+'])
                    && !topic.chars().any(char::is_control)
            })
            .ok_or("invalid publication topic")?
            .to_string();
        let payload = message["payload"]
            .as_str()
            .filter(|value| value.len() <= 1_048_576)
            .ok_or("invalid publication payload")?
            .to_string();
        let retain = message["retain"]
            .as_bool()
            .ok_or("retain boolean required")?;
        let owner = lifecycle_cleanup::device_owner(&context.device, context.session.incarnation)
            .map_err(|error| error.to_string())?;
        let mut cache = self.0.cache.lock().unwrap_or_else(|e| e.into_inner());
        let version = cache
            .next
            .checked_add(1)
            .ok_or("publication sequence exhausted")?;
        let message = Publication {
            retired,
            context: context.clone(),
            owner,
            topic: topic.clone(),
            payload,
            version,
        };
        if retain {
            let previous = cache.values.get(&topic).map_or(0, |value| {
                value.topic.len() + value.payload.len() + value.owner.len()
            });
            let bytes = cache
                .bytes
                .saturating_sub(previous)
                .checked_add(message.topic.len() + message.payload.len() + message.owner.len())
                .filter(|bytes| *bytes <= 33_554_432)
                .ok_or("retained cache byte budget exceeded")?;
            if !cache.values.contains_key(&topic) && cache.values.len() >= 16384 {
                return Err("retained topic capacity exceeded".into());
            }
            cache.values.insert(topic, message);
            cache.bytes = bytes;
        } else if !matches!(*self.0.status.borrow(), Status::Connected) {
            // No subscriber can receive a non-retained message without a broker; replaying
            // it after reconnect would deliver a stale occurrence. Count the loss instead of
            // failing the script's remaining outputs.
            self.0.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        } else {
            self.0
                .transient
                .try_send(message)
                .map_err(|error| error.to_string())?;
        }
        cache.next = version;
        drop(cache);
        self.0.notify.notify_one();
        Ok(())
    }
}
impl PublishSink for Handle {
    fn try_publish(&self, context: &Context, payload: String) -> Result<(), String> {
        self.publish(context, payload, false)
    }
    fn try_publish_retired(&self, context: &Context, payload: String) -> Result<(), String> {
        self.publish(context, payload, true)
    }
}
fn valid_scope(app: &Application, message: &Publication) -> bool {
    if message.retired {
        return app.retired_script(&message.context.device).as_ref() == Some(&message.context)
            && app.snapshot().iter().any(|device| {
                device.entry.id == message.context.device
                    && device.entry.incarnation == message.context.session.incarnation
                    && device.entry.last_generation == message.context.session.generation
                    && device.session.is_none()
                    && device.removal.is_none()
            });
    }
    app.snapshot().iter().any(|device| {
        device.entry.id == message.context.device
            && device.session == Some(message.context.session)
            && device.online
            && device.removal.is_none()
    }) && app
        .script_states()
        .get(&message.context.device)
        .is_some_and(|(session, generation, _)| {
            *session == message.context.session && *generation == message.context.generation
        })
}
fn current(app: &Application, message: &Publication) -> bool {
    valid_scope(app, message)
        && app.durable_devices().iter().any(|entry| {
            entry.id == message.context.device
                && entry.incarnation == message.context.session.incarnation
        })
}
impl Runtime {
    pub async fn run(
        mut self,
        app: impl Into<Application>,
        mut stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let app = app.into();
        let mut delay = 1u64;
        while !*stop.borrow() {
            self.shared.status.send_replace(Status::Connecting);
            let config = self.config.clone();
            let opened =
                tokio::task::spawn_blocking(move || Ledger::open(&config.inventory, 16384))
                    .await
                    .map_err(io::Error::other)?;
            let result = match opened {
                Err(error) => Err(error),
                Ok(ledger) => match self.config.connect().await {
                    Ok(session) => self.connected(&app, session, ledger, &mut stop).await,
                    Err(error) => Err(error),
                },
            };
            match result {
                Ok(()) => break,
                Err(error) => {
                    self.shared
                        .status
                        .send_replace(Status::Failed(error.to_string()));
                    while self.transient.try_recv().is_ok() {
                        self.shared.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(delay))=>{}}
            delay = (delay * 2).min(30);
        }
        self.transient.close();
        self.removals.close();
        while let Ok(removal) = self.removals.try_recv() {
            let result = match removal {
                Removal::Topic { result, .. }
                | Removal::Owner { result, .. }
                | Removal::All { result } => result,
            };
            let _ = result.send(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "adapter stopped",
            )));
        }
        self.shared.status.send_replace(Status::Stopped);
        Ok(())
    }
    async fn connected(
        &mut self,
        app: &Application,
        mut session: Session<Transport>,
        mut ledger: Ledger,
        stop: &mut watch::Receiver<bool>,
    ) -> io::Result<()> {
        let mut events = app.adapter_events(); // before recovery snapshot
        let mut versions = BTreeMap::new();
        let mut scopes = BTreeMap::<String, Context>::new();
        let mut ping = Instant::now() + Duration::from_secs(30);
        self.shared.status.send_replace(Status::Connected);
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            if let Ok(removal) = self.removals.try_recv() {
                ledger = self.remove(&mut session, ledger, removal, stop).await?;
            }
            let devices = app.snapshot();
            let mut recover = ledger.requested();
            for item in ledger.pending() {
                if lifecycle_cleanup::parse_owner(&item.owner).is_some_and(|(id, incarnation)| {
                    !devices.iter().any(|device| {
                        device.entry.id == id && device.entry.incarnation == incarnation
                    })
                }) && !recover.iter().any(|old| old.topic == item.topic)
                {
                    recover.push(item);
                }
            }
            ledger = Session::<Transport>::request_deletions(ledger, recover.clone()).await?;
            for deletion in recover {
                if *stop.borrow() {
                    return Ok(());
                }
                ledger = session.delete_one(ledger, deletion.topic.clone()).await?;
                versions.remove(&deletion.topic);
            }
            let pending: Vec<_> = {
                let mut cache = self.shared.cache.lock().unwrap_or_else(|e| e.into_inner());
                cache.values.retain(|_, value| valid_scope(app, value));
                cache.bytes = cache
                    .values
                    .values()
                    .map(|value| value.topic.len() + value.payload.len() + value.owner.len())
                    .sum();
                cache
                    .values
                    .values()
                    .filter(|value| {
                        current(app, value) && versions.get(&value.topic) != Some(&value.version)
                    })
                    .take(16)
                    .cloned()
                    .collect()
            };
            if let Ok(message) = self.transient.try_recv()
                && current(app, &message)
            {
                session
                    .publish_transient(&message.topic, message.payload.as_bytes())
                    .await?;
            }
            let progress = !pending.is_empty();
            for message in pending {
                if *stop.borrow() {
                    return Ok(());
                }
                if !current(app, &message) {
                    continue;
                }
                if !message.retired && scopes.get(&message.owner) != Some(&message.context) {
                    // Remove only what the current scope no longer publishes. A reconnect
                    // (fresh `scopes`) must not clear and re-create the device's state.
                    let live: std::collections::BTreeSet<String> = self
                        .shared
                        .cache
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .values
                        .values()
                        .filter(|value| {
                            value.owner == message.owner && value.context == message.context
                        })
                        .map(|value| value.topic.clone())
                        .collect();
                    let old: Vec<_> = ledger
                        .pending()
                        .into_iter()
                        .filter(|item| item.owner == message.owner && !live.contains(&item.topic))
                        .collect();
                    ledger = Session::<Transport>::request_deletions(ledger, old.clone()).await?;
                    for item in old {
                        if *stop.borrow() {
                            return Ok(());
                        }
                        ledger = session.delete_one(ledger, item.topic).await?;
                    }
                    scopes.insert(message.owner.clone(), message.context.clone());
                    versions.retain(|topic, _| {
                        self.shared
                            .cache
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .values
                            .get(topic)
                            .is_none_or(|value| value.owner != message.owner)
                    });
                }
                // A durable inventory write can span removal/replacement; check again afterwards.
                let owner = message.owner.clone();
                let topic = message.topic.clone();
                ledger = tokio::task::spawn_blocking(move || {
                    let mut ledger = ledger;
                    ledger.inventory_topic(owner, topic)?;
                    Ok::<_, io::Error>(ledger)
                })
                .await
                .map_err(io::Error::other)??;
                if !current(app, &message) {
                    continue;
                }
                if message.payload.is_empty() {
                    ledger = session.delete_one(ledger, message.topic.clone()).await?;
                } else {
                    session
                        .publish_registered_retained(
                            &ledger,
                            &message.owner,
                            &message.topic,
                            message.payload.as_bytes(),
                        )
                        .await?;
                }
                versions.insert(message.topic, message.version);
            }
            if progress {
                continue;
            }
            if self.transient.is_empty() {
                let cache = self.shared.cache.lock().unwrap_or_else(|e| e.into_inner());
                if cache
                    .values
                    .values()
                    .all(|value| versions.get(&value.topic) == Some(&value.version))
                {
                    self.shared.flushed.send_replace(cache.next);
                }
            }
            tokio::select! {
                _=stop.changed()=>return Ok(()),
                _=self.shared.notify.notified()=>{},
                _=tokio::time::sleep(Duration::from_secs(1))=>{
                    if Instant::now()>=ping {session.ping().await?;ping=Instant::now()+Duration::from_secs(30);}
                },
                event=events.recv()=>match event {Ok(_)|Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>{},Err(tokio::sync::broadcast::error::RecvError::Closed)=>return Ok(())},
                Some(message)=self.transient.recv()=>{
                    if current(app,&message) {session.publish_transient(&message.topic,message.payload.as_bytes()).await?;}
                },
                Some(removal)=self.removals.recv()=>{
                    ledger = self.remove(&mut session,ledger,removal,stop).await?;
                }
            }
        }
    }
    async fn remove(
        &mut self,
        session: &mut Session<Transport>,
        mut ledger: Ledger,
        removal: Removal,
        stop: &watch::Receiver<bool>,
    ) -> io::Result<Ledger> {
        let (owner, topic, result) = match removal {
            Removal::Topic {
                owner,
                topic,
                result,
            } => (Some(owner), Some(topic), result),
            Removal::Owner { owner, result } => (Some(owner), None, result),
            Removal::All { result } => (None, None, result),
        };
        let matches = |item_owner: &str, item_topic: &str| {
            owner.as_deref().is_none_or(|owner| owner == item_owner)
                && topic.as_deref().is_none_or(|topic| topic == item_topic)
        };
        if ledger.pending().iter().any(|item| {
            topic.as_deref() == Some(item.topic.as_str())
                && owner.as_deref() != Some(item.owner.as_str())
        }) {
            let _ = result.send(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "retained topic owner changed",
            )));
            return Ok(ledger);
        }
        let topics: Vec<_> = ledger
            .pending()
            .into_iter()
            .filter(|item| matches(&item.owner, &item.topic))
            .collect();
        {
            let mut cache = self.shared.cache.lock().unwrap_or_else(|e| e.into_inner());
            cache
                .values
                .retain(|_, value| !matches(&value.owner, &value.topic));
            cache.bytes = cache
                .values
                .values()
                .map(|value| value.topic.len() + value.payload.len() + value.owner.len())
                .sum();
        }
        ledger = Session::<Transport>::request_deletions(ledger, topics.clone()).await?;
        for deletion in &topics {
            if *stop.borrow() {
                let _ = result.send(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "cleanup durably pending after shutdown",
                )));
                return Ok(ledger);
            }
            match session.delete_one(ledger, deletion.topic.clone()).await {
                Ok(saved) => ledger = saved,
                Err(error) => {
                    let _ = result.send(Err(io::Error::other(error.to_string())));
                    return Err(error);
                }
            }
        }
        let _ = result.send(Ok(topics.len()));
        Ok(ledger)
    }
}
