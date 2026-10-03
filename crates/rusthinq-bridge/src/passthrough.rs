//! L4 raw TLS relay. L3 owns the returned future and its cancellation/join.
use crate::firmware::{Error, Hosts};
use rusthinq_server::tls::{Passthrough, PassthroughFuture, PrefixedStream};
use serde_json::Value;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{io::copy_bidirectional, net::TcpStream, sync::Semaphore, time::timeout};

pub type ConnectFuture = Pin<Box<dyn Future<Output = io::Result<TcpStream>> + Send>>;

/// Destination policy belongs to L4. This future must not spawn detached work.
pub trait Connector: Send + Sync {
    fn connect(&self, name: String) -> ConnectFuture;
}

/// Resolve the selected SNI name and connect to its real HTTPS endpoint.
pub struct HttpsConnector;
impl Connector for HttpsConnector {
    fn connect(&self, name: String) -> ConnectFuture {
        Box::pin(async move { crate::resolver::connect_tcp(&name, 443).await })
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub host_capacity: usize,
    pub max_connections: usize,
    pub connect_timeout: Duration,
    /// Absolute transfer bound, not an idle timeout. Expiry never retries bytes.
    pub transfer_timeout: Option<Duration>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            host_capacity: 256,
            max_connections: 64,
            connect_timeout: Duration::from_secs(10),
            transfer_timeout: None,
        }
    }
}

#[derive(Clone)]
pub struct Relay {
    inner: Arc<State>,
}
struct State {
    config: Config,
    hosts: Mutex<Hosts>,
    epoch: Instant,
    slots: Arc<Semaphore>,
    connector: Arc<dyn Connector>,
}
impl Relay {
    pub fn new(config: Config, connector: Arc<dyn Connector>) -> io::Result<Self> {
        if config.host_capacity == 0
            || config.max_connections == 0
            || config.max_connections > Semaphore::MAX_PERMITS
            || config.connect_timeout.is_zero()
            || config.transfer_timeout.is_some_and(|value| value.is_zero())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid relay configuration",
            ));
        }
        Ok(Self {
            inner: Arc::new(State {
                hosts: Mutex::new(Hosts::new(config.host_capacity).map_err(policy_error)?),
                slots: Arc::new(Semaphore::new(config.max_connections)),
                config,
                epoch: Instant::now(),
                connector,
            }),
        })
    }

    fn with_hosts<T>(&self, f: impl FnOnce(&mut Hosts, u64) -> Result<T, Error>) -> io::Result<T> {
        let mut hosts = self
            .inner
            .hosts
            .lock()
            .map_err(|_| io::Error::other("host policy poisoned"))?;
        let now = u64::try_from(self.inner.epoch.elapsed().as_millis())
            .map_err(|_| policy_error(Error::Clock))?;
        f(&mut hosts, now).map_err(policy_error)
    }

    /// Diagnostics advances expiry without renewing suspected leases.
    pub fn snapshot(
        &self,
    ) -> io::Result<std::collections::BTreeMap<String, crate::firmware::Evidence>> {
        self.with_hosts(|hosts, now| hosts.snapshot(now))
    }
    pub fn learn_command(&self, payload: &Value) -> io::Result<()> {
        self.with_hosts(|hosts, now| hosts.learn_command(payload, now))
    }
    pub fn confirm_local(&self, host: &str) -> io::Result<()> {
        self.with_hosts(|hosts, now| hosts.confirm_local(host, now))
    }
    pub fn protect_local_endpoints(&self, payload: &Value) -> io::Result<()> {
        self.with_hosts(|hosts, now| hosts.protect_local_endpoints(payload, now))
    }
    /// Explicit suspicion from a URL. Refused unlearned SNI names are suspected by `relay`.
    pub fn suspect(&self, download_url: &str) -> io::Result<()> {
        self.with_hosts(|hosts, now| hosts.suspect(download_url, now))
    }
}

impl Passthrough for Relay {
    fn relay(&self, name: String, mut stream: PrefixedStream) -> PassthroughFuture {
        let relay = self.clone();
        Box::pin(async move {
            let _permit = relay
                .inner
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "relay capacity"))?;
            if !relay.with_hosts(|hosts, now| hosts.route_or_suspect(&name, now))? {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "unlearned or local host",
                ));
            }
            let name = name.trim_end_matches('.').to_ascii_lowercase();
            let mut remote = timeout(
                relay.inner.config.connect_timeout,
                relay.inner.connector.connect(name),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "relay connect timeout"))??;
            let transfer = copy_bidirectional(&mut stream, &mut remote);
            if let Some(limit) = relay.inner.config.transfer_timeout {
                timeout(limit, transfer).await.map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "relay transfer timeout")
                })??;
            } else {
                transfer.await?;
            }
            Ok(())
        })
    }
}

fn policy_error(error: Error) -> io::Error {
    io::Error::new(
        match error {
            Error::Capacity => io::ErrorKind::WouldBlock,
            Error::InvalidHost | Error::PayloadExceeded => io::ErrorKind::InvalidInput,
            Error::Clock => io::ErrorKind::Other,
        },
        format!("firmware host policy: {error:?}"),
    )
}
