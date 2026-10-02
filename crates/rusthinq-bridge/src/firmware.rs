//! Bounded firmware/SOTA host evidence. The owner supplies monotonic milliseconds.
use rusthinq_protocol::client_hello::hostname;
use serde_json::Value;
use std::collections::BTreeMap;

const INITIAL_TTL: u64 = 60_000;
const RENEWED_TTL: u64 = 600_000;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_URL: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    Local,
    Command,
    Suspected { expires_at: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidHost,
    Capacity,
    PayloadExceeded,
    Clock,
}

#[derive(Debug, Clone)]
pub struct Hosts {
    capacity: usize,
    now: u64,
    entries: BTreeMap<String, Evidence>,
}

impl Hosts {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::Capacity);
        }
        Ok(Self {
            capacity,
            now: 0,
            entries: BTreeMap::new(),
        })
    }

    fn advance(&mut self, now: u64) -> Result<(), Error> {
        if now < self.now || now > u64::MAX - RENEWED_TTL {
            return Err(Error::Clock);
        }
        self.now = now;
        self.entries.retain(|_, evidence| !matches!(evidence, Evidence::Suspected { expires_at } if *expires_at <= now));
        Ok(())
    }

    fn record(&mut self, host: String, evidence: Evidence) -> Result<(), Error> {
        if self.entries.get(&host) == Some(&Evidence::Local) {
            return Ok(());
        }
        if self.entries.get(&host) == Some(&Evidence::Command)
            && matches!(evidence, Evidence::Suspected { .. })
        {
            return Ok(());
        }
        if !self.entries.contains_key(&host) && self.entries.len() == self.capacity {
            return Err(Error::Capacity);
        }
        self.entries.insert(host, evidence);
        Ok(())
    }

    /// Use only after successful local TLS/provisioning or for explicit local policy.
    pub fn confirm_local(&mut self, host: &str, now: u64) -> Result<(), Error> {
        let host = normalize(host)?;
        self.advance(now)?;
        self.record(host, Evidence::Local)
    }

    /// A TLS failure is weak evidence; callers must classify the failure first.
    /// An unrecognized/non-HTTP URL is ignored. No network operation occurs here.
    pub fn suspect(&mut self, download_url: &str, now: u64) -> Result<(), Error> {
        self.advance(now)?;
        if let Some(host) = url_host(download_url, false) {
            self.record(
                host,
                Evidence::Suspected {
                    expires_at: now + INITIAL_TTL,
                },
            )?;
        }
        Ok(())
    }

    /// Learn only from actual cloud-to-device commands, never arbitrary local data.
    /// Host admission is atomic: an exceeded payload or full registry learns nothing.
    pub fn learn_command(&mut self, payload: &Value, now: u64) -> Result<(), Error> {
        self.learn(payload, now, false)
    }

    /// Use only after successful local provisioning. Also accepts ssl:// endpoints.
    pub fn protect_local_endpoints(&mut self, payload: &Value, now: u64) -> Result<(), Error> {
        self.learn(payload, now, true)
    }

    fn learn(&mut self, payload: &Value, now: u64, local: bool) -> Result<(), Error> {
        self.advance(now)?;
        let mut staged = self.clone();
        let mut stack = vec![(payload, 0)];
        let mut nodes = 0;
        while let Some((value, depth)) = stack.pop() {
            nodes += 1;
            if nodes > MAX_NODES || depth > MAX_DEPTH {
                return Err(Error::PayloadExceeded);
            }
            match value {
                Value::String(value) => {
                    if value.len() > MAX_URL {
                        return Err(Error::PayloadExceeded);
                    }
                    if let Some(host) = url_host(value, local) {
                        staged.record(
                            host,
                            if local {
                                Evidence::Local
                            } else {
                                Evidence::Command
                            },
                        )?;
                    }
                }
                Value::Array(values) => {
                    if values.len() > MAX_NODES - nodes
                        || stack.len() + values.len() > MAX_NODES - nodes
                    {
                        return Err(Error::PayloadExceeded);
                    }
                    stack.extend(values.iter().map(|value| (value, depth + 1)));
                }
                Value::Object(values) => {
                    if values.len() > MAX_NODES - nodes
                        || stack.len() + values.len() > MAX_NODES - nodes
                    {
                        return Err(Error::PayloadExceeded);
                    }
                    stack.extend(values.values().map(|value| (value, depth + 1)));
                }
                _ => {}
            }
        }
        self.entries = staged.entries;
        Ok(())
    }

    /// A suspected host's live routing hit renews its lease to ten minutes.
    pub fn route(&mut self, host: &str, now: u64) -> Result<bool, Error> {
        let host = normalize(host)?;
        self.advance(now)?;
        match self.entries.get_mut(&host) {
            Some(Evidence::Command) => Ok(true),
            Some(Evidence::Suspected { expires_at }) => {
                *expires_at = now + RENEWED_TTL;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Route, or refuse and suspect a host with no evidence, so the appliance's retry
    /// passes through (0.1 suspected `https://{sni}/` after a failed local handshake).
    /// Local proof is never weakened.
    pub fn route_or_suspect(&mut self, host: &str, now: u64) -> Result<bool, Error> {
        if self.route(host, now)? {
            return Ok(true);
        }
        let host = normalize(host)?;
        if !self.entries.contains_key(&host) {
            self.record(
                host,
                Evidence::Suspected {
                    expires_at: now + INITIAL_TTL,
                },
            )?;
        }
        Ok(false)
    }

    /// Diagnostics does not renew leases. Expired entries are removed.
    pub fn snapshot(&mut self, now: u64) -> Result<BTreeMap<String, Evidence>, Error> {
        self.advance(now)?;
        Ok(self.entries.clone())
    }
}

fn normalize(host: &str) -> Result<String, Error> {
    if host.len() > 254 {
        return Err(Error::InvalidHost);
    }
    let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    if !hostname(&host) {
        return Err(Error::InvalidHost);
    }
    Ok(host)
}

fn url_host(value: &str, local: bool) -> Option<String> {
    if value.len() > MAX_URL {
        return None;
    }
    let parsed = url::Url::parse(value).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") && !(local && parsed.scheme() == "ssl") {
        return None;
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return None;
    }
    normalize(parsed.host_str()?).ok()
}
