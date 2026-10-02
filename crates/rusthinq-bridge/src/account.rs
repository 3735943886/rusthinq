//! L4 owns LG authentication, renewal, retry classification and cancellation.
use crate::cloud::Client;
use serde::{Deserialize, Serialize};
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot, watch};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub country: String,
    pub refresh: String,
}
/// Storage is supplied by L6; L4 owns network policy, not checkpoint files.
pub trait CredentialStore: Send + Sync {
    fn credentials(&self) -> Option<Credentials>;
    fn save(
        &self,
        credentials: Option<Credentials>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>>;
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub logged_in: bool,
    pub stored: bool,
    pub country: Option<String>,
    pub busy: bool,
    pub error: Option<String>,
    #[serde(skip)]
    expires: Option<tokio::time::Instant>,
}
#[derive(Debug)]
pub enum Error {
    Busy,
    Stopped,
    InvalidInput,
    Unavailable,
    Remote,
    Storage,
    Cancelled,
    Authentication,
    Rejected,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cloud account: {self:?}")
    }
}
impl std::error::Error for Error {}
enum Action {
    Login(String),
    Complete(String),
    Refresh,
    Logout,
}
struct Command {
    action: Action,
    epoch: u64,
    result: oneshot::Sender<Result<serde_json::Value, Error>>,
}
struct Admission {
    commands: mpsc::Sender<Command>,
    logout: mpsc::Sender<Command>,
    epoch: watch::Sender<u64>,
    client: watch::Sender<Option<Arc<Client>>>,
}
#[derive(Clone)]
pub struct Handle {
    admission: Arc<Mutex<Admission>>,
    status: watch::Receiver<Status>,
    client: watch::Receiver<Option<Arc<Client>>>,
}
pub struct Runtime {
    store: Arc<dyn CredentialStore>,
    commands: mpsc::Receiver<Command>,
    logout: mpsc::Receiver<Command>,
    epoch: watch::Receiver<u64>,
    status: watch::Sender<Status>,
    client: watch::Sender<Option<Arc<Client>>>,
}
pub fn new(store: Arc<dyn CredentialStore>) -> (Handle, Runtime) {
    let credentials = store.credentials();
    let status = Status {
        logged_in: false,
        stored: credentials.is_some(),
        country: credentials.as_ref().map(|c| c.country.clone()),
        busy: false,
        error: None,
        expires: None,
    };
    let (status, watched) = watch::channel(status);
    let (client, clients) = watch::channel(None);
    let (commands, received) = mpsc::channel(8);
    let (logout, urgent) = mpsc::channel(1);
    let (epoch, cancelled) = watch::channel(0);
    (
        Handle {
            admission: Arc::new(Mutex::new(Admission {
                commands,
                logout,
                epoch,
                client: client.clone(),
            })),
            status: watched,
            client: clients,
        },
        Runtime {
            store,
            commands: received,
            logout: urgent,
            epoch: cancelled,
            status,
            client,
        },
    )
}
impl Handle {
    /// Composition-only authenticated client snapshots. Never expose through adapters.
    /// Logout admission cancels existing leases before its checkpoint completes.
    pub fn clients(&self) -> watch::Receiver<Option<Arc<Client>>> {
        self.client.clone()
    }
    pub fn cancellation(&self) -> watch::Receiver<u64> {
        self.admission
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .epoch
            .subscribe()
    }
    pub fn authenticated_client(&self) -> Result<Arc<Client>, Error> {
        let client = self
            .client
            .borrow()
            .clone()
            .filter(|c| c.authenticated())
            .ok_or(Error::Unavailable)?;
        Ok(client)
    }
    /// Read-only cloud inventory, bound to the captured authenticated account lease.
    pub async fn list_devices(&self) -> Result<serde_json::Value, Error> {
        let mut cancelled = self.cancellation();
        let client = self.authenticated_client()?;
        let mut clients = self.clients();
        let account = client
            .account_identity()
            .ok_or(Error::Unavailable)?
            .to_owned();
        let deadline = client
            .expires_at()
            .ok_or(Error::Unavailable)?
            .min(tokio::time::Instant::now() + std::time::Duration::from_secs(30));
        let request = client.list_devices();
        tokio::pin!(request);
        loop {
            tokio::select! { biased;
                _ = cancelled.changed() => return Err(Error::Cancelled),
                _ = tokio::time::sleep_until(deadline) => return Err(Error::Unavailable),
                changed = clients.changed() => {
                    if changed.is_err() || !clients.borrow().as_ref().is_some_and(|c| c.authenticated() && c.account_identity() == Some(account.as_str())) { return Err(Error::Cancelled); }
                }
                result = &mut request => return result.map(|devices|serde_json::json!({"devices":devices})).map_err(|_|Error::Remote),
            }
        }
    }
    pub fn status(&self) -> Status {
        let mut status = self.status.borrow().clone();
        if status
            .expires
            .is_some_and(|expires| tokio::time::Instant::now() >= expires)
        {
            status.logged_in = false;
        }
        status
    }
    async fn request(&self, action: Action) -> Result<serde_json::Value, Error> {
        let (result, reply) = oneshot::channel();
        {
            let admission = self
                .admission
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let previous = *admission.epoch.borrow();
            let urgent = matches!(action, Action::Logout);
            let epoch = if urgent {
                previous.checked_add(1).ok_or(Error::Stopped)?
            } else {
                previous
            };
            let sender = if urgent {
                &admission.logout
            } else {
                &admission.commands
            };
            sender
                .try_send(Command {
                    action,
                    epoch,
                    result,
                })
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => Error::Busy,
                    mpsc::error::TrySendError::Closed(_) => Error::Stopped,
                })?;
            if urgent {
                admission.client.send_replace(None);
                admission.epoch.send_replace(epoch);
            }
        }
        reply.await.map_err(|_| Error::Stopped)?
    }
    pub async fn login(&self, country: String) -> Result<serde_json::Value, Error> {
        if country.len() != 2 || !country.bytes().all(|b| b.is_ascii_uppercase()) {
            return Err(Error::InvalidInput);
        }
        self.request(Action::Login(country)).await
    }
    pub async fn complete(&self, url: String) -> Result<serde_json::Value, Error> {
        code(&url)?;
        self.request(Action::Complete(url)).await
    }
    pub async fn refresh(&self) -> Result<serde_json::Value, Error> {
        self.request(Action::Refresh).await
    }
    pub async fn logout(&self) -> Result<serde_json::Value, Error> {
        self.request(Action::Logout).await
    }
}
fn code(url: &str) -> Result<String, Error> {
    if url.len() > 16384 {
        return Err(Error::InvalidInput);
    }
    let url = url::Url::parse(url).map_err(|_| Error::InvalidInput)?;
    if url.scheme() != "https"
        || url.host_str() != Some("kr.m.lgaccount.com")
        || url.path() != "/login/iabClose"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::InvalidInput);
    }
    let mut codes = url.query_pairs().filter(|(key, _)| key == "code");
    let value = codes.next().ok_or(Error::InvalidInput)?.1.into_owned();
    if codes.next().is_some()
        || value.is_empty()
        || value.len() > 8192
        || value.chars().any(char::is_control)
    {
        return Err(Error::InvalidInput);
    }
    Ok(value)
}
async fn owned_operation<T>(
    operation: impl std::future::Future<Output = Result<T, Error>>,
    stop: &mut watch::Receiver<bool>,
    cancelled: &mut watch::Receiver<u64>,
    epoch: u64,
) -> Result<T, Error> {
    if *stop.borrow() {
        return Err(Error::Stopped);
    }
    if *cancelled.borrow() != epoch {
        return Err(Error::Cancelled);
    }
    tokio::select! {biased; _=stop.changed()=>Err(Error::Stopped),_=cancelled.changed()=>Err(Error::Cancelled),result=operation=>result}
}
fn remote(error: crate::cloud::Error) -> Error {
    use crate::cloud::Error as Cloud;
    match error {
        Cloud::Http(401 | 403) => Error::Authentication,
        Cloud::Network | Cloud::InvalidResponse | Cloud::Http(408 | 429 | 500..=599) => {
            Error::Remote
        }
        _ => Error::Rejected,
    }
}

struct Renewal {
    next: Option<tokio::time::Instant>,
    delay: u64,
}
impl Renewal {
    fn new(stored: bool) -> Self {
        Self {
            next: stored.then(tokio::time::Instant::now),
            delay: 1,
        }
    }
    fn succeeded(&mut self, expires: tokio::time::Instant) {
        let now = tokio::time::Instant::now();
        let remaining = expires.saturating_duration_since(now);
        let margin = (remaining / 10).min(std::time::Duration::from_secs(60));
        self.next = Some(now + remaining - margin);
        self.delay = 1;
    }
    fn failed(&mut self, error: &Error) {
        self.next = if matches!(error, Error::Remote) {
            Some(tokio::time::Instant::now() + std::time::Duration::from_secs(self.delay))
        } else {
            None
        };
        self.delay = (self.delay * 2).min(300);
    }
}
impl Runtime {
    async fn save(&self, credentials: Option<Credentials>) -> Result<(), Error> {
        self.store
            .save(credentials)
            .await
            .map_err(|_| Error::Storage)
    }
    fn credentials(&self) -> Option<Credentials> {
        self.store.credentials()
    }
    fn publish(&self, active: Option<&Client>, busy: bool, error: Option<String>) {
        let credentials = self.credentials();
        self.client.send_replace(
            active
                .filter(|client| client.authenticated())
                .map(|client| Arc::new(client.clone())),
        );
        self.status.send_replace(Status {
            logged_in: active.is_some_and(Client::authenticated),
            expires: active.and_then(Client::expires_at),
            busy,
            stored: credentials.is_some(),
            country: credentials.map(|c| c.country),
            error,
        });
    }
    pub async fn run(mut self, mut stop: watch::Receiver<bool>) -> io::Result<()> {
        let mut active = None;
        let mut pending = None;
        let mut renewal = Renewal::new(self.credentials().is_some());
        while !*stop.borrow() {
            let due = renewal.next.unwrap_or_else(|| {
                tokio::time::Instant::now() + std::time::Duration::from_secs(86400)
            });
            let command = tokio::select! {biased;
                _=stop.changed()=>break,
                command=self.logout.recv()=>match command {Some(command)=>command,None=>break},
                _=tokio::time::sleep_until(due),if renewal.next.is_some()=>{
                    let (result,_)=oneshot::channel();Command{action:Action::Refresh,epoch:*self.epoch.borrow(),result}
                },
                command=self.commands.recv()=>match command {Some(command)=>command,None=>break}
            };
            let epoch = *self.epoch.borrow_and_update();
            if command.epoch != epoch {
                let _ = command.result.send(Err(Error::Cancelled));
                continue;
            }
            let refreshing = matches!(command.action, Action::Refresh);
            if matches!(command.action, Action::Logout) {
                self.publish(None, true, None);
            } else {
                self.publish(active.as_ref(), true, None);
            }
            let result = match command.action {
                Action::Logout => {
                    // Accepted logout revokes live access even if durable deletion
                    // fails. Report the storage error without restoring the lease.
                    active = None;
                    pending = None;
                    renewal.next = None;
                    self.save(None)
                        .await
                        .map(|()| serde_json::json!({"ok":true}))
                }
                action => {
                    let mut cancelled = self.epoch.clone();
                    let operation = async {
                        match action {
                            Action::Login(country) => {
                                let mut client =
                                    Client::new(&country).map_err(|_| Error::InvalidInput)?;
                                let url = client.sign_in_url().await.map_err(remote)?;
                                pending = Some((country, client));
                                Ok((serde_json::json!({"url":url}), None))
                            }
                            Action::Complete(url) => {
                                let code = code(&url)?;
                                let (country, mut client) =
                                    pending.take().ok_or(Error::Unavailable)?;
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map_err(|_| Error::InvalidInput)?
                                    .as_millis();
                                let token = client
                                    .exchange_code(
                                        &code,
                                        now.try_into().map_err(|_| Error::InvalidInput)?,
                                    )
                                    .await
                                    .map_err(remote)?;
                                client
                                    .authenticate(token.refresh_token())
                                    .await
                                    .map_err(remote)?;
                                Ok((
                                    serde_json::json!({"ok":true}),
                                    Some((
                                        client,
                                        Credentials {
                                            country,
                                            refresh: token.refresh_token().into(),
                                        },
                                    )),
                                ))
                            }
                            Action::Refresh => {
                                let credentials = self.credentials().ok_or(Error::Unavailable)?;
                                if let Some(client) = active.as_mut() {
                                    client
                                        .authenticate(&credentials.refresh)
                                        .await
                                        .map_err(remote)?;
                                    Ok((serde_json::json!({"ok":true}), None))
                                } else {
                                    let mut client = Client::new(&credentials.country)
                                        .map_err(|_| Error::InvalidInput)?;
                                    client
                                        .authenticate(&credentials.refresh)
                                        .await
                                        .map_err(remote)?;
                                    Ok((
                                        serde_json::json!({"ok":true}),
                                        Some((client, credentials)),
                                    ))
                                }
                            }
                            Action::Logout => unreachable!(),
                        }
                    };
                    let result =
                        owned_operation(operation, &mut stop, &mut cancelled, command.epoch).await;
                    let result = if command.epoch != *self.epoch.borrow() {
                        Err(Error::Cancelled)
                    } else {
                        result
                    };
                    match result {
                        Ok((reply, Some((client, credentials)))) => {
                            match self.save(Some(credentials)).await {
                                Ok(()) => {
                                    if command.epoch != *self.epoch.borrow() {
                                        Err(Error::Cancelled)
                                    } else {
                                        renewal.succeeded(
                                            client
                                                .expires_at()
                                                .expect("authenticated client has expiry"),
                                        );
                                        active = Some(client);
                                        Ok(reply)
                                    }
                                }
                                Err(error) => Err(error),
                            }
                        }
                        Ok((reply, None)) => Ok(reply),
                        Err(error) => Err(error),
                    }
                }
            };
            if refreshing {
                match &result {
                    Ok(_) => {
                        if let Some(expires) = active.as_ref().and_then(Client::expires_at) {
                            renewal.succeeded(expires);
                        }
                    }
                    Err(error) => {
                        renewal.failed(error);
                        if matches!(error, Error::Authentication) {
                            active = None;
                        }
                    }
                }
            }
            self.publish(
                active.as_ref(),
                false,
                result.as_ref().err().map(ToString::to_string),
            );
            let _ = command.result.send(result);
        }
        self.commands.close();
        self.logout.close();
        while let Ok(command) = self.logout.try_recv() {
            let _ = command.result.send(Err(Error::Stopped));
        }
        while let Ok(command) = self.commands.try_recv() {
            let _ = command.result.send(Err(Error::Stopped));
        }
        self.publish(None, false, Some("Stopped".into()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Memory(Mutex<Option<Credentials>>);
    impl CredentialStore for Memory {
        fn credentials(&self) -> Option<Credentials> {
            self.0.lock().unwrap().clone()
        }
        fn save(
            &self,
            credentials: Option<Credentials>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>>
        {
            Box::pin(async move {
                *self.0.lock().unwrap() = credentials;
                Ok(())
            })
        }
    }
    #[test]
    fn callback_validation_rejects_ambiguous_and_foreign_codes() {
        assert_eq!(
            code("https://kr.m.lgaccount.com/login/iabClose?code=a%26b").unwrap(),
            "a&b"
        );
        for url in [
            "https://evil.example/login/iabClose?code=secret",
            "https://kr.m.lgaccount.com/login/iabClose?code=a&code=b",
            "http://kr.m.lgaccount.com/login/iabClose?code=a",
            "https://kr.m.lgaccount.com/login/iabClose?code=",
            "https://kr.m.lgaccount.com/login/iabClose?code=a#fragment",
        ] {
            assert!(code(url).is_err());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn renewal_precedes_expiry_and_transient_backoff_stops_for_terminal_errors() {
        let now = tokio::time::Instant::now();
        let mut renewal = Renewal::new(true);
        assert_eq!(renewal.next, Some(now));
        renewal.succeeded(now + std::time::Duration::from_secs(3600));
        assert_eq!(
            renewal.next,
            Some(now + std::time::Duration::from_secs(3540))
        );
        renewal.succeeded(now + std::time::Duration::from_secs(30));
        assert_eq!(renewal.next, Some(now + std::time::Duration::from_secs(27)));
        for delay in [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300] {
            renewal.failed(&Error::Remote);
            assert_eq!(
                renewal.next,
                Some(now + std::time::Duration::from_secs(delay))
            );
        }
        renewal.failed(&Error::Authentication);
        assert!(renewal.next.is_none());
        renewal.failed(&Error::Storage);
        assert!(renewal.next.is_none());
        renewal.succeeded(now + std::time::Duration::from_secs(600));
        renewal.failed(&Error::Remote);
        assert_eq!(renewal.next, Some(now + std::time::Duration::from_secs(1)));
        let (commands, _) = mpsc::channel(1);
        let (logout, _) = mpsc::channel(1);
        let (epoch, _) = watch::channel(0);
        let (status, watched) = watch::channel(Status {
            logged_in: true,
            stored: true,
            country: Some("KR".into()),
            busy: false,
            error: None,
            expires: Some(now + std::time::Duration::from_secs(5)),
        });
        let handle = Handle {
            admission: Arc::new(Mutex::new(Admission {
                commands,
                logout,
                epoch,
                client: watch::channel(None).0,
            })),
            status: watched,
            client: watch::channel(None).1,
        };
        assert!(handle.status().logged_in);
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert!(!handle.status().logged_in);
        drop(status);
    }
    #[tokio::test]
    async fn cancellation_drops_owned_network_future_without_late_activation() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let proof = dropped.clone();
        let (started, ready) = oneshot::channel();
        let (_stop, mut stopped) = watch::channel(false);
        let (cancel, mut cancelled) = watch::channel(0);
        let task = tokio::spawn(async move {
            owned_operation(
                async move {
                    let _guard = Guard(proof);
                    let _ = started.send(());
                    std::future::pending::<Result<(), Error>>().await
                },
                &mut stopped,
                &mut cancelled,
                0,
            )
            .await
        });
        ready.await.unwrap();
        cancel.send_replace(1);
        assert!(matches!(task.await.unwrap(), Err(Error::Cancelled)));
        assert!(dropped.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn logout_invalidates_queued_login_and_startup_restore_without_lg_requests() {
        let store = Arc::new(Memory(Mutex::new(Some(Credentials {
            country: "KR".into(),
            refresh: "saved".into(),
        }))));
        let (handle, runtime) = new(store.clone());
        let (old_result, old_reply) = oneshot::channel();
        let (logout_result, logout_reply) = oneshot::channel();
        {
            let admission = handle.admission.lock().unwrap();
            admission
                .commands
                .try_send(Command {
                    action: Action::Login("KR".into()),
                    epoch: 0,
                    result: old_result,
                })
                .unwrap();
            admission
                .logout
                .try_send(Command {
                    action: Action::Logout,
                    epoch: 1,
                    result: logout_result,
                })
                .unwrap();
            admission.epoch.send_replace(1);
        }
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(runtime.run(stopped));
        tokio::time::timeout(std::time::Duration::from_secs(2), logout_reply)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(old_reply.await.unwrap(), Err(Error::Cancelled)));
        assert!(!handle.status().stored);
        assert!(matches!(handle.refresh().await, Err(Error::Unavailable)));
        stop.send_replace(true);
        task.await.unwrap().unwrap();
        assert!(store.credentials().is_none());
    }
}
