use super::*;
impl Handle {
    pub fn diagnostics(&self) -> serde_json::Value {
        self.0.observations.diagnostics()
    }
    pub fn publications(
        &self,
        id: &str,
        session: SessionKey,
        generation: u64,
    ) -> serde_json::Value {
        self.0.observations.publications(id, session, generation)
    }
    #[cfg(feature = "bridge")]
    pub(crate) fn cloud_changed(&self, device: String) {
        let _ = self.0.events.send(Event::CloudChanged { device });
    }
    #[cfg(feature = "bridge")]
    pub(crate) fn cloud_deploy(&self, id: &str, generation: u64) -> Option<serde_json::Value> {
        self.0
            .cloud_deploy
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .filter(|(g, _)| *g == generation)
            .map(|(_, v)| v.clone())
    }
    #[cfg(feature = "bridge")]
    pub(crate) fn cloud_devices(&self) -> Option<crate::cloud_devices::Handle> {
        self.0
            .cloud_devices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    #[cfg(feature = "bridge")]
    pub(crate) fn attach_cloud_devices(
        &self,
        handle: crate::cloud_devices::Handle,
    ) -> io::Result<()> {
        let mut binding = self
            .0
            .cloud_devices
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if binding.is_some() {
            return Err(io::Error::other("cloud devices already attached"));
        }
        *binding = Some(handle);
        Ok(())
    }
    /// Immutable configured capability projection; admission still checks the actor.
    pub fn driver_reload_configured(&self) -> bool {
        self.0
            .driver_reload_configured
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Drivers reload themselves on file change.
    pub fn driver_watch(&self) -> bool {
        self.0
            .driver_watch
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Composition-only binding; L4 remains the sole account state/policy owner.
    #[cfg(feature = "bridge")]
    pub fn attach_cloud_account(&self, account: crate::cloud_account::Handle) -> io::Result<()> {
        let mut binding = self.0.cloud.lock().unwrap_or_else(|e| e.into_inner());
        if binding.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "cloud account already bound",
            ));
        }
        *binding = Some(account);
        Ok(())
    }
    pub(crate) fn cloud_account(&self) -> Option<crate::cloud_account::Handle> {
        self.0
            .cloud
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn persisted_models(&self) -> BTreeMap<String, crate::lifecycle_storage::DeviceMetadata> {
        self.0
            .persisted_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub(crate) fn retired_script(&self, id: &str) -> Option<crate::scripts::Context> {
        self.0
            .retired_scripts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned()
    }
    #[cfg(feature = "scripting")]
    pub(crate) fn driver_error(&self, device: String, reason: String) {
        let _ = self.0.events.send(Event::Rejected { device, reason });
    }

    pub fn external_mqtt(&self) -> Option<crate::external_mqtt::Handle> {
        self.0
            .external_mqtt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn durable_devices(&self) -> Vec<rusthinq_lifecycle::Entry> {
        self.0
            .durable_devices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub async fn inject(
        &self,
        id: String,
        session: SessionKey,
        data: Vec<u8>,
        to_device: bool,
    ) -> Result<Option<rusthinq_server::Receipt>, rusthinq_server::Reject> {
        if data.len() > 1_000_000 {
            return Err(rusthinq_server::Reject::PayloadExceeded);
        }
        let (result, received) = oneshot::channel();
        self.management(ManagementCommand::Inject {
            id,
            session,
            data,
            to_device,
            result,
        })?;
        received
            .await
            .unwrap_or(Err(rusthinq_server::Reject::Stopped))
    }
    pub fn driver_models(&self) -> BTreeMap<String, (SessionKey, String, bool)> {
        self.0
            .driver_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn script_states(&self) -> BTreeMap<String, (SessionKey, u64, bool)> {
        self.0
            .script_states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// Compile before admission; the captured incarnation/session must still be current.
    #[cfg(feature = "scripting")]
    pub async fn attach_script(
        &self,
        device: String,
        session: SessionKey,
        compiled: rusthinq_scripting::Compiled,
        config: rusthinq_scripting::worker::Config,
        callbacks: crate::scripts::Callbacks,
    ) -> Result<(), rusthinq_scripting::Error> {
        if [
            &callbacks.response,
            &callbacks.data,
            &callbacks.ready,
            &callbacks.timer,
            &callbacks.shutdown,
        ]
        .iter()
        .any(|name| {
            name.as_ref()
                .is_some_and(|name| name.is_empty() || name.len() > 256)
        }) {
            return Err(rusthinq_scripting::Error::InvalidConfig);
        }
        let (result, received) = oneshot::channel();
        self.0
            .scripts
            .try_send(ScriptCommand::Attach(Box::new(AttachScript {
                device,
                session,
                compiled,
                config,
                callbacks,
                result,
            })))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_scripting::Error::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_scripting::Error::Stopped,
            })?;
        received
            .await
            .map_err(|_| rusthinq_scripting::Error::Stopped)?
    }
    pub fn metadata_snapshot(&self) -> Vec<rusthinq_server::thinq1_http::Metadata> {
        self.0
            .metadata
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
    /// Returns ordered execution admission; ScriptExecuted reports the eventual outcome.
    #[cfg(feature = "scripting")]
    pub async fn invoke_script(
        &self,
        device: String,
        session: SessionKey,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<u64, rusthinq_scripting::Error> {
        if input.len() > 1_000_000 || function.is_empty() || function.len() > 256 {
            return Err(rusthinq_scripting::Error::InputExceeded);
        }
        let (result, received) = oneshot::channel();
        self.0
            .scripts
            .try_send(ScriptCommand::Invoke {
                device,
                session,
                generation,
                function,
                input,
                result,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_scripting::Error::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_scripting::Error::Stopped,
            })?;
        received
            .await
            .map_err(|_| rusthinq_scripting::Error::Stopped)?
    }
    /// Reload a configured driver from disk. The actor owns and joins preparation.
    #[cfg(feature = "scripting")]
    pub async fn reload_configured_driver(
        &self,
        device: String,
        session: SessionKey,
        generation: u64,
    ) -> Result<u64, rusthinq_scripting::Error> {
        let (result, received) = oneshot::channel();
        self.0
            .scripts
            .try_send(ScriptCommand::PrepareReload(PrepareReload {
                device,
                session,
                generation,
                result,
            }))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_scripting::Error::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_scripting::Error::Stopped,
            })?;
        received
            .await
            .map_err(|_| rusthinq_scripting::Error::Stopped)?
    }
    /// Supply a successfully compiled replacement; admission fences session and generation.
    /// Callback names/encoding are kept. A lost reply must not trigger automatic retry.
    #[cfg(feature = "scripting")]
    pub async fn reload_script(
        &self,
        device: String,
        session: SessionKey,
        generation: u64,
        compiled: rusthinq_scripting::Compiled,
    ) -> Result<u64, rusthinq_scripting::Error> {
        self.reload_prepared(device, session, generation, compiled, false)
            .await
    }
    #[cfg(feature = "scripting")]
    pub(crate) async fn reload_driver(
        &self,
        device: String,
        session: SessionKey,
        generation: u64,
        compiled: rusthinq_scripting::Compiled,
    ) -> Result<u64, rusthinq_scripting::Error> {
        self.reload_prepared(device, session, generation, compiled, true)
            .await
    }
    #[cfg(feature = "scripting")]
    async fn reload_prepared(
        &self,
        device: String,
        session: SessionKey,
        generation: u64,
        compiled: rusthinq_scripting::Compiled,
        initialize: bool,
    ) -> Result<u64, rusthinq_scripting::Error> {
        let (result, received) = oneshot::channel();
        self.0
            .scripts
            .try_send(ScriptCommand::Reload(Box::new(ReloadScript {
                device,
                session,
                generation,
                compiled,
                initialize,
                result,
            })))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_scripting::Error::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_scripting::Error::Stopped,
            })?;
        received
            .await
            .map_err(|_| rusthinq_scripting::Error::Stopped)?
    }
    /// Supply a fresh session and reopened inventory after cleanup failure.
    /// A running worker is never replaced or aborted by this operation.
    pub async fn attach_retained_cleanup<S>(
        &self,
        session: crate::cleanup_mqtt::Session<S>,
        ledger: crate::retained_cleanup::Ledger,
    ) -> io::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (result, received) = oneshot::channel();
        self.0
            .attach
            .send(AttachCleanup {
                start: cleanup_start(session, ledger),
                result,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "runtime stopped"))?;
        received
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "runtime stopped"))?
    }
    /// Latest health survives observer broadcast loss.
    pub fn cleanup_status(&self) -> watch::Receiver<CleanupStatus> {
        self.0.cleanup_status.subscribe()
    }
    /// Request removal; configured bridge registrations are deregistered first.
    pub fn forget(&self, id: String) -> Result<(), mpsc::error::TrySendError<String>> {
        self.0
            .commands
            .try_send(ManagementCommand::Forget(id))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(ManagementCommand::Forget(id)) => {
                    mpsc::error::TrySendError::Full(id)
                }
                mpsc::error::TrySendError::Closed(ManagementCommand::Forget(id)) => {
                    mpsc::error::TrySendError::Closed(id)
                }
                _ => unreachable!("submitted forget command"),
            })
    }
    /// Admission only; durable removal success/failure is observed in lifecycle events.
    pub async fn forget_scoped(
        &self,
        id: String,
        incarnation: u64,
    ) -> Result<(), rusthinq_server::Reject> {
        let (result, received) = oneshot::channel();
        self.management(ManagementCommand::ForgetScoped {
            id,
            incarnation,
            result,
        })?;
        received
            .await
            .unwrap_or(Err(rusthinq_server::Reject::Stopped))
    }
    /// Raw JSON transport send, fenced again by the lifecycle actor and L3 registry.
    pub async fn send(
        &self,
        id: String,
        session: SessionKey,
        payload: Vec<u8>,
    ) -> Result<rusthinq_server::Receipt, rusthinq_server::Reject> {
        if payload.len() > 1_000_000 {
            return Err(rusthinq_server::Reject::PayloadExceeded);
        }
        let (result, received) = oneshot::channel();
        self.management(ManagementCommand::Send {
            id,
            session,
            payload,
            result,
        })?;
        received
            .await
            .unwrap_or(Err(rusthinq_server::Reject::Stopped))
    }
    fn management(&self, command: ManagementCommand) -> Result<(), rusthinq_server::Reject> {
        self.0
            .commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => rusthinq_server::Reject::Busy,
                mpsc::error::TrySendError::Closed(_) => rusthinq_server::Reject::Stopped,
            })
    }
    /// Subscribe before snapshot; receivers must handle Lagged and resnapshot.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.0.events.subscribe()
    }
    pub fn snapshot(&self) -> Vec<Device> {
        self.0
            .devices
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}
