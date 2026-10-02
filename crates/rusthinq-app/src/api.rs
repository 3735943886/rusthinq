//! Application-owned results and event projection for adapters. No transport receipts escape.
use crate::runtime::{Event, Handle};
use rusthinq_lifecycle::SessionKey;
use serde_json::{Value, json};
use tokio::sync::broadcast;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    Failed,
    Unknown,
}
impl From<rusthinq_server::Delivery> for Delivery {
    fn from(value: rusthinq_server::Delivery) -> Self {
        match value {
            rusthinq_server::Delivery::Sent => Self::Sent,
            rusthinq_server::Delivery::Failed => Self::Failed,
            rusthinq_server::Delivery::Unknown => Self::Unknown,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    Disabled,
    StaleSession,
    Busy,
    Stopped,
    PayloadExceeded,
    InvalidInput,
}
impl From<rusthinq_server::Reject> for Reject {
    fn from(value: rusthinq_server::Reject) -> Self {
        match value {
            rusthinq_server::Reject::StaleSession => Self::StaleSession,
            rusthinq_server::Reject::Busy => Self::Busy,
            rusthinq_server::Reject::Stopped => Self::Stopped,
            rusthinq_server::Reject::PayloadExceeded => Self::PayloadExceeded,
            _ => Self::InvalidInput,
        }
    }
}
pub struct Events(broadcast::Receiver<Event>);
impl Events {
    pub async fn recv(&mut self) -> Result<Value, broadcast::error::RecvError> {
        self.0.recv().await.map(event_value)
    }
}
impl Handle {
    pub fn adapter_events(&self) -> Events {
        Events(self.subscribe())
    }
    pub async fn adapter_forget(&self, id: String, incarnation: u64) -> Result<(), Reject> {
        self.forget_scoped(id, incarnation)
            .await
            .map_err(Into::into)
    }
    pub async fn adapter_send(
        &self,
        id: String,
        session: SessionKey,
        payload: Vec<u8>,
    ) -> Result<Delivery, Reject> {
        Ok(self
            .send(id, session, payload)
            .await
            .map_err(Reject::from)?
            .wait()
            .await
            .into())
    }
    pub async fn adapter_inject(
        &self,
        id: String,
        session: SessionKey,
        data: Vec<u8>,
        to_device: bool,
    ) -> Result<Option<Delivery>, Reject> {
        match self
            .inject(id, session, data, to_device)
            .await
            .map_err(Reject::from)?
        {
            Some(receipt) => Ok(Some(receipt.wait().await.into())),
            None => Ok(None),
        }
    }
    #[cfg(feature = "scripting")]
    pub async fn adapter_invoke(
        &self,
        id: String,
        session: SessionKey,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<u64, Reject> {
        self.invoke_script(id, session, generation, function, input)
            .await
            .map_err(|error| match error {
                rusthinq_scripting::Error::Stale => Reject::StaleSession,
                rusthinq_scripting::Error::Busy => Reject::Busy,
                rusthinq_scripting::Error::Stopped => Reject::Stopped,
                _ => Reject::InvalidInput,
            })
    }
}
fn context(context: &crate::scripts::Context) -> Value {
    json!({"device":context.device,"incarnation":context.session.incarnation.to_string(),"generation":context.session.generation.to_string(),"scriptGeneration":context.generation.to_string()})
}
fn event_value(event: Event) -> Value {
    match event {
        Event::Injected {
            session,
            data,
            to_device,
        } => {
            json!({"type":"injected","device":session.device,"generation":session.generation.to_string(),"hex":rusthinq_protocol::hex::encode(data),"toDevice":to_device})
        }
        Event::ScriptStopped {
            context: ctx,
            error,
        } => json!({"type":"scriptStopped","context":context(&ctx),"error":error}),
        Event::ScriptExecuted {
            sequence,
            context: scope,
            error,
        } => {
            json!({"type":"scriptExecuted","sequence":sequence.to_string(),"context":context(&scope),"error":error})
        }
        Event::ScriptOutput {
            context: scope,
            payload,
        } => json!({"type":"scriptOutput","context":context(&scope),"payload":payload}),
        Event::ScriptDelivery {
            context: scope,
            delivery,
        } => {
            json!({"type":"scriptDelivery","context":context(&scope),"delivery":format!("{delivery:?}"),"deviceAcknowledged":false})
        }
        Event::Transport(rusthinq_server::Event::Data(id, bytes)) => {
            json!({"type":"data","device":id.device,"generation":id.generation.to_string(),"hex":rusthinq_protocol::hex::encode(bytes)})
        }
        Event::Lost { transport_events } => json!({"type":"lost","events":transport_events}),
        Event::Rejected { device, reason } => {
            json!({"type":"rejected","device":device,"reason":reason})
        }
        Event::Metadata(metadata) => {
            json!({"type":"metadata","device":metadata.device_id,"model":metadata.model_name,"deviceType":metadata.device_type})
        }
        other => json!({"type":"stateChanged","detail":format!("{other:?}")}),
    }
}

#[cfg(not(feature = "scripting"))]
impl Handle {
    pub async fn adapter_invoke(
        &self,
        _id: String,
        _session: SessionKey,
        _generation: u64,
        _function: String,
        _input: String,
    ) -> Result<u64, Reject> {
        Err(Reject::Disabled)
    }
}

/// Adapter boundary. Runtime/transport handles, worker objects and receipts do not escape.
#[derive(Clone)]
pub struct AppHandle(Handle);
impl From<Handle> for AppHandle {
    fn from(handle: Handle) -> Self {
        Self(handle)
    }
}
#[derive(Clone, Debug)]
pub struct Metadata {
    pub device_id: String,
    pub model_name: String,
    pub device_type: String,
}
#[derive(Debug)]
pub enum CloudError {
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
impl std::fmt::Display for CloudError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cloud account: {self:?}")
    }
}
impl std::error::Error for CloudError {}
impl From<crate::cloud_account::Error> for CloudError {
    fn from(error: crate::cloud_account::Error) -> Self {
        use crate::cloud_account::Error as E;
        match error {
            E::Busy => Self::Busy,
            E::Stopped => Self::Stopped,
            E::InvalidInput => Self::InvalidInput,
            E::Unavailable => Self::Unavailable,
            E::Remote => Self::Remote,
            E::Storage => Self::Storage,
            E::Cancelled => Self::Cancelled,
            E::Authentication => Self::Authentication,
            E::Rejected => Self::Rejected,
        }
    }
}
impl AppHandle {
    pub fn snapshot(&self) -> Vec<rusthinq_lifecycle::Device> {
        self.0.snapshot()
    }
    pub fn durable_devices(&self) -> Vec<rusthinq_lifecycle::Entry> {
        self.0.durable_devices()
    }
    pub fn driver_models(&self) -> std::collections::BTreeMap<String, (SessionKey, String, bool)> {
        self.0.driver_models()
    }
    pub fn script_states(&self) -> std::collections::BTreeMap<String, (SessionKey, u64, bool)> {
        self.0.script_states()
    }
    pub fn persisted_models(
        &self,
    ) -> std::collections::BTreeMap<String, crate::lifecycle_storage::DeviceMetadata> {
        self.0.persisted_models()
    }
    pub fn metadata_snapshot(&self) -> Vec<Metadata> {
        self.0
            .metadata_snapshot()
            .into_iter()
            .map(|m| Metadata {
                device_id: m.device_id,
                model_name: m.model_name,
                device_type: m.device_type,
            })
            .collect()
    }
    pub fn cleanup_status(&self) -> tokio::sync::watch::Receiver<crate::runtime::CleanupStatus> {
        self.0.cleanup_status()
    }
    pub fn external_mqtt(&self) -> Option<crate::external_mqtt::Handle> {
        self.0.external_mqtt()
    }
    pub(crate) fn retired_script(&self, id: &str) -> Option<crate::script_types::Context> {
        self.0.retired_script(id)
    }
    #[cfg(feature = "scripting")]
    pub(crate) fn driver_error(&self, device: String, reason: String) {
        self.0.driver_error(device, reason)
    }
    pub fn adapter_events(&self) -> Events {
        self.0.adapter_events()
    }
    pub async fn adapter_forget(&self, id: String, incarnation: u64) -> Result<(), Reject> {
        self.0.adapter_forget(id, incarnation).await
    }
    pub async fn adapter_send(
        &self,
        id: String,
        session: SessionKey,
        payload: Vec<u8>,
    ) -> Result<Delivery, Reject> {
        self.0.adapter_send(id, session, payload).await
    }
    pub async fn adapter_inject(
        &self,
        id: String,
        session: SessionKey,
        data: Vec<u8>,
        to_device: bool,
    ) -> Result<Option<Delivery>, Reject> {
        self.0.adapter_inject(id, session, data, to_device).await
    }
    pub async fn adapter_invoke(
        &self,
        id: String,
        session: SessionKey,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<u64, Reject> {
        self.0
            .adapter_invoke(id, session, generation, function, input)
            .await
    }
    pub fn cloud_status(&self) -> Value {
        match self.0.cloud_account() {
            Some(account) => json!({"enabled":true,"account":account.status()}),
            None => json!({"enabled":false}),
        }
    }
    pub async fn cloud_login(&self, country: String) -> Result<Value, CloudError> {
        self.0
            .cloud_account()
            .ok_or(CloudError::Unavailable)?
            .login(country)
            .await
            .map_err(Into::into)
    }
    pub async fn cloud_complete(&self, url: String) -> Result<Value, CloudError> {
        self.0
            .cloud_account()
            .ok_or(CloudError::Unavailable)?
            .complete(url)
            .await
            .map_err(Into::into)
    }
    pub async fn cloud_refresh(&self) -> Result<Value, CloudError> {
        self.0
            .cloud_account()
            .ok_or(CloudError::Unavailable)?
            .refresh()
            .await
            .map_err(Into::into)
    }
    pub async fn cloud_logout(&self) -> Result<Value, CloudError> {
        self.0
            .cloud_account()
            .ok_or(CloudError::Unavailable)?
            .logout()
            .await
            .map_err(Into::into)
    }
}
