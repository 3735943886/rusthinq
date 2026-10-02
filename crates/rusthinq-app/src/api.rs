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
