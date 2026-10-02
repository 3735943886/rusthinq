//! Opaque publication scope; independent of the optional Rhai engine.
use rusthinq_lifecycle::SessionKey;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub device: String,
    pub session: SessionKey,
    pub generation: u64,
}
/// Called synchronously after fencing. Implementations must be bounded/nonblocking;
/// retained MQTT adapters must durably inventory ownership before publication.
pub trait PublishSink: Send + Sync {
    fn try_publish(&self, context: &Context, payload: String) -> Result<(), String>;
    fn try_publish_retired(&self, context: &Context, payload: String) -> Result<(), String> {
        self.try_publish(context, payload)
    }
}
