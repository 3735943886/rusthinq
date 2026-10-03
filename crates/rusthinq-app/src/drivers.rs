//! Driver preparation delegates to L5; L6 only supplies configuration and owns jobs.
pub use crate::driver_config::Config;
impl Config {
    pub(crate) fn engine_config(&self) -> rusthinq_scripting::drivers::Config {
        rusthinq_scripting::drivers::Config {
            watch: self.watch,
            directory: self.directory.clone(),
            topic_prefix: self.topic_prefix.clone(),
            bindings: self.bindings.clone(),
        }
    }
    pub(crate) fn source_revision(&self, id: &str, model: &str) -> std::io::Result<u64> {
        self.engine_config().source_revision(id, model)
    }
    pub fn prepare_with_metadata(
        &self,
        id: &str,
        model: &str,
        thinq2: bool,
        consumer: bool,
        metadata: Option<(&str, &str)>,
    ) -> Result<rusthinq_scripting::Compiled, rusthinq_scripting::Error> {
        self.engine_config()
            .prepare_with_metadata(id, model, thinq2, consumer, metadata)
    }
    pub fn prepare(
        &self,
        id: &str,
        model: &str,
        thinq2: bool,
        consumer: bool,
    ) -> Result<rusthinq_scripting::Compiled, rusthinq_scripting::Error> {
        self.engine_config().prepare(id, model, thinq2, consumer)
    }
}
