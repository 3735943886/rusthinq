//! Driver configuration is parsed independently of the optional Rhai engine.
use std::{collections::BTreeMap, io, path::PathBuf};
#[derive(Clone, Debug)]
pub struct Config {
    pub watch: bool,
    pub directory: PathBuf,
    pub topic_prefix: String,
    pub bindings: BTreeMap<String, String>,
}
impl Config {
    pub fn validate(&self) -> io::Result<()> {
        #[cfg(feature = "scripting")]
        {
            self.engine_config().validate()
        }
        #[cfg(not(feature = "scripting"))]
        {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scripting feature is disabled",
            ))
        }
    }
}
