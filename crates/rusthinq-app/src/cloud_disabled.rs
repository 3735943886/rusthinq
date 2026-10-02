//! Account routes retain explicit disabled results in local-only builds.
use serde::Serialize;
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub logged_in: bool,
    pub stored: bool,
    pub country: Option<String>,
    pub busy: bool,
    pub error: Option<String>,
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
#[derive(Clone)]
pub struct Handle {
    _private: (),
}
impl Handle {
    pub fn status(&self) -> Status {
        Status {
            logged_in: false,
            stored: false,
            country: None,
            busy: false,
            error: Some("bridge feature is disabled".into()),
        }
    }
    pub async fn login(&self, _country: String) -> Result<serde_json::Value, Error> {
        Err(Error::Unavailable)
    }
    pub async fn complete(&self, _url: String) -> Result<serde_json::Value, Error> {
        Err(Error::Unavailable)
    }
    pub async fn refresh(&self) -> Result<serde_json::Value, Error> {
        Err(Error::Unavailable)
    }
    pub async fn logout(&self) -> Result<serde_json::Value, Error> {
        Err(Error::Unavailable)
    }
}
