//! Correlates durable lifecycle removal with retained-topic ownership cleanup.
use crate::{cleanup_mqtt::Session, retained_cleanup::Ledger};
use rusthinq_lifecycle::Action;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ignored,
    Removed { owner: String, topics: usize },
}

/// Incarnation is part of ownership so a late cleanup cannot delete a recreated
/// device's retained state. The length prefix makes arbitrary valid IDs unambiguous.
pub fn device_owner(id: &str, incarnation: u64) -> io::Result<String> {
    if id.is_empty() || id.len() > 220 || id.chars().any(char::is_control) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid retained device owner",
        ));
    }
    Ok(format!("device/{}:{id}/{incarnation}", id.len()))
}

/// Invoke for lifecycle actions after their durable state transition completes.
/// Only Removed triggers cleanup; Changed/Offline/ForgetFailed never erase state.
/// A cleanup error does not resurrect the device. Reopen the durable ledger and
/// retry the same owner or drain it during startup/adapter shutdown.
pub async fn apply<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut Session<S>,
    ledger: Ledger,
    action: &Action,
) -> io::Result<(Ledger, Outcome)> {
    let Action::Removed { id, incarnation } = action else {
        return Ok((ledger, Outcome::Ignored));
    };
    let owner = device_owner(id, *incarnation)?;
    let (ledger, topics) = session.remove_owner(ledger, &owner).await?;
    Ok((ledger, Outcome::Removed { owner, topics }))
}
