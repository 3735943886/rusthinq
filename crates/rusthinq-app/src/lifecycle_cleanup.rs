//! Correlates durable lifecycle removal with retained-topic ownership cleanup.
use crate::{cleanup_mqtt::Session, retained_cleanup::Ledger};
use rusthinq_lifecycle::Action;
use rusthinq_lifecycle::Device;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

/// Recover only canonical device owners absent from the durable/live inventory.
/// Adapter owners and offline known devices are preserved.
pub async fn recover<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut Session<S>,
    mut ledger: Ledger,
    devices: &[Device],
) -> io::Result<Ledger> {
    let mut owners = std::collections::BTreeSet::new();
    for item in ledger.pending() {
        let Some((id, incarnation)) = parse_owner(&item.owner) else {
            continue;
        };
        if !devices
            .iter()
            .any(|device| device.entry.id == id && device.entry.incarnation == incarnation)
        {
            owners.insert(item.owner);
        }
    }
    for owner in owners {
        ledger = session.remove_owner(ledger, &owner).await?.0;
    }
    Ok(ledger)
}

fn parse_owner(owner: &str) -> Option<(&str, u64)> {
    let (length, rest) = owner.strip_prefix("device/")?.split_once(':')?;
    let length: usize = length.parse().ok()?;
    let id = rest.get(..length)?;
    let incarnation: u64 = rest.get(length..)?.strip_prefix('/')?.parse().ok()?;
    (device_owner(id, incarnation).ok()?.as_str() == owner).then_some((id, incarnation))
}

/// Latest snapshots coalesce without losing removal work: the durable retained
/// inventory is the source of pending cleanup, including across restart.
pub(crate) async fn run<S: AsyncRead + AsyncWrite + Unpin>(
    mut session: Session<S>,
    mut ledger: Ledger,
    mut state: watch::Receiver<(Vec<Device>, bool)>,
) -> io::Result<()> {
    loop {
        let (devices, stopped) = state.borrow_and_update().clone();
        ledger = recover(&mut session, ledger, &devices).await?;
        if stopped {
            return Ok(());
        }
        if state.changed().await.is_err() {
            return Ok(());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ignored,
    Removed { owner: String, topics: usize },
}

/// Incarnation is part of ownership so a late cleanup cannot delete a recreated
/// device's retained state. The length prefix makes arbitrary valid IDs unambiguous.
pub fn device_owner(id: &str, incarnation: u64) -> io::Result<String> {
    if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
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
