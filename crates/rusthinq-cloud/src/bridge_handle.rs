//! Re-export (or stub) of `rusthinq_bridge::Bridge`, so code that only ever reads
//! bridge *status* — devlist.rs's retained snapshot — compiles unchanged whether
//! or not the `bridge` Cargo feature is on. Actually driving a bridge session
//! (login/enable/disable) lives in bridge_adapter.rs/bridge_control.rs, which
//! are only compiled with the feature on.

#[cfg(feature = "bridge")]
pub use rusthinq_bridge::Bridge;

/// Never constructed — with the feature off, `main.rs` always passes `None` for
/// `Option<Arc<Bridge>>`, so these bodies exist only to type-check.
#[cfg(not(feature = "bridge"))]
pub struct Bridge;

#[cfg(not(feature = "bridge"))]
impl Bridge {
    pub fn status_for(&self, _id: &str) -> bool {
        false
    }
    pub fn is_logged_in(&self) -> bool {
        false
    }
}
