pub mod device;
pub mod firmware;
pub mod provisioning;
pub mod sni_passthrough;

/// Epoch milliseconds for a ThinQ2 message's `mid` field.
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
