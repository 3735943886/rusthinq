//! TLS compatibility policy, chosen by configuration rather than by device model.
/// LG-005: RTK_RTL8711am requires TLS 1.0/CBC support (firmware unrecorded).
/// Before identity is known, the listener's caller must explicitly select this
/// observed module profile; SNI alone does not establish a device model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsPolicy {
    #[default]
    Baseline,
    RtkRtl8711am,
}
