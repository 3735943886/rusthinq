//! Evidence-based protocol policy. Unknown model/firmware uses the baseline.
/// LG-005: RTK_RTL8711am requires TLS 1.0/CBC support (firmware unrecorded).
/// Before identity is known, the listener's caller must explicitly select this
/// observed module profile; SNI alone does not establish a device model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsPolicy {
    #[default]
    Baseline,
    RtkRtl8711am,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOwner {
    Local,
    Cloud,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub ack_owner: AckOwner,
    pub first_packet_completes_provisioning: bool,
}

/// LG-001: an active relay owns cloud ACK delivery; otherwise L1 generates them.
/// LG-002: T17A1EFHU_F can omit completeProvisioning_ack (firmware unrecorded).
/// No additional workaround is inferred from an unknown firmware string.
pub fn select(model: Option<&str>, _firmware: Option<&str>, bridge_active: bool) -> Policy {
    Policy {
        ack_owner: if bridge_active {
            AckOwner::Cloud
        } else {
            AckOwner::Local
        },
        first_packet_completes_provisioning: model == Some("T17A1EFHU_F"),
    }
}
