//! Pure diffing logic for `rusthinq-retained-gc`: which device ids the
//! `<prefix>/devices` snapshot still knows about, and how to pull an id back out
//! of a retained property/IL topic string, kept broker-free and unit-testable.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashSet;

/// Every device id listed in a `<prefix>/devices` retained payload (see
/// rusthinq-cloud's `devlist.rs`) -- includes ids that are `online: false`, since
/// that snapshot keeps a known-but-currently-disconnected device around rather
/// than dropping it. Only a device that went through `<prefix>/<id>/forget/set`
/// (or was never known at all) is missing from this set.
pub fn known_ids_from_devices_snapshot(payload: &str) -> Result<HashSet<String>> {
    let v: Value = serde_json::from_str(payload)?;
    Ok(v["devices"]
        .as_object()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default())
}

/// Split a retained `<prefix>/<id>/<property>` topic (`MqttSink::publish_property`)
/// into `(id, property)`. Rejects anything not exactly three segments deep, so a
/// sibling topic like `<prefix>/devices` (two segments) or `<prefix>/<id>/bridge/status`
/// (four, and non-retained anyway) can't be mistaken for a property topic.
pub fn parse_property_topic<'a>(topic: &'a str, prefix: &str) -> Option<(&'a str, &'a str)> {
    let rest = topic.strip_prefix(prefix)?.strip_prefix('/')?;
    match rest.split('/').collect::<Vec<_>>().as_slice() {
        [id, property] => Some((id, property)),
        _ => None,
    }
}

/// Split a retained `<il_prefix>/<id>` IL descriptor topic (`ctx.rs`'s
/// `publish_descriptor`) into `id`. Rejects anything not exactly one segment deep.
pub fn parse_il_topic<'a>(topic: &'a str, il_prefix: &str) -> Option<&'a str> {
    let rest = topic.strip_prefix(il_prefix)?.strip_prefix('/')?;
    (!rest.is_empty() && !rest.contains('/')).then_some(rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn known_ids_reads_devices_object_keys() {
        let payload = json!({"devices": {"dev-1": {}, "dev-2": {}}}).to_string();
        let ids = known_ids_from_devices_snapshot(&payload).unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("dev-1"));
        assert!(ids.contains("dev-2"));
    }

    #[test]
    fn known_ids_empty_when_devices_map_is_empty() {
        let payload = json!({"devices": {}}).to_string();
        assert!(
            known_ids_from_devices_snapshot(&payload)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn parse_property_topic_splits_id_and_property() {
        assert_eq!(
            parse_property_topic("rusthinq/dev-1/power", "rusthinq"),
            Some(("dev-1", "power"))
        );
    }

    #[test]
    fn parse_property_topic_rejects_devices_snapshot() {
        assert_eq!(parse_property_topic("rusthinq/devices", "rusthinq"), None);
    }

    #[test]
    fn parse_property_topic_rejects_deeper_bridge_topics() {
        assert_eq!(
            parse_property_topic("rusthinq/dev-1/bridge/status", "rusthinq"),
            None
        );
    }

    #[test]
    fn parse_property_topic_rejects_other_prefix() {
        assert_eq!(parse_property_topic("other/dev-1/power", "rusthinq"), None);
    }

    #[test]
    fn parse_il_topic_extracts_id() {
        assert_eq!(parse_il_topic("il/dev-1", "il"), Some("dev-1"));
    }

    #[test]
    fn parse_il_topic_rejects_deeper_paths() {
        assert_eq!(parse_il_topic("il/dev-1/extra", "il"), None);
    }
}
