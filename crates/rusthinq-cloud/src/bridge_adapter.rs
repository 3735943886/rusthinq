//! Adapt ConnectedDevice to rusthinq_bridge::LocalDevice.

use crate::devmgr::{ConnectedDevice, Platform, SendToDevice};
use rusthinq_bridge::LocalDevice;
use std::sync::Arc;

pub struct ConnectedAsLocal(pub Arc<ConnectedDevice>);

impl LocalDevice for ConnectedAsLocal {
    fn id(&self) -> &str {
        &self.0.id
    }
    fn platform(&self) -> &str {
        match self.0.platform {
            Platform::Thinq1 => "thinq1",
            Platform::Thinq2 => "thinq2",
        }
    }
    fn model_id(&self) -> &str {
        &self.0.meta.model_id
    }
    fn model_name(&self) -> &str {
        &self.0.meta.model_name
    }
    fn device_type(&self) -> Option<&str> {
        self.0.meta.device_type.as_deref()
    }
    fn on_data(&self, handler: Box<dyn Fn(&[u8]) + Send + Sync>) {
        self.0.add_data_handler(handler);
    }
    fn on_close(&self, handler: Box<dyn Fn() + Send + Sync>) {
        self.0.add_close_handler(handler);
    }
    fn send_to_local(&self, buf: &[u8]) {
        (self.0.send_to_device)(SendToDevice::T2Packet(buf.to_vec()));
    }
    fn send_json_to_local(&self, body: serde_json::Value) {
        (self.0.send_to_device)(SendToDevice::T1Json(body));
    }
    fn send_clip_to_local(&self, payload: serde_json::Value) {
        (self.0.send_to_device)(SendToDevice::T2Raw(payload));
    }
    fn deploy_info(&self) -> Option<(serde_json::Value, serde_json::Value)> {
        self.0.deploy_info.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::devmgr::Platform;
    use rusthinq_bridge::LocalDevice;
    use rusthinq_core::metadata::Metadata;
    use rusthinq_util::sync::Mutex as UtilMutex;

    fn dummy_dev() -> Arc<ConnectedDevice> {
        ConnectedDevice::new(
            "dev-1".into(),
            Platform::Thinq2,
            Metadata {
                model_id: "RAC_056905_WW".into(),
                model_name: "RAC".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(|_m| {}),
        )
    }

    #[test]
    fn send_clip_to_local_forwards_the_payload_unchanged_as_t2_raw() {
        let dev = dummy_dev();
        let sent: Arc<UtilMutex<Vec<SendToDevice>>> = Arc::new(UtilMutex::new(Vec::new()));
        let sent2 = sent.clone();
        // Swap in a send_to_device that records what it's given, so this checks the
        // exact SendToDevice variant/payload ConnectedAsLocal constructs.
        let dev = ConnectedDevice::new(
            dev.id.clone(),
            dev.platform,
            dev.meta.clone(),
            dev.emit_data.clone(),
            Arc::new(move |msg| sent2.lock().push(msg)),
        );
        let local = ConnectedAsLocal(dev);
        let payload = serde_json::json!({"cmd": "ack", "mid": 1785283454163_u64, "data": "AA"});

        local.send_clip_to_local(payload.clone());

        let sent = sent.lock();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            SendToDevice::T2Raw(v) => assert_eq!(v, &payload),
            other => panic!("expected T2Raw, got {other:?}"),
        }
    }

    #[test]
    fn deploy_info_is_none_until_set_then_reflects_what_was_set() {
        let dev = dummy_dev();
        let local = ConnectedAsLocal(dev.clone());
        assert!(local.deploy_info().is_none());

        let app_info = serde_json::json!({"protocolVer": "7"});
        let platform_info = serde_json::json!({"provisioningKey": "RAC"});
        dev.set_deploy_info(app_info.clone(), platform_info.clone());

        assert_eq!(local.deploy_info(), Some((app_info, platform_info)));
    }
}
