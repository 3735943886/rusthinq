//! Device manager — tracks connected ThinQ1/2 appliances.

use rusthinq_core::metadata::Metadata;
use rusthinq_util::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Thinq1,
    Thinq2,
}

impl Platform {
    pub fn as_str(&self) -> &'static str {
        match self {
            Platform::Thinq1 => "thinq1",
            Platform::Thinq2 => "thinq2",
        }
    }
}

/// A `T` that doesn't exist yet at the point a closure needing it is built (e.g. a
/// `ConnectedDevice` an `on_data`/`on_close` handler must reach, but which can only be
/// constructed once those handlers already exist), filled in once it does. Clone
/// shares the same backing cell, so every clone taken before `set` sees later calls.
#[derive(Clone)]
pub struct DeferredSlot<T>(Arc<Mutex<Option<T>>>);

impl<T: Clone> DeferredSlot<T> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    pub fn set(&self, value: T) {
        *self.0.lock() = Some(value);
    }

    pub fn get(&self) -> Option<T> {
        self.0.lock().clone()
    }

    /// Takes the value out, leaving the slot empty again.
    pub fn take(&self) -> Option<T> {
        self.0.lock().take()
    }
}

impl<T: Clone> Default for DeferredSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

type EmitDataFn = Arc<dyn Fn(Vec<u8>) + Send + Sync>;
type SendToDeviceFn = Arc<dyn Fn(SendToDevice) + Send + Sync>;
type CloseHandler = Box<dyn Fn() + Send + Sync>;
type DataHandler = Box<dyn Fn(&[u8]) + Send + Sync>;
type SendHandler = Box<dyn Fn(SendToDevice) + Send + Sync>;
type ResponseHandler = Box<dyn Fn(&serde_json::Value) + Send + Sync>;
type UnhandledClipHandler = Box<dyn Fn(serde_json::Value) + Send + Sync>;

/// Handle to a connected appliance (platform-agnostic view for management/integrations).
pub struct ConnectedDevice {
    pub id: String,
    pub platform: Platform,
    pub meta: Metadata,
    /// Inject data as if from appliance (T2: raw packet bytes hex path).
    pub emit_data: EmitDataFn,
    /// Send to appliance.
    pub send_to_device: SendToDeviceFn,
    pub on_close: Mutex<Vec<CloseHandler>>,
    pub on_data: Mutex<Vec<DataHandler>>,
    pub on_send: Mutex<Vec<SendHandler>>,
    /// ThinQ1 command-ack envelopes (a `Body` carrying `ReturnCode`) — see
    /// `thinq1/connection.rs`'s `on_response`. Always empty for ThinQ2 devices.
    pub on_response: Mutex<Vec<ResponseHandler>>,
    /// The ThinQ2 appliance's own (appInfo, platformInfo) from its deploy message —
    /// set once it completes provisioning locally (see thinq2/device.rs). Read by the
    /// LG bridge so it can introduce the appliance upstream as what it actually is
    /// instead of a fixed placeholder (see `rusthinq_bridge::pair::format_pre_deploy`).
    /// `None` for ThinQ1 devices and for a ThinQ2 device that hasn't deployed yet.
    pub deploy_info: Mutex<Option<(serde_json::Value, serde_json::Value)>>,
    /// A CLIP message from the appliance that nothing else here has a handler for
    /// (e.g. its `respUniversalCtrl` answer to a liveness check) — given to whoever's
    /// listening (the LG bridge, so it can carry the answer up) instead of being
    /// silently dropped. Always empty for ThinQ1 devices.
    pub on_unhandled_clip: Mutex<Vec<UnhandledClipHandler>>,
    /// Set by the device's driver when it acks the appliance's frames itself (a Rhai
    /// driver's `ctx.set_auto_ack`). The LG bridge then stops relaying the cloud's own
    /// acks, so each frame is acked once. Always false for ThinQ1 devices.
    pub auto_ack: AtomicBool,
}

#[derive(Debug, Clone)]
pub enum SendToDevice {
    /// Raw AABB/TLV packet (published as CLIP cmd=packet).
    T2Packet(Vec<u8>),
    /// Arbitrary CLIP command (setMaskingInfo, etc.).
    T2Clip {
        cmd: String,
        msg_type: i32,
        data: serde_json::Value,
    },
    /// A full CLIP envelope forwarded byte-for-byte, mid included — used to relay a
    /// bridged cloud message to the local device unchanged (see bridge_adapter.rs).
    /// Unlike `T2Clip`, nothing here is rebuilt: an ack's mid has to reach the
    /// appliance exactly as the cloud sent it, or the appliance can't match it to its
    /// own pending message and just repeats what it was acking.
    /// Only ever constructed by bridge_adapter.rs, which is `#[cfg(feature = "bridge")]`.
    #[allow(dead_code)]
    T2Raw(serde_json::Value),
    T1Json(serde_json::Value),
}

impl ConnectedDevice {
    pub fn new(
        id: String,
        platform: Platform,
        meta: Metadata,
        emit_data: EmitDataFn,
        send_to_device: SendToDeviceFn,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            platform,
            meta,
            emit_data,
            send_to_device,
            on_close: Mutex::new(Vec::new()),
            on_data: Mutex::new(Vec::new()),
            on_send: Mutex::new(Vec::new()),
            on_response: Mutex::new(Vec::new()),
            deploy_info: Mutex::new(None),
            on_unhandled_clip: Mutex::new(Vec::new()),
            auto_ack: AtomicBool::new(false),
        })
    }

    pub fn set_deploy_info(&self, app_info: serde_json::Value, platform_info: serde_json::Value) {
        *self.deploy_info.lock() = Some((app_info, platform_info));
    }

    /// Registered by `bridge_adapter.rs`'s `LocalDevice::on_unhandled_clip` (the
    /// `bridge` feature) and by `raw_bus.rs`'s `clip_rx` stream, which is compiled in
    /// every build -- so this must not be gated on `bridge`.
    pub fn add_unhandled_clip_handler<F: Fn(serde_json::Value) + Send + Sync + 'static>(
        &self,
        f: F,
    ) {
        self.on_unhandled_clip.lock().push(Box::new(f));
    }

    /// Guarded per handler, same reasoning as `notify_data`.
    pub fn notify_unhandled_clip(&self, payload: serde_json::Value) {
        for h in self.on_unhandled_clip.lock().iter() {
            let payload = payload.clone();
            rusthinq_core::panic_guard::guard(
                &format!("on_unhandled_clip handler for {}", self.id),
                || h(payload),
            );
        }
    }

    /// Guarded per handler: `on_data` chains several independent concerns (raw wire
    /// tap, downstream-integration translation, LG bridge forwarding) for the same
    /// device — a panic in one (a malformed packet tripping a future Rhai
    /// handler bug) must not stop the others from seeing this frame, nor unwind
    /// whatever task delivered it.
    pub fn notify_data(&self, buf: &[u8]) {
        for h in self.on_data.lock().iter() {
            rusthinq_core::panic_guard::guard(&format!("on_data handler for {}", self.id), || {
                h(buf)
            });
        }
    }

    pub fn notify_send(&self, msg: SendToDevice) {
        for h in self.on_send.lock().iter() {
            let msg = msg.clone();
            rusthinq_core::panic_guard::guard(&format!("on_send handler for {}", self.id), || {
                h(msg)
            });
        }
    }

    pub fn notify_response(&self, body: &serde_json::Value) {
        for h in self.on_response.lock().iter() {
            rusthinq_core::panic_guard::guard(
                &format!("on_response handler for {}", self.id),
                || h(body),
            );
        }
    }

    pub fn notify_close(&self) {
        for h in self.on_close.lock().iter() {
            rusthinq_core::panic_guard::guard(&format!("on_close handler for {}", self.id), h);
        }
    }

    pub fn add_close_handler<F: Fn() + Send + Sync + 'static>(&self, f: F) {
        self.on_close.lock().push(Box::new(f));
    }

    pub fn add_data_handler<F: Fn(&[u8]) + Send + Sync + 'static>(&self, f: F) {
        self.on_data.lock().push(Box::new(f));
    }

    pub fn add_send_handler<F: Fn(SendToDevice) + Send + Sync + 'static>(&self, f: F) {
        self.on_send.lock().push(Box::new(f));
    }

    pub fn add_response_handler<F: Fn(&serde_json::Value) + Send + Sync + 'static>(&self, f: F) {
        self.on_response.lock().push(Box::new(f));
    }
}

type NewDeviceHandler = Box<dyn Fn(Arc<ConnectedDevice>) + Send + Sync>;
type DropDeviceHandler = Box<dyn Fn(&str) + Send + Sync>;

pub struct DeviceManager {
    devices: Mutex<HashMap<String, Arc<ConnectedDevice>>>,
    on_new: Mutex<Vec<NewDeviceHandler>>,
    on_drop: Mutex<Vec<DropDeviceHandler>>,
}

impl DeviceManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            devices: Mutex::new(HashMap::new()),
            on_new: Mutex::new(Vec::new()),
            on_drop: Mutex::new(Vec::new()),
        })
    }

    pub fn accept(self: &Arc<Self>, device: Arc<ConnectedDevice>) {
        let id = device.id.clone();
        {
            let mut map = self.devices.lock();
            map.insert(id.clone(), device.clone());
        }
        let mgr = self.clone();
        let dev_id = id.clone();
        // Capture this Arc so close only removes *this* connection.
        // Matching by device id alone races: on ThinQ2 reconnect the old MQTT
        // client's disconnect can run after the new device is accepted and
        // wrongly wipe the live entry (UI empty, downstream integration shows the
        // device unavailable, packets still logged against the new MQTT session).
        let device_for_close = device.clone();
        device.add_close_handler(move || {
            let mut map = mgr.devices.lock();
            let still_ours = map
                .get(&dev_id)
                .map(|current| Arc::ptr_eq(current, &device_for_close))
                .unwrap_or(false);
            if still_ours {
                map.remove(&dev_id);
                drop(map);
                for h in mgr.on_drop.lock().iter() {
                    let dev_id = dev_id.clone();
                    rusthinq_core::panic_guard::guard(
                        &format!("on_drop_device handler for {dev_id}"),
                        || h(&dev_id),
                    );
                }
            }
        });
        for h in self.on_new.lock().iter() {
            let device = device.clone();
            rusthinq_core::panic_guard::guard(
                &format!("on_new_device handler for {}", device.id),
                || h(device),
            );
        }
    }

    pub fn on_new_device<F: Fn(Arc<ConnectedDevice>) + Send + Sync + 'static>(&self, f: F) {
        self.on_new.lock().push(Box::new(f));
    }

    pub fn on_drop_device<F: Fn(&str) + Send + Sync + 'static>(&self, f: F) {
        self.on_drop.lock().push(Box::new(f));
    }

    pub fn get(&self, id: &str) -> Option<Arc<ConnectedDevice>> {
        self.devices.lock().get(id).cloned()
    }

    pub fn all(&self) -> HashMap<String, Arc<ConnectedDevice>> {
        self.devices.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy_dev(id: &str) -> Arc<ConnectedDevice> {
        ConnectedDevice::new(
            id.into(),
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
    fn close_of_old_connection_does_not_drop_reconnected_device() {
        let mgr = DeviceManager::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let d = drops.clone();
        mgr.on_drop_device(move |_| {
            d.fetch_add(1, Ordering::SeqCst);
        });

        let first = dummy_dev("dev-1");
        mgr.accept(first.clone());
        assert!(mgr.get("dev-1").is_some());

        // Reconnect: new Arc for same id
        let second = dummy_dev("dev-1");
        mgr.accept(second.clone());
        assert!(mgr.get("dev-1").is_some());
        assert!(Arc::ptr_eq(&mgr.get("dev-1").unwrap(), &second));

        // Old connection closes — must not remove the live second entry
        first.notify_close();
        assert!(
            mgr.get("dev-1").is_some(),
            "reconnect survivor must stay in manager"
        );
        assert!(Arc::ptr_eq(&mgr.get("dev-1").unwrap(), &second));
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        // Live connection closes — then drop
        second.notify_close();
        assert!(mgr.get("dev-1").is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    /// Failure isolation: `notify_data` chains independent concerns (raw tap,
    /// downstream-integration translation, LG bridge forwarding) for the same
    /// device. A bad packet tripping
    /// a bug in one handler (e.g. a future Rhai script) must not stop a sibling
    /// handler from seeing the same frame.
    #[test]
    fn notify_data_runs_sibling_handlers_after_one_panics() {
        let dev = dummy_dev("dev-1");
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();

        dev.add_data_handler(|_buf| panic!("simulated malformed-packet bug"));
        dev.add_data_handler(move |_buf| {
            h.fetch_add(1, Ordering::SeqCst);
        });

        dev.notify_data(&[0xaa, 0xff]);
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // And the device keeps working for the next frame too.
        dev.notify_data(&[0xaa, 0xff]);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn notify_unhandled_clip_reaches_the_handler_with_the_payload() {
        let dev = dummy_dev("dev-1");
        let received: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let r = received.clone();
        dev.add_unhandled_clip_handler(move |payload| r.lock().push(payload));

        let payload = serde_json::json!({"cmd": "respUniversalCtrl", "reqType": "online_check"});
        dev.notify_unhandled_clip(payload.clone());

        assert_eq!(received.lock().clone(), vec![payload]);
    }

    /// Failure isolation across devices: one device's on_new_device handler panicking
    /// (e.g. a broken downstream-integration translation for a malformed metadata
    /// field) must not stop the manager from accepting and dispatching for a
    /// different device.
    #[test]
    fn on_new_device_panic_for_one_device_does_not_block_another() {
        let mgr = DeviceManager::new();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();

        mgr.on_new_device(move |dev| {
            if dev.id == "bad-device" {
                panic!("simulated bug handling bad-device");
            }
            h.fetch_add(1, Ordering::SeqCst);
        });

        mgr.accept(dummy_dev("bad-device"));
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        mgr.accept(dummy_dev("good-device"));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert!(mgr.get("good-device").is_some());
    }
}
