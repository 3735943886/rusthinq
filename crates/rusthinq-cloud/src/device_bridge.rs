//! Wire connected ThinQ devices to their DeviceHandler via the registry.

use crate::devmgr::{ConnectedDevice, Platform};
use rusthinq_core::mqtt::MqttConnection;
use rusthinq_core::thinq::{Thinq1Device, Thinq2Device};
use rusthinq_devices::device_trait::DeviceHandler;
use rusthinq_devices::registry::{lookup_t1, lookup_t2};
use rusthinq_util::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// How long a drop waits before it actually publishes "offline", in case the same
/// device id re-registers in time (see `DeviceBridge::new_device`'s doc comment).
const DISCONNECT_GRACE: Duration = Duration::from_millis(2000);

type DataHandler = Box<dyn Fn(&[u8]) + Send + Sync>;
type ResponseHandler = Box<dyn Fn(&serde_json::Value) + Send + Sync>;

/// Adapter: ConnectedDevice → Thinq2Device trait.
struct T2Adapter {
    dev: Arc<ConnectedDevice>,
    handlers: Mutex<Vec<DataHandler>>,
}

impl Thinq2Device for T2Adapter {
    fn id(&self) -> &str {
        &self.dev.id
    }
    fn meta(&self) -> &rusthinq_core::metadata::Metadata {
        &self.dev.meta
    }
    fn send_packet(&self, buf: &[u8]) {
        self.dev
            .notify_send(crate::devmgr::SendToDevice::T2Packet(buf.to_vec()));
        (self.dev.send_to_device)(crate::devmgr::SendToDevice::T2Packet(buf.to_vec()));
    }
    fn send(&self, cmd: &str, msg_type: i32, data: serde_json::Value) {
        let msg = crate::devmgr::SendToDevice::T2Clip {
            cmd: cmd.to_string(),
            msg_type,
            data,
        };
        self.dev.notify_send(msg.clone());
        (self.dev.send_to_device)(msg);
    }
    fn on_data(&self, handler: DataHandler) {
        self.handlers.lock().push(handler);
    }
    fn emit_data(&self, buf: &[u8]) {
        for h in self.handlers.lock().iter() {
            h(buf);
        }
        self.dev.notify_data(buf);
    }
}

struct T1Adapter {
    dev: Arc<ConnectedDevice>,
    handlers: Mutex<Vec<DataHandler>>,
    response_handlers: Mutex<Vec<ResponseHandler>>,
}

impl Thinq1Device for T1Adapter {
    fn id(&self) -> &str {
        &self.dev.id
    }
    fn meta(&self) -> &rusthinq_core::metadata::Metadata {
        &self.dev.meta
    }
    fn send(&self, body: serde_json::Value) {
        (self.dev.send_to_device)(crate::devmgr::SendToDevice::T1Json(body));
    }
    fn on_data(&self, handler: DataHandler) {
        self.handlers.lock().push(handler);
    }
    fn emit_data(&self, buf: &[u8]) {
        for h in self.handlers.lock().iter() {
            h(buf);
        }
        self.dev.notify_data(buf);
    }
    fn on_response(&self, handler: ResponseHandler) {
        self.response_handlers.lock().push(handler);
    }
    fn emit_response(&self, body: &serde_json::Value) {
        for h in self.response_handlers.lock().iter() {
            h(body);
        }
        self.dev.notify_response(body);
    }
}

pub struct DeviceBridge {
    mqtt: Arc<dyn MqttConnection>,
    handlers: Mutex<HashMap<String, Arc<dyn DeviceHandler>>>,
    t2_adapters: Mutex<HashMap<String, Arc<T2Adapter>>>,
    t1_adapters: Mutex<HashMap<String, Arc<T1Adapter>>>,
    /// Drops currently waiting out `grace`, keyed by device id — cancelled
    /// (best-effort) by `new_device` when the same id re-registers in time.
    pending_drops: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Connected devices no handler matched when they connected, so that a script
    /// added later can still be given to them (see `remap`).
    unmapped: Mutex<HashMap<String, Arc<ConnectedDevice>>>,
    grace: Duration,
}

impl DeviceBridge {
    pub fn new(mqtt: Arc<dyn MqttConnection>) -> Arc<Self> {
        Self::new_with_grace(mqtt, DISCONNECT_GRACE)
    }

    /// `grace` is only ever varied by tests — real callers get `DISCONNECT_GRACE`
    /// via [`Self::new`]; a shorter one here keeps a supersede-vs-drop-timing test
    /// from actually waiting out the real-world grace period.
    pub(crate) fn new_with_grace(mqtt: Arc<dyn MqttConnection>, grace: Duration) -> Arc<Self> {
        Arc::new(Self {
            mqtt: mqtt.clone(),
            handlers: Mutex::new(HashMap::new()),
            t2_adapters: Mutex::new(HashMap::new()),
            t1_adapters: Mutex::new(HashMap::new()),
            pending_drops: Mutex::new(HashMap::new()),
            unmapped: Mutex::new(HashMap::new()),
            grace,
        })
    }

    /// Wire MQTT setProperty / discovery onto this bridge (must share the same MqttSink
    /// instance as the MQTT client).
    pub fn attach_mqtt_sink(self: &Arc<Self>, sink: &rusthinq_core::mqtt::MqttSink) {
        let b = self.clone();
        sink.on_set_property(move |id, prop, value| {
            b.set_property(id, prop, value);
        });
        let b2 = self.clone();
        sink.on_discovery(move || {
            b2.republish_all();
        });
    }

    pub fn has_device(&self, id: &str) -> bool {
        self.handlers.lock().contains_key(id)
    }

    pub fn republish_all(&self) {
        // Clone Arcs out of the lock so publish_config (and nested MQTT work)
        // does not block set_property / new_device.
        let devices: Vec<_> = self.handlers.lock().values().cloned().collect();
        for d in devices {
            // Guarded: this whole method is itself one discovery handler running on
            // the shared MQTT event loop task, so a panic in one device's
            // publish_config (e.g. a future Rhai script bug) must not stop the rest
            // of the devices in this loop from being republished.
            let id = d.id().to_string();
            rusthinq_core::panic_guard::guard(&format!("publish_config for {id}"), || {
                d.publish_config();
            });
        }
    }

    pub fn set_property(&self, id: &str, prop: &str, value: &str) {
        if let Some(d) = self.handlers.lock().get(id) {
            // Also guarded here (in addition to the set_handlers loop in mqtt.rs) so a
            // panic mid-way through this device's set_property still leaves the
            // handlers map / this call site in a sane state.
            rusthinq_core::panic_guard::guard(&format!("set_property for {id}"), || {
                d.set_property(prop, value);
            });
        }
    }

    /// A ThinQ appliance can open its replacement connection before the old one's
    /// close event fires (e.g. right after a washer power-cycles). Dropping the
    /// superseded handler here immediately — or leaving a close-triggered drop
    /// waiting behind the grace period still running — would publish "offline" an
    /// instant before this call's `start()` publishes "online" again: a brief
    /// unavailable → available blip on every entity, for no real outage. So a same-id
    /// replacement only cancels pending work (timers, listeners) on the superseded
    /// handler and any drop still waiting for it; only a drop that survives the grace
    /// period with no replacement in time actually publishes offline (see the close
    /// handler installed by `install`, below).
    pub fn new_device(self: &Arc<Self>, thinqdev: Arc<ConnectedDevice>) {
        self.cancel_superseded(&thinqdev.id);
        self.unmapped.lock().remove(&thinqdev.id);

        let handler: Option<Arc<dyn DeviceHandler>> = match thinqdev.platform {
            Platform::Thinq1 => {
                if let Some(factory) = lookup_t1(&thinqdev.meta.model_id) {
                    let adapter = Arc::new(T1Adapter {
                        dev: thinqdev.clone(),
                        handlers: Mutex::new(Vec::new()),
                        response_handlers: Mutex::new(Vec::new()),
                    });
                    let ad = adapter.clone();
                    thinqdev.add_data_handler(move |buf| {
                        for h in ad.handlers.lock().iter() {
                            h(buf);
                        }
                    });
                    let ad = adapter.clone();
                    thinqdev.add_response_handler(move |body| {
                        for h in ad.response_handlers.lock().iter() {
                            h(body);
                        }
                    });
                    self.t1_adapters
                        .lock()
                        .insert(thinqdev.id.clone(), adapter.clone());
                    Some(factory(
                        self.mqtt.clone(),
                        adapter as Arc<dyn Thinq1Device>,
                        thinqdev.meta.clone(),
                    ))
                } else {
                    None
                }
            }
            Platform::Thinq2 => {
                if let Some(factory) = lookup_t2(&thinqdev.meta.model_id) {
                    let adapter = Arc::new(T2Adapter {
                        dev: thinqdev.clone(),
                        handlers: Mutex::new(Vec::new()),
                    });
                    let ad = adapter.clone();
                    thinqdev.add_data_handler(move |buf| {
                        for h in ad.handlers.lock().iter() {
                            h(buf);
                        }
                    });
                    self.t2_adapters
                        .lock()
                        .insert(thinqdev.id.clone(), adapter.clone());
                    Some(factory(
                        self.mqtt.clone(),
                        adapter as Arc<dyn Thinq2Device>,
                        thinqdev.meta.clone(),
                    ))
                } else {
                    None
                }
            }
        };

        let Some(handler) = handler else {
            tracing::warn!(
                "{:?} device type {} unknown",
                thinqdev.platform,
                thinqdev.meta.model_id
            );
            self.unmapped
                .lock()
                .insert(thinqdev.id.clone(), thinqdev.clone());
            let bridge = self.clone();
            let gone = thinqdev.clone();
            thinqdev.add_close_handler(move || {
                let mut unmapped = bridge.unmapped.lock();
                if unmapped
                    .get(&gone.id)
                    .is_some_and(|d| Arc::ptr_eq(d, &gone))
                {
                    unmapped.remove(&gone.id);
                }
            });
            return;
        };

        self.install(thinqdev, handler);
    }

    /// Called after the script hot-reload watcher handled a change: re-publishes every
    /// handler's config (a reload swaps code in but does not re-run `publish_config`, so
    /// a changed descriptor would otherwise stay unpublished until the next reconnect),
    /// then gives a handler to each connected device that had none and now has a script.
    #[cfg(feature = "scripting")]
    pub fn remap(self: &Arc<Self>) {
        self.republish_all();
        let pending: Vec<_> = self.unmapped.lock().values().cloned().collect();
        for dev in pending {
            let known = match dev.platform {
                Platform::Thinq1 => lookup_t1(&dev.meta.model_id).is_some(),
                Platform::Thinq2 => lookup_t2(&dev.meta.model_id).is_some(),
            };
            if known {
                tracing::info!(
                    "{}: a script for {} appeared — attaching it",
                    dev.id,
                    dev.meta.model_id
                );
                self.new_device(dev);
            }
        }
    }

    /// Cancels any drop still waiting out the grace period for `id`, and cancels
    /// pending work on (but does not drop) whatever handler is currently registered
    /// for it — see `new_device`'s doc comment.
    fn cancel_superseded(&self, id: &str) {
        if let Some(task) = self.pending_drops.lock().remove(id) {
            task.abort();
        }
        if let Some(old) = self.handlers.lock().remove(id) {
            rusthinq_core::panic_guard::guard(&format!("cancel_pending_work for {id}"), || {
                old.cancel_pending_work()
            });
        }
    }

    /// Test-only entry point exercising the exact `new_device` code path (supersede
    /// cancellation, adapter registration so `install`'s close handler can tell this
    /// connection apart from a same-id replacement, then install) with a handler
    /// supplied directly instead of looked up from the registry, which is empty until
    /// Phase 3 device handlers exist.
    #[cfg(test)]
    pub(crate) fn new_device_with_handler(
        self: &Arc<Self>,
        thinqdev: Arc<ConnectedDevice>,
        handler: Arc<dyn DeviceHandler>,
    ) {
        self.cancel_superseded(&thinqdev.id);
        let adapter = Arc::new(T2Adapter {
            dev: thinqdev.clone(),
            handlers: Mutex::new(Vec::new()),
        });
        self.t2_adapters.lock().insert(thinqdev.id.clone(), adapter);
        self.install(thinqdev, handler);
    }

    /// Registers `handler` as the live handler for `thinqdev.id` and wires its
    /// close-triggered (grace-period) drop. Split out of `new_device` so the
    /// supersede/drop-timing logic can be exercised directly in tests without needing
    /// a real entry in the (currently empty, pending Phase 3) device registry.
    fn install(self: &Arc<Self>, thinqdev: Arc<ConnectedDevice>, handler: Arc<dyn DeviceHandler>) {
        let id = thinqdev.id.clone();
        self.handlers.lock().insert(id.clone(), handler.clone());
        let bridge = self.clone();
        // Only tear down the handler when *this* ConnectedDevice closes — not when a
        // superseded reconnect's stale close handler runs after re-accept.
        let thinq_for_close = thinqdev.clone();
        thinqdev.add_close_handler(move || {
            // Deferred rather than checked-and-dropped here: at the moment *this*
            // event fires there may be no replacement yet, but one can still arrive
            // within the grace period (see new_device's doc comment above). So this
            // only schedules the drop; whether it actually happens is decided when it
            // fires, by re-checking ownership then — not now — since new_device's
            // task.abort() is best-effort and cannot be relied on alone (cooperative
            // cancellation only preempts at an await point, and this closure has none
            // left once the sleep is done).
            let bridge_task = bridge.clone();
            let id_task = id.clone();
            let thinq_for_close_task = thinq_for_close.clone();
            let grace = bridge_task.grace;
            let task = tokio::spawn(async move {
                tokio::time::sleep(grace).await;
                bridge_task.pending_drops.lock().remove(&id_task);

                let still_ours = {
                    let t2 = bridge_task.t2_adapters.lock();
                    if let Some(a) = t2.get(&id_task) {
                        Arc::ptr_eq(&a.dev, &thinq_for_close_task)
                    } else {
                        drop(t2);
                        bridge_task
                            .t1_adapters
                            .lock()
                            .get(&id_task)
                            .map(|a| Arc::ptr_eq(&a.dev, &thinq_for_close_task))
                            .unwrap_or(false)
                    }
                };
                if !still_ours {
                    return;
                }
                if let Some(handler) = bridge_task.handlers.lock().remove(&id_task) {
                    rusthinq_core::panic_guard::guard(
                        &format!("drop_device for {id_task}"),
                        || {
                            handler.drop_device();
                        },
                    );
                }
                // This survived the grace period with no same-id replacement, so the
                // local handler is torn down -- but retained MQTT state is *not*
                // cleared here anymore (it used to be, on the theory that 2s without
                // a reconnect means "permanently gone"). A device that's actually
                // coming back later (travel router down overnight, a power outage)
                // would otherwise lose its whole retained state for no reason, and
                // one that's genuinely gone for good would vanish from
                // known_devices()/devlist.rs's snapshot within seconds -- before a
                // human ever gets to see it as "online: false" and decide to forget
                // it (issue #9's whole point). `MqttSink` still knows this id from
                // `published_topics`/`known_devices()`, so it keeps showing up as
                // offline with a last-seen time; `device_control.rs`'s explicit
                // `forget` is now the only thing that calls `clear_retained`.
                bridge_task.t2_adapters.lock().remove(&id_task);
                bridge_task.t1_adapters.lock().remove(&id_task);
            });
            bridge.pending_drops.lock().insert(id.clone(), task);
        });
        let start_id = thinqdev.id.clone();
        rusthinq_core::panic_guard::guard(&format!("start for {start_id}"), || {
            handler.start();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusthinq_core::MockMqttConnection;
    use rusthinq_core::metadata::Metadata;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy_dev(id: &str) -> Arc<ConnectedDevice> {
        ConnectedDevice::new(
            id.into(),
            Platform::Thinq2,
            Metadata {
                model_id: "UNREGISTERED_MODEL".into(),
                model_name: "UNREGISTERED_MODEL".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(|_m| {}),
        )
    }

    #[derive(Default)]
    struct FakeDeviceHandler {
        id: String,
        start_calls: AtomicUsize,
        drop_calls: AtomicUsize,
        cancel_calls: AtomicUsize,
    }

    impl FakeDeviceHandler {
        fn new(id: &str) -> Arc<Self> {
            Arc::new(Self {
                id: id.into(),
                ..Default::default()
            })
        }
    }

    impl DeviceHandler for FakeDeviceHandler {
        fn id(&self) -> &str {
            &self.id
        }
        fn start(&self) {
            self.start_calls.fetch_add(1, Ordering::SeqCst);
        }
        fn drop_device(&self) {
            self.drop_calls.fetch_add(1, Ordering::SeqCst);
        }
        fn set_property(&self, _prop: &str, _value: &str) {}
        fn publish_config(&self) {}
        fn cancel_pending_work(&self) {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The bug this whole grace-period mechanism exists to fix: a ThinQ appliance can
    /// open its replacement connection before the old one's close event fires, and
    /// dropping the superseded handler right away (or letting a close-triggered drop
    /// fire on schedule) would publish "offline" for an instant right before the
    /// replacement publishes "online" — a flicker on every entity, for no real outage.
    #[tokio::test]
    async fn replacement_within_grace_cancels_the_pending_drop_instead_of_publishing_offline() {
        let mqtt = MockMqttConnection::new();
        let bridge = DeviceBridge::new_with_grace(mqtt.clone(), Duration::from_millis(150));

        let dev_a = dummy_dev("dev-1");
        let handler_a = FakeDeviceHandler::new("dev-1");
        bridge.new_device_with_handler(dev_a.clone(), handler_a.clone());
        assert_eq!(handler_a.start_calls.load(Ordering::SeqCst), 1);

        // The old connection's close fires — this only *schedules* a drop.
        dev_a.notify_close();
        assert_eq!(handler_a.drop_calls.load(Ordering::SeqCst), 0);

        // Well within the grace period, the replacement connection registers.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let dev_b = dummy_dev("dev-1");
        let handler_b = FakeDeviceHandler::new("dev-1");
        bridge.new_device_with_handler(dev_b.clone(), handler_b.clone());

        assert_eq!(
            handler_a.cancel_calls.load(Ordering::SeqCst),
            1,
            "superseded handler must release its pending work"
        );
        assert_eq!(handler_b.start_calls.load(Ordering::SeqCst), 1);

        // Long enough for the original close's grace period to have elapsed — the
        // cancelled drop must never have fired.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            handler_a.drop_calls.load(Ordering::SeqCst),
            0,
            "a superseded handler must never publish offline"
        );
        assert_eq!(
            handler_b.drop_calls.load(Ordering::SeqCst),
            0,
            "the live replacement must not be dropped either"
        );
    }

    /// The other half: a device that is genuinely gone (no replacement shows up) must
    /// still end up offline — the grace period delays this, it does not cancel it.
    #[tokio::test]
    async fn close_with_no_replacement_drops_after_the_grace_period() {
        let mqtt = MockMqttConnection::new();
        let bridge = DeviceBridge::new_with_grace(mqtt.clone(), Duration::from_millis(50));

        let dev = dummy_dev("dev-1");
        let handler = FakeDeviceHandler::new("dev-1");
        bridge.new_device_with_handler(dev.clone(), handler.clone());

        dev.notify_close();
        assert_eq!(handler.drop_calls.load(Ordering::SeqCst), 0);

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(handler.drop_calls.load(Ordering::SeqCst), 1);
    }

    /// Issue #9 regression: this grace-period drop used to also call
    /// `mqtt.clear_retained(id)`, which wiped the device out of
    /// `MqttSink::known_devices()` within seconds of any real disconnect -- long
    /// before a human could ever see it listed as `online: false` in
    /// `devlist.rs`'s snapshot and decide whether to forget it. Retained state (and
    /// `known_devices()`) must survive this drop; only an explicit `forget`
    /// (device_control.rs) clears it now.
    #[tokio::test]
    async fn close_with_no_replacement_does_not_clear_retained_mqtt_state() {
        let mqtt = MockMqttConnection::new();
        mqtt.publish_property("dev-1", "power", "ON");
        let bridge = DeviceBridge::new_with_grace(mqtt.clone(), Duration::from_millis(50));

        let dev = dummy_dev("dev-1");
        let handler = FakeDeviceHandler::new("dev-1");
        bridge.new_device_with_handler(dev.clone(), handler.clone());

        dev.notify_close();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            handler.drop_calls.load(Ordering::SeqCst),
            1,
            "still drops locally"
        );

        assert!(
            mqtt.device("dev-1").is_some(),
            "retained MQTT properties must survive the grace-period drop"
        );
        assert_eq!(
            mqtt.known_devices().len(),
            1,
            "the id must still be known (and thus listable as offline) after the drop"
        );
    }

    /// A device that connected before its script existed gets the script once `remap`
    /// runs (what the hot-reload watcher triggers), without reconnecting.
    #[cfg(feature = "scripting")]
    #[test]
    fn remap_attaches_a_script_that_appeared_after_the_device_connected() {
        const MODEL: &str = "REMAP_TEST_MODEL";
        let dir = std::env::temp_dir().join(format!("remap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        rusthinq_devices::scripting::init(dir.clone(), false);

        let bridge = DeviceBridge::new(MockMqttConnection::new());
        let dev = ConnectedDevice::new(
            "remap-dev".into(),
            Platform::Thinq2,
            Metadata {
                model_id: MODEL.into(),
                model_name: MODEL.into(),
                device_type: None,
                sw_version: None,
            },
            Arc::new(|_b| {}),
            Arc::new(|_m| {}),
        );
        bridge.new_device(dev);
        assert!(!bridge.has_device("remap-dev"), "no script yet");

        bridge.remap();
        assert!(!bridge.has_device("remap-dev"), "still no script");

        std::fs::write(dir.join(format!("{MODEL}.rhai")), "fn start(ctx) {}").unwrap();
        bridge.remap();
        assert!(
            bridge.has_device("remap-dev"),
            "the new script was attached"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
