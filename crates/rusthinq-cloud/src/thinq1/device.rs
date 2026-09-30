//! ThinQ1 device acceptor over TLS streams.

use super::connection::{T1ConnectionEvents, run_connection_with_acks};
use super::http::MetaStore;
use crate::devmgr::{ConnectedDevice, DeferredSlot, DeviceManager, Platform, SendToDevice};
use rusthinq_util::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Every live RTI socket currently serving one ThinQ1 device id. Some appliances
/// (observed on air purifiers) open a second RTI socket for the same device id
/// alongside the first instead of replacing it — treating the new one as a stale
/// reconnect and killing the old one (as this used to) can flap the device mid-use.
/// Both sockets live under the one shared `ConnectedDevice`; outbound commands go out
/// the most recently attached (and presumably still-live) one, while status/response
/// frames from *either* socket reach the same device — see `DeviceAcceptor::accept`.
struct Thinq1Sockets {
    conns: Mutex<Vec<(u64, mpsc::UnboundedSender<serde_json::Value>)>>,
}

impl Thinq1Sockets {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            conns: Mutex::new(Vec::new()),
        })
    }

    fn add(&self, conn_id: u64, tx: mpsc::UnboundedSender<serde_json::Value>) {
        self.conns.lock().push((conn_id, tx));
    }

    /// Drops `conn_id`'s socket; returns `true` if no sockets remain afterwards.
    fn remove(&self, conn_id: u64) -> bool {
        let mut conns = self.conns.lock();
        conns.retain(|(id, _)| *id != conn_id);
        conns.is_empty()
    }

    /// Sends via the most recently attached socket; silently dropped if none remain
    /// (matches the pre-existing behavior of sending into a channel nobody reads).
    fn send(&self, msg: serde_json::Value) {
        if let Some((_, tx)) = self.conns.lock().last() {
            let _ = tx.send(msg);
        }
    }
}

type DeviceEntry = (Arc<Thinq1Sockets>, Arc<ConnectedDevice>);

pub struct DeviceAcceptor {
    meta: MetaStore,
    manager: Arc<DeviceManager>,
    /// device_id → its live sockets and the `ConnectedDevice` they share.
    devices: Mutex<HashMap<String, DeviceEntry>>,
    next_conn_id: AtomicU64,
}

impl DeviceAcceptor {
    pub fn new(meta: MetaStore, manager: Arc<DeviceManager>) -> Arc<Self> {
        Arc::new(Self {
            meta,
            manager,
            devices: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
        })
    }

    pub async fn accept<S>(self: &Arc<Self>, stream: S)
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();

        let tx_for_ack = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = ack_rx.recv().await {
                let _ = tx_for_ack.send(msg);
            }
        });

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let device_slot: DeferredSlot<Arc<ConnectedDevice>> = DeferredSlot::new();
        let sockets_slot: DeferredSlot<Arc<Thinq1Sockets>> = DeferredSlot::new();
        let device_slot_init = device_slot.clone();
        let device_slot_status = device_slot.clone();
        let device_slot_response = device_slot.clone();
        let device_slot_close = device_slot.clone();
        let sockets_slot_init = sockets_slot.clone();
        let sockets_slot_close = sockets_slot.clone();
        let acceptor = self.clone();
        let acceptor_close = self.clone();
        let tx_init = tx.clone();

        let events = T1ConnectionEvents {
            on_init: Arc::new(move |device_id: String| {
                let meta = acceptor.meta.lock().get(&device_id).cloned();
                let Some(meta) = meta else {
                    tracing::warn!("device {device_id} metadata not known, send HTTP POST first!");
                    return;
                };

                let mut devices = acceptor.devices.lock();
                let (sockets, dev, is_new) = if let Some((sockets, dev)) = devices.get(&device_id) {
                    tracing::debug!("device {device_id} opened an additional connection");
                    (sockets.clone(), dev.clone(), false)
                } else {
                    let sockets = Thinq1Sockets::new();

                    let sockets_for_send = sockets.clone();
                    let id_send = device_id.clone();
                    let send_to = Arc::new(move |msg: SendToDevice| {
                        if let SendToDevice::T1Json(body) = msg {
                            let mut body_obj = body.as_object().cloned().unwrap_or_default();
                            body_obj.insert(
                                "CmdWId".into(),
                                serde_json::json!(format!("n-{}", Uuid::new_v4())),
                            );
                            let packet = serde_json::json!({
                                "Header": { "x-lgedm-deviceId": id_send },
                                "Body": body_obj,
                            });
                            sockets_for_send.send(packet);
                        }
                    });

                    let device_slot_emit: DeferredSlot<std::sync::Weak<ConnectedDevice>> =
                        DeferredSlot::new();
                    let slot = device_slot_emit.clone();
                    let emit = Arc::new(move |buf: Vec<u8>| {
                        if let Some(dev) = slot.get().and_then(|dev| dev.upgrade()) {
                            dev.notify_data(&buf);
                        }
                    });

                    let dev = ConnectedDevice::new(
                        device_id.clone(),
                        Platform::Thinq1,
                        meta,
                        emit,
                        send_to,
                    );
                    device_slot_emit.set(Arc::downgrade(&dev));
                    devices.insert(device_id.clone(), (sockets.clone(), dev.clone()));
                    (sockets, dev, true)
                };
                drop(devices);

                sockets.add(conn_id, tx_init.clone());
                sockets_slot_init.set(sockets);
                device_slot_init.set(dev.clone());
                if is_new {
                    acceptor.manager.accept(dev);
                }
            }),
            on_status: Arc::new(move |buf: Vec<u8>| {
                if let Some(dev) = device_slot_status.get() {
                    dev.notify_data(&buf);
                }
            }),
            on_response: Arc::new(move |body: serde_json::Value| {
                if let Some(dev) = device_slot_response.get() {
                    dev.notify_response(&body);
                }
            }),
            on_close: Arc::new(move || {
                let Some(dev) = device_slot_close.take() else {
                    return;
                };
                let Some(sockets) = sockets_slot_close.take() else {
                    return;
                };
                // Only tear the device down once every socket serving it has closed —
                // an appliance that opened a second RTI socket may still be using the
                // first one, or vice versa.
                if sockets.remove(conn_id) {
                    acceptor_close.devices.lock().remove(&dev.id);
                    dev.notify_close();
                }
            }),
        };

        run_connection_with_acks(stream, events, rx, ack_tx).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::wait_until as wait_for;
    use crate::thinq1::http::device_metadata_store;
    use rusthinq_core::metadata::Metadata;
    use rusthinq_util::length_prefixed_frame;
    use tokio::io::DuplexStream;

    fn alive_frame(device_id: &str) -> Vec<u8> {
        let body = serde_json::json!({
            "Header": { "x-lgedm-deviceId": device_id },
            "Body": { "Cmd": "Alive" },
        })
        .to_string();
        length_prefixed_frame::make(body.as_bytes())
    }

    fn status_frame(device_id: &str, data_b64: &str) -> Vec<u8> {
        let body = serde_json::json!({
            "Header": { "x-lgedm-deviceId": device_id },
            "Body": { "Cmd": "Mon", "Format": "B64", "Data": data_b64 },
        })
        .to_string();
        length_prefixed_frame::make(body.as_bytes())
    }

    fn response_frame(device_id: &str, return_code: &str, cmd_wid: &str) -> Vec<u8> {
        let body = serde_json::json!({
            "Header": { "x-lgedm-deviceId": device_id },
            "Body": { "ReturnCode": return_code, "CmdWId": cmd_wid },
        })
        .to_string();
        length_prefixed_frame::make(body.as_bytes())
    }

    async fn socket_pair() -> (DuplexStream, DuplexStream) {
        tokio::io::duplex(64 * 1024)
    }

    /// Real (not mocked) flow: two independent duplex-stream "sockets" driven through
    /// the actual `DeviceAcceptor`/`run_connection_with_acks` code a real TLS
    /// connection would use.
    #[tokio::test]
    async fn parallel_sockets_for_one_device_share_a_connecteddevice_and_dont_flap_it() {
        let meta_store = device_metadata_store();
        let device_id = "air-purifier-1";
        meta_store.lock().insert(
            device_id.to_string(),
            Metadata {
                model_id: "AIR_910604_WW".into(),
                model_name: "AIR_910604_WW".into(),
                device_type: Some("401".into()),
                sw_version: None,
            },
        );
        let manager = DeviceManager::new();
        let new_devices = Arc::new(Mutex::new(0usize));
        let dropped_devices = Arc::new(Mutex::new(0usize));
        {
            let n = new_devices.clone();
            manager.on_new_device(move |_| *n.lock() += 1);
        }
        {
            let d = dropped_devices.clone();
            manager.on_drop_device(move |_| *d.lock() += 1);
        }
        let acceptor = DeviceAcceptor::new(meta_store, manager.clone());

        let (mut first_local, first_remote) = socket_pair().await;
        let acc1 = acceptor.clone();
        tokio::spawn(async move { acc1.accept(first_remote).await });
        first_local
            .write_all(&alive_frame(device_id))
            .await
            .unwrap();

        // Give the first socket's Alive frame time to register the device before the
        // second socket opens — this test is about *both staying up*, not a race.
        wait_for(|| manager.get(device_id).is_some()).await;
        assert_eq!(
            *new_devices.lock(),
            1,
            "only one ConnectedDevice for both sockets"
        );

        let (mut second_local, second_remote) = socket_pair().await;
        let acc2 = acceptor.clone();
        tokio::spawn(async move { acc2.accept(second_remote).await });
        second_local
            .write_all(&alive_frame(device_id))
            .await
            .unwrap();
        // No reliable synchronous signal for "the second socket registered" from the
        // outside, so drive real data through it below instead of asserting on timing.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            *new_devices.lock(),
            1,
            "a second socket for the same id must not be treated as a new device"
        );

        let dev = manager.get(device_id).unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        {
            let r = received.clone();
            dev.add_data_handler(move |buf| r.lock().push(buf.to_vec()));
        }

        // Status from the *first* socket must still reach the shared device.
        first_local
            .write_all(&status_frame(device_id, "AQI="))
            .await
            .unwrap();
        wait_for(|| !received.lock().is_empty()).await;
        assert_eq!(received.lock().len(), 1);

        // Closing the first socket must not drop the device — the second is still up.
        drop(first_local);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            *dropped_devices.lock(),
            0,
            "second socket keeps the device alive"
        );
        assert!(manager.get(device_id).is_some());

        // Status from the *second* socket (the one that outlived the first) must also
        // still reach the same shared device.
        second_local
            .write_all(&status_frame(device_id, "Ag=="))
            .await
            .unwrap();
        wait_for(|| received.lock().len() >= 2).await;
        assert_eq!(received.lock().len(), 2);

        // Only once the *last* socket closes does the device actually drop.
        drop(second_local);
        wait_for(|| *dropped_devices.lock() == 1).await;
        assert!(manager.get(device_id).is_none());
    }

    /// Real (not mocked) flow, driven through the actual `DeviceAcceptor`: a
    /// command-ack envelope (`ReturnCode`) must reach `ConnectedDevice::on_response`
    /// so a driver can react to its own command's ack instead of waiting for the next
    /// status poll — and must not also be mistaken for a status report.
    #[tokio::test]
    async fn returncode_body_reaches_the_devices_response_handler() {
        let meta_store = device_metadata_store();
        let device_id = "washer-1";
        meta_store.lock().insert(
            device_id.to_string(),
            Metadata {
                model_id: "F_V9_D___0.B_2QEUK".into(),
                model_name: "F_V9_D___0.B_2QEUK".into(),
                device_type: Some("201".into()),
                sw_version: None,
            },
        );
        let manager = DeviceManager::new();
        let acceptor = DeviceAcceptor::new(meta_store, manager.clone());

        let (mut local, remote) = socket_pair().await;
        let acc = acceptor.clone();
        tokio::spawn(async move { acc.accept(remote).await });
        local.write_all(&alive_frame(device_id)).await.unwrap();
        wait_for(|| manager.get(device_id).is_some()).await;

        let dev = manager.get(device_id).unwrap();
        let responses = Arc::new(Mutex::new(Vec::new()));
        let statuses = Arc::new(Mutex::new(Vec::new()));
        {
            let r = responses.clone();
            dev.add_response_handler(move |body| r.lock().push(body.clone()));
        }
        {
            let s = statuses.clone();
            dev.add_data_handler(move |buf| s.lock().push(buf.to_vec()));
        }

        local
            .write_all(&response_frame(device_id, "0000", "n-42"))
            .await
            .unwrap();
        wait_for(|| !responses.lock().is_empty()).await;

        let got = responses.lock().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["ReturnCode"], "0000");
        assert_eq!(got[0]["CmdWId"], "n-42");
        // A command ack still reaches on_data too (it's raw frame plumbing shared with
        // status reports) but must not be misread as one by anything downstream just
        // because on_data fired.
        assert_eq!(statuses.lock().len(), 1);
    }
}
