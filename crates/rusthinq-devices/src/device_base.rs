//! Device protocol/state base classes (AABB framing, TLV field registry).
//!
//! Consumer-neutral on purpose: nothing here knows about any specific downstream
//! integration. `config` is an opaque `serde_json::Value` blob the caller
//! builds and hands in — `set_config`/`republish_config` just cache it and (re)publish
//! it as an ordinary `config` property (via `MqttConnection::publish_property`, the
//! same primitive every other property goes through — no dedicated "discovery" concept
//! or topic convention here). Whatever needs to *render* device state into some
//! specific integration's shape is a Rhai script's job, not this crate's.

use crate::property::PropertyValue;
use rusthinq_core::mqtt::MqttConnection;
use rusthinq_core::thinq::Thinq2Device;
use rusthinq_util::crc16::crc16;
use rusthinq_util::sync::Mutex;
use rusthinq_util::tlv::{self, Tlv};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// ── AABB device ────────────────────────────────────────────────────────────

pub struct AabbDeviceCore {
    pub id: String,
    pub mqtt: Arc<dyn MqttConnection>,
    pub thinq: Arc<dyn Thinq2Device>,
    pub publish_cache: Mutex<HashMap<String, String>>,
    pub config: Mutex<Option<Value>>,
}

impl AabbDeviceCore {
    pub fn new(mqtt: Arc<dyn MqttConnection>, thinq: Arc<dyn Thinq2Device>) -> Arc<Self> {
        Arc::new(Self {
            id: thinq.id().to_string(),
            mqtt,
            thinq,
            publish_cache: Mutex::new(HashMap::new()),
            config: Mutex::new(None),
        })
    }

    pub fn send(&self, inner: &[u8]) {
        self.thinq.send_packet(&wrap_aabb(inner));
    }

    pub fn process_data_envelope(&self, buf: &[u8]) -> Option<Vec<u8>> {
        unwrap_aabb(buf)
    }

    pub fn publish_on_off(&self, prop: &str, on: bool) {
        self.publish_property(prop, if on { "ON" } else { "OFF" }.into());
    }

    pub fn publish_flag(&self, prop: &str, bits: u8, mask: u8) {
        self.publish_on_off(prop, bits & mask != 0);
    }

    /// Re-publish the last config document (reload / reconnect).
    pub fn republish_config(&self) {
        republish_config(&*self.mqtt, &self.id, &self.config);
    }

    pub fn publish_property(&self, prop: &str, value: PropertyValue) {
        let s = value.as_string();
        {
            let mut cache = self.publish_cache.lock();
            if cache.get(prop) == Some(&s) {
                return;
            }
            cache.insert(prop.to_string(), s.clone());
        }
        self.mqtt.publish_property(&self.id, prop, &s);
    }

    /// Publish fridge/freezer door binary state (automate on binary_sensor state).
    pub fn publish_door_with_trigger(&self, open: bool) {
        let val = if open { "ON" } else { "OFF" };
        let prev = self.publish_cache.lock().get("door").cloned();
        if prev.as_deref() == Some(val) {
            return;
        }
        self.publish_property("door", val.into());
    }

    pub fn set_config(&self, config: Value) {
        *self.config.lock() = Some(config.clone());
        self.mqtt.publish_property(&self.id, "availability", "online");
        self.mqtt
            .publish_property(&self.id, "config", &config.to_string());
    }

    pub fn drop_device(&self) {
        self.mqtt
            .publish_property(&self.id, "availability", "offline");
    }
}

// ── TLV device ─────────────────────────────────────────────────────────────

pub type ReadXform = Box<dyn Fn(u32) -> Option<PropertyValue> + Send + Sync>;
pub type WriteXform = Box<dyn Fn(&str) -> Option<PropertyValue> + Send + Sync>;
pub type WriteAttach = Box<dyn Fn(u32) -> Vec<u16> + Send + Sync>;
pub type ReadCallback = Box<dyn Fn(&PropertyValue) -> bool + Send + Sync>;
type VoidHook = Box<dyn Fn() + Send + Sync>;
type TlvPredicate = Box<dyn Fn(&[Tlv]) -> bool + Send + Sync>;
type PrivDataHook = Box<dyn Fn(u8, u8, &[u8]) + Send + Sync>;
type PrivCmdRespHook = Box<dyn Fn(bool, u8, u8, &[u8]) + Send + Sync>;
type KeyValueExtraHook = Box<dyn Fn(u16, u32) + Send + Sync>;
type KeyValueInterceptHook = Box<dyn Fn(u16, u32) -> bool + Send + Sync>;
pub type WriteCallback = Box<dyn Fn(u32) -> bool + Send + Sync>;

pub struct FieldDefinition {
    pub id: Option<u16>,
    pub name: String,
    pub comp: String,
    pub readable: bool,
    pub writable: bool,
    pub write_xform: Option<WriteXform>,
    pub write_attach: Option<WriteAttach>,
    pub write_attach_static: Option<Vec<u16>>,
    pub read_xform: Option<ReadXform>,
    pub read_callback: Option<ReadCallback>,
    pub write_callback: Option<WriteCallback>,
}

impl FieldDefinition {
    pub fn new(comp: &str, name: &str) -> Self {
        Self {
            id: None,
            name: name.into(),
            comp: comp.into(),
            readable: true,
            writable: true,
            write_xform: None,
            write_attach: None,
            write_attach_static: None,
            read_xform: None,
            read_callback: None,
            write_callback: None,
        }
    }

    pub fn with_id(mut self, id: u16) -> Self {
        self.id = Some(id);
        self
    }

    pub fn read_only(mut self) -> Self {
        self.writable = false;
        self
    }

    pub fn write_only(mut self) -> Self {
        self.readable = false;
        self
    }
}

/// TLV devices don't emit an async notification for every tag change, so the
/// values query is repeated on this cadence as a backstop.
const REQUERY_INTERVAL: Duration = Duration::from_secs(15 * 60);

pub struct TlvDeviceCore {
    pub id: String,
    pub mqtt: Arc<dyn MqttConnection>,
    pub thinq: Arc<dyn Thinq2Device>,
    pub config: Mutex<Option<Value>>,
    pub fields_by_id: Mutex<HashMap<u16, Arc<FieldDefinition>>>,
    pub fields_by_ha: Mutex<HashMap<String, Arc<FieldDefinition>>>,
    pub raw_clip_state: Mutex<HashMap<u16, u32>>,
    /// true while waiting for caps
    pub waiting_caps: Mutex<bool>,
    /// true while waiting for initial values
    pub waiting_values: Mutex<bool>,
    /// Hooks for subclasses
    pub on_caps: Mutex<Option<VoidHook>>,
    pub on_values: Mutex<Option<VoidHook>>,
    pub is_caps_response: Mutex<Option<TlvPredicate>>,
    pub is_values_response: Mutex<Option<TlvPredicate>>,
    pub on_priv_data: Mutex<Option<PrivDataHook>>,
    pub on_priv_cmd_resp: Mutex<Option<PrivCmdRespHook>>,
    pub on_key_value_extra: Mutex<Option<KeyValueExtraHook>>,
    /// If set and returns true, skip default process_key_value handling (including storage).
    pub key_value_intercept: Mutex<Option<KeyValueInterceptHook>>,
}

impl TlvDeviceCore {
    pub fn new(mqtt: Arc<dyn MqttConnection>, thinq: Arc<dyn Thinq2Device>) -> Arc<Self> {
        let core = Arc::new(Self {
            id: thinq.id().to_string(),
            mqtt,
            thinq: thinq.clone(),
            config: Mutex::new(None),
            fields_by_id: Mutex::new(HashMap::new()),
            fields_by_ha: Mutex::new(HashMap::new()),
            raw_clip_state: Mutex::new(HashMap::new()),
            waiting_caps: Mutex::new(true),
            waiting_values: Mutex::new(false),
            on_caps: Mutex::new(None),
            on_values: Mutex::new(None),
            is_caps_response: Mutex::new(None),
            is_values_response: Mutex::new(None),
            on_priv_data: Mutex::new(None),
            on_priv_cmd_resp: Mutex::new(None),
            on_key_value_extra: Mutex::new(None),
            key_value_intercept: Mutex::new(None),
        });
        let c = core.clone();
        thinq.on_data(Box::new(move |data| c.process_data(data)));
        // initial caps query
        core.query_caps();
        Self::spawn_periodic_requery(&core, REQUERY_INTERVAL);
        core
    }

    /// Not every tag change produces an async notify, so re-send the values query
    /// on a timer as a backstop — matches rethink's `TLVDevice.start()`. Runs on its
    /// own thread (this crate deliberately has no async-runtime dependency) and
    /// holds only a `Weak` ref, so the timer itself never keeps `core` alive or
    /// blocks it from being freed — it just stops once nothing else does. Note the
    /// `on_data` closure registered above holds a strong ref back to `core`, same as
    /// every other device handler in this crate, so today that's what actually keeps
    /// a live device's core (and this timer) around, not this thread.
    fn spawn_periodic_requery(core: &Arc<Self>, interval: Duration) {
        let weak = Arc::downgrade(core);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(interval);
                let Some(core) = weak.upgrade() else {
                    break;
                };
                tracing::debug!(target: "rusthinq_devices", id = %core.id, "sending periodic refresh query");
                core.query();
            }
        });
    }

    pub fn set_config(&self, config: Value) {
        *self.config.lock() = Some(config.clone());
        self.mqtt.publish_property(&self.id, "availability", "online");
        self.mqtt
            .publish_property(&self.id, "config", &config.to_string());
    }

    /// Register a field's id/name/read-write behavior so `process_key_value`/
    /// `set_property` can route by it. Building whatever "config" document describes
    /// this field to a downstream integration is the caller's job, not this method's.
    pub fn add_field(&self, options: FieldDefinition) {
        let full_name = format!("{}-{}", options.comp, options.name);
        let options = Arc::new(options);
        if let Some(id) = options.id {
            self.fields_by_id.lock().insert(id, options.clone());
        }
        self.fields_by_ha.lock().insert(full_name, options);
    }

    pub fn query_caps(&self) {
        self.send(&[1, 1, 2, 2, 1], &[Tlv::new(0x1f5, 1)]);
    }

    pub fn query(&self) {
        self.send(&[1, 1, 2, 2, 1], &[Tlv::new(0x1f5, 2)]);
    }

    pub fn send(&self, header: &[u8], tlv_els: &[Tlv]) {
        let b0 = header[0];
        let b1 = header[1];
        let b2 = header.get(2).copied().unwrap_or(2);
        let b3 = header.get(3).copied().unwrap_or(2);
        let b4 = header.get(4).copied().unwrap_or(1);
        let tlv_array = tlv::build(tlv_els);
        let mut buf = vec![
            0x04,
            0x00,
            0x00,
            0x00,
            0x65,
            b2,
            b3,
            b4,
            tlv_array.len() as u8,
        ];
        buf.extend_from_slice(&tlv_array);
        let result = crc16(&buf);
        let mut out = vec![b0, b1];
        out.extend_from_slice(&buf);
        out.push((result >> 8) as u8);
        out.push((result & 0xff) as u8);
        self.thinq.send_packet(&out);
    }

    pub fn send_priv_command(&self, cmd: u8, cmd_sub: u8, data: &[u8]) {
        let cmd_data_len = data.len() + 1;
        let mut buf = vec![
            0x00,
            0xff,
            0x04,
            0x00,
            0x00,
            0x00,
            0x65,
            0xfd,
            cmd_sub,
            (cmd_data_len >> 8) as u8,
            (cmd_data_len & 0xff) as u8,
            cmd,
        ];
        buf.extend_from_slice(data);
        let crc = crc16(&buf[2..]);
        buf.push((crc >> 8) as u8);
        buf.push((crc & 0xff) as u8);
        self.thinq.send_packet(&buf);
    }

    pub fn process_data(&self, buf: &[u8]) {
        if buf.len() < 13 {
            return;
        }
        // Standard TLV fromDevice
        if buf[2] == 0x04
            && buf[3] == 0x00
            && buf[4] == 0x00
            && buf[5] == 0x00
            && (buf[6] == 0x87 || buf[6] == 0xa7)
            && buf[7] == 0x02
            && (buf[8] == 0x01 || buf[8] == 0x04)
            && buf[10] as usize == buf.len() - 13
        {
            let tlv = tlv::parse(&buf[11..buf.len() - 2]);
            self.process_tlv(&tlv);
            return;
        }
        // priv data
        if buf[1] == 0xff
            && buf[2] == 0x04
            && buf[3] == 0x00
            && buf[4] == 0x00
            && buf[5] == 0x00
            && buf[6] == 0x87
            && buf[7] == 0xfd
            && buf[8] == 0x03
            && buf[10] as usize == buf.len() - 13
        {
            if let Some(h) = self.on_priv_data.lock().as_ref() {
                h(buf[0], buf[9], &buf[11..buf.len() - 2]);
            }
            return;
        }
        // priv cmd response
        if (buf[0] == 0x02 || buf[0] == 0x03)
            && buf[2] == 0x04
            && buf[3] == 0x00
            && buf[4] == 0x00
            && buf[5] == 0x00
            && buf[6] == 0x87
            && buf[7] == 0xfd
            && buf[8] == 0x10
            && buf[9] == 0x00
            && buf[10] == 0x05
            && buf[11] == 0xfe
            && buf.len() > 12
            && let Some(h) = self.on_priv_cmd_resp.lock().as_ref()
        {
            h(
                buf[0] == 0x02,
                buf[1],
                buf[12],
                &buf[13..buf.len().saturating_sub(2)],
            );
        }
    }

    pub fn process_tlv(&self, tlv_array: &[Tlv]) {
        for el in tlv_array {
            self.process_key_value(el.t, el.v);
        }

        let is_caps = self
            .is_caps_response
            .lock()
            .as_ref()
            .map(|f| f(tlv_array))
            .unwrap_or(false);
        let is_vals = self
            .is_values_response
            .lock()
            .as_ref()
            .map(|f| f(tlv_array))
            .unwrap_or(false);

        if *self.waiting_caps.lock() && is_caps {
            *self.waiting_caps.lock() = false;
            if let Some(h) = self.on_caps.lock().as_ref() {
                h();
            }
            self.query();
            *self.waiting_values.lock() = true;
        }

        if !*self.waiting_caps.lock() && is_vals {
            if *self.waiting_values.lock() {
                *self.waiting_values.lock() = false;
            }
            if let Some(h) = self.on_values.lock().as_ref() {
                h();
            }
        }
    }

    pub fn process_key_value(&self, k: u16, v: u32) {
        if let Some(h) = self.key_value_intercept.lock().as_ref()
            && h(k, v)
        {
            return;
        }
        self.raw_clip_state.lock().insert(k, v);
        if let Some(h) = self.on_key_value_extra.lock().as_ref() {
            h(k, v);
        }
        let def = self.fields_by_id.lock().get(&k).cloned();
        let Some(def) = def else { return };

        let mut processed = PropertyValue::Int(v as i64);
        if let Some(ref xform) = def.read_xform {
            match xform(v) {
                Some(p) => processed = p,
                None => return,
            }
        }
        let do_read = def
            .read_callback
            .as_ref()
            .map(|cb| cb(&processed))
            .unwrap_or(true);
        if do_read && def.readable {
            let full_name = format!("{}-{}", def.comp, def.name);
            self.mqtt
                .publish_property(&self.id, &full_name, &processed.as_string());
        }
    }

    pub fn set_property(&self, prop: &str, mqtt_value: &str) {
        let def = self.fields_by_ha.lock().get(prop).cloned();
        let Some(def) = def else {
            tracing::warn!("Attempting to set unknown property {prop}");
            return;
        };
        if !def.writable {
            tracing::warn!("Attempting to set read-only property {prop}");
            return;
        }

        let value = if let Some(ref xform) = def.write_xform {
            match xform(mqtt_value) {
                Some(v) => v,
                None => return,
            }
        } else {
            PropertyValue::Str(mqtt_value.into())
        };

        let num = match &value {
            // `as u32` on an out-of-range i64 wraps (two's complement), turning e.g. a
            // -1 "invalid"/"unset" sentinel into 4294967295 instead of being rejected.
            // `try_from` catches that instead of silently sending a huge unsigned value
            // to the appliance.
            PropertyValue::Int(i) => match u32::try_from(*i) {
                Ok(n) => n,
                Err(_) => {
                    tracing::warn!("Attempting to set property {prop} to out-of-range value {i}");
                    return;
                }
            },
            PropertyValue::Num(n) => *n as u32,
            PropertyValue::Str(s) => match s.parse::<u32>() {
                Ok(n) => n,
                Err(_) => return,
            },
        };

        let do_write = def
            .write_callback
            .as_ref()
            .map(|cb| cb(num))
            .unwrap_or(true);
        if do_write && let Some(id) = def.id {
            self.raw_clip_state.lock().insert(id, num);
            let mut attach = Vec::new();
            if let Some(ref a) = def.write_attach_static {
                attach = a.clone();
            }
            if let Some(ref a) = def.write_attach {
                attach = a(num);
            }
            let mut write_fields = vec![id];
            write_fields.extend(attach);
            let state = self.raw_clip_state.lock();
            let tlv_array: Vec<Tlv> = write_fields
                .iter()
                .map(|&fid| Tlv::new(fid, *state.get(&fid).unwrap_or(&0)))
                .collect();
            drop(state);
            self.send(&[1, 1, 2, 1, 1], &tlv_array);
        }
    }

    pub fn drop_device(&self) {
        self.mqtt
            .publish_property(&self.id, "availability", "offline");
    }

    pub fn republish_config(&self) {
        republish_config(&*self.mqtt, &self.id, &self.config);
    }

    pub fn get_raw(&self, id: u16) -> Option<u32> {
        self.raw_clip_state.lock().get(&id).copied()
    }

    pub fn set_raw(&self, id: u16, v: u32) {
        self.raw_clip_state.lock().insert(id, v);
    }
}

/// Wrap an AABB inner body as `AA <len> <inner> <csum> BB`.
pub fn wrap_aabb(inner: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(inner.len() + 4);
    packet.push(0xaa);
    packet.push((inner.len() + 4) as u8);
    packet.extend_from_slice(inner);
    packet.push(0x00);
    packet.push(0x00);
    let sum: u32 = packet.iter().map(|&b| u32::from(b)).sum();
    let last = packet.len() - 2;
    packet[last] = ((sum & 0xff) as u8) ^ 0x55;
    packet[last + 1] = 0xbb;
    packet
}

/// Strip `AA … BB` framing. Checksum is not validated (devices send it; tests often omit it).
pub fn unwrap_aabb(buf: &[u8]) -> Option<Vec<u8>> {
    if buf.len() >= 4 && buf[0] == 0xaa && buf[buf.len() - 1] == 0xbb {
        Some(buf[2..buf.len() - 2].to_vec())
    } else {
        None
    }
}

fn republish_config(mqtt: &dyn MqttConnection, id: &str, config: &Mutex<Option<Value>>) {
    if let Some(cfg) = config.lock().clone() {
        mqtt.publish_property(id, "availability", "online");
        mqtt.publish_property(id, "config", &cfg.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_unwrap_roundtrip() {
        let inner = vec![0x20, 0xde, 0x01, 0x02];
        let pkt = wrap_aabb(&inner);
        assert_eq!(pkt[0], 0xaa);
        assert_eq!(*pkt.last().unwrap(), 0xbb);
        assert_eq!(pkt[1] as usize, inner.len() + 4);
        assert_eq!(unwrap_aabb(&pkt).as_deref(), Some(inner.as_slice()));
    }

    #[test]
    fn unwrap_rejects_short_or_unframed() {
        assert!(unwrap_aabb(&[]).is_none());
        assert!(unwrap_aabb(&[0xaa, 0x04, 0xbb]).is_none()); // len 3
        assert!(unwrap_aabb(&[0x00, 0x04, 0x00, 0xbb]).is_none());
        assert!(unwrap_aabb(&[0xaa, 0x04, 0x00, 0x00]).is_none());
    }

    #[test]
    fn laundry_status_query_frame_matches_known_hex() {
        let inner = [0xf0, 0xed, 0x11, 0x21, 0x01, 0x00, 0x00, 0x00, 0x18, 0x00];
        let pkt = wrap_aabb(&inner);
        assert_eq!(
            rusthinq_core::hex_encode(&pkt).to_ascii_uppercase(),
            "AA0EF0ED1121010000001800B5BB"
        );
    }

    #[test]
    fn set_config_publishes_whatever_json_the_caller_hands_in() {
        use rusthinq_core::MockMqttConnection;
        use rusthinq_core::thinq::MockThinq2Device;

        let mqtt = MockMqttConnection::new();
        let thinq = MockThinq2Device::new("dev-1", rusthinq_core::Metadata::new("X", "X", "1.0"));
        let core = AabbDeviceCore::new(mqtt.clone(), thinq);

        // Consumer-neutral: this crate has no idea what shape this is, or whether
        // it's recognizable to anything — it just caches and forwards it as an
        // ordinary "config" property, same as any other.
        core.set_config(serde_json::json!({ "anything": "the caller wants" }));

        let dev = mqtt.device("dev-1").expect("device registered on publish");
        assert_eq!(
            dev.properties.get("config").map(String::as_str),
            Some(r#"{"anything":"the caller wants"}"#)
        );
        assert_eq!(dev.properties.get("availability").map(String::as_str), Some("online"));
    }

    /// The regression this test exists for: TLV devices don't emit an async
    /// notification for every tag change (see rethink's `TLVDevice.start()`), so
    /// without a periodic backstop query some value changes would never be
    /// discovered until the next reconnect. `TlvDeviceCore::new` starts this timer
    /// at construction; here it's driven at a millisecond cadence instead of the
    /// real 15 minutes so the test doesn't have to wait.
    #[test]
    fn tlv_device_requeries_periodically() {
        use rusthinq_core::MockMqttConnection;
        use rusthinq_core::thinq::MockThinq2Device;

        let mqtt = MockMqttConnection::new();
        let thinq = MockThinq2Device::new("dev-1", rusthinq_core::Metadata::new("X", "X", "1.0"));
        let core = TlvDeviceCore::new(mqtt, thinq.clone());
        // Constructor already started a real 15-minute timer; also start a fast one
        // for the test so we don't have to wait for it.
        TlvDeviceCore::spawn_periodic_requery(&core, Duration::from_millis(5));

        thinq.reset_recorder();
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            !thinq.outbox().is_empty(),
            "expected at least one periodic re-query to have been sent"
        );
    }

    /// #35: `PropertyValue::Int(i) as u32` silently wraps a negative `i` via two's
    /// complement (e.g. -1i64 as u32 -> 4294967295) instead of being rejected.
    /// Currently unreachable (no native device handler exists yet in this fork to
    /// produce a negative `write_xform` sentinel), but latent for whoever adds the
    /// first one.
    #[test]
    fn set_property_rejects_an_out_of_range_negative_int_instead_of_wrapping() {
        use rusthinq_core::MockMqttConnection;
        use rusthinq_core::thinq::MockThinq2Device;

        let mqtt = MockMqttConnection::new();
        let thinq = MockThinq2Device::new("dev-1", rusthinq_core::Metadata::new("X", "X", "1.0"));
        let core = TlvDeviceCore::new(mqtt, thinq);

        // Simulates a native handler whose write_xform hands back an
        // "invalid"/"unset" sentinel as a negative Int.
        let field = FieldDefinition {
            write_xform: Some(Box::new(|_| Some(PropertyValue::Int(-1)))),
            ..FieldDefinition::new("comp", "prop").with_id(0x1234)
        };
        core.add_field(field);

        core.set_property("comp-prop", "anything");

        assert_eq!(
            core.raw_clip_state.lock().get(&0x1234),
            None,
            "an out-of-range Int must be rejected, not wrapped into a huge u32 and sent \
             to the appliance"
        );
    }
}
