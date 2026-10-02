//! Bounded device state and metadata. No IL types, topics, transport, or timers.
use crate::Error;
use rhai::{Array, Dynamic, Engine, EvalAltResult, FLOAT, INT, ImmutableString, Map};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug)]
pub struct Config {
    pub device: String,
    pub model: String,
    pub state_keys: usize,
    /// Logical value/key budget; excludes allocator overhead.
    pub state_bytes: usize,
    pub model_name: String,
    pub sw_version: String,
    pub topic_prefix: String,
    pub il_prefix: Option<String>,
    pub thinq2: bool,
    pub driver_api: bool,
    pub message_seed: i64,
}
impl Config {
    pub fn new(device: String, model: String) -> Self {
        Self {
            device,
            model_name: model.clone(),
            model,
            state_keys: 64,
            state_bytes: 65536,
            sw_version: String::new(),
            topic_prefix: "rusthinq".into(),
            il_prefix: None,
            thinq2: false,
            driver_api: false,
            message_seed: 0,
        }
    }
}
struct State {
    values: BTreeMap<String, (Dynamic, usize)>,
    bytes: usize,
    next_id: i64,
}
#[derive(Clone)]
pub(crate) struct Context {
    config: Arc<Config>,
    state: Arc<Mutex<State>>,
}
impl Context {
    pub(crate) fn new(config: Config, string_bytes: usize) -> Result<Self, Error> {
        if config.device.is_empty()
            || config.device.len() > string_bytes
            || config.model.len() > string_bytes
            || config.state_keys == 0
            || config.state_keys > 1024
            || config.state_bytes == 0
            || config.topic_prefix.is_empty()
            || config.topic_prefix.len() > 256
            || config.topic_prefix.contains(['#', '+', '\0'])
            || config
                .il_prefix
                .as_ref()
                .is_some_and(|p| p.is_empty() || p.len() > 256 || p.contains(['#', '+', '\0']))
            || config.model_name.len() > string_bytes
            || config.sw_version.len() > string_bytes
            || config.message_seed < 0
        {
            return Err(Error::InvalidConfig);
        }
        let next_id = config.message_seed;
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(State {
                values: BTreeMap::new(),
                bytes: 0,
                next_id,
            })),
        })
    }
    pub(crate) fn device(&self) -> &str {
        &self.config.device
    }
    pub(crate) fn driver_api(&self) -> bool {
        self.config.driver_api
    }
    pub(crate) fn wire(
        &self,
        cmd: &str,
        msg_type: i64,
        data: serde_json::Value,
    ) -> Result<String, Box<EvalAltResult>> {
        if !self.config.thinq2 {
            return Err("ThinQ2 send requested on ThinQ1".into());
        }
        let mid = self.message_id()?;
        Ok(serde_json::json!({"did":self.config.device,"mid":mid,"cmd":cmd,"type":msg_type,"data":data}).to_string())
    }
    fn message_id(&self) -> Result<i64, Box<EvalAltResult>> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or("message identifier exhausted")?;
        Ok(state.next_id)
    }
    pub(crate) fn json(&self, text: String) -> Result<String, Box<EvalAltResult>> {
        if !self.config.driver_api {
            return Ok(text);
        }
        if self.config.thinq2 {
            return Err("ThinQ1 send requested on ThinQ2".into());
        }
        let mut body: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        let object = body
            .as_object_mut()
            .ok_or("ThinQ1 body must be an object")?;
        object.insert("CmdWId".into(), format!("n-{}", self.message_id()?).into());
        Ok(
            serde_json::json!({"Header":{"x-lgedm-deviceId":self.config.device},"Body":body})
                .to_string(),
        )
    }
    fn set(&mut self, key: String, value: Dynamic) -> Result<(), Box<EvalAltResult>> {
        if key.is_empty() {
            return Err("empty state key".into());
        }
        let mut bytes = key.len();
        let mut nodes = 0;
        measure(&value, 0, &mut nodes, &mut bytes, self.config.state_bytes)?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let previous = state.values.get(&key).map_or(0, |(_, bytes)| *bytes);
        let total = state
            .bytes
            .saturating_sub(previous)
            .checked_add(bytes)
            .ok_or_else(|| Box::<EvalAltResult>::from("state size overflow"))?;
        if total > self.config.state_bytes
            || (previous == 0 && state.values.len() >= self.config.state_keys)
        {
            return Err("state capacity exceeded".into());
        }
        state.values.insert(key, (value, bytes));
        state.bytes = total;
        Ok(())
    }
}
fn measure(
    value: &Dynamic,
    depth: usize,
    nodes: &mut usize,
    bytes: &mut usize,
    maximum: usize,
) -> Result<(), Box<EvalAltResult>> {
    if value.is_shared() {
        return Err("shared state value unsupported".into());
    }
    *nodes += 1;
    if depth > 16 || *nodes > 4096 {
        return Err("state complexity exceeded".into());
    }
    *bytes = bytes
        .checked_add(16)
        .ok_or_else(|| Box::<EvalAltResult>::from("state size overflow"))?;
    if let Some(text) = value.read_lock::<ImmutableString>() {
        *bytes = bytes
            .checked_add(text.len())
            .ok_or_else(|| Box::<EvalAltResult>::from("state size overflow"))?;
    } else if let Some(array) = value.read_lock::<Array>() {
        for item in array.iter() {
            measure(item, depth + 1, nodes, bytes, maximum)?;
        }
    } else if let Some(map) = value.read_lock::<Map>() {
        for (key, item) in map.iter() {
            *bytes = bytes
                .checked_add(key.len())
                .ok_or_else(|| Box::<EvalAltResult>::from("state size overflow"))?;
            measure(item, depth + 1, nodes, bytes, maximum)?;
        }
    } else if !(value.is_unit() || value.is::<bool>() || value.is::<INT>() || value.is::<FLOAT>()) {
        // Reject ctx/function pointers/custom values, preventing reference cycles or hidden state.
        return Err("unsupported state value".into());
    }
    if *bytes > maximum {
        return Err("state capacity exceeded".into());
    }
    Ok(())
}
pub(crate) fn install(engine: &mut Engine) {
    engine.register_type_with_name::<Context>("DeviceContext");
    engine.register_fn("id", |ctx: &mut Context| ctx.config.device.clone());
    engine.register_fn("model_id", |ctx: &mut Context| ctx.config.model.clone());
    engine.register_fn("model_name", |ctx: &mut Context| {
        ctx.config.model_name.clone()
    });
    engine.register_fn("sw_version", |ctx: &mut Context| {
        ctx.config.sw_version.clone()
    });
    engine.register_fn("device_topic", |ctx: &mut Context| {
        format!("{}/{}", ctx.config.topic_prefix, ctx.config.device)
    });
    engine.register_fn("il_topic", |ctx: &mut Context| {
        ctx.config
            .il_prefix
            .as_ref()
            .map_or(String::new(), |prefix| {
                format!("{prefix}/{}", ctx.config.device)
            })
    });
    engine.register_fn("is_thinq2", |ctx: &mut Context| ctx.config.thinq2);
    engine.register_fn("state_set", Context::set);
    engine.register_fn("state_get", |ctx: &mut Context, key: String| {
        ctx.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values
            .get(&key)
            .map_or(Dynamic::UNIT, |(value, _)| value.clone())
    });
    engine.register_fn("state_has", |ctx: &mut Context, key: String| {
        ctx.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values
            .contains_key(&key)
    });
    engine.register_fn("state_remove", |ctx: &mut Context, key: String| {
        let mut state = ctx.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some((_, bytes)) = state.values.remove(&key) {
            state.bytes -= bytes;
            true
        } else {
            false
        }
    });
}
