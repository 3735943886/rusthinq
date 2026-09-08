//! Bridge mode: optional forwarding to the real LG ThinQ cloud.
//!
//! Session membership is **live-only** (mirrors TypeScript `bridgedDevices`):
//! - `sessions` contains only active bridge sessions with wired handlers
//! - local `on_close` stops upstream and **removes** the session (want_enabled + storage remain)
//! - `status_for(id)` == live session present
//! - reconnect re-attaches via `start_session` when saved state / want_enabled exists

pub mod oauth2;
pub mod pair;
pub mod state;
pub mod thinq1_conn;
pub mod thinq2_conn;
pub mod thinq_api;
pub mod util;

pub use state::{BridgeState, Credentials, Environment, JsonStorage};
pub use util::{SubprocessError, SubprocessOptions, subprocess};

use pair::{Thinq1DeviceState, Thinq2DeviceState};
use rusthinq_util::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use thinq1_conn::{Thinq1Handle, connect_thinq1};
use thinq2_conn::{Thinq2Handle, connect_thinq2};

type DataHandler = Box<dyn Fn(&[u8]) + Send + Sync>;
type CloseHandler = Box<dyn Fn() + Send + Sync>;
type ClipHandler = Box<dyn Fn(serde_json::Value) + Send + Sync>;
/// Reports enable/disable progress back to the caller (`status_for` polling).
type StatusCallback = Box<dyn FnMut(&str) + Send>;
/// See `note_urls` on [`Bridge`] and `set_note_urls_hook`.
type NoteUrlsHook = Arc<dyn Fn(&serde_json::Value) + Send + Sync>;
/// See `on_session_change` on [`Bridge`] and `set_on_session_change_hook`.
type SessionChangeHook = Arc<dyn Fn() + Send + Sync>;

/// Minimal local device view for bridge enable/disable (avoids cyclic deps on cloud).
pub trait LocalDevice: Send + Sync {
    fn id(&self) -> &str;
    fn platform(&self) -> &str;
    fn model_id(&self) -> &str;
    fn model_name(&self) -> &str;
    fn device_type(&self) -> Option<&str>;
    fn on_data(&self, handler: DataHandler);
    fn on_close(&self, handler: CloseHandler);
    fn send_to_local(&self, buf: &[u8]);
    fn send_json_to_local(&self, body: serde_json::Value);
    /// Forward a full ThinQ2 CLIP envelope from the cloud to the local device
    /// unchanged (mid included) — see `SendToDevice::T2Raw`. Default no-op so ThinQ1
    /// implementors and test doubles don't need to care.
    fn send_clip_to_local(&self, _payload: serde_json::Value) {}
    /// Register a handler for a CLIP message the local device received that nothing
    /// else has a handler for (e.g. its `respUniversalCtrl` answer to a liveness
    /// check) — carried to the real cloud via `Thinq2Handle::send_clip`. Default
    /// no-op so ThinQ1 implementors and test doubles don't need to care.
    fn on_unhandled_clip(&self, _handler: ClipHandler) {}
    /// The physical appliance's own (appInfo, platformInfo), if it has reported one —
    /// preferred over `format_pre_deploy`'s placeholders when introducing it upstream.
    /// Default `None` so ThinQ1 implementors and test doubles don't need to care.
    fn deploy_info(&self) -> Option<(serde_json::Value, serde_json::Value)> {
        None
    }
    /// How many `on_data` handlers are currently registered (tests / diagnostics).
    fn data_handler_count(&self) -> usize {
        0
    }
}

enum UpstreamHandle {
    T2(Thinq2Handle),
    T1(Thinq1Handle),
    /// Offline / unit-test session with no real LG socket.
    Mock,
}

struct BridgedSession {
    /// The exact local connection this session was opened for — identity (not id)
    /// is what a same-id reconnect race needs, see the `on_close` handler in
    /// `start_session` and `on_local_device` below.
    device: Arc<dyn LocalDevice>,
    /// Set true when detaching; forward tasks exit.
    stopped: Arc<AtomicBool>,
    upstream: Mutex<Option<UpstreamHandle>>,
}

pub struct Bridge {
    storage: Arc<dyn BridgeState>,
    /// Live sessions only — never zombies.
    sessions: Mutex<HashMap<String, Arc<BridgedSession>>>,
    want_enabled: Mutex<HashSet<String>>,
    logged_in: Mutex<bool>,
    /// Called with every relayable downlink CLIP payload from the cloud, for a
    /// caller that wants to scan cloud→device traffic for something (rusthinq-cloud
    /// wires this to `FirmwareHosts::note_urls_in` — see `set_note_urls_hook`).
    /// `rusthinq-bridge` has no opinion on what this is used for; kept generic to
    /// avoid a dependency on rusthinq-cloud.
    note_urls: Mutex<Option<NoteUrlsHook>>,
    /// Called every time a session is attached or detached, from *any* path —
    /// including `on_local_device`'s auto-restore, which runs on a spawned task the
    /// caller never awaits. rusthinq-cloud wires this to republish its retained
    /// `<prefix>/devices` snapshot (`status_for`'s source of truth); without it, a
    /// device whose bridge session was silently auto-restored on reconnect could keep
    /// reporting `bridged: false` forever, since nothing else re-triggers that publish
    /// for this path (unlike the explicit enable/disable MQTT commands, which already
    /// publish after their own `await` completes).
    on_session_change: Mutex<Option<SessionChangeHook>>,
    /// What the owner calls each appliance, from the ThinQ account (`alias` in LG's
    /// device list) — the same names the app shows. rusthinq only ever knows a
    /// device by its id and model, useless for telling identical appliances apart;
    /// the account already has the answer. See `name`/`start_name_refresh_loop`.
    device_names: Mutex<HashMap<String, String>>,
    /// Wakes `run_name_refresh_loop` early — `complete_login` fires this so a fresh
    /// login doesn't wait out the rest of `NAME_REFRESH_INTERVAL` before names show
    /// up.
    name_refresh_notify: tokio::sync::Notify,
    /// Called every time `device_names` actually changes — rusthinq-cloud wires
    /// this to republish its retained `<prefix>/devices` snapshot, same reason
    /// `on_session_change` exists: without it, a name that just arrived from the
    /// account wouldn't reach a subscriber until some unrelated event (a device
    /// connecting, a bridge enable/disable) happened to republish next.
    on_names_changed: Mutex<Option<SessionChangeHook>>,
}

impl Bridge {
    pub fn new(storage: Arc<dyn BridgeState>) -> Arc<Self> {
        let logged_in = storage.get_credentials().is_some();
        Arc::new(Self {
            storage,
            sessions: Mutex::new(HashMap::new()),
            want_enabled: Mutex::new(HashSet::new()),
            logged_in: Mutex::new(logged_in),
            note_urls: Mutex::new(None),
            on_session_change: Mutex::new(None),
            device_names: Mutex::new(HashMap::new()),
            name_refresh_notify: tokio::sync::Notify::new(),
            on_names_changed: Mutex::new(None),
        })
    }

    /// Install a hook run every time `refresh_names` actually changes the cached
    /// names — see `on_names_changed` above.
    pub fn set_on_names_changed_hook(&self, hook: SessionChangeHook) {
        *self.on_names_changed.lock() = Some(hook);
    }

    /// Install a hook run every time a session is attached or detached — see
    /// `on_session_change` above.
    pub fn set_on_session_change_hook(&self, hook: SessionChangeHook) {
        *self.on_session_change.lock() = Some(hook);
    }

    fn notify_session_change(&self) {
        if let Some(hook) = self.on_session_change.lock().clone() {
            hook();
        }
    }

    /// Install a hook run against every relayable downlink CLIP payload, for as long
    /// as this bridge exists — see `note_urls` above.
    pub fn set_note_urls_hook(&self, hook: NoteUrlsHook) {
        *self.note_urls.lock() = Some(hook);
    }

    pub fn is_logged_in(&self) -> bool {
        *self.logged_in.lock() || self.storage.get_credentials().is_some()
    }

    /// True only while a **live** session is registered (map membership == live).
    pub fn status_for(&self, id: &str) -> bool {
        self.sessions.lock().contains_key(id)
    }

    pub fn storage(&self) -> &Arc<dyn BridgeState> {
        &self.storage
    }

    /// The owner's name for `id`, if the account has one cached (see
    /// `start_name_refresh_loop`). `None` before the first successful refresh, or if
    /// the account never gave this device an alias.
    pub fn name(&self, id: &str) -> Option<String> {
        self.device_names.lock().get(id).cloned()
    }

    /// Starts the periodic ThinQ-account device-name refresh as a background task —
    /// call this once, from an async context, after constructing the bridge (see
    /// `rusthinq-cloud`'s `main.rs`). Deliberately not started by `new()` itself:
    /// that's a plain sync constructor callable outside a Tokio runtime (tests do
    /// this), and starting a background task there would panic in exactly that case.
    pub fn start_name_refresh_loop(self: &Arc<Self>) {
        let bridge = self.clone();
        tokio::spawn(async move { bridge.run_name_refresh_loop().await });
    }

    async fn run_name_refresh_loop(self: Arc<Self>) {
        const NAME_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15 * 60);
        loop {
            if self.is_logged_in()
                && let Err(e) = self.refresh_names().await
            {
                tracing::warn!("could not refresh device names from the ThinQ account: {e:#}");
            }
            tokio::select! {
                _ = tokio::time::sleep(NAME_REFRESH_INTERVAL) => {}
                _ = self.name_refresh_notify.notified() => {}
            }
        }
    }

    /// Best-effort, mirrors rethink's `Bridge.refreshNames`: not logged in just
    /// clears the cache; any other failure (network, auth) is left for the caller
    /// to log, same policy as every other real-cloud call in this file.
    async fn refresh_names(&self) -> anyhow::Result<()> {
        let Some(creds) = self.storage.get_credentials() else {
            self.set_device_names(HashMap::new());
            return Ok(());
        };
        let mut client = thinq_api::Client::new(creds.env.clone());
        client.auth(&creds.refresh_token).await?;
        let devices = client.list_devices().await?;
        let names: HashMap<String, String> = devices
            .iter()
            .filter_map(|d| {
                let id = d.get("deviceId").and_then(|v| v.as_str())?;
                let alias = d.get("alias").and_then(|v| v.as_str())?;
                (!alias.is_empty()).then(|| (id.to_string(), alias.to_string()))
            })
            .collect();
        self.set_device_names(names);
        Ok(())
    }

    /// Replaces the cached names and fires `on_names_changed`, but only if they
    /// actually changed -- `run_name_refresh_loop` calls this every
    /// `NAME_REFRESH_INTERVAL`, and most of those ticks change nothing.
    fn set_device_names(&self, names: HashMap<String, String>) {
        let mut current = self.device_names.lock();
        if *current == names {
            return;
        }
        *current = names;
        drop(current);
        self.notify_names_changed();
    }

    fn notify_names_changed(&self) {
        if let Some(hook) = self.on_names_changed.lock().clone() {
            hook();
        }
    }

    /// Stop upstream and remove from live map; keep want_enabled + device state.
    pub fn detach_session(&self, id: &str) {
        // `sessions.lock()`'s guard is a temporary in the `if let` scrutinee below --
        // Rust extends a scrutinee temporary's lifetime to the whole `if let` body, so
        // writing this as `if let Some(sess) = self.sessions.lock().remove(id) { ... }`
        // would hold the lock across `notify_session_change()` -> the hook ->
        // `status_for()`, which re-locks the same (non-reentrant) mutex and deadlocks
        // every caller of this function forever. Binding the `.remove()` result to a
        // plain `let` first drops the guard at that statement's end, before any of
        // this runs.
        let removed = self.sessions.lock().remove(id);
        if let Some(sess) = removed {
            sess.stopped.store(true, Ordering::SeqCst);
            if let Some(up) = sess.upstream.lock().take() {
                match up {
                    UpstreamHandle::T2(h) => h.stop(),
                    UpstreamHandle::T1(h) => h.stop(),
                    UpstreamHandle::Mock => {}
                }
            }
            self.notify_session_change();
        }
    }

    pub async fn begin_login(&self, country_code: &str) -> anyhow::Result<String> {
        let mut client = thinq_api::Client::new(Environment {
            country_code: country_code.into(),
            language_code: None,
        });
        let (web, _auth) = client.get_urls().await?;
        thinq_api::sign_in_url(&web, country_code)
    }

    pub async fn complete_login(
        &self,
        country_code: &str,
        callback_url: &str,
    ) -> anyhow::Result<bool> {
        let mut client = thinq_api::Client::new(Environment {
            country_code: country_code.into(),
            language_code: None,
        });
        let (_web, auth) = client.get_urls().await?;
        let url = url::Url::parse(callback_url)?;
        let code = url
            .query_pairs()
            .find(|(k, _)| k == "code")
            .map(|(_, v)| v.to_string());
        let Some(code) = code else {
            return Ok(false);
        };
        let token = oauth2::from_code(&auth, &code).await?;
        self.storage.set_credentials(Some(Credentials {
            refresh_token: token.refresh_token,
            env: Environment {
                country_code: country_code.into(),
                language_code: None,
            },
        }));
        *self.logged_in.lock() = true;
        // Wakes `run_name_refresh_loop` (if `start_name_refresh_loop` was ever
        // called) so names show up right away instead of after the rest of
        // NAME_REFRESH_INTERVAL. A no-op permit if nothing's listening yet.
        self.name_refresh_notify.notify_one();
        Ok(true)
    }

    pub async fn logout(&self) -> anyhow::Result<()> {
        self.storage.set_credentials(None);
        *self.logged_in.lock() = false;
        self.set_device_names(HashMap::new());
        let ids: Vec<String> = self.sessions.lock().keys().cloned().collect();
        for id in ids {
            self.detach_session(&id);
        }
        self.want_enabled.lock().clear();
        Ok(())
    }

    pub async fn enable(
        self: &Arc<Self>,
        device: Arc<dyn LocalDevice>,
        device_type: Option<&str>,
        mut status: Option<StatusCallback>,
    ) -> anyhow::Result<bool> {
        let mut report = |s: &str| {
            if let Some(ref mut cb) = status {
                cb(s);
            }
        };

        if !self.is_logged_in() {
            report("not logged in");
            return Ok(false);
        }
        let id = device.id().to_string();

        // Live session already — only short-circuit if truly live (map membership).
        if self.sessions.lock().contains_key(&id) {
            return Ok(true);
        }

        let creds = self
            .storage
            .get_credentials()
            .ok_or_else(|| anyhow::anyhow!("Not logged in"))?;

        // Test seam only: a real LG registration never carries this flag, so this
        // never fires against saved state a real device produced. Matches rethink's
        // `enable()`, which always re-registers from scratch here (OTP -> pair ->
        // addDevice) rather than trusting a saved certificate that may have gone
        // stale on LG's side with no way for us to tell -- reconnecting an already
        // -registered device without a fresh `enable()` call still reuses saved
        // state via `on_local_device`/`start_session`, same as rethink's `#start`.
        if let Some(saved) = self.storage.get_device_state_json(&id)
            && saved.get("testMode").and_then(|v| v.as_bool()) == Some(true)
        {
            report("Restoring saved bridge session");
            self.start_session(device, saved).await?;
            self.want_enabled.lock().insert(id);
            return Ok(true);
        }

        let mut client = thinq_api::Client::new(creds.env.clone());
        client.auth(&creds.refresh_token).await?;

        let dtype = device_type
            .map(|s| s.to_string())
            .or_else(|| device.device_type().map(|s| s.to_string()))
            .ok_or_else(|| anyhow::anyhow!("Device type must be specified"))?;

        let plan = thinq_api::registration_plan(&client.list_devices().await?, &id);
        if plan.remove_first {
            report("Removing device from home");
            let _ = client.remove_device(&id).await;
        }

        let lg_state = if device.platform() == "thinq1" {
            report("Adding ThinQ1 device to home");
            let state = client.thinq1_state()?;
            client
                .add_device(
                    &id,
                    &plan.alias,
                    device.model_name(),
                    &dtype,
                    "thinq1",
                    None,
                )
                .await?;
            serde_json::json!({
                "rtiServer": state.get("rtiServer").cloned().unwrap_or(serde_json::Value::Null),
                "httpServer": state.get("httpServer").cloned().unwrap_or(serde_json::Value::Null),
                "platform": "thinq1",
            })
        } else {
            report("Fetching otp key");
            let (otp, pubkey) = client.prepare_new_t2_device().await?;
            report("Pairing ThinQ2 device (route + certificate)");
            let mut pair = match pair::pair_thinq2(&creds.env, &id, &otp, &pubkey).await {
                Ok(p) => p,
                Err(e) => {
                    report(&format!(
                        "Pairing failed: {e}. Make sure common.lgthinq.com is not redirected"
                    ));
                    return Err(e);
                }
            };
            report("Adding ThinQ2 device to home");
            let ct_b64 = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &pair.add_device_ciphertext,
            );
            client
                .add_device(
                    &id,
                    &plan.alias,
                    device.model_name(),
                    &dtype,
                    "thinq2",
                    Some(&ct_b64),
                )
                .await?;
            // Persist the appliance's real deploy info alongside the paired state (if
            // it's already reported one locally by now), so a restart before its next
            // re-deploy still introduces it upstream as itself instead of a placeholder.
            if let Some((app_info, platform_info)) = device.deploy_info() {
                pair.state.deploy_app_info = Some(app_info);
                pair.state.deploy_platform_info = Some(platform_info);
            }
            serde_json::to_value(&pair.state)?
        };

        report("Device registered successfully");
        self.storage
            .set_device_state_json(&id, Some(lg_state.clone()));
        self.start_session(device, lg_state).await?;
        self.want_enabled.lock().insert(id);
        Ok(true)
    }

    /// Open upstream (or mock) and wire bidirectional forward; register live session.
    pub async fn start_session(
        self: &Arc<Self>,
        device: Arc<dyn LocalDevice>,
        lg_state: serde_json::Value,
    ) -> anyhow::Result<()> {
        let id = device.id().to_string();
        // Replace any stale entry (should not happen if detach is correct).
        self.detach_session(&id);

        let model_name = device.model_name().to_string();
        let device_type = device.device_type().map(|s| s.to_string());
        let stopped = Arc::new(AtomicBool::new(false));

        let test_mode = lg_state
            .get("testMode")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let is_t2 = device.platform() == "thinq2"
            || lg_state.get("mqttServer").is_some()
            || lg_state.get("mqtt_server").is_some()
            || lg_state.get("platform").and_then(|v| v.as_str()) == Some("thinq2");

        let upstream = if test_mode {
            // Unit-test path: no network; still wire local handlers for lifecycle.
            let stop = stopped.clone();
            device.on_data(Box::new(move |_buf| {
                let _ = stop.load(Ordering::SeqCst);
            }));
            UpstreamHandle::Mock
        } else if is_t2 {
            let state: Thinq2DeviceState =
                serde_json::from_value(normalize_t2_state(lg_state.clone()))?;
            if state.mqtt_server.is_empty() {
                anyhow::bail!("ThinQ2 state missing mqttServer — re-enable to re-pair");
            }
            let (live_app_info, live_platform_info) =
                pair::resolve_deploy_info(device.deploy_info(), &state);
            let (handle, mut from_lg) =
                connect_thinq2(&state, &id, &model_name, live_app_info, live_platform_info).await?;

            let dev = device.clone();
            let stop_f = stopped.clone();
            let note_urls = self.note_urls.lock().clone();
            tokio::spawn(async move {
                while let Some(payload) = from_lg.recv().await {
                    if stop_f.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Some(hook) = &note_urls {
                        hook(&payload);
                    }
                    dev.send_clip_to_local(payload);
                }
            });

            let h = handle.clone();
            let stop_l = stopped.clone();
            device.on_data(Box::new(move |buf| {
                if stop_l.load(Ordering::SeqCst) {
                    return;
                }
                let h = h.clone();
                let data = buf.to_vec();
                tokio::spawn(async move {
                    let _ = h.send_from_local(&data).await;
                });
            }));

            // The appliance's answer to a cmd the cloud relayed down (e.g.
            // respUniversalCtrl, answering the liveness check the cloud needs before
            // it will offer a firmware update) has to cross back upstream, or from
            // the cloud's side the appliance never answered at all.
            let h = handle.clone();
            let stop_c = stopped.clone();
            device.on_unhandled_clip(Box::new(move |payload| {
                if stop_c.load(Ordering::SeqCst) {
                    return;
                }
                let h = h.clone();
                tokio::spawn(async move {
                    let _ = h.send_clip(payload).await;
                });
            }));

            UpstreamHandle::T2(handle)
        } else {
            let t1 = parse_t1_state(&lg_state)?;
            let (handle, mut from_lg) =
                connect_thinq1(&t1, &id, &model_name, device_type.as_deref()).await?;

            let dev = device.clone();
            let stop_f = stopped.clone();
            tokio::spawn(async move {
                while let Some(body) = from_lg.recv().await {
                    if stop_f.load(Ordering::SeqCst) {
                        break;
                    }
                    dev.send_json_to_local(body);
                }
            });

            let h = handle.clone();
            let stop_l = stopped.clone();
            device.on_data(Box::new(move |buf| {
                if stop_l.load(Ordering::SeqCst) {
                    return;
                }
                h.send_from_local(buf);
            }));

            UpstreamHandle::T1(handle)
        };

        let session = Arc::new(BridgedSession {
            device: device.clone(),
            stopped: stopped.clone(),
            upstream: Mutex::new(Some(upstream)),
        });

        // Local close → detach (remove from map + stop upstream). want_enabled stays.
        //
        // A same-id reconnect can open its replacement connection and get bridged
        // again (via on_local_device below) before this old connection's close event
        // actually fires — devmgr.rs's DeviceManager::accept hits the exact same race
        // and documents it: "on ThinQ2 reconnect the old MQTT client's disconnect can
        // run after the new device is accepted". Detaching by id alone here would then
        // tear down the *new* session an instant after it was established. Comparing
        // the live session's device identity (not just its id) before detaching is
        // what device_bridge.rs's own close handler and devmgr.rs's accept() both
        // already do for exactly this reason.
        let bridge = self.clone();
        let id_close = id.clone();
        let device_for_close = device.clone();
        device.on_close(Box::new(move || {
            let still_ours = bridge
                .sessions
                .lock()
                .get(&id_close)
                .map(|s| Arc::ptr_eq(&s.device, &device_for_close))
                .unwrap_or(false);
            if still_ours {
                bridge.detach_session(&id_close);
            }
        }));

        self.sessions.lock().insert(id, session);
        self.notify_session_change();
        Ok(())
    }

    pub async fn enable_id(
        self: &Arc<Self>,
        device_id: &str,
        device_type: Option<&str>,
        mut status: Option<StatusCallback>,
        lookup: &dyn Fn(&str) -> Option<Arc<dyn LocalDevice>>,
    ) -> anyhow::Result<bool> {
        let Some(dev) = lookup(device_id) else {
            if let Some(ref mut cb) = status {
                cb("device not connected");
            }
            return Ok(false);
        };
        self.enable(dev, device_type, status).await
    }

    pub async fn disable(&self, device_id: &str) -> anyhow::Result<()> {
        self.storage.set_device_state_json(device_id, None);
        self.detach_session(device_id);
        self.want_enabled.lock().remove(device_id);
        Ok(())
    }

    /// When a local device appears, auto-restore bridge if previously enabled and not live.
    pub fn on_local_device(self: &Arc<Self>, device: Arc<dyn LocalDevice>) {
        let id = device.id().to_string();
        if !self.want_enabled.lock().contains(&id)
            && self.storage.get_device_state_json(&id).is_none()
        {
            return;
        }
        // Live session already for *this exact connection* — do not double-attach.
        // Note this must not bail out just because *some* session is registered for
        // `id`: on a same-id reconnect, the old connection's close event can still be
        // pending (see the race explained on the `on_close` handler in
        // `start_session`), so the session found here may belong to a now-superseded
        // connection. Leaving it alone in that case would permanently strand the new
        // connection un-bridged — nothing else ever re-triggers `start_session` for
        // it. `start_session` already replaces any existing entry for `id`
        // unconditionally, so falling through here is always safe.
        if self
            .sessions
            .lock()
            .get(&id)
            .map(|s| Arc::ptr_eq(&s.device, &device))
            .unwrap_or(false)
        {
            return;
        }
        let Some(state) = self.storage.get_device_state_json(&id) else {
            return;
        };
        let this = self.clone();
        let id_for_log = id.clone();
        tokio::spawn(async move {
            match this.start_session(device, state).await {
                Ok(()) => {
                    this.want_enabled.lock().insert(id);
                }
                Err(e) => {
                    rusthinq_core::logging::log(
                        "bridge",
                        &[&format!("auto-restore failed for {id_for_log}: {e}")],
                    );
                }
            }
        });
    }
}

fn normalize_t2_state(v: serde_json::Value) -> serde_json::Value {
    if v.get("mqtt_server").is_some() && v.get("mqttServer").is_none() {
        return serde_json::json!({
            "countryCode": v.get("country_code").cloned().unwrap_or(serde_json::json!("US")),
            "apiServer": v.get("api_server").cloned().unwrap_or(serde_json::json!("")),
            "mqttServer": v.get("mqtt_server").cloned().unwrap_or(serde_json::json!("")),
            "caCertificate": v.get("ca_certificate").cloned().unwrap_or(serde_json::json!("")),
            "privateKey": v.get("private_key").cloned().unwrap_or(serde_json::json!("")),
            "certificate": v.get("certificate").cloned().unwrap_or(serde_json::json!("")),
            "pubTopic": v.get("pub_topic").cloned().unwrap_or(serde_json::json!("")),
            "provTopic": v.get("prov_topic").cloned().unwrap_or(serde_json::json!("")),
            "subTopic": v.get("sub_topic").cloned().unwrap_or(serde_json::json!("")),
            "deployAppInfo": v.get("deployAppInfo").cloned().unwrap_or(serde_json::Value::Null),
            "deployPlatformInfo": v.get("deployPlatformInfo").cloned().unwrap_or(serde_json::Value::Null),
        });
    }
    serde_json::json!({
        "countryCode": v.get("countryCode").or_else(|| v.get("country_code")).cloned().unwrap_or(serde_json::json!("US")),
        "apiServer": v.get("apiServer").or_else(|| v.get("api_server")).cloned().unwrap_or(serde_json::json!("")),
        "mqttServer": v.get("mqttServer").or_else(|| v.get("mqtt_server")).cloned().unwrap_or(serde_json::json!("")),
        "caCertificate": v.get("caCertificate").or_else(|| v.get("ca_certificate")).cloned().unwrap_or(serde_json::json!("")),
        "privateKey": v.get("privateKey").or_else(|| v.get("private_key")).cloned().unwrap_or(serde_json::json!("")),
        "certificate": v.get("certificate").cloned().unwrap_or(serde_json::json!("")),
        "pubTopic": v.get("pubTopic").or_else(|| v.get("pub_topic")).cloned().unwrap_or(serde_json::json!("")),
        "provTopic": v.get("provTopic").or_else(|| v.get("prov_topic")).cloned().unwrap_or(serde_json::json!("")),
        "subTopic": v.get("subTopic").or_else(|| v.get("sub_topic")).cloned().unwrap_or(serde_json::json!("")),
        "deployAppInfo": v.get("deployAppInfo").cloned().unwrap_or(serde_json::Value::Null),
        "deployPlatformInfo": v.get("deployPlatformInfo").cloned().unwrap_or(serde_json::Value::Null),
    })
}

fn parse_t1_state(v: &serde_json::Value) -> anyhow::Result<Thinq1DeviceState> {
    let rti = v
        .get("rtiServer")
        .or_else(|| v.get("rti_server"))
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing rtiServer"))?
        .to_string();
    let http = v
        .get("httpServer")
        .or_else(|| v.get("http_server"))
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing httpServer"))?
        .to_string();
    Ok(Thinq1DeviceState {
        rti_server: rti,
        http_server: http,
    })
}

// ── Lifecycle tests (close → reconnect → re-wire) ──────────────────────────

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::pair::{format_device_packet, parse_lg_packet_payload};
    use crate::thinq1_conn::format_status_body;
    use std::sync::atomic::AtomicUsize; // used by MockLocal

    /// Real mock: stores handlers; simulate_close fires them.
    struct MockLocal {
        id: String,
        platform: String,
        to_local: Mutex<Vec<Vec<u8>>>,
        data_handlers: Mutex<Vec<DataHandler>>,
        close_handlers: Mutex<Vec<CloseHandler>>,
        data_handler_regs: AtomicUsize,
    }

    impl MockLocal {
        fn new(id: &str, platform: &str) -> Arc<Self> {
            Arc::new(Self {
                id: id.into(),
                platform: platform.into(),
                to_local: Mutex::new(Vec::new()),
                data_handlers: Mutex::new(Vec::new()),
                close_handlers: Mutex::new(Vec::new()),
                data_handler_regs: AtomicUsize::new(0),
            })
        }

        fn simulate_close(&self) {
            let handlers: Vec<_> = self.close_handlers.lock().drain(..).collect();
            for h in handlers {
                h();
            }
        }

        #[allow(
            dead_code,
            reason = "symmetric with simulate_close; not yet exercised by a test"
        )]
        fn emit_data(&self, buf: &[u8]) {
            for h in self.data_handlers.lock().iter() {
                h(buf);
            }
        }
    }

    impl LocalDevice for MockLocal {
        fn id(&self) -> &str {
            &self.id
        }
        fn platform(&self) -> &str {
            &self.platform
        }
        fn model_id(&self) -> &str {
            "MODEL"
        }
        fn model_name(&self) -> &str {
            "MODEL"
        }
        fn device_type(&self) -> Option<&str> {
            Some("401")
        }
        fn on_data(&self, handler: DataHandler) {
            self.data_handler_regs.fetch_add(1, Ordering::SeqCst);
            self.data_handlers.lock().push(handler);
        }
        fn on_close(&self, handler: CloseHandler) {
            self.close_handlers.lock().push(handler);
        }
        fn send_to_local(&self, buf: &[u8]) {
            self.to_local.lock().push(buf.to_vec());
        }
        fn send_json_to_local(&self, _body: serde_json::Value) {}
        fn data_handler_count(&self) -> usize {
            self.data_handler_regs.load(Ordering::SeqCst)
        }
    }

    fn test_bridge() -> Arc<Bridge> {
        let dir = tempfile_dir();
        let storage = Arc::new(JsonStorage::new(&dir));
        // Pretend logged in
        storage.set_credentials(Some(Credentials {
            refresh_token: "test-refresh".into(),
            env: Environment {
                country_code: "US".into(),
                language_code: None,
            },
        }));
        Bridge::new(storage)
    }

    fn tempfile_dir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("rusthinq-bridge-lc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn mock_saved_state() -> serde_json::Value {
        serde_json::json!({
            "testMode": true,
            "platform": "thinq2",
            "mqttServer": "mock://test",
        })
    }

    #[tokio::test]
    async fn close_removes_session_and_reconnect_rewires() {
        let bridge = test_bridge();
        let id = "dev-lifecycle";

        // Seed saved state (as if previously registered with LG).
        bridge
            .storage
            .set_device_state_json(id, Some(mock_saved_state()));
        bridge.want_enabled.lock().insert(id.to_string());

        let local1 = MockLocal::new(id, "thinq2");
        // enable with saved state → start_session (testMode, no network)
        let ok = bridge
            .enable(local1.clone() as Arc<dyn LocalDevice>, Some("401"), None)
            .await
            .unwrap();
        assert!(ok);
        assert!(
            bridge.status_for(id),
            "status_for must be true while live session exists"
        );
        assert!(
            local1.data_handler_count() >= 1,
            "on_data must be registered on live attach"
        );
        let regs_after_enable = local1.data_handler_count();

        // Close local device → detach (map empty, want_enabled kept, storage kept)
        local1.simulate_close();
        assert!(
            !bridge.status_for(id),
            "status_for must be false after close (no zombie session)"
        );
        assert!(
            !bridge.sessions.lock().contains_key(id),
            "sessions map must not contain id after close"
        );
        assert!(
            bridge.want_enabled.lock().contains(id),
            "want_enabled must remain so reconnect can re-attach"
        );
        assert!(
            bridge.storage.get_device_state_json(id).is_some(),
            "device state must remain for restore"
        );

        // enable short-circuit must NOT return Ok(true) with no live session
        // New device Arc (reconnect)
        let local2 = MockLocal::new(id, "thinq2");
        bridge.on_local_device(local2.clone() as Arc<dyn LocalDevice>);
        // on_local_device spawns async — poll until live or timeout
        for _ in 0..50 {
            if bridge.status_for(id) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            bridge.status_for(id),
            "on_local_device must re-attach live session after close"
        );
        assert!(
            local2.data_handler_count() >= 1,
            "new LocalDevice Arc must get on_data handlers re-registered"
        );

        // disable clears everything
        bridge.disable(id).await.unwrap();
        assert!(!bridge.status_for(id));
        assert!(!bridge.want_enabled.lock().contains(id));
        assert!(bridge.storage.get_device_state_json(id).is_none());

        let _ = regs_after_enable;
    }

    /// `on_local_device`'s auto-restore runs on a spawned task the caller never
    /// awaits — rusthinq-cloud's retained device-list snapshot only gets republished
    /// on this path via `on_session_change`. Without that hook firing *after* the
    /// spawned `start_session` actually finishes (not just at the initial `enable`),
    /// a device auto-restored on reconnect would report `bridged: false` forever.
    #[tokio::test]
    async fn on_local_device_auto_restore_fires_the_session_change_hook() {
        let bridge = test_bridge();
        let id = "dev-hook";

        bridge
            .storage
            .set_device_state_json(id, Some(mock_saved_state()));
        bridge.want_enabled.lock().insert(id.to_string());

        let local1 = MockLocal::new(id, "thinq2");
        bridge
            .enable(local1.clone() as Arc<dyn LocalDevice>, Some("401"), None)
            .await
            .unwrap();

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_c = calls.clone();
        bridge.set_on_session_change_hook(Arc::new(move || {
            calls_c.fetch_add(1, Ordering::SeqCst);
        }));

        local1.simulate_close();
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "detach on close must fire the hook"
        );
        let calls_after_close = calls.load(Ordering::SeqCst);

        let local2 = MockLocal::new(id, "thinq2");
        bridge.on_local_device(local2.clone() as Arc<dyn LocalDevice>);
        for _ in 0..50 {
            if bridge.status_for(id) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(bridge.status_for(id), "auto-restore must succeed");
        assert!(
            calls.load(Ordering::SeqCst) > calls_after_close,
            "the hook must fire again once the spawned auto-restore actually attaches \
             the session, not just at the initial enable — this is what lets a caller \
             know to republish state that was stale while the restore was in flight"
        );
    }

    /// Real usage (rusthinq-cloud's main.rs) wires the session-change hook to
    /// `DeviceListPublisher::publish`, which calls `status_for` for every device —
    /// i.e. re-locks `sessions` from inside the hook. `detach_session` used to do
    /// `if let Some(sess) = self.sessions.lock().remove(id) { ...; notify(); }`,
    /// and Rust extends an `if let` scrutinee temporary's lifetime across the whole
    /// body, so that `.lock()` guard was still held while `notify()` ran the hook —
    /// deadlocking every caller of `detach_session` forever the moment any real
    /// hook touched `sessions` again. This starved the whole tokio runtime in
    /// production (a real deploy: one bridge disable hung the entire process, no
    /// further logs, every client timing out). A hook that merely counts calls (see
    /// the test above) cannot catch this — it must re-lock `sessions` to reproduce it.
    // Needs its own worker thread so a real deadlock in the spawned task below (a
    // thread permanently blocked on re-locking a mutex it already holds) can't also
    // starve the timer that's supposed to catch it — see the doc comment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn detach_session_does_not_deadlock_when_the_hook_relocks_sessions() {
        let bridge = test_bridge();
        let id = "dev-deadlock";

        bridge
            .storage
            .set_device_state_json(id, Some(mock_saved_state()));
        bridge.want_enabled.lock().insert(id.to_string());

        let local = MockLocal::new(id, "thinq2");
        bridge
            .enable(local.clone() as Arc<dyn LocalDevice>, Some("401"), None)
            .await
            .unwrap();
        assert!(bridge.status_for(id));

        let bridge_in_hook = bridge.clone();
        bridge.set_on_session_change_hook(Arc::new(move || {
            // Mirrors DeviceListPublisher::snapshot's per-device status_for call.
            let _ = bridge_in_hook.status_for(id);
        }));

        // Spawned onto its own task/thread (not just wrapped in a timeout on this
        // task) so a genuine self-deadlock — the same thread blocking forever trying
        // to re-lock a mutex it already holds — can't also block the timer that's
        // supposed to catch it; a plain `timeout(async { detach_session() }).await`
        // here would itself hang forever instead of failing.
        let handle = tokio::spawn(async move {
            bridge.detach_session(id);
            bridge
        });
        let detached = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        let Ok(Ok(bridge)) = detached else {
            panic!("detach_session must not deadlock when its own hook re-locks sessions");
        };
        assert!(!bridge.status_for(id));
    }

    /// The race devmgr.rs's own `accept()` comment warns about: "on ThinQ2 reconnect
    /// the old MQTT client's disconnect can run after the new device is accepted".
    /// Here the replacement connection's `on_local_device` call lands *before* the
    /// superseded connection's close event fires — the opposite ordering from
    /// `close_removes_session_and_reconnect_rewires` above. Before the fix, the old
    /// session's presence made `on_local_device` bail out (leaving the new connection
    /// permanently un-bridged), and the old close firing afterward tore down whatever
    /// session was live for the id — which, had a replacement raced in, would have
    /// been the *new* one.
    #[tokio::test]
    async fn reconnect_racing_ahead_of_old_close_still_ends_up_bridged() {
        let bridge = test_bridge();
        let id = "dev-race";

        bridge
            .storage
            .set_device_state_json(id, Some(mock_saved_state()));
        bridge.want_enabled.lock().insert(id.to_string());

        let local1 = MockLocal::new(id, "thinq2");
        let ok = bridge
            .enable(local1.clone() as Arc<dyn LocalDevice>, Some("401"), None)
            .await
            .unwrap();
        assert!(ok);
        assert!(bridge.status_for(id));

        // The replacement connection registers — old connection's close has NOT
        // fired yet.
        let local2 = MockLocal::new(id, "thinq2");
        bridge.on_local_device(local2.clone() as Arc<dyn LocalDevice>);
        for _ in 0..50 {
            if local2.data_handler_count() >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            local2.data_handler_count() >= 1,
            "the replacement connection must get its own session, not be stranded \
             behind the still-registered old one"
        );
        assert!(bridge.status_for(id));

        // Now the old connection's close fires, late. It must not tear down the
        // replacement's live session.
        local1.simulate_close();
        assert!(
            bridge.status_for(id),
            "a late close from the superseded connection must not detach the live \
             replacement session"
        );
    }

    #[tokio::test]
    async fn enable_does_not_short_circuit_on_absent_session() {
        let bridge = test_bridge();
        let id = "dev-short";
        bridge
            .storage
            .set_device_state_json(id, Some(mock_saved_state()));

        // No live session — enable must start_session, not pretend success without wiring
        assert!(!bridge.status_for(id));
        let local = MockLocal::new(id, "thinq2");
        bridge
            .enable(local.clone() as Arc<dyn LocalDevice>, None, None)
            .await
            .unwrap();
        assert!(bridge.status_for(id));
        assert!(local.data_handler_count() >= 1);

        // Second enable while live — short-circuit Ok(true) without double-start
        let before = local.data_handler_count();
        bridge
            .enable(local.clone() as Arc<dyn LocalDevice>, None, None)
            .await
            .unwrap();
        assert_eq!(
            local.data_handler_count(),
            before,
            "live short-circuit must not re-register handlers"
        );
    }

    #[tokio::test]
    async fn name_is_none_before_any_refresh() {
        let bridge = test_bridge();
        assert_eq!(bridge.name("dev-1"), None);
    }

    #[tokio::test]
    async fn set_device_names_only_fires_the_hook_when_the_map_actually_changes() {
        let bridge = test_bridge();
        let fires = Arc::new(rusthinq_util::sync::Mutex::new(0usize));
        let fires2 = fires.clone();
        bridge.set_on_names_changed_hook(Arc::new(move || *fires2.lock() += 1));

        let mut names = HashMap::new();
        names.insert("dev-1".to_string(), "Kitchen Fridge".to_string());
        bridge.set_device_names(names.clone());
        assert_eq!(*fires.lock(), 1);
        assert_eq!(bridge.name("dev-1"), Some("Kitchen Fridge".to_string()));

        // Same content again -- no real change, hook must not re-fire.
        bridge.set_device_names(names.clone());
        assert_eq!(*fires.lock(), 1);

        names.insert("dev-2".to_string(), "Living Room AC".to_string());
        bridge.set_device_names(names);
        assert_eq!(*fires.lock(), 2);
    }

    #[tokio::test]
    async fn logout_clears_cached_names_and_fires_the_hook() {
        let bridge = test_bridge();
        let mut names = HashMap::new();
        names.insert("dev-1".to_string(), "Kitchen Fridge".to_string());
        bridge.set_device_names(names);
        assert_eq!(bridge.name("dev-1"), Some("Kitchen Fridge".to_string()));

        let fired = Arc::new(rusthinq_util::sync::Mutex::new(false));
        let fired2 = fired.clone();
        bridge.set_on_names_changed_hook(Arc::new(move || *fired2.lock() = true));

        bridge.logout().await.unwrap();

        assert_eq!(bridge.name("dev-1"), None);
        assert!(*fired.lock());
    }

    #[tokio::test]
    async fn simulate_close_then_enable_restores() {
        let bridge = test_bridge();
        let id = "dev-enable-restore";
        bridge
            .storage
            .set_device_state_json(id, Some(mock_saved_state()));

        let a = MockLocal::new(id, "thinq2");
        bridge
            .enable(a.clone() as Arc<dyn LocalDevice>, None, None)
            .await
            .unwrap();
        a.simulate_close();
        assert!(!bridge.status_for(id));

        let b = MockLocal::new(id, "thinq2");
        bridge
            .enable(b.clone() as Arc<dyn LocalDevice>, None, None)
            .await
            .unwrap();
        assert!(bridge.status_for(id));
        assert!(b.data_handler_count() >= 1);
    }

    // Packet format tests (real shipped formatters)
    #[test]
    fn local_to_lg_t2_uses_device_packet_cmd() {
        let msg = format_device_packet(10001, "mock-dev", "MODEL", "AABB");
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["cmd"], "device_packet");
        assert_eq!(v["data"], "AABB");
    }

    #[test]
    fn lg_to_local_t2_parse() {
        let buf =
            parse_lg_packet_payload(&serde_json::json!({"cmd":"packet","data":"0102"})).unwrap();
        assert_eq!(buf, vec![1, 2]);
    }

    #[test]
    fn t1_status_b64() {
        let body = format_status_body("id-1", &[0xDE, 0xAD]);
        assert_eq!(body["Body"]["Format"], "B64");
    }

    /// The regression this test exists for: `normalize_t2_state` rebuilds the state
    /// JSON from an explicit field whitelist, so a new field added to
    /// `Thinq2DeviceState` without also being added here would be silently dropped
    /// every time a saved state round-trips through it (every reconnect).
    #[test]
    fn normalize_t2_state_carries_deploy_info_through() {
        let app_info = serde_json::json!({"protocolVer": "7"});
        let platform_info = serde_json::json!({"provisioningKey": "REAL_MODEL"});
        let saved = serde_json::json!({
            "countryCode": "US",
            "apiServer": "https://api",
            "mqttServer": "ssl://mqtt:8883",
            "caCertificate": "ca",
            "privateKey": "key",
            "certificate": "cert",
            "pubTopic": "pub",
            "provTopic": "prov",
            "subTopic": "sub",
            "deployAppInfo": app_info,
            "deployPlatformInfo": platform_info,
        });
        let state: Thinq2DeviceState =
            serde_json::from_value(normalize_t2_state(saved)).unwrap();
        assert_eq!(state.deploy_app_info, Some(app_info));
        assert_eq!(state.deploy_platform_info, Some(platform_info));
    }

    /// A state saved before this field existed has neither key — must deserialize as
    /// `None`, not fail, so an old saved session keeps reconnecting.
    #[test]
    fn normalize_t2_state_defaults_deploy_info_to_none_for_old_states() {
        let saved = serde_json::json!({
            "countryCode": "US",
            "apiServer": "https://api",
            "mqttServer": "ssl://mqtt:8883",
            "caCertificate": "ca",
            "privateKey": "key",
            "certificate": "cert",
            "pubTopic": "pub",
            "provTopic": "prov",
            "subTopic": "sub",
        });
        let state: Thinq2DeviceState =
            serde_json::from_value(normalize_t2_state(saved)).unwrap();
        assert_eq!(state.deploy_app_info, None);
        assert_eq!(state.deploy_platform_info, None);
    }
}
