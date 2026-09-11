//! rusthinq-cloud: emulates LG ThinQ cloud and exposes devices over MQTT.

#[cfg(feature = "bridge")]
mod bridge_adapter;
#[cfg(feature = "bridge")]
mod bridge_control;
mod bridge_handle;
mod certs;
mod device_bridge;
mod device_control;
mod devlist;
mod devmgr;
mod mqtt_broker;
mod mqtt_client;
mod raw_bus;
mod sim_device;
#[cfg(test)]
mod test_support;
mod thinq1;
mod thinq2;

use anyhow::{Context, Result};
use bridge_handle::Bridge;
use rusthinq_core::config::load_config;
use rusthinq_core::logging;
use rusthinq_core::mqtt::MqttSink;
use rusthinq_util::backoff::ExponentialBackoff;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

/// Binds `port` on all interfaces, retrying with exponential backoff until it
/// succeeds, instead of leaving a listener permanently dead for the rest of this
/// process's life the moment one bind attempt fails (a lingering previous instance
/// mid-restart releases its port eventually; a genuine misconfiguration doesn't,
/// but retrying forever is still harmless and self-heals the moment it's fixed) —
/// the same "keep retrying" treatment MQTT/LG-cloud reconnects already get.
/// Logged at `error!`, unconditionally: unlike the topic-filtered
/// `logging::log("status", ...)` used for routine status lines, a stuck bind should
/// never be silenced by `config.toml`'s `log = [...]` list.
async fn bind_with_retry(label: &str, port: u16) -> TcpListener {
    let mut backoff = ExponentialBackoff::for_local_control_plane();
    loop {
        match TcpListener::bind(("0.0.0.0", port)).await {
            Ok(listener) => return listener,
            Err(e) => {
                let delay = backoff.next_delay();
                tracing::error!("{label} bind failed on {port}: {e} (retrying in {delay:?})");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./config.toml"));
    let config_dir = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    let mut config = load_config(&config_path).with_context(|| {
        format!(
            "failed to load config from {} (pass a TOML config path as argv[1])",
            config_path.display()
        )
    })?;

    config.ca_key_file = config_dir
        .join(&config.ca_key_file)
        .to_string_lossy()
        .into();
    config.ca_cert_file = config_dir
        .join(&config.ca_cert_file)
        .to_string_lossy()
        .into();
    if let Some(ref mut bridge) = config.bridge {
        bridge.storage_path = config_dir
            .join(&bridge.storage_path)
            .to_string_lossy()
            .into();
    }
    if let Some(ref mut devices) = config.devices {
        devices.rhai_dir = config_dir.join(&devices.rhai_dir).to_string_lossy().into();
    }
    // On by default (not an opt-in feature — see MqttConfig::state_file's doc
    // comment): falls back to a default path next to the config file rather than
    // being absent when unset.
    config.mqtt.state_file = Some(
        config_dir
            .join(
                config
                    .mqtt
                    .state_file
                    .as_deref()
                    .unwrap_or("mqtt_state.json"),
            )
            .to_string_lossy()
            .into(),
    );

    let enabled: std::collections::HashMap<String, bool> =
        config.log.iter().map(|k| (k.clone(), true)).collect();
    logging::set_filter(move |topic| {
        enabled.get(topic).copied().unwrap_or(false) || enabled.get("all").copied().unwrap_or(false)
    });

    logging::log(
        "status",
        &[&format!(
            "rusthinq-cloud starting hostname={} https={} mqtts={}",
            config.hostname, config.https_port.bind, config.mqtts_port.bind,
        )],
    );

    let ca = certs::load_or_create(
        &config.hostname,
        Path::new(&config.ca_key_file),
        Path::new(&config.ca_cert_file),
    )?;
    logging::log("status", &["CA certificate ready"]);

    let manager = devmgr::DeviceManager::new();

    // Rhai device scripting (see rusthinq_devices::scripting) — only exists at all
    // when [devices] is configured, same opt-in-by-presence pattern as [bridge]/
    // raw_prefix. Must run before any device can connect (before DeviceBridge exists),
    // since registry.rs's script fallback checks rhai_dir on every lookup. Compiled
    // out entirely without the `scripting` feature.
    #[cfg(feature = "scripting")]
    if let Some(ref devices) = config.devices {
        rusthinq_devices::scripting::init(PathBuf::from(&devices.rhai_dir), devices.watch);
        logging::log(
            "status",
            &[&format!(
                "rhai device scripting enabled: dir={} watch={}",
                devices.rhai_dir, devices.watch
            )],
        );
    }
    #[cfg(not(feature = "scripting"))]
    if config.devices.is_some() {
        tracing::warn!(
            "config.toml has a [devices] section but this binary was built without \
             the `scripting` feature — ignoring it, no .rhai device scripts available"
        );
    }

    // Single shared MQTT control-plane sink — used by DeviceBridge publishes AND the rumqttc client.
    let mqtt_sink = MqttSink::new(config.mqtt.clone());
    let mqtt_dyn: Arc<dyn rusthinq_core::mqtt::MqttConnection> = mqtt_sink.clone();
    let device_bridge = device_bridge::DeviceBridge::new(mqtt_sink.clone());
    device_bridge.attach_mqtt_sink(&mqtt_sink);

    // Raw wire-frame observer/inject bus, over the same MQTT connection (see
    // raw_bus.rs) — only exists at all when raw_prefix is configured.
    if config.mqtt.raw_prefix.is_some() {
        raw_bus::register_inject(&mqtt_sink, manager.clone());
    }

    // Optional LG cloud bridge (compiled out entirely without the `bridge`
    // feature — see bridge_handle.rs).
    #[cfg(feature = "bridge")]
    let lg_bridge: Option<Arc<Bridge>> = config.bridge.as_ref().map(|b| {
        let storage = Arc::new(rusthinq_bridge::JsonStorage::new(&b.storage_path));
        Bridge::new(storage)
    });
    #[cfg(not(feature = "bridge"))]
    let lg_bridge: Option<Arc<Bridge>> = {
        if config.bridge.is_some() {
            tracing::warn!(
                "config.toml has a [bridge] section but this binary was built \
                 without the `bridge` feature — ignoring it, no LG cloud bridge available"
            );
        }
        None
    };

    // Retained <rusthinq_prefix>/devices snapshot (see devlist.rs) — republished on
    // every connect/disconnect so a late MQTT subscriber sees current state at once.
    let device_list = devlist::DeviceListPublisher::new(
        mqtt_dyn.clone(),
        manager.clone(),
        device_bridge.clone(),
        lg_bridge.clone(),
    );

    // "Forget device" — clears retained MQTT state (and saved bridge pairing state,
    // if any) for an id regardless of whether it's currently connected. Unlike
    // bridge_control.rs below, always registered: it doesn't need the `bridge`
    // feature to be useful (see device_control.rs).
    device_control::register(
        &mqtt_sink,
        mqtt_dyn.clone(),
        lg_bridge.clone(),
        device_list.clone(),
    );

    // A bridge session can attach or detach from a path nothing else here awaits —
    // e.g. `on_local_device`'s auto-restore on reconnect, spawned on its own task.
    // Without this, the retained snapshot below could keep reporting a stale
    // `bridged: false` for a device whose session actually came back up fine, forever
    // (nothing else re-triggers the publish for that path).
    #[cfg(feature = "bridge")]
    if let Some(ref br) = lg_bridge {
        let device_list = device_list.clone();
        br.set_on_session_change_hook(Arc::new(move || device_list.publish()));
    }

    // Owner-given device names from the ThinQ account (see rusthinq_bridge::Bridge's
    // `name`/`start_name_refresh_loop`) — same "republish when it changes" reasoning
    // as the session-change hook above, so a freshly fetched name reaches a
    // subscriber without waiting for some unrelated event to next call publish().
    #[cfg(feature = "bridge")]
    if let Some(ref br) = lg_bridge {
        let device_list = device_list.clone();
        br.set_on_names_changed_hook(Arc::new(move || device_list.publish()));
        br.start_name_refresh_loop();
    }

    // Retained, but only ever *written* on a device connect/disconnect above — if the
    // broker itself loses its retained store (e.g. restarted) around the same time this
    // MQTT client reconnects, nothing else would put the snapshot back until the next
    // device event. `on_discovery` is exactly the generic "resync everything on
    // reconnect" hook device_bridge.rs's republish_all also rides.
    {
        let device_list = device_list.clone();
        mqtt_sink.on_discovery(move || device_list.publish());
    }

    {
        let device_bridge = device_bridge.clone();
        #[cfg(feature = "bridge")]
        let lg_bridge = lg_bridge.clone();
        let mqtt_dyn = mqtt_dyn.clone();
        let device_list = device_list.clone();
        let raw_prefix = config.mqtt.raw_prefix.clone();
        manager.on_new_device(move |dev| {
            if let Some(ref raw_prefix) = raw_prefix {
                raw_bus::attach(&mqtt_dyn, &dev, raw_prefix);
            }
            device_bridge.new_device(dev.clone());
            #[cfg(feature = "bridge")]
            if let Some(ref br) = lg_bridge {
                br.on_local_device(Arc::new(bridge_adapter::ConnectedAsLocal(dev)));
            }
            device_list.publish();
        });
    }
    {
        let device_list = device_list.clone();
        manager.on_drop_device(move |_id| device_list.publish());
    }

    // Bridge enable/disable, and LG account login/logout, all over MQTT — see
    // bridge_control.rs for the full topic list.
    #[cfg(feature = "bridge")]
    if let Some(ref br) = lg_bridge {
        bridge_control::register(
            &mqtt_sink,
            mqtt_dyn.clone(),
            manager.clone(),
            br.clone(),
            device_list.clone(),
        );
    }

    // MQTT control-plane client — same sink instance so publish_fn is set where DeviceBridge publishes
    if config.mqtt_enabled {
        let sink = mqtt_sink.clone();
        tokio::spawn(async move {
            if let Err(e) = mqtt_client::start_mqtt_client(sink).await {
                logging::log(
                    "status",
                    &[&format!("MQTT control-plane client ended: {e}")],
                );
            }
        });
    }

    // Optional web dashboard (rusthinq_gui) — its own independent MQTT client, not
    // wired into `mqtt_sink`/`DeviceBridge` at all (see rusthinq-gui's crate docs).
    // Same opt-in-by-presence pattern as `[bridge]`/`[devices]`.
    #[cfg(feature = "gui")]
    if let Some(ref gui) = config.gui {
        let gui_cfg = gui.clone();
        let mqtt_cfg = config.mqtt.clone();
        tokio::spawn(async move {
            if let Err(e) = rusthinq_gui::run(gui_cfg, mqtt_cfg).await {
                logging::log("status", &[&format!("rusthinq-gui ended: {e}")]);
            }
        });
    }
    #[cfg(not(feature = "gui"))]
    if config.gui.is_some() {
        tracing::warn!(
            "config.toml has a [gui] section but this binary was built without the \
             `gui` feature — ignoring it, no web dashboard available"
        );
    }

    // Not every connection arriving on the HTTPS port is one rusthinq should answer —
    // an appliance reached by port redirection (see advertise_requested_host) sends
    // every 443 connection it makes here, including firmware/SOTA downloads that live
    // on a public CDN and must validate against a real root, not rusthinq's CA. See
    // thinq2/sni_passthrough.rs and thinq2/firmware.rs. Built before the acceptor
    // below so a device's own deploy info can immunize its self-reported endpoints the
    // moment it finishes provisioning, not only after a lucky first HTTPS hit.
    let firmware_hosts = Arc::new(thinq2::firmware::FirmwareHosts::new());

    let broker = Arc::new(mqtt_broker::Broker::new());
    let t2_acceptor = thinq2::device::DeviceAcceptor::new(
        broker.clone(),
        manager.clone(),
        firmware_hosts.clone(),
    );

    // Device simulator (see sim_device.rs) — the other half of what the old plaintext
    // 1884 listener did (bootstrapping a brand-new fake device with no TLS/certs, for
    // local RE/dev work), moved onto this same already-authenticated connection.
    // Registering it is the only thing that makes it reachable; without raw_prefix
    // there is no listener anywhere that could stand in for it.
    if let Some(ref raw_prefix) = config.mqtt.raw_prefix {
        sim_device::register(
            &mqtt_sink,
            t2_acceptor.clone(),
            broker.clone(),
            raw_prefix.clone(),
        );
    }

    #[cfg(feature = "bridge")]
    if let Some(ref br) = lg_bridge {
        let fh = firmware_hosts.clone();
        br.set_note_urls_hook(Arc::new(move |payload| fh.note_urls_in(payload)));
    }

    // Device-facing TLS (HTTPS + MQTTS): OpenSSL with legacy CBC-SHA / TLS1.0
    // so RTK_RTL8711am and similar CLIP modules can complete handshake (PR#131).
    let device_tls = match certs::device_ssl_acceptor(&ca, &config.hostname) {
        Ok(a) => {
            logging::log(
                "status",
                &["device TLS: OpenSSL legacy profile (TLS1.0+, SECLEVEL=0), per-SNI leaf certs"],
            );
            Some(a)
        }
        Err(e) => {
            // MQTTS and HTTPS both depend on this — neither listener can come up
            // without it, so this isn't a routine status line.
            tracing::error!("device OpenSSL TLS config failed: {e:#}");
            None
        }
    };

    // MQTTS
    if let Some(ssl_acceptor) = device_tls.clone() {
        let b = broker.clone();
        let port = config.mqtts_port.bind;
        tokio::spawn(async move {
            // Outer loop: an `accept()` failure (not a per-connection TLS failure,
            // which is handled below and never reaches here) drops the whole
            // listener — rebind rather than leaving MQTTS permanently dead for the
            // rest of this process's life, the same as a bind failure itself.
            loop {
                let listener = bind_with_retry("MQTTS", port).await;
                logging::log(
                    "status",
                    &[&format!("MQTTS listening on {port} (legacy device TLS)")],
                );
                loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            let ssl_acceptor = ssl_acceptor.clone();
                            let b = b.clone();
                            tokio::spawn(async move {
                                match certs::accept_device_tls(&ssl_acceptor, stream).await {
                                    Ok(tls) => b.accept_tls(tls).await,
                                    Err(e) => {
                                        // A device's own reconnect attempt failing here is a
                                        // real, actionable event (it means that device is now
                                        // completely dark until this succeeds) — not routine
                                        // noise. Previously logged at debug!, which the default
                                        // "info" filter drops entirely, making a device's TLS
                                        // handshake silently and permanently failing look
                                        // indistinguishable from it just being offline.
                                        tracing::warn!(
                                            %peer,
                                            "MQTTS TLS accept failed (legacy module?): {e:#}"
                                        );
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("MQTTS accept error: {e} (rebinding)");
                            break;
                        }
                    }
                }
            }
        });
    }

    // HTTPS ThinQ2 provisioning (/route, certificate, …)
    if let Some(ssl_acceptor) = device_tls.clone() {
        let port = config.https_port.bind;
        let ca = Arc::new(ca.clone());
        let cfg = Arc::new(config.clone());
        let router = thinq2::provisioning::routes(cfg, ca.clone());
        let firmware_hosts = firmware_hosts.clone();
        tokio::spawn(async move {
            // Outer loop: rebind if `accept()` itself ever fails, instead of
            // leaving HTTPS permanently dead for the rest of this process's life —
            // same reasoning as the MQTTS listener above.
            loop {
                let listener = bind_with_retry("HTTPS", port).await;
                logging::log(
                    "status",
                    &[&format!("HTTPS listening on {port} (legacy device TLS)")],
                );
                loop {
                    let (stream, peer) = match listener.accept().await {
                        Ok(pair) => pair,
                        Err(e) => {
                            tracing::error!("HTTPS accept error: {e} (rebinding)");
                            break;
                        }
                    };
                    let ssl_acceptor = ssl_acceptor.clone();
                    let router = router.clone();
                    let firmware_hosts = firmware_hosts.clone();
                    tokio::spawn(async move {
                        // Decide from the ClientHello, before any TLS state
                        // exists: a name already known as a firmware/SOTA
                        // host is spliced to the real server so the
                        // appliance validates against it directly; anything
                        // else is terminated here exactly as before this
                        // existed.
                        let sni = thinq2::sni_passthrough::peek_sni(&stream).await;
                        if let Some(name) = sni.as_deref()
                            && firmware_hosts.has(name)
                        {
                            logging::log(
                                "status",
                                &[&format!("HTTPS passthrough: {name} -> real server")],
                            );
                            if let Err(e) =
                                thinq2::sni_passthrough::splice_to_real_host(stream, name).await
                            {
                                tracing::debug!(
                                    %peer, name, "HTTPS passthrough failed: {e:#}"
                                );
                            }
                            return;
                        }

                        match certs::accept_device_tls(&ssl_acceptor, stream).await {
                            Ok(tls) => {
                                // Getting this far means a client completed a
                                // TLS handshake against a certificate issued
                                // for this name — proof rusthinq answers it,
                                // which the passthrough paths above must
                                // never override for this name again.
                                if let Some(name) = sni.as_deref() {
                                    firmware_hosts.confirm_local(name);
                                }
                                let _ = hyper_util::server::conn::auto::Builder::new(
                                    hyper_util::rt::TokioExecutor::new(),
                                )
                                .serve_connection(
                                    hyper_util::rt::TokioIo::new(tls),
                                    hyper_util::service::TowerToHyperService::new(router),
                                )
                                .await;
                            }
                            Err(e) => {
                                // A name terminated here that the appliance
                                // immediately reset is the same symptom a
                                // firmware download produces: it wanted a
                                // real root, not ours. Note it, so its retry
                                // (appliances do retry) gets passed through
                                // next time — a catch-all for the cmd/field
                                // shapes note_urls_in doesn't already know.
                                if let Some(name) = sni.as_deref() {
                                    firmware_hosts.note(&format!("https://{name}/"));
                                }
                                // See the matching MQTTS warn! above: this used to be debug!,
                                // invisible under the default "info" filter, so a device stuck
                                // failing its handshake here looked identical to it being
                                // offline for an unrelated reason.
                                tracing::warn!(
                                    %peer,
                                    "HTTPS TLS accept failed (legacy module?): {e:#}"
                                );
                            }
                        }
                    });
                }
            }
        });
    }

    // ThinQ1 HTTP
    {
        let port = config.thinq1_https_port.bind;
        let meta = thinq1::http::device_metadata_store();
        let router = thinq1::http::routes(meta.clone());
        let acceptor = thinq1::device::DeviceAcceptor::new(meta, manager.clone());
        tokio::spawn(async move {
            // Kept alive for as long as this task runs (never used directly again;
            // constructing it is the point — see DeviceAcceptor::new).
            let _acceptor = acceptor;
            // Outer loop: `axum::serve` only returns when its listener dies —
            // rebind rather than leaving ThinQ1 HTTP permanently dead for the rest
            // of this process's life, same reasoning as MQTTS/HTTPS above.
            loop {
                let listener = bind_with_retry("ThinQ1 HTTP", port).await;
                logging::log("status", &[&format!("ThinQ1 HTTP listening on {port}")]);
                if let Err(e) = axum::serve(listener, router.clone()).await {
                    tracing::error!("ThinQ1 HTTP ended: {e} (rebinding)");
                }
            }
        });
    }

    // ThinQ1 device TCP port
    {
        let port = config.thinq1_port.bind;
        let meta = thinq1::http::device_metadata_store();
        let acceptor = thinq1::device::DeviceAcceptor::new(meta, manager.clone());
        tokio::spawn(async move {
            // Outer loop: rebind if `accept()` itself ever fails, instead of
            // leaving this port permanently dead for the rest of this process's
            // life — same reasoning as the other listeners above.
            loop {
                let listener = bind_with_retry("ThinQ1 device port", port).await;
                logging::log(
                    "status",
                    &[&format!("ThinQ1 device port listening on {port}")],
                );
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let a = acceptor.clone();
                            tokio::spawn(async move {
                                a.accept(stream).await;
                            });
                        }
                        Err(e) => {
                            tracing::error!("ThinQ1 accept error: {e} (rebinding)");
                            break;
                        }
                    }
                }
            }
        });
    }

    // No more always-on management HTTP/WS surface: the RE decode/catalog/export
    // helpers are plain library calls now (rusthinq_util::decode, used directly by
    // rusthinq-tools/MCP), device/bridge status is the retained `<prefix>/devices`
    // MQTT topic (devlist.rs), and bridge enable/disable/login/logout all moved to
    // MQTT (bridge_control.rs).
    logging::log("status", &["rusthinq-cloud ready"]);
    std::future::pending::<()>().await;

    Ok(())
}
