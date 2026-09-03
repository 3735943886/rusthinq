//! ThinQ1 upstream RTI TLS connection (port of bridge/thinq1connection.ts).
//!
//! Unlike rumqttc's `AsyncClient`/`EventLoop` (used for ThinQ2, see
//! thinq2_conn.rs), this is a hand-rolled TCP+TLS session with no built-in
//! reconnect — a network blip on the LG side used to kill the bridge for that
//! device silently and permanently until the local device itself reconnected.
//! [`connect_thinq1`] now owns a supervising loop that re-dials on failure so a
//! transient upstream drop is just a brief gap, not a dead bridge session.

use crate::pair::Thinq1DeviceState;
use anyhow::Context;
use rusthinq_util::backoff::ExponentialBackoff;
use rusthinq_util::length_prefixed_frame::{self, Splitter};
use rustls::pki_types::{CertificateDer, ServerName};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::ClientConfig;

type TlsHalf = TlsStream<TcpStream>;

/// Cloneable send handle for local→LG status.
#[derive(Clone)]
pub struct Thinq1Handle {
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    is_live: Arc<AtomicBool>,
    device_id: String,
    last_state: Arc<rusthinq_util::sync::Mutex<Option<Vec<u8>>>>,
    stopped: Arc<AtomicBool>,
}

impl Thinq1Handle {
    pub fn send_from_local(&self, data: &[u8]) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        *self.last_state.lock() = Some(data.to_vec());
        rusthinq_core::logging::log(
            "bridge",
            &[&format!(
                "{} -> {}",
                self.device_id,
                rusthinq_util::hex::encode(data)
            )],
        );
        if !self.is_live.load(Ordering::SeqCst) {
            return;
        }
        let body = format_status_body(&self.device_id, data);
        let frame = length_prefixed_frame::make(body.to_string().as_bytes());
        let _ = self.write_tx.send(frame);
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

pub async fn connect_thinq1(
    state: &Thinq1DeviceState,
    device_id: &str,
    model_name: &str,
    device_type: Option<&str>,
) -> anyhow::Result<(Thinq1Handle, mpsc::UnboundedReceiver<serde_json::Value>)> {
    connect_thinq1_impl(state, device_id, model_name, device_type, None).await
}

/// `extra_trust_root` exists only so tests can point this at a local, self-signed
/// TLS acceptor without weakening the real thing: production always passes `None`,
/// which means only the OS's own trusted CA roots (loaded fresh per connect via
/// `rustls-native-certs`) can vouch for `state.rti_server`'s certificate.
async fn connect_thinq1_impl(
    state: &Thinq1DeviceState,
    device_id: &str,
    model_name: &str,
    device_type: Option<&str>,
    extra_trust_root: Option<CertificateDer<'static>>,
) -> anyhow::Result<(Thinq1Handle, mpsc::UnboundedReceiver<serde_json::Value>)> {
    // Fail fast on the first attempt (bad state / totally unreachable server);
    // later drops are handled by the supervising loop below instead of bailing.
    let (reader, writer) = connect_once(
        state,
        device_id,
        model_name,
        device_type,
        extra_trust_root.clone(),
    )
    .await?;

    let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (from_lg_tx, from_lg_rx) = mpsc::unbounded_channel();
    let is_live = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicBool::new(false));
    let last_state = Arc::new(rusthinq_util::sync::Mutex::new(None::<Vec<u8>>));

    // Alive, every 60s for the life of the handle (reconnects reuse the same
    // write_tx/write_rx pair, so this doesn't need to know about them).
    let _ = write_tx.send(length_prefixed_frame::make(
        format_alive(device_id).as_bytes(),
    ));
    let write_tx_alive = write_tx.clone();
    let did_alive = device_id.to_string();
    let stopped_a = stopped.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            if stopped_a.load(Ordering::SeqCst) {
                break;
            }
            if write_tx_alive
                .send(length_prefixed_frame::make(
                    format_alive(&did_alive).as_bytes(),
                ))
                .is_err()
            {
                break;
            }
        }
    });

    let handle = Thinq1Handle {
        write_tx: write_tx.clone(),
        is_live: is_live.clone(),
        device_id: device_id.to_string(),
        last_state: last_state.clone(),
        stopped: stopped.clone(),
    };

    let state = state.clone();
    let device_id_owned = device_id.to_string();
    let model_name_owned = model_name.to_string();
    let device_type_owned = device_type.map(|s| s.to_string());
    tokio::spawn(async move {
        let mut backoff = ExponentialBackoff::for_external_upstream();
        let mut write_rx = write_rx;
        let mut conn = Some((reader, writer));
        loop {
            if stopped.load(Ordering::SeqCst) {
                break;
            }
            let (reader, writer) = match conn.take() {
                Some(c) => c,
                None => {
                    match connect_once(
                        &state,
                        &device_id_owned,
                        &model_name_owned,
                        device_type_owned.as_deref(),
                        extra_trust_root.clone(),
                    )
                    .await
                    {
                        Ok(c) => {
                            backoff.reset();
                            c
                        }
                        Err(e) => {
                            let delay = backoff.next_delay();
                            tracing::error!(
                                "{device_id_owned} reconnect failed: {e:#} (retrying in {delay:?})"
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                }
            };
            is_live.store(false, Ordering::SeqCst);
            run_session(
                reader,
                writer,
                &device_id_owned,
                &is_live,
                &last_state,
                &mut write_rx,
                &write_tx,
                &from_lg_tx,
                &stopped,
            )
            .await;
            if stopped.load(Ordering::SeqCst) {
                break;
            }
            let delay = backoff.next_delay();
            tracing::warn!("{device_id_owned} disconnected, reconnecting in {delay:?}");
            tokio::time::sleep(delay).await;
        }
        rusthinq_core::logging::log("bridge", &[&format!("{device_id_owned} stopped")]);
    });

    Ok((handle, from_lg_rx))
}

/// Bounds every network step of a connect attempt so `stopped` (checked once
/// per reconnect-loop iteration) is never blocked behind a half-open socket or
/// a handshake the real LG server never finishes.
const RTI_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

async fn connect_once(
    state: &Thinq1DeviceState,
    device_id: &str,
    model_name: &str,
    device_type: Option<&str>,
    extra_trust_root: Option<CertificateDer<'static>>,
) -> anyhow::Result<(ReadHalf<TlsHalf>, WriteHalf<TlsHalf>)> {
    let _ = reqwest::Client::builder()
        .timeout(RTI_CONNECT_TIMEOUT)
        .build()?
        .post(format!(
            "{}/lgehadm/api/Device/TotalDeviceInfoSvc",
            state.http_server.trim_end_matches('/')
        ))
        .header("Accept", "text/xml")
        .header("content-type", "text/xml;charset=utf-8")
        .header("x-lgedm-userid", "lgehadmUser")
        .header(
            "x-lgedm-password",
            "bxLoLAZ+rp3oJDbEzRuIfAG4YumeqwWM9l6uUH6TupQ=",
        )
        .header("x-lgedm-deviceid", device_id)
        .header("x-lgedm-devicetype", device_type.unwrap_or("201"))
        .body(format!(
            "<lgedmRoot><countryCode>WW</countryCode><modelName>{model_name}</modelName>\
             <itemList><item>THINQ_TIME_SYNC_URI</item>\
             <elementList><elementCode>pushDetailYn</elementCode>\
             <elementValue>Y</elementValue></elementList></itemList></lgedmRoot>"
        ))
        .send()
        .await;

    rusthinq_core::logging::log(
        "bridge",
        &[&format!("{device_id} connecting to {}", state.rti_server)],
    );
    let (host, port) = parse_host_port(&state.rti_server)?;

    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    if let Some(extra) = extra_trust_root {
        let _ = roots.add(extra);
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = tokio::time::timeout(RTI_CONNECT_TIMEOUT, TcpStream::connect((host.as_str(), port)))
        .await
        .context("TCP connect to RTI server timed out")??;
    let server_name = ServerName::try_from(host.clone())
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .to_owned();
    let tls = tokio::time::timeout(RTI_CONNECT_TIMEOUT, connector.connect(server_name, tcp))
        .await
        .context("TLS handshake with RTI server timed out")??;
    rusthinq_core::logging::log("bridge", &[&format!("{device_id} connected")]);
    Ok(tokio::io::split(tls))
}

/// Drive one live TCP+TLS session until it errors out (read EOF/error, or the
/// write side fails), multiplexing device→LG frames from `write_rx` with
/// LG→device frames read off the socket. Returns (doesn't reconnect itself —
/// that's the caller's supervising loop) so a network blip just means this
/// returns and the caller re-dials.
#[allow(clippy::too_many_arguments)]
async fn run_session(
    mut reader: ReadHalf<TlsHalf>,
    mut writer: WriteHalf<TlsHalf>,
    device_id: &str,
    is_live: &AtomicBool,
    last_state: &rusthinq_util::sync::Mutex<Option<Vec<u8>>>,
    write_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    write_tx: &mpsc::UnboundedSender<Vec<u8>>,
    from_lg_tx: &mpsc::UnboundedSender<serde_json::Value>,
    stopped: &AtomicBool,
) {
    let mut splitter = Splitter::new(1_000_000);
    let mut buf = [0u8; 8192];
    loop {
        if stopped.load(Ordering::SeqCst) {
            return;
        }
        tokio::select! {
            res = reader.read(&mut buf) => {
                match res {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        let Ok(frames) = splitter.feed(&buf[..n]) else {
                            return;
                        };
                        for payload in frames {
                            if let Ok(j) = serde_json::from_slice::<serde_json::Value>(&payload) {
                                handle_lg_json(&j, device_id, is_live, last_state, write_tx, from_lg_tx);
                            }
                        }
                    }
                }
            }
            frame = write_rx.recv() => {
                match frame {
                    Some(frame) => {
                        if writer.write_all(&frame).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
        }
    }
}

fn handle_lg_json(
    j: &serde_json::Value,
    device_id: &str,
    is_live: &AtomicBool,
    last_state: &rusthinq_util::sync::Mutex<Option<Vec<u8>>>,
    write_tx: &mpsc::UnboundedSender<Vec<u8>>,
    from_lg: &mpsc::UnboundedSender<serde_json::Value>,
) {
    let Some(body) = j.get("Body") else {
        return;
    };
    if body.get("CmdOpt").and_then(|v| v.as_str()) == Some("Start") {
        is_live.store(true, Ordering::SeqCst);
        if let Some(ref st) = *last_state.lock() {
            let msg = format_status_body(device_id, st);
            let _ = write_tx.send(length_prefixed_frame::make(msg.to_string().as_bytes()));
        }
        return;
    }
    if body.get("CmdOpt").and_then(|v| v.as_str()) == Some("Stop") {
        is_live.store(false, Ordering::SeqCst);
        return;
    }
    rusthinq_core::logging::log("bridge", &[&format!("{device_id} <- {body}")]);
    let _ = from_lg.send(body.clone());
    if body.get("ReturnCode").is_none()
        && let Some(cmd_w_id) = body.get("CmdWId")
    {
        let ack = serde_json::json!({
            "Header": { "x-lgedm-deviceId": device_id },
            "Body": { "CmdWId": cmd_w_id, "ReturnCode": "0000" }
        });
        let _ = write_tx.send(length_prefixed_frame::make(ack.to_string().as_bytes()));
    }
}

pub fn format_status_body(device_id: &str, data: &[u8]) -> serde_json::Value {
    serde_json::json!({
        "Header": { "x-lgedm-deviceId": device_id },
        "Body": {
            "CmdWId": format!("n-{device_id}"),
            "ReturnCode": "0000",
            "Format": "B64",
            "Data": base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                data
            ),
        }
    })
}

fn format_alive(device_id: &str) -> String {
    serde_json::json!({
        "Header": { "x-lgedm-deviceId": device_id },
        "Body": {
            "CmdWId": uuid::Uuid::new_v4().to_string(),
            "Cmd": "Alive",
        }
    })
    .to_string()
}

fn parse_host_port(s: &str) -> anyhow::Result<(String, u16)> {
    let mut parts = s.split(':');
    let host = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("bad rtiServer"))?
        .to_string();
    let port: u16 = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("bad rtiServer port"))?
        .parse()?;
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusthinq_util::length_prefixed_frame;

    #[test]
    fn status_body_is_b64_and_framed() {
        let body = format_status_body("id-1", &[0xAA, 0xBB]);
        assert_eq!(body["Body"]["Format"], "B64");
        let data = body["Body"]["Data"].as_str().unwrap();
        assert_eq!(
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data).unwrap(),
            vec![0xAA, 0xBB]
        );
        let frame = length_prefixed_frame::make(body.to_string().as_bytes());
        assert!(frame.len() > 4);
    }
}

#[cfg(test)]
mod reconnect_tests {
    use super::*;
    use rcgen::{CertificateParams, KeyPair};
    use rustls::ServerConfig;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    /// A real self-signed TLS acceptor plus its own certificate (handed back so
    /// the test client can trust it explicitly via `extra_trust_root` — the
    /// production path only trusts the OS's real CA roots, so this is the only
    /// way a test server's cert can pass verification).
    fn test_tls_acceptor() -> (TlsAcceptor, CertificateDer<'static>) {
        let params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let key_pair = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();

        let mut cert_reader = std::io::Cursor::new(cert.pem().into_bytes());
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut key_reader = std::io::Cursor::new(key_pair.serialize_pem().into_bytes());
        let key = rustls_pemfile::private_key(&mut key_reader)
            .unwrap()
            .unwrap();

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.clone(), key)
            .unwrap();
        (TlsAcceptor::from(Arc::new(config)), certs[0].clone())
    }

    #[tokio::test]
    async fn reconnects_after_upstream_drops_connection() {
        // Idempotent: ignore "already installed" if another test set it first.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (acceptor, cert_der) = test_tls_acceptor();

        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_count_srv = accept_count.clone();
        tokio::spawn(async move {
            for i in 0..2 {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    break;
                };
                accept_count_srv.fetch_add(1, Ordering::SeqCst);
                // Read the client's initial "Alive" frame so we know this is a
                // genuine protocol session, not just a bare TCP+TLS handshake.
                let mut buf = [0u8; 512];
                let _ = tls.read(&mut buf).await;
                if i == 0 {
                    // Simulate an upstream drop: just close this connection.
                    drop(tls);
                } else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        });

        let state = Thinq1DeviceState {
            // Must match the test cert's SAN ("localhost") — real cert
            // verification now checks hostname, not just CA trust.
            rti_server: format!("localhost:{port}"),
            // Refused instantly (nothing listens on port 1); connect_once discards
            // this POST's result either way, so it doesn't block the TLS connect.
            http_server: "http://127.0.0.1:1".to_string(),
        };

        let (_handle, _from_lg) = connect_thinq1_impl(
            &state,
            "dev-reconnect-test",
            "MODEL",
            None,
            Some(cert_der),
        )
        .await
        .expect("initial connect must succeed");

        let deadline = Instant::now() + Duration::from_secs(5);
        while accept_count.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            accept_count.load(Ordering::SeqCst),
            2,
            "client must reconnect after the upstream drops the connection"
        );
    }
}
