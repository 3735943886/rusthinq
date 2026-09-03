//! ThinQ2 HTTPS provisioning routes (/route, certificates).

use crate::certs::{Ca, is_plausible_hostname, sign_csr};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rusthinq_core::config::Config;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone)]
pub struct T2HttpState {
    pub config: Arc<Config>,
    pub ca: Arc<Ca>,
}

pub fn routes(config: Arc<Config>, ca: Arc<Ca>) -> Router {
    let state = T2HttpState { config, ca };
    Router::new()
        .route("/route", get(route))
        .route("/route/certificate", get(route_certificate))
        .route("/device/{device_id}/certificate", post(device_certificate))
        .fallback(fallback)
        .with_state(state)
}

/// Which name `/route` tells the appliance to use from now on.
///
/// Normally that's `config.hostname`: the appliance was pointed at us during setup and
/// needs a name it can resolve to reach us afterwards. That breaks down for an appliance
/// that was never set up against us and only arrives because its traffic is redirected at
/// the router — handing it `config.hostname` means it now needs a new resolvable name, and
/// because it stores what `/route` tells it, it keeps asking for that name long after the
/// redirection is gone. Echoing back the name it already asked for keeps the redirection
/// the only thing standing between it and the manufacturer's cloud.
///
/// Off by default: wrong for an appliance set up through SoftAP, which has no redirection
/// to carry it here. Also ignored for anything that isn't a plausible DNS hostname — an
/// address would be stored by the appliance and pin it to one machine.
fn advertised_host(config: &Config, requested_host: Option<&str>) -> String {
    if !config.advertise_requested_host {
        return config.hostname.clone();
    }
    match requested_host {
        Some(h) if is_plausible_hostname(h) => h.to_string(),
        _ => config.hostname.clone(),
    }
}

/// The `Host` header's value with any `:port` suffix stripped (mirrors Express's
/// `req.hostname`, which `advertised_host`'s TypeScript counterpart reads from).
fn requested_host(headers: &HeaderMap) -> Option<&str> {
    let host = headers.get(axum::http::header::HOST)?.to_str().ok()?;
    Some(host.rsplit_once(':').map_or(host, |(h, _port)| h))
}

async fn route(State(state): State<T2HttpState>, headers: HeaderMap) -> Json<Value> {
    rusthinq_core::logging::log("HTTPS", &["/route"]);
    let host = advertised_host(&state.config, requested_host(&headers));
    Json(json!({
        "resultCode": "0000",
        "result": {
            "apiServer": format!("https://{host}:{}", state.config.https_port.advertise),
            "mqttServer": format!("ssl://{host}:{}", state.config.mqtts_port.advertise),
        }
    }))
}

#[derive(Deserialize)]
struct CertQuery {
    name: Option<String>,
}

async fn route_certificate(
    State(state): State<T2HttpState>,
    Query(q): Query<CertQuery>,
) -> Json<Value> {
    if q.name.is_some() {
        Json(json!({
            "resultCode": "0000",
            "result": { "certificatePem": state.ca.cert_pem }
        }))
    } else {
        Json(json!({
            "resultCode": "0000",
            "result": ["common-server", "aws-iot"]
        }))
    }
}

#[derive(Deserialize)]
struct CsrBody {
    csr: String,
}

/// Whatever CSR the appliance sends is signed as it stands (no otp check, no subject
/// binding to `device_id`) — this is the appliance's own local cloud and the CA it
/// pinned here is one we made for it, so signing what it asks for is the point. What
/// *is* checked is that a request carrying no real CSR, or a CSR openssl refuses to
/// sign, is answered with a failure — not with `resultCode: "0000"` and the CA's own
/// certificate standing in for a signed leaf, which used to happen here and only
/// shows up later as some unrelated failure on the appliance.
async fn device_certificate(
    State(state): State<T2HttpState>,
    Path(_device_id): Path<String>,
    Json(body): Json<CsrBody>,
) -> Response {
    if !body.csr.contains("CERTIFICATE REQUEST") {
        tracing::warn!("device_certificate: request carried no CSR");
        return Json(json!({ "resultCode": "9999" })).into_response();
    }
    match sign_csr(&state.ca, &body.csr).await {
        Ok(pem) => Json(json!({
            "resultCode": "0000",
            "result": { "certificatePem": pem }
        }))
        .into_response(),
        Err(e) => {
            tracing::warn!("CSR sign failed: {e}");
            Json(json!({ "resultCode": "9999" })).into_response()
        }
    }
}

async fn fallback() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/xml;charset=utf-8")],
        "",
    )
        .into_response()
}

pub fn generate_deploy_response(payload: &Value) -> Value {
    let did = payload
        .get("did")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let cmd = payload
        .get("cmd")
        .and_then(|v| v.as_str())
        .unwrap_or("deploy")
        .to_string();
    let mid = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    json!({
        "did": did,
        "mid": mid,
        "cmd": "completeProvisioning",
        "type": 0,
        "data": {
            "result": 0,
            "host": "message",
            "appInfo": {
                "host": "message",
                "publication": {
                    "message": format!("clip/message/devices/{did}"),
                    "provisioning": format!("clip/provisioning/devices/{did}"),
                }
            },
            "provisioningType": cmd,
            "deployInterval": 600,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(advertise_requested_host: bool) -> Config {
        let toml = format!(
            r#"
                hostname = "rusthinq.local"
                ca_key_file = "ca.key"
                ca_cert_file = "ca.cert"
                https_port = 443
                mqtts_port = 8883
                advertise_requested_host = {advertise_requested_host}

                [mqtt]
                mqtt_url = "mqtt://x"
                rusthinq_prefix = "rusthinq"
            "#
        );
        rusthinq_core::config::parse_config_text(&toml).expect("parse test config")
    }

    #[test]
    fn advertised_host_defaults_to_config_hostname() {
        let config = test_config(false);
        assert_eq!(
            advertised_host(&config, Some("kic-common.lgthinq.com")),
            "rusthinq.local"
        );
        assert_eq!(advertised_host(&config, None), "rusthinq.local");
    }

    #[test]
    fn advertised_host_echoes_requested_hostname_when_enabled() {
        let config = test_config(true);
        assert_eq!(
            advertised_host(&config, Some("kic-common.lgthinq.com")),
            "kic-common.lgthinq.com"
        );
    }

    #[test]
    fn advertised_host_falls_back_when_enabled_but_no_request_host() {
        let config = test_config(true);
        assert_eq!(advertised_host(&config, None), "rusthinq.local");
    }

    #[test]
    fn advertised_host_falls_back_for_addresses_and_garbage() {
        let config = test_config(true);
        assert_eq!(
            advertised_host(&config, Some("10.1.1.45")),
            "rusthinq.local"
        );
        assert_eq!(advertised_host(&config, Some("::1")), "rusthinq.local");
        assert_eq!(
            advertised_host(&config, Some("not a host")),
            "rusthinq.local"
        );
    }

    #[test]
    fn requested_host_strips_port_from_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HOST,
            "kic-common.lgthinq.com:443".parse().unwrap(),
        );
        assert_eq!(requested_host(&headers), Some("kic-common.lgthinq.com"));
    }

    #[test]
    fn requested_host_none_when_header_absent() {
        assert_eq!(requested_host(&HeaderMap::new()), None);
    }

    fn test_state() -> T2HttpState {
        let (key_path, cert_path) = crate::certs::test_support::temp_ca_paths();
        let ca = crate::certs::test_support::fast_test_ca("rusthinq.test", &key_path, &cert_path);
        T2HttpState {
            config: Arc::new(test_config(false)),
            ca: Arc::new(ca),
        }
    }

    fn real_csr_pem() -> String {
        let key = rcgen::KeyPair::generate().expect("generate CSR key");
        let params = rcgen::CertificateParams::new(vec!["device.local".to_string()])
            .expect("build CSR params");
        params
            .serialize_request(&key)
            .expect("serialize CSR")
            .pem()
            .expect("CSR to PEM")
    }

    #[tokio::test]
    async fn device_certificate_signs_a_real_csr() {
        let state = test_state();
        let resp = device_certificate(
            State(state),
            Path("dev-1".to_string()),
            Json(CsrBody {
                csr: real_csr_pem(),
            }),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["resultCode"], "0000");
        let pem = v["result"]["certificatePem"].as_str().unwrap();
        assert!(pem.contains("BEGIN CERTIFICATE"));
        // Must be a leaf signed for this request, not the CA's own certificate standing
        // in for one — that used to be the fallback on a signing failure.
        assert_ne!(pem, state_cert_pem());
    }

    fn state_cert_pem() -> String {
        // Kept separate from test_state() so the assertion above compares against a
        // *different* CA instance's cert — a stray match would prove nothing.
        let (key_path, cert_path) = crate::certs::test_support::temp_ca_paths();
        crate::certs::test_support::fast_test_ca("rusthinq.test", &key_path, &cert_path).cert_pem
    }

    /// The bug this whole handler exists to fix: a missing or unparseable CSR, or one
    /// openssl refuses to sign, used to still answer `resultCode: "0000"` with the CA's
    /// own certificate filled in as if it were a signed leaf — a success the appliance
    /// could only report later as some unrelated failure.
    #[tokio::test]
    async fn device_certificate_answers_failure_for_missing_csr() {
        let state = test_state();
        let resp = device_certificate(
            State(state),
            Path("dev-1".to_string()),
            Json(CsrBody { csr: String::new() }),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["resultCode"], "9999");
        assert!(v.get("result").is_none());
    }

    #[tokio::test]
    async fn device_certificate_answers_failure_for_garbage_csr() {
        let state = test_state();
        let resp = device_certificate(
            State(state),
            Path("dev-1".to_string()),
            Json(CsrBody {
                csr: "-----BEGIN CERTIFICATE REQUEST-----\nnot actually one\n-----END CERTIFICATE REQUEST-----\n"
                    .to_string(),
            }),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["resultCode"], "9999");
        assert!(v.get("result").is_none());
    }
}
