//! ThinQ2 device pairing (port of Thinq2Device.pair in bridge/thinqApi.ts).

use crate::state::Environment;
use crate::util::{SubprocessOptions, subprocess};
use base64::Engine;
use rand::RngCore;
use rsa::pkcs8::DecodePublicKey;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const IOT_BASE_URL: &str = "https://common.lgthinq.com";
/// How long to wait for LG's `/route` lookup during pairing before giving up.
const ROUTE_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Thinq2DeviceState {
    pub country_code: String,
    pub api_server: String,
    pub mqtt_server: String,
    pub ca_certificate: String,
    pub private_key: String,
    pub certificate: String,
    pub pub_topic: String,
    pub prov_topic: String,
    pub sub_topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Thinq1DeviceState {
    pub rti_server: String,
    pub http_server: String,
}

/// Result of pair(): LG-side state + ciphertext for addDevice.
pub struct PairResult {
    pub state: Thinq2DeviceState,
    /// Final ciphertext buffer returned by pair() for addDevice body.
    pub add_device_ciphertext: Vec<u8>,
}

async fn api_fetch_json(
    url: &str,
    method: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let mut req = match method {
        "POST" => client.post(url),
        _ => client.get(url),
    };
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    if let Some(b) = body {
        req = req
            .header("Content-Type", "application/json")
            .body(b.to_string());
    }
    let resp = req.send().await?;
    let out: serde_json::Value = resp.json().await?;
    let code = out.get("resultCode").and_then(|v| v.as_str()).unwrap_or("");
    if !code.is_empty() && code != "0000" {
        anyhow::bail!("thinq route error {code}: {out}");
    }
    // Some IoT endpoints return result wrapper, others return flat body
    Ok(out.get("result").cloned().unwrap_or(out))
}

fn rsa_pkcs1_encrypt(public_key_pem: &str, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
    // OTP publicKey may be bare base64 or PEM
    let pem = if public_key_pem.contains("BEGIN") {
        public_key_pem.to_string()
    } else {
        format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
            public_key_pem
                .as_bytes()
                .chunks(64)
                .map(|c| std::str::from_utf8(c).unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    let pubkey = RsaPublicKey::from_public_key_pem(&pem).or_else(|_| {
        // try SPKI DER from base64
        let der = base64::engine::general_purpose::STANDARD.decode(public_key_pem.trim())?;
        RsaPublicKey::from_public_key_der(&der).map_err(|e| anyhow::anyhow!("{e}"))
    })?;
    let mut rng = rand::thread_rng();
    let encrypted = pubkey
        .encrypt(&mut rng, Pkcs1v15Encrypt, plaintext)
        .map_err(|e| anyhow::anyhow!("RSA encrypt: {e}"))?;
    Ok(encrypted)
}

/// Generate EC P-256 key + CSR via openssl (matches TS subprocess path).
async fn generate_ec_key_and_csr() -> anyhow::Result<(String, String, String)> {
    let private_key = subprocess(
        "openssl",
        &[
            "ecparam",
            "-genkey",
            "-name",
            "prime256v1",
            "-noout",
            "-out",
            "-",
        ],
        "",
        SubprocessOptions::default(),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e.0))?;

    let public_key = subprocess(
        "openssl",
        &["ec", "-pubout", "-out", "-"],
        &private_key,
        SubprocessOptions::default(),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e.0))?;

    let csr = subprocess(
        "sh",
        &[
            "-c",
            "cat | openssl req -new -key /dev/stdin -subj '/CN=*.clip.com/O=LGE/C=KR'",
        ],
        &private_key,
        SubprocessOptions::default(),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e.0))?;

    Ok((private_key, public_key, csr))
}

/// Full ThinQ2 pair flow: route → CA → EC key/CSR → certificate → mqtt state.
pub async fn pair_thinq2(
    env: &Environment,
    device_id: &str,
    otp: &str,
    public_key_pem: &str,
) -> anyhow::Result<PairResult> {
    let mut nonce = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut nonce);

    // Route (with timeout race similar to TS)
    let route_url = format!("{IOT_BASE_URL}/route");
    let country = env.country_code.clone();
    let servers = tokio::time::timeout(ROUTE_FETCH_TIMEOUT, async {
        api_fetch_json(
            &route_url,
            "GET",
            &[
                ("x-country-code", country.as_str()),
                ("x-service-phase", "OP"),
                ("accept", "application/json"),
            ],
            None,
        )
        .await
    })
    .await
    .map_err(|_| anyhow::anyhow!("route fetch failed on {IOT_BASE_URL}"))?
    .map_err(|e| {
        anyhow::anyhow!(
            "Failed to fetch {IOT_BASE_URL}, make sure that you are not redirecting this address: {e}"
        )
    })?;

    let api_server = servers
        .get("apiServer")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing apiServer"))?
        .to_string();
    let mqtt_server = servers
        .get("mqttServer")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing mqttServer"))?
        .to_string();

    let ca_resp = api_fetch_json(
        &format!("{IOT_BASE_URL}/route/certificate?name=aws-iot"),
        "GET",
        &[("accept", "application/json")],
        None,
    )
    .await?;
    let ca = ca_resp
        .get("certificatePem")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing CA certificatePem"))?
        .to_string();

    let (private_key, public_key, csr) = generate_ec_key_and_csr().await?;

    let mut plain = Vec::new();
    plain.extend_from_slice(&nonce);
    plain.extend_from_slice(otp.as_bytes());
    plain.extend_from_slice(&Sha256::digest(device_id.as_bytes()));
    plain.extend_from_slice(&Sha256::digest(csr.as_bytes()));
    plain.extend_from_slice(&Sha256::digest(public_key.as_bytes()));
    let ciphertext = rsa_pkcs1_encrypt(public_key_pem, &plain)?;

    let body = serde_json::json!({
        "otp": otp,
        "csr": csr,
        "publickey": public_key,
        "ciphertext": base64::engine::general_purpose::STANDARD.encode(&ciphertext),
    });
    let device_config = api_fetch_json(
        &format!("{api_server}/device/{device_id}/certificate"),
        "POST",
        &[
            ("x-provide-type", "immediate"),
            ("Content-type", "application/json"),
        ],
        Some(&body.to_string()),
    )
    .await?;

    let certificate = device_config
        .get("certificatePem")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing device certificatePem"))?
        .to_string();
    let pub_topic = device_config
        .pointer("/publication/message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing publication.message"))?
        .to_string();
    let prov_topic = device_config
        .pointer("/publication/provisioning")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing publication.provisioning"))?
        .to_string();
    let sub_topic = device_config
        .pointer("/subscription/message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing subscription.message"))?
        .to_string();

    let state = Thinq2DeviceState {
        country_code: env.country_code.clone(),
        api_server,
        mqtt_server,
        ca_certificate: ca,
        private_key,
        certificate,
        pub_topic,
        prov_topic,
        sub_topic,
    };

    // Final ciphertext for addDevice: RSA(nonce || sha256(deviceId))
    let mut final_plain = Vec::new();
    final_plain.extend_from_slice(&nonce);
    final_plain.extend_from_slice(&Sha256::digest(device_id.as_bytes()));
    let add_device_ciphertext = rsa_pkcs1_encrypt(public_key_pem, &final_plain)?;

    Ok(PairResult {
        state,
        add_device_ciphertext,
    })
}

/// Build the CLIP device_packet JSON published to LG MQTT (shared with tests).
pub fn format_device_packet(mid: u32, device_id: &str, model_name: &str, data_hex: &str) -> String {
    serde_json::json!({
        "mid": mid,
        "did": device_id,
        "kind": model_name,
        "cmd": "device_packet",
        "rssi": -48,
        "fs": "idle",
        "data": data_hex,
        "type": 1,
    })
    .to_string()
}

/// Build preDeploy payload published on connect (mirrors thinq2connection.ts).
///
/// `live_app_info`/`live_platform_info` are the physical appliance's own deploy
/// message, when the bridge has one on hand (see `LocalDevice::deploy_info` /
/// `deployInfo()` in thinq2connection.ts). Telling the cloud a fixed set of HNA
/// placeholders — `protocolVer: "1"` in particular — regardless of what unit is
/// actually in the room breaks real behavior: a protocolVer 7 appliance ignores the
/// legacy-framed reservation polls the cloud sends a protocolVer-1 device, so the
/// official app's reservation screen reports it unreachable even though basic control
/// keeps working (that framing is version-independent). Falls back to the placeholders
/// only when the appliance hasn't reported its real appInfo/platformInfo yet.
pub fn format_pre_deploy(
    mid: u32,
    device_id: &str,
    model_name: &str,
    country_code: &str,
    live_app_info: Option<&serde_json::Value>,
    live_platform_info: Option<&serde_json::Value>,
) -> String {
    let app_info = live_app_info.cloned().unwrap_or_else(|| {
        serde_json::json!({
            "modelName": model_name,
            "modelLanguage": country_code,
            "softVer": "690409",
            "ruleVer": "2.0.11",
            "countryCode": country_code,
            "subCountryCode": country_code,
            "appVersion": "clip_hna_v1.9.183",
            "modemType": "RTK_RTL8711am",
            "regionalCode": "eic",
            "timezone": "+0100",
            "svcCode": "SVC202",
            "HomeApSsid": "whatever",
            "DeviceType": "",
            "ruleEngine": "y",
            "protocolVer": "1",
            "oneshot": "y",
            "size": 1572864,
            "fwUpgradeInfo": { "upgSched": { "cmd": "none", "upgUtc": "0" } },
        })
    });
    let platform_info = live_platform_info.cloned().unwrap_or_else(|| {
        serde_json::json!({
            "provisioningKey": model_name,
            "version": "clip_v2.00.15.05-RTK_RTL8711am-SDK-8-RELEASE",
        })
    });
    serde_json::json!({
        "mid": mid,
        "did": device_id,
        "kind": model_name,
        "cmd": "preDeploy",
        "rssi": -48,
        "fs": "idle",
        "data": { "appInfo": app_info, "platformInfo": platform_info },
        "type": 0,
    })
    .to_string()
}

/// Parse LG → local packet command from MQTT JSON. Used to decode-log a `packet` cmd
/// as it passes through unchanged (see `is_relayable_cmd` / thinq2_conn.rs) — not to
/// unwrap-and-rebuild it, which is what used to lose the cloud's own `mid`.
pub fn parse_lg_packet_payload(json: &serde_json::Value) -> Option<Vec<u8>> {
    if json.get("cmd").and_then(|v| v.as_str()) != Some("packet") {
        return None;
    }
    let data = json.get("data").and_then(|v| v.as_str())?;
    rusthinq_util::hex::decode(data).ok()
}

/// Cmds that must never cross the bridge, in either direction: the provisioning verbs.
/// `undeploy` could tear down the registration bridging exists to preserve, and the
/// deploy pair (`preDeploy`/`completeProvisioning`/`completeProvisioning_ack`) is
/// already handled explicitly on both sides — relaying it too would fight that
/// handling. Everything else an appliance emits, or the cloud sends it, crosses
/// unchanged: this was an allow list holding only `check_mfota` at one point, and that
/// turned out to be too narrow — pressing "update" in the ThinQ app needs
/// `reqUniversalCtrl`/`respUniversalCtrl` to cross too, and an allow list has to be
/// right about a name before it has ever been observed.
const NEVER_RELAYED_CMDS: &[&str] = &[
    "undeploy",
    "deploy",
    "preDeploy",
    "completeProvisioning",
    "completeProvisioning_ack",
];

pub fn is_relayable_cmd(cmd: &str) -> bool {
    !NEVER_RELAYED_CMDS.contains(&cmd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_packet_json_matches_ts_shape() {
        let s = format_device_packet(10001, "dev-1", "RAC_056905_WW", "AABBCC");
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["cmd"], "device_packet");
        assert_eq!(v["did"], "dev-1");
        assert_eq!(v["data"], "AABBCC");
        assert_eq!(v["type"], 1);
        assert_eq!(v["mid"], 10001);
    }

    #[test]
    fn pre_deploy_falls_back_to_placeholders_without_live_info() {
        let s = format_pre_deploy(10000, "dev-1", "MODEL", "US", None, None);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["cmd"], "preDeploy");
        assert_eq!(v["data"]["appInfo"]["countryCode"], "US");
        assert_eq!(v["data"]["appInfo"]["protocolVer"], "1");
    }

    #[test]
    fn pre_deploy_prefers_the_appliance_own_deploy_info() {
        let live_app = serde_json::json!({"protocolVer": "7", "softVer": "1.2.3"});
        let live_platform = serde_json::json!({"provisioningKey": "REAL_MODEL"});
        let s = format_pre_deploy(
            10000,
            "dev-1",
            "MODEL",
            "US",
            Some(&live_app),
            Some(&live_platform),
        );
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["data"]["appInfo"], live_app);
        assert_eq!(v["data"]["platformInfo"], live_platform);
    }

    #[test]
    fn provisioning_verbs_are_never_relayed_but_everything_else_is() {
        for cmd in [
            "undeploy",
            "deploy",
            "preDeploy",
            "completeProvisioning",
            "completeProvisioning_ack",
        ] {
            assert!(!is_relayable_cmd(cmd), "{cmd} must never cross the bridge");
        }
        for cmd in [
            "packet",
            "ack",
            "reqUniversalCtrl",
            "respUniversalCtrl",
            "check_mfota",
        ] {
            assert!(
                is_relayable_cmd(cmd),
                "{cmd} should cross the bridge unchanged"
            );
        }
    }

    #[test]
    fn parse_lg_packet_hex() {
        let v = serde_json::json!({"cmd":"packet","data":"0102"});
        assert_eq!(parse_lg_packet_payload(&v), Some(vec![1, 2]));
        let v2 = serde_json::json!({"cmd":"other","data":"0102"});
        assert!(parse_lg_packet_payload(&v2).is_none());
    }

    #[tokio::test]
    async fn openssl_keygen_works() {
        let (pk, pubk, csr) = generate_ec_key_and_csr().await.unwrap();
        assert!(pk.contains("PRIVATE KEY") || pk.contains("EC PRIVATE"));
        assert!(pubk.contains("PUBLIC KEY"));
        assert!(csr.contains("CERTIFICATE REQUEST") || csr.contains("BEGIN"));
    }
}
