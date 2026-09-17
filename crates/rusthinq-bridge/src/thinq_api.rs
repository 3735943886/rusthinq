//! LG ThinQ cloud client (port of bridge/thinqApi.ts — gateway, auth, homes, devices).

use crate::oauth2;
use crate::state::Environment;
use anyhow::Context;
use rusthinq_util::sync::Mutex;
use serde_json::{Value, json};
use std::collections::HashMap;

const GATEWAY_URL: &str = "https://route.lgthinq.com:46030/v1/service/application/gateway-uri";

static GATEWAY_CACHE: Mutex<Option<HashMap<String, Value>>> = Mutex::new(None);

/// `web_base` is `empFrontBaseUri2` from the LG gateway response (see [`Client::get_urls`]) —
/// external, not a constant — so a malformed value must surface as an error, not panic the
/// whole process mid-login.
pub fn sign_in_url(web_base: &str, country_code: &str) -> anyhow::Result<String> {
    let mut url = url::Url::parse(&format!("{web_base}signin")).with_context(|| {
        format!("gateway returned an unparseable sign-in base url {web_base:?}")
    })?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("callback_url", "https://kr.m.lgaccount.com/login/iabClose");
        q.append_pair("redirect_url", "https://kr.m.lgaccount.com/login/iabClose");
        q.append_pair("client_id", "LGAO221A02");
        q.append_pair("country", country_code);
        q.append_pair("language", "en");
        q.append_pair("svc_integrated", "Y");
        q.append_pair("state", "signin");
        q.append_pair("svc_code", "SVC202");
    }
    Ok(url.to_string())
}

/// What `enable()` should do before adding a device to the home.
///
/// An appliance that is already in this home keeps its registration and its name. Deleting and
/// re-adding it renames it to "Rusthinq xxxxxxxx", announces the removal to every app on the
/// account, and leaves the appliance unable to reach LG on its own. Bridging runs on the
/// certificate/key/topics from `pair()`, not a fresh registration, so none of that is needed.
pub struct RegistrationPlan {
    pub remove_first: bool,
    pub alias: String,
}

/// Decide the plan from the home's current device list (see [`Client::list_devices`]).
pub fn registration_plan(home_devices: &[Value], device_id: &str) -> RegistrationPlan {
    let registered = home_devices
        .iter()
        .find(|d| d.get("deviceId").and_then(|v| v.as_str()) == Some(device_id));
    RegistrationPlan {
        remove_first: registered.is_none(),
        alias: registered
            .and_then(|d| d.get("alias").and_then(|v| v.as_str()))
            .map(str::to_string)
            .unwrap_or_else(|| format!("Rusthinq {}", &device_id[..device_id.len().min(8)])),
    }
}

pub struct Client {
    pub env: Environment,
    pub client_id: String,
    headers: HashMap<String, String>,
    gateway: Option<Value>,
    pub home_id: Option<String>,
}

impl Client {
    pub fn new(env: Environment) -> Self {
        let client_id = rusthinq_util::hex::encode(uuid::Uuid::new_v4().as_bytes())
            + &rusthinq_util::hex::encode(uuid::Uuid::new_v4().as_bytes());
        let mut headers = HashMap::new();
        headers.insert(
            "content-type".into(),
            "application/json;charset=UTF-8".into(),
        );
        headers.insert("accept".into(), "application/json".into());
        headers.insert("x-thinq-app-ver".into(), "4.1.5000".into());
        headers.insert("x-thinq-app-type".into(), "NUTS".into());
        headers.insert("x-thinq-app-level".into(), "PRD".into());
        headers.insert("x-thinq-app-os".into(), "ANDROID".into());
        headers.insert("x-service-code".into(), "SVC202".into());
        headers.insert("x-country-code".into(), env.country_code.clone());
        headers.insert("x-language-code".into(), format!("en-{}", env.country_code));
        headers.insert("x-service-phase".into(), "OP".into());
        headers.insert("x-origin".into(), "app-web-ANDROID".into());
        headers.insert("x-thinq-app-logintype".into(), "LGE".into());
        headers.insert("x-api-key".into(), "VGhpblEyLjAgU0VSVklDRQ==".into());
        headers.insert("x-client-id".into(), client_id.clone());
        Self {
            env,
            client_id,
            headers,
            gateway: None,
            home_id: None,
        }
    }

    /// A one-shot LG API call, not a persistent connection — so a fixed short delay
    /// across a handful of attempts (not `ExponentialBackoff`, which is for something
    /// that keeps retrying indefinitely) is enough to ride out a transient network
    /// blip before giving up and surfacing the error to the caller.
    async fn api_fetch(
        &self,
        url: &str,
        method: &str,
        body: Option<Value>,
    ) -> anyhow::Result<Value> {
        let client = reqwest::Client::new();
        let mut last_err: Option<reqwest::Error> = None;
        for _ in 0..4 {
            let mut req = match method {
                "POST" => client.post(url),
                "DELETE" => client.delete(url),
                _ => client.get(url),
            };
            for (k, v) in &self.headers {
                req = req.header(k, v);
            }
            req = req.header(
                "x-message-id",
                rusthinq_util::hex::encode(uuid::Uuid::new_v4().as_bytes()),
            );
            if let Some(ref b) = body {
                req = req.json(b);
            }
            match req.send().await {
                // A malformed/truncated body is exactly what a transient network blip
                // produces on an otherwise-200 response, so it's retried the same as a
                // send() failure below — not `?`-propagated immediately, which would
                // defeat this loop's whole purpose for that failure class. A non-"0000"
                // `resultCode` is an application-level error, not a transient one, so
                // that still bails out immediately rather than retrying.
                Ok(resp) => match resp.json::<Value>().await {
                    Ok(out) => {
                        let code = out.get("resultCode").and_then(|v| v.as_str()).unwrap_or("");
                        if code != "0000" {
                            anyhow::bail!(
                                "thinq error {code}: {}",
                                out.get("result").cloned().unwrap_or(Value::Null)
                            );
                        }
                        return Ok(out.get("result").cloned().unwrap_or(Value::Null));
                    }
                    Err(e) => {
                        rusthinq_core::logging::log(
                            "bridge",
                            &[&format!("Error parsing response body from {url}: {e}")],
                        );
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                },
                Err(e) => {
                    rusthinq_core::logging::log("bridge", &[&format!("Error fetching {url}: {e}")]);
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        match last_err {
            Some(e) => Err(e.into()),
            None => anyhow::bail!("api_fetch: retries exhausted without a single attempt"),
        }
    }

    pub async fn ensure_gateway(&mut self) -> anyhow::Result<&Value> {
        if self.gateway.is_none() {
            // cache per country
            {
                let cache = GATEWAY_CACHE.lock();
                if let Some(map) = cache.as_ref()
                    && let Some(g) = map.get(&self.env.country_code)
                {
                    self.gateway = Some(g.clone());
                }
            }
            if self.gateway.is_none() {
                let g = self.api_fetch(GATEWAY_URL, "GET", None).await?;
                let mut cache = GATEWAY_CACHE.lock();
                let map = cache.get_or_insert_with(HashMap::new);
                map.insert(self.env.country_code.clone(), g.clone());
                self.gateway = Some(g);
            }
        }
        self.gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("gateway not loaded"))
    }

    pub async fn get_urls(&mut self) -> anyhow::Result<(String, String)> {
        let g = self.ensure_gateway().await?;
        let web = g
            .pointer("/uris/empFrontBaseUri2")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing empFrontBaseUri2"))?
            .to_string();
        let auth = g
            .pointer("/uris/empOauthBaseUri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing empOauthBaseUri"))?
            .to_string();
        Ok((web, auth))
    }

    pub async fn auth(&mut self, refresh_token: &str) -> anyhow::Result<()> {
        let g = self.ensure_gateway().await?.clone();
        let auth_url = g
            .pointer("/uris/empOauthBaseUri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing auth url"))?
            .to_string();
        let thinq2 = g
            .get("thinq2Uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing thinq2Uri"))?
            .to_string();

        let access = oauth2::refresh(&auth_url, refresh_token).await?;
        let profile = oauth2::signed_request(
            &format!("{auth_url}/users/profile"),
            &[
                ("Authorization", &format!("Bearer {access}")),
                ("X-Device-Type", "M01"),
                ("X-Device-Platform", "ADR"),
            ],
            None,
        )
        .await?;
        if profile.get("status").and_then(|v| v.as_i64()) != Some(1) {
            anyhow::bail!("Can't query user information");
        }
        let user_no = profile
            .pointer("/account/userNo")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        self.headers.insert("x-user-no".into(), user_no);
        self.headers.insert("x-emp-token".into(), access);

        // Register client
        let mut h = self.headers.clone();
        h.insert("x-device-type".into(), "601".into());
        // temporary override for this call
        let saved = self.headers.clone();
        self.headers = h;
        let _ = self
            .api_fetch(&format!("{thinq2}/service/users/client"), "POST", None)
            .await;
        self.headers = saved;

        let homes = self
            .api_fetch(&format!("{thinq2}/service/homes"), "GET", None)
            .await?;
        if let Some(items) = homes.get("item").and_then(|v| v.as_array()) {
            for home in items {
                if home.get("currentHomeYn").and_then(|v| v.as_str()) == Some("Y") {
                    self.home_id = home
                        .get("homeId")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
            }
        }
        Ok(())
    }

    /// Devices currently registered in the home (`deviceId`/`alias`/etc per entry).
    pub async fn list_devices(&self) -> anyhow::Result<Vec<Value>> {
        let g = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("gateway not loaded"))?;
        let thinq2 = g
            .get("thinq2Uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing thinq2Uri"))?;
        let home = self
            .home_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("home not set"))?;
        let result = self
            .api_fetch(&format!("{thinq2}/service/homes/{home}"), "GET", None)
            .await?;
        Ok(result
            .get("devices")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default())
    }

    pub async fn remove_device(&self, device_id: &str) -> anyhow::Result<()> {
        let g = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("gateway not loaded"))?;
        let thinq2 = g
            .get("thinq2Uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing thinq2Uri"))?;
        let home = self
            .home_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("home not set"))?;
        // Best-effort remove
        let _ = self
            .api_fetch(
                &format!("{thinq2}/service/homes/{home}/devices/{device_id}"),
                "DELETE",
                None,
            )
            .await;
        Ok(())
    }

    pub async fn prepare_new_t2_device(&self) -> anyhow::Result<(String, String)> {
        let g = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("gateway not loaded"))?;
        let thinq2 = g
            .get("thinq2Uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing thinq2Uri"))?;
        let otp = self
            .api_fetch(
                &format!("{thinq2}/service/devices/otp/certificate"),
                "POST",
                Some(json!({})),
            )
            .await?;
        let otp_str = otp
            .get("otp")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing otp"))?
            .to_string();
        let pubkey = otp
            .get("publicKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing publicKey"))?
            .to_string();
        Ok((otp_str, pubkey))
    }

    pub async fn add_device(
        &self,
        device_id: &str,
        alias: &str,
        model_name: &str,
        device_type: &str,
        platform_type: &str,
        ciphertext_b64: Option<&str>,
    ) -> anyhow::Result<Value> {
        let g = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("gateway not loaded"))?;
        let thinq2 = g
            .get("thinq2Uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing thinq2Uri"))?;
        let home = self
            .home_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("home not set"))?;
        let mut body = json!({
            "deviceId": device_id,
            "countryCode": self.env.country_code,
            "deviceType": device_type,
            "modelName": model_name,
            "aliasPrefix": alias,
            "platformType": platform_type,
            "initDevice": false,
        });
        if let Some(ct) = ciphertext_b64 {
            body["ciphertext"] = json!(ct);
        }
        match self
            .api_fetch(
                &format!("{thinq2}/service/homes/{home}/devices"),
                "POST",
                Some(body),
            )
            .await
        {
            Ok(v) => Ok(v),
            // 0125 = ERROR_ALREADY_DEVICES_REGISTERED_IN_HOME. The appliance is already
            // registered (registration_plan() should have skipped remove_device() for it, so
            // this is the expected path, not a race) — keep the existing registration rather
            // than retrying with initDevice=true, which would tear it down and rebuild it.
            Err(e) if e.to_string().contains("0125") => Ok(Value::Null),
            Err(e) => Err(e),
        }
    }

    pub fn thinq1_state(&self) -> anyhow::Result<Value> {
        let g = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("gateway not loaded"))?;
        let thinq1 = g
            .get("thinq1Uri")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .replace("/api", "");
        let rti = g
            .get("rtiUri")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(json!({ "httpServer": thinq1, "rtiServer": rti }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_in_url_contains_required_params() {
        let u = sign_in_url("https://example.com/", "US").unwrap();
        assert!(u.contains("client_id=LGAO221A02"));
        assert!(u.contains("country=US"));
        assert!(u.contains("svc_code=SVC202"));
        assert!(u.contains("signin"));
    }

    #[test]
    fn registration_plan_removes_first_when_not_yet_registered() {
        let home_devices = vec![json!({"deviceId": "other-device", "alias": "Fridge"})];
        let plan = registration_plan(&home_devices, "my-device");
        assert!(plan.remove_first);
        assert_eq!(plan.alias, "Rusthinq my-devic");
    }

    #[test]
    fn registration_plan_keeps_existing_registration_and_alias() {
        let home_devices = vec![json!({"deviceId": "my-device", "alias": "Kitchen Fridge"})];
        let plan = registration_plan(&home_devices, "my-device");
        assert!(
            !plan.remove_first,
            "must not delete an already-registered device"
        );
        assert_eq!(plan.alias, "Kitchen Fridge");
    }

    #[test]
    fn registration_plan_on_empty_home_removes_first() {
        let plan = registration_plan(&[], "my-device");
        assert!(plan.remove_first);
        assert_eq!(plan.alias, "Rusthinq my-devic");
    }

    /// #27: api_fetch's retry loop only retried `req.send().await` failures. A
    /// malformed/truncated JSON body -- exactly what a real network blip produces on
    /// an otherwise-200 response -- hit `resp.json().await?` and propagated
    /// immediately via `?`, bypassing the retry loop the doc comment says exists for
    /// exactly this kind of transient failure.
    #[tokio::test]
    async fn api_fetch_retries_a_malformed_json_body_and_succeeds_once_it_is_valid() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_srv = attempts.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let n = attempts_srv.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                // Truncated/malformed on the first two attempts, a valid body on the
                // third -- reproducing a transient blip that heals on retry.
                let body = if n < 2 {
                    "{\"resultCode\":\"0000\",\"result\":{"
                } else {
                    r#"{"resultCode":"0000","result":{"ok":true}}"#
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });

        let env = Environment {
            country_code: "KR".to_string(),
            language_code: None,
        };
        let client = Client::new(env);
        let url = format!("http://127.0.0.1:{port}/test");

        let result = client.api_fetch(&url, "GET", None).await;
        assert_eq!(
            result.unwrap(),
            json!({"ok": true}),
            "must succeed once the body is valid JSON instead of giving up on the \
             first malformed one"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "must have retried both malformed-body attempts before the valid one"
        );
    }
}
