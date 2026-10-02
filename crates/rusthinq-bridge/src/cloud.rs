//! Bounded LG HTTP/OAuth adapter. Callers own account and registration lifetimes.
//! No mutation retries, detached account tasks, credential logs or redirect forwarding.
use base64::Engine;
use reqwest::{
    Client as Http, Method,
    header::{HeaderMap, HeaderValue},
};
use serde_json::{Value, json};
use std::{
    fmt,
    time::{Duration, SystemTime},
};
use url::Url;

const GATEWAY: &str = "https://route.lgthinq.com:46030/v1/service/application/gateway-uri";
const CALLBACK: &str = "https://kr.m.lgaccount.com/login/iabClose";
const LIMIT: usize = 1_048_576;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidInput,
    Network,
    Http(u16),
    ResponseExceeded,
    InvalidResponse,
    Api(String),
    NotAuthenticated,
    Crypto,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LG API error: {self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone)]
pub struct Token {
    access: String,
    refresh: String,
    pub valid_until_ms: u64,
}
impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Token")
            .field("credentials", &"<redacted>")
            .field("valid_until_ms", &self.valid_until_ms)
            .finish()
    }
}
impl Token {
    pub fn access_token(&self) -> &str {
        &self.access
    }
    pub fn refresh_token(&self) -> &str {
        &self.refresh
    }
}
#[derive(Clone)]
struct Gateway {
    web: Url,
    auth: Url,
    api: Url,
    thinq1: Option<Url>,
    rti: Option<String>,
}
struct Account {
    headers: HeaderMap,
    home: String,
    expires: tokio::time::Instant,
}
pub struct Client {
    http: Http,
    country: String,
    headers: HeaderMap,
    gateway_url: Url,
    gateway: Option<Gateway>,
    account: Option<Account>,
}
fn text(value: &str, max: usize) -> Result<(), Error> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        Err(Error::InvalidInput)
    } else {
        Ok(())
    }
}
pub(crate) fn endpoint(value: &str) -> Result<Url, Error> {
    if value.len() > 4096 {
        return Err(Error::InvalidResponse);
    }
    let url = Url::parse(value).map_err(|_| Error::InvalidResponse)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::InvalidResponse);
    }
    Ok(url)
}
pub(crate) fn append(base: &Url, path: &str) -> Result<Url, Error> {
    Url::parse(&format!(
        "{}/{}",
        base.as_str().trim_end_matches('/'),
        path.trim_start_matches('/')
    ))
    .map_err(|_| Error::InvalidInput)
}
fn header(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<(), Error> {
    let mut value = HeaderValue::from_str(value).map_err(|_| Error::InvalidInput)?;
    if name == "x-emp-token" || name == "authorization" {
        value.set_sensitive(true);
    }
    headers.insert(name, value);
    Ok(())
}
fn nonce() -> Result<String, Error> {
    let mut bytes = [0; 16];
    openssl::rand::rand_bytes(&mut bytes).map_err(|_| Error::Crypto)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn signature(path: &str, body: Option<&str>, date: &str) -> Result<String, Error> {
    let key = openssl::pkey::PKey::hmac(b"c053c2a6ddeb7ad97cb0eed0dcb31cf8")
        .map_err(|_| Error::Crypto)?;
    let mut signer = openssl::sign::Signer::new(openssl::hash::MessageDigest::sha1(), &key)
        .map_err(|_| Error::Crypto)?;
    let signed = match body {
        Some(body) => format!("{path}?{body}\n{date}"),
        None => format!("{path}\n{date}"),
    };
    signer
        .update(signed.as_bytes())
        .map_err(|_| Error::Crypto)?;
    Ok(base64::engine::general_purpose::STANDARD
        .encode(signer.sign_to_vec().map_err(|_| Error::Crypto)?))
}
fn parse_token(raw: &Value, now_ms: u64) -> Result<Token, Error> {
    let access = raw["access_token"].as_str().ok_or(Error::InvalidResponse)?;
    let refresh = raw["refresh_token"]
        .as_str()
        .ok_or(Error::InvalidResponse)?;
    text(access, 8192).map_err(|_| Error::InvalidResponse)?;
    text(refresh, 8192).map_err(|_| Error::InvalidResponse)?;
    let seconds = raw["expires_in"]
        .as_u64()
        .or_else(|| raw["expires_in"].as_str()?.parse().ok())
        .filter(|s| *s > 0)
        .ok_or(Error::InvalidResponse)?;
    let valid_until_ms = seconds
        .checked_mul(1000)
        .and_then(|s| now_ms.checked_add(s))
        .ok_or(Error::InvalidResponse)?;
    Ok(Token {
        access: access.into(),
        refresh: refresh.into(),
        valid_until_ms,
    })
}
impl Client {
    pub fn new(country: &str) -> Result<Self, Error> {
        if country.len() != 2 || !country.bytes().all(|b| b.is_ascii_uppercase()) {
            return Err(Error::InvalidInput);
        }
        let http = Http::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .build()
            .map_err(|_| Error::Network)?;
        let mut headers = HeaderMap::new();
        for (key, value) in [
            ("accept", "application/json"),
            ("x-thinq-app-ver", "4.1.5000"),
            ("x-thinq-app-type", "NUTS"),
            ("x-thinq-app-level", "PRD"),
            ("x-thinq-app-os", "ANDROID"),
            ("x-service-code", "SVC202"),
            ("x-service-phase", "OP"),
            ("x-origin", "app-web-ANDROID"),
            ("x-thinq-app-logintype", "LGE"),
            ("x-api-key", "VGhpblEyLjAgU0VSVklDRQ=="),
        ] {
            header(&mut headers, key, value)?;
        }
        header(&mut headers, "x-country-code", country)?;
        header(&mut headers, "x-language-code", &format!("en-{country}"))?;
        header(
            &mut headers,
            "x-client-id",
            &format!("{}{}", nonce()?, nonce()?),
        )?;
        Ok(Self {
            http,
            country: country.into(),
            headers,
            gateway_url: endpoint(GATEWAY)?,
            gateway: None,
            account: None,
        })
    }
    pub(crate) async fn request(
        &self,
        url: Url,
        method: Method,
        headers: HeaderMap,
        body: Option<String>,
    ) -> Result<Value, Error> {
        if body.as_ref().is_some_and(|body| body.len() > LIMIT) {
            return Err(Error::InvalidInput);
        }
        let mut request = self.http.request(method, url).headers(headers);
        if let Some(body) = body {
            request = request.body(body);
        }
        let mut response = request.send().await.map_err(|_| Error::Network)?;
        if !response.status().is_success() {
            return Err(Error::Http(response.status().as_u16()));
        }
        if response.content_length().is_some_and(|n| n > LIMIT as u64) {
            return Err(Error::ResponseExceeded);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Network)? {
            if chunk.len() > LIMIT - bytes.len() {
                return Err(Error::ResponseExceeded);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| Error::InvalidResponse)
    }
    async fn api(
        &self,
        url: Url,
        method: Method,
        mut headers: HeaderMap,
        body: Option<Value>,
    ) -> Result<Value, Error> {
        header(&mut headers, "x-message-id", &nonce()?)?;
        header(&mut headers, "content-type", "application/json")?;
        let body = body.map(|v| v.to_string());
        let attempts = if method == Method::GET { 3 } else { 1 };
        let mut attempt = 0;
        let raw = loop {
            attempt += 1;
            match self
                .request(url.clone(), method.clone(), headers.clone(), body.clone())
                .await
            {
                Ok(raw) => break raw,
                Err(error)
                    if attempt < attempts
                        && matches!(
                            error,
                            Error::Network | Error::InvalidResponse | Error::Http(502..=504)
                        ) =>
                {
                    tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
                }
                Err(error) => return Err(error),
            }
        };
        let code = raw["resultCode"].as_str().ok_or(Error::InvalidResponse)?;
        if code != "0000" {
            if code.len() > 32 || !code.bytes().all(|b| b.is_ascii_alphanumeric()) {
                return Err(Error::InvalidResponse);
            }
            return Err(Error::Api(code.into()));
        }
        raw.get("result").cloned().ok_or(Error::InvalidResponse)
    }
    async fn gateway(&mut self) -> Result<Gateway, Error> {
        if let Some(gateway) = &self.gateway {
            return Ok(gateway.clone());
        }
        let raw = self
            .api(
                self.gateway_url.clone(),
                Method::GET,
                self.headers.clone(),
                None,
            )
            .await?;
        let gateway = Gateway {
            web: endpoint(
                raw.pointer("/uris/empFrontBaseUri2")
                    .and_then(Value::as_str)
                    .ok_or(Error::InvalidResponse)?,
            )?,
            auth: endpoint(
                raw.pointer("/uris/empOauthBaseUri")
                    .and_then(Value::as_str)
                    .ok_or(Error::InvalidResponse)?,
            )?,
            api: endpoint(raw["thinq2Uri"].as_str().ok_or(Error::InvalidResponse)?)?,
            thinq1: raw
                .get("thinq1Uri")
                .map(|value| endpoint(value.as_str().ok_or(Error::InvalidResponse)?))
                .transpose()?,
            rti: raw
                .get("rtiUri")
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or(Error::InvalidResponse)
                })
                .transpose()?,
        };
        self.gateway = Some(gateway.clone());
        Ok(gateway)
    }
    pub async fn sign_in_url(&mut self) -> Result<String, Error> {
        let mut url = append(&self.gateway().await?.web, "signin")?;
        url.query_pairs_mut().extend_pairs([
            ("callback_url", CALLBACK),
            ("redirect_url", CALLBACK),
            ("client_id", "LGAO221A02"),
            ("country", &self.country),
            ("language", "en"),
            ("svc_integrated", "Y"),
            ("state", "signin"),
            ("svc_code", "SVC202"),
        ]);
        Ok(url.into())
    }
    async fn oauth(
        &self,
        url: Url,
        body: Option<String>,
        extra: &[(&'static str, String)],
    ) -> Result<Value, Error> {
        let date = httpdate::fmt_http_date(SystemTime::now()).replace("GMT", "+0000");
        let mut headers = HeaderMap::new();
        for (key, value) in [
            ("accept", "application/json"),
            ("x-lge-appkey", "LGAO221A02"),
            ("x-lge-app-os", "ANDROID"),
            ("x-application-key", "LGAO221A02"),
            ("lgemp-x-app-key", "LGAO221A02"),
            (
                "content-type",
                "application/x-www-form-urlencoded;charset=UTF-8",
            ),
        ] {
            header(&mut headers, key, value)?;
        }
        header(&mut headers, "x-lge-oauth-date", &date)?;
        header(
            &mut headers,
            "x-lge-oauth-signature",
            &signature(url.path(), body.as_deref(), &date)?,
        )?;
        for (key, value) in extra {
            header(&mut headers, key, value)?;
        }
        self.request(
            url,
            if body.is_some() {
                Method::POST
            } else {
                Method::GET
            },
            headers,
            body,
        )
        .await
    }
    pub async fn exchange_code(&mut self, code: &str, now_ms: u64) -> Result<Token, Error> {
        text(code, 8192)?;
        let auth = self.gateway().await?.auth;
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("code", code),
                ("grant_type", "authorization_code"),
                ("redirect_uri", CALLBACK),
                ("sso_id", &nonce()?),
            ])
            .finish();
        parse_token(
            &self
                .oauth(append(&auth, "oauth/1.0/oauth2/token")?, Some(body), &[])
                .await?,
            now_ms,
        )
    }
    /// Stage a new account and publish it atomically only after all required calls succeed.
    pub async fn authenticate(&mut self, refresh: &str) -> Result<(), Error> {
        text(refresh, 8192)?;
        let gateway = self.gateway().await?;
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([("grant_type", "refresh_token"), ("refresh_token", refresh)])
            .finish();
        let raw = self
            .oauth(
                append(&gateway.auth, "oauth/1.0/oauth2/token")?,
                Some(body),
                &[],
            )
            .await?;
        let access = raw["access_token"].as_str().ok_or(Error::InvalidResponse)?;
        text(access, 8192).map_err(|_| Error::InvalidResponse)?;
        let seconds = match raw.get("expires_in") {
            None => 3600,
            Some(value) => value
                .as_u64()
                .or_else(|| value.as_str()?.parse().ok())
                .filter(|seconds| *seconds > 0)
                .ok_or(Error::InvalidResponse)?,
        };
        let expires = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(seconds))
            .ok_or(Error::InvalidResponse)?;
        let profile = self
            .oauth(
                append(&gateway.auth, "users/profile")?,
                None,
                &[
                    ("authorization", format!("Bearer {access}")),
                    ("x-device-type", "M01".into()),
                    ("x-device-platform", "ADR".into()),
                ],
            )
            .await?;
        if profile["status"].as_i64() != Some(1) {
            return Err(Error::InvalidResponse);
        }
        let user = profile
            .pointer("/account/userNo")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidResponse)?;
        text(user, 256).map_err(|_| Error::InvalidResponse)?;
        let mut headers = self.headers.clone();
        header(&mut headers, "x-user-no", user)?;
        header(&mut headers, "x-emp-token", access)?;
        let mut registration = headers.clone();
        header(&mut registration, "x-device-type", "601")?;
        self.api(
            append(&gateway.api, "service/users/client")?,
            Method::POST,
            registration,
            None,
        )
        .await?;
        let homes = self
            .api(
                append(&gateway.api, "service/homes")?,
                Method::GET,
                headers.clone(),
                None,
            )
            .await?;
        let home = homes["item"]
            .as_array()
            .and_then(|homes| homes.iter().find(|home| home["currentHomeYn"] == "Y"))
            .and_then(|home| home["homeId"].as_str())
            .ok_or(Error::InvalidResponse)?;
        text(home, 256).map_err(|_| Error::InvalidResponse)?;
        if matches!(home, "." | "..") {
            return Err(Error::InvalidResponse);
        }
        if tokio::time::Instant::now() >= expires {
            return Err(Error::InvalidResponse);
        }
        self.account = Some(Account {
            headers,
            home: home.into(),
            expires,
        });
        Ok(())
    }
    pub fn expires_at(&self) -> Option<tokio::time::Instant> {
        self.account.as_ref().map(|account| account.expires)
    }
    pub fn authenticated(&self) -> bool {
        self.expires_at()
            .is_some_and(|expires| tokio::time::Instant::now() < expires)
    }
    pub fn logout(&mut self) {
        self.account = None;
    }
    fn home_url(&self, device: Option<&str>) -> Result<Url, Error> {
        if !self.authenticated() {
            return Err(Error::NotAuthenticated);
        }
        let account = self.account.as_ref().ok_or(Error::NotAuthenticated)?;
        let mut url = append(
            &self.gateway.as_ref().ok_or(Error::NotAuthenticated)?.api,
            "service/homes",
        )?;
        {
            let mut path = url.path_segments_mut().map_err(|_| Error::InvalidInput)?;
            path.push(&account.home);
            if let Some(device) = device {
                text(device, 256)?;
                if matches!(device, "." | "..") {
                    return Err(Error::InvalidInput);
                }
                path.push("devices").push(device);
            }
        }
        Ok(url)
    }
    pub async fn list_devices(&self) -> Result<Vec<Value>, Error> {
        let raw = self
            .api(
                self.home_url(None)?,
                Method::GET,
                self.account
                    .as_ref()
                    .ok_or(Error::NotAuthenticated)?
                    .headers
                    .clone(),
                None,
            )
            .await?;
        let devices = raw["devices"]
            .as_array()
            .filter(|devices| devices.len() <= 4096)
            .ok_or(Error::InvalidResponse)?;
        Ok(devices.clone())
    }
    /// Success requires a confirmed remote acknowledgement; never best-effort.
    pub async fn remove_device(&self, device: &str) -> Result<(), Error> {
        self.api(
            self.home_url(Some(device))?,
            Method::DELETE,
            self.account
                .as_ref()
                .ok_or(Error::NotAuthenticated)?
                .headers
                .clone(),
            None,
        )
        .await
        .map(|_| ())
    }
    pub async fn certificate_otp(&self) -> Result<(String, String), Error> {
        if !self.authenticated() {
            return Err(Error::NotAuthenticated);
        }
        let account = self.account.as_ref().ok_or(Error::NotAuthenticated)?;
        let api = &self.gateway.as_ref().ok_or(Error::NotAuthenticated)?.api;
        let raw = self
            .api(
                append(api, "service/devices/otp/certificate")?,
                Method::POST,
                account.headers.clone(),
                Some(json!({})),
            )
            .await?;
        let otp = raw["otp"].as_str().ok_or(Error::InvalidResponse)?;
        let key = raw["publicKey"].as_str().ok_or(Error::InvalidResponse)?;
        text(otp, 8192).map_err(|_| Error::InvalidResponse)?;
        text(key, 65536).map_err(|_| Error::InvalidResponse)?;
        Ok((otp.into(), key.into()))
    }
    /// Pair and register without deleting an existing appliance or changing its alias.
    pub async fn pair_device(
        &self,
        device: NewDevice<'_>,
    ) -> Result<crate::pairing::Material, Error> {
        let devices = self.list_devices().await?;
        let alias = devices
            .iter()
            .find(|entry| entry["deviceId"] == device.id)
            .and_then(|entry| entry["alias"].as_str())
            .unwrap_or(device.alias);
        let device = NewDevice { alias, ..device };
        if device.platform == "thinq2" {
            let (otp, key) = self.certificate_otp().await?;
            let paired = crate::pairing::pair_at(
                self,
                endpoint("https://common.lgthinq.com")?,
                &self.country,
                device.id,
                &otp,
                &key,
            )
            .await?;
            self.add_device(NewDevice {
                ciphertext: Some(&paired.registration_ciphertext),
                ..device
            })
            .await?;
            return Ok(paired.material);
        }
        if device.platform != "thinq1" {
            return Err(Error::InvalidInput);
        }
        let gateway = self.gateway.as_ref().ok_or(Error::NotAuthenticated)?;
        let mut http = gateway.thinq1.clone().ok_or(Error::InvalidResponse)?;
        if let Some(path) = http.path().strip_suffix("/api") {
            let path = path.to_owned();
            http.set_path(&path);
        }
        let material = crate::pairing::Material::ThinQ1 {
            http_server: http.into(),
            rti_server: gateway.rti.clone().ok_or(Error::InvalidResponse)?,
        };
        material.validate()?;
        self.add_device(NewDevice {
            ciphertext: None,
            ..device
        })
        .await?;
        Ok(material)
    }
    pub async fn add_device(&self, device: NewDevice<'_>) -> Result<Value, Error> {
        for value in [
            device.id,
            device.alias,
            device.model,
            device.device_type,
            device.platform,
        ] {
            text(value, 256)?;
        }
        let account = self.account.as_ref().ok_or(Error::NotAuthenticated)?;
        let mut url = self.home_url(None)?;
        url.path_segments_mut()
            .map_err(|_| Error::InvalidInput)?
            .push("devices");
        let mut body = json!({"deviceId":device.id,"countryCode":self.country,"deviceType":device.device_type,"modelName":device.model,"aliasPrefix":device.alias,"platformType":device.platform,"initDevice":false});
        if let Some(ciphertext) = device.ciphertext {
            text(ciphertext, 65536)?;
            body["ciphertext"] = json!(ciphertext);
        }
        match self
            .api(url, Method::POST, account.headers.clone(), Some(body))
            .await
        {
            Err(Error::Api(code)) if code == "0125" => Ok(Value::Null),
            result => result,
        }
    }
}
pub struct NewDevice<'a> {
    pub id: &'a str,
    pub alias: &'a str,
    pub model: &'a str,
    pub device_type: &'a str,
    pub platform: &'a str,
    pub ciphertext: Option<&'a str>,
}

/// A captured authenticated account for L6's confirmed deregistration contract.
/// Account replacement does not silently redirect an already admitted removal.
pub struct CloudDeregistration(std::sync::Arc<Client>);
impl CloudDeregistration {
    pub fn new(client: std::sync::Arc<Client>) -> Result<Self, Error> {
        if !client.authenticated() {
            return Err(Error::NotAuthenticated);
        }
        Ok(Self(client))
    }
}
impl crate::devices::Deregistration for CloudDeregistration {
    fn deregister(
        &self,
        registration: crate::devices::Registration,
    ) -> crate::devices::DeregisterFuture {
        let client = self.0.clone();
        Box::pin(async move {
            client
                .remove_device(&registration.device)
                .await
                .map_err(std::io::Error::other)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[test]
    fn signing_tokens_and_secret_redaction() {
        assert_eq!(
            signature(
                "/oauth/1.0/oauth2/token",
                Some("grant_type=refresh_token&refresh_token=a%26b"),
                "Mon, 01 Jan 2020 00:00:00 +0000"
            )
            .unwrap(),
            "kRmVgswSIYjg5z5yEDm3Jz+eN5o="
        );
        for expiry in [json!(3600), json!("3600")] {
            let token=parse_token(&json!({"access_token":"private-access","refresh_token":"private-refresh","expires_in":expiry}),1000).unwrap();
            assert_eq!(token.valid_until_ms, 3_601_000);
            assert_eq!(token.access_token(), "private-access");
            assert_eq!(token.refresh_token(), "private-refresh");
            assert!(!format!("{token:?}").contains("private"));
        }
        for raw in [
            json!({}),
            json!({"access_token":"a","refresh_token":"r","expires_in":0}),
            json!({"access_token":"a","refresh_token":"r","expires_in":u64::MAX}),
        ] {
            assert!(parse_token(&raw, 0).is_err());
        }
    }
    #[test]
    fn gateway_inputs_require_https_and_no_embedded_secrets() {
        for url in [
            "http://lg.example/",
            "https://u:p@lg.example/",
            "https://lg.example/?token=a",
            "https://lg.example/#f",
            "file:///tmp/x",
        ] {
            assert!(endpoint(url).is_err());
        }
        assert!(endpoint("https://lg.example:46030/v1").is_ok());
        assert!(Client::new("kr").is_err());
        assert!(Client::new("KRR").is_err());
    }
    async fn mock(replies: Vec<(u16, String)>) -> (Client, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                    assert!(request.len() < 32768);
                }
                let headers = String::from_utf8(request.clone()).unwrap();
                let size = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|n| n.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                let mut data = vec![0; size];
                stream.read_exact(&mut data).await.unwrap();
                request.extend_from_slice(&data);
                requests.push(String::from_utf8(request).unwrap());
                stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        let mut client = Client::new("KR").unwrap();
        // Plaintext is confined to this local fixture. Production Client forbids HTTP.
        client.http = Http::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .unwrap();
        client.gateway = Some(Gateway {
            web: url.clone(),
            auth: url.clone(),
            api: url,
            thinq1: None,
            rti: None,
        });
        (client, task)
    }
    fn ok(value: Value) -> (u16, String) {
        (200, json!({"resultCode":"0000","result":value}).to_string())
    }
    fn auth_replies() -> Vec<(u16, String)> {
        vec![
            (200, json!({"access_token":"private-access"}).to_string()),
            (
                200,
                json!({"status":1,"account":{"userNo":"user"}}).to_string(),
            ),
            ok(Value::Null),
            ok(json!({"item":[{"homeId":"h/a","currentHomeYn":"Y"}]})),
        ]
    }
    #[tokio::test]
    async fn account_registration_listing_and_confirmed_removal() {
        let mut replies = auth_replies();
        replies.push(ok(json!({"devices":[{"deviceId":"d"}]})));
        replies.push((200, json!({"resultCode":"0125","result":null}).to_string()));
        replies.push((
            200,
            json!({"resultCode":"9999","result":"private-secret"}).to_string(),
        ));
        replies.push(ok(Value::Null));
        let (mut client, task) = mock(replies).await;
        assert_eq!(
            client.list_devices().await.unwrap_err(),
            Error::NotAuthenticated
        );
        client.authenticate("refresh&secret").await.unwrap();
        assert_eq!(client.list_devices().await.unwrap()[0]["deviceId"], "d");
        assert_eq!(
            client
                .add_device(NewDevice {
                    id: "d",
                    alias: "Kitchen",
                    model: "model",
                    device_type: "type",
                    platform: "thinq2",
                    ciphertext: None
                })
                .await
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            client.remove_device("d/a").await.unwrap_err(),
            Error::Api("9999".into())
        );
        client.remove_device("d/a").await.unwrap();
        client.logout();
        assert_eq!(
            client.list_devices().await.unwrap_err(),
            Error::NotAuthenticated
        );
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), 8);
        assert!(requests[0].contains("refresh_token=refresh%26secret"));
        assert!(requests[1].contains("authorization: Bearer private-access"));
        assert!(requests[2].contains("x-device-type: 601"));
        assert!(requests[4].starts_with("GET /service/homes/h%2Fa "));
        assert!(requests[5].contains("\"initDevice\":false"));
        assert!(requests[6].starts_with("DELETE /service/homes/h%2Fa/devices/d%2Fa "));
    }
    #[tokio::test]
    async fn refresh_lifetime_stops_expired_requests_before_wire_delivery() {
        let mut replies = auth_replies();
        replies[0] = (
            200,
            json!({"access_token":"private-access","expires_in":"600"}).to_string(),
        );
        let (mut client, task) = mock(replies).await;
        client.authenticate("refresh").await.unwrap();
        assert_eq!(task.await.unwrap().len(), 4);
        assert!(client.authenticated());
        let remaining = client
            .expires_at()
            .unwrap()
            .saturating_duration_since(tokio::time::Instant::now());
        assert!(remaining <= Duration::from_secs(600));
        assert!(remaining > Duration::from_secs(590));
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(600)).await;
        assert!(!client.authenticated());
        assert_eq!(
            client.remove_device("d").await.unwrap_err(),
            Error::NotAuthenticated
        );
        assert_eq!(
            client.certificate_otp().await.unwrap_err(),
            Error::NotAuthenticated
        );
    }
    #[tokio::test]
    async fn invalid_refresh_lifetime_does_not_register_a_client() {
        let (mut client, task) = mock(vec![(
            200,
            json!({"access_token":"private-access","expires_in":"0"}).to_string(),
        )])
        .await;
        assert_eq!(
            client.authenticate("refresh").await.unwrap_err(),
            Error::InvalidResponse
        );
        assert!(!client.authenticated());
        assert_eq!(task.await.unwrap().len(), 1);
    }
    #[tokio::test]
    async fn failed_auth_does_not_replace_existing_account_or_retry_mutations() {
        let mut replies = auth_replies();
        replies.extend(auth_replies().into_iter().take(2));
        replies.push((503, "private-secret".into()));
        let (mut client, task) = mock(replies).await;
        client.authenticate("first").await.unwrap();
        assert_eq!(
            client.authenticate("second").await.unwrap_err(),
            Error::Http(503)
        );
        assert_eq!(client.account.as_ref().unwrap().home, "h/a");
        assert_eq!(task.await.unwrap().len(), 7);
    }
    #[tokio::test]
    async fn body_limits_redirects_and_invalid_json_fail_closed() {
        let (client, task) = mock(vec![
            (200, "x".repeat(LIMIT + 1)),
            (302, "private-secret".into()),
            (200, "not json".into()),
        ])
        .await;
        let url = client.gateway.as_ref().unwrap().api.clone();
        assert_eq!(
            client
                .request(url.clone(), Method::GET, HeaderMap::new(), None)
                .await
                .unwrap_err(),
            Error::ResponseExceeded
        );
        assert_eq!(
            client
                .request(url.clone(), Method::GET, HeaderMap::new(), None)
                .await
                .unwrap_err(),
            Error::Http(302)
        );
        assert_eq!(
            client
                .request(url, Method::GET, HeaderMap::new(), None)
                .await
                .unwrap_err(),
            Error::InvalidResponse
        );
        assert_eq!(task.await.unwrap().len(), 3);
    }
    #[tokio::test]
    async fn deregistration_adapter_requires_authentication_and_remote_confirmation() {
        use crate::devices::{Deregistration, Registration};
        assert!(matches!(
            CloudDeregistration::new(std::sync::Arc::new(Client::new("KR").unwrap())),
            Err(Error::NotAuthenticated)
        ));
        let mut replies = auth_replies();
        replies.push((200, json!({"resultCode":"9999","result":null}).to_string()));
        replies.push(ok(Value::Null));
        let (mut client, task) = mock(replies).await;
        client.authenticate("refresh").await.unwrap();
        let adapter = CloudDeregistration::new(std::sync::Arc::new(client)).unwrap();
        let registration = Registration {
            device: "d".into(),
            incarnation: 1,
            generation: 1,
        };
        assert!(adapter.deregister(registration.clone()).await.is_err());
        adapter.deregister(registration).await.unwrap();
        assert_eq!(task.await.unwrap().len(), 6);
    }
    #[tokio::test]
    async fn gateway_discovery_retries_reads_and_caches_valid_routes() {
        let routes = json!({"uris":{"empFrontBaseUri2":"https://login.example/base/","empOauthBaseUri":"https://auth.example"},"thinq2Uri":"https://api.example"});
        let (mut client, task) = mock(vec![(503, "temporary".into()), ok(routes)]).await;
        client.gateway_url = client.gateway.as_ref().unwrap().api.clone();
        client.gateway = None;
        let first = client.sign_in_url().await.unwrap();
        assert!(first.starts_with("https://login.example/base/signin?"));
        assert_eq!(client.sign_in_url().await.unwrap(), first);
        assert_eq!(task.await.unwrap().len(), 2);
    }
    #[tokio::test]
    async fn authorization_code_exchange_and_login_url() {
        let (mut client, task) = mock(vec![(
            200,
            json!({"access_token":"access","refresh_token":"refresh","expires_in":"60"})
                .to_string(),
        )])
        .await;
        let login = Url::parse(&client.sign_in_url().await.unwrap()).unwrap();
        assert_eq!(login.path(), "/signin");
        assert!(
            login
                .query_pairs()
                .any(|(k, v)| k == "country" && v == "KR")
        );
        assert_eq!(
            client
                .exchange_code("code&value", 1000)
                .await
                .unwrap()
                .valid_until_ms,
            61000
        );
        let requests = task.await.unwrap();
        assert!(requests[0].contains("code=code%26value"));
        assert!(requests[0].contains("x-lge-oauth-signature:"));
    }
}
