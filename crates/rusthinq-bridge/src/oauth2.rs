//! LG EMP OAuth2 signed requests (port of bridge/oauth2.ts).

use hmac::{Hmac, KeyInit, Mac};
use sha1::Sha1;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha1 = Hmac<Sha1>;

const OAUTH2_SECRET: &[u8] = b"c053c2a6ddeb7ad97cb0eed0dcb31cf8";

/// Build `x-lge-oauth-date` timestamp (UTC HTTP-date with +0000).
fn oauth_date() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Format like: "Fri, 07 Aug 2026 12:00:00 +0000"
    let dt =
        chrono::DateTime::from_timestamp(secs as i64, 0).unwrap_or(chrono::DateTime::UNIX_EPOCH);
    dt.format("%a, %d %b %Y %H:%M:%S +0000").to_string()
}

fn sign(path_and_maybe_body: &str, timestamp: &str) -> String {
    let mut mac = HmacSha1::new_from_slice(OAUTH2_SECRET).expect("HMAC key");
    let signed = format!("{path_and_maybe_body}\n{timestamp}");
    mac.update(signed.as_bytes());
    base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    )
}

/// Signed GET/POST to EMP OAuth endpoints.
pub async fn signed_request(
    url: &str,
    extra_headers: &[(&str, &str)],
    body: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let path = url::Url::parse(url)?.path().to_string();
    let timestamp = oauth_date();
    let mut to_sign = path;
    if let Some(b) = body {
        to_sign.push('?');
        to_sign.push_str(b);
    }
    let signature = sign(&to_sign, &timestamp);

    let client = reqwest::Client::new();
    let mut req = if body.is_some() {
        client.post(url)
    } else {
        client.get(url)
    };
    req = req
        .header("x-lge-oauth-signature", signature)
        .header("x-lge-oauth-date", timestamp)
        .header("Accept", "application/json")
        .header("x-lge-appkey", "LGAO221A02")
        .header("x-lge-app-os", "ANDROID")
        .header("X-Application-Key", "LGAO221A02")
        .header("lgemp-x-app-key", "LGAO221A02");
    for (k, v) in extra_headers {
        req = req.header(*k, *v);
    }
    if let Some(b) = body {
        req = req
            .header(
                "Content-Type",
                "application/x-www-form-urlencoded;charset=UTF-8",
            )
            .body(b.to_string());
    }
    match send_and_parse(req).await {
        Ok(val) => Ok(val),
        Err(e) => {
            rusthinq_core::logging::log("bridge", &[&format!("Failed to fetch {url}: {e}")]);
            Err(e)
        }
    }
}

async fn send_and_parse(req: reqwest::RequestBuilder) -> anyhow::Result<serde_json::Value> {
    let resp = req.send().await?;
    let val = resp.json().await?;
    Ok(val)
}

#[derive(Debug, Clone)]
pub struct Token {
    pub access_token: String,
    pub refresh_token: String,
    pub valid_until_ms: u64,
}

pub async fn from_code(auth_url: &str, code: &str) -> anyhow::Result<Token> {
    let sso_id = rusthinq_util::hex::encode(uuid::Uuid::new_v4().as_bytes());
    let body = format!(
        "code={}&grant_type=authorization_code&redirect_uri={}&sso_id={}",
        urlencoding(code),
        urlencoding("https://kr.m.lgaccount.com/login/iabClose"),
        sso_id
    );
    let raw = signed_request(
        &format!("{auth_url}/oauth/1.0/oauth2/token"),
        &[],
        Some(&body),
    )
    .await?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    parse_token_response(&raw, now_ms)
}

/// A response carrying only one of access_token/refresh_token, an empty token
/// string, or a non-positive expires_in would leave the credential pair stale or
/// immediately-expired — reject the whole response rather than accept a partial one.
fn parse_token_response(raw: &serde_json::Value, now_ms: u64) -> anyhow::Result<Token> {
    let invalid = || anyhow::anyhow!("OAuth2 sign-in failed: invalid response");
    let access = raw
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(invalid)?
        .to_string();
    let refresh = raw
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(invalid)?
        .to_string();
    let expires_in: u64 = raw
        .get("expires_in")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .filter(|&n| n > 0)
        .ok_or_else(invalid)?;
    Ok(Token {
        access_token: access,
        refresh_token: refresh,
        valid_until_ms: now_ms.saturating_add(expires_in.saturating_mul(1000)),
    })
}

pub async fn refresh(auth_url: &str, refresh_token: &str) -> anyhow::Result<String> {
    let body = format!(
        "grant_type=refresh_token&refresh_token={}",
        urlencoding(refresh_token)
    );
    let raw = signed_request(
        &format!("{auth_url}/oauth/1.0/oauth2/token"),
        &[],
        Some(&body),
    )
    .await?;
    parse_refresh_response(&raw)
}

fn parse_refresh_response(raw: &serde_json::Value) -> anyhow::Result<String> {
    raw.get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("OAuth2 refresh failed: invalid response"))
}

fn urlencoding(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_is_stable_for_known_input() {
        // HMAC-SHA1 of "path\ntimestamp" with known secret must be deterministic.
        let s1 = sign("/oauth/1.0/oauth2/token", "Mon, 01 Jan 2020 00:00:00 +0000");
        let s2 = sign("/oauth/1.0/oauth2/token", "Mon, 01 Jan 2020 00:00:00 +0000");
        assert_eq!(s1, s2);
        assert!(!s1.is_empty());
        // base64 alphabet
        assert!(
            s1.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
        );
    }

    #[test]
    fn parse_token_response_accepts_numeric_string_and_number_expiry() {
        for expires_in in [serde_json::json!("3600"), serde_json::json!(3600)] {
            let raw = serde_json::json!({
                "access_token": "access", "refresh_token": "refresh", "expires_in": expires_in,
            });
            let token = parse_token_response(&raw, 1_000).unwrap();
            assert_eq!(token.access_token, "access");
            assert_eq!(token.refresh_token, "refresh");
            assert_eq!(token.valid_until_ms, 1_000 + 3_600_000);
        }
    }

    /// The regression this test exists for: a response carrying only one of
    /// access_token/refresh_token (or an empty/non-positive one) used to be
    /// accepted, silently leaving the other half of the credential pair stale.
    #[test]
    fn parse_token_response_rejects_partial_and_non_positive_responses() {
        let invalid = [
            serde_json::json!({ "access_token": "access", "expires_in": 3600 }),
            serde_json::json!({ "refresh_token": "refresh", "expires_in": 3600 }),
            serde_json::json!({ "access_token": "", "refresh_token": "refresh", "expires_in": 3600 }),
            serde_json::json!({ "access_token": "access", "refresh_token": "", "expires_in": 3600 }),
            serde_json::json!({ "access_token": "access", "refresh_token": "refresh", "expires_in": "not-a-number" }),
            serde_json::json!({ "access_token": "access", "refresh_token": "refresh", "expires_in": 0 }),
            serde_json::json!(null),
        ];
        for raw in invalid {
            assert!(parse_token_response(&raw, 1_000).is_err(), "expected {raw} to be rejected");
        }
    }

    #[test]
    fn parse_refresh_response_requires_non_empty_access_token() {
        assert_eq!(
            parse_refresh_response(&serde_json::json!({ "access_token": "new-access" })).unwrap(),
            "new-access"
        );
        assert!(parse_refresh_response(&serde_json::json!({ "access_token": "" })).is_err());
        assert!(parse_refresh_response(&serde_json::json!({})).is_err());
    }
}
