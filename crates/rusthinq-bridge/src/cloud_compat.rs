//! LG app identity and routing constants, matching anszom/rethink at
//! 0e570698713b038ea0a25da3cc36fd97aea84813 (bridge/thinqApi.ts and bridge/oauth2.ts).
//! https://github.com/anszom/rethink/tree/0e570698713b038ea0a25da3cc36fd97aea84813/bridge
//!
//! The Korean callback is the shared OAuth redirect for every country, not the
//! account's regional API. Keep it identical for sign-in and code exchange.
//! Regional web/auth/API endpoints come from gateway discovery using country headers.
//! These values identify the emulated LG app; they are not user credentials.
pub(crate) const GATEWAY: &str =
    "https://route.lgthinq.com:46030/v1/service/application/gateway-uri";
pub(crate) const CALLBACK: &str = "https://kr.m.lgaccount.com/login/iabClose";
pub(crate) const IOT_BASE_URL: &str = "https://common.lgthinq.com";
pub(crate) const APP_KEY: &str = "LGAO221A02";
pub(crate) const SERVICE_CODE: &str = "SVC202";
pub(crate) const OAUTH_SIGNING_KEY: &[u8] = b"c053c2a6ddeb7ad97cb0eed0dcb31cf8";
pub(crate) const APP_HEADERS: &[(&str, &str)] = &[
    ("accept", "application/json"),
    ("x-thinq-app-ver", "4.1.5000"),
    ("x-thinq-app-type", "NUTS"),
    ("x-thinq-app-level", "PRD"),
    ("x-thinq-app-os", "ANDROID"),
    ("x-service-code", SERVICE_CODE),
    ("x-service-phase", "OP"),
    ("x-origin", "app-web-ANDROID"),
    ("x-thinq-app-logintype", "LGE"),
    ("x-api-key", "VGhpblEyLjAgU0VSVklDRQ=="),
];
