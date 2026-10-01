//! Framing for the Whisen SoftAP setup protocol: HTTP/1.1 POST over TLS on port 9000,
//! one request per connection, no authentication. Bodies are bare JSON members separated
//! by CRLF. `SetDeviceConfig` declares `Content-Length: 0` and an empty `Session-Id:`
//! although it carries a body; the LG app has always sent it that way, so it is
//! reproduced verbatim. Port of upstream rethink's `util/whisen.ts`
//! (anszom/rethink@6f82ef6).

use serde_json::Value;

pub const WHISEN_PORT: u16 = 9000;

/// Headers the app puts on `/SetDeviceConfig`, body notwithstanding.
pub const DEVICE_CONFIG_HEADERS: &[&str] = &["Content-Length: 0", "Session-Id: "];

/// A `POST path` request carrying `body`. `headers` of `None` means just a correct
/// `Content-Length`.
pub fn request(path: &str, body: &str, headers: Option<&[&str]>) -> String {
    let default = [format!("Content-Length: {}", body.len())];
    let headers: Vec<&str> = match headers {
        Some(h) => h.to_vec(),
        None => default.iter().map(String::as_str).collect(),
    };
    let mut lines = vec![format!("POST {path} HTTP/1.1")];
    lines.extend(headers.iter().map(|h| h.to_string()));
    lines.push(String::new());
    lines.push(body.to_string());
    lines.join("\r\n")
}

/// Timezone as the app formats it: sign, two-digit hours, two-digit minutes, e.g. `+0100`.
pub fn timezone(offset_minutes_east_of_utc: i32) -> String {
    let sign = if offset_minutes_east_of_utc < 0 {
        '-'
    } else {
        '+'
    };
    let a = offset_minutes_east_of_utc.unsigned_abs();
    format!("{sign}{:02}{:02}", a / 60, a % 60)
}

/// JSON-encoded members, one per line like the app sends them.
fn members(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(k, v)| format!("{}:{}", Value::from(*k), Value::from(*v)))
        .collect::<Vec<_>>()
        .join(",\r\n")
}

/// Body for `/SetDeviceInfo`. Some firmware ignores `regionalCode` and picks its server
/// from `Nation`.
pub fn device_info_body(nation: &str, regional_code: &str) -> String {
    members(&[("Nation", nation), ("regionalCode", regional_code)]) + "\r\n"
}

/// Body for `/SetDeviceConfig`. `key_type` (`WPA/WPA2`, `WEP`, `OPEN`) is only sent by the
/// app for a hand-typed (hidden) SSID; for a network picked from the scan list it is left
/// out.
pub fn device_config_body(ssid: &str, password: &str, tz: &str, key_type: Option<&str>) -> String {
    let mut fields = vec![("HomeApSSID", ssid), ("HomeApPW", password)];
    if let Some(k) = key_type {
        fields.push(("HomeApKeyType", k));
    }
    fields.push(("TimeZone", tz));
    members(&fields)
}

/// The status code from a reply, or `None` when there is no status line.
pub fn status_code(reply: &str) -> Option<u16> {
    let rest = reply
        .strip_prefix("HTTP/1.1 ")
        .or_else(|| reply.strip_prefix("HTTP/1.0 "))?;
    let code = rest.get(..3)?;
    code.bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| code.parse().ok())
        .flatten()
}

/// What the app keeps of a reply: everything from the first double quote onward, wrapped
/// in braces.
pub fn parse_members(reply: &str) -> Option<serde_json::Map<String, Value>> {
    let i = reply.find('"')?;
    let joined: String = reply
        .get(i..)?
        .split('\n')
        .map(|l| l.strip_suffix('\r').unwrap_or(l))
        .collect();
    let trimmed = joined.trim_end();
    let trimmed = trimmed.strip_suffix(',').unwrap_or(trimmed);
    match serde_json::from_str::<Value>(&format!("{{{trimmed}}}")) {
        Ok(Value::Object(m)) => Some(m),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_sets_content_length_by_default() {
        assert_eq!(
            request("/SetDeviceInit", "", None),
            "POST /SetDeviceInit HTTP/1.1\r\nContent-Length: 0\r\n\r\n"
        );
        assert_eq!(
            request("/X", "abc", None),
            "POST /X HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc"
        );
    }

    #[test]
    fn set_device_config_keeps_the_apps_headers_verbatim() {
        let body = device_config_body("ssid", "pw", "+0100", None);
        assert_eq!(
            request("/SetDeviceConfig", &body, Some(DEVICE_CONFIG_HEADERS)),
            format!(
                "POST /SetDeviceConfig HTTP/1.1\r\nContent-Length: 0\r\nSession-Id: \r\n\r\n{body}"
            )
        );
    }

    #[test]
    fn timezone_is_signed_hhmm() {
        assert_eq!(timezone(60), "+0100");
        assert_eq!(timezone(0), "+0000");
        assert_eq!(timezone(330), "+0530");
        assert_eq!(timezone(-210), "-0330");
        assert_eq!(timezone(-600), "-1000");
    }

    #[test]
    fn bodies_are_json_members_one_per_line() {
        assert_eq!(
            device_info_body("DE", "rusthinq"),
            "\"Nation\":\"DE\",\r\n\"regionalCode\":\"rusthinq\"\r\n"
        );
        assert_eq!(
            device_config_body("My \"Net\"", "p\\w", "+0200", None),
            "\"HomeApSSID\":\"My \\\"Net\\\"\",\r\n\"HomeApPW\":\"p\\\\w\",\r\n\"TimeZone\":\"+0200\""
        );
        assert_eq!(
            device_config_body("s", "p", "+0000", Some("WPA/WPA2")),
            "\"HomeApSSID\":\"s\",\r\n\"HomeApPW\":\"p\",\r\n\"HomeApKeyType\":\"WPA/WPA2\",\r\n\"TimeZone\":\"+0000\""
        );
    }

    #[test]
    fn status_code_reads_the_status_line() {
        assert_eq!(status_code("HTTP/1.1 200 OK\r\n\r\n"), Some(200));
        assert_eq!(status_code("HTTP/1.0 500 Internal\r\n"), Some(500));
        assert_eq!(status_code(""), None);
        assert_eq!(status_code("garbage"), None);
    }

    #[test]
    fn parse_members_wraps_the_reply_members_in_braces() {
        let reply = "HTTP/1.1 200 OK\r\nContent-Length: 40\r\n\r\n\"modelName\":\"RAC\",\r\n\"swVer\":\"2.6.7\",\r\n";
        let m = parse_members(reply).unwrap();
        assert_eq!(m["modelName"], "RAC");
        assert_eq!(m["swVer"], "2.6.7");
        assert!(parse_members("HTTP/1.1 500 Internal\r\n\r\n").is_none());
        assert!(parse_members("\"broken").is_none());
    }
}
