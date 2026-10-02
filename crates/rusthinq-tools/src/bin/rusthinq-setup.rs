//! rusthinq-setup: SoftAP Wi-Fi provisioning without the LG app.

use rusthinq_tools::softap::whisen;

use anyhow::{Context, Result, bail};
use base64::Engine;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
use rusthinq_protocol::json_splitter;
use rusthinq_protocol::mtosp;
use std::env;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_openssl::SslStream;

const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApYRAZXRWijMuWNr9LHOJ
fcPcZHDYcO3CwRF9olsPvtJpkrDXR7jEDA6qPHF1jvJ7ArxDLVj8rbkwXb3oXNmN
Sc+n0DPNDiRgghDaDyJpN0qfzmt06MKdihVScwghyYKWD+oA9d1+j3wy3W32he+X
7FnS+yUmmbQ8cT0PYS7p2E8YtbgHrH+SbUzHAgBbaS8E92l7f0qOpQFmYEyP/OX+
1n0dLdXXJ8kFxCLP2n8Wy6XXTutrT0YuZCxabPVYSKsjLh86MuHEM6V8BdBoZItW
qA1bDeDvjP7QC93lGxmwIYR0H8VVQq7gBZYWpPfsRSfwsE/PCMrF1WS4sPnSauaV
QwIDAQAB
-----END PUBLIC KEY-----
";

fn usage() {
    eprintln!(
        "Usage:
\trusthinq-setup hostname wifi_ssid wifi_password

\thostname is usually 192.168.120.254, or 192.168.1.1 for appliances whose SoftAP
\thands out 192.168.1.x addresses
\tAlways quote the password (special chars like ! $ etc.):
\t  rusthinq-setup 192.168.120.254 'MySSID' 'MyPassword!'

\tOptional env (for modules that accept setApInfo but never join STA):
\t  SETUP_SECURITY=WPA2_PSK|WPA_PSK   (default WPA2_PSK)
\t  SETUP_FORMAT=B64|plain           (default B64)
\t  SETUP_CIPHER=AES                 (default AES)
\t  SETUP_RELEASE_DELAY_MS=3000      (delay before releaseDev)
\t  SETUP_FREQ=2417                  (force AP frequency MHz; else taken from scan)
\t  SETUP_AP_BSSID=aa:bb:cc:dd:ee:ff (home AP MAC if known — not the SoftAP IP)
\t  SETUP_SSID_AS_BSSID=1            (also send B64 SSID in legacy \"bssid\" field; default on)
\t  SETUP_TRY_THINQ1=1               (try ThinQ1 mTosp first)
"
    );
}

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

// SoftAP TLS is device-local bootstrap trust, separate from authenticated cloud TLS.
async fn connect_tls(host: &str, port: u16) -> Result<SslStream<TcpStream>> {
    timeout(Duration::from_secs(10), async {
        let mut builder = SslConnector::builder(SslMethod::tls_client())?;
        builder.set_verify(SslVerifyMode::NONE);
        builder.set_min_proto_version(Some(SslVersion::TLS1_2))?;
        let tcp = TcpStream::connect((host, port)).await?;
        let ssl = builder.build().configure()?.into_ssl(host)?;
        let mut stream = SslStream::new(ssl, tcp)?;
        Pin::new(&mut stream).connect().await?;
        Ok::<_, anyhow::Error>(stream)
    })
    .await
    .context("SoftAP TCP/TLS connection timed out")?
}

async fn thinq1_request(host: &str, xml: &str) -> Result<String> {
    let mut stream = connect_tls(host, 5500).await?;
    let frame = mtosp::format(xml)?;
    timeout(Duration::from_secs(10), stream.write_all(&frame))
        .await
        .context("SoftAP write timed out")??;

    let mut splitter = mtosp::Splitter::new();
    let mut buf = [0u8; 4096];
    let result = timeout(Duration::from_secs(30), async {
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                bail!("connection closed");
            }
            for &byte in &buf[..n] {
                if let Some(xml) = splitter.feed(byte)? {
                    return Ok::<String, anyhow::Error>(xml);
                }
            }
        }
    })
    .await??;
    Ok(result)
}

async fn thinq1_setup(host: &str, ssid: &str, pass: &str) -> Result<()> {
    println!("Connecting to {host}:5500");
    println!("Request: deviceinfo");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let resp = thinq1_request(
        host,
        &format!(
            r#"<mTosp><data type="deviceinfo"><time>{now}</time><reg>000</reg><errorCode>N</errorCode></data></mTosp>"#
        ),
    )
    .await?;
    let _ = resp;
    println!("SoftAP XML response received");

    let b64ssid = b64(ssid);
    let b64password = b64(pass);
    println!("Request: apinfo");
    let resp = thinq1_request(
        host,
        &format!(
            r#"<mTosp><data type="apinfo">
		<format>B64</format>
		<bssid>{b64ssid}</bssid>
		<security>WPA_PSK</security>
		<password>{b64password}</password>
		<subCountryCode>DE</subCountryCode>
		<regionalCode>rusthinq</regionalCode>
	</data></mTosp>"#
        ),
    )
    .await?;
    let _ = resp;
    println!("SoftAP XML response received");
    println!("ThinQ1 setup successful, see rusthinq logs for a follow-up");
    Ok(())
}

async fn thinq2_setup(host: &str, ssid: &str, pass: &str) -> Result<()> {
    let security = env::var("SETUP_SECURITY").unwrap_or_else(|_| "WPA2_PSK".into());
    let cipher = env::var("SETUP_CIPHER").unwrap_or_else(|_| "AES".into());
    let format = env::var("SETUP_FORMAT")
        .unwrap_or_else(|_| "B64".into())
        .to_uppercase();
    // Delay before releaseDev: modules that accept setApInfo but then get releaseDev
    // too soon have been seen to come back up still associated to the SoftAP instead
    // of the target network, presumably needing a moment to act on setApInfo first.
    // Configurable since how long that takes is model-dependent.
    let release_delay_ms: u64 = env::var("SETUP_RELEASE_DELAY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);
    let force_freq = env::var("SETUP_FREQ").ok();
    let ap_bssid = env::var("SETUP_AP_BSSID").ok();
    let ssid_as_bssid = env::var("SETUP_SSID_AS_BSSID")
        .map(|v| v != "0")
        .unwrap_or(true);

    println!(
        "Wi-Fi credentials: ssid_len={} pass_len={} security={security} format={format}",
        ssid.len(),
        pass.len()
    );
    println!("Connecting to {host}:5500");
    let mut stream = connect_tls(host, 5500).await?;
    println!("TLS connection established");

    async fn write_json(stream: &mut SslStream<TcpStream>, obj: &serde_json::Value) -> Result<()> {
        let mut line = serde_json::to_string(obj)?;
        line.push('\n');
        timeout(Duration::from_secs(10), stream.write_all(line.as_bytes()))
            .await
            .context("SoftAP write timed out")??;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_ap_info(
        ssid: &str,
        pass: &str,
        format: &str,
        security: &str,
        cipher: &str,
        force_freq: &Option<String>,
        ap_bssid: &Option<String>,
        ssid_as_bssid: bool,
        extra: serde_json::Map<String, serde_json::Value>,
    ) -> serde_json::Value {
        let use_b64 = format == "B64";
        let ssid_v = if use_b64 { b64(ssid) } else { ssid.to_string() };
        let pass_v = if use_b64 { b64(pass) } else { pass.to_string() };
        let mut data = serde_json::Map::new();
        data.insert(
            "format".into(),
            serde_json::json!(if use_b64 { "B64" } else { "plain" }),
        );
        data.insert("ssid".into(), serde_json::json!(ssid_v.clone()));
        data.insert("password".into(), serde_json::json!(pass_v));
        data.insert("security".into(), serde_json::json!(security));
        data.insert("cipher".into(), serde_json::json!(cipher));
        data.insert("subCountryCode".into(), serde_json::json!("DE"));
        data.insert("regionalCode".into(), serde_json::json!("eic"));
        data.insert("constantConnect".into(), serde_json::json!("Y"));
        data.insert("multiProfile".into(), serde_json::json!("Y"));
        if let Some(b) = ap_bssid {
            data.insert("bssid".into(), serde_json::json!(b));
        } else if ssid_as_bssid {
            data.insert("bssid".into(), serde_json::json!(ssid_v));
        }
        if let Some(f) = force_freq {
            if let Ok(n) = f.parse::<i64>() {
                data.insert("frequency".into(), serde_json::json!(n));
            } else {
                data.insert("frequency".into(), serde_json::json!(f));
            }
        }
        for (k, v) in extra {
            data.insert(k, v);
        }
        serde_json::Value::Object(data)
    }

    write_json(
        &mut stream,
        &serde_json::json!({
            "type": "request",
            "cmd": "setDeviceInit",
            "data": { "set": "true", "constantConnect": "Y" }
        }),
    )
    .await?;

    let mut splitter = json_splitter::Splitter::new();
    let mut buf = [0u8; 8192];
    let mut ap_info_pass = 0u32;
    let mut done = false;

    while !done {
        let n = timeout(Duration::from_secs(60), stream.read(&mut buf))
            .await
            .context("read timeout")??;
        if n == 0 {
            bail!("connection closed");
        }
        for &byte in &buf[..n] {
            if let Some(json) = splitter.feed(byte)? {
                println!("SoftAP response received");
                if json.get("type").and_then(|t| t.as_str()) != Some("response") {
                    continue;
                }
                if let Some(result) = json.pointer("/data/result").and_then(|r| r.as_str())
                    && result != "000"
                {
                    eprintln!("Error code returned!");
                    continue;
                }
                let cmd = json.get("cmd").and_then(|c| c.as_str()).unwrap_or("");
                match cmd {
                    "setDeviceInit" => {
                        write_json(
                            &mut stream,
                            &serde_json::json!({
                                "type": "request",
                                "cmd": "getDeviceInfo",
                                "data": {
                                    "subCountryCode": "DE",
                                    "regionalCode": "eic",
                                    "timezone": "+0100",
                                    "publicKey": PUBLIC_KEY,
                                    "constantConnect": "Y",
                                }
                            }),
                        )
                        .await?;
                    }
                    "getDeviceInfo" => {
                        let info = json.get("data").cloned().unwrap_or(serde_json::json!({}));
                        println!(
                            "Device: model={} type={} protocol={} modem={} multiProfile={} wpa3={}",
                            info.get("modelName")
                                .and_then(|v| v.as_str())
                                .unwrap_or("?"),
                            info.get("deviceType")
                                .and_then(|v| v.as_str())
                                .unwrap_or("?"),
                            info.get("protocolVer")
                                .and_then(|v| v.as_str())
                                .unwrap_or("?"),
                            info.get("demandType")
                                .or_else(|| info.get("modemType"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("?"),
                            info.get("supportsMultiProfile")
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "?".into()),
                            info.get("supportsWpa3")
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "?".into()),
                        );
                        write_json(
                            &mut stream,
                            &serde_json::json!({
                                "type": "request",
                                "cmd": "setCertInfo",
                                "data": {
                                    "otp": "0123456789abcdef0123456789abcdef0123456789abcdef",
                                    "svccode": "SVC202",
                                    "svcphase": "OP",
                                    "constantConnect": "Y",
                                }
                            }),
                        )
                        .await?;
                    }
                    "setCertInfo" => {
                        ap_info_pass = 1;
                        let data = build_ap_info(
                            ssid,
                            pass,
                            &format,
                            &security,
                            &cipher,
                            &force_freq,
                            &ap_bssid,
                            ssid_as_bssid,
                            serde_json::Map::new(),
                        );
                        let keys: Vec<_> = data
                            .as_object()
                            .map(|o| {
                                o.keys()
                                    .filter(|k| k.as_str() != "password")
                                    .cloned()
                                    .collect()
                            })
                            .unwrap_or_default();
                        println!("setApInfo pass {ap_info_pass}: keys={}", keys.join(","));
                        write_json(
                            &mut stream,
                            &serde_json::json!({
                                "type": "request",
                                "cmd": "setApInfo",
                                "data": data,
                            }),
                        )
                        .await?;
                    }
                    "setApInfo" => {
                        let hn = json.pointer("/data/homeNetwork");
                        if ap_info_pass == 1 && hn.is_some() && force_freq.is_none() {
                            ap_info_pass = 2;
                            let mut extra = serde_json::Map::new();
                            if let Some(freq) = hn.and_then(|h| h.get("freq")) {
                                extra.insert("frequency".into(), freq.clone());
                            }
                            if let Some(sec) = hn.and_then(|h| h.get("security")) {
                                extra.insert("security".into(), sec.clone());
                            }
                            if let Some(enc) = hn.and_then(|h| h.get("encryption")) {
                                extra.insert("cipher".into(), enc.clone());
                            }
                            println!(
                                "setApInfo pass 2 (scan refine): freq={:?} security={:?} encryption={:?} oui={:?}",
                                hn.and_then(|h| h.get("freq")),
                                hn.and_then(|h| h.get("security")),
                                hn.and_then(|h| h.get("encryption")),
                                hn.and_then(|h| h.get("oui")),
                            );
                            let data = build_ap_info(
                                ssid,
                                pass,
                                &format,
                                &security,
                                &cipher,
                                &force_freq,
                                &ap_bssid,
                                ssid_as_bssid,
                                extra,
                            );
                            write_json(
                                &mut stream,
                                &serde_json::json!({
                                    "type": "request",
                                    "cmd": "setApInfo",
                                    "data": data,
                                }),
                            )
                            .await?;
                        } else {
                            println!(
                                "setApInfo ok (pass {ap_info_pass}); waiting {release_delay_ms}ms before releaseDev"
                            );
                            tokio::time::sleep(Duration::from_millis(release_delay_ms)).await;
                            write_json(
                                &mut stream,
                                &serde_json::json!({
                                    "type": "request",
                                    "cmd": "releaseDev",
                                    "data": { "constantConnect": "Y" }
                                }),
                            )
                            .await?;
                        }
                    }
                    "releaseDev" => {
                        println!("Setup completed, the device will now connect to your Wi-Fi");
                        // Purely a UX pause so the line above is readable before the
                        // process prints its last line and exits - nothing here is
                        // waiting on the device itself.
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                        println!("ThinQ2 setup successful, see rusthinq logs for a follow-up");
                        done = true;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        usage();
        std::process::exit(1);
    }
    let host = &args[1];
    let ssid = &args[2];
    let pass = &args[3];
    if host.len() > 256
        || ssid.is_empty()
        || ssid.len() > 32
        || pass.len() > 64
        || host.chars().any(char::is_control)
        || ssid.chars().any(char::is_control)
    {
        bail!("invalid SoftAP host or Wi-Fi input");
    }

    // Whisen appliances only listen on port 9000 and ThinQ1/ThinQ2 ones only on 5500, so
    // both are tried at once; the first to succeed wins.
    let whisen = whisen_setup(host, ssid, pass);
    let thinq = thinq_setup(host, ssid, pass);
    tokio::pin!(whisen, thinq);
    let (mut whisen_err, mut thinq_err) = (None, None);
    while whisen_err.is_none() || thinq_err.is_none() {
        tokio::select! {
            r = &mut whisen, if whisen_err.is_none() => match r {
                Ok(()) => break,
                Err(e) => {
                    println!("Whisen setup failed {e:#}");
                    whisen_err = Some(e);
                }
            },
            r = &mut thinq, if thinq_err.is_none() => match r {
                Ok(()) => break,
                Err(e) => {
                    println!("ThinQ setup failed {e:#}");
                    thinq_err = Some(e);
                }
            },
        }
    }
    if let (Some(_), Some(e)) = (whisen_err, thinq_err) {
        return Err(e.context("Setup failed"));
    }
    print_footer();
    Ok(())
}

async fn thinq_setup(host: &str, ssid: &str, pass: &str) -> Result<()> {
    if env::var("SETUP_TRY_THINQ1").as_deref() == Ok("1") {
        match thinq1_setup(host, ssid, pass).await {
            Ok(()) => return Ok(()),
            Err(e) => println!("ThinQ 1 setup failed {e}"),
        }
    }
    println!("Trying ThinQ 2 setup");
    thinq2_setup(host, ssid, pass).await
}

/// One Whisen request: its own TLS connection, the reply read until the appliance closes it.
async fn whisen_request(
    host: &str,
    path: &str,
    body: &str,
    headers: Option<&[&str]>,
) -> Result<String> {
    let mut stream = connect_tls(host, whisen::WHISEN_PORT).await?;
    println!("Request: POST {path}");
    timeout(
        Duration::from_secs(10),
        stream.write_all(whisen::request(path, body, headers).as_bytes()),
    )
    .await
    .context("SoftAP write timed out")??;
    let mut reply = Vec::new();
    timeout(Duration::from_secs(15), async {
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => return Ok(()),
                Ok(n) => {
                    if reply.len() + n > 1_048_576 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "SoftAP response exceeded",
                        ));
                    }
                    reply.extend_from_slice(&buf[..n]);
                }
                // The module may drop the connection without a TLS close_notify; what
                // arrived before that is the reply.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    })
    .await
    .context("timeout")??;
    let reply = String::from_utf8_lossy(&reply).into_owned();
    println!("SoftAP HTTP response received");
    Ok(reply)
}

async fn whisen_setup(host: &str, ssid: &str, pass: &str) -> Result<()> {
    println!("Connecting to {host}:{}", whisen::WHISEN_PORT);
    whisen_request(host, "/SetDeviceInit", "", None).await?;

    // Answers 500 on some firmware.
    if let Some(info) =
        whisen::parse_members(&whisen_request(host, "/GetDeviceInfo", "", None).await?)
    {
        println!("device info: {}", serde_json::Value::Object(info));
    }

    // Nation is the country code, sent as subCountryCode by the other setups. Some firmware
    // ignores regionalCode and picks its server from Nation: DE makes it connect to
    // eic.lgthinq.com. Must precede SetDeviceConfig: ReleaseDevAp at the end takes the
    // SoftAP down.
    let info_reply = whisen_request(
        host,
        "/SetDeviceInfo",
        &whisen::device_info_body("DE", "rusthinq"),
        None,
    )
    .await?;
    if whisen::status_code(&info_reply) != Some(200) {
        bail!("SetDeviceInfo rejected");
    }

    let tz = whisen::timezone(chrono::Local::now().offset().local_minus_utc() / 60);
    let cfg_reply = whisen_request(
        host,
        "/SetDeviceConfig",
        &whisen::device_config_body(ssid, pass, &tz, None),
        Some(whisen::DEVICE_CONFIG_HEADERS),
    )
    .await?;
    if whisen::status_code(&cfg_reply) != Some(200) {
        bail!("SetDeviceConfig rejected, appliance left in AP mode");
    }

    whisen_request(host, "/ReleaseDevAp", "", None).await?;
    println!("Whisen setup successful, see rusthinq logs for a follow-up");
    Ok(())
}

fn print_footer() {
    println!(
        "

    Author's request: 

    Once you finish setting up rusthinq (or encounter a problem), please let
    me know about your experiences by filling out this form:
    		https://forms.gle/B4vUGGZHa8HsfsQW6 
    Thanks!
"
    );
}
