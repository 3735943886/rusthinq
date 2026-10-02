//! Offline, staged configuration migration. Never modifies the 0.1 installation.
use serde_json::{Value, json};
use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn read(path: &Path) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1_048_576 {
        return Err(invalid("migration input exceeded"));
    }
    Ok(bytes)
}
fn write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
fn absolute(base: &Path, value: &str) -> PathBuf {
    base.join(value)
}
fn bind(value: Option<&toml::Value>, default: u16) -> io::Result<String> {
    let (address, port) = match value {
        None => ("0.0.0.0".to_owned(), i64::from(default)),
        Some(toml::Value::Integer(n)) => ("0.0.0.0".to_owned(), *n),
        Some(toml::Value::Table(t)) => {
            if t.contains_key("advertise") {
                return Err(invalid(
                    "advertised endpoint overrides require manual migration",
                ));
            }
            let port = t
                .get("bind")
                .and_then(toml::Value::as_integer)
                .ok_or_else(|| invalid("listener bind missing"))?;
            (
                t.get("address")
                    .and_then(toml::Value::as_str)
                    .unwrap_or("0.0.0.0")
                    .to_owned(),
                port,
            )
        }
        _ => return Err(invalid("unsupported listener configuration")),
    };
    let port = u16::try_from(port).map_err(|_| invalid("invalid port"))?;
    if port == 0 {
        return Err(invalid("invalid port"));
    }
    let address = address
        .parse::<std::net::IpAddr>()
        .map_err(|_| invalid("listener address must be an IP"))?;
    Ok(std::net::SocketAddr::new(address, port).to_string())
}
pub fn migrate(source: &Path, destination: &Path) -> io::Result<Value> {
    if destination.exists() {
        return Err(invalid("migration destination already exists"));
    }
    let source = fs::canonicalize(source)?;
    let base = source
        .parent()
        .ok_or_else(|| invalid("configuration parent missing"))?;
    let bytes = read(&source)?;
    let legacy: toml::Value = toml::from_str(
        std::str::from_utf8(&bytes).map_err(|_| invalid("configuration must be UTF-8"))?,
    )
    .map_err(|_| invalid("invalid legacy TOML"))?;
    for key in [
        "custom_root_cert_file",
        "http_port",
        "advertise_requested_host",
    ] {
        if legacy.get(key).is_some() {
            return Err(invalid(
                "proxy/root/advertised host settings require manual migration",
            ));
        }
    }
    if legacy
        .get("thinq1_https_port")
        .is_some_and(|v| v.as_integer() != Some(46030))
    {
        return Err(invalid(
            "custom ThinQ1 HTTPS endpoint requires manual migration",
        ));
    }
    if legacy.get("bridge").and_then(|b| b.get("dns")).is_some() {
        return Err(invalid("custom outbound DNS requires manual migration"));
    }
    let mut config = json!({"hostname":legacy.get("hostname").and_then(toml::Value::as_str).unwrap_or("rusthinq.lan"),"thinq1_bind":bind(legacy.get("thinq1_port"),47878)?,"mqtt_bind":bind(legacy.get("mqtts_port"),8883)?,"https_bind":bind(legacy.get("https_port"),443)?,"ca_key":absolute(base,legacy.get("ca_key_file").and_then(toml::Value::as_str).unwrap_or("ca.key")),"ca_certificate":absolute(base,legacy.get("ca_cert_file").and_then(toml::Value::as_str).unwrap_or("ca.cert")),"device_ledger":"devices.json","management":{"bind":"127.0.0.1:8080","gui":true,"raw_inject":false}});
    let prefix = legacy
        .get("mqtt")
        .and_then(|v| v.get("rusthinq_prefix"))
        .and_then(toml::Value::as_str)
        .unwrap_or("rusthinq");
    if let Some(scripting) = legacy.get("scripting") {
        let directory = scripting
            .get("rhai_dir")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| invalid("script directory missing"))?;
        config["drivers"] = json!({"directory":absolute(base,directory),"topic_prefix":prefix,"watch":scripting.get("watch").and_then(toml::Value::as_bool).unwrap_or(false)});
        if let Some(il) = scripting.get("il_prefix").and_then(toml::Value::as_str) {
            config["drivers"]["il_prefix"] = json!(il);
        }
    }
    // External MQTT and raw ACLs must be reviewed instead of silently reactivating
    // old injection permissions against the new incarnation-scoped command API.
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let stage = tempfile::Builder::new()
        .prefix(".rusthinq-migrate-")
        .tempdir_in(parent)?;
    write(&stage.path().join("config.0.1.toml"), &bytes)?;
    let mut archived = Vec::new();
    let mut warnings = vec![
        "Review external MQTT host, credentials, retained inventory and raw ACLs before enabling the adapter.",
        "Legacy device pairing files remain archived; reconcile account identity and registration before enabling cloud sessions.",
        "Rollback: stop 0.2 and restart 0.1 with the untouched original configuration and state.",
    ];
    if let Some(bridge) = legacy.get("bridge") {
        let storage = absolute(
            base,
            bridge
                .get("storage_path")
                .and_then(toml::Value::as_str)
                .ok_or_else(|| invalid("bridge storage path missing"))?,
        );
        let archive = stage.path().join("legacy-cloud");
        fs::create_dir(&archive)?;
        let mut count = 0;
        for entry in fs::read_dir(&storage)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if !kind.is_file() {
                return Err(invalid("cloud archive contains non-file entries"));
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("invalid archive filename"))?;
            if !name.ends_with(".json") {
                continue;
            }
            count += 1;
            if count > 4096 {
                return Err(invalid("cloud archive exceeded"));
            }
            let bytes = read(&entry.path())?;
            let _: Value =
                serde_json::from_slice(&bytes).map_err(|_| invalid("invalid legacy cloud JSON"))?;
            write(&archive.join(name), &bytes)?;
            archived.push(name.to_owned());
            if name == "oauth2.json" {
                let account: Value = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                let refresh = account["refreshToken"]
                    .as_str()
                    .filter(|s| !s.is_empty() && s.len() <= 16384)
                    .ok_or_else(|| invalid("invalid refresh token"))?;
                let country = account["env"]["countryCode"]
                    .as_str()
                    .filter(|s| s.len() == 2 && s.bytes().all(|b| b.is_ascii_uppercase()))
                    .ok_or_else(|| invalid("invalid account country"))?;
                write(
                    &stage.path().join("account.json"),
                    serde_json::to_string(
                        &json!({"schema":1,"credentials":{"country":country,"refresh":refresh}}),
                    )
                    .unwrap()
                    .as_bytes(),
                )?;
                config["cloud_account"] = json!("account.json");
            }
        }
    }
    if legacy.get("gui").is_some() {
        warnings.push("Management binds to loopback with raw injection disabled; review old GUI bind and authentication explicitly.");
    }
    let config: toml::Value = serde_json::from_value(config).map_err(io::Error::other)?;
    write(
        &stage.path().join("config.toml"),
        toml::to_string_pretty(&config)
            .map_err(io::Error::other)?
            .as_bytes(),
    )?;
    let report = json!({"schema":1,"source":source,"archivedCloudFiles":archived,"requiresReview":true,"warnings":warnings});
    write(
        &stage.path().join("migration.json"),
        serde_json::to_vec_pretty(&report).unwrap().as_slice(),
    )?;
    // Refuse overwrite even after staging. The caller must keep both daemons stopped
    // during cutover; migration itself never starts one or performs network requests.
    if destination.exists() {
        return Err(invalid("migration destination already exists"));
    }
    fs::rename(stage.path(), destination)?;
    fs::File::open(parent)?.sync_all()?;
    Ok(report)
}
