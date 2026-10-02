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
    for key in ["hostname", "ca_key_file", "ca_cert_file"] {
        if legacy
            .get(key)
            .is_some_and(|v| v.as_str().is_none_or(str::is_empty))
        {
            return Err(invalid("invalid legacy configuration string"));
        }
    }
    for section in ["mqtt", "scripting", "bridge", "gui"] {
        if legacy.get(section).is_some_and(|v| !v.is_table()) {
            return Err(invalid("invalid legacy configuration section"));
        }
    }
    for key in ["rusthinq_prefix", "state_file"] {
        if legacy
            .get("mqtt")
            .and_then(|v| v.get(key))
            .is_some_and(|v| v.as_str().is_none_or(str::is_empty))
        {
            return Err(invalid("invalid legacy MQTT migration string"));
        }
    }
    if let Some(script) = legacy.get("scripting")
        && (script.get("watch").is_some_and(|v| v.as_bool().is_none())
            || script
                .get("il_prefix")
                .is_some_and(|v| v.as_str().is_none_or(str::is_empty)))
    {
        return Err(invalid("invalid legacy scripting settings"));
    }
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
    let retained_path = absolute(
        base,
        legacy
            .get("mqtt")
            .and_then(|m| m.get("state_file"))
            .and_then(toml::Value::as_str)
            .unwrap_or("mqtt_state.json"),
    );
    let retained_topics = match read(&retained_path) {
        Ok(bytes) => {
            let inventory = legacy_retained(&bytes, prefix)?;
            write(&stage.path().join("mqtt-state.0.1.json"), &bytes)?;
            write(
                &stage.path().join("retained-import.json"),
                &serde_json::to_vec_pretty(&inventory).map_err(io::Error::other)?,
            )?;
            inventory["pending"].as_array().map_or(0, Vec::len)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    let migrated_devices = match read(&base.join("known_devices.json")) {
        Ok(bytes) => {
            let ledger = legacy_devices(&bytes)?;
            write(&stage.path().join("known-devices.0.1.json"), &bytes)?;
            write(
                &stage.path().join("devices.json"),
                &serde_json::to_vec_pretty(&ledger).map_err(io::Error::other)?,
            )?;
            ledger["entries"].as_array().map_or(0, Vec::len)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
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
    if legacy
        .get("scripting")
        .is_some_and(|s| s.get("il_prefix").is_some())
    {
        warnings.push("0.1 [scripting] il_prefix is not a host setting in 0.2; the scripts choose their own topics (rusthinq-scripts: il_common.rhai prefix()).");
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
    let report = json!({"schema":1,"source":source,"archivedCloudFiles":archived,"retainedTopics":retained_topics,"migratedDevices":migrated_devices,"requiresReview":true,"warnings":warnings});
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

/// Convert both 0.1 retained-state formats into a schema-2 inventory. Legacy
/// ownership never impersonates a new device incarnation. No deletion is requested.
pub fn legacy_retained(bytes: &[u8], prefix: &str) -> io::Result<Value> {
    if bytes.len() > 1_048_576
        || prefix.is_empty()
        || prefix.len() > 256
        || prefix.ends_with('/')
        || prefix.contains(['+', '#'])
        || prefix.chars().any(char::is_control)
    {
        return Err(invalid("invalid retained migration input"));
    }
    let document: Value =
        serde_json::from_slice(bytes).map_err(|_| invalid("invalid legacy retained JSON"))?;
    let devices = document
        .as_object()
        .filter(|d| d.len() <= 4096)
        .ok_or_else(|| invalid("invalid legacy retained device inventory"))?;
    let mut topics = std::collections::BTreeMap::new();
    for (device, state) in devices {
        if device.is_empty()
            || device.len() > 256
            || device.contains(['/', '+', '#'])
            || device.chars().any(char::is_control)
        {
            return Err(invalid("invalid legacy retained device id"));
        }
        let properties = if state.is_array() {
            state.as_array()
        } else {
            let object = state
                .as_object()
                .ok_or_else(|| invalid("invalid legacy retained device state"))?;
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "properties" | "last_seen_unix"))
                || object
                    .get("last_seen_unix")
                    .is_some_and(|v| v.as_i64().is_none())
            {
                return Err(invalid("invalid legacy retained device metadata"));
            }
            state["properties"].as_array()
        }
        .ok_or_else(|| invalid("legacy retained properties missing"))?;
        if properties.len() > 16384 {
            return Err(invalid("legacy retained properties exceeded"));
        }
        for property in properties {
            let property = property
                .as_str()
                .filter(|p| {
                    !p.is_empty() && !p.contains(['+', '#']) && !p.chars().any(char::is_control)
                })
                .ok_or_else(|| invalid("invalid legacy retained property"))?;
            let topic = format!("{prefix}/{device}/{property}");
            if topic.len() > 1024 {
                return Err(invalid("legacy retained topic exceeded"));
            }
            topics.insert(topic, format!("legacy/0.1:{device}"));
            if topics.len() > 16384 {
                return Err(invalid("legacy retained inventory exceeded"));
            }
        }
    }
    Ok(
        json!({"version":2,"next":0,"pending":topics.into_iter().map(|(topic,owner)|json!({"topic":topic,"owner":owner})).collect::<Vec<_>>(),"deleting":[]}),
    )
}

/// Assign fresh, deterministic incarnation identities to archived 0.1 devices.
/// No connection or session generation is fabricated; all start offline.
pub fn legacy_devices(bytes: &[u8]) -> io::Result<Value> {
    if bytes.len() > 1_048_576 {
        return Err(invalid("legacy device ledger exceeded"));
    }
    let document: Value =
        serde_json::from_slice(bytes).map_err(|_| invalid("invalid legacy device ledger"))?;
    let devices = document
        .as_object()
        .filter(|d| d.len() <= 4096)
        .ok_or_else(|| invalid("invalid legacy device inventory"))?;
    let mut entries = Vec::new();
    let mut metadata = serde_json::Map::new();
    for (index, (id, state)) in devices.iter().enumerate() {
        if id.is_empty()
            || id.len() > 256
            || id.contains(['/', '+', '#'])
            || id.chars().any(char::is_control)
            || !state.is_object()
        {
            return Err(invalid("invalid legacy device identity"));
        }
        let incarnation = index as u64 + 1;
        entries.push(json!({"id":id,"incarnation":incarnation,"last_generation":0}));
        let model = state["modelName"]
            .as_str()
            .ok_or_else(|| invalid("legacy model name missing"))?;
        let platform = state["platform"]
            .as_str()
            .ok_or_else(|| invalid("legacy platform missing"))?;
        let device_type = match state.get("deviceType") {
            None | Some(Value::Null) => "",
            Some(v) => v
                .as_str()
                .ok_or_else(|| invalid("invalid legacy device type"))?,
        };
        if model.len() > 256
            || model.chars().any(char::is_control)
            || device_type.len() > 128
            || device_type.chars().any(char::is_control)
            || state
                .get("last_seen_unix")
                .is_some_and(|v| v.as_i64().is_none())
        {
            return Err(invalid("invalid legacy device metadata"));
        }
        if !matches!(platform, "" | "thinq1" | "thinq2") {
            return Err(invalid("unknown legacy device platform"));
        }
        if !model.is_empty() && !platform.is_empty() {
            metadata.insert(id.clone(),json!({"incarnation":incarnation,"model_name":model,"device_type":device_type,"thinq2":platform=="thinq2"}));
        }
    }
    Ok(
        json!({"version":2,"revision":0,"next_incarnation":entries.len()+1,"generation_floor":0,"entries":entries,"metadata":metadata}),
    )
}
