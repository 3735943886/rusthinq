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
/// A 0.1 `PortSpec`: a port, or `{ bind?, address?, advertise? }`. Returns the 0.2 bind
/// (`None` when 0.1 bound nothing) and the advertise override (a port or a URL).
fn listener(
    value: Option<&toml::Value>,
    default: u16,
) -> io::Result<(Option<String>, Option<Value>)> {
    let (address, port, advertise) = match value {
        None => (None, Some(i64::from(default)), None),
        Some(toml::Value::Integer(n)) => (None, Some(*n), None),
        Some(toml::Value::Table(t)) => {
            if t.keys()
                .any(|k| !["bind", "address", "advertise"].contains(&k.as_str()))
            {
                return Err(invalid("unsupported listener configuration"));
            }
            let advertise = match t.get("advertise") {
                None => None,
                Some(toml::Value::Integer(n)) => Some(json!(port_number(*n)?)),
                Some(toml::Value::String(url)) if !url.is_empty() => Some(json!(url)),
                Some(_) => return Err(invalid("invalid listener advertise")),
            };
            let port = match t.get("bind") {
                None => None,
                Some(value) => Some(
                    value
                        .as_integer()
                        .ok_or_else(|| invalid("invalid listener bind"))?,
                ),
            };
            let address = match t.get("address") {
                None => None,
                Some(value) => Some(
                    value
                        .as_str()
                        .ok_or_else(|| invalid("invalid listener address"))?,
                ),
            };
            (address, port, advertise)
        }
        _ => return Err(invalid("unsupported listener configuration")),
    };
    let bind = port.map(|port| socket(address, port)).transpose()?;
    Ok((bind, advertise))
}
/// A 0.1 `PlainPortSpec`: a port, or `{ bind, address? }`.
fn plain_listener(value: &toml::Value) -> io::Result<String> {
    match value {
        toml::Value::Integer(n) => socket(None, *n),
        toml::Value::Table(t) => {
            if t.keys().any(|k| !["bind", "address"].contains(&k.as_str())) {
                return Err(invalid("unsupported listener configuration"));
            }
            socket(
                match t.get("address") {
                    None => None,
                    Some(value) => Some(
                        value
                            .as_str()
                            .ok_or_else(|| invalid("invalid listener address"))?,
                    ),
                },
                t.get("bind")
                    .and_then(toml::Value::as_integer)
                    .ok_or_else(|| invalid("listener bind missing"))?,
            )
        }
        _ => Err(invalid("unsupported listener configuration")),
    }
}
fn port_number(port: i64) -> io::Result<u16> {
    u16::try_from(port)
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| invalid("invalid port"))
}
fn socket(address: Option<&str>, port: i64) -> io::Result<String> {
    let address = address
        .unwrap_or("0.0.0.0")
        .parse::<std::net::IpAddr>()
        .map_err(|_| invalid("listener address must be an IP"))?;
    Ok(std::net::SocketAddr::new(address, port_number(port)?).to_string())
}
/// 0.1 `parse_mqtt_url`: `mqtt://` plain, `mqtts://`/`ssl://` TLS, default ports.
fn mqtt_url(value: &str) -> io::Result<(String, u16, bool)> {
    let url = url::Url::parse(value).map_err(|_| invalid("invalid [mqtt] mqtt_url"))?;
    let tls = match url.scheme() {
        "mqtt" => false,
        "mqtts" | "ssl" => true,
        _ => return Err(invalid("unsupported [mqtt] mqtt_url scheme")),
    };
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| invalid("[mqtt] mqtt_url requires a host"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    Ok((
        host,
        url.port().unwrap_or(if tls { 8883 } else { 1883 }),
        tls,
    ))
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
    let advertise_requested_host = match legacy.get("advertise_requested_host") {
        None => false,
        Some(value) => value
            .as_bool()
            .ok_or_else(|| invalid("advertise_requested_host must be boolean"))?,
    };
    // 0.1 always served the legacy RTL8711am TLS profile (TLS1.0+, SECLEVEL=0) to devices.
    let mut config = json!({"hostname":legacy.get("hostname").and_then(toml::Value::as_str).unwrap_or("rusthinq.lan"),"ca_key":absolute(base,legacy.get("ca_key_file").and_then(toml::Value::as_str).unwrap_or("ca.key")),"ca_certificate":absolute(base,legacy.get("ca_cert_file").and_then(toml::Value::as_str).unwrap_or("ca.cert")),"device_ledger":"devices.json","legacy_tls":true,"advertise_requested_host":advertise_requested_host,"management":{"bind":"127.0.0.1:44401","gui":true}});
    // Listeners keep 0.1's addresses; a port without `bind` stays unbound. As in 0.1,
    // `advertise` only reaches devices for HTTPS and MQTTS (`/route`).
    for (key, bind, advertise, default) in [
        ("https_port", "https_bind", Some("https_advertise"), 443),
        ("mqtts_port", "mqtt_bind", Some("mqtt_advertise"), 8883),
        ("thinq1_port", "thinq1_bind", None, 47878),
        // rethink serves thinq1_https_port as HTTPS (0.1 served plain HTTP there).
        ("thinq1_https_port", "thinq1_https_bind", None, 46030),
    ] {
        let (address, advertised) = listener(legacy.get(key), default)?;
        if let Some(address) = address {
            config[bind] = json!(address);
        }
        if let (Some(field), Some(advertised)) = (advertise, advertised) {
            config[field] = advertised;
        }
    }
    if let Some(http) = legacy.get("http_port") {
        config["http_bind"] = json!(plain_listener(http)?);
    }
    if let Some(root) = legacy.get("custom_root_cert_file") {
        let root = root
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("invalid custom_root_cert_file"))?;
        config["custom_root_certificate"] = json!(absolute(base, root));
    }
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
    let mut warnings: Vec<String> = [
        "Legacy device registrations are converted under legacy-cloud/ and stay disabled; adopt each one (rusthinqctl adopt) after reconciling account identity and registration.",
        "Rollback: stop 0.2 and restart 0.1 with the untouched original configuration and state.",
    ]
    .map(str::to_owned)
    .to_vec();
    let ledger = match read(&base.join("known_devices.json")) {
        Ok(bytes) => {
            let ledger = legacy_devices(&bytes)?;
            write(&stage.path().join("known-devices.0.1.json"), &bytes)?;
            write(
                &stage.path().join("devices.json"),
                &serde_json::to_vec_pretty(&ledger).map_err(io::Error::other)?,
            )?;
            Some(ledger)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let migrated_devices = ledger
        .as_ref()
        .and_then(|ledger| ledger["entries"].as_array())
        .map_or(0, Vec::len);
    let mut retained_imported = false;
    let retained_topics = match read(&retained_path) {
        Ok(bytes) => {
            retained_imported = true;
            let (inventory, omitted) =
                legacy_retained(&bytes, prefix, ledger.as_ref().unwrap_or(&Value::Null))?;
            if omitted > 0 {
                warnings.push(format!("{omitted} retained 0.1 topics belong to devices absent from known_devices.json and were not imported; remove them at the broker if they are stale."));
            }
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
    let mut archived = Vec::new();
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
        let mut files = std::collections::BTreeMap::new();
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
            if files.len() >= 4096 {
                return Err(invalid("cloud archive exceeded"));
            }
            let bytes = read(&entry.path())?;
            let value: Value =
                serde_json::from_slice(&bytes).map_err(|_| invalid("invalid legacy cloud JSON"))?;
            files.insert(name.to_owned(), (bytes, value));
        }
        // The account comes first: its country completes older ThinQ2 registrations.
        let mut country = None;
        if let Some((_, account)) = files.get("oauth2.json") {
            let refresh = account["refreshToken"]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 16384)
                .ok_or_else(|| invalid("invalid refresh token"))?;
            let code = account["env"]["countryCode"]
                .as_str()
                .filter(|s| s.len() == 2 && s.bytes().all(|b| b.is_ascii_uppercase()))
                .ok_or_else(|| invalid("invalid account country"))?;
            write(
                &stage.path().join("account.json"),
                serde_json::to_string(
                    &json!({"schema":1,"credentials":{"country":code,"refresh":refresh}}),
                )
                .unwrap()
                .as_bytes(),
            )?;
            config["cloud_account"] = json!("account.json");
            country = Some(code.to_owned());
        }
        for (name, (bytes, value)) in files {
            // A device registration is stored as 0.2 pairing material, the form adoption takes.
            let bytes = if name.starts_with("device_") {
                match legacy_material(value, country.as_deref()) {
                    Ok(material) => {
                        serde_json::to_vec_pretty(&material).map_err(io::Error::other)?
                    }
                    Err(error) => {
                        warnings.push(format!("legacy-cloud/{name} was not migrated: {error}."));
                        continue;
                    }
                }
            } else {
                bytes
            };
            write(&archive.join(&name), &bytes)?;
            archived.push(name);
        }
        if let Some(dns) = bridge.get("dns") {
            let servers = dns
                .as_array()
                .and_then(|entries| {
                    entries
                        .iter()
                        .map(|entry| entry.as_str().map(str::to_owned))
                        .collect::<Option<Vec<_>>>()
                })
                .ok_or_else(|| invalid("invalid [bridge] dns"))?;
            // 0.1 accepted a DoH URL or a plain DNS server address, nothing else.
            if !servers.iter().all(|entry| {
                if entry.starts_with("https://") {
                    url::Url::parse(entry).is_ok()
                } else {
                    entry.parse::<std::net::SocketAddr>().is_ok()
                        || entry.parse::<std::net::IpAddr>().is_ok()
                }
            }) {
                return Err(invalid("invalid [bridge] dns server"));
            }
            if !servers.is_empty() {
                config["bridge_dns"] = json!(servers);
            }
        }
    }
    let mqtt_enabled = match legacy.get("mqtt_enabled") {
        None => true,
        Some(value) => value
            .as_bool()
            .ok_or_else(|| invalid("mqtt_enabled must be boolean"))?,
    };
    if let Some(mqtt) = legacy.get("mqtt") {
        let url = mqtt
            .get("mqtt_url")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| invalid("[mqtt] mqtt_url missing"))?;
        let (host, port, tls) = mqtt_url(url)?;
        let inventory = if retained_imported {
            "retained-import.json"
        } else {
            "retained.json"
        };
        let mut external = json!({"host":host,"port":port,"tls":tls,"inventory":inventory});
        for (from, to) in [("mqtt_user", "username"), ("mqtt_pass", "password")] {
            if let Some(value) = mqtt.get(from) {
                let value = value
                    .as_str()
                    .ok_or_else(|| invalid("invalid [mqtt] credentials"))?;
                if !value.is_empty() {
                    external[to] = json!(value);
                }
            }
        }
        if mqtt_enabled {
            config["external_mqtt"] = external;
        } else {
            warnings.push("0.1 mqtt_enabled = false: [external_mqtt] was not written. Add it to connect to the broker.".to_owned());
        }
        // 0.1 raw injection streams: allow switching injection on at runtime (it starts off).
        let raw: Vec<&str> = mqtt
            .get("raw")
            .and_then(toml::Value::as_array)
            .map(|names| names.iter().filter_map(toml::Value::as_str).collect())
            .unwrap_or_default();
        if mqtt.get("raw_prefix").is_some()
            && raw
                .iter()
                .any(|name| ["inject", "inject_clip", "emit"].contains(name))
        {
            config["management"]["raw_inject_toggle"] = json!(true);
        }
    }
    if let Some(gui) = legacy.get("gui") {
        let bind = plain_listener(
            gui.get("gui_port")
                .ok_or_else(|| invalid("[gui] gui_port missing"))?,
        )?;
        let credential = |key: &str| -> io::Result<Option<String>> {
            match gui.get(key) {
                None => Ok(None),
                Some(value) => Ok(value
                    .as_str()
                    .ok_or_else(|| invalid("invalid [gui] credentials"))?
                    .to_owned())
                .map(|value: String| (!value.is_empty()).then_some(value)),
            }
        };
        config["management"]["bind"] = json!(bind);
        // As 0.1, authentication is optional (HTTP Basic only when both are set).
        if let (Some(user), Some(password)) = (credential("gui_user")?, credential("gui_pass")?) {
            config["management"]["user"] = json!(user);
            config["management"]["password"] = json!(password);
        }
    }
    if let Some(prefix) = legacy
        .get("scripting")
        .and_then(|s| s.get("il_prefix"))
        .and_then(toml::Value::as_str)
    {
        warnings.push(format!("0.1 [scripting] il_prefix = \"{prefix}\" is not a host setting in 0.2 and was dropped. The scripts choose their own topics: to keep publishing descriptors under {prefix}/<id>, return \"{prefix}\" from prefix() in rusthinq-scripts il_common.rhai."));
    }
    if legacy
        .get("mqtt")
        .is_some_and(|m| m.get("raw_prefix").is_some() || m.get("raw").is_some())
    {
        warnings.push("0.1 MQTT raw observation streams (raw/rx, raw/tx, clip, lg) are retired; use the management device monitor or rusthinqctl capture. Raw injection, if 0.1 enabled it, moves to the $raw/{inject,emit}/set routes.".to_owned());
    }
    if legacy.get("log").is_some() {
        warnings.push("0.1 log categories are not migrated; 0.2 writes application events to stderr without categories.".to_owned());
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

/// The retained owner of a device incarnation, as `rusthinq_app::lifecycle_cleanup::device_owner`.
fn device_owner(id: &str, incarnation: u64) -> String {
    format!("device/{}:{id}/{incarnation}", id.len())
}

/// Convert both 0.1 retained-state formats into a schema-2 inventory owned by the devices
/// of the migrated `ledger` (`legacy_devices`), so 0.2 maintains them as its own state.
/// Topics of devices absent from the ledger are omitted and counted. No deletion is requested.
pub fn legacy_retained(bytes: &[u8], prefix: &str, ledger: &Value) -> io::Result<(Value, usize)> {
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
    let incarnations: std::collections::BTreeMap<&str, u64> = ledger["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| Some((entry["id"].as_str()?, entry["incarnation"].as_u64()?)))
        .collect();
    let mut topics = std::collections::BTreeMap::new();
    let mut omitted = 0;
    for (device, state) in devices {
        if device.is_empty()
            || device.len() > 256
            || device.contains(['/', '+', '#'])
            || device.chars().any(char::is_control)
        {
            return Err(invalid("invalid legacy retained device id"));
        }
        let owner = incarnations
            .get(device.as_str())
            .map(|incarnation| device_owner(device, *incarnation));
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
            let Some(owner) = &owner else {
                omitted += 1;
                continue;
            };
            topics.insert(topic, owner.clone());
            if topics.len() > 16384 {
                return Err(invalid("legacy retained inventory exceeded"));
            }
        }
    }
    Ok((
        json!({"version":2,"next":0,"pending":topics.into_iter().map(|(topic,owner)|json!({"topic":topic,"owner":owner})).collect::<Vec<_>>(),"deleting":[]}),
        omitted,
    ))
}

/// Convert a 0.1 device registration (`device_<id>.json`) into 0.2 pairing material
/// (`rusthinq_bridge::pairing::Material`) without inventing deploy information. Keys may be
/// camelCase or snake_case, not both; other keys are ignored, as 0.1 read these files (real
/// archives carry fields such as `httpsServer`). An older ThinQ2 registration without a
/// country takes the 0.1 account's. The daemon validates the material when it is adopted.
pub fn legacy_material(value: Value, country: Option<&str>) -> io::Result<Value> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("not a registration object"))?;
    let field = |camel: &str, snake: &str| -> io::Result<String> {
        let snake_value = if camel == snake {
            None
        } else {
            object.get(snake)
        };
        match (object.get(camel), snake_value) {
            (Some(_), Some(_)) => Err(invalid(&format!("both {camel} and {snake} are set"))),
            (Some(v), None) | (None, Some(v)) => v
                .as_str()
                .filter(|s| s.len() <= 65536)
                .map(str::to_owned)
                .ok_or_else(|| invalid(&format!("invalid {camel}"))),
            (None, None) => Err(invalid(&format!("{camel} missing"))),
        }
    };
    let thinq1 = object.contains_key("rtiServer") || object.contains_key("rti_server");
    if object
        .get("platform")
        .is_some_and(|v| v.as_str() != Some(if thinq1 { "thinq1" } else { "thinq2" }))
    {
        return Err(invalid("platform contradicts the registration fields"));
    }
    if thinq1 {
        return Ok(json!({
            "platform": "thinq1",
            "http_server": field("httpServer", "http_server")?,
            "rti_server": field("rtiServer", "rti_server")?,
        }));
    }
    let region = match field("countryCode", "country_code") {
        Ok(region) => region,
        Err(_) if !object.contains_key("countryCode") && !object.contains_key("country_code") => {
            country
                .ok_or_else(|| invalid("countryCode missing and no 0.1 account country"))?
                .to_owned()
        }
        Err(error) => return Err(error),
    };
    Ok(json!({
        "platform": "thinq2",
        "country": region,
        "api_server": field("apiServer", "api_server")?,
        "mqtt_server": field("mqttServer", "mqtt_server")?,
        "ca_certificate": field("caCertificate", "ca_certificate")?,
        "private_key": field("privateKey", "private_key")?,
        "certificate": field("certificate", "certificate")?,
        "pub_topic": field("pubTopic", "pub_topic")?,
        "prov_topic": field("provTopic", "prov_topic")?,
        "sub_topic": field("subTopic", "sub_topic")?,
    }))
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
        if ["modelId", "swVersion"].iter().any(|key| {
            state.get(key).is_some_and(|value| {
                value
                    .as_str()
                    .is_none_or(|v| v.len() > 256 || v.chars().any(char::is_control))
            })
        }) || model.len() > 256
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
            metadata.insert(id.clone(),json!({"incarnation":incarnation,"model_name":model,"device_type":device_type,"thinq2":platform=="thinq2","model_id":state["modelId"].as_str().unwrap_or(model),"sw_version":state["swVersion"].as_str().unwrap_or(""),"last_seen_unix":state["last_seen_unix"].as_u64().unwrap_or(0)}));
        }
    }
    Ok(
        json!({"version":2,"revision":0,"next_incarnation":entries.len()+1,"generation_floor":0,"entries":entries,"metadata":metadata}),
    )
}
