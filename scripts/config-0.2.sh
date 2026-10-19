# shellcheck shell=sh
# Configuration generator for the 0.2 runtime, shared by every deployment path:
# thinqctl sources it from a release, rusthinq-docker's entrypoint from the image.
# Published with each release as rusthinq-config-0.2.sh (listed in SHA256SUMS).
#
# Inputs are RUSTHINQ_* variables plus DATA_DIR and SCRIPTS_DIR. The caller
# provides die, bool, require_port, toml_str and toml_list.
#
# generate_config [path prefix] writes the TOML to stdout; every file path gets
# the prefix, so a copy kept elsewhere still points at the files in DATA_DIR.
# initialize_ca creates DATA_DIR/ca.key and ca.cert unless a CA is already there.
generate_config() {
    p="${1:-}"
    for removed in RUSTHINQ_RAW_PREFIX RUSTHINQ_RAW RUSTHINQ_IL_PREFIX RUSTHINQ_LOG; do
        if printenv "$removed" >/dev/null; then
            die "$removed is a retired 0.1 option; remove it and use 0.2 management/driver configuration"
        fi
    done
    printf 'hostname = %s\n' "$(toml_str "${RUSTHINQ_HOSTNAME:-rusthinq.lan}")"
    printf 'ca_key = %s\n' "$(toml_str "${p}ca.key")"
    printf 'ca_certificate = %s\n' "$(toml_str "${p}ca.cert")"
    printf 'device_ledger = %s\n' "$(toml_str "${p}devices-v2.json")"
    echo 'https_bind = "0.0.0.0:443"'
    echo 'mqtt_bind = "0.0.0.0:8883"'
    echo 'thinq1_https_bind = "0.0.0.0:46030"'
    echo 'thinq1_bind = "0.0.0.0:47878"'
    printf 'legacy_tls = %s\n' "$(if bool "${RUSTHINQ_LEGACY_TLS:-true}"; then echo true; else echo false; fi)"
    printf 'advertise_requested_host = %s\n' "$(if bool "${RUSTHINQ_ADVERTISE_REQUESTED_HOST:-true}"; then echo true; else echo false; fi)"
    if bool "${RUSTHINQ_BRIDGE:-true}"; then
        printf 'cloud_account = %s\n' "$(toml_str "${p}cloud-account-v2.json")"
        # Set but empty means the system resolver.
        bridge_dns="${RUSTHINQ_BRIDGE_DNS-https://1.1.1.1/dns-query,https://8.8.8.8/dns-query}"
        printf 'bridge_dns = %s\n' "$(toml_list "$bridge_dns")"
    fi
    if bool "${RUSTHINQ_SCRIPTING:-true}"; then
        echo '[drivers]'
        printf 'directory = %s\n' "$(toml_str "$SCRIPTS_DIR")"
        printf 'topic_prefix = %s\n' "$(toml_str "${RUSTHINQ_PREFIX:-rusthinq}")"
        echo 'watch = true'
    fi
    # Retain the familiar URL option, translating only supported broker URLs.
    mqtt_url="${RUSTHINQ_MQTT_URL-mqtt://localhost:1883}"
    if [ -n "$mqtt_url" ]; then
        case "$mqtt_url" in
            mqtt://*) mqtt_tls=false; endpoint=${mqtt_url#mqtt://}; mqtt_port=1883 ;;
            mqtts://*) mqtt_tls=true; endpoint=${mqtt_url#mqtts://}; mqtt_port=8883 ;;
            *) die "MQTT URL must use mqtt:// or mqtts://" ;;
        esac
        case "$endpoint" in *'/'* | *'?'* | *'#'* | *'@'*) die "MQTT URL must contain only host and port; use separate credentials" ;; esac
        case "$endpoint" in
            '['*']'*)
                mqtt_host=${endpoint#\[}; mqtt_host=${mqtt_host%%\]*}
                suffix=${endpoint#*\]}
                case "$suffix" in '') ;; :*) mqtt_port=${suffix#:} ;; *) die "invalid MQTT IPv6 URL" ;; esac ;;
            *:*) mqtt_host=${endpoint%:*}; mqtt_port=${endpoint##*:}; case "$mqtt_host" in *:*) die "IPv6 MQTT hosts need brackets" ;; esac ;;
            *) mqtt_host=$endpoint ;;
        esac
        [ -n "$mqtt_host" ] || die "MQTT host is empty"
        require_port RUSTHINQ_MQTT_URL "$mqtt_port"
        { [ "$mqtt_port" -gt 0 ] && [ "$mqtt_port" -le 65535 ]; } || die "MQTT port out of range"
        echo '[external_mqtt]'
        printf 'host = %s\nport = %s\ntls = %s\n' "$(toml_str "$mqtt_host")" "$mqtt_port" "$mqtt_tls"
        printf 'inventory = %s\n' "$(toml_str "${p}retained-v2.json")"
        if [ -n "${RUSTHINQ_MQTT_USER:-}" ]; then
            printf 'username = %s\npassword = %s\n' "$(toml_str "$RUSTHINQ_MQTT_USER")" "$(toml_str "${RUSTHINQ_MQTT_PASSWORD:-}")"
        fi
    fi
    if bool "${RUSTHINQ_MANAGEMENT:-true}"; then
        gui_port="${RUSTHINQ_GUI_PORT:-44401}"
        require_port RUSTHINQ_GUI_PORT "$gui_port"
        { [ "$gui_port" -gt 0 ] && [ "$gui_port" -le 65535 ]; } || die "management port out of range"
        echo '[management]'
        printf 'bind = "0.0.0.0:%s"\n' "$gui_port"
        printf 'gui = %s\n' "$(if bool "${RUSTHINQ_GUI:-true}"; then echo true; else echo false; fi)"
        printf 'raw_inject_toggle = %s\n' "$(if bool "${RUSTHINQ_RAW_INJECT_TOGGLE:-false}"; then echo true; else echo false; fi)"
        if [ -n "${RUSTHINQ_GUI_USER:-}" ]; then
            printf 'user = %s\npassword = %s\n' "$(toml_str "$RUSTHINQ_GUI_USER")" "$(toml_str "${RUSTHINQ_GUI_PASSWORD:-}")"
        fi
    fi
}

initialize_ca() {
    key="$DATA_DIR/ca.key"
    cert="$DATA_DIR/ca.cert"
    if [ -e "$key" ] || [ -e "$cert" ]; then
        { [ -f "$key" ] && [ -f "$cert" ]; } || die "incomplete CA: restore both ca.key and ca.cert from backup"
        return
    fi
    # Old state requires explicit migration, never a fresh identity.
    for old_state in state known_devices.json retained.json; do
        [ ! -e "$DATA_DIR/$old_state" ] || die "legacy state detected; migrate it before initializing 0.2"
    done
    ca_tmp=$(mktemp -d "$DATA_DIR/.ca.XXXXXX")
    if ! (umask 077; openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 36500 \
        -subj '/CN=rusthinq CA' -addext 'basicConstraints=critical,CA:TRUE' \
        -addext 'keyUsage=critical,keyCertSign,cRLSign' \
        -addext 'subjectKeyIdentifier=hash' \
        -keyout "$ca_tmp/ca.key" -out "$ca_tmp/ca.cert" >/dev/null 2>&1); then
        rm -rf "$ca_tmp"
        die "could not initialize CA"
    fi
    mv "$ca_tmp/ca.key" "$key"
    mv "$ca_tmp/ca.cert" "$cert"
    rmdir "$ca_tmp"
}
