#!/usr/bin/env bash
# Run with: bash scripts/tests/thinqctl.sh (no root, network, or systemd needed).
# shellcheck source-path=SCRIPTDIR/..
# The generated fake binaries test their own $1.
# Each test subshell intentionally has independent fake system state.
# shellcheck disable=SC2016,SC2030,SC2031
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
# As root, thinqctl runs a binary only from a path no other user can write to, and
# the adopted fake below must be one. /tmp is not, so pick a parent that is.
test_parent="${TMPDIR:-/tmp}"
if [ "$(id -u)" -eq 0 ]; then
    for test_parent in "$test_parent" "$HOME" /var/lib; do
        # shellcheck source=/dev/null
        (source "$script_dir/thinqctl.sh" && root_trusted_path "$test_parent") && break
        test_parent=
    done
    [ -n "$test_parent" ] || { echo "No root-owned, write-protected parent for test files; set TMPDIR." >&2; exit 1; }
fi
test_dir=$(mktemp -d -p "$test_parent")
trap 'rm -rf -- "$test_dir"' EXIT
export THINQCTL_SCRIPT="$script_dir/thinqctl.sh" TEST_DIR="$test_dir"

# A release v0.2.1 whose binaries are scripts; `rusthinq --version` reports it.
make_release() {
    local version="${1#v}" name="rusthinq-$1-linux-amd64" bin
    mkdir -p "$test_dir/build/$name" "$test_dir/assets"
    for bin in rusthinq rusthinqctl rusthinq-setup; do
        printf '#!/bin/sh\n[ "$1" = --version ] && echo "rusthinq %s"\n' "$version" > "$test_dir/build/$name/$bin"
        chmod +x "$test_dir/build/$name/$bin"
    done
    tar -czf "$test_dir/assets/$name.tar.gz" -C "$test_dir/build" "$name"
    cp "$script_dir/config-0.2.sh" "$test_dir/assets/rusthinq-config-0.2.sh"
    ( cd "$test_dir/assets" && sha256sum "$name.tar.gz" rusthinq-config-0.2.sh > SHA256SUMS )
}
make_release v0.2.1
mkdir -p "$test_dir/mock-bin"
printf '#!/bin/sh\nprintf "%%s\n" "${FAKE_SS:-}" | sed "/^$/d"\n' > "$test_dir/mock-bin/ss"
chmod +x "$test_dir/mock-bin/ss"
export PATH="$test_dir/mock-bin:$PATH"

# Each invocation has fresh globals and a fake system. Only system-facing
# operations are replaced; detection, questions, version comparison, config
# generation, download ordering and the install/upgrade flows remain real.
cat > "$test_dir/run" <<'MOCK'
set -euo pipefail
source "$THINQCTL_SCRIPT"
R="$TEST_DIR/root"
PREFIX="$R/opt/rusthinq" BIN_DIR="$R/opt/rusthinq/bin" LINK_DIR="$R/usr/local/bin"
DATA_DIR="$R/var/lib/rusthinq" CONFIG_FILE="$R/var/lib/rusthinq/config.toml"
SCRIPTS_DIR="$R/var/lib/rusthinq/scripts" UNIT_PATH="$R/etc/systemd/system/rusthinq.service"
STATE_FILE="$R/etc/thinqctl.conf" BACKUP_DIR="$R/var/lib/thinqctl/previous"
SELF_PATH="$R/usr/local/bin/thinqctl"
mkdir -p "$R/usr/local/bin" "$R/etc/systemd/system" "$R/var/lib"
event() { echo "$*" >> "$TEST_DIR/events"; }
require_root() { :; }
require_systemd() { :; }
require_tools() { :; }
detect_arch() { echo amd64; }
api_latest_tag() { event "latest $PRERELEASE"; echo "${LATEST:-v0.2.1}"; }
curl() {
    local out="" url=""
    while [ $# -gt 0 ]; do
        case "$1" in -o) out="$2"; shift ;; -*) ;; *) url="$1" ;; esac
        shift
    done
    case "$url" in
        "$SELF_URL") cp "$THINQCTL_SCRIPT" "$out"; return 0 ;;
        */releases/download/*) event "download ${url##*/}"; [[ "${FAIL_DOWNLOAD:-0}" != 1 ]] || return 22 ;;
    esac
    [ -f "$TEST_DIR/assets/${url##*/}" ] || return 22
    cp "$TEST_DIR/assets/${url##*/}" "$out"
}
systemctl() {
    event "systemctl $*"
    case "$1" in
        is-active) return "${SERVICE_INACTIVE:-0}" ;;
        show) return 1 ;;
    esac
}
proc_pids() {
    case "$1" in
        rusthinq) [ -z "${FAKE_PID:-}" ] || echo "$FAKE_PID" ;;
        rusthinq-cloud) [ -z "${FAKE_LPID:-}" ] || echo "$FAKE_LPID" ;;
    esac
}
proc_exe() { if [ "$1" = "${FAKE_LPID:-}" ]; then echo "$FAKE_LEXE"; else echo "$FAKE_EXE"; fi; }
proc_args() { if [ "$1" = "${FAKE_LPID:-}" ]; then printf '%s\n' "$FAKE_LEXE" ${FAKE_LCONFIG:+"$FAKE_LCONFIG"}; else printf '%s\n%s\n' "$FAKE_EXE" "$FAKE_CONFIG"; fi; }
proc_cwd() { echo "${FAKE_CWD:-/}"; }
proc_user() { echo "${FAKE_USER:-root}"; }
proc_cgroup() { if [ "$1" = "${FAKE_LPID:-}" ]; then echo "${FAKE_LCGROUP:-0::/user.slice}"; else echo "$FAKE_CGROUP"; fi; }
id() { if [ "${1:-}" = -u ] && [ -n "${2:-}" ]; then return 1; fi; command id "$@"; }
useradd() { event "useradd $*"; }
userdel() { event "userdel $*"; }
chown() { :; }
runuser() { shift 3; "$@"; }
install() {
    local args=()
    while [ $# -gt 0 ]; do
        case "$1" in -o|-g) shift 2; continue ;; esac
        args+=("$1"); shift
    done
    command install "${args[@]}"
}
git() { event "git $*"; [ "$1" != clone ] || mkdir -p "${*: -1}"; }
wait_running() { event "wait $1"; [[ "${FAIL_START:-0}" != 1 ]]; }
stop_pid() { event "stop_pid $1"; }
sleep() { :; }
main "$@"
MOCK

run() {
    : > "$test_dir/events"
    bash "$test_dir/run" "$@" > "$test_dir/output" 2>&1
}
fails() {
    if run "$@"; then echo "Unexpected success: $*" >&2; cat "$test_dir/output" >&2; exit 1; fi
}
assert_output() { grep -Fq -- "$1" "$test_dir/output" || { echo "missing output: $1" >&2; cat "$test_dir/output" >&2; exit 1; }; }
assert_event() { grep -Fxq -- "$1" "$test_dir/events" || { echo "missing event: $1" >&2; cat "$test_dir/events" >&2; exit 1; }; }
assert_no_event() { ! grep -Eq -- "$1" "$test_dir/events" || { echo "unexpected event: $1" >&2; cat "$test_dir/events" >&2; exit 1; }; }
assert_file_has() { grep -Fq -- "$2" "$1" || { echo "$1 lacks: $2" >&2; cat "$1" >&2; exit 1; }; }
reset_root() { rm -rf "$test_dir/root"; }
root="$test_dir/root"

# ── Pure helpers ───────────────────────────────────────────────────────────────
(
    # shellcheck source=thinqctl.sh
    source "$THINQCTL_SCRIPT"
    [ "$(printf '0::/system.slice/thinq.service\n' | classify_cgroup)" = unit:thinq.service ]
    [ "$(printf '0::/system.slice/docker-0123abcd.scope\n' | classify_cgroup)" = container ]
    [ "$(printf '12:pids:/docker/0123\n1:name=systemd:/docker/0123\n' | classify_cgroup)" = container ]
    [ "$(printf '0::/user.slice/user-1000.slice/session-3.scope\n' | classify_cgroup)" = manual ]
    [ "$(printf '0::/user.slice/user-1000.slice/user@1000.service/app.slice/x.service\n' | classify_cgroup)" = manual ]
    [ "$(version_direction 0.2.0-dev6 0.2.0-dev10)" = older ]
    [ "$(version_direction 0.2.0-dev6 0.2.0)" = older ]
    [ "$(version_direction 0.2.0-dev6 0.2.0-rc.1)" = older ]
    [ "$(version_direction 0.2.0 0.2.0-rc.1)" = newer ]
    [ "$(version_direction 0.2.1 0.2.1)" = equal ]
    [ "$(toml_str 'a"b\c')" = '"a\"b\\c"' ]
    if ( toml_str $'a\tb' ) 2>/dev/null; then exit 1; fi
    [ "$(toml_list ' a, b ,,c')" = '["a", "b", "c"]' ]
    [ "$(resolve_path /srv/x ./config.toml)" = /srv/x/config.toml ]
    [ "$(resolve_path /srv/x /etc/c.toml)" = /etc/c.toml ]
    printf '[management]\nbind = "0.0.0.0:44401"\n[drivers]\ndirectory = "../scripts" # local\nwatch = true\n' > "$TEST_DIR/c.toml"
    [ "$(config_drivers_dir "$TEST_DIR/c.toml")" = "$TEST_DIR/../scripts" ]
    [ "$(config_management_port "$TEST_DIR/c.toml")" = 44401 ]
)

# ── Security boundaries (simulate root without requiring privileges) ───────────
# Mocks below are called indirectly by sourced functions.
# shellcheck disable=SC2329,SC2031
(
    # shellcheck source=thinqctl.sh
    source "$THINQCTL_SCRIPT"
    probe="$TEST_DIR/version-probe"
    printf '#!/bin/sh\ntouch "$TEST_DIR/executed"\necho "rusthinq 0.2.1"\n' > "$probe"
    chmod 755 "$probe"
    ln -s "$probe" "$TEST_DIR/probe-link"
    id() { echo 0; }
    # Model secure ancestors, then vary the executable and its parent.
    stat() {
        local path="${*: -1}"
        if [ "$1" = -Lc ] && [ "$2" = '%u %a' ]; then
            if [ "$path" = "$probe" ]; then
                echo "${PROBE_UID:-0} ${PROBE_MODE:-755}"
            elif [ "$path" = "$TEST_DIR" ]; then
                echo "0 ${PARENT_MODE:-755}"
            else
                echo '0 755'
            fi
        else
            echo "${REPO_OWNER:-root}"
        fi
    }
    [ "$(binary_version "$TEST_DIR/probe-link")" = 0.2.1 ]
    [ -f "$TEST_DIR/executed" ]
    rm "$TEST_DIR/executed"
    [ -z "$(PROBE_UID=1001 binary_version "$probe")" ]
    [ -z "$(PROBE_MODE=775 binary_version "$probe")" ]
    [ -z "$(PARENT_MODE=777 binary_version "$probe")" ]
    [ ! -e "$TEST_DIR/executed" ]
    git() { printf 'git %s\n' "$*" >> "$TEST_DIR/security-events"; }
    runuser() { printf 'runuser %s\n' "$*" >> "$TEST_DIR/security-events"; }
    : > "$TEST_DIR/security-events"
    if REPO_OWNER=attacker scripts_git /repo log 2>/dev/null; then exit 1; fi
    [ ! -s "$TEST_DIR/security-events" ]
    REPO_OWNER=root scripts_git /repo log
    REPO_OWNER="$SVC_USER" scripts_git /repo log
    I_USER=pi REPO_OWNER=pi scripts_git /repo log
    grep -Fxq 'runuser -u pi -- git -C /repo log' "$TEST_DIR/security-events"
    grep -Fxq 'git -C /repo log' "$TEST_DIR/security-events"
    grep -Fxq "runuser -u $SVC_USER -- git -C /repo log" "$TEST_DIR/security-events"
)

# ── Fresh install with defaults ────────────────────────────────────────────────
reset_root
run install --prerelease --yes
assert_event 'download rusthinq-v0.2.1-linux-amd64.tar.gz'
assert_event 'download rusthinq-config-0.2.sh'
assert_event 'git clone --quiet --depth 1 https://github.com/3735943886/rusthinq-scripts.git '"$root/var/lib/rusthinq/scripts"
assert_event 'systemctl enable --now rusthinq'
assert_output 'Installation complete'
config="$root/var/lib/rusthinq/config.toml"
assert_file_has "$config" 'hostname = "rusthinq.lan"'
assert_file_has "$config" 'mqtt_bind = "0.0.0.0:8883"'
assert_file_has "$config" "directory = \"$root/var/lib/rusthinq/scripts\""
assert_file_has "$config" 'host = "localhost"'
assert_file_has "$config" 'bind = "0.0.0.0:44401"'
assert_file_has "$config" 'bridge_dns = ["https://1.1.1.1/dns-query", "https://8.8.8.8/dns-query"]'
[ -s "$root/var/lib/rusthinq/ca.key" ] && [ -s "$root/var/lib/rusthinq/ca.cert" ]
[ "$("$root/opt/rusthinq/bin/rusthinq" --version)" = "rusthinq 0.2.1" ]
[ "$(readlink "$root/usr/local/bin/rusthinqctl")" = "$root/opt/rusthinq/bin/rusthinqctl" ]
[ ! -e "$root/usr/local/bin/packet-parser" ]
unit="$root/etc/systemd/system/rusthinq.service"
assert_file_has "$unit" "ExecStart=\"$root/opt/rusthinq/bin/rusthinq\" \"$config\""
assert_file_has "$unit" 'AmbientCapabilities=CAP_NET_BIND_SERVICE'
assert_file_has "$unit" "ReadWritePaths=$root/var/lib/rusthinq"
assert_file_has "$root/etc/thinqctl.conf" 'OWNED=1'
assert_file_has "$root/etc/thinqctl.conf" 'VERSION=0.2.1'
assert_file_has "$root/etc/thinqctl.conf" 'SCRIPTS=git'

# A second install points at upgrade; the managed install is recognized from state.
fails install --yes
assert_output 'Already installed by thinqctl'
run upgrade --yes
assert_output 'Already at requested version (0.2.1).'
assert_no_event '^download'

# remove keeps data; install afterwards reuses the configuration.
run remove --yes
[ ! -e "$root/opt/rusthinq" ] && [ ! -e "$unit" ] && [ -f "$config" ] && [ ! -e "$root/etc/thinqctl.conf" ]
before="$(cat "$config")"
run install --yes
assert_output "Keeping existing $config"
assert_no_event 'download rusthinq-config-0.2.sh'
[ "$(cat "$config")" = "$before" ]

# purge deletes everything thinqctl created.
run purge --yes
[ ! -e "$root/var/lib/rusthinq" ] && [ ! -e "$root/opt/rusthinq" ] && [ ! -e "$unit" ]
fails purge --yes
assert_output 'Nothing to purge.'

# ── Fresh install answers from RUSTHINQ_* ──────────────────────────────────────
reset_root
RUSTHINQ_HOSTNAME=thinq.home RUSTHINQ_MQTT_URL=- RUSTHINQ_BRIDGE=false run install --yes
config="$root/var/lib/rusthinq/config.toml"
assert_file_has "$config" 'hostname = "thinq.home"'
if grep -q 'external_mqtt\|cloud_account' "$config"; then exit 1; fi
reset_root
RUSTHINQ_MQTT_URL=http://broker fails install --yes
assert_output 'MQTT URL must use mqtt:// or mqtts://'
assert_no_event '^systemctl (enable|start|stop|daemon-reload)'
[ ! -e "$root/var/lib/rusthinq" ]

# ── Adopting a systemd installation where it is ────────────────────────────────
reset_root
adopted="$test_dir/adopted"
rm -rf "$adopted"; mkdir -p "$adopted/bin"
printf '#!/bin/sh\n[ "$1" = --version ] && echo "rusthinq 0.2.0"\n' > "$adopted/bin/rusthinq"
cp "$adopted/bin/rusthinq" "$adopted/bin/rusthinqctl"
# A tool of releases up to 0.2.0-rc5, now a rusthinqctl subcommand.
cp "$adopted/bin/rusthinq" "$adopted/bin/rusthinq-mcp"
chmod +x "$adopted/bin/"*
export FAKE_PID=4242 FAKE_EXE="$adopted/bin/rusthinq" FAKE_CONFIG=config.toml FAKE_CWD="$adopted"
export FAKE_CGROUP=0::/system.slice/thinq.service
run install --yes
assert_output 'adopted, systemd unit'
assert_output "Config:  $adopted/config.toml"
assert_file_has "$root/etc/thinqctl.conf" 'OWNED=0'
assert_file_has "$root/etc/thinqctl.conf" 'UNIT=thinq.service'
assert_file_has "$root/etc/thinqctl.conf" 'VERSION=0.2.0'
[ ! -e "$root/opt/rusthinq" ] && [ ! -e "$root/etc/systemd/system/rusthinq.service" ]
assert_no_event '^download'

run status
assert_output '0.2.0'
assert_output 'upgrade available'

# Upgrade replaces only what the directory has, downloading before the stop.
run upgrade --yes
expected="latest 0
download rusthinq-v0.2.1-linux-amd64.tar.gz
download SHA256SUMS
systemctl is-active --quiet thinq.service
systemctl stop thinq.service
systemctl start thinq.service
wait thinq.service"
[ "$(cat "$test_dir/events")" = "$expected" ] || { cat "$test_dir/events" >&2; exit 1; }
[ "$("$adopted/bin/rusthinq" --version)" = "rusthinq 0.2.1" ]
[ "$("$adopted/bin/rusthinqctl" --version)" = "rusthinq 0.2.1" ]
[ ! -e "$adopted/bin/packet-parser" ] && [ ! -e "$adopted/bin/rusthinq-mcp" ]
[ ! -e "$root/opt/rusthinq" ]
assert_file_has "$root/etc/thinqctl.conf" 'VERSION=0.2.1'
[ "$(cat "$root/var/lib/thinqctl/previous/.version")" = 0.2.0 ]

run rollback --yes
assert_event 'systemctl stop thinq.service'
assert_event 'systemctl start thinq.service'
[ "$("$adopted/bin/rusthinq" --version)" = "rusthinq 0.2.0" ]
[ "$("$adopted/bin/rusthinq-mcp" --version)" = "rusthinq 0.2.0" ]
assert_file_has "$root/etc/thinqctl.conf" 'VERSION=0.2.0'

# A failed download leaves the service alone.
FAIL_DOWNLOAD=1 fails upgrade --yes
assert_no_event 'systemctl stop'

# Someone else's installation is never purged; remove only forgets it.
fails purge --yes
assert_output 'does not delete it'
run remove --yes
[ ! -e "$root/etc/thinqctl.conf" ] && [ -x "$adopted/bin/rusthinq" ]
assert_no_event '^systemctl'

# ── Adopting a hand-started installation ───────────────────────────────────────
reset_root
export FAKE_CGROUP=0::/user.slice/user-0.slice/session-1.scope
run upgrade --yes
assert_output 'adopted, started outside systemd'
assert_output 'PID 4242 still runs the previous version'
assert_no_event '^systemctl'
assert_no_event '^stop_pid'
[ "$("$adopted/bin/rusthinq" --version)" = "rusthinq 0.2.1" ]

# ── Containers are left to their image ─────────────────────────────────────────
reset_root
export FAKE_CGROUP=0::/system.slice/docker-0123.scope
fails install --yes
assert_output 'runs in a container'
fails upgrade --yes
[ ! -e "$root/etc/thinqctl.conf" ]
unset FAKE_PID FAKE_EXE FAKE_CONFIG FAKE_CWD FAKE_CGROUP

# ── A running 0.1 ──────────────────────────────────────────────────────────────
reset_root
export FAKE_LPID=77 FAKE_LEXE=/srv/0.1/rusthinq-cloud FAKE_CWD=/srv/0.1
run status
assert_output 'rusthinq 0.1 only'
assert_output 'rusthinq-cloud PID 77, /srv/0.1/config.toml'
fails upgrade --yes
assert_output 'Only rusthinq 0.1 is running'
fails install --yes
assert_output 'Cannot read the 0.1 configuration /srv/0.1/config.toml'
unset FAKE_LPID FAKE_LEXE FAKE_CWD

# ── Listener conflicts and address overrides ──────────────────────────────────
reset_root
FAKE_SS='LISTEN 0 128 0.0.0.0:443 0.0.0.0:*' fails install --yes
assert_output 'IN USE'
assert_no_event '^systemctl (enable|start|stop|daemon-reload)'
[ ! -e "$root/var/lib/rusthinq/config.toml" ]
reset_root
RUSTHINQ_HTTPS_BIND=127.0.0.1:8443 RUSTHINQ_MQTT_BIND=127.0.0.1:18883 \
    RUSTHINQ_MANAGEMENT_BIND='[::1]:44402' run install --yes
assert_file_has "$root/var/lib/rusthinq/config.toml" 'https_bind = "127.0.0.1:8443"'
assert_file_has "$root/var/lib/rusthinq/config.toml" 'mqtt_bind = "127.0.0.1:18883"'
assert_file_has "$root/var/lib/rusthinq/config.toml" 'bind = "[::1]:44402"'
reset_root
RUSTHINQ_HTTPS_BIND=0.0.0.0:8883 fails install --yes
assert_output 'duplicates 0.0.0.0:8883'
reset_root
RUSTHINQ_HTTPS_BIND=0.0.0.0:65536 fails install --yes
assert_output 'port must be'
(
    # shellcheck source=thinqctl.sh
    source "$THINQCTL_SCRIPT"
    export FAKE_SS='LISTEN 0 128 [::]:443 [::]:*'
    if listener_check check 127.0.0.1:443 >/dev/null; then exit 1; fi
    export FAKE_SS='LISTEN 0 128 127.0.0.1:443 0.0.0.0:*'
    listener_check check 127.0.0.2:443 >/dev/null
    export FAKE_SS='LISTEN 0 128 [::ffff:127.0.0.1]:443 [::]:*'
    if listener_check check 127.0.0.1:443 >/dev/null; then exit 1; fi
    if listener_check validate '::1:443' 2>/dev/null; then exit 1; fi
    CONFIG_FILE="$TEST_DIR/listeners.toml"
    printf 'https_bind = "127.0.0.1:8443"\n[management]\nbind = "127.0.0.1:44402"\n' > "$CONFIG_FILE"
    check_existing_listeners >/dev/null
)

# Simulate editing a conflicting default and retrying without touching the host.
(
    # shellcheck source=thinqctl.sh
    source "$THINQCTL_SCRIPT"
    ASSUME_YES=0 A_GUI_PORT=44401
    export FAKE_SS='LISTEN 0 128 0.0.0.0:443 0.0.0.0:*'
    # shellcheck disable=SC2329
    have_tty() { return 0; }
    # shellcheck disable=SC2329
    ask() {
        local answer="$3"
        case "$2" in Resolve:*) answer=edit ;; HTTPS_BIND) answer=0.0.0.0:8443 ;; esac
        printf -v "$1" '%s' "$answer"
    }
    configure_listeners > "$TEST_DIR/output"
    [ "${LISTENER_VALUES[0]}" = 0.0.0.0:8443 ]
    assert_output 'IN USE'
    assert_output '0.0.0.0:8443 — available'
)

# ── Self checks and updates never touch a service ──────────────────────────────
reset_root
run self check
assert_output 'up to date'
mkdir -p "$root/usr/local/bin"
printf '#!/usr/bin/env bash\n# old copy\n' > "$root/usr/local/bin/thinqctl"
run self check
assert_output 'differs from master'
run self update --yes
assert_output 'thinqctl updated'
cmp "$THINQCTL_SCRIPT" "$root/usr/local/bin/thinqctl"
assert_no_event '^systemctl'
(
    # shellcheck source=thinqctl.sh
    source "$THINQCTL_SCRIPT"
    curl() { printf '<html>bad download</html>' > "${*: -1}"; }
    if self_download "$TEST_DIR/bad-self"; then exit 1; fi
)

# ── Driver checks query upstream without fetching; update preserves dirty files ─
(
    # shellcheck source=thinqctl.sh
    source "$THINQCTL_SCRIPT"
    repo="$TEST_DIR/driver-upstream"
    checkout="$TEST_DIR/driver-checkout"
    git init -q --initial-branch=master "$repo"
    printf 'first\n' > "$repo/driver.rhai"
    git -C "$repo" add .
    git -C "$repo" -c user.name=Test -c user.email=test@example.invalid commit -qm first
    git clone -q "$repo" "$checkout"
    CONFIG_FILE="$TEST_DIR/drivers.toml"
    printf '[drivers]\ndirectory = "%s"\n' "$checkout" > "$CONFIG_FILE"
    # These are deliberately invoked indirectly by cmd_scripts.
    # shellcheck disable=SC2329
    detect_installation() { I_KIND=systemd I_CONFIG="$CONFIG_FILE" I_USER="$(id -un)"; }
    cmd_scripts check > "$TEST_DIR/output"
    assert_output 'HEAD matches'
    printf 'second\n' >> "$repo/driver.rhai"
    git -C "$repo" -c user.name=Test -c user.email=test@example.invalid commit -qam second
    before="$(git -C "$checkout" rev-parse origin/master)"
    cmd_scripts check > "$TEST_DIR/output"
    assert_output 'HEAD differs'
    [ "$(git -C "$checkout" rev-parse origin/master)" = "$before" ]
    # Test harness may run unprivileged; the update logic itself is unchanged.
    # shellcheck disable=SC2329
    require_root() { :; }
    cmd_scripts update > "$TEST_DIR/output"
    [ "$(git -C "$checkout" rev-parse HEAD)" = "$(git -C "$repo" rev-parse HEAD)" ]
    printf 'local edit\n' >> "$checkout/driver.rhai"
    if (cmd_scripts update) > "$TEST_DIR/output" 2>&1; then exit 1; fi
    assert_output 'local changes'
    grep -q 'local edit' "$checkout/driver.rhai"
)

# ── Arguments ──────────────────────────────────────────────────────────────────
fails --version 0.2.1 status
assert_output 'only supported for install and upgrade'
fails install --version 0.2.1 --prerelease
assert_output 'cannot be combined'
fails install --version banana
assert_output 'Invalid release version'

echo "thinqctl tests passed"
