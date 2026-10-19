#!/usr/bin/env bash
# thinqctl.sh — install, adopt, upgrade and remove rusthinq as a systemd service.
#
# Usage:
#   thinqctl                              # show status (default)
#   sudo thinqctl install [--yes]         # adopt a running rusthinq, migrate a running
#                                         # 0.1, or install afresh (asks questions)
#   sudo thinqctl upgrade [--yes] [--version 0.2.0]
#   sudo thinqctl rollback [--yes]        # restore the binaries the last upgrade replaced
#   sudo thinqctl scripts update          # update the driver checkout
#   sudo thinqctl remove  [--yes]
#   sudo thinqctl purge   [--yes]
#   thinqctl help
#
# One-line install (Linux + systemd, x86_64/aarch64/armv7):
#   curl -fsSL https://raw.githubusercontent.com/3735943886/rusthinq/master/scripts/thinqctl.sh \
#     | sudo bash -s -- install
#
# Questions are read from the terminal even when the script itself arrives on stdin.
# --yes answers every question with its default; RUSTHINQ_* variables set those
# defaults (see config-0.2.sh), e.g. sudo RUSTHINQ_HOSTNAME=thinq.lan thinqctl install -y.

# shellcheck source-path=SCRIPTDIR
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────────
GH_OWNER="3735943886"
GH_REPO="rusthinq"
SCRIPTS_REPO="rusthinq-scripts"
SERVICE="rusthinq"
SVC_USER="rusthinq"

PREFIX="/opt/rusthinq"
BIN_DIR="${PREFIX}/bin"
LINK_DIR="/usr/local/bin"
DATA_DIR="/var/lib/rusthinq"
CONFIG_FILE="${DATA_DIR}/config.toml"
SCRIPTS_DIR="${DATA_DIR}/scripts"
UNIT_PATH="/etc/systemd/system/${SERVICE}.service"
# What thinqctl manages and which parts of it thinqctl created.
STATE_FILE="/etc/thinqctl.conf"
BACKUP_DIR="/var/lib/thinqctl/previous"

SELF_PATH="${LINK_DIR}/thinqctl"
SELF_URL="https://raw.githubusercontent.com/${GH_OWNER}/${GH_REPO}/master/scripts/thinqctl.sh"
GENERATOR_ASSET="rusthinq-config-0.2.sh"
GENERATOR_URL="https://raw.githubusercontent.com/${GH_OWNER}/${GH_REPO}/master/scripts/config-0.2.sh"
SCRIPTS_GIT="https://github.com/${GH_OWNER}/${SCRIPTS_REPO}.git"
SCRIPTS_TARBALL="https://github.com/${GH_OWNER}/${SCRIPTS_REPO}/archive/refs/heads/master.tar.gz"

# Every executable of a release archive. Only the daemon and the CLI go on PATH;
# the rest keep generic names (packet-parser, ...) under BIN_DIR.
RELEASE_BINARIES="rusthinq rusthinqctl rusthinq-setup rusthinq-migrate rusthinq-mcp
rusthinq-capture rusthinq-retained-gc rusthinq-script-test packet-parser packet-sender
lgcloud-monitor"
LINKED_BINARIES="rusthinq rusthinqctl"

ASSUME_YES=0
PRERELEASE=0
REQUESTED_VERSION=""

# ── Logging helpers ────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'
    C_BOLD=$'\033[1m'; C_RESET=$'\033[0m'
else
    C_RED=""; C_GREEN=""; C_YELLOW=""; C_BOLD=""; C_RESET=""
fi

log()  { printf '%s\n' "$*"; }
warn() { printf '%s! %s%s\n' "$C_YELLOW" "$*" "$C_RESET" >&2; }
err()  { printf '%s✗ %s%s\n' "$C_RED" "$*" "$C_RESET" >&2; }
ok()   { printf '%s✓ %s%s\n' "$C_GREEN" "$*" "$C_RESET"; }
die()  { err "$*"; exit 1; }

# ── Helpers config-0.2.sh expects from its caller ──────────────────────────────
bool() {
    case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
        1 | true | yes | on) return 0 ;;
        *) return 1 ;;
    esac
}

require_port() {
    case "$2" in
        '' | *[!0-9]*) die "$1 must be a port number, got '$2'" ;;
    esac
}

# A TOML basic string. Answers come from single terminal lines, so control
# characters are refused rather than escaped.
toml_str() {
    local s="$1"
    [[ "$s" != *[[:cntrl:]]* ]] || die "control characters are not supported in configuration values"
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    printf '"%s"' "$s"
}

# "a, b,c" -> ["a", "b", "c"]
toml_list() {
    local out="" item items=()
    IFS=, read -r -a items <<< "$1"
    for item in "${items[@]}"; do
        item="$(printf '%s' "$item" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
        [ -n "$item" ] || continue
        out="${out:+$out, }$(toml_str "$item")"
    done
    printf '[%s]' "$out"
}

# ── Pre-flight ─────────────────────────────────────────────────────────────────
require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        err "Root privileges required for this command."
        log "Re-run with: ${C_BOLD}sudo thinqctl ${*:-install}${C_RESET}"
        exit 1
    fi
}

require_systemd() {
    [ -d /run/systemd/system ] || die "systemd not detected (thinqctl only supports systemd-based Linux)."
    command -v systemctl >/dev/null 2>&1 || die "systemctl not found in PATH."
}

require_tools() {
    local missing=() cmd
    for cmd in curl tar sha256sum install; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    [ ${#missing[@]} -eq 0 ] || die "Missing required tool(s): ${missing[*]}"
}

# ── Questions ──────────────────────────────────────────────────────────────────
# `curl ... | sudo bash` leaves stdin on the pipe, so questions use the terminal.
have_tty() { [ "$ASSUME_YES" -eq 0 ] && { : < /dev/tty; } 2>/dev/null; }

read_tty() { IFS= read -r "$@" < /dev/tty; }

# ask VAR "Question" DEFAULT
ask() {
    local __answer=""
    if have_tty; then
        read_tty -p "$2${3:+ [$3]}: " __answer
    fi
    __answer="${__answer:-$3}"
    [[ "$__answer" != *[[:cntrl:]]* ]] || die "control characters are not supported"
    printf -v "$1" '%s' "$__answer"
}

# ask_secret VAR "Question" — empty unless typed.
ask_secret() {
    local __answer=""
    if have_tty; then
        read_tty -s -p "$2: " __answer
        printf '\n' >&2
    fi
    [[ "$__answer" != *[[:cntrl:]]* ]] || die "control characters are not supported"
    printf -v "$1" '%s' "$__answer"
}

# ask_yn "Question" y|n — the default applies without a terminal and with --yes.
ask_yn() {
    local answer="" hint="y/N"
    [ "$2" = y ] && hint="Y/n"
    if have_tty; then
        read_tty -p "$1 [$hint] " answer
    fi
    case "${answer:-$2}" in
        [Yy]*) return 0 ;;
        *) return 1 ;;
    esac
}

confirm() {
    [ "$ASSUME_YES" -eq 0 ] || return 0
    have_tty || die "$1 — no terminal for the question; pass --yes to proceed."
    ask_yn "$1" n
}

# confirm_word "Prompt" WORD — the word must be typed; --yes skips it.
confirm_word() {
    local answer=""
    if [ "$ASSUME_YES" -eq 1 ]; then
        warn "--yes given; not asking for '$2'."
        return 0
    fi
    have_tty || die "$1 — no terminal for the question; pass --yes to proceed."
    read_tty -p "$1 (type '$2'): " answer
    [ "$answer" = "$2" ]
}

# ── Releases ───────────────────────────────────────────────────────────────────
detect_arch() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64)  echo "amd64" ;;
        aarch64|arm64) echo "aarch64" ;;
        armv7l|armhf)  echo "armv7" ;;
        *) die "Unsupported architecture: $arch" ;;
    esac
}

api_latest_tag() {
    # Default: stable only (`releases/latest` excludes pre-releases by design).
    # With PRERELEASE=1: the newest `v*` release by numeric id, which is monotonic,
    # rather than trusting the order `/releases` returns. Without jq, awk pairs
    # each release's own url (`/releases/<id>`, not an asset's or author's id)
    # with its tag_name, and reads to the end so pipefail never sees SIGPIPE.
    local url body
    if [ "$PRERELEASE" -eq 1 ]; then
        url="https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/releases"
        body="$(curl -fsSL "$url" 2>/dev/null)" || return 1
        if command -v jq >/dev/null 2>&1; then
            printf '%s' "$body" \
                | jq -r '[.[] | select(.tag_name | startswith("v"))] | sort_by(.id) | last.tag_name // empty'
        else
            printf '%s' "$body" \
                | awk -F'"' '
                    $2 == "url" && $4 ~ /\/releases\/[0-9]+$/ {
                        n = split($4, parts, "/"); cur_id = parts[n] + 0
                    }
                    $2 == "tag_name" && $4 ~ /^v/ {
                        if (cur_id > max_id) { max_id = cur_id; max_tag = $4 }
                    }
                    END { if (max_tag != "") print max_tag }'
        fi
    else
        url="https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/releases/latest"
        body="$(curl -fsSL "$url" 2>/dev/null)" || return 1
        if command -v jq >/dev/null 2>&1; then
            printf '%s' "$body" | jq -r '.tag_name'
        else
            printf '%s' "$body" | awk -F'"' '!found && /"tag_name"/ { print $4; found=1 }'
        fi
    fi
}

release_tag() {
    if [ -n "$REQUESTED_VERSION" ]; then
        printf 'v%s\n' "$REQUESTED_VERSION"
    else
        local tag
        tag="$(api_latest_tag)" || tag=""
        if [ -z "$tag" ] || [ "$tag" = "null" ]; then
            [ "$PRERELEASE" -eq 1 ] && die "Failed to query releases from GitHub."
            die "No stable rusthinq release found. Pre-releases need --prerelease."
        fi
        printf '%s\n' "$tag"
    fi
}

# Trust the entire resolved path, not just the executable: a writable parent
# would let another user replace even a root-owned binary.
root_trusted_path() {
    local path="$1" uid mode
    while :; do
        read -r uid mode < <(stat -Lc '%u %a' -- "$path" 2>/dev/null)
        [[ "$uid" = 0 && "$mode" =~ ^[0-7]+$ ]] || return 1
        (( (8#$mode & 0022) == 0 )) || return 1
        [ "$path" != / ] || return 0
        path="$(dirname -- "$path")"
    done
}

# Version of a rusthinq binary; empty if unavailable or unsafe to execute.
# Secure root-owned custom paths remain usable for adopted installations.
binary_version() {
    local bin
    bin="$(realpath -e -- "$1" 2>/dev/null)" || return 0
    [[ -f "$bin" && -x "$bin" ]] || return 0
    if [ "$(id -u)" -eq 0 ]; then
        root_trusted_path "$bin" || return 0
    fi
    "$bin" --version 2>/dev/null | awk '$1 == "rusthinq" { print $2 }' || true
}

# Echoes `equal`, `newer` (when $1 is newer than $2) or `older`. A pre-release
# sorts before its base release, which plain `sort -V` gets wrong; `dev` is the
# least mature pre-release although it sorts between beta and rc alphabetically.
version_direction() {
    if [ "$1" = "$2" ]; then
        printf 'equal'
        return
    fi
    local a_base="${1%%-*}" b_base="${2%%-*}" a_pre="" b_pre="" first
    case "$1" in *-*) a_pre="${1#*-}" ;; esac
    case "$2" in *-*) b_pre="${2#*-}" ;; esac
    if [ "$a_base" != "$b_base" ]; then
        first="$(printf '%s\n%s\n' "$a_base" "$b_base" | sort -V | head -n1)"
        [ "$first" = "$a_base" ] && printf 'older' || printf 'newer'
        return
    fi
    if [ -z "$a_pre" ]; then
        printf 'newer'
    elif [ -z "$b_pre" ]; then
        printf 'older'
    else
        case "$a_pre" in
            dev|dev.*|dev[0-9]*)
                case "$b_pre" in dev|dev.*|dev[0-9]*) ;; *) printf 'older'; return ;; esac ;;
        esac
        case "$b_pre" in
            dev|dev.*|dev[0-9]*)
                case "$a_pre" in dev|dev.*|dev[0-9]*) ;; *) printf 'newer'; return ;; esac ;;
        esac
        first="$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)"
        [ "$first" = "$1" ] && printf 'older' || printf 'newer'
    fi
}

# Verifies $2 in directory $1 against the SHA256SUMS there.
verify_asset() {
    local line
    line="$(awk -v name="$2" '$2 == name || $2 == "*" name' "$1/SHA256SUMS")"
    [ -n "$line" ] || return 2
    ( cd "$1" && printf '%s\n' "$line" | sha256sum -c - >/dev/null )
}

# Downloads, verifies and unpacks a release; sets RELEASE_DIR to the binaries.
download_release() {
    local tag="$1" arch="$2" tmpdir="$3"
    local base="https://github.com/${GH_OWNER}/${GH_REPO}/releases/download/${tag}"
    local name="rusthinq-${tag}-linux-${arch}"
    local archive="${name}.tar.gz" bin

    log "  Downloading ${archive}..."
    curl -fsSL -o "${tmpdir}/${archive}" "${base}/${archive}" \
        || die "Could not download ${tag} for linux-${arch}; check that the release and asset exist."
    curl -fsSL -o "${tmpdir}/SHA256SUMS" "${base}/SHA256SUMS" \
        || die "Could not download SHA256SUMS for ${tag}."
    log "  Verifying sha256..."
    verify_asset "$tmpdir" "$archive" || die "sha256 verification failed for ${archive}"
    log "  Extracting..."
    tar -xzf "${tmpdir}/${archive}" -C "$tmpdir"
    RELEASE_DIR="${tmpdir}/${name}"
    for bin in $RELEASE_BINARIES; do
        [ -f "${RELEASE_DIR}/${bin}" ] || die "'${bin}' not found inside ${archive}"
    done
    RELEASE_TAG="$tag"
}

# Fetches the configuration generator of the release download_release fetched;
# sets GENERATOR. Releases before it was published fall back to master's copy.
fetch_generator() {
    local tmpdir="$1"
    local base="https://github.com/${GH_OWNER}/${GH_REPO}/releases/download/${RELEASE_TAG}"
    GENERATOR="${tmpdir}/${GENERATOR_ASSET}"
    if curl -fsSL -o "$GENERATOR" "${base}/${GENERATOR_ASSET}" 2>/dev/null; then
        verify_asset "$tmpdir" "$GENERATOR_ASSET" \
            || die "sha256 verification failed for ${GENERATOR_ASSET}"
    else
        warn "${RELEASE_TAG} does not publish ${GENERATOR_ASSET}; using the copy on master."
        curl -fsSL -o "$GENERATOR" "$GENERATOR_URL" \
            || die "Could not download the configuration generator."
    fi
}

# ── Inspecting processes and units ─────────────────────────────────────────────
# Thin wrappers so the tests can supply a process table.
proc_pids()   { pgrep -x "$1" 2>/dev/null || true; }
proc_exe()    { local e; e="$(readlink "/proc/$1/exe" 2>/dev/null)" || return 1; printf '%s' "${e% (deleted)}"; }
proc_args()   { tr '\0' '\n' < "/proc/$1/cmdline" 2>/dev/null; }
proc_cwd()    { readlink "/proc/$1/cwd" 2>/dev/null; }
proc_user()   { ps -o user= -p "$1" 2>/dev/null | tr -d ' '; }
proc_cgroup() { cat "/proc/$1/cgroup" 2>/dev/null; }
unit_prop()   { systemctl show -p "$2" --value "$1" 2>/dev/null || true; }

# Reads a cgroup listing on stdin; echoes `container`, `unit:NAME` or `manual`.
classify_cgroup() {
    local line path=""
    while IFS= read -r line; do
        case "$line" in
            0::*) path="${line#0::}" ;;
            *:name=systemd:*) [ -n "$path" ] || path="${line#*:name=systemd:}" ;;
        esac
    done
    case "$path" in
        */docker/*|*/docker-*.scope|*containerd*|*kubepods*|*libpod*) echo container ;;
        /system.slice/*.service) echo "unit:${path##*/}" ;;
        *) echo manual ;;
    esac
}

# Joins a relative path onto a directory.
resolve_path() {
    case "$2" in
        /*) printf '%s' "$2" ;;
        *) printf '%s/%s' "${1%/}" "${2#./}" ;;
    esac
}

# Reads the state file into S_* without executing it.
load_state() {
    S_OWNED="" S_BIN="" S_CONFIG="" S_UNIT="" S_UNIT_OWNED="" S_RUN_USER=""
    S_VERSION="" S_SCRIPTS="" S_MIGRATED_FROM=""
    [ -f "$STATE_FILE" ] || return 1
    local key value
    while IFS='=' read -r key value; do
        case "$key" in
            OWNED|BIN|CONFIG|UNIT|UNIT_OWNED|RUN_USER|VERSION|SCRIPTS|MIGRATED_FROM)
                printf -v "S_${key}" '%s' "$value" ;;
        esac
    done < "$STATE_FILE"
    [ -n "$S_BIN" ]
}

save_state() {
    local key value
    {
        printf '# Written by thinqctl: the rusthinq installation it manages.\n'
        for key in OWNED BIN CONFIG UNIT UNIT_OWNED RUN_USER VERSION SCRIPTS MIGRATED_FROM; do
            value="S_${key}"
            value="${!value}"
            [[ "$value" != *$'\n'* ]] || die "invalid state value for $key"
            printf '%s=%s\n' "$key" "$value"
        done
    } > "${STATE_FILE}.tmp"
    chmod 644 "${STATE_FILE}.tmp"
    mv -f "${STATE_FILE}.tmp" "$STATE_FILE"
}

# Fills I_* (the 0.2 installation) and L_* (a running 0.1). I_KIND is one of
#   none       nothing found
#   managed    created by thinqctl
#   systemd    someone else's, run by a systemd unit
#   manual     someone else's, started by hand or a script
#   container  run inside a container: left to its image
#   legacy     no 0.2, but a 0.1 rusthinq-cloud is running
detect_installation() {
    I_KIND="none" I_BIN="" I_CONFIG="" I_UNIT="" I_USER="" I_PID="" I_OWNED=0
    L_PID="" L_BIN="" L_CONFIG="" L_UNIT="" L_KIND=""
    local state=0 pid args config cwd where
    load_state && state=1

    pid="$(proc_pids "$SERVICE" | head -n1)"
    if [ -n "$pid" ]; then
        [ "$(proc_pids "$SERVICE" | wc -l)" -le 1 ] \
            || warn "Several rusthinq processes are running; looking at PID $pid."
        args="$(proc_args "$pid")"
        I_PID="$pid"
        I_BIN="$(proc_exe "$pid")" || I_BIN=""
        if [ -z "$I_BIN" ]; then
            # /proc/PID/exe of another user's process needs root.
            I_BIN="$(printf '%s\n' "$args" | sed -n 1p)"
        fi
        config="$(printf '%s\n' "$args" | sed -n 2p)"
        cwd="$(proc_cwd "$pid")" || cwd=""
        if [ -n "$config" ]; then
            case "$config" in
                /*) I_CONFIG="$config" ;;
                *) [ -n "$cwd" ] && I_CONFIG="$(resolve_path "$cwd" "$config")" || I_CONFIG="$config" ;;
            esac
        fi
        I_USER="$(proc_user "$pid")"
        where="$(proc_cgroup "$pid" | classify_cgroup)"
        case "$where" in
            container) I_KIND="container" ;;
            unit:*) I_KIND="systemd"; I_UNIT="${where#unit:}" ;;
            *) I_KIND="manual" ;;
        esac
        if [ "$state" -eq 1 ] && [ "$I_KIND" != container ] && [ "$S_BIN" = "$I_BIN" ]; then
            [ -n "$I_UNIT" ] || I_UNIT="$S_UNIT"
            if [ "$S_OWNED" = 1 ]; then I_KIND="managed"; I_OWNED=1; fi
        fi
    elif [ "$state" -eq 1 ]; then
        I_BIN="$S_BIN" I_CONFIG="$S_CONFIG" I_UNIT="$S_UNIT" I_USER="$S_RUN_USER"
        if [ "$S_OWNED" = 1 ]; then
            I_KIND="managed"; I_OWNED=1
        elif [ -n "$S_UNIT" ]; then
            I_KIND="systemd"
        else
            I_KIND="manual"
        fi
    elif [ "$(unit_prop "${SERVICE}.service" LoadState)" = loaded ]; then
        # A stopped unit of someone else's: its ExecStart names the binary and config.
        local exec argv workdir
        exec="$(unit_prop "${SERVICE}.service" ExecStart)"
        argv="$(printf '%s' "$exec" | sed -n 's/.*argv\[\]=\([^;]*\) ;.*/\1/p')"
        workdir="$(unit_prop "${SERVICE}.service" WorkingDirectory)"
        I_BIN="${argv%% *}"
        config="${argv#* }"
        [ "$config" != "$argv" ] || config=""
        I_CONFIG="$(resolve_path "${workdir:-/}" "${config%% *}")"
        I_USER="$(unit_prop "${SERVICE}.service" User)"
        I_USER="${I_USER:-root}"
        I_UNIT="${SERVICE}.service"
        I_KIND="systemd"
    fi

    pid="$(proc_pids rusthinq-cloud | head -n1)"
    if [ -n "$pid" ]; then
        args="$(proc_args "$pid")"
        L_PID="$pid"
        L_BIN="$(proc_exe "$pid")" || L_BIN="$(printf '%s\n' "$args" | sed -n 1p)"
        config="$(printf '%s\n' "$args" | sed -n 2p)"
        cwd="$(proc_cwd "$pid")" || cwd=""
        # 0.1 defaults to ./config.toml.
        L_CONFIG="$(resolve_path "${cwd:-.}" "${config:-config.toml}")"
        where="$(proc_cgroup "$pid" | classify_cgroup)"
        case "$where" in
            container) L_KIND="container" ;;
            unit:*) L_KIND="systemd"; L_UNIT="${where#unit:}" ;;
            *) L_KIND="manual" ;;
        esac
        [ "$I_KIND" != none ] || I_KIND="legacy"
    fi
}

describe_kind() {
    case "$1" in
        managed)   printf 'installed by thinqctl' ;;
        systemd)   printf 'adopted, systemd unit' ;;
        manual)    printf 'adopted, started outside systemd' ;;
        container) printf 'running in a container' ;;
        legacy)    printf 'rusthinq 0.1 only' ;;
        *)         printf 'not installed' ;;
    esac
}

# Value of `directory` in the [drivers] table of a config file, resolved.
config_drivers_dir() {
    [ -r "$1" ] || return 0
    local raw
    raw="$(awk '
        /^[[:space:]]*\[/ { section = $0; gsub(/[[:space:]]/, "", section) }
        section == "[drivers]" && /^[[:space:]]*directory[[:space:]]*=/ {
            sub(/^[^=]*=[[:space:]]*"/, ""); sub(/"[[:space:]]*(#.*)?$/, ""); print; exit
        }' "$1")"
    [ -n "$raw" ] || return 0
    resolve_path "$(dirname "$1")" "$raw"
}

# `bind = "host:port"` of [management], as a port.
config_management_port() {
    [ -r "$1" ] || return 0
    awk '
        /^[[:space:]]*\[/ { section = $0; gsub(/[[:space:]]/, "", section) }
        section == "[management]" && /^[[:space:]]*bind[[:space:]]*=/ {
            sub(/"[[:space:]]*(#.*)?$/, ""); n = split($0, parts, ":"); print parts[n]; exit
        }' "$1"
}

# ── System integration ─────────────────────────────────────────────────────────
ensure_user() {
    if ! id -u "$SVC_USER" >/dev/null 2>&1; then
        useradd --system --no-create-home --shell /usr/sbin/nologin "$SVC_USER"
        log "  Created system user '${SVC_USER}'."
    fi
}

ensure_data_dir() {
    install -d -m 750 -o "$SVC_USER" -g "$SVC_USER" "$DATA_DIR"
}

# Copies $1/<binary> over $2/<binary> by rename, so a running daemon keeps its
# old inode. With "existing", only binaries already in $2 (and the daemon) are
# replaced: an adopted directory may hold a subset.
install_binaries() {
    local src="$1" dest="$2" mode="${3:-all}" bin
    [ -d "$dest" ] || install -d -m 755 "$dest"
    for bin in $RELEASE_BINARIES; do
        if [ "$mode" = existing ] && [ "$bin" != "$SERVICE" ] && [ ! -e "${dest}/${bin}" ]; then
            continue
        fi
        install -m 755 "${src}/${bin}" "${dest}/.${bin}.thinqctl-new"
        # A replaced binary keeps its owner.
        [ ! -e "${dest}/${bin}" ] || chown --reference="${dest}/${bin}" "${dest}/.${bin}.thinqctl-new"
        mv -f "${dest}/.${bin}.thinqctl-new" "${dest}/${bin}"
    done
}

link_binaries() {
    local bin
    for bin in $LINKED_BINARIES; do
        ln -sfn "${BIN_DIR}/${bin}" "${LINK_DIR}/${bin}"
    done
}

unlink_binaries() {
    local bin
    for bin in $LINKED_BINARIES; do
        if [ -L "${LINK_DIR}/${bin}" ] && [ "$(readlink "${LINK_DIR}/${bin}")" = "${BIN_DIR}/${bin}" ]; then
            rm -f "${LINK_DIR}/${bin}"
        fi
    done
}

# Keeps the binaries an upgrade is about to replace, for `rollback`.
backup_binaries() {
    local dir="$1" version="$2" bin
    rm -rf "$BACKUP_DIR"
    install -d -m 755 "$BACKUP_DIR"
    for bin in $RELEASE_BINARIES; do
        [ ! -e "${dir}/${bin}" ] || cp -p "${dir}/${bin}" "${BACKUP_DIR}/${bin}"
    done
    printf '%s\n' "$dir" > "${BACKUP_DIR}/.dir"
    printf '%s\n' "$version" > "${BACKUP_DIR}/.version"
}

# Paths written into a unit must not need systemd quoting or expand specifiers.
unit_safe_path() {
    [[ "$1" != *[\"\\%\$]* && "$1" != *[[:cntrl:]]* ]] || die "Path not supported in a systemd unit: $1"
}

# write_unit BIN CONFIG USER HARDENED
write_unit() {
    local bin="$1" config="$2" user="$3" hardened="$4"
    unit_safe_path "$bin"
    unit_safe_path "$config"
    {
        cat <<EOF
[Unit]
Description=rusthinq - local LG ThinQ appliance server
Documentation=https://github.com/${GH_OWNER}/${GH_REPO}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${user}
WorkingDirectory=$(dirname "$config")
ExecStart="${bin}" "${config}"
# The daemon has no in-process restart; systemd brings it back.
Restart=on-failure
RestartSec=5
# Appliances connect on fixed ports, 443 among them.
AmbientCapabilities=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
EOF
        if [ "$hardened" = 1 ]; then
            cat <<EOF
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
ProtectSystem=strict
# A driver directory may live in a home directory.
ProtectHome=read-only
PrivateTmp=true
ReadWritePaths=${DATA_DIR}
EOF
        fi
        cat <<EOF

[Install]
WantedBy=multi-user.target
EOF
    } > "$UNIT_PATH"
    chmod 644 "$UNIT_PATH"
    systemctl daemon-reload
    log "  Wrote ${UNIT_PATH}."
}

# Download into a private temporary file; never execute downloaded code to check it.
self_download() {
    curl --connect-timeout 10 --max-time 60 -fsSL "$SELF_URL" -o "$1" || return 1
    [ -s "$1" ] && head -n1 "$1" | grep -qx '#!/usr/bin/env bash' || return 1
    bash -n "$1"
}

install_self() (
    local tmp
    tmp="$(mktemp "${LINK_DIR}/.thinqctl.XXXXXX")" || return 1
    trap 'rm -f -- "$tmp"' EXIT
    log "  Fetching thinqctl to ${SELF_PATH}..."
    self_download "$tmp" || { warn "Could not fetch a valid thinqctl; existing copy retained."; return 0; }
    chmod 755 "$tmp"
    mv -f "$tmp" "$SELF_PATH"
)

cmd_self() (
    local self_action="${1:-check}" tmp current
    case "$self_action" in check) ;; update) require_root "self update" ;; *) die "Unknown self action: $self_action (check, update)" ;; esac
    tmp="$(mktemp -d)"
    trap 'rm -rf -- "$tmp"' EXIT
    self_download "$tmp/thinqctl" || die "Could not download a valid thinqctl; existing copy retained."
    current="${BASH_SOURCE[0]}"
    [ -f "$SELF_PATH" ] && current="$SELF_PATH"
    if [ -f "$current" ] && cmp -s "$current" "$tmp/thinqctl"; then
        ok "thinqctl is up to date with master."
        return
    fi
    if [ "$self_action" = check ]; then
        log "thinqctl differs from master. Update with: sudo thinqctl self update"
    else
        confirm "Replace ${SELF_PATH} with thinqctl from master?" || return 0
        local staged
        staged="$(mktemp "${LINK_DIR}/.thinqctl.XXXXXX")"
        trap 'rm -rf -- "$tmp"; rm -f -- "$staged"' EXIT
        install -m 755 "$tmp/thinqctl" "$staged"
        mv -f "$staged" "$SELF_PATH"
        ok "thinqctl updated. Services and configuration were left untouched."
    fi
)

# Waits for a started unit to come up and stay up.
wait_running() {
    local unit="$1" _
    for _ in $(seq 1 15); do
        systemctl is-active --quiet "$unit" 2>/dev/null && break
        sleep 1
    done
    sleep 3
    if ! systemctl is-active --quiet "$unit" 2>/dev/null; then
        err "${unit} did not stay running."
        log "  Inspect: ${C_BOLD}journalctl -u ${unit} -n 50${C_RESET}"
        return 1
    fi
}

stop_pid() {
    local pid="$1" _
    kill -TERM "$pid" 2>/dev/null || return 0
    for _ in $(seq 1 10); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 1
    done
    die "PID $pid did not exit on SIGTERM."
}

# Validate and compare numeric socket addresses without binding or sending traffic.
# IPv6 wildcard is conservatively treated as dual stack.
listener_check() {
    command -v python3 >/dev/null || die "python3 is required to check listener addresses."
    command -v ss >/dev/null || die "ss (iproute2) is required to check occupied ports."
    python3 - "$@" <<'PYLISTEN'
import ipaddress, re, subprocess, sys

def parse(value):
    host, port = value.rsplit(":", 1)
    host = host.strip("[]")
    if not port.isascii() or not port.isdecimal() or not 1 <= int(port) <= 65535:
        raise ValueError("port must be 1–65535")
    if "%" in host and not re.fullmatch(r"[A-Za-z0-9_.-]+", host.split("%", 1)[1]):
        raise ValueError("invalid IPv6 scope")
    ip = ipaddress.ip_address(host)
    if ip.version == 6 and not value.startswith("["):
        raise ValueError("IPv6 addresses require brackets")
    if ip.version == 6 and ip.ipv4_mapped is not None: ip = ip.ipv4_mapped
    return ip, int(port)

def overlap(a, b):
    if a[1] != b[1]: return False
    x, y = a[0], b[0]
    if x.version != y.version:
        return (x.version == 6 and x.is_unspecified) or (y.version == 6 and y.is_unspecified)
    return x == y or x.is_unspecified or y.is_unspecified

try:
    mode, *values = sys.argv[1:]
    chosen = [parse(v) for v in values]
    if mode == "validate": sys.exit(0)
    output = subprocess.run(["ss", "-H", "-ltnp"], check=True, capture_output=True, text=True).stdout
    occupied = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 5: raise ValueError("unexpected ss output")
        addr = fields[3]
        if addr.startswith("*:"): addr = "[::]:" + addr[2:]
        occupied.append((parse(addr), line))
    conflict = False
    for i, address in enumerate(chosen):
        reasons = [line for other, line in occupied if overlap(address, other)]
        reasons += ["duplicates " + values[j] for j in range(i) if overlap(address, chosen[j])]
        print("  " + values[i] + (" — IN USE" if reasons else " — available"))
        for reason in reasons: print("    " + reason)
        conflict |= bool(reasons)
    sys.exit(1 if conflict else 0)
except (ValueError, OSError, subprocess.CalledProcessError) as e:
    print("Cannot check listeners: " + str(e), file=sys.stderr)
    sys.exit(2)
PYLISTEN
}

configure_listeners() {
    local edit=0 result key
    log "Changed device ports require matching appliance routing/port forwarding."
    local keys=(HTTPS_BIND MQTT_BIND THINQ1_HTTPS_BIND THINQ1_BIND MANAGEMENT_BIND)
    local defaults=(0.0.0.0:443 0.0.0.0:8883 0.0.0.0:46030 0.0.0.0:47878 "0.0.0.0:${A_GUI_PORT}")
    LISTENER_VALUES=()
    for key in "${!keys[@]}"; do
        local env_key="RUSTHINQ_${keys[$key]}"
        LISTENER_VALUES+=("${!env_key:-${defaults[$key]}}")
    done
    while :; do
        log "Listeners: HTTPS, device MQTT, ThinQ1 HTTPS, ThinQ1, dashboard"
        result=0
        listener_check check "${LISTENER_VALUES[@]}" || result=$?
        if [ "$result" -eq 0 ]; then
            if [ "$edit" -eq 1 ] || ! ask_yn "Customize listener addresses?" n; then return; fi
        else
            if [ "$ASSUME_YES" -ne 0 ] || ! have_tty; then
                die "Listener checks failed. Set RUSTHINQ_{HTTPS,MQTT,THINQ1_HTTPS,THINQ1,MANAGEMENT}_BIND and retry."
            fi
            log "Changing device ports requires matching appliance routing/port forwarding."
            local action
            ask action "Resolve: 'edit', 'retry', or 'cancel'" "edit"
            case "$action" in retry) continue ;; cancel) die "Installation cancelled." ;; edit) ;; *) continue ;; esac
        fi
        log "Use numeric IP:port (IPv6: [address]:port). Device MQTT is separate from the external broker."
        for key in "${!keys[@]}"; do
            local value
            ask value "${keys[$key]}" "${LISTENER_VALUES[$key]}"
            LISTENER_VALUES[key]="$value"
        done
        edit=1
    done
}

check_existing_listeners() {
    local values=()
    mapfile -t values < <(awk '
        /^[[:space:]]*\[/ { section=$0; gsub(/[[:space:]]/, "", section) }
        (section == "" && /^[[:space:]]*(https_bind|mqtt_bind|thinq1_https_bind|thinq1_bind)[[:space:]]*=/) ||
        (section == "[management]" && /^[[:space:]]*bind[[:space:]]*=/) {
            sub(/^[^=]*=[[:space:]]*"/, ""); sub(/"[[:space:]]*(#.*)?$/, ""); print
        }' "$CONFIG_FILE")
    [ "${#values[@]}" -gt 0 ] || die "Cannot find listener addresses in $CONFIG_FILE; check configuration manually."
    listener_check check "${values[@]}" || die "Existing configuration has invalid or occupied listeners; resolve before installing."
}

# fetch_scripts git|tarball DIR
fetch_scripts() {
    local how="$1" dir="$2" tmp
    case "$how" in
        git)
            git clone --quiet --depth 1 "$SCRIPTS_GIT" "$dir" \
                || die "Could not clone ${SCRIPTS_GIT}."
            ;;
        tarball)
            tmp="$(mktemp -d)"
            curl -fsSL "$SCRIPTS_TARBALL" | tar -xz -C "$tmp" --strip-components=1 \
                || { rm -rf "$tmp"; die "Could not download ${SCRIPTS_TARBALL}."; }
            rm -rf "${dir}.thinqctl-new"
            mv "$tmp" "${dir}.thinqctl-new"
            chmod 755 "${dir}.thinqctl-new"
            rm -rf "$dir"
            mv "${dir}.thinqctl-new" "$dir"
            ;;
    esac
}

# Runs the release's generator with the A_* answers; config on stdout. The
# answers apply to the call only, so the RUSTHINQ_* defaults stay as they were.
render_config() {
    (
        # shellcheck source=config-0.2.sh
        . "$GENERATOR"
        RUSTHINQ_HOSTNAME="$A_HOSTNAME" RUSTHINQ_BRIDGE="$A_BRIDGE" \
            RUSTHINQ_MQTT_URL="$A_MQTT_URL" \
            RUSTHINQ_MQTT_USER="$A_MQTT_USER" RUSTHINQ_MQTT_PASSWORD="$A_MQTT_PASSWORD" \
            RUSTHINQ_GUI_PORT="$A_GUI_PORT" \
            RUSTHINQ_GUI_USER="$A_GUI_USER" RUSTHINQ_GUI_PASSWORD="$A_GUI_PASSWORD" \
            RUSTHINQ_SCRIPTING="$A_SCRIPTING" RUSTHINQ_PREFIX="$A_PREFIX" \
            SCRIPTS_DIR="$A_SCRIPTS_DIR" generate_config "" | awk \
            -v https="${LISTENER_VALUES[0]}" -v mqtt="${LISTENER_VALUES[1]}" \
            -v t1https="${LISTENER_VALUES[2]}" -v t1="${LISTENER_VALUES[3]}" \
            -v management="${LISTENER_VALUES[4]}" '
                /^https_bind =/ { $0="https_bind = \"" https "\"" }
                /^mqtt_bind =/ { $0="mqtt_bind = \"" mqtt "\"" }
                /^thinq1_https_bind =/ { $0="thinq1_https_bind = \"" t1https "\"" }
                /^thinq1_bind =/ { $0="thinq1_bind = \"" t1 "\"" }
                /^bind =/ { $0="bind = \"" management "\"" }
                { print }
            '

    )
}

generate_ca() {
    command -v openssl >/dev/null 2>&1 || die "openssl is needed to create a CA."
    (
        # shellcheck source=config-0.2.sh
        . "$GENERATOR"
        initialize_ca
    )
}

# ── Install paths ──────────────────────────────────────────────────────────────
# A rusthinq that already runs is configured where it is: thinqctl records it and
# from then on only replaces its binaries.
adopt_installation() {
    printf '%sFound rusthinq%s (%s)\n' "$C_BOLD" "$C_RESET" "$(describe_kind "$I_KIND")"
    printf '  Binary:  %s\n  Config:  %s\n  User:    %s\n' "$I_BIN" "${I_CONFIG:-unknown}" "${I_USER:-unknown}"
    [ -z "$I_UNIT" ] || printf '  Unit:    %s\n' "$I_UNIT"
    [ -z "$I_PID" ] || printf '  PID:     %s\n' "$I_PID"
    [[ -n "$I_BIN" && -x "$I_BIN" ]] || die "Cannot use the binary of this installation: ${I_BIN:-unknown}"
    confirm "Manage this installation with thinqctl? Its binary location, configuration and data stay as they are." \
        || { log "Aborted."; return 1; }

    S_OWNED=0 S_BIN="$I_BIN" S_CONFIG="$I_CONFIG" S_UNIT="$I_UNIT" S_UNIT_OWNED=0
    S_RUN_USER="$I_USER" S_VERSION="$(binary_version "$I_BIN")" S_SCRIPTS="" S_MIGRATED_FROM=""

    if [ "$I_KIND" = manual ]; then
        log "  This rusthinq is not run by systemd. Upgrades will replace its binaries and leave"
        log "  restarting it to you, unless thinqctl creates a systemd unit for it now."
        if [ -n "$I_PID" ] && [ -n "$I_CONFIG" ] && [ ! -e "$UNIT_PATH" ] \
            && ask_yn "Create ${SERVICE}.service for it (stops PID ${I_PID} and starts the unit)?" n; then
            write_unit "$I_BIN" "$I_CONFIG" "${I_USER:-root}" 0
            stop_pid "$I_PID"
            systemctl enable --now "$SERVICE"
            wait_running "${SERVICE}.service" || exit 1
            S_UNIT="${SERVICE}.service" S_UNIT_OWNED=1
            I_KIND="systemd" I_UNIT="${SERVICE}.service"
            ok "Now run by ${SERVICE}.service."
        fi
    fi
    save_state
    install_self
    ok "thinqctl manages ${I_BIN}."
    log "  Check for a newer release: ${C_BOLD}thinqctl status${C_RESET} (pre-releases: --prerelease)"
}

migrate_legacy() {
    printf '%sFound rusthinq 0.1%s (PID %s)\n' "$C_BOLD" "$C_RESET" "$L_PID"
    printf '  Binary:  %s\n  Config:  %s\n' "$L_BIN" "$L_CONFIG"
    [ "$L_KIND" != container ] || die "This 0.1 runs in a container; migrate it with rusthinq-docker."
    [ -f "$L_CONFIG" ] || die "Cannot read the 0.1 configuration ${L_CONFIG}."
    [ ! -e "$DATA_DIR" ] || die "${DATA_DIR} already exists; move it away before migrating."
    confirm "Stage a 0.2 installation from ${L_CONFIG} into ${DATA_DIR}? 0.1 files are not modified." \
        || { log "Aborted."; return 0; }

    local arch tag tmpdir report scripts
    arch="$(detect_arch)"
    tag="$(release_tag)"
    tmpdir="$(mktemp -d)"
    # shellcheck disable=SC2064 # Capture the local path before the function returns.
    trap "rm -rf -- '$tmpdir'" EXIT
    download_release "$tag" "$arch" "$tmpdir"
    install_binaries "$RELEASE_DIR" "$BIN_DIR"
    link_binaries
    ensure_user

    report="$("${BIN_DIR}/rusthinq-migrate" "$L_CONFIG" "$DATA_DIR")" \
        || die "rusthinq-migrate failed; nothing of 0.1 was changed."
    printf '%s\n' "$report"
    chown -R "${SVC_USER}:${SVC_USER}" "$DATA_DIR"
    chmod 750 "$DATA_DIR"
    scripts="$(config_drivers_dir "${DATA_DIR}/config.toml")"
    if [ -n "$scripts" ] && ! runuser -u "$SVC_USER" -- test -r "$scripts"; then
        warn "${SVC_USER} cannot read the driver directory ${scripts}."
    fi
    write_unit "${BIN_DIR}/rusthinq" "${DATA_DIR}/config.toml" "$SVC_USER" 0
    systemctl enable "$SERVICE" >/dev/null 2>&1

    S_OWNED=1 S_BIN="${BIN_DIR}/rusthinq" S_CONFIG="${DATA_DIR}/config.toml"
    S_UNIT="${SERVICE}.service" S_UNIT_OWNED=1 S_RUN_USER="$SVC_USER" S_VERSION="${tag#v}"
    S_SCRIPTS="" S_MIGRATED_FROM="$L_CONFIG"
    save_state
    install_self
    ok "Staged 0.2 in ${DATA_DIR}. 0.1 is still running."
    log "  Review the warnings above. 0.1-era drivers need the il_common revision of"
    log "  ${SCRIPTS_REPO} (docs/0.2-tools-migration.md, Upgrading)."

    # Switching takes the ports 0.1 holds; --yes alone never does it.
    if [ "$ASSUME_YES" -eq 0 ] && have_tty && ask_yn "Stop 0.1 and start 0.2 now?" n; then
        if [ "$L_KIND" = systemd ]; then
            systemctl disable --now "$L_UNIT"
        else
            stop_pid "$L_PID"
        fi
        systemctl start "$SERVICE"
        if wait_running "${SERVICE}.service"; then
            ok "0.2 is running."
            log "  Adopt archived LG registrations per device: rusthinqctl adopt <id>"
        else
            log "  Roll back: sudo systemctl stop ${SERVICE}, then start 0.1 again."
            exit 1
        fi
    else
        log "  Switch later: stop 0.1, then ${C_BOLD}sudo systemctl start ${SERVICE}${C_RESET}"
    fi
}

fresh_install() {
    local arch tag tmpdir keep_config=0 answer
    arch="$(detect_arch)"
    tag="$(release_tag)"
    tmpdir="$(mktemp -d)"
    # shellcheck disable=SC2064 # Capture the local path before the function returns.
    trap "rm -rf -- '$tmpdir'" EXIT

    log "Installing rusthinq ${tag} for linux-${arch}..."
    download_release "$tag" "$arch" "$tmpdir"

    A_SCRIPTS_HOW="none"
    if [ -f "$CONFIG_FILE" ]; then
        keep_config=1
        log "  Keeping existing ${CONFIG_FILE} (and the CA next to it)."
    else
        fetch_generator "$tmpdir"
        log ""
        log "${C_BOLD}Configuration${C_RESET} (Enter keeps the default)"
        ask A_HOSTNAME "Hostname appliances are routed to" "${RUSTHINQ_HOSTNAME:-rusthinq.lan}"
        log "  Appliances trust the CA they were set up with. Reuse the CA of an earlier"
        log "  installation, or every appliance has to be set up again."
        ask A_CA_KEY "Existing CA key file (empty creates a new CA)" ""
        A_CA_CERT=""
        if [ -n "$A_CA_KEY" ]; then
            [ -f "$A_CA_KEY" ] || die "No such file: $A_CA_KEY"
            ask A_CA_CERT "Existing CA certificate file" "$(dirname "$A_CA_KEY")/ca.cert"
            [ -f "$A_CA_CERT" ] || die "No such file: $A_CA_CERT"
        fi
        A_BRIDGE=false
        ask_yn "Enable the LG cloud bridge?" "$(bool "${RUSTHINQ_BRIDGE:-true}" && echo y || echo n)" && A_BRIDGE=true
        ask A_MQTT_URL "MQTT broker (mqtt:// or mqtts://, '-' for none)" "${RUSTHINQ_MQTT_URL:-mqtt://localhost:1883}"
        [ "$A_MQTT_URL" != "-" ] || A_MQTT_URL=""
        A_MQTT_USER="" A_MQTT_PASSWORD=""
        if [ -n "$A_MQTT_URL" ]; then
            ask A_MQTT_USER "MQTT user (empty for none)" "${RUSTHINQ_MQTT_USER:-}"
            if [ -n "$A_MQTT_USER" ]; then
                ask_secret A_MQTT_PASSWORD "MQTT password"
                A_MQTT_PASSWORD="${A_MQTT_PASSWORD:-${RUSTHINQ_MQTT_PASSWORD:-}}"
            fi
        fi
        ask A_GUI_PORT "Dashboard port" "${RUSTHINQ_GUI_PORT:-44401}"
        require_port "Dashboard port" "$A_GUI_PORT"
        ask A_GUI_USER "Dashboard user (empty: no login, reachable from the network)" "${RUSTHINQ_GUI_USER:-}"
        A_GUI_PASSWORD=""
        if [ -n "$A_GUI_USER" ]; then
            ask_secret A_GUI_PASSWORD "Dashboard password"
            A_GUI_PASSWORD="${A_GUI_PASSWORD:-${RUSTHINQ_GUI_PASSWORD:-}}"
            [ -n "$A_GUI_PASSWORD" ] || die "A dashboard user needs a password."
        fi
        log "  Device drivers come from ${SCRIPTS_REPO}."
        ask answer "Drivers: 'download', a directory path, or 'none'" "download"
        A_SCRIPTING=true A_SCRIPTS_DIR="$SCRIPTS_DIR" A_PREFIX="${RUSTHINQ_PREFIX:-rusthinq}"
        case "$answer" in
            download)
                if command -v git >/dev/null 2>&1; then A_SCRIPTS_HOW=git; else A_SCRIPTS_HOW=tarball; fi
                ;;
            none) A_SCRIPTING=false ;;
            /*)
                [ -d "$answer" ] || die "No such directory: $answer"
                A_SCRIPTS_DIR="$answer"
                ;;
            *) die "Expected 'download', 'none' or an absolute directory path." ;;
        esac
        if [ "$A_SCRIPTING" = true ]; then
            ask A_PREFIX "MQTT topic prefix for driver output" "$A_PREFIX"
        fi
        configure_listeners
        # Fail on a bad answer before anything is installed.
        render_config > /dev/null
    fi

    [ "$keep_config" -eq 0 ] || check_existing_listeners

    log ""
    printf '%sInstall plan%s\n' "$C_BOLD" "$C_RESET"
    printf '  Binaries:  %s (rusthinq, rusthinqctl linked into %s)\n' "$BIN_DIR" "$LINK_DIR"
    printf '  Data:      %s, owned by user %s\n' "$DATA_DIR" "$SVC_USER"
    if [ "$keep_config" -eq 0 ]; then
        printf '  Hostname:  %s\n' "$A_HOSTNAME"
        if [ -n "$A_CA_KEY" ]; then printf '  CA:        copy of %s\n' "$A_CA_KEY"; else printf '  CA:        new\n'; fi
        printf '  LG bridge: %s\n' "$A_BRIDGE"
        printf '  MQTT:      %s\n' "${A_MQTT_URL:-none}"
        if [ -n "$A_GUI_USER" ]; then
            printf '  Dashboard: port %s, login %s\n' "$A_GUI_PORT" "$A_GUI_USER"
        else
            printf '  Dashboard: port %s, no login\n' "$A_GUI_PORT"
        fi
        case "$A_SCRIPTING:$A_SCRIPTS_HOW" in
            false:*) printf '  Drivers:   none\n' ;;
            true:none) printf '  Drivers:   %s\n' "$A_SCRIPTS_DIR" ;;
            *) printf '  Drivers:   %s into %s\n' "$A_SCRIPTS_HOW" "$A_SCRIPTS_DIR" ;;
        esac
    fi
    confirm "Install?" || { log "Aborted."; return 0; }

    ensure_user
    ensure_data_dir
    install_binaries "$RELEASE_DIR" "$BIN_DIR"
    link_binaries
    S_SCRIPTS=""
    if [ "$keep_config" -eq 0 ]; then
        if [ -n "$A_CA_KEY" ]; then
            [[ ! -e "${DATA_DIR}/ca.key" && ! -e "${DATA_DIR}/ca.cert" ]] \
                || die "${DATA_DIR} already holds a CA; refusing to overwrite it."
            install -m 600 -o "$SVC_USER" -g "$SVC_USER" "$A_CA_KEY" "${DATA_DIR}/ca.key"
            install -m 644 -o "$SVC_USER" -g "$SVC_USER" "$A_CA_CERT" "${DATA_DIR}/ca.cert"
        else
            generate_ca
            chown "${SVC_USER}:${SVC_USER}" "${DATA_DIR}/ca.key" "${DATA_DIR}/ca.cert"
        fi
        if [ "$A_SCRIPTS_HOW" != none ]; then
            if [ -e "$A_SCRIPTS_DIR" ]; then
                log "  Keeping existing ${A_SCRIPTS_DIR}."
            else
                log "  Fetching ${SCRIPTS_REPO}..."
                fetch_scripts "$A_SCRIPTS_HOW" "$A_SCRIPTS_DIR"
                S_SCRIPTS="$A_SCRIPTS_HOW"
            fi
        fi
        if [ "$A_SCRIPTING" = true ] && ! runuser -u "$SVC_USER" -- test -r "$A_SCRIPTS_DIR"; then
            warn "${SVC_USER} cannot read the driver directory ${A_SCRIPTS_DIR}."
        fi
        ( umask 077 && render_config > "${CONFIG_FILE}.tmp" )
        chown "root:${SVC_USER}" "${CONFIG_FILE}.tmp"
        chmod 640 "${CONFIG_FILE}.tmp"
        mv -f "${CONFIG_FILE}.tmp" "$CONFIG_FILE"
        log "  Wrote ${CONFIG_FILE}."
    fi
    write_unit "${BIN_DIR}/rusthinq" "$CONFIG_FILE" "$SVC_USER" 1

    S_OWNED=1 S_BIN="${BIN_DIR}/rusthinq" S_CONFIG="$CONFIG_FILE" S_UNIT="${SERVICE}.service"
    S_UNIT_OWNED=1 S_RUN_USER="$SVC_USER" S_VERSION="${tag#v}" S_MIGRATED_FROM=""
    save_state
    install_self

    systemctl enable --now "$SERVICE"
    if wait_running "${SERVICE}.service"; then
        print_install_summary "$tag"
    else
        log "  Fix ${CONFIG_FILE}, then: sudo systemctl restart ${SERVICE}"
        exit 1
    fi
}

print_install_summary() {
    local port
    port="$(config_management_port "$CONFIG_FILE")"
    printf '\n%sInstallation complete%s\n' "$C_BOLD" "$C_RESET"
    printf '  Version:   %s\n' "$1"
    printf '  Config:    %s\n' "$CONFIG_FILE"
    printf '  CA:        %s/ca.key, ca.cert  (back these up)\n' "$DATA_DIR"
    printf '  Service:   %senabled and running%s\n' "$C_GREEN" "$C_RESET"
    [ -z "$port" ] || printf '  Dashboard: http://<this host>:%s/\n' "$port"
    printf '\n%sNext steps%s\n' "$C_BOLD" "$C_RESET"
    printf '  • Route the appliance hostnames to this host (docs/0.2-runtime.md).\n'
    printf '  • Logs:    %sjournalctl -u %s -f%s\n' "$C_BOLD" "$SERVICE" "$C_RESET"
    printf '  • Manage:  %ssudo thinqctl {status,upgrade,scripts,remove,purge}%s\n' "$C_BOLD" "$C_RESET"
}

# ── Commands ───────────────────────────────────────────────────────────────────
cmd_status() {
    local cur latest latest_ver label hint drivers
    detect_installation
    printf '%srusthinq%s  %s\n' "$C_BOLD" "$C_RESET" "$(describe_kind "$I_KIND")"

    if [[ "$I_KIND" != none && "$I_KIND" != legacy ]]; then
        cur="$(binary_version "$I_BIN")"
        [ -n "$cur" ] || cur="$S_VERSION"
        printf '  Binary:    %s  (%s%s%s)\n' "$I_BIN" "$C_BOLD" "${cur:-version unknown}" "$C_RESET"
        printf '  Config:    %s\n' "${I_CONFIG:-unknown}"
        if [ -n "$I_UNIT" ]; then
            printf '  Service:   %s %s, %s\n' "$I_UNIT" \
                "$(systemctl is-active "$I_UNIT" 2>/dev/null || true)" \
                "$(systemctl is-enabled "$I_UNIT" 2>/dev/null || true)"
        fi
        [ -z "$I_PID" ] || printf '  PID:       %s (user %s)\n' "$I_PID" "${I_USER:-unknown}"
        drivers="$(config_drivers_dir "$I_CONFIG")"
        if [ -n "$drivers" ]; then
            if [ -d "${drivers}/.git" ]; then
                printf '  Drivers:   %s  (%s)\n' "$drivers" \
                    "$(git -C "$drivers" log -1 --format='%h %cs' 2>/dev/null || echo git)"
            else
                printf '  Drivers:   %s\n' "$drivers"
            fi
        fi
        [ "$(id -u)" -eq 0 ] || [ -r "${I_CONFIG:-/}" ] || log "  (run with sudo for details)"
    fi
    if [ -n "$L_PID" ]; then
        printf '  0.1:       rusthinq-cloud PID %s, %s  (sudo thinqctl install migrates it)\n' "$L_PID" "$L_CONFIG"
    fi

    if [ "$PRERELEASE" -eq 1 ]; then
        label="Pre-release"; hint="--prerelease"
    else
        label="Latest"; hint=""
    fi
    if [ "$I_KIND" = container ]; then
        return 0
    fi
    if latest="$(api_latest_tag 2>/dev/null)" && [ -n "$latest" ] && [ "$latest" != null ]; then
        latest_ver="${latest#v}"
        if [ "$I_KIND" = none ] || [ "$I_KIND" = legacy ]; then
            printf '  %-10s %s  (run: sudo thinqctl install%s)\n' "$label:" "$latest_ver" "${hint:+ $hint}"
        elif [ -z "${cur:-}" ]; then
            printf '  %-10s %s\n' "$label:" "$latest_ver"
        else
            case "$(version_direction "$cur" "$latest_ver")" in
                equal) printf '  %-10s %s  %s(up to date)%s\n' "$label:" "$latest_ver" "$C_GREEN" "$C_RESET" ;;
                older) printf '  %-10s %s  %s⬆ upgrade available%s  (run: sudo thinqctl upgrade%s)\n' \
                    "$label:" "$latest_ver" "$C_YELLOW" "$C_RESET" "${hint:+ $hint}" ;;
                newer) printf '  %-10s %s  (installed %s is newer)\n' "$label:" "$latest_ver" "$cur" ;;
            esac
        fi
    elif [ "$PRERELEASE" -eq 0 ]; then
        printf '  %-10s %snone%s  (pre-releases: thinqctl status --prerelease)\n' "$label:" "$C_YELLOW" "$C_RESET"
    else
        printf '  %-10s %s(could not query GitHub)%s\n' "$label:" "$C_YELLOW" "$C_RESET"
    fi
}

cmd_install() {
    require_root install
    require_systemd
    require_tools
    detect_installation
    case "$I_KIND" in
        container) die "rusthinq runs in a container. Upgrade it through its image; thinqctl manages host installs." ;;
        managed)   die "Already installed by thinqctl. Use: sudo thinqctl upgrade" ;;
        systemd|manual)
            if [ -n "$S_BIN" ] && [ "$S_BIN" = "$I_BIN" ]; then
                die "thinqctl already manages ${I_BIN}. Use: sudo thinqctl upgrade"
            fi
            adopt_installation ;;
        legacy)    migrate_legacy ;;
        none)      fresh_install ;;
    esac
}

cmd_upgrade() {
    require_root upgrade
    require_tools
    detect_installation
    case "$I_KIND" in
        none)      die "rusthinq is not installed. Use: sudo thinqctl install" ;;
        container) die "rusthinq runs in a container. Upgrade it through its image." ;;
        legacy)    die "Only rusthinq 0.1 is running. Use: sudo thinqctl install (migrates it)" ;;
    esac
    [ -z "$I_UNIT" ] || require_systemd
    if [ -z "$S_BIN" ] || [ "$S_BIN" != "$I_BIN" ]; then
        adopt_installation || return 0
        detect_installation
    fi

    local cur arch tag target_ver dir direction prompt tmpdir was_active=0
    dir="$(dirname "$I_BIN")"
    cur="$(binary_version "$I_BIN")"
    [ -n "$cur" ] || cur="$S_VERSION"
    arch="$(detect_arch)"
    tag="$(release_tag)"
    target_ver="${tag#v}"
    if [ "$cur" = "$target_ver" ]; then
        ok "Already at requested version (${cur})."
        return 0
    fi
    if [ -z "$cur" ]; then
        prompt="Install rusthinq ${target_ver} into ${dir}? (installed version unknown)"
        direction="older"
    else
        direction="$(version_direction "$cur" "$target_ver")"
        if [ "$direction" = newer ]; then
            warn "Installed ${cur} is newer than target ${target_ver}."
            if [ -z "$REQUESTED_VERSION" ] && [ "$PRERELEASE" -ne 1 ]; then
                warn "For the newest pre-release instead, re-run: sudo thinqctl upgrade --prerelease"
            fi
            prompt="Downgrade rusthinq ${cur} → ${target_ver} in ${dir}?"
        else
            prompt="Upgrade rusthinq ${cur} → ${target_ver} in ${dir}?"
        fi
    fi
    confirm "$prompt" || { log "Aborted."; return 0; }

    tmpdir="$(mktemp -d)"
    # shellcheck disable=SC2064 # Capture the local path before the function returns.
    trap "rm -rf -- '$tmpdir'" EXIT
    # Everything that can fail remotely happens before the service stops.
    download_release "$tag" "$arch" "$tmpdir"

    backup_binaries "$dir" "${cur:-unknown}"
    if [ -n "$I_UNIT" ] && systemctl is-active --quiet "$I_UNIT" 2>/dev/null; then
        was_active=1
        systemctl stop "$I_UNIT"
    fi
    if [ "$I_OWNED" -eq 1 ]; then
        install_binaries "$RELEASE_DIR" "$dir"
        link_binaries
        # The unit is a template of thinqctl's; refresh it so its changes reach upgrades.
        [ "$S_UNIT_OWNED" != 1 ] || [ -n "$S_MIGRATED_FROM" ] \
            || write_unit "$I_BIN" "$I_CONFIG" "$S_RUN_USER" 1
    else
        install_binaries "$RELEASE_DIR" "$dir" existing
    fi
    S_VERSION="$target_ver"
    save_state
    install_self
    if [ "$direction" = newer ]; then ok "Downgraded to ${target_ver}."; else ok "Upgraded to ${target_ver}."; fi

    if [ "$was_active" -eq 1 ]; then
        systemctl start "$I_UNIT"
        if wait_running "$I_UNIT"; then
            ok "Service restarted."
        else
            log "  Previous binaries: ${C_BOLD}sudo thinqctl rollback${C_RESET}"
            exit 1
        fi
    elif [ -n "$I_UNIT" ]; then
        if systemctl is-enabled --quiet "$I_UNIT" 2>/dev/null; then
            warn "Service is enabled but inactive — not started by upgrade."
            printf '    Start it with: %ssudo systemctl start %s%s\n' "$C_BOLD" "$I_UNIT" "$C_RESET"
        fi
    elif [ -n "$I_PID" ]; then
        warn "PID ${I_PID} still runs the previous version; restart it to run ${target_ver}."
    fi
}

cmd_rollback() {
    require_root rollback
    [ -f "${BACKUP_DIR}/.dir" ] || die "No previous binaries kept; nothing to roll back."
    detect_installation
    local dir version was_active=0 bin
    dir="$(cat "${BACKUP_DIR}/.dir")"
    version="$(cat "${BACKUP_DIR}/.version")"
    [ "$dir" = "$(dirname "$I_BIN")" ] || die "The kept binaries belong to ${dir}, not to ${I_BIN}."
    confirm "Restore rusthinq ${version} into ${dir}?" || { log "Aborted."; return 0; }
    if [ -n "$I_UNIT" ] && systemctl is-active --quiet "$I_UNIT" 2>/dev/null; then
        was_active=1
        systemctl stop "$I_UNIT"
    fi
    for bin in $RELEASE_BINARIES; do
        [ -e "${BACKUP_DIR}/${bin}" ] || continue
        cp -p "${BACKUP_DIR}/${bin}" "${dir}/.${bin}.thinqctl-new"
        mv -f "${dir}/.${bin}.thinqctl-new" "${dir}/${bin}"
    done
    if load_state; then
        S_VERSION="$version"
        save_state
    fi
    ok "Restored ${version}."
    if [ "$was_active" -eq 1 ]; then
        systemctl start "$I_UNIT"
        wait_running "$I_UNIT" || exit 1
        ok "Service restarted."
    elif [ -n "$I_PID" ]; then
        warn "PID ${I_PID} still runs the replaced version; restart it."
    fi
}

# Never read service-owned repository configuration as root. Git still checks
# ownership of the repository and its metadata under the selected identity.
scripts_git() {
    local dir="$1" owner
    shift
    owner="$(stat -Lc %U -- "$dir")" || return 1
    case "$owner" in
        root|"$SVC_USER") ;;
        "${I_USER:-}") [ -n "${I_USER:-}" ] || return 1 ;;
        *) err "Refusing Git operations in ${dir}: untrusted owner ${owner}."; return 1 ;;
    esac
    if [ "$(id -u)" -eq 0 ] && [ "$owner" != root ]; then
        runuser -u "$owner" -- git -C "$dir" "$@"
    else
        git -C "$dir" "$@"
    fi
}

cmd_scripts() {
    local action="${1:-status}" dir changes
    detect_installation
    [[ "$I_KIND" != none && "$I_KIND" != legacy ]] || die "rusthinq is not installed."
    [ -r "$I_CONFIG" ] || die "Cannot read ${I_CONFIG:-the configuration}; try sudo."
    dir="$(config_drivers_dir "$I_CONFIG")"
    [ -n "$dir" ] || die "${I_CONFIG:-The configuration} has no [drivers] directory."
    case "$action" in
        status)
            printf 'Drivers: %s\n' "$dir"
            if [ -d "${dir}/.git" ]; then
                scripts_git "$dir" log -1 --format='  %h %cs %s' 2>/dev/null || true
            fi
            ;;
        check)
            [ -d "${dir}/.git" ] || die "Driver freshness checks require a Git checkout; tarball freshness is unknown."
            local upstream remote ref local_head remote_head
            upstream="$(scripts_git "$dir" rev-parse --abbrev-ref --symbolic-full-name '@{upstream}')" \
                || die "Driver checkout has no upstream branch."
            remote="${upstream%%/*}" ref="${upstream#*/}"
            local_head="$(scripts_git "$dir" rev-parse HEAD)"
            remote_head="$(scripts_git "$dir" ls-remote --exit-code "$remote" "refs/heads/$ref")" \
                || die "Could not query driver upstream; freshness is unknown."
            remote_head="${remote_head%%[[:space:]]*}"
            if [ "$local_head" = "$remote_head" ]; then
                ok "Driver HEAD matches $upstream."
            else
                log "Driver HEAD differs from $upstream; update with: sudo thinqctl scripts update"
            fi
            changes="$(scripts_git "$dir" --no-optional-locks status --porcelain)" || die "Cannot inspect driver checkout changes."
            [ -z "$changes" ] || warn "Driver checkout has local changes."
            ;;
        update)
            require_root "scripts update"
            if [ -d "${dir}/.git" ]; then
                changes="$(scripts_git "$dir" --no-optional-locks status --porcelain)" || die "Cannot inspect driver checkout changes."
                [ -z "$changes" ] || die "Driver checkout has local changes; resolve them before updating."
                scripts_git "$dir" pull --ff-only --quiet
                scripts_git "$dir" log -1 --format='  %h %cs %s'
            elif [ "$dir" = "$SCRIPTS_DIR" ] && [ "$S_SCRIPTS" = tarball ]; then
                fetch_scripts tarball "$dir"
            else
                die "${dir} is not a checkout thinqctl can update."
            fi
            ok "Drivers updated; rusthinq reloads changed drivers when watch is on."
            ;;
        *) die "Unknown scripts action: $action (status, check, update)" ;;
    esac
}

stop_disable_unit() {
    systemctl stop "$1" 2>/dev/null || true
    systemctl disable "$1" 2>/dev/null || true
}

cmd_remove() {
    require_root remove
    detect_installation
    case "$I_KIND" in
        none|legacy) die "rusthinq is not installed. (Use 'purge' to clean up data if needed.)" ;;
        container) die "rusthinq runs in a container; thinqctl does not manage it." ;;
    esac
    if [ "$I_OWNED" -ne 1 ]; then
        [ -f "$STATE_FILE" ] || die "thinqctl does not manage this installation."
        confirm "Stop managing ${I_BIN} with thinqctl? The installation itself is left untouched." \
            || { log "Aborted."; return 0; }
        rm -f "$STATE_FILE"
        rm -rf "$BACKUP_DIR"
        ok "thinqctl no longer manages ${I_BIN}."
        [ "$S_UNIT_OWNED" != 1 ] || log "  ${UNIT_PATH} was written by thinqctl and keeps running it."
        return 0
    fi
    require_systemd
    confirm "Remove rusthinq? (data dir, CA, configuration and user preserved)" || { log "Aborted."; return 0; }
    stop_disable_unit "$SERVICE"
    rm -f "$UNIT_PATH"
    systemctl daemon-reload
    unlink_binaries
    rm -rf "$PREFIX" "$BACKUP_DIR"
    rm -f "$STATE_FILE"
    ok "Removed binaries and unit. Preserved: ${DATA_DIR}, user ${SVC_USER}."
    log "  'sudo thinqctl install' reuses the configuration; 'sudo thinqctl purge' deletes it."
}

cmd_purge() {
    require_root purge
    require_systemd
    detect_installation
    case "$I_KIND" in
        systemd|manual)
            die "thinqctl did not create this installation and does not delete it. Use: sudo thinqctl remove" ;;
        container) die "rusthinq runs in a container; thinqctl does not manage it." ;;
        none|legacy)
            [ -e "$DATA_DIR" ] || [ -e "$PREFIX" ] || [ -e "$UNIT_PATH" ] || id -u "$SVC_USER" >/dev/null 2>&1 \
                || die "Nothing to purge." ;;
    esac
    local drivers
    drivers="$(config_drivers_dir "$CONFIG_FILE")"

    cat <<EOF

${C_YELLOW}PURGE permanently deletes:${C_RESET}
  • unit         ${UNIT_PATH}
  • binaries     ${PREFIX}/ and the links in ${LINK_DIR}
  • helper       ${SELF_PATH}
  • data dir     ${DATA_DIR}/   (configuration, CA, device ledger, LG account)
  • user         ${SVC_USER}
EOF
    if [ -n "$drivers" ] && [[ "$drivers" != "$DATA_DIR"/* ]]; then
        printf '  Kept: the driver directory %s\n' "$drivers"
    fi
    printf '\n'
    warn "Appliances trust the CA in ${DATA_DIR}. Without a copy of ca.key and ca.cert every appliance must be set up again."
    printf '\n'
    confirm_word "Proceed with PURGE?" purge || { log "Aborted."; return 0; }

    stop_disable_unit "$SERVICE"
    rm -f "$UNIT_PATH"
    systemctl daemon-reload || true
    unlink_binaries
    rm -rf "$PREFIX" "$DATA_DIR" "$BACKUP_DIR"
    rm -f "$STATE_FILE" "$SELF_PATH"
    if id -u "$SVC_USER" >/dev/null 2>&1; then
        userdel "$SVC_USER" 2>/dev/null || warn "Could not remove user '${SVC_USER}' (still has running processes?)."
    fi
    ok "Purged rusthinq and its data."
}

cmd_help() {
    cat <<EOF
${C_BOLD}thinqctl${C_RESET} — install and manage rusthinq as a systemd service.

${C_BOLD}USAGE${C_RESET}
  thinqctl [status]               Installation, service and version state (default)
  sudo thinqctl install [opts]    Adopt the running rusthinq where it is, migrate a running
                                  0.1, or install afresh with a few questions
  sudo thinqctl upgrade [opts]    Replace the binaries with the selected release
  sudo thinqctl rollback          Restore the binaries the last upgrade replaced
  thinqctl scripts [status|check] Show drivers or compare HEAD with the remote upstream
  sudo thinqctl scripts update    Fast-forward the driver checkout (preserves local changes)
  thinqctl self check             Compare thinqctl with master
  sudo thinqctl self update       Update thinqctl itself without restarting services
  sudo thinqctl remove  [opts]    Remove binaries and unit, keep data; an adopted
                                  installation is only forgotten
  sudo thinqctl purge   [opts]    Also delete data, CA and user (thinqctl installs only)
  thinqctl help                   Show this help

${C_BOLD}OPTIONS${C_RESET}
  --yes, -y          Answer every question with its default (RUSTHINQ_* variables set
                     the defaults). Never switches a running 0.1 over to 0.2.
  --version VERSION  An exact release, e.g. 0.2.0 or v0.2.0 (install, upgrade).
  --prerelease       Include pre-releases (status, install, upgrade).

${C_BOLD}LISTENERS${C_RESET} (fresh installs; numeric IP:port, IPv6 in brackets)
  RUSTHINQ_HTTPS_BIND          0.0.0.0:443
  RUSTHINQ_MQTT_BIND           0.0.0.0:8883 (device listener, not external broker)
  RUSTHINQ_THINQ1_HTTPS_BIND   0.0.0.0:46030
  RUSTHINQ_THINQ1_BIND         0.0.0.0:47878
  RUSTHINQ_MANAGEMENT_BIND    0.0.0.0:44401 (RUSTHINQ_GUI_PORT sets default port)
  Checks require python3 and ss (iproute2). --yes stops on unresolved conflicts.
  Changed device ports require matching appliance routing/port forwarding.

${C_BOLD}FILES${C_RESET} (fresh installs)
  Binaries: ${BIN_DIR}/  (rusthinq and rusthinqctl linked into ${LINK_DIR})
  Data:     ${DATA_DIR}/  (config.toml, CA, ledger; drivers in scripts/)
  Unit:     ${UNIT_PATH}
  State:    ${STATE_FILE}
  Backup:   ${BACKUP_DIR}/

${C_BOLD}FIRST INSTALL${C_RESET}
  curl -fsSL ${SELF_URL} \\
    | sudo bash -s -- install
EOF
}

# ── Argument parsing ───────────────────────────────────────────────────────────
main() {
    local subcmd="" subarg=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --yes|-y)     ASSUME_YES=1 ;;
            --prerelease) PRERELEASE=1 ;;
            --version)
                [[ "$#" -ge 2 && -n "$2" && "$2" != -* ]] \
                    || die "--version requires a release version (e.g. 0.2.0)."
                [ -z "$REQUESTED_VERSION" ] || die "--version may only be specified once."
                REQUESTED_VERSION="$2"
                shift
                ;;
            --version=*)
                [ -z "$REQUESTED_VERSION" ] || die "--version may only be specified once."
                REQUESTED_VERSION="${1#*=}"
                [ -n "$REQUESTED_VERSION" ] || die "--version requires a release version (e.g. 0.2.0)."
                ;;
            -h|--help)    cmd_help; exit 0 ;;
            -*)           die "Unknown flag: $1" ;;
            *)
                if [ -z "$subcmd" ]; then
                    subcmd="$1"
                elif [[ "$subcmd" = scripts || "$subcmd" = self ]] && [ -z "$subarg" ]; then
                    subarg="$1"
                else
                    die "Unexpected argument: $1"
                fi
                ;;
        esac
        shift
    done

    if [ -n "$REQUESTED_VERSION" ]; then
        REQUESTED_VERSION="${REQUESTED_VERSION#v}"
        [[ "$REQUESTED_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]] \
            || die "Invalid release version: $REQUESTED_VERSION (expected e.g. 0.2.0 or v0.2.0-dev6)."
        [ "$PRERELEASE" -eq 0 ] || die "--version cannot be combined with --prerelease."
        case "${subcmd:-status}" in
            install|upgrade) ;;
            *) die "--version is only supported for install and upgrade." ;;
        esac
    fi

    case "${subcmd:-status}" in
        status)   cmd_status ;;
        install)  cmd_install ;;
        upgrade)  cmd_upgrade ;;
        rollback) cmd_rollback ;;
        scripts)  cmd_scripts "$subarg" ;;
        self)     cmd_self "$subarg" ;;
        remove)   cmd_remove ;;
        purge)    cmd_purge ;;
        help)     cmd_help ;;
        *)        err "Unknown command: $subcmd"; cmd_help; exit 2 ;;
    esac
}

if [[ "${BASH_SOURCE[0]:-$0}" == "$0" ]]; then
    main "$@"
fi
