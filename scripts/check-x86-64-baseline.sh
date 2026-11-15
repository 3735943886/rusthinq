#!/usr/bin/env bash
# Run the linux-amd64 release binaries on an emulated baseline x86-64 CPU (no AVX).
#
#   scripts/check-x86-64-baseline.sh BIN_DIR
#
# Build hosts may default to a newer ISA (GitHub's ubuntu-26.04 image ships gcc with
# an x86-64-v3 default), which a disassembly check cannot catch because OpenSSL's
# assembly legitimately carries AVX paths behind run-time CPU detection. So the
# daemon starts under qemu-user and serves one TLS handshake, which runs OpenSSL
# initialization, CA loading and certificate minting. Needs qemu-x86_64 and openssl.
# QEMU_CPU overrides the CPU model (e.g. max, as a positive control).
set -euo pipefail

bin_dir=$1
cpu=${QEMU_CPU:-qemu64}
port=${CHECK_PORT:-18443}
run=(qemu-x86_64 -cpu "$cpu")

work=$(mktemp -d)
pid=
cleanup() {
    [[ -z $pid ]] || kill "$pid" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

fail() {
    echo "check-x86-64-baseline: $*" >&2
    [[ ! -s $work/daemon.log ]] || sed 's/^/  daemon: /' "$work/daemon.log" >&2
    exit 1
}

# The tools must at least start; rusthinqctl decodes without a daemon.
"${run[@]}" "$bin_dir/rusthinqctl" decode 00 >/dev/null 2>&1 || [[ $? -lt 128 ]] \
    || fail "rusthinqctl crashed on CPU $cpu"
"${run[@]}" "$bin_dir/rusthinq-setup" --help >/dev/null 2>&1 || [[ $? -lt 128 ]] \
    || fail "rusthinq-setup crashed on CPU $cpu"

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 -subj '/CN=rusthinq check CA' \
    -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign' \
    -keyout "$work/ca.key" -out "$work/ca.cert" 2>/dev/null
cat >"$work/config.toml" <<EOF
hostname = "rusthinq.lan"
ca_key = "$work/ca.key"
ca_certificate = "$work/ca.cert"
device_ledger = "$work/devices.json"
https_bind = "127.0.0.1:$port"
EOF

"${run[@]}" "$bin_dir/rusthinq" "$work/config.toml" >"$work/daemon.log" 2>&1 &
pid=$!
for _ in $(seq 120); do
    grep -q 'device listeners started' "$work/daemon.log" && break
    kill -0 "$pid" 2>/dev/null || { wait "$pid" && status=0 || status=$?; pid=; fail "rusthinq exited ($status) on CPU $cpu"; }
    sleep 0.5
done
grep -q 'device listeners started' "$work/daemon.log" || fail "rusthinq did not start on CPU $cpu"

openssl s_client -connect "127.0.0.1:$port" -servername rusthinq.lan </dev/null >"$work/tls.log" 2>&1 \
    || { sed 's/^/  s_client: /' "$work/tls.log" >&2; fail "TLS handshake with rusthinq failed on CPU $cpu"; }
sleep 1
kill -0 "$pid" 2>/dev/null || { wait "$pid" && status=0 || status=$?; pid=; fail "rusthinq exited ($status) after a TLS handshake on CPU $cpu"; }
echo "rusthinq runs on CPU $cpu"
