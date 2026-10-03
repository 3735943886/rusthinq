#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all -- --check
cargo test --workspace --no-fail-fast -j 2
cargo clippy --workspace --all-targets -j 2 -- -D warnings
for rusthinq_features in '' bridge scripting gui bridge,scripting bridge,gui scripting,gui bridge,scripting,gui; do
    if [[ -z "$rusthinq_features" ]]; then
        cargo check -p rusthinq-app --no-default-features -j 2
    else
        cargo check -p rusthinq-app --no-default-features --features "$rusthinq_features" -j 2
    fi
done
node --check crates/rusthinq-app/assets/ui.js
node --check crates/rusthinq-app/assets/panel.js
node --check crates/rusthinq-app/assets/monitor.js
if [[ -n "${RUSTHINQ_SCRIPTS_CHECKOUT:-}" ]]; then
    rusthinq_revision=$(git -C "$RUSTHINQ_SCRIPTS_CHECKOUT" rev-parse HEAD)
    if [[ "$rusthinq_revision" != '54292921c6edc72ea6ec901b1137845bff14e6ae' ]]; then
        echo 'Driver checkout differs from recorded compatibility revision' >&2
        exit 1
    fi
    cargo run -p rusthinq-tools --bin rusthinq-script-test -- "$RUSTHINQ_SCRIPTS_CHECKOUT"
fi
