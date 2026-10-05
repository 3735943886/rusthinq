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
node --check crates/rusthinq-app/assets/controls.js
node --check crates/rusthinq-app/assets/cloud-feed.js
node --check crates/rusthinq-app/assets/ui.js
node --check crates/rusthinq-app/assets/panel.js
node --check crates/rusthinq-app/assets/monitor.js
if [[ -n "${RUSTHINQ_SCRIPTS_CHECKOUT:-}" ]]; then
    cargo run -p rusthinq-tools --bin rusthinq-script-test -- "$RUSTHINQ_SCRIPTS_CHECKOUT"
fi
