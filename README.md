# rusthinq 0.2

The workspace contains the independent 0.2 runtime. The fixed 0.1 reference is
archived in `legacy/0.1`; migration is still in progress.

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p rusthinq-app --bin rusthinq -- /path/to/config.toml
```

Configure TLS device endpoints, an existing CA and durable device inventory using
[the runtime guide](docs/0.2-runtime.md). Optional management, real Rhai drivers and
external MQTT are documented in [operations](docs/0.2-operations.md).
The management API and dashboard work independently of the external broker.

rusthinq is IL-agnostic: it carries device and cloud traffic and runs Rhai drivers,
whose output it routes without interpreting. Drivers and any IL output live in
[rusthinq-scripts](https://github.com/3735943886/rusthinq-scripts) (`il_common.rhai`).

Upgrading from 0.1: read the [release notes](docs/0.2-release-notes.md) and the
[compatibility inventory](docs/0.2-compatibility.md). `scripts/check-0.2.sh` runs the
full local check (format, tests, Clippy, the eight feature combinations, GUI JavaScript).

[Milestones](docs/0.2-milestones.md) record completed evidence and remaining work.
Real-appliance/LG-account validation is excluded at the user's request; unfinished
implementation items remain visible and are not recorded as completed validation.

Build CLI, MCP, capture, packet analysis, SoftAP setup and migration tools with
`cargo build -p rusthinq-tools --bins`. Their replacements and rollback procedure
are in [tool migration](docs/0.2-tools-migration.md). Run the recorded upstream
Rhai compatibility suite using `rusthinq-script-test /path/to/rusthinq-scripts`.

`rusthinq-migrate OLD_CONFIG NEW_DIRECTORY` stages saved device/retained state
without modifying 0.1. `rusthinqctl replay DEVICE CAPTURE.jsonl --inject-ok`
replays validated receive packets through session-scoped management injection.
