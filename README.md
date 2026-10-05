# rusthinq 0.2

rusthinq is a local LG ThinQ device server and optional LG cloud bridge written in
Rust. It accepts appliance connections, runs Rhai device drivers, and exposes a
management API, web dashboard and MQTT output.

## How 0.2 differs

| Area | TypeScript rethink | Rust 0.1 | Rust 0.2 |
| --- | --- | --- | --- |
| Structure | Object/event-driven server with device, cloud and Home Assistant components | Rust port organized around core, devices, cloud, bridge and GUI components | Protocol and lifecycle decisions separated from transport I/O, adapters and application orchestration |
| Device semantics | Built-in TypeScript model drivers and Home Assistant entity mappings | Rhai drivers and external raw-bus consumers; host also provides IL descriptors and property helpers | Rhai owns model semantics, descriptors, validation and integration output; host carries opaque publications |
| Management | Web management alongside built-in integrations | MQTT control/state topics plus optional GUI | HTTP/WebSocket API and GUI operate independently of external MQTT |
| Runtime ownership | Callbacks and event emitters connect components | Shared services and device callbacks connect components | Explicit task ownership, session generations and durable device identity fence stale work |
| Compatibility | Upstream implementation and model catalog | Archived migration source | Selectively reimplemented contracts with replay, integration and feature checks; full upstream parity is not claimed |

The comparison describes architectural boundaries, not model coverage. Upstream
references are [rethink](https://github.com/anszom/rethink) and its
[model/HA bridge](https://github.com/anszom/rethink/blob/30835bdb6eac3fc05c05ca824e29373bf9711d3e/cloud/ha_bridge.ts).

### Runtime boundaries

- **Protocol and lifecycle:** parsing, encoding and state transitions can be tested
  without sockets or an LG account.
- **Transport and adapters:** ThinQ1 TLS/XML, ThinQ2 device MQTT/provisioning, LG
  cloud communication and external MQTT handle I/O around those decisions.
- **Application:** owns devices, sessions, task shutdown, durable checkpoints and
  coordination between adapters, scripts and management. Session and script
  generations prevent obsolete connections or drivers from issuing current work.
- **Scripts:** translate appliance packets into properties and commands. IL and
  Home Assistant semantics belong here, rather than in the host. Drivers and IL
  helpers live in [rusthinq-scripts](https://github.com/3735943886/rusthinq-scripts).

This makes an integration change a script concern where the existing host APIs
cover it. The current MQTT command route is `{prefix}/{device}/{property}/set`;
see [operations](docs/0.2-operations.md) for its limits and delivery semantics.

### Workspace

| Crate | Responsibility |
| --- | --- |
| `rusthinq-protocol` | Device wire formats and protocol decisions |
| `rusthinq-lifecycle` | Device lifecycle, identity and generation transitions |
| `rusthinq-server` | Local device transports and provisioning services |
| `rusthinq-bridge` | LG account, cloud protocol and relay adapters |
| `rusthinq-scripting` | Rhai execution and generic host effects |
| `rusthinq-app` | Composed daemon, durable ownership, management, GUI and external MQTT |
| `rusthinq-tools` | CLI, MCP, capture/replay, setup and migration utilities |

## Build and run

```sh
cargo build --release -p rusthinq-app --bin rusthinq
cargo run --release -p rusthinq-app --bin rusthinq -- ./config.toml
```

A local configuration example:

```toml
thinq1_bind = "127.0.0.1:5502"
mqtt_bind = "127.0.0.1:8883"
https_bind = "127.0.0.1:8443"
hostname = "local.example"
ca_certificate = "ca.pem"
ca_key = "ca-key.pem"
device_ledger = "devices.json"
legacy_tls = false

[management]
bind = "127.0.0.1:44401"
gui = true
```

Prepare an existing valid CA and its private key before starting. Paths are
relative to the configuration file, and parent directories must exist. This
loopback example is for local use; connecting appliances also requires reachable
listeners and device hostname routing. Follow the [runtime guide](docs/0.2-runtime.md#runtime)
for endpoint configuration.

Open `http://127.0.0.1:44401/` for the dashboard. Its Devices, LG cloud, Activity
and System views share the management API. The [dashboard guide](docs/0.2-dashboard.md)
covers packet monitoring and analysis.

Add `drivers`, `external_mqtt` and `cloud_account` as needed using
[operations](docs/0.2-operations.md). External MQTT is optional: management and
local script-output observation continue without it. LG authentication is also
optional for local device service.

For deployment use `--release`; the [build guide](docs/0.2-runtime.md#build) explains
size optimization and debugging tradeoffs.

### Build variants

The default build enables `bridge`, `scripting` and `gui`.

```sh
# LG bridge and Rhai, without dashboard assets
cargo build --release -p rusthinq-app --bin rusthinq --no-default-features --features bridge,scripting

# Local transports and management, without LG bridge or Rhai
cargo build --release -p rusthinq-app --bin rusthinq --no-default-features
```

## Tools and diagnostics

```sh
cargo build --release -p rusthinq-tools --bins
```

The tool suite includes CLI management, an MCP server, device/cloud capture,
packet encode/decode, replay, SoftAP setup, migration and retained-output cleanup.
MCP supports live device observation through `device_start`, `read_device` and
`device_stop`, with bounded capture buffers, cursors, loss reporting and session
snapshots. See [tool migration](docs/0.2-tools-migration.md#tools-migration) for commands and
configuration.

Model-specific Rhai drivers, shared helpers and their tests are maintained in
`rusthinq-scripts`. This repository tests the Rust host and transport using synthetic
scripts and provides the runner for the scripts repository's CI. The runner accepts
the checkout being tested without requiring a fixed scripts revision.

```sh
# Run the device-driver compatibility suite from a separate scripts checkout
rusthinq-script-test /path/to/rusthinq-scripts

# Replay recorded receive packets through explicit management injection
rusthinqctl replay DEVICE CAPTURE.jsonl --inject-ok
```

Replay requires the management injection capability to be enabled. Command
admission, transport write and appliance acknowledgement are distinct results;
management events expose them separately where supported.

## Upgrading from 0.1

Read the [release notes](docs/0.2-tools-migration.md#release-notes),
[compatibility inventory](docs/0.2-validation.md#compatibility) and
[tool migration guide](docs/0.2-tools-migration.md#tools-migration) before switching runtimes.
Configuration, management contracts and script host APIs have changed; this is
not a drop-in binary replacement.

```sh
rusthinq-migrate OLD_CONFIG NEW_DIRECTORY
```

The migrator stages configuration and saved device/retained state without
modifying the 0.1 source. Follow the migration guide for validation and rollback.
Model porting and deployment packaging are deferred until the host is complete;
they are separate from the runtime architecture work.

The [documentation index](docs/README.md) separates operating guides, architecture
contracts and dated implementation evidence.

## Development and verification

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/check-0.2.sh
```

The check script runs formatting, workspace tests, strict Clippy, eight feature
combinations and GUI JavaScript checks. These checks do not establish live
appliance compatibility. [Refactoring](docs/0.2-runtime-implementation.md#refactoring),
[milestones](docs/0.2-validation.md#milestones) and the
[polling audit](docs/0.2-polling-audit.md) record implementation decisions,
verification evidence and outstanding work.

## Credits and license

Based on the LG ThinQ protocol work in
[anszom/rethink](https://github.com/anszom/rethink) by Andrzej Szombierski and
contributors, and the initial Rust port in
[BluSyn/rethink](https://github.com/BluSyn/rethink). 0.2 reorganizes that work into
an independent runtime. Licensed under GPL-2.0-or-later.
