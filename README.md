# rusthinq 0.2

rusthinq is a local LG ThinQ device server and optional LG cloud bridge written in
Rust. It accepts appliance connections, runs Rhai device drivers, and exposes a
management API, web dashboard and MQTT output.

## Architecture

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
| `rusthinq-task` | Bounded blocking task ownership and asynchronous drain |
| `rusthinq-protocol` | Device wire formats and protocol decisions |
| `rusthinq-lifecycle` | Device lifecycle, identity and generation transitions |
| `rusthinq-server` | Local device transports and provisioning services |
| `rusthinq-bridge` | LG account, cloud protocol and relay adapters |
| `rusthinq-driver-config` | Shared driver resource defaults and validation, independent of Rhai |
| `rusthinq-scripting` | Rhai execution and generic host effects |
| `rusthinq-app` | Composed daemon, durable ownership, management, GUI and external MQTT |
| `rusthinq-tools` | CLI, MCP, capture/replay and device setup utilities |

## Install as a service

[`scripts/thinqctl.sh`](scripts/thinqctl.sh) installs a release as a systemd service
on Linux (x86_64, aarch64, armv7) and copies itself to `/usr/local/bin/thinqctl`:

```sh
curl -fsSL https://raw.githubusercontent.com/3735943886/rusthinq/master/scripts/thinqctl.sh \
  | sudo bash -s -- install --prerelease
```

`install` first looks for a running rusthinq:

- **A running rusthinq** is adopted where it is. Its binary directory, configuration and
  data stay put; later upgrades only replace its binaries. A copy started outside
  systemd can get a unit, or keep being restarted by hand.
- **Nothing running** starts a fresh install. It asks for the hostname, an existing CA
  (or creates one), the LG bridge, MQTT, dashboard login and drivers
  ([rusthinq-scripts](https://github.com/3735943886/rusthinq-scripts)). The
  configuration comes from the generator rusthinq-docker uses
  ([`config-0.2.sh`](scripts/config-0.2.sh)).

A container installation is left to its image.

```sh
thinqctl                           # installation, service and version state
sudo thinqctl upgrade --prerelease # download and verify, then replace and restart
sudo thinqctl rollback             # restore the binaries the last upgrade replaced
thinqctl scripts check             # compare driver HEAD with the upstream branch
sudo thinqctl scripts update       # update drivers (requires a clean checkout)
thinqctl self check                # compare thinqctl itself with master
sudo thinqctl self update          # update thinqctl without restarting rusthinq
sudo thinqctl remove               # keep data and CA; an adopted install is only forgotten
sudo thinqctl purge                # also delete data, CA and user (thinqctl installs only)
```

`--yes` takes every default, and `RUSTHINQ_*` variables set those defaults. For example,
`sudo RUSTHINQ_HOSTNAME=thinq.lan thinqctl install --yes`. Until 0.2.0 is released,
only pre-releases exist, so pass `--prerelease`. `thinqctl help` lists every option.

Fresh installs show all five listener addresses and check for occupied ports and
conflicting selections before changing the system. You can customize all addresses,
or resolve a conflict by editing an address, retrying the check, or cancelling.
Checks require `python3` and `ss` (`iproute2`); an unavailable check is an error.
`--yes` stops on unresolved conflicts instead of choosing another port.

| Listener | Environment variable | Default |
| --- | --- | --- |
| Device HTTPS | `RUSTHINQ_HTTPS_BIND` | `0.0.0.0:443` |
| Device MQTT | `RUSTHINQ_MQTT_BIND` | `0.0.0.0:8883` |
| ThinQ1 HTTPS | `RUSTHINQ_THINQ1_HTTPS_BIND` | `0.0.0.0:46030` |
| ThinQ1 | `RUSTHINQ_THINQ1_BIND` | `0.0.0.0:47878` |
| Management/dashboard | `RUSTHINQ_MANAGEMENT_BIND` | `0.0.0.0:44401` |

Use numeric IP addresses, with brackets around IPv6 addresses (`[::1]:44401`).
`RUSTHINQ_GUI_PORT` still sets the default dashboard port. Changing a device port
requires matching appliance routing or port forwarding. Device MQTT is an inbound
listener, separate from the optional external MQTT broker connection.
Existing installations keep their configuration when adopted or upgraded.

Driver `scripts check` queries the remote upstream without fetching or changing the
checkout. `scripts update` uses a fast-forward pull and refuses local changes;
with driver watching enabled, rusthinq reloads the changes. Tarball installs support
updates but cannot report whether their current copy matches upstream.
`self check` compares script contents with master; `self update` downloads and
syntax-checks the replacement before installing it. These commands update only
thinqctl; use `upgrade` for the rusthinq binaries.

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
[operations](docs/0.2-operations.md).

Normal ThinQ2 commands can use `POST /api/devices/{id}/packet` (hex bytes) or
`POST /api/devices/{id}/clip` (`cmd`, `type`, `data`); the core generates `did`/`mid`.
These authenticated, session-scoped APIs work without Rhai or external MQTT and
are independent of diagnostic injection. See the [request examples and delivery
contract](docs/0.2-operations.md#generated-thinq2-commands).

External MQTT is optional: management and
local script-output observation continue without it. LG authentication is also
optional for local device service.

Daemon logs default to `info`; use `RUST_LOG` for diagnostics. See
[logging](docs/0.2-runtime.md#logging) for levels and systemd configuration.

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
packet encode/decode, replay, SoftAP setup and retained-output cleanup.
MCP supports live device observation through `device_start`, `read_device` and
`device_stop`, with bounded capture buffers, cursors, loss reporting and session
snapshots. See the [tools guide](docs/0.2-tools-migration.md#tools-migration) for commands and
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

The [documentation index](docs/README.md) separates operating guides, architecture
contracts and dated implementation evidence.

## Development and verification

GitHub Actions runs `scripts/check-0.2.sh` on pushes to `master` and on
pull requests targeting that branch. Local commits trigger CI after they are pushed.

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
