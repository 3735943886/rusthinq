# rusthinq

De-clouds LG ThinQ appliances: it talks to them directly, without the official LG
app/cloud, and exposes them over MQTT. An optional **bridge** mode still forwards traffic
to LG's real cloud, so the official app keeps working alongside.

A Rust rewrite of [anszom/rethink](https://github.com/anszom/rethink), based on
[BluSyn's `rust-rewrite` branch](https://github.com/BluSyn/rethink/tree/rust-rewrite).
The protocol reverse engineering and reference implementation are
[Andrzej Szombierski's](https://github.com/anszom) and
[upstream's contributors'](https://github.com/anszom/rethink/graphs/contributors); the
initial Rust port is [BluSyn's](https://github.com/BluSyn).

- **MQTT is the only control plane.** Device state, a retained `<rusthinq_prefix>/devices`
  snapshot, the optional raw wire-frame bus, and LG bridge login/logout are all MQTT
  topics: no separate CLI or process.
- **Built to run 24/7.** Reconnects back off exponentially (to the broker and to LG's
  cloud), a panicking device handler is contained (`panic_guard`), and a short reconnect
  doesn't flap a device's availability. A device that stops connecting keeps its retained
  state and shows as `online: false` until it is
  [forgotten](docs/mqtt-control.md#forgetting-a-device-gone-for-good).
- **Consumer-neutral core.** It knows only "a device has properties and emits events":
  no discovery topics or payload shapes for any particular consumer, and no built-in
  device list. What a model does depends on the driver it is given.

## Adding a device

**Recommended: register it in the ThinQ app as usual, then redirect it with DNAT.**

1. Set the appliance up with the official ThinQ app. Nothing about it changes.
2. Run rusthinq with `advertise_requested_host = true`.
3. On your router, DNAT the appliance's `tcp/443` and `tcp/8883` to the rusthinq host
   (DNS stays untouched):

   ```bash
   APPLIANCE=192.168.0.50; RUSTHINQ=192.168.0.10     # their addresses
   for port in 443 8883; do
     iptables -t nat -A PREROUTING -s $APPLIANCE -p tcp --dport $port \
       -j DNAT --to-destination $RUSTHINQ:$port
   done
   ```
4. Drop its existing connection (`conntrack -D -s $APPLIANCE`, or power-cycle it). It
   reconnects to rusthinq.

Delete the rules and it goes back to LG's cloud. Enable bridge mode to keep the official
app working. Details and troubleshooting: [getting-started §5a](docs/getting-started.md#5a-appliance-already-set-up-with-the-thinq-app-recommended).

A factory-fresh appliance in setup mode can be provisioned without the app by
`rusthinq-setup` (SoftAP): [§5b](docs/getting-started.md#5b-appliance-not-yet-set-up-softap).

**New here?** [docs/getting-started.md](docs/getting-started.md) goes from install to a
device publishing on MQTT.

## Driving a device

Two ways, freely mixed per model:

1. **A Rhai driver** (`<modelId>.rhai` in `[scripting] rhai_dir`): no rebuild, no restart;
   with `watch = true` a saved script hot-reloads into connected devices. Scripts get raw
   wire bytes in-process and publish through the same MQTT primitives as the host (API:
   `scripting::ctx`). The drivers for specific models are in
   [rusthinq-scripts](https://github.com/3735943886/rusthinq-scripts); to write one, see
   [docs/il-rusthinq.md](docs/il-rusthinq.md) and that repository's
   [writing guide](https://github.com/3735943886/rusthinq-scripts/blob/master/docs/writing-a-driver.md).
2. **A raw-bus consumer in any language.** With `[mqtt] raw_prefix` set, connected
   devices' frames appear on `<raw_prefix>/<id>/raw/rx|tx` (plus `raw/clip/rx|tx`), and
   `<raw_prefix>/<id>/raw/inject/set` sends one back; which streams exist is listed in
   `[mqtt] raw`. [rusthinq-adapter](https://github.com/3735943886/rusthinq-adapter) is a
   ready-made consumer that runs rethink's TypeScript drivers unmodified.

Don't give one model both a Rhai driver and a raw-bus consumer that is its only driver:
nothing arbitrates, so two unaware drivers would interpret the same device. Using the raw
bus alongside a script to debug it is fine (see the `[scripting]` comment in `config.toml`).

## Build & run

Requires Rust **1.88+** and the OpenSSL CLI (CA and device certificate signing).

```bash
cargo build -p rusthinq-cloud --features bridge,scripting -p rusthinq-setup -p rusthinq-tools
cargo run -p rusthinq-cloud --features bridge,scripting -- ./config.toml
cargo test --workspace
```

A plain `cargo build -p rusthinq-cloud` is minimal (local devices over MQTT only). Optional
features need both the Cargo feature and their section in `config.toml`; a missing section
leaves the feature off even in a build that includes it. Release builds use all three:

| Feature | Section | Adds |
|---|---|---|
| `bridge` | `[bridge]` | Forwarding to the real LG cloud |
| `scripting` | `[scripting]` | Rhai device drivers |
| `gui` | `[gui]` | The [web dashboard](#web-dashboard-optional) |

## Configuration

`config.toml` documents every option inline. In short:

| Section | Purpose |
|---|---|
| top-level (required) | hostname, TLS CA files, HTTPS/MQTTS ports, log filter |
| `[mqtt]` (required) | broker URL and credentials, `rusthinq_prefix` (device state), `raw_prefix` (raw bus, off by default) |
| `[bridge]` | LG-cloud forwarding storage path |
| `[scripting]` | `rhai_dir`, hot-reload `watch` |
| `[gui]` | dashboard `gui_port` and optional Basic Auth |

LG account login for bridge mode is over MQTT too ([getting-started §7](docs/getting-started.md#7-keep-the-official-app-working-bridge-mode)).
[docs/mqtt-control.md](docs/mqtt-control.md) is a `mosquitto_pub`/`mosquitto_sub` cheat
sheet for the device list, bridge on/off and login/logout.

## Crates and tools

| Crate | Role |
|---|---|
| `rusthinq-util` | Codecs (TLV, CRC16, framing, MTOSP) |
| `rusthinq-core` | Config, MQTT transport, ThinQ1/2 device traits |
| `rusthinq-devices` | Device bases, `modelId` lookup, the Rhai engine |
| `rusthinq-bridge` | LG-cloud bridge helpers |
| `rusthinq-gui` | Web dashboard (MQTT only) |
| `rusthinq-cloud` | The server binary |
| `rusthinq-setup` | SoftAP provisioning CLI |
| `rusthinq-tools` | The RE/ops CLIs below |

| Tool | Purpose |
|---|---|
| `rusthinq-packet-parser` / `-sender` | Interpret / build TLV packets for a device via MQTT |
| `rusthinq-capture` | Record a device's live traffic to a JSONL file |
| `rusthinq-mcp` | [MCP](https://modelcontextprotocol.io) server: decode, encode, capture, inject |
| `rusthinq-lgcloud-monitor` | Watch the real LG cloud's live notifications like the official app |
| `rusthinq-script-test` | Run a driver directory's tests and checks (`rusthinq-devices`, `scripting` feature) |

All of them talk MQTT to the running `rusthinq-cloud`; there is no management HTTP port.

## Web dashboard (optional)

`rusthinq-gui` serves a browser dashboard (device list with offline devices and "forget",
a raw-traffic monitor per device, and, with the `bridge` feature, bridge enable/disable and
LG login) built on the same topics as [mqtt-control.md](docs/mqtt-control.md). It is a
separate MQTT client and knows nothing about the daemon's internals. Build with
`--features gui` and add:

```toml
[gui]
gui_port = 44401
gui_user = "admin"      # without these, anyone who can reach the port has full access
gui_pass = "change-me"
```

A `[gui]` section in a build without the feature is ignored with a warning. It binds
`0.0.0.0` by default; `gui_port = { bind = 44401, address = "192.168.0.111" }` restricts
it to one interface.
