# rusthinq

De-clouds LG ThinQ-branded appliances: it talks to them directly, without the official
LG app/cloud, and exposes them over MQTT. An optional **bridge** mode can still forward
traffic to LG's real cloud, useful for reverse engineering or to keep the official app
working alongside this.

This is a Rust rewrite and independent continuation of
[anszom/rethink](https://github.com/anszom/rethink) (TypeScript/Node). Credit for the
original protocol reverse engineering, device research, and reference implementation
belongs to [Andrzej Szombierski](https://github.com/anszom) and
[upstream's contributors](https://github.com/anszom/rethink/graphs/contributors).

## What makes this different from upstream

- **Rust only.** No Node/TypeScript in the core daemon, no web UI, no management
  HTTP/WS server. The entire control surface is MQTT: device traffic, a retained
  `<rusthinq_prefix>/devices` snapshot for connected-device state, (optionally) a raw
  wire-frame tap/inject bus for RE tooling, and even LG-cloud bridge login/logout
  (`<rusthinq_prefix>/bridge/login/set` etc. — see `bridge_control.rs`) — no separate
  CLI or process needed for any of it.
- **Built to run 24/7 as a minimal, self-recovering daemon.** MQTT reconnects use
  exponential backoff (both to the local broker and to LG's cloud in bridge mode);
  a device handler panicking doesn't take down the process or other devices
  (`panic_guard`); a device dropping and reconnecting within a short grace period
  doesn't flap its published availability state; permanently-removed devices have
  their retained MQTT state cleaned up automatically.
- **The core has zero knowledge of any specific downstream integration.**
  `rusthinq-core` and `rusthinq-devices` only know "a device has properties and emits
  events" — nothing here hardcodes discovery topics or payload shapes for any
  particular consumer. What a device's data *means* to whatever's listening is
  entirely up to how you drive it (see below), which is also why this fork carries no
  device support list: whether a model works, and how well, depends on what handler you
  give it.
- **Four ways to drive a device, freely mixed per model:**
  1. **A native Rust handler.** `crates/rusthinq-devices/src/devices/` — the same style
     upstream device handlers use, ported into a small `DeviceHandler` trait. Requires
     a rebuild to add or change.
  2. **A `.rhai` script — no rebuild, no restart.** Drop a `<modelId>.rhai` file into
     `rhai_dir` (see `[devices]` in `config.toml`) and it starts handling that model on
     its next connection; with `watch = true`, editing and saving the script hot-reloads
     it into every already-connected device of that model within a couple hundred
     milliseconds (a script that fails to compile just leaves the previous version
     running — never worse than before you saved). Scripts get raw wire bytes
     in-process (no MQTT round trip), opt-in access to `rusthinq-util`'s TLV/CRC16/hex
     codec helpers, and publish through the same MQTT primitives a native handler uses.
     See `crates/rusthinq-devices/src/scripting/` for the engine, and
     `scripting::ctx` for the exact script-facing API.
  3. **Your own consumer, in whatever language you want.** Set `[mqtt] raw_prefix` and
     every connected device's raw rx/tx frames get tapped onto MQTT
     (`<raw_prefix>/<id>/raw/rx|tx`), with an inject topic to send frames back
     (`<raw_prefix>/<id>/raw/inject/set`). rusthinq still owns the TLS/socket/framing
     plumbing; whatever's on the other end of that bus — a Python script, a Node
     process, a one-off shell pipeline — sees the same bytes a native handler or a
     script would and can drive the device however it wants, with no Rust or Rhai
     involved at all. See `raw_bus.rs` and the `[mqtt]`/`[devices]` comments in
     `config.toml` for the wire shape.
  4. **Existing rethink TypeScript device driver, completely unmodified.** A pre-built
     instance of (3): the `rethink` repo's `rusthinq-adapter.ts` +
     `cloud/thinq2/rusthinq_transport.ts` plug the same raw bus in as a drop-in
     replacement for rethink's own MQTT broker connection, runs completely unchanged, as a
     separate long-lived process, with rethink's own `Connection`/`Bridge` still doing
     its own discovery and state publishing exactly as it always has. Useful for a model
     whose upstream driver is too involved to be worth re-writing in Rhai — or anything
     else — from scratch. See `rethink`'s `rusthinq-adapter-config.jsonc` for the config
     shape.

  **Don't combine a registry handler (1 or 2) with raw_bus used as another process's
  full driver (3 or 4) for the same model.** `raw_bus` taps every connected device
  unconditionally the moment `raw_prefix` is set, with no idea whether `registry.rs`
  also gave that model a native handler or a script — nothing arbitrates between them,
  and nothing warns you. Using raw_bus purely for *debugging* alongside (1) or (2)
  (watching a handler's real wire traffic, testing a command via inject before adding
  it to the script) is exactly what the raw bus is for and is fine. What isn't fine is
  a model where raw_bus is another process's *only* data source (3 or 4) also getting a
  native handler or a `.rhai` script from the registry (1 or 2): that's two
  independent, mutually-unaware full drivers for one physical device. See the
  `[devices]` comment in `config.toml` for the long-form version of this warning.

## Usage

Initial device setup (SoftAP adoption, or DNS/redirection for devices already paired to
LG), the MQTT topic shape a device publishes under, and how to point an MQTT-based
consumer at the result are all unchanged from upstream rethink — follow
**[upstream's installation instructions](https://github.com/anszom/rethink/wiki/Installing-rethink‐cloud)**
and treat `rethink-cloud`/`rethink-setup` there as `rusthinq-cloud`/`rusthinq-setup` here.
The [wiki](https://github.com/anszom/rethink/wiki) is also the best source for
protocol/device reverse-engineering notes in general.

## Build & run

Requirements: Rust **1.88+**, OpenSSL CLI (CA / device CSR signing).

```bash
cargo build -p rusthinq-cloud -p rusthinq-setup -p rusthinq-bridge -p rusthinq-tools
cargo test --workspace
cargo run -p rusthinq-cloud -- ./config.toml
cargo run -p rusthinq-setup -- 192.168.120.254 'MySSID' 'MyPassword!'
```

Package names above are Cargo's `-p` selector; the binaries `cargo build` produces
under `target/release/` are named `rusthinq-*` — see the tables below.

`bridge` (optional forwarding to the real LG cloud — pulls in `reqwest`/`rsa`/oauth2) is
a default Cargo feature on `rusthinq-cloud`. A deployment that only ever runs
local/SoftAP-adopted devices against MQTT can drop it:

```bash
cargo build -p rusthinq-cloud --no-default-features
```

## Configuration

See `config.toml` in the repo root — every option is documented inline where it's
declared, including the `[devices]` Rhai-scripting section and the raw-bus/script
coexistence warning above. In short:

| Section | Purpose |
|---|---|
| top-level | hostname, TLS CA files, HTTPS/MQTTS port mapping, log filter |
| `[mqtt]` | broker URL/credentials, `rusthinq_prefix` (device state), `raw_prefix` (RE tap/inject bus, off by default), retained-state persistence path |
| `[bridge]` | LG-cloud forwarding storage path (only if the `bridge` feature is built) |
| `[devices]` | `rhai_dir` + hot-reload `watch` flag for `.rhai` scripts (absent entirely = scripting off) |

LG account login for bridge mode is over MQTT too, not a separate CLI: publish an LG
country code (or an empty payload for "US") to `<rusthinq_prefix>/bridge/login/set`,
the daemon publishes the LG sign-in URL to `<rusthinq_prefix>/bridge/login-url` — open
it in a browser, log in, and publish the final redirected URL to
`<rusthinq_prefix>/bridge/login/complete/set`. `<rusthinq_prefix>/bridge/logout/set`
clears stored credentials. Outcomes land on `<rusthinq_prefix>/bridge/status`; current
logged-in state is always visible in `<rusthinq_prefix>/devices`. See
`bridge_control.rs` for the exact topic list, including per-device enable/disable,
or **[`docs/mqtt-control.md`](docs/mqtt-control.md)** for a copy-pasteable
`mosquitto_pub`/`mosquitto_sub` cheat sheet covering device list/status,
bridge on/off, and login/logout.

### Workspace

| Crate | Role |
|---|---|
| `rusthinq-util` | Codecs (TLV, CRC16, framing, MTOSP) |
| `rusthinq-core` | Config, MQTT transport (consumer-neutral), ThinQ1/2 device traits |
| `rusthinq-devices` | Device protocol/state bases, native `modelId` handlers, and the Rhai scripting engine (`scripting/`) |
| `rusthinq-bridge` | Optional LG-cloud bridge helpers |
| `rusthinq-cloud` | Main server — binary `rusthinq-cloud` |
| `rusthinq-setup` | SoftAP provisioning CLI — binary `rusthinq-setup` |
| `rusthinq-tools` | RE/ops CLIs — see table below |

### Tools (`rusthinq-tools`)

| Binary | Purpose |
|---|---|
| `rusthinq-packet-parser` | Interpret TLV-formatted packets from a device via MQTT |
| `rusthinq-packet-sender` | Build & send TLV-formatted packets to a device via MQTT |
| `rusthinq-capture` | Record a device's live wire traffic (+ time-aligned LG-cloud notifications) to an LLM-friendly JSONL file |
| `rusthinq-mcp` | [MCP](https://modelcontextprotocol.io) server exposing decode/encode/enumerate/capture/inject to an LLM agent |
| `rusthinq-lgcloud-monitor` | Connect to the real LG cloud like the official app and watch its live device notifications |

No management HTTP port anywhere — all of the above talk MQTT to the running
`rusthinq-cloud` on `mqtt.mqtt_url`.

### Adding a device

Pick whichever of the three approaches above fits the device, then:

- **Native handler:** implement `crates/rusthinq-devices/src/devices/<module>.rs`
  (tests in the same file), add one line to the `register_devices!` table in
  `devices/mod.rs`, rebuild.
- **Rhai script:** drop `<modelId>.rhai` in `rhai_dir`; no rebuild, and no restart if
  `watch = true`.
- **Existing TS driver via raw bus:** point `rethink`'s `rusthinq-adapter.ts` at this
  daemon's `raw_prefix`/`rusthinq_prefix`; no changes on this side at all.

## Notice

LG ThinQ is used here for identification only. This project is not affiliated with LG.

## Warning

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
