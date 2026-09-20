# rusthinq

De-clouds LG ThinQ-branded appliances: it talks to them directly, without the official
LG app/cloud, and exposes them over MQTT. An optional **bridge** mode can still forward
traffic to LG's real cloud, useful for reverse engineering or to keep the official app
working alongside this.

This is a Rust rewrite of [anszom/rethink](https://github.com/anszom/rethink)
(TypeScript/Node), based on
[BluSyn/rethink's `rust-rewrite` branch](https://github.com/BluSyn/rethink/tree/rust-rewrite)
— an earlier Rust port of the same project. Credit for the original protocol reverse
engineering, device research, and reference implementation belongs to
[Andrzej Szombierski](https://github.com/anszom) and
[upstream's contributors](https://github.com/anszom/rethink/graphs/contributors); credit
for the initial Rust port belongs to [BluSyn](https://github.com/BluSyn).

## What makes this different from upstream

- **Rust only, MQTT is the one control plane.** Device traffic, a retained
  `<rusthinq_prefix>/devices` snapshot for connected-device state, (optionally) a raw
  wire-frame tap/inject bus for RE tooling, and even LG-cloud bridge login/logout (`<rusthinq_prefix>/bridge/login/set`
  etc. — see `bridge_control.rs`) all live on MQTT — no separate CLI or process needed
  for any of it. An optional web dashboard (`rusthinq-gui`, off by default — see
  [Web dashboard](#web-dashboard-optional) below) is bundled into the same binary for
  deploy convenience, but it's just another MQTT client talking the same topics any
  `mosquitto_pub`/`mosquitto_sub` invocation would — not a second control plane, and
  nothing the core daemon knows about.
- **Built to run 24/7 as a minimal, self-recovering daemon.** MQTT reconnects use
  exponential backoff (both to the local broker and to LG's cloud in bridge mode);
  a device handler panicking doesn't take down the process or other devices
  (`panic_guard`); a device dropping and reconnecting within a short grace period
  doesn't flap its published availability state. A device that stops connecting
  altogether keeps its retained MQTT state (in case it's coming back) but shows up
  in `<rusthinq_prefix>/devices` as `online: false` with a last-seen time, and stays
  there — nothing clears it automatically — until it's explicitly forgotten (see
  [Forgetting a device](docs/mqtt-control.md#forgetting-a-device-gone-for-good)).
- **The core has zero knowledge of any specific downstream integration.**
  `rusthinq-core` and `rusthinq-devices` only know "a device has properties and emits
  events" — nothing here hardcodes discovery topics or payload shapes for any
  particular consumer. What a device's data *means* to whatever's listening is
  entirely up to how it's driven (see below), which is also why this fork carries no
  device support list: whether a model works, and how well, depends on what handler
  it's given.
- **Four ways to drive a device, freely mixed per model:**
  1. **A native Rust handler.** `crates/rusthinq-devices/src/devices/` — the same style
     upstream device handlers use, ported into a small `DeviceHandler` trait. Requires
     a rebuild to add or change.
  2. **A `.rhai` script — no rebuild, no restart.** Drop a `<modelId>.rhai` file into
     `rhai_dir` (see `[scripting]` in `config.toml`) and it starts handling that model on
     its next connection; with `watch = true`, editing and saving the script hot-reloads
     it into every already-connected device of that model within a couple hundred
     milliseconds (a script that fails to compile just leaves the previous version
     running — never worse than before the save). Scripts get raw wire bytes
     in-process (no MQTT round trip), opt-in access to `rusthinq-util`'s TLV/CRC16/hex
     codec helpers, and publish through the same MQTT primitives a native handler uses.
     See `crates/rusthinq-devices/src/scripting/` for the engine, and
     `scripting::ctx` for the exact script-facing API.
  3. **A custom consumer, in any language.** Setting `[mqtt] raw_prefix` taps every
     connected device's raw rx/tx frames onto MQTT
     (`<raw_prefix>/<id>/raw/rx|tx`, plus `raw/clip/rx|tx` for the CLIP layer), with an
     inject topic to send frames back (`<raw_prefix>/<id>/raw/inject/set`). Which
     streams exist is listed in `[mqtt] raw` (off unless listed; see `config.toml`). rusthinq still owns the TLS/socket/framing
     plumbing; whatever's on the other end of that bus — a Python script, a Node
     process, a one-off shell pipeline — sees the same bytes a native handler or a
     script would and can drive the device however it needs to, with no Rust or Rhai
     involved at all. See `raw_bus.rs` and the `[mqtt]`/`[scripting]` comments in
     `config.toml` for the wire shape.
  4. **[rusthinq-adapter](https://github.com/3735943886/rusthinq-adapter): rethink's
     TypeScript device driver, unmodified.** A pre-built instance of (3): plugs the same
     raw bus in as a drop-in replacement for rethink's own MQTT broker connection, runs
     completely unchanged, as a separate long-lived process, with rethink's own
     `Connection`/`Bridge` still doing its own discovery and state publishing exactly as
     it always has. Useful for a model whose upstream driver is too involved to be worth
     re-writing in Rhai — or anything else — from scratch. See that repo's README for
     setup and its `rusthinq-adapter-config.jsonc` for the config shape.

  **A registry handler (1 or 2) should not be combined with raw_bus used as another
  process's full driver (3 or 4) for the same model.** `raw_bus` taps every connected
  device unconditionally the moment `raw_prefix` is set, with no idea whether
  `registry.rs` also gave that model a native handler or a script — nothing arbitrates
  between them, and nothing raises a warning. Using raw_bus purely for *debugging* alongside (1) or (2)
  (watching a handler's real wire traffic, testing a command via inject before adding
  it to the script) is exactly what the raw bus is for and is fine. What isn't fine is
  a model where raw_bus is another process's *only* data source (3 or 4) also getting a
  native handler or a `.rhai` script from the registry (1 or 2): that's two
  independent, mutually-unaware full drivers for one physical device. See the
  `[scripting]` comment in `config.toml` for the long-form version of this warning.

## Usage

**New here? Start with [`docs/getting-started.md`](docs/getting-started.md)** — install,
configure, register a device, check it on MQTT, and (optionally) keep the official app working.

Initial device setup (SoftAP adoption, or DNS/redirection for devices already paired to
LG), the MQTT topic shape a device publishes under, and how to point an MQTT-based
consumer at the result are all unchanged from upstream rethink — documented in
**[upstream's installation instructions](https://github.com/anszom/rethink/wiki/Installing-rethink‐cloud)**,
with `rethink-cloud`/`rethink-setup` there corresponding to `rusthinq-cloud`/`rusthinq-setup` here.
The [wiki](https://github.com/anszom/rethink/wiki) is also the best source for
protocol/device reverse-engineering notes in general.

## Build & run

Requirements: Rust **1.88+**, OpenSSL CLI (CA / device CSR signing).

```bash
cargo build -p rusthinq-cloud --features bridge,native,scripting -p rusthinq-setup -p rusthinq-bridge -p rusthinq-tools
cargo test --workspace
cargo run -p rusthinq-cloud --features bridge,native,scripting -- ./config.toml
cargo run -p rusthinq-setup -- 192.168.120.254 'MySSID' 'MyPassword!'
```

Package names above are Cargo's `-p` selector; the binaries `cargo build` produces
under `target/release/` are named `rusthinq-*` — see the tables below.

`rusthinq-cloud` ships **nothing extra by default** — a plain `cargo build -p
rusthinq-cloud` is a minimal build (local/SoftAP devices over MQTT only: no LG-cloud
bridge, no native/Rhai device handlers, no web dashboard). Additional functionality is
opted into with Cargo features. A feature that has its own config section needs
**both** to do anything: compiled in, *and* the section present in `config.toml` (absent
section = the feature stays off at runtime, even in a build that includes it):

| Feature | Config section | Adds | Off by default because |
|---|---|---|---|
| `bridge` | `[bridge]` | Forwarding to the real LG cloud (pulls in `reqwest`/`rsa`/oauth2) | Not every deployment talks to LG at all |
| `scripting` | `[scripting]` | Rhai `.rhai` device-scripting support (pulls in `rhai`/`notify`) | Only needed when driving a device via a script |
| `gui` | `[gui]` | The optional web dashboard (`rusthinq-gui`, see [below](#web-dashboard-optional)) | Not every deployment wants a dashboard |
| `native` | none | Built-in Rust device handlers (`rusthinq-devices/native`) | Gates no dependencies today (upstream handlers haven't been ported yet), kept for symmetry with `scripting` |

```bash
# everything (what release.yml's published binaries are built with)
cargo build -p rusthinq-cloud --features bridge,native,scripting,gui
```

## Configuration

See `config.toml` in the repo root — every option is documented inline where it's
declared, including the `[scripting]` Rhai-scripting section and the raw-bus/script
coexistence warning above. In short:

| Section | Required? | Purpose |
|---|---|---|
| top-level | **Required** | hostname, TLS CA files, HTTPS/MQTTS port mapping, log filter |
| `[mqtt]` | **Required** | broker URL/credentials, `rusthinq_prefix` (device state), `raw_prefix` (RE tap/inject bus, off by default), retained-state persistence path |
| `[bridge]` | Optional | LG-cloud forwarding storage path (requires the `bridge` feature) |
| `[scripting]` | Optional | `rhai_dir` + hot-reload `watch` flag for `.rhai` scripts (requires the `scripting` feature) |
| `[gui]` | Optional | `gui_port` + optional `gui_user`/`gui_pass` Basic Auth for the web dashboard (requires the `gui` feature) |

LG account login for bridge mode is over MQTT too, not a separate CLI: publishing an LG
country code (or an empty payload for "US") to `<rusthinq_prefix>/bridge/login/set`
makes the daemon publish the LG sign-in URL to `<rusthinq_prefix>/bridge/login-url`;
opening that URL in a browser, logging in, and publishing the final redirected URL to
`<rusthinq_prefix>/bridge/login/complete/set` completes the flow.
`<rusthinq_prefix>/bridge/logout/set` clears stored credentials. Outcomes land on
`<rusthinq_prefix>/bridge/status`; current
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
| `rusthinq-gui` | Optional web dashboard, talking to `rusthinq-cloud` only over MQTT |
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

No management HTTP port for any of the above — all of them talk MQTT to the running
`rusthinq-cloud` on `mqtt.mqtt_url`. The only HTTP port anywhere is the optional web
dashboard (`gui` feature + `[gui]` config, off by default) described next.

### Web dashboard (optional)

`rusthinq-gui` is a small Axum HTTP/WebSocket server that renders a browser dashboard
(device list — including known-but-offline devices, with a "forget this device"
action for one that's gone for good, a per-device raw wire-traffic monitor page, and —
only in a build with the `bridge` feature — per-device bridge enable/disable and LG
account login/logout) on top of the same MQTT
topics documented in [`docs/mqtt-control.md`](docs/mqtt-control.md). It's off by
default and needs both:

- the `rusthinq-cloud` binary built with `--features gui`, and
- a `[gui]` section in `config.toml` (see the commented-out example there):

  ```toml
  [gui]
  gui_port = 44401
  ```

If the config section is present but the binary wasn't built with the `gui` feature,
`rusthinq-cloud` logs a warning and ignores it rather than failing to start. The
dashboard runs its own independent MQTT connection to `mqtt.mqtt_url` — it isn't wired
into the daemon's device-handling code at all, so it sees (and can only do) exactly
what any other MQTT client subscribed to `<rusthinq_prefix>/#` could.

It binds `0.0.0.0` by default with **no authentication unless one is configured** —
setting `gui_user`/`gui_pass` requires matching HTTP Basic Auth on every request
(checked before any route runs, including the static assets):

```toml
[gui]
gui_port = 44401
gui_user = "admin"
gui_pass = "change-me"
```

Leaving either one unset, anyone who can reach `gui_port` can enable/disable bridging,
trigger LG account login/logout, and read raw device traffic — only reasonable on a
trusted LAN. `gui_port` also takes the same `{ bind, address }` table form as
`https_port`/`mqtts_port` (see [Configuration](#configuration) above), to
restrict it to one interface instead of every one this host has:

```toml
gui_port = { bind = 44401, address = "192.168.0.111" }
```