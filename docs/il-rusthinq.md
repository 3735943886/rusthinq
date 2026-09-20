# Hosting the IL in rusthinq

rusthinq-specific notes for running drivers of the IL (a separate specification
repository). rusthinq does device → IL only: a driver is a Rhai script, and every consumer
of the IL (Home Assistant, Matter, …) is an external project.

## What a driver is here

A `<modelId>.rhai` script in `[scripting] rhai_dir`, using the existing hooks. The IL's
inputs and outputs map onto them:

| IL | script |
|---|---|
| `Connected` | `start(ctx)` |
| `Frame` | `on_data(ctx, data)` |
| `Command { prop, value }` | `on_set_property(ctx, prop, value)` |
| `Timer(name)` | `on_timer(ctx, name)` |
| `Disconnected` | `on_drop(ctx)` |
| `Descriptor` | `ctx.publish_il(json)` from `publish_config(ctx)` |
| `Value` | `ctx.publish_property(prop, text)` |
| `SendFrame` | `ctx.send_raw(bytes)` |
| `SendMessage` | `ctx.send_clip(cmd, type, data_json)` (T2) / `ctx.send_json(text)` (T1) |
| `SetTimer` / `CancelTimer` | `ctx.set_timer(name, ms)` / `ctx.cancel_timer(name)` |
| `Reject` | `ctx.publish_event("reject", json)` |

Timers are requests: the script never sleeps or spawns anything. The host arms one thread
per timer that holds only a weak reference to the device, and a re-armed or cancelled
timer never fires. `cancel_pending_work` / `drop_device` clear them all.

AABB drivers share `scripts/aabb_common.rhai` (`import "aabb_common" as c;`): frame check, name/flag/bit helpers, reject.

Pure helpers for AABB devices: `aabb_wrap(inner)` (array or blob) and `aabb_unwrap(frame)` (`()` if not `AA..BB`; the checksum is not validated, as elsewhere).

Pure helpers for TLV devices: `tlv_frame_parse(bytes)` (returns the tag list of a standard
state frame, or `()`) and `tlv_frame_build(header, tlvs)` (header and CRC included).

## Configuration

```toml
[scripting]
rhai_dir = "./scripts"
il_prefix = "il"      # unset (the default) = descriptors are not published
```

With `il_prefix` set, `ctx.publish_il` publishes the descriptor retained at
`<il_prefix>/<id>`. The driver supplies only the device-neutral part; the host fills `id`
and `source` and adds the `x-mqtt` block pointing at the device's own
`<rusthinq_prefix>/<id>/<prop>` topics, because a driver does not know topics. Values and
commands keep using those existing topics, so the IL adds only the descriptor.

## Testing

`ScriptHarness` drives a script with frames and asserts on what it published or sent:
`pending_timers()`, `fire_timer(name)` (no waiting), `sent_raw()`, `sent_clip()`,
`property()`, `event()`, `raw_publish()`. The DHUM_056905_WW driver's tests run against
frames captured from a real appliance.

## Drivers

| model | script | status |
|---|---|---|
| DHUM_056905_WW (LG dehumidifier) | `scripts/DHUM_056905_WW.rhai` | tested against captured frames; not yet run against the live appliance |
| AIR_910604_WW (LG air purifier) | `scripts/AIR_910604_WW.rhai` | same |
| 1WPU4CIGCR__2 (LG water purifier, AABB) | `scripts/1WPU4CIGCR__2.rhai` | tested against real captured frames and the write frames the appliance accepted (both from rethink's test suite); not yet run live |
| D140110 (LG dishwasher, AABB, read-only) | `scripts/D140110.rhai` | tested against nine real frames of a full cycle (from rethink's test suite); not yet run live |

TLV drivers share `scripts/tlv_common.rhai` (`import "tlv_common" as c;`): the capability to values handshake with retries, the slow refresh, and write framing. A module cannot call back into its importer, so each device script keeps the hooks and delegates to it.

Do not run a driver alongside another consumer that already drives the same appliance
(for example the rusthinq-adapter): two unaware drivers would both write to it.
