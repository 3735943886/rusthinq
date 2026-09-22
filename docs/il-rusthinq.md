# Hosting the IL in rusthinq

rusthinq-specific notes for running drivers of the IL (a separate specification
repository). rusthinq does device → IL only: a driver is a Rhai script, and every consumer
of the IL (Home Assistant, Matter, …) is an external project.

Writing one? See [writing-a-driver.md](writing-a-driver.md) for the step-by-step.

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

A runtime error in a hook is logged and also published as a `script_error` event (`<hook>: <error>`), so a broken driver is visible over MQTT; a script that fails to compile publishes the `script_error` property once instead.

**Commands are validated by the host, not by each script.** When a script has published a
descriptor (`ctx.publish_il`), `ScriptedDevice::set_property` checks every `set` against it
before `on_set_property` runs: the property must exist and be writable, `requires` (a binary
that must be true, or a select whose value must be one of a list) must be satisfied by the
last value the script published for that property, and the value must fit its type (a `text`
must not be empty), options, `min`/`max` and `step` (see the Commands section of the IL). A
failure is a `reject` event (`{"prop": .., "code": .., "reason": ..}`, `code` being the IL's
`unknown_property`, `read_only`, `requires_unmet`, `invalid_value`, `out_of_range` or
`bad_step`) and nothing reaches the script; a reject a script publishes itself
(`ctx.publish_event("reject", ..)`, e.g. the appliance refused) gets the code `refused` unless
it names one; a valid value
reaches it in canonical form. A `set` that arrives retained is ignored (a command is an instruction for now, not state).
The host keeps the descriptor whether or not `il_prefix` is
set, so this does not depend on publishing. A script that publishes no descriptor is not
checked. A `set` for a device that is not connected at all is still dropped silently, because
the same `set` stream carries bridge control and raw injection that other handlers consume.

With `watch = true`, a saved, added or removed script is picked up while running: the new
code takes effect on already-connected devices at once, every device's descriptor is
published again (a reload does not re-run `start`, so a changed descriptor would otherwise
wait for a reconnect), and a connected device that had no script when it connected is given
one that has since appeared.

The host also keeps the `available` property honest, so a driver need not: when a descriptor
with an `available` role is published and the script has not reported it, the host publishes
`false`; the script's own `available` report replaces that. When the link drops, after
`on_drop` returns the host publishes every value the script had reported as absent (an empty
retained payload) and forgets it, except `available`, which stays as the script left it.

Timers are requests: the script never sleeps or spawns anything. The host arms one thread
per timer that holds only a weak reference to the device, and a re-armed or cancelled
timer never fires. `cancel_pending_work` / `drop_device` clear them all.

The washer / dryer / styler family shares `scripts/monitoring_common.rhai` (record parsing for the 0xEC / 0xEB / 0xE2 frames, command acknowledgements reported as rejections, the course a Start will ask for), which itself imports `aabb_common`.

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
and `source`, replaces `label` with the owner's name for the device when the ThinQ account has one (the driver's label is the fallback; a new or changed name re-publishes every descriptor), and adds the `x-mqtt` block pointing at the device's own
`<rusthinq_prefix>/<id>/<prop>` topics, because a driver does not know topics. Values and
commands keep using those existing topics, so the IL adds only the descriptor.

Forgetting a device (`<rusthinq_prefix>/<id>/forget/set`) also clears its retained descriptor at
`<il_prefix>/<id>`, so a consumer that follows the descriptors drops the device.

## Testing

A driver's tests are written in Rhai and live beside it, so a driver and its tests can be
kept apart from the Rust source (in their own repository, say): `scripts/tests/<Model>.test.rhai`
tests `scripts/<Model>.rhai`. Every zero-argument `test_*` function in it is one test, run
against a fresh device; it fails on the first failed `expect*` or runtime error. Run them with

```
cargo run -p rusthinq-devices --features scripting --bin rusthinq-script-test -- scripts
```

(or `cargo test`, which runs the same files and also checks that every driver has tests).

```rhai
import "tlv_test" as t;                       // helpers in scripts/tests/

fn caps() { "0000…" }                          // captured frames (functions: constants are not visible)
fn test_target_write_attaches_power_and_mode() {
    let d = t::running(device(), caps(), state());
    let n = d.sent().len();
    d.set("target", "45");
    expect_eq(t::sent_since(d, n), [[[0x253, 45], [0x1f7, 1], [0x1f9, 17]]]);
}
```

What a test can do: `device()` gives a fresh device running the driver; on it `start()`,
`drop_device()`, `feed(hex or blob)`, `set(prop, value)`, `fire(timer)` (no waiting), and, to
look at what happened, `property(name)`, `event(name)`, `script_error()`, `sent()` (frames sent,
as blobs), `sent_tlvs(i)`, `timers()`, `clips()`, `descriptor()`. Assertions are `expect(cond,
msg)`, `expect_eq(actual, expected[, msg])` and `expect_props(dev, #{prop: "value"})`. Everything a
driver can call (`hex_encode`, `tlv_frame_build`, `aabb_wrap`, …) is available to a test too.
`tests/tlv_test.rhai` and `tests/aabb_test.rhai` hold the helpers shared by the TLV and AABB
drivers. Tests run against frames captured from real appliances wherever there is one.

The host's own behaviour (timers, host-side command validation, the descriptor binding) is
tested in Rust with `ScriptHarness`, which is what the runner drives. `tests/il_vectors.rs`
runs the IL's `vectors/commands.json` through the validator when the `ildevice` checkout is
next to this one.

## Drivers

| model | script | status |
|---|---|---|
| DHUM_056905_WW (LG dehumidifier) | `scripts/DHUM_056905_WW.rhai` | tested against captured frames; not yet run against the live appliance |
| AIR_910604_WW (LG air purifier) | `scripts/AIR_910604_WW.rhai` | same |
| 1WPU4CIGCR__2 (LG water purifier, AABB) | `scripts/1WPU4CIGCR__2.rhai` | tested against real captured frames and the write frames the appliance accepted (both from rethink's test suite); not yet run live |
| D140110 (LG dishwasher, AABB, read-only) | `scripts/D140110.rhai` | tested against nine real frames of a full cycle (from rethink's test suite); not yet run live |
| WBEY3GT (LG cooktop, AABB) | `scripts/WBEY3GT.rhai` | tested against real frames and the command frames the LG app sent, byte for byte; not yet run live. Writes are rejected unless the panel has granted remote start, and no command lights a ring |
| Pd0F_F (LG mini washer, AABB monitoring record) | `scripts/Pd0F_F.rhai` | commands byte for byte as the LG app sent them (from rethink's test suite); status frames built from the documented offsets and the state the rethink adapter had retained, not yet checked against a live capture |
| RH14_N_KR (LG dryer, AABB monitoring record) | `scripts/RH14_N_KR.rhai` | tested against three real frames captured from the appliance while it ran a cycle (their previous records agree with what the rethink adapter had retained at that moment) and the start frame the LG app sent; not yet run live |
| S3BF_POD_DN4 (LG styler, AABB monitoring record) | `scripts/S3BF_POD_DN4.rhai` | tested against a real idle frame from the cabinet (energy and downloaded course agree with what the rethink adapter had retained) and the 46-byte Fine Dust start the LG app sent, byte for byte; there is no power-on command (measured: the cabinet acknowledges and ignores them); not yet run live |
| F24VDD (LG washer, AABB monitoring record) | `scripts/F24VDD.rhai` | tested against a real idle frame from the washer (energy, download course, Tub Clean count, last operating course and end sound agree with what the rethink adapter had retained) and the Colour Care, Heavy Duty and Steam Refresh starts the LG app sent, byte for byte; not yet run live |
| CST_570004_WW (LG ceiling-cassette air conditioner, TLV) | `scripts/CST_570004_WW.rhai` | written for this model only; tested against the real capability and state frames a unit reported (from rethink's test suite); write frames follow rethink's write-attach rules (power on and mode writes carry the other core tags), not yet compared with frames the LG app sent, and not yet run live |
| 2RSFL2DBN3K_Z (LG refrigerator, AABB) | `scripts/2RSFL2DBN3K_Z.rhai` | live against the real appliance, captured with `rusthinq-capture`: every property was read back after toggling the matching LG app control, including all three night-glare modes (off / sunset-to-sunrise / custom schedule). Fridge/freezer setpoint, express freeze, and AI Saving Mode (off/balanced/max, its own short F0 10 frame acked with an inner `0x67`, not the F0 17 template every other write here uses) were written from here (`rusthinq/<id>/<prop>/set`) and confirmed acked and reflected in the appliance's own next status frame; `ai_saving_max_schedule` (max mode's own active-hours window, same F0 10 frame) is decoded and reproduced byte-for-byte from a live nudge but never echoed by any status frame, so it publishes its own write back instead. Smart Care+, night-glare mode and the door-alarm-mute toggle are all confirmed writable at the protocol level too (night-glare via its own short F0 10 02 frame, which also carries a custom schedule's start/end time and LCD brightness, and for sunset-to-sunrise two bytes this driver could not pin down, none of it exposed) but are kept read-only here, as sensors rather than controls |

TLV drivers share `scripts/tlv_common.rhai` (`import "tlv_common" as c;`): the capability to values handshake with retries, the slow refresh, and write framing. A module cannot call back into its importer, so each device script keeps the hooks and delegates to it.

Do not run a driver alongside another consumer that already drives the same appliance
(for example the rusthinq-adapter): two unaware drivers would both write to it.
