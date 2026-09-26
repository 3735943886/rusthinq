# Hosting the IL in rusthinq

rusthinq-specific notes for running drivers of the IL (a separate specification
repository). rusthinq does device → IL only: a driver is a Rhai script, and every consumer
of the IL (Home Assistant, Matter, …) is an external project.

The drivers themselves live in their own repository, [rusthinq-scripts](https://github.com/3735943886/rusthinq-scripts). Writing
one? See its [docs/writing-a-driver.md](https://github.com/3735943886/rusthinq-scripts/blob/main/docs/writing-a-driver.md) for the step-by-step.

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

Drivers share modules (`aabb_common`, `monitoring_common`, `tlv_common`, each imported with `import "<name>" as c;`) that sit beside them in the drivers repository.

Pure helpers for AABB devices: `aabb_wrap(inner)` (array or blob) and `aabb_unwrap(frame)` (`()` if not `AA..BB`; the checksum is not validated, as elsewhere).

Pure helpers for TLV devices: `tlv_frame_parse(bytes)` (returns the tag list of a standard
state frame, or `()`) and `tlv_frame_build(header, tlvs)` (header and CRC included).

## Configuration

```toml
[scripting]
rhai_dir = "./rusthinq-scripts"
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

A driver's tests are written in Rhai and live beside it in the drivers repository:
`tests/<Model>.test.rhai` tests `<Model>.rhai`. Every zero-argument `test_*` function in it is
one test, run against a fresh device; it fails on the first failed `expect*` or runtime error.
Run them, and the checks every driver must pass (a test file per driver, a descriptor well
formed against the IL), from this repository with

```
cargo run -p rusthinq-devices --features scripting --bin rusthinq-script-test -- <drivers dir>
```

```rhai
import "tlv_test" as t;                       // helpers in the drivers' tests/ directory

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
In the drivers repository, `tests/tlv_test.rhai` and `tests/aabb_test.rhai` hold the helpers shared by the TLV and AABB
drivers. Tests run against frames captured from real appliances wherever there is one.

The host's own behaviour (timers, host-side command validation, the descriptor binding) is
tested in Rust with `ScriptHarness`, which is what the runner drives. `tests/il_vectors.rs`
runs the IL's `vectors/commands.json` through the validator when the `ildevice` checkout is
next to this one.

## Drivers

The drivers, and what each was verified against, are listed in the
[rusthinq-scripts](https://github.com/3735943886/rusthinq-scripts#drivers) repository.
