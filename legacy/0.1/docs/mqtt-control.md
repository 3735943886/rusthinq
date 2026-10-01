# MQTT control & monitoring cheat sheet

All examples assume the defaults in `config.toml`: broker at `localhost:1883`,
no auth, `rusthinq_prefix = "rusthinq"`. Adjust `-h`/`-p`/`-u`/`-P` and the
`rusthinq/` prefix to match your own `config.toml`.

`<id>` below is a real connected device's id (as shown in `rusthinq/devices`,
see below) — never the literal string `bridge`, which is reserved for the
account-level topics in this same shape.

## Device list / status snapshot

Retained topic, republished on every relevant change (connect/disconnect,
bridge enable/disable, login/logout). Subscribing gets you the current state
immediately, no request needed:

```bash
mosquitto_sub -h localhost -t 'rusthinq/devices' -v
```

Payload shape:

```json
{
  "mqtt": true,
  "bridgeLoggedIn": true,
  "devices": {
    "<id>": {
      "online": true,
      "model": "...",
      "modelName": "...",
      "deviceType": "...",
      "swVersion": "...",
      "platform": "thinq1 | thinq2",
      "mapped": true,
      "bridged": false
    },
    "<offline-id>": {
      "online": false,
      "lastSeenUnix": 1234567890
    }
  }
}
```

- `online`: whether this id has a live connection right now. An `online: false`
  entry is a device rusthinq has published properties for before but hasn't seen
  since — it carries only `lastSeenUnix` (unix seconds of its last publish), never
  `model`/`platform`/etc, since those are only ever known live. See
  [forgetting a device](#forgetting-a-device-gone-for-good) below to remove one.
- `mapped`: a local device-type handler is wired up for it (Rhai script / raw bus / etc). Only present for `online: true` entries.
- `bridged`: **live** — there's currently a forwarding session to the real LG cloud for it (see [bridge on/off semantics](#bridge-onoff-per-device) below). Only present for `online: true` entries.
- `bridgeLoggedIn`: `null` if the `bridge` feature wasn't built at all.

## Bridge on/off (per device)

Turn bridging **on** (registers with LG cloud if not already paired, or
resumes from saved pairing state, then starts forwarding):

```bash
# empty payload = use the device type it already reported itself
mosquitto_pub -h localhost -t 'rusthinq/<id>/bridge/enable/set' -m ''

# or force a specific LG device type
mosquitto_pub -h localhost -t 'rusthinq/<id>/bridge/enable/set' -m 'DEVICE_AIR_CONDITIONER'
```

Turn bridging **off** — note this isn't a pause: it deletes the saved LG
pairing state and clears the "should auto-resume" flag, so the device won't
come back on reconnect/restart until you `enable` it again:

```bash
mosquitto_pub -h localhost -t 'rusthinq/<id>/bridge/disable/set' -m ''
```

Watch progress/result (non-retained — subscribe before or during the
enable/disable, not after):

```bash
mosquitto_sub -h localhost -t 'rusthinq/<id>/bridge/status' -v
# ... "pairing" / "enabled" / "disabled" / "enable failed" / "enable error: ..."
```

## LG account login/logout (account-level, not per-device)

These use the fixed literal `bridge` in place of `<id>` in the same topic
shape. Only needed once per LG account (credentials are persisted).

```bash
# 1. kick off login (payload = LG country code, empty defaults to "US")
mosquitto_pub -h localhost -t 'rusthinq/bridge/login/set' -m 'US'

# 2. get the sign-in URL
mosquitto_sub -h localhost -t 'rusthinq/bridge/login-url' -v
# open it in a browser, log in, and copy the FINAL redirected URL

# 3. complete login with that full redirected URL (its `code` query param is what's used)
mosquitto_pub -h localhost -t 'rusthinq/bridge/login/complete/set' \
  -m 'https://.../redirect?code=abcd1234&...'

# outcome ("logged in" / "login error: ...") lands on:
mosquitto_sub -h localhost -t 'rusthinq/bridge/status' -v
```

Logout (clears stored LG credentials **and detaches every live bridge
session** — every device's `bridged` flips to `false`):

```bash
mosquitto_pub -h localhost -t 'rusthinq/bridge/logout/set' -m ''
```

Current logged-in state doesn't need its own poll topic — it's always in
`rusthinq/devices` as `bridgeLoggedIn`.

## Forgetting a device (gone for good)

For a device that's never coming back (thrown away, factory-reset, replaced) —
listed in `rusthinq/devices` as `"online": false` with a `lastSeenUnix` that's
only getting older. This clears its retained MQTT state (so it drops out of
`rusthinq/devices` entirely) and, if the `bridge` feature is built, its saved LG
pairing state too — same effect as `bridge/disable/set`, plus the MQTT cleanup.
Works whether or not the device is currently connected.

```bash
mosquitto_pub -h localhost -t 'rusthinq/<id>/forget/set' -m ''

# outcome:
mosquitto_sub -h localhost -t 'rusthinq/<id>/forget/status' -v
```

## Quick reference

| Topic | Direction | Payload | Effect |
|---|---|---|---|
| `rusthinq/devices` | published (retained) | JSON snapshot | connection/mapping/bridge state for every device |
| `rusthinq/<id>/bridge/enable/set` | subscribed | LG device type, or empty | pair (if needed) + start forwarding that device to LG |
| `rusthinq/<id>/bridge/disable/set` | subscribed | ignored | stop forwarding, erase saved pairing, no auto-resume |
| `rusthinq/<id>/bridge/status` | published | text | enable/disable progress and outcome |
| `rusthinq/bridge/login/set` | subscribed | country code, empty = "US" | start LG OAuth login |
| `rusthinq/bridge/login-url` | published | URL | LG sign-in URL to open in a browser |
| `rusthinq/bridge/login/complete/set` | subscribed | full redirected URL | finish LG OAuth login |
| `rusthinq/bridge/logout/set` | subscribed | ignored | clear LG credentials, detach every bridge session |
| `rusthinq/bridge/status` | published | text | login/logout outcome |
| `rusthinq/<id>/forget/set` | subscribed | ignored | clear retained MQTT state (and the retained IL descriptor, if `il_prefix` is set; + saved bridge pairing, if built) for an id, live or not |
| `rusthinq/<id>/forget/status` | published | text | forget outcome |

Source of truth for the exact topic strings and payload handling:
`crates/rusthinq-cloud/src/bridge_control.rs` (doc comment at the top),
`crates/rusthinq-cloud/src/device_control.rs`, and
`crates/rusthinq-cloud/src/devlist.rs`.
