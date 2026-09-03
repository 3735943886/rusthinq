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
      "model": "...",
      "modelName": "...",
      "deviceType": "...",
      "swVersion": "...",
      "platform": "thinq1 | thinq2",
      "mapped": true,
      "bridged": false
    }
  }
}
```

- `mapped`: a local device-type handler is wired up for it (Rhai script / raw bus / etc).
- `bridged`: **live** — there's currently a forwarding session to the real LG cloud for it (see [bridge on/off semantics](#bridge-onoff-per-device) below).
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

Source of truth for the exact topic strings and payload handling:
`crates/rusthinq-cloud/src/bridge_control.rs` (doc comment at the top) and
`crates/rusthinq-cloud/src/devlist.rs`.
