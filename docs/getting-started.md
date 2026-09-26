# Getting started

> **Draft.** Steps marked **(verify)** are written from the design and haven't been
> re-checked end to end on a clean machine.

rusthinq is a local stand-in for LG's ThinQ cloud. An appliance that connects to it
is driven locally and shows up on your MQTT broker; nothing needs to reach LG.

This guide goes from nothing to a device publishing on MQTT:

1. [What you need](#1-what-you-need)
2. [Install](#2-install)
3. [Configure](#3-configure)
4. [Run](#4-run)
5. [Register a device](#5-register-a-device)
6. [Check that it works](#6-check-that-it-works)
7. [Keep the official app working (bridge mode)](#7-keep-the-official-app-working-bridge-mode)
8. [Next steps](#8-next-steps)

## 1. What you need

- A Linux host with a **fixed IP address** on the same network as the appliances. A
  Raspberry Pi is enough.
- An **MQTT broker** (e.g. Mosquitto). rusthinq publishes device state there and takes
  commands from it; it doesn't embed a broker for consumers.
- The **OpenSSL CLI** on that host. rusthinq uses it to create its CA and sign device
  certificates.
- For an appliance that is already set up in the ThinQ app: a router or gateway you
  control that can do **DNAT** (port forwarding by source address), and root on it.
- For a factory-fresh appliance instead: a machine that can join the appliance's setup
  Wi-Fi (SoftAP). See [5b](#5b-appliance-not-yet-set-up-softap).

## 2. Install

Either download a release build, or build from source.

**Release build.** Run the *Release builds* workflow, or take a published archive, for
your platform (`linux-amd64`, `linux-aarch64`). It contains `rusthinq-cloud` and
`rusthinq-setup`, built with every feature (`bridge`, `scripting`, `gui`).
**(verify: archive names and where they're published)**

**From source.** Rust 1.88 or newer:

```bash
git clone https://github.com/3735943886/rusthinq.git
cd rusthinq
cargo build --release -p rusthinq-cloud --features bridge,scripting,gui -p rusthinq-setup
```

A plain `cargo build` enables no optional feature. Pick only what you need; see the
feature table in the [README](../README.md#build--run).

## 3. Configure

Copy `config.toml` from the repo next to the binary and edit it. Every option is
documented inline; these are the ones you have to think about.

```toml
hostname = "rusthinq.local"        # a name, not an IP address
advertise_requested_host = true    # needed for the "already set up" flow in 5a

[mqtt]
mqtt_url = "mqtt://localhost:1883"
mqtt_user = ""
mqtt_pass = ""
rusthinq_prefix = "rusthinq"       # all of rusthinq's topics live under this
```

Optional sections (each needs its Cargo feature and its section in the file, see the
[README](../README.md#build--run)):

```toml
[bridge]                           # keep the official app working, see step 7
storage_path = "./state"

[scripting]                        # drive a model with a .rhai script; the drivers are
rhai_dir = "./rusthinq-scripts"    # https://github.com/3735943886/rusthinq-scripts
watch = true

[gui]                              # web dashboard
gui_port = 44401
gui_user = "admin"                 # set these: the dashboard binds 0.0.0.0
gui_pass = "change-me"
```

Leave the ports alone (`https_port = 443`, `mqtts_port = 8883`) unless you know why.
Appliances expect those two, and other values can break compatibility.

The CA key and certificate (`ca.key`, `ca.cert`) are created on the first run.

## 4. Run

Try it in the foreground first:

```bash
./rusthinq-cloud ./config.toml
```

You should see the log say it is ready and connected to MQTT. Then make it a service.
Example unit:

```ini
# /etc/systemd/system/rusthinq-cloud.service
[Unit]
Description=rusthinq-cloud
After=network-online.target
Wants=network-online.target

[Service]
User=rusthinq
WorkingDirectory=/opt/rusthinq
ExecStart=/opt/rusthinq/rusthinq-cloud /opt/rusthinq/config.toml
Restart=always
RestartSec=2
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Binding ports 443 and 8883 as a non-root user needs
`AmbientCapabilities=CAP_NET_BIND_SERVICE` in the unit, or run rusthinq on higher ports
and forward to them (see the `https_port` examples in `config.toml`).
**(verify)**

## 5. Register a device

"Registering" means getting the appliance to connect to rusthinq instead of LG's cloud.
There are two ways, depending on where the appliance is now.

### 5a. Appliance already set up with the ThinQ app (recommended)

This flow has nothing in common with SoftAP provisioning. The appliance is never put
into setup mode and never leaves the state it is already in.

1. **Set the appliance up as normal, with the official ThinQ app.** It ends up
   registered to your account, which is where it stays.
2. **Start rusthinq with `advertise_requested_host = true`** (step 3), so `/route`
   leaves the appliance on a name it already resolves.
3. **Add DNAT rules for that appliance's address:** `tcp/443` and `tcp/8883`, to the
   rusthinq host. DNS is left alone, so the appliance goes on resolving the
   manufacturer's real addresses. On a Linux gateway:

   ```bash
   APPLIANCE=192.168.0.50      # the appliance's address
   RUSTHINQ=192.168.0.10       # the rusthinq host
   for port in 443 8883; do
     iptables -t nat -A PREROUTING -s $APPLIANCE -p tcp --dport $port \
       -j DNAT --to-destination $RUSTHINQ:$port
   done
   ```

   If the gateway and rusthinq are on the same subnet as the appliance, replies from
   rusthinq go straight back to the appliance without passing through the gateway, and
   the connection won't work. Add a masquerade rule on the gateway for this traffic, or
   put rusthinq on a different subnet. **(verify)**

4. **Break the appliance's existing connection to the cloud.** The nat table is
   consulted only for the first packet of a connection, and the appliance holds a
   long-lived MQTT session, so until that session drops it keeps talking straight past
   the new rule. Drop its conntrack entries (on the gateway):

   ```bash
   conntrack -D -s $APPLIANCE
   ```

   Power-cycling the appliance does the same.

5. **It reconnects and lands on rusthinq.** Nothing was changed on the appliance and
   nothing was stored on it that outlives the rule. Delete the DNAT rules and the
   appliance goes back to LG's cloud on its next reconnect.
6. **Turn on bridge mode** if you want the official app to keep working, and anything
   else attached to that account, while rusthinq drives the appliance locally. See
   [step 7](#7-keep-the-official-app-working-bridge-mode).

### 5b. Appliance not yet set up (SoftAP)

For an appliance that is in setup mode, `rusthinq-setup` joins it to your Wi-Fi and
points it at rusthinq without the ThinQ app:

```bash
# connect this machine to the appliance's setup Wi-Fi first, then:
rusthinq-setup 192.168.120.254 'MySSID' 'MyPassword!'
```

Quote the password. The exact setup-mode procedure differs by appliance; upstream's
[installation instructions](https://github.com/anszom/rethink/wiki/Installing-rethink‐cloud)
cover it, with `rethink-setup` there corresponding to `rusthinq-setup` here.

## 6. Check that it works

Subscribe to the device list (retained, so you get the current state immediately):

```bash
mosquitto_sub -h localhost -t 'rusthinq/devices' -v
```

The appliance should appear with `"online": true` and its `model`. Note its id: every
other topic is `rusthinq/<id>/...`. The dashboard shows the same list if you enabled
it (`http://<host>:44401`).

If it doesn't show up:

- Watch the log; `incoming` shows every message the appliance publishes.
- Confirm the DNAT rule matches (`iptables -t nat -L PREROUTING -n -v` counters
  increase) and that step 5a.4 really dropped the old connection.
- Confirm the appliance can reach the rusthinq host on 443 and 8883.

## 7. Keep the official app working (bridge mode)

Bridge mode forwards an appliance's traffic to LG's real cloud, so the official app
keeps working. It needs the `bridge` feature and a `[bridge]` section.

```bash
# 1. start the LG login (payload = country code; empty means US)
mosquitto_pub -h localhost -t 'rusthinq/bridge/login/set' -m 'US'

# 2. get the sign-in URL, open it in a browser, log in,
#    and copy the FINAL redirected URL
mosquitto_sub -h localhost -t 'rusthinq/bridge/login-url' -v

# 3. complete the login with that URL
mosquitto_pub -h localhost -t 'rusthinq/bridge/login/complete/set' -m '<redirected URL>'

# 4. turn bridging on for a device
mosquitto_pub -h localhost -t 'rusthinq/<id>/bridge/enable/set' -m ''
```

Login is once per LG account; credentials are stored under `storage_path`. Progress and
results appear on `rusthinq/bridge/status` and `rusthinq/<id>/bridge/status`. The
[MQTT cheat sheet](mqtt-control.md) has the rest, including turning bridging off.

The LG account is read every 15 minutes for the devices' names and for devices removed
from the account, so renaming a device in the official app shows up on the next read. A
fresh LG login or a rusthinq restart reads it immediately (enabling a device does not), and
a failed read is retried sooner with backoff. A read that lists no devices at all while some
are known is ignored unless the next one, a minute later, agrees.

## 8. Next steps

- **Using the data:** what an appliance's properties mean depends on how it's driven.
  See [the two ways to drive a device](../README.md#driving-a-device)
  (a Rhai driver, or a raw-frame consumer such as rusthinq-adapter).
- **Home Assistant or anything else:** it reads the same MQTT topics; rusthinq itself
  is consumer-neutral.
- **Everything you can do over MQTT:** [mqtt-control.md](mqtt-control.md).
