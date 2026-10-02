function toastText(text) {
    const content = document.createElement('span')
    content.textContent = text
    M.toast({ html: content })
}
document.addEventListener('DOMContentLoaded', function () {
    M.Tooltip.init(document.querySelectorAll('.tooltipped'))
    M.Modal.init(document.querySelectorAll('.modal'))
    M.FormSelect.init(document.querySelectorAll('select'))
    M.Autocomplete.init(document.querySelectorAll('.autocomplete'), {
        data: {
            '101 (Refrigerator)': null,
            '201 (Washer)': null,
            '202 (Dryer)': null,
            '204 (Dishwasher)': null,
            '223 (WashTower)': null,
            '301 (Range)': null,
            '302 (Microwave)': null,
            '401 (Air Conditioner)': null,
        },
    })
})

let ws
let reconnectTimer
const STATUS_OK = `<i class="tiny material-icons green-text">check</i>`
const STATUS_ERROR = `<i class="tiny material-icons red-text">error</i>`
const STATUS_UNKNOWN = `<i class="tiny material-icons red-text">question_mark</i>`
// Not an error: the bridge feature is compiled in, but rusthinq-cloud's own [bridge]
// config section is absent, so there's no LG account to log into at all.
const STATUS_DISABLED = `<i class="tiny material-icons grey-text">block</i>`
let bridge_status = false
// Whether this build could possibly map *any* device (scripting compiled in)
// -- gates the per-device "no script handler" warning,
// which means nothing when it is off: every device would show it then,
// regardless of the device itself. Set from the first `features` snapshot.
let deviceMappingCapable = false
// Whether this build has the `bridge` Cargo feature at all -- there's no LG
// account to log into or session to toggle without it, so the bridge column
// and the "Bridge mode" section are dropped rather than shown disabled with
// nothing on the page explaining why. Set from the first `features` snapshot.
let bridgeFeatureEnabled = false

// Renders one link in the Browser<->rusthinq<->MQTT chain: `connected` true/false
// draws a matching pair -- a green "link" icon when up, a red "link_off" (broken
// chain) icon when not -- rather than an arrow character that would only change
// color; `null` (not known yet, e.g. before the very first status arrives) draws
// the same "unknown" icon the other status fields start as.
function setConnArrow(id, connected) {
    const el = get(id)
    if (connected === true) {
        el.innerHTML = `<i class="tiny material-icons green-text" style="vertical-align:middle">link</i>`
        el.className = 'conn-arrow conn-ok'
    } else if (connected === false) {
        el.innerHTML = `<i class="tiny material-icons red-text" style="vertical-align:middle">link_off</i>`
        el.className = 'conn-arrow conn-error'
    } else {
        el.innerHTML = `<i class="tiny material-icons red-text" style="vertical-align:middle">question_mark</i>`
        el.className = 'conn-arrow'
    }
}

setConnArrow('conn_ws', null)
setConnArrow('conn_mqtt', null)
get('status_bridge').innerHTML = STATUS_UNKNOWN
get('status_bridge_text').innerText = 'Unknown'

const devices = {}

const baseUrl = new URL(window.location)
baseUrl.search = ''
baseUrl.hash = ''

// Materialize appends a tooltip's floating .material-tooltip div to document.body, separate
// from the .tooltipped trigger it belongs to. Discarding the trigger (e.g. via replaceChildren)
// doesn't clean that div up, so every row rebuild without this leaked one into the body forever
// - each frozen at whatever position it last had, which is how they end up overflowing the page
// horizontally once the window is narrower than it was when a leaked tooltip was last shown.
function destroyTooltips(root) {
    for (const el of root.getElementsByClassName('tooltipped')) {
        M.Tooltip.getInstance(el)?.destroy()
    }
}

class DeviceEntry {
    constructor(id, remoteState, parent) {
        this.id = id
        this.remoteState = remoteState
        this.row = document.createElement('tr')
        this.updateDom()
        parent.appendChild(this.row)
    }

    destroy() {
        destroyTooltips(this.row)
        this.row.remove()
    }

    update(remoteState) {
        this.remoteState = remoteState
        this.updateDom()
    }

    updateDom() {
        destroyTooltips(this.row)
        const children = []

        let td
        // The owner's own name for the appliance, which only the bridge can know. Without it four
        // identical ceiling cassettes are four rows of the same model and a different UUID.
        td = document.createElement('td')
        td.className = 'dev-name'
        td.innerText = this.remoteState.name || '—'
        td.title = this.remoteState.name || '' // the cell is cut off on a narrow screen
        children.push(td)

        td = document.createElement('td')
        td.className = 'dev-id'
        td.innerText = this.id
        td.title = this.id
        children.push(td)

        if (this.remoteState.online === false) {
            // Known but not currently connected (see devlist.rs) -- there's nothing
            // but an id and a last-seen time for it, so there's no model/platform to
            // show and no bridge session to toggle. Offer to forget it instead of
            // the monitor link, which needs a live connection too.
            td = document.createElement('td')
            td.className = 'dev-model'
            td.colSpan = 3
            // Still paired: the bridge will auto-resume for this device the moment it
            // reconnects, with no action needed here -- worth saying so in the same
            // merged cell rather than adding a column just for this.
            const bridgeNote = this.remoteState.bridgePaired
                ? ` <i class="material-icons tiny tooltipped" data-position="bottom" data-tooltip="Still paired with the LG cloud -- bridging resumes automatically once this device reconnects" style="vertical-align:bottom">cloud_queue</i>`
                : ''
            td.innerHTML = `<i class="material-icons tiny" style="vertical-align:bottom">wifi_off</i> Offline${formatLastSeen(this.remoteState.lastSeenUnix)}${bridgeNote}`
            children.push(td)

            td = document.createElement('td')
            td.className = 'dev-actions'
            td.innerHTML = `
                <span class="tooltipped" style="display: inline-block" data-position="bottom" data-tooltip="Forget this device -- clears its saved state; it won't be listed again unless it reconnects">
                    <a class="btn waves-effect waves-light red" href="#"><i class="material-icons">delete_forever</i></a>
                </span>`
            children.push(td)

            this.row.replaceChildren(...children)
            Array.from(this.row.getElementsByClassName('tooltipped')).forEach((e) => M.Tooltip.init(e))
            td.querySelector('a').onclick = async (ev) => {
                ev.preventDefault()
                await fetchWrapper(`forget/${encodeURIComponent(this.id)}`, { incarnation: this.remoteState.incarnation }, { method: 'POST' })
            }
            return
        }

        td = document.createElement('td')
        td.className = 'dev-model'
        // The model string is whatever the appliance reported about itself (thinq2 deploy
        // `kind`, thinq1 `modelName`), so it goes in as text, never as markup.
        td.textContent = this.remoteState.model
        if (!this.remoteState.mapped && deviceMappingCapable) {
            // "mapped" just means registry.rs found a .rhai handler for this
            // modelId -- it says nothing about whether some other tool is driving the
            // device over the raw bus instead (rusthinq has no way to know that, MQTT
            // pub/sub doesn't expose who's subscribed), so this can't claim "unsupported".
            // Only shown when this build could possibly have mapped it (scripting
            // compiled in) -- with it off, *every* device is unmapped
            // regardless of the device itself, so the warning would say nothing.
            td.insertAdjacentHTML(
                'beforeend',
                ` <i class="material-icons tooltipped tiny" data-position="bottom" data-tooltip="No script handler for this device in rusthinq -- its state isn't exposed as MQTT properties">warning</i>`,
            )
        }
        children.push(td)

        td = document.createElement('td')
        td.className = 'dev-platform'
        td.innerText = this.remoteState.platform
        children.push(td)

        // No LG account to log into or session to toggle without the `bridge` Cargo feature --
        // see panel.js's `features` handling. The cell (and its column) is dropped rather than
        // shown full of disabled switches with nothing on the page explaining why.
        if (bridgeFeatureEnabled) {
            // The width lives in the stylesheet now: on a narrow screen this cell moves out of
            // the column layout entirely, and a fixed width there would push the row wide again.
            td = document.createElement('td')
            td.className = 'dev-bridge'
            // Paired but not live while the device itself is online: the switch alone would look
            // identical to "never bridged", hiding a real problem -- the LG-cloud session died, or
            // never came back up after this device reconnected (see Bridge::is_paired's doc comment
            // in rusthinq-bridge). One small icon next to the existing switch says so without a new
            // column.
            const bridgeWarning =
                !this.remoteState.bridged && this.remoteState.bridgePaired
                    ? `<i class="material-icons tiny tooltipped" data-position="bottom" data-tooltip="Paired with the LG cloud but not currently relaying" style="vertical-align:middle; color:#e65100">cloud_off</i> `
                    : ''
            td.innerHTML = `
                ${bridgeWarning}<div class="switch">
                    <label>Off <input type="checkbox"> <span class="lever"></span>On</label>
                </div>
                <div class="hide preloader-wrapper verysmall active">
                    <div class="spinner-layer spinner-green-only">
                    <div class="circle-clipper left">
                        <div class="circle"></div>
                    </div><div class="gap-patch">
                        <div class="circle"></div>
                    </div><div class="circle-clipper right">
                        <div class="circle"></div>
                    </div>
                    </div>
                </div>`
            children.push(td)

            this.bridgeSwitch = td.getElementsByTagName('input')[0]
            this.bridgeDiv = td.getElementsByClassName('switch')[0]
            this.spinner = td.getElementsByClassName('preloader-wrapper')[0]

            const startBridge = async (deviceType) => {
                this.bridgeBusy = true
                this.refreshUI()

                try {
                    await fetchWrapper(`bridge/${this.id}/enable`, { deviceType }, { method: 'POST' })
                    this.remoteState.bridged = true
                } finally {
                    this.bridgeBusy = false
                    this.refreshUI()
                }
            }

            const stopBridge = async () => {
                this.bridgeBusy = true
                this.refreshUI()

                try {
                    await fetchWrapper(`bridge/${this.id}/disable`, {}, { method: 'POST' })
                    this.remoteState.bridged = false
                } finally {
                    this.bridgeBusy = false
                    this.refreshUI()
                }
            }

            this.bridgeSwitch.onchange = () => {
                if (this.bridgeSwitch.checked) {
                    if (this.remoteState.deviceType) {
                        startBridge(this.remoteState.deviceType)
                    } else {
                        get('btn_devicetype_continue').onclick = () => {
                            let devType = get('devtype-input').value
                            devType = devType.split(' ')[0]
                            startBridge(devType)
                            M.Modal.getInstance(get('devicetype_query')).close()
                        }
                        M.Modal.getInstance(get('devicetype_query')).open()
                    }
                } else {
                    stopBridge()
                }
            }
        }

        td = document.createElement('td')
        // Materialize disables a button with pointer-events: none, which would swallow the hover
        // that opens its tooltip - so the tooltip lives on a wrapper instead of on the button.
        td.className = 'dev-actions'
        td.innerHTML = `
            <span class="tooltipped" style="display: inline-block" data-position="bottom" data-tooltip="Monitor">
                <a class="btn waves-effect waves-light"><i class="material-icons">troubleshoot</i></a>
            </span>`
        // Set as a property, not interpolated into the markup: the id also comes from the device.
        td.querySelector('a').href = `monitor?id=${encodeURIComponent(this.id)}`
        children.push(td)

        this.row.replaceChildren(...children)
        Array.from(this.row.getElementsByClassName('tooltipped')).forEach((e) => M.Tooltip.init(e))

        // The markup above is rebuilt from scratch, so the switch comes back unchecked and the
        // spinner comes back visible. Nothing else re-applies the row's actual state: a plain
        // {devices} broadcast - which is what enabling a bridge, or any appliance connecting or
        // dropping, sends - never reaches the branch that refreshes every row. Without this the
        // whole table reads as "all bridges off" until the page is reloaded.
        this.refreshUI()
    }

    refreshUI() {
        // Offline rows, and any row when this build has no `bridge` feature at all, have no
        // bridge switch/spinner to update -- see updateDom above.
        if (this.remoteState.online === false || !bridgeFeatureEnabled) return
        if (this.bridgeBusy) {
            this.bridgeDiv.classList.add('hide')
            this.spinner.classList.remove('hide')
        } else {
            this.spinner.classList.add('hide')
            this.bridgeDiv.classList.remove('hide')
            this.bridgeSwitch.checked = !!this.remoteState.bridged
        }

        // Materialize greys out a switch from the disabled attribute, not from a class, so setting
        // a class left the switch live while logged out - clicking it just produced an HTTP 400.
        this.bridgeSwitch.disabled = !bridge_status
    }
}

// The first reconnect is near-immediate and only then does it back off. A socket that closes because
// the page went into the back/forward cache, or because rusthinq-gui restarted under it, otherwise
// leaves the panel blank - everything is behind .hide-when-offline - for the whole retry interval.
let retryDelay = 250

function connect() {
    clearTimeout(reconnectTimer)
    if (ws) {
        // detach first: a socket replaced mid-flight still fires its close, which would queue a second
        // reconnect on top of this one
        ws.onclose = ws.onopen = ws.onmessage = null
        try {
            ws.close()
        } catch {}
    }
    ws = new WebSocket(baseUrl + 'ws')

    ws.onclose = () => {
        setConnArrow('conn_ws', false)
        // Can't know rusthinq-gui's own MQTT state without the WS that carries it.
        setConnArrow('conn_mqtt', null)
        document.getElementsByTagName('body')[0].classList.add('offline')
        reconnectTimer = setTimeout(connect, retryDelay)
        retryDelay = 5000
    }

    ws.onopen = () => {
        retryDelay = 250
        setConnArrow('conn_ws', true)
        document.getElementsByTagName('body')[0].classList.remove('offline')
    }

    ws.onmessage = (ev) => {
        if (typeof ev.data === 'string') {
            const json = JSON.parse(ev.data)
            if (typeof json.version === 'string') {
                // one in the header bar, one under the title on a narrow screen
                document.querySelectorAll('.version').forEach((el) => (el.innerText = 'v' + json.version))
            }

            if (typeof json.features === 'object' && json.features !== null) {
                const enabled = Object.entries(json.features)
                    .filter(([, v]) => v)
                    .map(([k]) => k)
                const tooltip =
                    enabled.length > 0
                        ? `Build features: ${enabled.join(', ')}`
                        : 'Build features: none (bridge/scripting all off)'
                document.querySelectorAll('.version').forEach((el) => {
                    M.Tooltip.getInstance(el)?.destroy()
                    el.setAttribute('data-tooltip', tooltip)
                    M.Tooltip.init(el, { position: 'bottom' })
                })
                deviceMappingCapable = !!json.features.scripting

                bridgeFeatureEnabled = !!json.features.bridge
                document.getElementById('bridge_mode_section').classList.toggle('hide', !bridgeFeatureEnabled)
                get('devices_table').classList.toggle('no-bridge', !bridgeFeatureEnabled)
                // A row already on the page when this arrives was built assuming the feature
                // was on (the default before the first snapshot) -- redraw it without the
                // bridge cell now that we know better.
                for (const id in devices) devices[id].updateDom()
            }

            // `guiMqtt` is rusthinq-gui's own MQTT connection (mqtt.rs) -- what the
            // "<-> MQTT" link in the chain actually means. `mqtt` (rusthinq-cloud's own
            // connection, reported inside the same devlist.rs payload this arrives in)
            // is deliberately not shown here: it can only ever be seen at all once
            // rusthinq-gui's own connection is already up, so it says nothing extra
            // about *this* link.
            if (typeof json.guiMqtt === 'boolean') {
                setConnArrow('conn_mqtt', json.guiMqtt)
            }

            if (typeof json.devices === 'object') {
                let deletedDevices = Object.keys(devices).filter((id) => !json.devices[id])
                deletedDevices.forEach((id) => {
                    devices[id].destroy()
                    delete devices[id]
                })

                for (const id in json.devices) {
                    const j = json.devices[id]

                    if (!devices[id]) devices[id] = new DeviceEntry(id, j, get('devices_body'))
                    else devices[id].update(j)
                }

                // Only the bridge knows the names, and only when a ThinQ account is linked. Decided
                // over the whole list rather than per row: the column either says something about
                // these appliances or it says nothing about any of them.
                const named = Object.values(devices).some((dev) => dev.remoteState.name)
                get('devices_table').classList.toggle('no-names', !named)
            }

            if (typeof json.bridge === 'object' && json.bridge !== null) {
                bridge_status = json.bridge.loggedIn
                if (json.bridge.loggedIn === null) {
                    document.getElementById('btn_thinq_login').classList.add('hide')
                    document.getElementById('btn_thinq_logout').classList.add('hide')

                    get('status_bridge').innerHTML = STATUS_DISABLED
                    get('status_bridge_text').innerText = 'Disabled by configuration file'
                } else if (json.bridge.loggedIn === true) {
                    document.getElementById('btn_thinq_login').classList.add('hide')
                    document.getElementById('btn_thinq_logout').classList.remove('hide')

                    get('status_bridge').innerHTML = STATUS_OK
                    get('status_bridge_text').innerText = 'Ok'
                } else {
                    document.getElementById('btn_thinq_login').classList.remove('hide')
                    document.getElementById('btn_thinq_logout').classList.add('hide')

                    get('status_bridge').innerHTML = STATUS_ERROR
                    get('status_bridge_text').innerText = 'Not configured'
                }

                for (const id in devices) devices[id].refreshUI()
            }

            if (typeof json.status === 'string') {
                toastText(json.status)
            }
        }
    }
}

get('btn_thinq_login_continue').onclick = () => {
    if (!get('country_code').validity.valid) return

    const countryCode = get('country_code').value.toUpperCase()

    window.open(`${baseUrl}thinq_login?countryCode=${countryCode}`, '_blank')
}

get('btn_thinq_login_complete').onclick = async () => {
    if (!get('country_code').validity.valid) return

    if (!get('login_url').validity.valid) return

    const countryCode = get('country_code').value.toUpperCase()
    const url = get('login_url').value
    await fetchWrapper(`thinq_login_accept`, { url, countryCode }, { method: 'POST' })
    M.Modal.getInstance(get('thinq_login')).close()
}

get('btn_thinq_logout_continue').onclick = async () => {
    await fetchWrapper(`thinq_logout`, {}, { method: 'POST' })
    M.Modal.getInstance(get('thinq_logout')).close()
}

/*
 * A page restored from the browser's back/forward cache comes back with a socket the browser has
 * killed on the way in, and the close handler hides everything behind .hide-when-offline - so
 * pressing Back from the monitor lands on a panel with no device list. Reconnect unconditionally:
 * the socket can still read as OPEN at this point and only report its close a moment later, so
 * checking readyState here is exactly the mistake that made the first attempt at this a no-op.
 */
window.addEventListener('pageshow', (ev) => {
    if (ev.persisted) connect() // a full load runs connect() on its own
})

function get(id) {
    return document.getElementById(id)
}

// `lastSeenUnix` is 0 for a device whose state predates last-seen tracking
// (mqtt.rs's migration from the pre-#9 state-file shape) -- nothing meaningful to
// show then, so this leaves it out rather than claiming "last seen 1970".
function formatLastSeen(lastSeenUnix) {
    if (!lastSeenUnix) return ''
    const seconds = Math.max(0, Date.now() / 1000 - lastSeenUnix)
    const units = [
        ['d', 86400],
        ['h', 3600],
        ['m', 60],
    ]
    for (const [suffix, secondsPerUnit] of units) {
        if (seconds >= secondsPerUnit) {
            return ` — last seen ${Math.floor(seconds / secondsPerUnit)}${suffix} ago`
        }
    }
    return ' — last seen just now'
}

async function fetchWrapper(path, body, options) {
    if (options.method !== 'GET') {
        if (!options.headers) options.headers = {}
        options.headers['Content-type'] = 'application/json'
    }
    options.body = JSON.stringify(body)
    try {
        const response = await fetch(`${baseUrl}${path}`, options)
        if (response.status >= 300) toastText(`HTTP error ${response.status}: ${await response.text()}`)

        return response
    } catch (err) {
        toastText(`FETCH error: ${err}`)
    }
}

// Dark mode: persisted per-browser (see index.html/monitor.html's early inline
// script, which reads the same key before first paint).
get('btn_dark_toggle').onclick = (ev) => {
    ev.preventDefault()
    const dark = document.documentElement.classList.toggle('dark-theme')
    try {
        localStorage.setItem('rusthinq-dark', dark ? '1' : '0')
    } catch (e) {}
}

connect()
