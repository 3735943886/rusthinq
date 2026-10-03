document.addEventListener('DOMContentLoaded', function () {})

let ws
let reconnectTimer

get('device_id').innerText = new URLSearchParams(window.location.search).get('id')
get('device_status').innerText = 'Waiting for rusthinq-gui connection...'

// The socket lives at /device, a sibling of this page. Appending to the page's own path instead
// asks for /monitordevice, which nothing serves.
function deviceSocketUrl() {
    const url = new URL('device', window.location.href)
    url.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    url.search = window.location.search
    return url
}

let deviceOnline = false
let injectionEnabled = false

// Send buttons follow both the device and the daemon's runtime injection switch.
function applyInjection() {
    const usable = deviceOnline && injectionEnabled
    get('btn_send1').disabled = !usable
    get('btn_send2').disabled = !usable
    get('injection').checked = injectionEnabled
}

get('btn_send1').onclick = () => {
    // Hex only. There is no ThinQ1-JSON or CLIP-envelope inject path yet, unlike the
    // rethink original this page was ported from.
    ws.send(JSON.stringify({ sendToDevice: get('send1').value }))
}
get('btn_send2').onclick = () => {
    ws.send(JSON.stringify({ sendFromDevice: get('send2').value }))
}

// Runtime only: the daemon starts with injection off and forgets this on restart.
get('injection').onchange = async () => {
    const wanted = get('injection').checked
    try {
        const response = await fetch(new URL('api/raw-inject', window.location.href), {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ enabled: wanted }),
        })
        const state = await response.json()
        if (!response.ok) throw new Error(state.error || response.statusText)
        injectionEnabled = state.enabled === true
    } catch (error) {
        const message = document.createElement('span')
        message.textContent = `Raw injection: ${error.message}`
        M.toast({ html: message })
    }
    applyInjection()
}

// As on the panel: first retry near-immediately, back off only if that fails too.
let retryDelay = 250

function connect() {
    clearTimeout(reconnectTimer)
    if (ws) {
        ws.onclose = ws.onopen = ws.onmessage = null
        try {
            ws.close()
        } catch {}
    }
    ws = new WebSocket(deviceSocketUrl())

    ws.onclose = () => {
        reconnectTimer = setTimeout(connect, retryDelay)
        retryDelay = 5000
        get('device_status').innerText = 'Waiting for rusthinq-gui connection...'
    }

    ws.onopen = () => {
        retryDelay = 250
        get('device_status').innerText = 'offline'
    }

    ws.onmessage = (ev) => {
        if (typeof ev.data === 'string') {
            const json = JSON.parse(ev.data)
            if (json.error || json.lostEvents || json.delivery) {
                const message = document.createElement('span')
                message.textContent = json.error || (json.lostEvents ? `Lost ${json.lostEvents} events; reconnect to refresh.` : `Transport delivery: ${json.delivery}`)
                M.toast({ html: message })
            }
            if (json.rx) {
                const div = pushMessage('rx', json.rx, json.injected)
                div.onclick = () => {
                    get('send2').value = json.rx
                    M.updateTextFields()
                }
            }

            if (json.tx) {
                const div = pushMessage('tx', json.tx, json.injected)
                div.onclick = () => {
                    get('send1').value = json.tx
                    M.updateTextFields()
                }
            }

            if (json.status) {
                get('device_status').innerText = json.status
                deviceOnline = json.status === 'online'
                injectionEnabled = json.injectionEnabled === true
                get('injection_row').style.display = json.injectionToggle ? '' : 'none'
                applyInjection()
            }

            if (json.meta) {
                get('device_model').innerText = json.meta.modelId
            }
        }
    }
}

// Same as the panel, and for the same reason its readyState check had to go: the restored socket can
// still read as OPEN here and only report its close afterwards.
window.addEventListener('pageshow', (ev) => {
    if (ev.persisted) connect()
})

function pushMessage(direction, payload, injected) {
    const timestamp = document.createElement('span')
    const messages = get('messages')

    timestamp.innerText = new Date().toLocaleTimeString()
    timestamp.classList.add('timestamp')
    const div = document.createElement('div')
    div.classList.add(direction, 'message')
    if (injected) div.classList.add('injected')
    div.innerText = payload
    div.appendChild(timestamp)

    messages.appendChild(div)

    if (get('autoscroll').checked) messages.scrollTop = messages.scrollHeight

    return div
}

function get(id) {
    return document.getElementById(id)
}

connect()
