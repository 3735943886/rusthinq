/* Shared interface primitives. Device/account data is always rendered as text. */
window.UI = (() => {
  // Safari can still smart-zoom text/cards despite touch-action: manipulation.
  // Cancel only the second stationary tap on non-interactive content.
  // Leave native control activation, text editing, scrolling and pinch zoom alone.
  let tapStart = null;
  let previousTap = null;
  const nativeTarget =
    'a, button, input, select, textarea, label, summary, [role="button"], [role="switch"], [contenteditable]:not([contenteditable="false"])';
  document.addEventListener("touchstart", (event) => {
    tapStart = null;
    if (event.touches.length !== 1 || event.target.closest(nativeTarget)) {
      previousTap = null;
      return;
    }
    const touch = event.touches[0];
    tapStart = { x: touch.clientX, y: touch.clientY, time: event.timeStamp };
  }, { passive: true, capture: true });
  document.addEventListener("touchmove", () => {
    tapStart = null;
    previousTap = null;
  }, { passive: true, capture: true });
  document.addEventListener("touchcancel", () => {
    tapStart = null;
    previousTap = null;
  }, { passive: true, capture: true });
  document.addEventListener("touchend", (event) => {
    const start = tapStart;
    tapStart = null;
    if (!start || event.touches.length || event.changedTouches.length !== 1) {
      previousTap = null;
      return;
    }
    const touch = event.changedTouches[0];
    const tap = { x: touch.clientX, y: touch.clientY, time: event.timeStamp };
    if (tap.time - start.time > 350 ||
        Math.hypot(tap.x - start.x, tap.y - start.y) > 20) {
      previousTap = null;
      return;
    }
    if (previousTap && tap.time - previousTap.time < 350 &&
        Math.hypot(tap.x - previousTap.x, tap.y - previousTap.y) < 30 &&
        event.cancelable) {
      event.preventDefault();
    }
    previousTap = tap;
  }, { passive: false, capture: true });
  const $ = (id) => document.getElementById(id);
  const paths = {
    grid: '<rect x="3" y="3" width="7" height="7" rx="2"/><rect x="14" y="3" width="7" height="7" rx="2"/><rect x="3" y="14" width="7" height="7" rx="2"/><rect x="14" y="14" width="7" height="7" rx="2"/>',
    cloud: '<path d="M7 18a5 5 0 1 1 1-10 7 7 0 0 1 13 4 3 3 0 0 1-3 6Z"/>',
    pulse: '<path d="M3 12h4l3-8 4 16 3-8h4"/>',
    settings:
      '<path d="M4 7h16M4 17h16"/><circle cx="9" cy="7" r="3"/><circle cx="15" cy="17" r="3"/>',
    arrow: '<path d="M7 17 17 7M7 7h10v10"/>',
    search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/>',
    moon: '<path d="M20 14a9 9 0 0 1-10-10 9 9 0 1 0 10 10Z"/>',
    refresh:
      '<path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8M21 3v5h-5M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16M8 16H3v5"/>',
    device: '<rect x="4" y="5" width="16" height="14" rx="2"/><path d="M8 9h8M8 13h4M8 19v2m8-2v2"/>',
    refrigerator: '<rect x="6" y="2" width="12" height="20" rx="2"/><path d="M6 10h12M9 5v2m0 6v4"/>',
    dryer: '<rect x="5" y="2" width="14" height="20" rx="2"/><circle cx="12" cy="13" r="5"/><path d="M8 5h1m3 0h4M10 12l2 2 2-2"/>',
    dishwasher: '<rect x="4" y="2" width="16" height="20" rx="2"/><path d="M4 7h16M8 4h1m3 0h4M8 11v7m4-7v7m4-7v7"/>',
    tower: '<rect x="5" y="1" width="14" height="22" rx="2"/><path d="M5 12h14"/><circle cx="12" cy="7" r="3"/><circle cx="12" cy="17" r="3"/>',
    oven: '<rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 8h18M7 5h1m3 0h1m3 0h1"/><rect x="6" y="11" width="12" height="7" rx="1"/>',
    microwave: '<rect x="2" y="5" width="20" height="14" rx="2"/><rect x="5" y="8" width="10" height="8" rx="1"/><path d="M18 9h1m-1 3h1m-1 3h1"/>',
    air: '<rect x="2" y="4" width="20" height="8" rx="2"/><path d="M5 9h14M7 15v4m5-4v6m5-6v4"/>',
    water: '<rect x="6" y="2" width="12" height="20" rx="2"/><path d="M9 6h6M12 6v5h3M9 18h6"/><path d="M12 13s-2 2-2 3a2 2 0 0 0 4 0c0-1-2-3-2-3Z"/>',
    purifier: '<rect x="6" y="2" width="12" height="20" rx="4"/><path d="M9 6h6M9 10h6M9 13h6M9 16h6"/><circle cx="12" cy="19" r=".5"/>',
    cooktop: '<rect x="2" y="3" width="20" height="18" rx="2"/><circle cx="8" cy="8" r="2"/><circle cx="16" cy="8" r="2"/><circle cx="8" cy="15" r="3"/><circle cx="16" cy="15" r="2"/>',
    dehumidifier: '<rect x="5" y="2" width="14" height="20" rx="3"/><path d="M8 6h8M8 9h8M5 18h14"/><path d="M12 11s-2 2-2 3a2 2 0 0 0 4 0c0-1-2-3-2-3Z"/>',
    styler: '<rect x="5" y="2" width="14" height="20" rx="2"/><path d="M16 11v4M10 7a2 2 0 0 1 4 0c0 1-2 1-2 3l-4 3h8l-4-3"/>',
    hood: '<path d="M9 2h6v7l6 6H3l6-6Z M3 19h18M9 15v4m6-4v4"/>',
    fan: '<circle cx="12" cy="10" r="8"/><circle cx="12" cy="10" r="2"/><path d="M12 8V4m2 6h4m-6 2v4m-2-6H6M12 18v4M7 22h10"/>',
    heater: '<rect x="6" y="2" width="12" height="20" rx="3"/><path d="M9 22v2m6-2v2M12 6c-4 4-4 7 0 9 4-2 4-5 0-9Z"/>',
    robotVacuum: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="9" r="2"/><path d="M5 16h14"/>',
    stickVacuum: '<path d="M14 2v12l-3 5M7 19h10v3H7Z"/><rect x="11" y="5" width="6" height="7" rx="2"/>',
    gateway: '<rect x="3" y="8" width="18" height="12" rx="2"/><path d="M7 8V3m10 5V3M7 15h1m3 0h1m3 0h2"/>',
    tv: '<rect x="2" y="3" width="20" height="15" rx="2"/><path d="M12 18v4M7 22h10"/>',
    speaker: '<rect x="6" y="2" width="12" height="20" rx="2"/><circle cx="12" cy="7" r="2"/><circle cx="12" cy="16" r="4"/>',
    camera: '<rect x="2" y="6" width="20" height="14" rx="2"/><circle cx="12" cy="13" r="4"/><path d="M8 6l2-3h4l2 3"/>',
    sensor: '<rect x="5" y="5" width="14" height="14" rx="3"/><circle cx="12" cy="12" r="3"/><path d="M9 2h6M9 22h6M2 9v6m20-6v6"/>',
    solar: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l2 2m10 10 2 2M5 19l2-2M17 7l2-2"/>',
    light: '<path d="M8 16a7 7 0 1 1 8 0v4H8Z M9 23h6M8 17h8"/>',
    motion: '<circle cx="12" cy="5" r="2"/><path d="m7 11 5-3 4 3h4M12 8l-2 7-4 6m4-6 5 2v5M3 4l2 2M2 10h3"/>',
    plug: '<path d="M8 2v5m8-5v5M6 7h12v5a6 6 0 0 1-12 0Z M12 18v4"/>',
    phone: '<rect x="6" y="2" width="12" height="20" rx="2"/><path d="M10 5h4M11 19h2"/>',
    robot: '<rect x="4" y="6" width="16" height="14" rx="3"/><path d="M12 6V2M1 10v6m22-6v6M8 16h8"/><circle cx="8" cy="11" r="1"/><circle cx="16" cy="11" r="1"/>',
    humidifier: '<path d="M6 10h12l2 12H4Z M8 7c-3-3 3-3 0-6m4 6c-3-3 3-3 0-6m4 6c-3-3 3-3 0-6"/>',
    wine: '<rect x="5" y="2" width="14" height="20" rx="2"/><path d="M9 6h6v4a3 3 0 0 1-6 0ZM12 13v5M9 18h6"/>',
    plant: '<path d="M7 15h10l-2 7H9ZM12 15V9M12 11C5 11 5 5 5 5c7 0 7 6 7 6Zm0-2c0-6 7-7 7-7s0 7-7 7Z"/>',
    brew: '<path d="M8 2h8v4l3 4v12H5V10l3-4Z M8 13h8M8 17h8"/>',
    washer:
      '<rect x="5" y="2" width="14" height="20" rx="3"/><circle cx="12" cy="12" r="4"/><path d="M8 6h1m3 0h4"/>',
    check: '<path d="m5 12 4 4L19 6"/>',
    close: '<path d="m6 6 12 12M6 18 18 6"/>',
    back: '<path d="m12 5-7 7 7 7M5 12h15"/>',
    download: '<path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5"/>',
    copy: '<rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V3H3v13h5"/>',
    bolt: '<path d="m13 2-9 12h7l-1 8 10-12h-7Z"/>',
    info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6m0-10v1"/>',
  };
  function icon(name) {
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name] || paths.device}</svg>`;
  }
  // Numeric ThinQ account types: wideq/device_info.py. Named types: ThinQ Connect.
  const applianceTypes = {
    101: { icon: "refrigerator", label: "Refrigerator" },
    102: { icon: "refrigerator", label: "Kimchi refrigerator" },
    103: { icon: "water", label: "Water purifier" },
    201: { icon: "washer", label: "Washer" },
    202: { icon: "dryer", label: "Dryer" },
    203: { icon: "styler", label: "Styler" },
    204: { icon: "dishwasher", label: "Dishwasher" },
    221: { icon: "washer", label: "WashTower washer" },
    222: { icon: "dryer", label: "WashTower dryer" },
    223: { icon: "tower", label: "WashTower" },
    301: { icon: "oven", label: "Range" },
    302: { icon: "microwave", label: "Microwave" },
    303: { icon: "cooktop", label: "Cooktop" },
    304: { icon: "hood", label: "Hood" },
    401: { icon: "air", label: "Air conditioner" },
    402: { icon: "purifier", label: "Air purifier" },
    403: { icon: "dehumidifier", label: "Dehumidifier" },
    405: { icon: "fan", label: "Fan" },
    406: { icon: "heater", label: "Water heater" },
    410: { icon: "purifier", label: "Air purifier fan" },
    501: { icon: "robotVacuum", label: "Robot vacuum" },
    504: { icon: "stickVacuum", label: "Stick vacuum" },
    603: { icon: "gateway", label: "Cloud gateway" },
    701: { icon: "tv", label: "TV" },
    801: { icon: "heater", label: "Boiler" },
    901: { icon: "speaker", label: "Speaker" },
    902: { icon: "camera", label: "HomeView" },
    1001: { icon: "gateway", label: "ARCH" },
    3001: { icon: "gateway", label: "MISSG" },
    3002: { icon: "sensor", label: "Sensor" },
    3102: { icon: "solar", label: "Solar sensor" },
    3003: { icon: "light", label: "Lighting" },
    3004: { icon: "motion", label: "Motion sensor" },
    3005: { icon: "plug", label: "Smart plug" },
    3006: { icon: "sensor", label: "Dust sensor" },
    4001: { icon: "sensor", label: "Air station" },
    4003: { icon: "sensor", label: "Air sensor" },
    4004: { icon: "sensor", label: "Air detector" },
    6001: { icon: "phone", label: "Phone" },
    9000: { icon: "robot", label: "Home robot" },
    DEVICE_AIR_CONDITIONER: { icon: "air", label: "Air Conditioner" },
    DEVICE_AIR_PURIFIER: { icon: "purifier", label: "Air Purifier" },
    DEVICE_AIR_PURIFIER_FAN: { icon: "purifier", label: "Air Purifier Fan" },
    DEVICE_CEILING_FAN: { icon: "fan", label: "Ceiling Fan" },
    DEVICE_COOKTOP: { icon: "cooktop", label: "Cooktop" },
    DEVICE_DEHUMIDIFIER: { icon: "dehumidifier", label: "Dehumidifier" },
    DEVICE_DISH_WASHER: { icon: "dishwasher", label: "Dish Washer" },
    DEVICE_DRYER: { icon: "dryer", label: "Dryer" },
    DEVICE_HOME_BREW: { icon: "brew", label: "Home Brew" },
    DEVICE_HOOD: { icon: "hood", label: "Hood" },
    DEVICE_HUMIDIFIER: { icon: "humidifier", label: "Humidifier" },
    DEVICE_KIMCHI_REFRIGERATOR: { icon: "refrigerator", label: "Kimchi Refrigerator" },
    DEVICE_MICROWAVE_OVEN: { icon: "microwave", label: "Microwave Oven" },
    DEVICE_OVEN: { icon: "oven", label: "Oven" },
    DEVICE_PLANT_CULTIVATOR: { icon: "plant", label: "Plant Cultivator" },
    DEVICE_REFRIGERATOR: { icon: "refrigerator", label: "Refrigerator" },
    DEVICE_ROBOT_CLEANER: { icon: "robotVacuum", label: "Robot Cleaner" },
    DEVICE_STICK_CLEANER: { icon: "stickVacuum", label: "Stick Cleaner" },
    DEVICE_STYLER: { icon: "styler", label: "Styler" },
    DEVICE_SYSTEM_BOILER: { icon: "heater", label: "System Boiler" },
    DEVICE_VENTILATOR: { icon: "fan", label: "Ventilator" },
    DEVICE_WASHCOMBO_MAIN: { icon: "washer", label: "Washcombo Main" },
    DEVICE_WASHCOMBO_MINI: { icon: "washer", label: "Washcombo Mini" },
    DEVICE_WASHER: { icon: "washer", label: "Washer" },
    DEVICE_WASHTOWER: { icon: "tower", label: "Washtower" },
    DEVICE_WASHTOWER_DRYER: { icon: "dryer", label: "Washtower Dryer" },
    DEVICE_WASHTOWER_WASHER: { icon: "washer", label: "Washtower Washer" },
    DEVICE_WATER_HEATER: { icon: "heater", label: "Water Heater" },
    DEVICE_WATER_PURIFIER: { icon: "water", label: "Water Purifier" },
    DEVICE_WINE_CELLAR: { icon: "wine", label: "Wine Cellar" },
  };
  function applianceIcon(device) {
    return applianceTypes[String(device?.deviceType ?? "")]?.icon || "device";
  }
  const deviceTypes = $("device-types");
  if (deviceTypes) deviceTypes.replaceChildren(...Object.entries(applianceTypes)
    .filter(([type]) => /^\d+$/.test(type))
    .map(([type, info]) => {
      const option = document.createElement("option");
      option.value = type;
      option.textContent = info.label;
      return option;
    }));
  document.querySelectorAll("[data-icon]").forEach((node) => {
    node.innerHTML = icon(node.dataset.icon);
  });
  const base = new URL("./", window.location.href);
  function url(path) {
    return new URL(path, base);
  }
  async function api(path, body, signal) {
    const response = await fetch(url(path), {
      method: body === undefined ? "GET" : "POST",
      headers: body === undefined ? {} : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal:
        signal || AbortSignal.timeout(body === undefined ? 30000 : 105000),
    });
    const result = await response.json();
    if (!response.ok)
      throw new Error(result.error || `Request failed (${response.status})`);
    return result;
  }
  function toast(message, kind = "info") {
    const node = document.createElement("div");
    node.className = `toast ${kind}`;
    node.textContent = message;
    const region = $("notifications");
    while (region.children.length >= 4) region.firstElementChild.remove();
    region.append(node);
    setTimeout(() => node.remove(), 6500);
  }
  function theme() {
    const dark = document.documentElement.classList.toggle("dark-theme");
    try {
      localStorage.setItem("rusthinq-dark", dark ? "1" : "0");
    } catch (_) {}
    $("theme-toggle")?.setAttribute("aria-pressed", String(dark));
  }
  $("theme-toggle")?.addEventListener("click", theme);
  $("theme-toggle")?.setAttribute(
    "aria-pressed",
    String(document.documentElement.classList.contains("dark-theme")),
  );
  document
    .querySelectorAll("[data-close]")
    .forEach((button) =>
      button.addEventListener("click", () => button.closest("dialog").close()),
    );
  document.querySelectorAll("dialog").forEach((dialog) =>
    dialog.addEventListener("click", (event) => {
      const box = dialog.getBoundingClientRect();
      if (
        event.target === dialog &&
        (event.clientX < box.left ||
          event.clientX > box.right ||
          event.clientY < box.top ||
          event.clientY > box.bottom)
      )
        dialog.close();
    }),
  );
  function confirm(title, description, label = "Continue", danger = false) {
    const dialog = $("confirm-dialog");
    $("confirm-title").textContent = title;
    $("confirm-description").textContent = description;
    const button = $("confirm-accept");
    button.textContent = label;
    button.className = danger ? "button danger" : "button primary";
    return new Promise((resolve) => {
      let accepted = false;
      button.onclick = () => {
        accepted = true;
        dialog.close();
      };
      dialog.addEventListener("close", () => resolve(accepted), { once: true });
      dialog.showModal();
    });
  }
  async function busy(button, action) {
    if (button.disabled) return;
    button.disabled = true;
    button.setAttribute("aria-busy", "true");
    try {
      return await action();
    } catch (error) {
      toast(error.message, "error");
    } finally {
      button.disabled = false;
      button.removeAttribute("aria-busy");
    }
  }
  function download(name, contents, type = "text/plain") {
    const object = URL.createObjectURL(new Blob([contents], { type }));
    const link = document.createElement("a");
    link.href = object;
    link.download = name;
    link.click();
    setTimeout(() => URL.revokeObjectURL(object), 1000);
  }
  function socket(path, handlers) {
    let ws,
      timer,
      stopped = false,
      delay = 300;
    function connect() {
      clearTimeout(timer);
      if (ws) {
        ws.onclose = ws.onmessage = ws.onopen = null;
        ws.close();
      }
      const endpoint = url(typeof path === "function" ? path() : path);
      endpoint.protocol = location.protocol === "https:" ? "wss:" : "ws:";
      ws = new WebSocket(endpoint);
      ws.onopen = () => {
        delay = 300;
        handlers.open?.();
      };
      ws.onmessage = (event) => {
        try {
          handlers.message(JSON.parse(event.data));
        } catch (error) {
          handlers.error?.(error);
        }
      };
      ws.onclose = () => {
        handlers.close?.();
        if (!stopped) {
          timer = setTimeout(connect, delay);
          delay = Math.min(delay * 2, 10000);
        }
      };
    }
    window.addEventListener("pagehide", () => {
      stopped = true;
      clearTimeout(timer);
      ws?.close();
    });
    window.addEventListener("pageshow", (event) => {
      if (event.persisted) {
        stopped = false;
        connect();
      }
    });
    connect();
    return {
      reconnect: connect,
      send(value) {
        if (ws?.readyState !== WebSocket.OPEN)
          throw new Error("Connection unavailable. Wait for reconnection.");
        ws.send(JSON.stringify(value));
      },
    };
  }
  function text(tag, value, className) {
    const node = document.createElement(tag);
    node.textContent = value;
    if (className) node.className = className;
    return node;
  }
  function relative(seconds) {
    if (!seconds) return "Not recorded";
    const age = Math.max(0, Date.now() / 1000 - seconds);
    if (age < 60) return "Just now";
    for (const [unit, size] of [
      ["day", 86400],
      ["hour", 3600],
      ["minute", 60],
    ]) {
      if (age >= size)
        return new Intl.RelativeTimeFormat(undefined, {
          numeric: "auto",
        }).format(-Math.floor(age / size), unit);
    }
  }
  return {
    $,
    icon,
    applianceIcon,
    applianceTypes,
    url,
    api,
    toast,
    confirm,
    busy,
    download,
    socket,
    text,
    relative,
  };
})();
