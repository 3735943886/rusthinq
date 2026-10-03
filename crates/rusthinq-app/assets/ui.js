/* Shared interface primitives. Device/account data is always rendered as text. */
window.UI = (() => {
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
      '<path d="M20 7v5h-5M4 17v-5h5"/><path d="M5 8a8 8 0 0 1 13-3l2 2M4 17l2 2a8 8 0 0 0 13-3"/>',
    device:
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
