/* Dashboard state, device operations and service health. */
(() => {
  const { $, icon, api, toast, confirm, busy, text, relative } = UI;
  let snapshot = { devices: {}, features: {} },
    connected = false,
    received = false;
  let account = { enabled: false, account: {} },
    filter = "all",
    selected = null,
    pairing = null;
  let mqttConnected = false,
    statusTimer,
    polling = false;
  const pending = new Set(),
    cards = new Map();
  const attention = (device) =>
    !!(
      device.scriptFaulted ||
      device.bridgeError ||
      device.bridgePending ||
      device.removal
    );
  const deviceName = (id, device) =>
    device.name || device.modelName || device.model || id;
  function badge(label, kind = "neutral") {
    return text("span", label, `badge ${kind}`);
  }
  function devicePath(id, action) {
    return `api/devices/${encodeURIComponent(id)}/${action}`;
  }
  function scope(device) {
    return {
      incarnation: device.incarnation,
      generation: device.generation,
      script_generation: device.scriptGeneration,
    };
  }
  function actionButton(label, action, disabled = false, style = "secondary") {
    const button = text("button", label, `button ${style}`);
    button.type = "button";
    button.disabled = disabled;
    button.onclick = () => busy(button, action);
    return button;
  }
  function connection(up) {
    connected = up;
    $("connection").textContent = up
      ? "Connected locally"
      : received
        ? "Reconnecting"
        : "Connecting";
    $("connection").className = `badge ${up ? "good" : "warning"}`;
    $("sidebar-dot").className = `status-dot ${up ? "good" : "error"}`;
    $("sidebar-status").replaceChildren(
      text("span", up ? "Workspace online" : "Reconnecting…"),
      text("small", "Local management"),
    );
    $("connection-banner").hidden = up || !received;
    renderDevices();
    renderDetail();
    renderAccount();
  }
  function renderDevices() {
    const entries = Object.entries(snapshot.devices);
    $("nav-count").textContent = entries.length;
    $("stat-total").textContent = entries.length;
    $("stat-online").textContent = entries.filter(([, d]) => d.online).length;
    $("stat-bridged").textContent = entries.filter(([, d]) => d.bridged).length;
    $("stat-attention").textContent = entries.filter(([, d]) =>
      attention(d),
    ).length;
    const query = $("device-search").value.trim().toLocaleLowerCase();
    const visible = entries.filter(
      ([id, d]) =>
        (!query ||
          [id, d.name, d.model, d.modelName, d.platform].some((v) =>
            String(v || "")
              .toLocaleLowerCase()
              .includes(query),
          )) &&
        (filter === "all" ||
          (filter === "online" && d.online) ||
          (filter === "offline" && !d.online) ||
          (filter === "attention" && attention(d))),
    );
    visible.sort(([a, da], [b, db]) => {
      if ($("device-sort").value === "status" && da.online !== db.online)
        return da.online ? -1 : 1;
      const first =
        $("device-sort").value === "model" ? da.model : deviceName(a, da);
      const second =
        $("device-sort").value === "model" ? db.model : deviceName(b, db);
      return (
        String(first || "").localeCompare(String(second || "")) ||
        a.localeCompare(b)
      );
    });
    $("device-count").textContent =
      `${visible.length} ${visible.length === 1 ? "device" : "devices"}${visible.length !== entries.length ? ` of ${entries.length}` : ""}`;
    $("device-empty").hidden = visible.length > 0 || !received;
    $("device-empty").querySelector("h2").textContent = entries.length
      ? "No matching devices"
      : "Your workspace is ready";
    $("device-empty").querySelector("p").textContent = entries.length
      ? "Try a different search or filter."
      : "Devices will appear here as soon as they connect to rusthinq.";
    for (const [id, card] of cards)
      if (!snapshot.devices[id]) {
        card.node.remove();
        cards.delete(id);
      }
    const shown = new Set(visible.map(([id]) => id));
    for (const [id, card] of cards) card.node.hidden = !shown.has(id);
    visible.forEach(([id, device], index) => {
      const revision = JSON.stringify([
        device,
        connected,
        pending.has(id),
        account.account.loggedIn,
      ]);
      let card = cards.get(id);
      if (!card) {
        card = { node: document.createElement("article") };
        cards.set(id, card);
      }
      card.node.hidden = false;
      if (card.revision !== revision) {
        updateCard(card.node, id, device);
        card.revision = revision;
      }
      const position = $("device-grid").children[index];
      if (position !== card.node)
        $("device-grid").insertBefore(card.node, position || null);
    });
  }
  function updateCard(node, id, device) {
    node.className = `device-card${device.online ? "" : " offline"}`;
    const top = document.createElement("div");
    top.className = "card-top";
    const symbol = document.createElement("span");
    symbol.className = "device-symbol";
    symbol.innerHTML = icon("device");
    top.append(
      symbol,
      badge(
        device.removal ? "Removing" : device.online ? "Online" : "Offline",
        device.removal ? "warning" : device.online ? "good" : "neutral",
      ),
    );
    const facts = document.createElement("div");
    facts.className = "card-facts";
    if (device.platform) facts.append(badge(device.platform));
    if (device.scriptFaulted) facts.append(badge("Driver fault", "error"));
    else if (device.mapped) facts.append(badge("Driver ready", "good"));
    else if (snapshot.features.scripting && device.online)
      facts.append(badge("No driver", "warning"));
    if (device.bridged) facts.append(badge("Cloud connected", "good"));
    else if (device.bridgePaired)
      facts.append(
        badge(
          device.bridgeEnabled ? "Cloud reconnecting" : "Cloud paused",
          device.bridgeEnabled ? "warning" : "neutral",
        ),
      );
    else if (device.bridgePending)
      facts.append(badge("Pairing unresolved", "warning"));
    const footer = document.createElement("div");
    footer.className = "card-bottom";
    footer.append(
      text(
        "small",
        device.online
          ? "Live local connection"
          : device.lastSeenUnix
            ? `Seen ${relative(device.lastSeenUnix)}`
            : "Waiting for connection",
      ),
    );
    const details = actionButton(
      "View device",
      async () => {
        selected = { id, ...scope(device) };
        renderDetail();
        $("device-dialog").showModal();
      },
      false,
    );
    details.insertAdjacentHTML("beforeend", icon("arrow"));
    footer.append(details);
    const children = [
      top,
      text("h2", deviceName(id, device)),
      text("p", device.model || "Model not reported", "device-model"),
      text("p", id, "device-id"),
      facts,
    ];
    if (device.bridgeError || device.removal)
      children.push(
        text("p", device.bridgeError || device.removal, "device-warning"),
      );
    children.push(footer);
    node.replaceChildren(...children);
  }
  function detailRows(element, pairs) {
    element.replaceChildren(
      ...pairs.flatMap(([label, value, mono]) => [
        text("dt", label),
        text(
          "dd",
          value === undefined || value === null || value === ""
            ? "Not reported"
            : String(value),
          mono ? "mono" : "",
        ),
      ]),
    );
  }
  function renderDetail() {
    if (!selected) return;
    const device = snapshot.devices[selected.id];
    $("detail-title").textContent = device
      ? deviceName(selected.id, device)
      : "Device removed";
    const stale =
      !device ||
      device.incarnation !== selected.incarnation ||
      device.generation !== selected.generation ||
      device.scriptGeneration !== selected.script_generation;
    $("detail-status").hidden = !stale && connected;
    $("detail-status").textContent = !connected
      ? "Connection unavailable. Wait for the workspace to reconnect."
      : "This device changed while you were viewing it. Close and reopen its details to use the current connection.";
    const blocked =
      !connected || stale || pending.has(selected.id) || !!device?.removal;
    detailRows($("device-details"), [
      ["Device ID", selected.id, true],
      ["Connection", device?.online ? "Online" : "Offline"],
      ["Platform", device?.platform],
      ["Model", device?.modelName || device?.model],
      ["Model ID", device?.modelId || device?.model, true],
      ["Software", device?.swVersion],
      ["Device type", device?.deviceType],
      ["Last seen", relative(device?.lastSeenUnix)],
      [
        "Driver",
        device?.scriptFaulted
          ? "Faulted"
          : device?.mapped
            ? "Ready"
            : "Not attached",
      ],
      [
        "Cloud",
        device?.bridged
          ? "Connected"
          : device?.bridgePaired
            ? device.bridgeEnabled
              ? "Reconnecting"
              : "Paused"
            : device?.bridgePending
              ? "Pairing unresolved"
              : "Not paired",
      ],
    ]);
    $("detail-error").hidden = !device?.bridgeError;
    $("detail-error").textContent = device?.bridgeError || "";
    const actions = [];
    const monitor = document.createElement("a");
    monitor.className = "button primary";
    monitor.textContent = "Open packet studio";
    monitor.href = `monitor?id=${encodeURIComponent(selected.id)}`;
    actions.push(monitor);
    if (device?.driverReloadable)
      actions.push(
        actionButton(
          device.scriptFaulted ? "Recover driver" : "Reload driver",
          () => operate(selected.id, "reload", selected),
          blocked,
        ),
      );
    if (account.enabled && device) {
      if (device.bridgePaired)
        actions.push(
          actionButton(
            device.bridgeEnabled ? "Pause bridge" : "Resume bridge",
            () =>
              operate(
                selected.id,
                `bridge/${device.bridgeEnabled ? "disable" : "enable"}`,
                { incarnation: selected.incarnation },
              ),
            blocked || (!device.bridgeEnabled && !account.account.loggedIn),
          ),
        );
      else if (!device.bridgePending)
        actions.push(
          actionButton(
            "Pair with LG",
            async () => {
              pairing = { id: selected.id, incarnation: selected.incarnation };
              $("pair-description").textContent =
                `Register ${deviceName(selected.id, device)} with your LG account. Use the device type reported during setup.`;
              $("pair-type").value = device.deviceType || "";
              $("pair-alias").value = deviceName(selected.id, device);
              $("pair-dialog").showModal();
            },
            blocked || !account.account.loggedIn,
          ),
        );
      if (device.bridgePaired || device.bridgePending)
        actions.push(
          actionButton(
            "Unpair",
            async () => {
              const captured = {
                id: selected.id,
                incarnation: selected.incarnation,
              };
              if (
                await confirm(
                  "Unpair from LG cloud?",
                  "This deletes the device’s LG registration. Using the bridge again will require a new pairing.",
                  "Unpair device",
                  true,
                )
              )
                await operate(captured.id, "bridge/unpair", {
                  incarnation: captured.incarnation,
                });
            },
            blocked || !account.account.loggedIn,
            "danger",
          ),
        );
    }
    actions.push(
      actionButton(
        "Forget device",
        async () => {
          const captured = {
            id: selected.id,
            incarnation: selected.incarnation,
          };
          if (
            await confirm(
              "Forget this device?",
              "Its saved local state and owned retained messages will be removed. If paired, its LG registration is also removed. The device can appear again if it reconnects.",
              "Forget device",
              true,
            )
          ) {
            await operate(captured.id, "forget", {
              incarnation: captured.incarnation,
            });
            $("device-dialog").close();
          }
        },
        blocked,
        "danger",
      ),
    );
    $("device-actions").replaceChildren(...actions);
    $("property-form").hidden = !device?.mapped;
    $("property-submit").disabled =
      blocked || !device?.online || !!device?.scriptFaulted;
  }
  async function operate(id, action, body) {
    if (!connected || pending.has(id))
      throw new Error(
        "Device operation unavailable. Wait for the current operation to finish.",
      );
    pending.add(id);
    renderDevices();
    renderDetail();
    try {
      const result = await api(devicePath(id, action), body);
      toast(
        action === "forget"
          ? "Removal accepted. Follow the device status for completion."
          : action === "reload"
            ? "Driver replacement accepted. Initialization continues in the background."
            : "Operation completed.",
        "info",
      );
      await refreshDevices();
      await refreshServices();
      return result;
    } finally {
      pending.delete(id);
      renderDevices();
      renderDetail();
    }
  }
  async function refreshDevices() {
    const value = await api("api/devices");
    receive(value);
  }
  function receive(value) {
    if (value.devices) {
      received = true;
      snapshot = value;
      renderDevices();
      renderDetail();
    }
    if (value.version) $("version").textContent = `v${value.version}`;
    if (value.lostEvents)
      toast(
        `${value.lostEvents} updates were missed. State has been refreshed.`,
        "error",
      );
  }
  function renderAccount() {
    const state = account.account || {},
      loggedIn = !!state.loggedIn;
    $("cloud-badge").textContent = !account.enabled
      ? "Not configured"
      : state.busy
        ? "Connecting"
        : loggedIn
          ? "Authenticated"
          : state.error
            ? "Needs attention"
            : "Not connected";
    $("cloud-badge").className =
      `badge ${loggedIn ? "good" : state.error ? "error" : "neutral"}`;
    $("cloud-description").textContent = !account.enabled
      ? "LG cloud is not configured for this runtime."
      : state.error ||
        (loggedIn
          ? "Your account is ready. Manage individual bridges from each device."
          : "Connect your account to pair devices and enable cloud bridges.");
    $("cloud-login").hidden = loggedIn;
    $("cloud-login").disabled = !connected || !account.enabled || !!state.busy;
    $("cloud-refresh").hidden = !state.stored;
    $("cloud-refresh").disabled = !connected || !!state.busy;
    $("cloud-logout").hidden = !state.stored;
    $("cloud-logout").disabled = !connected || !!state.busy;
    $("load-inventory").disabled = !connected || !loggedIn || !!state.busy;
    if (!loggedIn)
      $("cloud-inventory").replaceChildren(
        text("p", "Connect your account to view its devices.", "muted"),
      );
    renderDevices();
    renderDetail();
  }
  async function refreshServices() {
    if (polling) return;
    polling = true;
    try {
      const results = await Promise.allSettled([
        api("api/cloud"),
        api("api/mqtt"),
        api("api/health"),
      ]);
      if (results[0].status === "fulfilled") {
        account = results[0].value;
        renderAccount();
      }
      if (results[1].status === "fulfilled") {
        const mqtt = results[1].value;
        mqttConnected = mqtt.status === "Connected";
        detailRows($("mqtt-details"), [
          ["Status", mqtt.status],
          ["Missed transient messages", mqtt.droppedTransient ?? 0],
        ]);
        $("cleanup-submit").disabled = !connected || !mqttConnected;
      }
      if (results[2].status === "fulfilled") {
        const health = results[2].value;
        detailRows($("runtime-details"), [
          ["Status", health.running ? "Running" : "Stopped"],
          ["Version", health.version],
          ["Retained cleanup", health.retainedCleanup],
        ]);
      }
    } finally {
      polling = false;
    }
  }
  async function poll() {
    await refreshServices();
    statusTimer = setTimeout(poll, document.hidden ? 15000 : 5000);
  }
  window.addEventListener("pagehide", () => clearTimeout(statusTimer));
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) poll();
  });
  const views = {
    devices: "Devices",
    cloud: "LG cloud",
    activity: "Activity",
    system: "System",
  };
  function navigate() {
    const view = Object.hasOwn(views, location.hash.slice(1))
      ? location.hash.slice(1)
      : "devices";
    Object.keys(views).forEach((name) => {
      $(`view-${name}`).hidden = name !== view;
    });
    document.querySelectorAll("[data-view]").forEach((node) => {
      node.classList.toggle("active", node.dataset.view === view);
      if (node.dataset.view === view) node.setAttribute("aria-current", "page");
      else node.removeAttribute("aria-current");
    });
    $("view-label").textContent = views[view];
    document.title = `rusthinq · ${views[view]}`;
  }
  window.addEventListener("hashchange", navigate);
  navigate();
  document.querySelectorAll("[data-filter]").forEach(
    (button) =>
      (button.onclick = () => {
        filter = button.dataset.filter;
        document.querySelectorAll("[data-filter]").forEach((node) => {
          node.classList.toggle("active", node === button);
          node.setAttribute("aria-pressed", String(node === button));
        });
        renderDevices();
      }),
  );
  $("device-search").oninput = renderDevices;
  $("device-sort").onchange = renderDevices;
  $("refresh-devices").onclick = () =>
    busy($("refresh-devices"), refreshDevices);
  $("cloud-login").onclick = () => {
    $("login-dialog").showModal();
    $("country-code").focus();
  };
  $("cloud-refresh").onclick = () =>
    busy($("cloud-refresh"), async () => {
      await api("api/cloud/refresh", {});
      await refreshServices();
      toast("Account refreshed.");
    });
  $("cloud-logout").onclick = () =>
    busy($("cloud-logout"), async () => {
      if (
        await confirm(
          "Sign out of LG?",
          "Saved account credentials will be removed and cloud connections will stop. Your local devices remain available.",
          "Sign out",
        )
      ) {
        await api("api/cloud/logout", {});
        await refreshServices();
        $("cloud-inventory").replaceChildren(
          text("p", "Connect your account to view its devices.", "muted"),
        );
        toast("Signed out of LG.");
      }
    });
  $("load-inventory").onclick = () =>
    busy($("load-inventory"), async () => {
      const inventory = await api("api/cloud/inventory");
      $("cloud-inventory").replaceChildren(
        ...(Array.isArray(inventory) ? inventory : []).map((device) => {
          const row = document.createElement("div");
          row.className = "inventory-item";
          const symbol = document.createElement("span");
          symbol.className = "device-symbol";
          symbol.innerHTML = icon("device");
          const info = document.createElement("div");
          info.append(
            text("strong", device.alias || device.modelName || "LG device"),
            text("small", device.deviceId),
          );
          row.append(symbol, info, badge(device.platformType || "LG"));
          return row;
        }),
      );
      if (!$("cloud-inventory").children.length)
        $("cloud-inventory").append(
          text("p", "No devices registered to this account.", "muted"),
        );
    });
  $("login-start-form").onsubmit = (event) => {
    event.preventDefault();
    const popup = window.open("about:blank", "_blank");
    if (popup) popup.opener = null;
    if (!popup) {
      toast("Allow popups to open LG login, then try again.", "error");
      return;
    }
    busy($("login-start"), async () => {
      try {
        const result = await api("api/cloud/login", {
          country: $("country-code").value.toUpperCase(),
        });
        const destination = new URL(result.url);
        if (destination.protocol !== "https:")
          throw new Error("LG returned an invalid login address.");
        popup.location = destination.href;
        $("login-complete-form").hidden = false;
        $("login-feedback").textContent =
          "LG login opened. Complete sign-in, then paste the final URL.";
        $("login-dialog")
          .querySelectorAll(".steps span")
          .forEach((node, index) =>
            node.classList.toggle("active", index === 1),
          );
        await refreshServices();
      } catch (error) {
        popup.close();
        throw error;
      }
    });
  };
  $("login-complete-form").onsubmit = (event) => {
    event.preventDefault();
    busy($("login-complete"), async () => {
      await api("api/cloud/login/complete", { url: $("login-url").value });
      $("login-url").value = "";
      $("login-dialog").close();
      await refreshServices();
      toast("LG account connected.");
    });
  };
  $("pair-form").onsubmit = (event) => {
    event.preventDefault();
    const target = { ...pairing };
    const type = $("pair-type").value.trim();
    const alias = $("pair-alias").value.trim();
    busy($("pair-submit"), async () => {
      await operate(target.id, "bridge/pair", {
        incarnation: target.incarnation,
        deviceType: type,
        alias,
      });
      // The captured incarnation is preserved even if pairing spans a local replacement.
      await operate(target.id, "bridge/enable", {
        incarnation: target.incarnation,
      });
      $("pair-dialog").close();
    });
  };
  $("property-form").onsubmit = (event) => {
    event.preventDefault();
    const target = { ...selected };
    const prop = $("property-name").value.trim();
    const value = $("property-value").value;
    busy($("property-submit"), async () => {
      const result = await api(devicePath(target.id, "invoke"), {
        ...target,
        id: undefined,
        function: "__command",
        input: JSON.stringify({ prop, value }),
      });
      $("property-result").textContent =
        `Command queued${result.sequence ? ` · ${result.sequence}` : ""}. Driver execution and device response appear in Activity.`;
      toast("Command queued. This does not confirm appliance acceptance.");
    });
  };
  $("device-dialog").addEventListener("close", () => {
    selected = null;
    $("property-result").textContent = "";
    $("property-form").reset();
  });
  $("cleanup-scope").onchange = () => {
    const all = $("cleanup-scope").value === "all",
      topic = $("cleanup-scope").value === "topic";
    $("cleanup-owner-label").hidden = all;
    $("cleanup-owner").required = !all;
    $("cleanup-topic-label").hidden = !topic;
    $("cleanup-topic").required = topic;
  };
  $("cleanup-form").onsubmit = (event) => {
    event.preventDefault();
    const body = { scope: $("cleanup-scope").value };
    if (body.scope !== "all") body.owner = $("cleanup-owner").value.trim();
    if (body.scope === "topic") body.topic = $("cleanup-topic").value.trim();
    busy($("cleanup-submit"), async () => {
      if (
        !(await confirm(
          "Delete retained messages?",
          body.scope === "all"
            ? "All MQTT topics owned by rusthinq will be cleared from the broker. This cannot be undone."
            : `Clear ${body.scope === "topic" ? body.topic : `all topics for ${body.owner}`} from the broker. This cannot be undone.`,
          "Delete messages",
          true,
        ))
      )
        return;
      const result = await api("api/mqtt/retained/delete", body);
      $("cleanup-result").textContent =
        `${result.confirmed} deletions confirmed by the broker.`;
      await refreshServices();
    });
  };
  let lastEvent = "";
  function activity(event) {
    // Packet bytes and script publications belong in the studio, not the event list.
    if (["data", "sent", "scriptOutput"].includes(event.type)) return;
    const summary =
      event.error ||
      event.reason ||
      event.delivery ||
      event.detail ||
      event.type;
    const fingerprint = JSON.stringify([event.type, event.device, summary]);
    if (fingerprint === lastEvent && event.type === "stateChanged") return;
    lastEvent = fingerprint;
    $("activity-empty")?.remove();
    const row = document.createElement("article");
    row.className = "activity-item";
    const symbol = document.createElement("span");
    symbol.className = "event-icon";
    symbol.innerHTML = icon(event.error ? "info" : "pulse");
    const info = document.createElement("div");
    const labels = {
      stateChanged: "Device state updated",
      scriptExecuted: "Driver executed",
      scriptStopped: "Driver stopped",
      scriptDelivery: "Device transmission",
      rejected: "Operation rejected",
      metadata: "Device information updated",
      lost: "Events missed",
      injected: "Packet injected",
    };
    info.append(
      text("strong", labels[event.type] || event.type),
      text(
        "p",
        `${event.device || event.context?.device || ""}${event.device || event.context?.device ? " · " : ""}${event.type === "lost" ? `${event.events} events missed` : event.type === "scriptExecuted" ? event.error || "Execution completed" : String(summary).slice(0, 500)}`,
      ),
    );
    const time = text("time", new Date().toLocaleTimeString());
    row.append(symbol, info, time);
    $("activity-list").prepend(row);
    while ($("activity-list").children.length > 100)
      $("activity-list").lastElementChild.remove();
    if (event.type === "lost")
      toast(`${event.events} activity events were missed.`, "error");
  }
  $("clear-activity").onclick = () => {
    $("activity-list").replaceChildren(
      text("p", "Listening for events…", "empty-note"),
    );
    lastEvent = "";
  };
  UI.socket("ws", {
    open: () => connection(true),
    close: () => connection(false),
    message: receive,
    error: (error) => toast(error.message, "error"),
  });
  UI.socket("api/events", { message: activity });
  poll();
})();
