(() => {
  const { $, api, toast, confirm, busy, text } = UI;
  const id = new URLSearchParams(location.search).get("id");
  $("device-id").textContent = id || "No device selected";
  let connected = false,
    online = false,
    injection = false,
    stale = false,
    paused = false;
  let direction = "all",
    selected = null,
    analysis = null,
    analysisRevision = 0;
  let rx = 0,
    tx = 0,
    cloud = 0,
    loss = 0,
    evicted = 0,
    nextRecord = 0,
    bufferedBytes = 0,
    sending = false;
  let connectionRevision = 0;
  const records = [],
    nodes = new Map();
  function applyInjection() {
    $("inject-submit").disabled =
      !connected || !online || !injection || stale || sending;
    $("injection").checked = injection;
    $("injection").disabled = !connected || sending;
    $("injection-badge").textContent = stale
      ? "Connection changed"
      : injection
        ? "Injection enabled"
        : "Read only";
    $("injection-badge").className =
      `badge ${injection || stale ? "warning" : "neutral"}`;
    $("reconnect-monitor").hidden = !stale;
  }
  function state(up) {
    connected = up;
    connectionRevision++;
    if (!up) online = false;
    $("monitor-connection").textContent = up
      ? "Connected locally"
      : "Reconnecting";
    $("monitor-connection").className = `badge ${up ? "good" : "warning"}`;
    $("monitor-notice").hidden = up && !stale;
    $("monitor-notice").textContent = stale
      ? "The device connection changed. Refresh the monitor connection before sending another packet."
      : "Connection interrupted. Traffic will resume after reconnection.";
    applyInjection();
  }
  function render() {
    $("cloud-count").textContent = cloud;
    $("rx-count").textContent = rx;
    $("tx-count").textContent = tx;
    $("loss-count").textContent = loss;
    $("buffer-count").textContent =
      `${records.length} buffered${evicted ? ` · ${evicted} older records discarded` : ""}`;
    $("export-capture").disabled = !records.length;
    if (paused) return;
    const visible = records.filter(
      (record) =>
        !["lost", "note"].includes(record.k) &&
        (direction === "all" || record.k === direction),
    );
    const active = new Set(visible.map((record) => record.key));
    for (const [key, node] of nodes)
      if (!active.has(key)) {
        node.remove();
        nodes.delete(key);
      }
    $("messages").querySelector(".empty-note")?.remove();
    if (!visible.length)
      $("messages").append(
        text("p", "No packets in this view yet.", "empty-note"),
      );
    for (const record of visible) {
      if (!nodes.has(record.key)) {
        const row = document.createElement("button");
        row.type = "button";
        row.className = `packet-row${record.k === "cloud" ? " cloud-packet" : ""}${record.injected ? " injected" : ""}`;
        row.setAttribute(
          "aria-label",
          `${record.k === "rx" ? "Received" : "Sent"} packet at ${new Date(record.t).toLocaleTimeString()}`,
        );
        row.append(
          text("time", new Date(record.t).toLocaleTimeString()),
          text(
            "span",
            record.k === "cloud" ? "LG" : record.k === "rx" ? "RX" : "TX",
            `badge ${record.k === "rx" ? "good" : "neutral"}`,
          ),
          text(
            "code",
            record.k === "cloud"
              ? `${record.topic} · ${record.raw}`
              : record.hex,
          ),
        );
        row.onclick = () => inspect(record);
        nodes.set(record.key, row);
        $("messages").append(row);
      }
      const node = nodes.get(record.key);
      node.classList.toggle("selected", selected?.key === record.key);
      node.setAttribute("aria-pressed", String(selected?.key === record.key));
    }
    if ($("autoscroll").checked)
      $("messages").scrollTop = $("messages").scrollHeight;
  }
  function recordBytes(record) {
    return record.k === "rx" || record.k === "tx"
      ? (record.hex?.length || 0) * 2
      : JSON.stringify(record).length * 2;
  }
  new CloudFeed({
    badge: $("studio-cloud-status"),
    button: $("studio-cloud-toggle"),
    device: id,
    onReset: () => {
      for (let index = records.length - 1; index >= 0; index--)
        if (records[index].k === "cloud") {
          bufferedBytes -= recordBytes(records[index]);
          nodes.get(records[index].key)?.remove();
          nodes.delete(records[index].key);
          records.splice(index, 1);
        }
      cloud = 0;
      if (selected?.k === "cloud") {
        selected = analysis = null;
        analysisRevision++;
        $("analysis-empty").hidden = false;
        $("analysis-content").hidden = true;
      }
      render();
    },
    onRecord: (record) => {
      const value = { ...record, key: ++nextRecord };
      records.push(value);
      bufferedBytes += recordBytes(value);
      if (value.k === "cloud") cloud++;
      if (value.k === "lost") loss += Number(value.events) || 1;
      trim();
      render();
    },
  });
  function recordPacket(k, hex, injected = false) {
    if (typeof hex !== "string") return;
    const record = { key: ++nextRecord, k, t: Date.now(), hex, injected };
    records.push(record);
    bufferedBytes += hex.length * 2;
    if (k === "rx") rx++;
    else tx++;
    trim();
    render();
  }
  function trim() {
    while (records.length > 500 || bufferedBytes > 8 * 1024 * 1024) {
      bufferedBytes -= recordBytes(records.shift());
      evicted++;
    }
  }
  function recordLoss(count) {
    loss += count;
    records.push({
      key: ++nextRecord,
      k: "lost",
      t: Date.now(),
      events: count,
    });
    trim();
    render();
    toast(
      `${count} traffic events were missed. The capture contains a loss marker.`,
      "error",
    );
  }
  async function inspect(record) {
    selected = record;
    analysis = null;
    const revision = ++analysisRevision;
    render();
    $("use-packet").disabled = record.k === "cloud";
    if (record.k === "cloud") {
      $("analysis-empty").hidden = true;
      $("analysis-content").hidden = false;
      $("analysis-direction").textContent =
        "LG notification · " +
        (record.correlation === "device"
          ? record.devices.join(", ")
          : "Account event; device unknown");
      $("packet-hex").textContent = record.raw;
      $("analysis-summary").replaceChildren(
        text("span", record.topic, "badge neutral"),
      );
      $("analysis-fields").replaceChildren();
      $("analysis-notes").replaceChildren(
        text(
          "p",
          "Time-aligned observation; temporal proximity does not prove a packet caused this notification.",
          "muted",
        ),
      );
      $("analysis-export").textContent = JSON.stringify(
        record.payload,
        null,
        2,
      );
      analysis = { exportText: JSON.stringify(record.payload, null, 2) };
      return;
    }
    $("analysis-empty").hidden = true;
    $("analysis-content").hidden = false;
    $("analysis-direction").textContent =
      `${record.k === "rx" ? "Received" : "Sent"}${record.injected ? " · injected" : ""}`;
    $("packet-hex").textContent = record.hex;
    $("analysis-summary").replaceChildren(
      text("span", "Analyzing…", "badge neutral"),
    );
    $("analysis-fields").replaceChildren();
    $("analysis-notes").replaceChildren();
    $("analysis-export").textContent = "";
    try {
      const decoded = await api("api/packets/decode", {
        hex: record.hex,
        direction: record.k === "rx" ? "fromDevice" : "toDevice",
        model_id: $("device-title").dataset.model || "",
      });
      if (revision !== analysisRevision) return;
      analysis = decoded;
      const integrity = decoded.crcOk;
      const summary = [text("span", decoded.protocol, "badge neutral")];
      if (typeof integrity === "boolean")
        summary.push(
          text(
            "span",
            integrity ? "Integrity valid" : "Integrity mismatch",
            `badge ${integrity ? "good" : "error"}`,
          ),
        );
      if (decoded.unknownCount)
        summary.push(
          text("span", `${decoded.unknownCount} unknown tags`, "badge warning"),
        );
      $("analysis-summary").replaceChildren(...summary);
      const fields = decoded.elements?.length
        ? decoded.elements.map((field) => [
            field.name || field.hex,
            String(field.v),
          ])
        : (decoded.binaryAnalysis?.fields || []).map((field) => [
            field.name,
            `${field.interpretation || field.raw}${field.confidence ? ` · ${field.confidence}` : ""}`,
          ]);
      $("analysis-fields").replaceChildren(
        ...fields.map(([key, value]) => {
          const row = document.createElement("div");
          row.className = "analysis-field";
          row.append(text("span", key), text("span", value));
          return row;
        }),
      );
      if (!fields.length)
        $("analysis-fields").append(
          text(
            "p",
            "No decoded fields. Original bytes remain available.",
            "analysis-note",
          ),
        );
      const notes = [
        ...(decoded.notes || []),
        ...(decoded.binaryAnalysis?.re_notes || []),
      ];
      $("analysis-notes").replaceChildren(
        ...notes.slice(0, 12).map((note) => text("p", note, "analysis-note")),
      );
      $("analysis-export").textContent =
        decoded.exportText || JSON.stringify(decoded, null, 2);
    } catch (error) {
      if (revision !== analysisRevision) return;
      $("analysis-summary").replaceChildren(
        text("span", "Analysis unavailable", "badge warning"),
      );
      $("analysis-notes").replaceChildren(
        text("p", error.message, "analysis-note"),
      );
    }
  }
  $("copy-packet").onclick = () =>
    busy($("copy-packet"), async () => {
      if (selected) {
        await navigator.clipboard.writeText(
          selected.k === "cloud" ? selected.raw : selected.hex,
        );
        toast("Packet content copied.");
      }
    });
  $("use-packet").onclick = () => {
    if (!selected || selected.k === "cloud") return;
    $("inject-hex").value = selected.hex;
    $("inject-direction").value =
      selected.k === "rx" ? "fromDevice" : "toDevice";
    $("inject-hex").focus();
  };
  $("export-analysis").onclick = () => {
    if (analysis)
      UI.download(
        "rusthinq-packet-analysis.txt",
        analysis.exportText || JSON.stringify(analysis, null, 2),
      );
  };
  $("export-capture").onclick = () => {
    const output = [];
    if (evicted)
      output.push({
        k: "lost",
        t: records[0]?.t || Date.now(),
        events: evicted,
        reason: "browser buffer limit",
      });
    output.push(
      ...records.map(({ key, ...record }) => {
        if (["lost", "note", "cloud"].includes(record.k)) return record;
        let type = "packet",
          hex = record.hex;
        try {
          const bytes = Uint8Array.from(hex.match(/.{2}/g) || [], (value) =>
            parseInt(value, 16),
          );
          const payload = new TextDecoder("utf-8", { fatal: true }).decode(
            bytes,
          );
          const value = JSON.parse(payload);
          if (value.cmd === "ack" && typeof value.data === "string") {
            type = "ack";
            hex = value.data;
          } else if (value.Body?.Format === "B64") {
            hex = Array.from(atob(value.Body.Data), (value) =>
              value.charCodeAt(0).toString(16).padStart(2, "0"),
            ).join("");
          } else {
            type = "clip";
            hex = payload;
          }
        } catch {}
        return { ...record, type, hex };
      }),
    );
    UI.download(
      `rusthinq-capture-${new Date().toISOString().replaceAll(":", "-")}.jsonl`,
      output.map((record) => JSON.stringify(record)).join("\n") + "\n",
      "application/x-ndjson",
    );
    toast(
      "Exported the buffered capture. Packet, clip and acknowledgment records are compatible with CLI replay; loss markers are preserved.",
    );
  };
  $("pause-stream").onclick = () => {
    paused = !paused;
    $("pause-stream").textContent = paused ? "▷" : "Ⅱ";
    $("pause-stream").title = paused ? "Resume display" : "Pause display";
    $("pause-stream").setAttribute("aria-label", $("pause-stream").title);
    $("pause-stream").setAttribute("aria-pressed", String(paused));
    render();
  };
  $("clear-stream").onclick = () => {
    records.length = 0;
    bufferedBytes = 0;
    nodes.clear();
    $("messages").replaceChildren();
    rx = tx = cloud = loss = evicted = 0;
    selected = analysis = null;
    analysisRevision++;
    $("analysis-empty").hidden = false;
    $("analysis-content").hidden = true;
    $("analysis-direction").textContent = "Select a packet";
    render();
  };
  document.querySelectorAll("[data-direction]").forEach(
    (button) =>
      (button.onclick = () => {
        direction = button.dataset.direction;
        document.querySelectorAll("[data-direction]").forEach((node) => {
          node.classList.toggle("active", node === button);
          node.setAttribute("aria-pressed", String(node === button));
        });
        render();
      }),
  );
  $("injection").onchange = () => {
    const wanted = $("injection").checked;
    $("injection").disabled = true;
    (async () => {
      try {
        const result = await api("api/raw-inject", { enabled: wanted });
        injection = result.enabled === true;
      } catch (error) {
        toast(error.message, "error");
      } finally {
        applyInjection();
      }
    })();
  };
  $("inject-form").onsubmit = async (event) => {
    event.preventDefault();
    if (!connected || !online || !injection || stale || sending) return;
    const hex = $("inject-hex").value.replace(/\s+/g, "");
    if (!hex || hex.length % 2 || !/^[0-9a-f]+$/i.test(hex)) {
      toast("Enter complete hexadecimal byte pairs.", "error");
      return;
    }
    const direction = $("inject-direction").value,
      revision = connectionRevision;
    sending = true;
    applyInjection();
    try {
      const allowed = await confirm(
        direction === "toDevice"
          ? "Send this packet to the device?"
          : "Simulate this received packet?",
        `${hex.length / 2} bytes will ${direction === "toDevice" ? "be written to the connected device" : "enter the device processing path"}.`,
        "Send packet",
      );
      if (!allowed) return;
      if (
        !connected ||
        !online ||
        !injection ||
        stale ||
        revision !== connectionRevision
      )
        throw new Error(
          "The connection changed. Review the packet again before sending.",
        );
      socket.send(
        direction === "toDevice"
          ? { sendToDevice: hex }
          : { sendFromDevice: hex },
      );
      $("delivery-result").textContent =
        "Submitted. Waiting for the runtime result…";
    } catch (error) {
      toast(error.message, "error");
    } finally {
      sending = false;
      applyInjection();
    }
  };
  if (!id) {
    $("device-title").textContent =
      "Open a device from the workspace to inspect traffic.";
    $("monitor-connection").textContent = "No device";
    return;
  }
  const socket = UI.socket(`device?id=${encodeURIComponent(id)}`, {
    open: () => {
      stale = false;
      state(true);
    },
    close: () => state(false),
    error: (error) => toast(error.message, "error"),
    message(value) {
      if (value.rx) recordPacket("rx", value.rx, value.injected);
      if (value.tx) recordPacket("tx", value.tx, value.injected);
      if (value.lostEvents) recordLoss(Number(value.lostEvents));
      if (value.error) {
        $("delivery-result").textContent = value.error;
        toast(value.error, "error");
      }
      if (value.delivery)
        $("delivery-result").textContent =
          `Transport: ${value.delivery}. This does not confirm appliance acceptance.`;
      if (value.injected === true && !value.rx && !value.tx)
        $("delivery-result").textContent =
          "Received data admitted to the processing path.";
      if (value.status) {
        online = value.status === "online";
        injection = value.injectionEnabled === true;
        stale = value.sessionChanged === true;
        if (stale) connectionRevision++;
        $("injection-control").hidden = !value.injectionToggle;
        $("monitor-connection").textContent = stale
          ? "Connection changed"
          : online
            ? "Device online"
            : "Device offline";
        $("monitor-connection").className =
          `badge ${online && !stale ? "good" : "warning"}`;
        $("monitor-notice").hidden = !stale;
        $("monitor-notice").textContent =
          "The device connection changed. Refresh the monitor connection before sending another packet.";
        applyInjection();
      }
      if (value.meta) {
        $("device-title").textContent =
          value.meta.name ||
          value.meta.modelName ||
          value.meta.modelId ||
          "Model not reported";
        $("device-title").dataset.model = value.meta.modelId || "";
      }
    },
  });
  $("reconnect-monitor").onclick = () => socket.reconnect();
})();
