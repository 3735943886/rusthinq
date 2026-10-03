/* One account observer shared by all dashboards/studios; starting never changes a device. */
window.CloudFeed = class {
  constructor({
    badge,
    button,
    container,
    onRecord,
    onReset,
    device,
    history = false,
  }) {
    Object.assign(this, {
      badge,
      button,
      container,
      onRecord,
      onReset,
      device,
      history,
    });
    this.cursor = "0";
    this.rows = [];
    this.bytes = 0;
    this.enabled = false;
    this.available = false;
    if (button)
      button.onclick = () =>
        UI.busy(button, async () => {
          const value = await UI.api("api/cloud/notifications", {
            enabled: !this.enabled,
          });
          this.status(value);
        });
    this.start().catch((error) => {
      this.badge.textContent = error.message;
    });
  }
  status(value) {
    this.enabled = value.enabled === true;
    if (value.available !== undefined) this.available = value.available;
    this.badge.textContent = this.available
      ? `LG feed · ${value.status || "disabled"}`
      : "LG feed unavailable";
    this.badge.className = `badge ${value.status === "connected" ? "good" : this.enabled ? "warning" : "neutral"}`;
    if (this.button) {
      this.button.textContent = this.enabled ? "Stop LG feed" : "Start LG feed";
      this.button.disabled = !this.available;
    }
  }
  async start() {
    const state = await UI.api("api/cloud/notifications?limit=0");
    this.status(state);
    if (!this.available) return;
    this.cursor = this.history
      ? BigInt(state.cursor || "0") > 200n
        ? String(BigInt(state.cursor) - 200n)
        : "0"
      : state.cursor || "0";
    this.socket = UI.socket(
      () =>
        `api/cloud/notifications/ws?cursor=${this.cursor}${this.device ? "&device=" + encodeURIComponent(this.device) : ""}`,
      {
        close: () => {
          this.badge.textContent = "LG feed connection interrupted";
          this.onRecord?.({
            k: "note",
            t: Date.now(),
            text: "LG notification stream interrupted; correlation may be incomplete.",
          });
        },
        message: (value) => {
          if (value.type === "cloudSnapshot") {
            const snapshot = value.snapshot;
            this.status(snapshot);
            if (snapshot.reset) {
              this.cursor = "0";
              this.rows = [];
              this.bytes = 0;
              this.container?.replaceChildren();
              this.onReset?.();
              this.onRecord?.({
                k: "note",
                t: Date.now(),
                text: "LG observation runtime restarted; history reset.",
              });
            }
            if (snapshot.lost)
              this.accept({
                type: "cloudLoss",
                k: "lost",
                t: Date.now(),
                events: null,
                reason: "LG notification buffer overflow while disconnected",
              });
            for (const event of snapshot.events || []) this.accept(event);
            this.cursor = snapshot.cursor || this.cursor;
          } else if (value.type === "cloudStatus") {
            this.status(value);
            this.onRecord?.({
              k: "note",
              t: Date.now(),
              text: `LG notification feed: ${value.status}`,
            });
          } else if (value.type === "cloudReset") {
            this.rows = [];
            this.bytes = 0;
            this.container?.replaceChildren();
            this.onReset?.();
          } else this.accept(value);
        },
        error: (error) => UI.toast(error.message, "error"),
      },
    );
  }
  accept(value) {
    if (value.type === "cloudLoss") {
      this.onRecord?.({
        k: "lost",
        t: value.t || Date.now(),
        events: value.events ?? null,
        source: "cloud",
        reason: value.reason || "LG notification subscriber missed events",
      });
      return;
    }
    if (value.type !== "cloudNotification") return;
    if (BigInt(value.sequence || "0") <= BigInt(this.cursor)) return;
    this.cursor = value.sequence;
    if (this.container) {
      const row = UI.text("article", "", "cloud-row");
      row.append(
        UI.text(
          "small",
          `${new Date(value.t).toLocaleTimeString()} · ${value.correlation === "account" ? "Account event (device unknown)" : value.devices.join(", ")}`,
          "muted",
        ),
        UI.text("strong", value.topic),
        UI.text("pre", value.raw),
      );
      this.container.prepend(row);
      const bytes = (value.raw.length + value.topic.length) * 2;
      this.bytes += bytes;
      this.rows.push({ node: row, bytes });
      while (this.rows.length > 200 || this.bytes > 8 * 1024 * 1024) {
        const old = this.rows.shift();
        this.bytes -= old.bytes;
        old.node.remove();
      }
    }
    this.onRecord?.({ ...value, observedAt: value.t, t: Date.now() });
  }
};
