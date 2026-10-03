/* Presentation-only IL widgets. The runtime continues to route opaque script output. */
window.DeviceControls = (() => {
  const { text, confirm } = UI;
  const yes = (value) => [true, "true", "on", "1", 1].includes(value);
  function projection(publications, id) {
    const messages = new Map(
      (Array.isArray(publications) ? publications : [])
        .slice(0, 256)
        .filter(
          (v) =>
            v && typeof v.topic === "string" && typeof v.payload === "string",
        )
        .map((v) => [v.topic, v.payload]),
    );
    let descriptor = null;
    for (const payload of messages.values()) {
      try {
        const value = JSON.parse(payload);
        if (
          value?.id === id &&
          value.props &&
          typeof value.props === "object" &&
          !Array.isArray(value.props) &&
          value.il === 0
        ) {
          descriptor = value;
          break;
        }
      } catch {}
    }
    const values = Object.create(null);
    const pattern = descriptor?.["x-mqtt"]?.state;
    if (typeof pattern === "string")
      for (const prop of Object.keys(descriptor.props).slice(0, 256)) {
        const topic = pattern.replace("{id}", id).replace("{prop}", prop);
        if (messages.has(topic)) values[prop] = messages.get(topic);
      }
    return { descriptor, values };
  }
  function allowed(def, values) {
    if (!def.requires) return true;
    if (typeof def.requires === "string") return yes(values[def.requires]);
    return (
      typeof def.requires === "object" &&
      Array.isArray(def.requires.in) &&
      def.requires.in.map(String).includes(String(values[def.requires.prop]))
    );
  }
  class Panel {
    constructor(container, send) {
      this.container = container;
      this.send = send;
      this.rows = new Map();
      this.token = "";
      this.revision = 0;
      this.queued = new Map();
      this.executed = new Map();
    }
    reset(target) {
      const token = JSON.stringify(target);
      if (token === this.token) return;
      this.token = token;
      this.target = target;
      this.signature = "";
      this.rows.clear();
      this.queued.clear();
      this.executed.clear();
      this.revision++;
      this.container.replaceChildren(
        text("p", "Loading device controls…", "muted"),
      );
    }
    render(publications, target, blocked) {
      this.reset(target);
      this.blocked = blocked;
      const { descriptor, values } = projection(publications, target.id);
      this.values = values;
      const signature = JSON.stringify(descriptor?.props || null);
      if (signature !== this.signature) {
        this.signature = signature;
        this.rows.clear();
        this.container.replaceChildren();
        if (!descriptor) {
          this.container.append(
            text(
              "p",
              "This driver has not published a control description. Advanced commands remain available.",
              "muted",
            ),
          );
          return;
        }
        const entries = Object.entries(descriptor.props).slice(0, 256);
        for (const [prop, def] of entries) {
          if (!def || typeof def !== "object") continue;
          const row = document.createElement("form");
          row.className = "control-row";
          const label = text("label", def.label || prop);
          const value = text("output", "—", "control-value");
          const result = text("p", "", "small control-result");
          result.setAttribute("role", "status");
          row.append(label, value);
          const writable = def.rw === true || def.type === "trigger";
          let input = null,
            button = null;
          if (
            writable &&
            ["binary", "select", "number", "string", "trigger"].includes(
              def.type,
            )
          ) {
            if (def.type === "binary") {
              input = document.createElement("input");
              input.type = "checkbox";
              input.setAttribute("role", "switch");
            } else if (def.type === "select") {
              input = document.createElement("select");
              const options =
                def.type === "binary"
                  ? ["false", "true"]
                  : Array.isArray(def.options)
                    ? def.options.slice(0, 256)
                    : [];
              for (const option of options) {
                const node = text(
                  "option",
                  def.type === "binary"
                    ? option === "true"
                      ? "On"
                      : "Off"
                    : String(option),
                );
                node.value = String(option);
                input.append(node);
              }
            } else if (def.type !== "trigger") {
              input = document.createElement("input");
              input.type = def.type === "number" ? "number" : "text";
              input.maxLength = 1024;
              if (def.type === "number") {
                input.step =
                  Number.isFinite(def.step) && def.step > 0
                    ? String(def.step)
                    : "any";
                if (Number.isFinite(def.min)) input.min = String(def.min);
                if (Number.isFinite(def.max)) input.max = String(def.max);
              }
              input.required = true;
            }
            if (input) {
              const key = `control-${this.rows.size}`;
              input.id = key;
              label.htmlFor = key;
              input.setAttribute("aria-label", String(def.label || prop));
              row.append(input);
              input.addEventListener("input", () => {
                row.dataset.edited = "true";
              });
            }
            button = text(
              "button",
              def.type === "trigger" ? "Run" : "Apply",
              "button secondary",
            );
            button.type = "submit";
            row.append(button);
            row.onsubmit = async (event) => {
              event.preventDefault();
              if (this.blocked || button.disabled || !allowed(def, this.values))
                return;
              const revision = this.revision,
                target = { ...this.target };
              const next =
                def.type === "trigger"
                  ? "true"
                  : def.type === "binary"
                    ? input.checked
                      ? "true"
                      : "false"
                    : input.value;
              button.disabled = true;
              row.dataset.sending = "true";
              result.textContent = "Preparing command…";
              try {
                if (
                  def.type === "trigger" &&
                  !(await confirm(
                    `Run ${def.label || prop}?`,
                    "The driver will validate the current appliance conditions before sending this command.",
                    "Run command",
                  ))
                ) {
                  result.textContent = "";
                  return;
                }
                if (
                  revision !== this.revision ||
                  this.blocked ||
                  !allowed(def, this.values)
                )
                  throw new Error(
                    "Device or command conditions changed. Reopen the device controls.",
                  );
                const response = await this.send(target, prop, next);
                if (revision !== this.revision) return;
                row.dataset.edited = "";
                result.textContent =
                  "Queued. Awaiting driver execution; appliance acceptance is not confirmed.";
                if (response.sequence) {
                  this.queued.set(String(response.sequence), result);
                  const earlier = this.executed.get(String(response.sequence));
                  if (earlier) this.event(earlier);
                }
              } catch (error) {
                if (revision === this.revision)
                  result.textContent = error.message;
              } finally {
                row.dataset.sending = "";
                if (revision === this.revision)
                  button.disabled = this.blocked || !allowed(def, this.values);
              }
            };
          } else row.append(text("span", "Read only", "badge neutral"));
          row.append(result);
          this.container.append(row);
          this.rows.set(prop, { row, def, value, input, button, result });
        }
      }
      for (const [prop, item] of this.rows) {
        item.value.textContent =
          values[prop] === undefined
            ? "Not reported"
            : `${values[prop]}${item.def.unit ? " " + item.def.unit : ""}`;
        if (
          item.input &&
          !item.row.dataset.edited &&
          document.activeElement !== item.input &&
          values[prop] !== undefined
        )
          if (item.def.type === "binary")
            item.input.checked = yes(values[prop]);
          else item.input.value = String(values[prop]);
        const unmet = !allowed(item.def, values);
        if (item.button)
          item.button.disabled = blocked || unmet || !!item.row.dataset.sending;
        if (item.input) item.input.disabled = blocked || unmet;
        item.row.title = unmet ? "Required appliance condition is not met" : "";
        if (unmet && !item.result.textContent)
          item.result.textContent = "Required appliance condition is not met.";
        if (
          !unmet &&
          item.result.textContent === "Required appliance condition is not met."
        )
          item.result.textContent = "";
      }
    }
    fence(blocked) {
      this.blocked = blocked;
      for (const item of this.rows.values()) {
        if (item.button)
          item.button.disabled =
            blocked ||
            !!item.row.dataset.sending ||
            !allowed(item.def, this.values || {});
        if (item.input)
          item.input.disabled =
            blocked ||
            !!item.row.dataset.sending ||
            !allowed(item.def, this.values || {});
      }
    }
    event(event) {
      if (
        event.context?.device !== this.target?.id ||
        event.context.incarnation !== this.target.incarnation ||
        event.context.generation !== this.target.generation ||
        event.context.scriptGeneration !== this.target.script_generation
      )
        return;
      if (event.type === "scriptExecuted") {
        this.executed.set(String(event.sequence), event);
        while (this.executed.size > 128)
          this.executed.delete(this.executed.keys().next().value);
        const result = this.queued.get(String(event.sequence));
        if (result) {
          result.textContent = event.error
            ? "Driver failed: " + event.error
            : "Driver executed. Check the reported appliance state for the result.";
          this.queued.delete(String(event.sequence));
        }
      }
      if (event.type === "scriptOutput") {
        try {
          const publication = JSON.parse(event.payload);
          if (publication.topic?.endsWith("/reject")) {
            const rejection = JSON.parse(publication.payload);
            const row = this.rows.get(rejection.prop);
            if (row)
              row.result.textContent =
                "Rejected: " +
                (rejection.reason || rejection.code || "driver validation");
          }
        } catch {}
      }
    }
  }
  return { Panel, projection };
})();
