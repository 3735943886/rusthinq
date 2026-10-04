const { chromium } = require("playwright");
const fs = require("fs");
const path = require("node:path");
const base = process.env.GUI_TEST_URL || "http://127.0.0.1:8765/";
const artifacts = process.env.GUI_TEST_ARTIFACTS || "/tmp/rusthinq-gui-check";
fs.mkdirSync(artifacts, { recursive: true });
const assert = require("node:assert/strict");
(async () => {
  const browser = await chromium.launch({ args: ["--no-sandbox"] });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1050 },
    colorScheme: "light",
  });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const requests = [],
    sockets = {};
  const fixture = {
    version: "0.2.0-dev",
    features: { scripting: true, bridge: true },
    devices: {
      "washer-01": {
        name: "Laundry room",
        model: "F24VDD",
        modelName: "Front-load washer",
        modelId: "F24VDD",
        swVersion: "1.2.0",
        deviceType: "201",
        platform: "ThinQ2",
        online: true,
        mapped: true,
        incarnation: "9007199254740993",
        generation: "11",
        scriptGeneration: "1",
        driverReloadable: true,
        bridgePaired: true,
        bridgeEnabled: true,
        bridged: true,
        lastSeenUnix: Math.floor(Date.now() / 1000),
      },
      "climate-01": {
        name: "Living room",
        model: "AIR_910604_WW",
        deviceType: "401",
        platform: "ThinQ2",
        online: true,
        mapped: true,
        incarnation: "2",
        generation: "12",
        scriptGeneration: "1",
        driverReloadable: true,
        bridgePaired: false,
      },
      "dryer-01": {
        name: "Utility room",
        model: "D140110",
        platform: "ThinQ1",
        online: false,
        incarnation: "3",
        generation: null,
        lastSeenUnix: Math.floor(Date.now() / 1000) - 3600,
        bridgePaired: true,
        bridgeEnabled: false,
      },
      "dishwasher-01": {
        name: "Kitchen",
        model: "2RSFL2DBN3K_Z",
        platform: "ThinQ2",
        online: true,
        mapped: true,
        scriptFaulted: true,
        incarnation: "4",
        generation: "14",
        scriptGeneration: "1",
        driverReloadable: true,
      },
      "air-02": {
        name: "Bedroom",
        model: "CST_570004_WW",
        platform: "ThinQ2",
        online: true,
        mapped: true,
        incarnation: "5",
        generation: "15",
        scriptGeneration: "1",
        bridgePaired: true,
        bridgeEnabled: true,
        bridgeError: "Cloud connection interrupted",
      },
      hostile: {
        name: '<img src=x onerror="window.bad=true">',
        model: "Unmapped",
        platform: "ThinQ2",
        online: false,
        incarnation: "6",
        generation: null,
      },
    },
  };
  let cloudStatus = {
    available: true,
    enabled: false,
    status: "disabled",
    cursor: "0",
    events: [],
    nextCursor: "0",
    lost: false,
  };
  const descriptor = {
    il: 0,
    id: "washer-01",
    "x-mqtt": { state: "rusthinq/{id}/{prop}" },
    props: {
      power: { type: "binary", rw: true, label: "Power" },
      mode: { type: "select", rw: true, options: ["eco", "normal"] },
      temperature: {
        type: "number",
        rw: true,
        min: 10,
        max: 80,
        step: 5,
        unit: "°C",
      },
      remaining: { type: "number", unit: "min" },
      start: { type: "trigger", requires: "remote_start" },
      remote_start: { type: "binary" },
    },
  };
  let presentation = {
    publications: [
      {
        topic: "il/washer-01",
        payload: JSON.stringify(descriptor),
        retain: true,
      },
      ...Object.entries({
        power: "true",
        mode: "eco",
        temperature: "30",
        remaining: "25",
        remote_start: "false",
      }).map(([prop, payload]) => ({
        topic: "rusthinq/washer-01/" + prop,
        payload,
        retain: true,
      })),
    ],
  };
  let monitorStatus = {
    status: "online",
    injectionEnabled: false,
    injectionToggle: true,
    meta: { modelId: "F24VDD", modelName: "Laundry room" },
  };
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url());
    const body = route.request().postDataJSON();
    requests.push({ path: url.pathname, body });
    let result = {};
    if (url.pathname === "/api/devices") result = fixture;
    else if (url.pathname === "/api/cloud")
      result = {
        enabled: true,
        account: { loggedIn: true, stored: true, busy: false },
      };
    else if (url.pathname === "/api/cloud/notifications") {
      if (body?.enabled !== undefined)
        cloudStatus = {
          ...cloudStatus,
          enabled: body.enabled,
          status: body.enabled ? "connected" : "disabled",
        };
      result = cloudStatus;
    } else if (url.pathname.endsWith("/presentation")) result = presentation;
    else if (url.pathname === "/api/diagnostics")
      result = {
        version: "0.2",
        runtime: { counters: { received: 12 } },
        privacy: "No credentials",
      };
    else if (url.pathname === "/api/mqtt")
      result = { status: "Connected", droppedTransient: 3 };
    else if (url.pathname === "/api/health")
      result = { running: true, version: "0.2.0-dev", retainedCleanup: "Idle" };
    else if (url.pathname === "/api/cloud/inventory")
      result = [{ deviceId: "washer-01", alias: "Laundry room" }];
    else if (url.pathname === "/api/raw-inject") {
      monitorStatus.injectionEnabled = body?.enabled === true;
      result = { enabled: monitorStatus.injectionEnabled };
    } else if (url.pathname === "/api/packets/decode")
      result = {
        protocol: "Aabb",
        crcOk: true,
        elements: [],
        binaryAnalysis: {
          fields: [
            {
              name: "phase",
              raw: 2,
              interpretation: "Drying",
              confidence: "observed",
            },
          ],
        },
        notes: ["Captured appliance body"],
        exportText: "MODEL: F24VDD\nphase: Drying",
      };
    else if (url.pathname.endsWith("/invoke")) result = { sequence: "123" };
    else if (url.pathname === "/api/mqtt/retained/delete")
      result = { confirmed: 2 };
    await route.fulfill({ json: result });
  });
  await page.route("**/monitor?*", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: fs.readFileSync(
        path.join(__dirname, "../crates/rusthinq-app/assets/monitor.html"),
        "utf8",
      ),
    }),
  );
  await page.routeWebSocket("**/ws", (ws) => {
    sockets.panel = ws;
    setTimeout(() => ws.send(JSON.stringify(fixture)), 30);
  });
  await page.routeWebSocket("**/api/cloud/notifications/ws?*", (ws) => {
    sockets.cloud = ws;
    setTimeout(
      () =>
        ws.send(
          JSON.stringify({ type: "cloudSnapshot", snapshot: cloudStatus }),
        ),
      20,
    );
  });
  await page.routeWebSocket("**/api/events", (ws) => {
    sockets.events = ws;
  });
  await page.routeWebSocket("**/device?*", (ws) => {
    sockets.monitor = ws;
    setTimeout(() => ws.send(JSON.stringify(monitorStatus)), 30);
    ws.onMessage((message) => {
      requests.push({ path: "ws-send", body: JSON.parse(message) });
      ws.send(JSON.stringify({ delivery: "Sent" }));
    });
  });
  await page.goto(base);
  await page.locator(".device-card").first().waitFor();
  assert.equal(await page.locator(".device-card").count(), 6);
  assert.equal(await page.locator("h1").first().textContent(), "Devices");
  const beforeNameRefresh = requests.filter(request => request.path === "/api/cloud/inventory").length;
  await page.locator("#refresh-devices").click();
  await page.waitForFunction(() => !document.getElementById("refresh-devices").disabled);
  assert.ok(requests.filter(request => request.path === "/api/cloud/inventory").length > beforeNameRefresh);
  const typeCatalog = await page.evaluate(() => Object.entries(UI.applianceTypes));
  assert.equal(typeCatalog.filter(([type]) => /^\d+$/.test(type)).length, 40);
  assert.equal(await page.locator("#device-types option").count(), 40);
  for (const [deviceType, info] of typeCatalog) {
    const rendered = await page.evaluate(type => ({name: UI.applianceIcon({deviceType: type}), svg: UI.icon(UI.applianceIcon({deviceType: type}))}), deviceType);
    assert.equal(rendered.name, info.icon);
    assert.ok(rendered.svg.includes("<svg"));
    assert.notEqual(rendered.svg, await page.evaluate(() => UI.icon("device")));
  }
  const applianceIcons = await page.locator(".device-card .device-symbol").evaluateAll(nodes => nodes.map(node => node.innerHTML));
  assert.ok(new Set(applianceIcons).size >= 3);
  assert.equal(await page.evaluate(() => UI.applianceIcon({deviceType: "unknown", model: "Washer"})), "device");
  for (const [deviceType, expected] of Object.entries({101:"refrigerator",103:"water",201:"washer",202:"dryer",203:"styler",204:"dishwasher",223:"tower",301:"oven",302:"microwave",303:"cooktop",401:"air",402:"purifier",403:"dehumidifier"})) {
    assert.equal(await page.evaluate(type => UI.applianceIcon({deviceType: type}), deviceType), expected);
  }

  assert.equal(await page.locator("#stat-online").textContent(), "4");
  assert.equal(await page.evaluate(() => !!window.bad), false);
  await page.waitForTimeout(400);
  await page.screenshot({
    path: path.join(artifacts, "dashboard-desktop.png"),
    fullPage: true,
  });
  await page.locator("#device-search").fill("Laundry");
  assert.equal(await page.locator(".device-card:visible").count(), 1);
  await page
    .getByRole("button", { name: "View device", exact: true })
    .filter({ visible: true })
    .click();
  await page.locator(".control-row").first().waitFor();
  await page.screenshot({
    path: path.join(artifacts, "controls-desktop.png"),
    fullPage: true,
  });
  const power = page
    .locator(".control-row")
    .filter({ has: page.locator("label", { hasText: "Power" }) });
  assert.equal(await power.getByRole("switch").isChecked(), true);
  const start = page
    .locator(".control-row")
    .filter({ has: page.locator("label", { hasText: /^start$/ }) });
  assert.equal(await start.locator("button").isDisabled(), true);
  const remaining = page
    .locator(".control-row")
    .filter({ has: page.locator("label", { hasText: /^remaining$/ }) });
  assert.equal(await remaining.locator(".badge").textContent(), "Read only");
  await power.getByRole("switch").uncheck();
  await power.locator("button").click();
  await page.waitForFunction(() =>
    document.querySelector(".control-result").textContent.includes("Queued"),
  );
  const widget = requests.filter((r) => r.path.endsWith("/invoke")).at(-1);
  assert.deepEqual(JSON.parse(widget.body.input), {
    prop: "power",
    value: "false",
  });
  sockets.events.send(
    JSON.stringify({
      type: "scriptExecuted",
      context: {
        device: "washer-01",
        incarnation: "9007199254740993",
        generation: "11",
        scriptGeneration: "1",
      },
      sequence: "123",
    }),
  );
  await page.waitForFunction(() =>
    document
      .querySelector(".control-result")
      .textContent.includes("Driver executed"),
  );
  requests.length = 0;
  await page
    .locator("#advanced-command")
    .evaluate((node) => (node.open = true));
  await page.locator("#property-name").fill("power");
  await page.locator("#property-value").fill("on");
  await page.locator("#property-submit").click();
  await page.waitForFunction(() =>
    document.querySelector("#property-result").textContent.includes("queued"),
  );
  assert.equal(
    requests.find((r) => r.path.endsWith("/invoke")).body.incarnation,
    "9007199254740993",
  );
  fixture.devices["washer-01"].generation = "99";
  sockets.panel.send(JSON.stringify(fixture));
  await page.waitForFunction(
    () => !document.querySelector("#detail-status").hidden,
  );
  assert.equal(await page.locator("#property-submit").isDisabled(), true);
  assert.equal(await power.locator("button").isDisabled(), true);
  await page.locator("#device-dialog [data-close]").click();
  await page.locator("#device-search").fill("");
  await page.locator("[data-filter=attention]").click();
  assert.equal(await page.locator(".device-card:visible").count(), 2);
  await page.locator("[data-filter=all]").click();
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
    true,
  );
  await page.screenshot({
    path: path.join(artifacts, "dashboard-mobile.png"),
    fullPage: true,
  });
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.locator("#theme-toggle").click();
  await page.screenshot({
    path: path.join(artifacts, "dashboard-dark.png"),
    fullPage: true,
  });
  await page.locator("[data-view=system]").click();
  const diagnosis = page.waitForEvent("download");
  await page.locator("#download-diagnostics").click();
  const report = JSON.parse(
    fs.readFileSync(await (await diagnosis).path(), "utf8"),
  );
  assert.equal(report.privacy, "No credentials");
  await page.locator("#cleanup-scope").selectOption("all");
  await page.locator("#cleanup-submit").click();
  assert.equal(await page.locator("#confirm-dialog").isVisible(), true);
  assert.equal(
    requests.some((r) => r.path === "/api/mqtt/retained/delete"),
    false,
  );
  await page.locator("#confirm-accept").click();
  await page.waitForFunction(() =>
    document.querySelector("#cleanup-result").textContent.includes("confirmed"),
  );
  for (let i = 0; i < 120; i++)
    sockets.events.send(
      JSON.stringify({
        type: "scriptExecuted",
        context: {
          device: "washer-01",
          incarnation: "9007199254740993",
          generation: "11",
          scriptGeneration: "1",
        },
        sequence: i,
        error: null,
      }),
    );
  await page.waitForFunction(
    () => document.querySelector("#activity-list").children.length === 100,
  );
  await page.goto(new URL("monitor?id=washer-01", base).href);
  await page.waitForFunction(
    () =>
      document.querySelector("#monitor-connection").textContent ===
      "Device online",
  );
  sockets.monitor.send(JSON.stringify({ rx: "AA0630F000BB" }));
  sockets.monitor.send(JSON.stringify({ tx: "AA0630F100BB" }));
  await page.locator(".packet-row").first().click();
  await page.waitForFunction(() =>
    document.querySelector("#analysis-summary").textContent.includes("Aabb"),
  );
  await page.screenshot({
    path: path.join(artifacts, "studio-desktop.png"),
    fullPage: true,
  });
  assert.equal(await page.locator("#inject-submit").isDisabled(), true);
  await page.locator("#injection").check();
  await page.waitForFunction(
    () => !document.querySelector("#inject-submit").disabled,
  );
  await page.locator("#inject-hex").fill("AA 06 30 F0 00 BB");
  await page.locator("#inject-submit").click();
  sockets.monitor.send(
    JSON.stringify({ ...monitorStatus, sessionChanged: true }),
  );
  await page.locator("#confirm-accept").click();
  assert.equal(
    requests.some((r) => r.path === "ws-send"),
    false,
  );
  for (let i = 0; i < 510; i++)
    sockets.monitor.send(JSON.stringify({ rx: "AA0630F000BB" }));
  await page.waitForFunction(() =>
    document
      .querySelector("#buffer-count")
      .textContent.includes("500 buffered"),
  );
  assert.ok((await page.locator(".packet-row").count()) <= 500);
  await page.locator("#studio-cloud-toggle").click();
  sockets.cloud.send(
    JSON.stringify({ type: "cloudStatus", enabled: true, status: "connected" }),
  );
  sockets.cloud.send(
    JSON.stringify({
      type: "cloudNotification",
      k: "cloud",
      sequence: "1",
      t: Date.now(),
      topic: "account/state",
      raw: '{"deviceId":"washer-01","power":true}',
      payload: { deviceId: "washer-01", power: true },
      devices: ["washer-01"],
      correlation: "device",
    }),
  );
  await page.waitForFunction(
    () => document.querySelector("#cloud-count").textContent === "1",
  );
  await page.locator('[data-direction="cloud"]').click();
  assert.equal(await page.locator(".packet-row").count(), 1);
  await page.locator(".packet-row").click();
  assert.ok(
    (await page.locator("#analysis-direction").textContent()).includes(
      "LG notification",
    ),
  );
  assert.equal(await page.locator("#use-packet").isDisabled(), true);
  await page.locator('[data-direction="all"]').click();
  const wire = (value) => Buffer.from(JSON.stringify(value)).toString("hex");
  sockets.monitor.send(
    JSON.stringify({
      rx: wire({
        Body: {
          Format: "B64",
          Data: Buffer.from([1, 2, 3]).toString("base64"),
        },
      }),
    }),
  );
  sockets.monitor.send(
    JSON.stringify({ rx: wire({ cmd: "ack", data: "ack-data" }) }),
  );
  sockets.monitor.send(
    JSON.stringify({ rx: wire({ cmd: "status", data: { power: 1 } }) }),
  );
  await page.waitForTimeout(250);
  const exported = page.waitForEvent("download");
  await page.locator("#export-capture").click();
  const capture = await exported;
  const lines = require("fs")
    .readFileSync(await capture.path(), "utf8")
    .trim()
    .split("\n")
    .map(JSON.parse);
  assert.ok(
    lines.some((value) => value.type === "packet" && value.hex === "010203"),
  );
  assert.ok(
    lines.some((value) => value.type === "ack" && value.hex === "ack-data"),
  );
  assert.ok(
    lines.some(
      (value) =>
        value.type === "clip" && JSON.parse(value.hex).cmd === "status",
    ),
  );
  assert.ok(lines.some((value) => value.k === "lost"));
  assert.ok(
    lines.some(
      (value) => value.k === "cloud" && value.payload.deviceId === "washer-01",
    ),
  );
  const huge = "ab".repeat(1050000);
  for (let i = 0; i < 3; i++)
    sockets.monitor.send(JSON.stringify({ rx: huge }));
  await page.waitForFunction(
    () => document.querySelectorAll(".packet-row").length <= 2,
  );
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
    true,
  );
  await page.screenshot({
    path: path.join(artifacts, "studio-mobile.png"),
    fullPage: true,
  });
  assert.deepEqual(errors, []);
  console.log(
    "Browser checks passed: typed controls/read-only/conditions, driver execution feedback, diagnostics, dashboard/mobile/dark, scope fencing, LG timeline/filter, correlated export and bounded buffers.",
  );
  await browser.close();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
