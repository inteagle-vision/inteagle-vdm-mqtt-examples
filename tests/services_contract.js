#!/usr/bin/env node
"use strict";
// Language-neutral black-box suite: real MQTT broker, simulated device, actual HTTP.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const mqtt = require("../javascript/node_modules/mqtt");
const protobuf = require("../javascript/node_modules/protobufjs");
const httpServer = require("node:http");
const root = path.resolve(__dirname, "..");
const schema = protobuf.loadSync(
  path.join(root, "proto/inteagle_vdm_mqtt_v1.proto"),
);
const type = (n) => schema.lookupType("inteagle.vdm.mqtt.v1." + n);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(fn, label) {
  let last;
  for (let i = 0; i < 150; i++) {
    try {
      const r = await fn();
      if (r) return r;
    } catch (e) {
      last = e;
    }
    await sleep(100);
  }
  throw new Error(`Timeout: ${label}: ${last?.message || ""}`);
}
async function broker(port, label) {
  const c = mqtt.connect(`mqtt://127.0.0.1:${port}`, {
    clientId: "vdm-contract-simulator-" + label,
    connectTimeout: 3000,
    reconnectPeriod: 0,
  });
  await new Promise((r, j) => {
    c.once("connect", r);
    c.once("error", j);
  });
  await new Promise((r, j) =>
    c.subscribe("vdm/+/rpc/req", { qos: 1 }, (e) => (e ? j(e) : r())),
  );
  c.on("message", (topic, raw) => {
    const device = topic.split("/")[1];
    let req, field, method;
    const pb = device === "PB";
    if (pb) {
      req = type("RpcRequest").toObject(type("RpcRequest").decode(raw), {
        longs: String,
        enums: String,
      });
      field = Object.keys(req).find(
        (k) => !["schemaVersion", "reqId"].includes(k),
      );
      method = field;
    } else {
      req = JSON.parse(raw);
      field = req.method;
      method = req.method;
    }
    const params = pb ? req[field] : req.params;
    if (method === "ackEvidencePackage") {
      c.acks.push({ device, params });
    }
    const keys = params?.keys || [];
    if (keys.includes("timeout")) return;
    const response = pb
      ? {
          schemaVersion: 1,
          reqId: req.reqId,
          code: keys.includes("device-error") ? 2 : 0,
        }
      : { reqId: req.reqId, code: keys.includes("device-error") ? 2 : 0 };
    if (method === "ackEvidencePackage" && c.rejectNextAck) {
      response.code = 4;
      c.rejectNextAck = false;
    }
    if (!response.code) {
      const result =
        method === "getAttr"
          ? { deviceId: device, deviceModel: label }
          : method === "getAlarmState"
            ? { active: [] }
            : {};
      if (pb)
        response[field] =
          method === "getAttr" ? { attributes: result } : result;
      else response.data = result;
    }
    const payload = pb
      ? type("RpcResponse")
          .encode(type("RpcResponse").fromObject(response))
          .finish()
      : Buffer.from(JSON.stringify(response));
    // A same-ID response from another device must never settle this call.
    c.publish("vdm/WRONG/rpc/resp", payload, { qos: 1 });
    setTimeout(
      () => c.publish(`vdm/${device}/rpc/resp`, payload, { qos: 1 }),
      15,
    );
  });
  c.acks = [];
  c.rejectNextAck = false;
  return c;
}
async function publish(c, device, suffix, value) {
  const names = {
    telemetry: "Telemetry",
    attributes: "Attributes",
    "3A": "Alarm",
    event: "Event",
  };
  const payload =
    device === "PB"
      ? type(names[suffix])
          .encode(type(names[suffix]).fromObject(value))
          .finish()
      : Buffer.from(JSON.stringify(value));
  await new Promise((r, j) =>
    c.publish(`vdm/${device}/${suffix}`, payload, { qos: 1 }, (e) =>
      e ? j(e) : r(),
    ),
  );
}
async function run(language, brokers) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "vdm-" + language + "-"));
  const config = {
    connections: brokers.map((b, i) => ({
      id: "c" + i,
      host: "127.0.0.1",
      port: b.port,
      devices: [
        { id: "JSON", format: "json", capabilities: i ? ["motor"] : [] },
        { id: "PB", format: "protobuf", capabilities: i ? ["motor"] : [] },
      ],
    })),
    rpcTimeoutMs: 500,
    notificationMaxAgeSeconds: 300,
    maxInboxRows: 1000,
    retentionDays: 1,
    maxEvidenceBytes: 67108864,
  };
  const configPath = path.join(dir, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  const deliveries = [];
  const webhook = httpServer.createServer((req, res) => {
    let body = "";
    req.on("data", (b) => (body += b));
    req.on("end", () => {
      deliveries.push({
        key: req.headers["idempotency-key"],
        body: JSON.parse(body),
      });
      res.statusCode = deliveries.length === 1 ? 503 : 204;
      res.end();
    });
  });
  await new Promise((r) => webhook.listen(0, "127.0.0.1", r));
  const port = Number(process.env.VDM_TEST_HTTP_PORT || 18080);
  const base = `http://127.0.0.1:${port}`;
  const commands = {
    javascript: [process.execPath, ["service/main.js"]],
    python: [
      process.env.VDM_PYTHON || path.join(root, "python/.venv/bin/python"),
      ["-m", "vdm_service"],
    ],
    go: [
      process.env.VDM_GO_SERVICE || path.join(root, "go/bin/vdm-service"),
      [],
    ],
    java: ["java", ["-jar", "target/vdm-mqtt-consumer-1.0.0-service.jar"]],
  };
  const logs = fs.openSync(path.join(dir, "service.log"), "a");
  let child;
  const start = () => {
    const [bin, args] = commands[language];
    child = spawn(bin, args, {
      cwd: path.join(root, language),
      env: {
        ...process.env,
        VDM_SERVICE_CONFIG: configPath,
        VDM_DATA_DIR: path.join(dir, "data"),
        VDM_HTTP_HOST: "127.0.0.1",
        VDM_HTTP_PORT: String(port),
        VDM_API_TOKEN: "contract-token",
        VDM_WEBHOOK_URL: `http://127.0.0.1:${webhook.address().port}`,
      },
      stdio: ["ignore", logs, logs],
    });
    child.on("error", (e) => console.error(e));
  };
  const stop = async () => {
    if (!child || child.exitCode !== null) return;
    const done = new Promise((r) => child.once("exit", r));
    child.kill("SIGTERM");
    const timer = setTimeout(() => child.kill("SIGKILL"), 10000);
    await done;
    clearTimeout(timer);
  };
  const http = async (route, body) => {
    const r = await fetch(base + route, {
      method: body === undefined ? "GET" : "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer contract-token",
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(10000),
    });
    return { status: r.status, body: await r.json() };
  };
  const prefix = (c, d) => `/v1/connections/${c}/devices/${d}`;
  try {
    for (const b of brokers) {
      b.client.acks = [];
      b.client.rejectNextAck = true;
    }
    start();
    await until(async () => {
      const h = await http("/health");
      return h.body.status === "ok";
    }, language + " health");
    assert.equal((await http("/v1/devices")).body.devices.length, 4);
    assert.equal((await fetch(base + "/v1/devices")).status, 401);
    const calls = [];
    for (const c of ["c0", "c1"])
      for (const d of ["JSON", "PB"])
        calls.push(
          http(prefix(c, d) + "/device/attributes/query", { params: {} }),
        );
    const replies = await Promise.all(calls);
    for (const r of replies) {
      assert.equal(r.status, 200, JSON.stringify(r));
      assert.equal(r.body.status, "completed");
      assert.equal(r.body.response.reqId !== undefined, true);
      const payload =
        r.body.response.getAttr?.attributes || r.body.response.data;
      assert.equal(payload.deviceId, r.body.deviceId);
      assert.equal(
        payload.deviceModel,
        r.body.connectionId === "c0" ? "BROKER_A" : "BROKER_B",
      );
    }
    for (const d of ["JSON", "PB"]) {
      assert.equal(
        (
          await http(prefix("c0", d) + "/device/attributes/query", {
            params: { keys: ["device-error"] },
          })
        ).status,
        502,
      );
      const timed = await http(prefix("c0", d) + "/device/attributes/query", {
        params: { keys: ["timeout"] },
      });
      assert.equal(timed.status, 504, JSON.stringify(timed));
      assert.equal(timed.body.error.outcome, "unknown");
      assert.equal(
        (
          await http(prefix("c0", d) + "/device/motor/angle/query", {
            params: {},
          })
        ).status,
        409,
      );
      assert.equal(
        (await http(prefix("c0", d) + "/evidence/snapshot", { params: {} }))
          .status,
        202,
      );
      assert.equal(
        (await http(prefix("c0", d) + "/device/attributes/query", [])).status,
        400,
      );
    }
    assert.equal(
      (
        await http(prefix("bad", "JSON") + "/device/attributes/query", {
          params: {},
        })
      ).status,
      404,
    );
    for (const fixture of require("./fixtures/rpc-cases.json")) {
      const operation = require("../contracts/operations.json").find(
        (x) => x.method === fixture.method,
      );
      for (const device of ["JSON", "PB"]) {
        const result = await http(prefix("c1", device) + "/" + fixture.route, {
          params: fixture[device === "JSON" ? "json" : "protobuf"],
        });
        const expected =
          fixture.method === "ackEvidencePackage"
            ? 400
            : operation.mode === "async"
              ? 202
              : 200;
        assert.equal(
          result.status,
          expected,
          `${language} ${device} ${fixture.method}: ${JSON.stringify(result)}`,
        );
      }
    }
    const now = Math.floor(Date.now() / 1000);
    for (let i = 0; i < brokers.length; i++) {
      await publish(brokers[i].client, "JSON", "telemetry", {
        ts: now,
        source: "broker" + i,
      });
      await publish(brokers[i].client, "PB", "attributes", {
        schemaVersion: 1,
        deviceId: "PB",
        deviceModel: "BROKER_" + i,
      });
      const alarm = {
        eventId: "9007199254740993",
        alarmId: "9007199254740900",
        ruleId: 1,
        type: "DISP_LIMIT",
        transition: "TRIGGERED",
        ts: now,
        level: "ALERT",
      };
      await publish(brokers[i].client, "JSON", "3A", alarm);
      await publish(brokers[i].client, "JSON", "3A", alarm);
      await publish(brokers[i].client, "JSON", "3A", {
        ...alarm,
        eventId: "9007199254740994",
        transition: "SYNCED",
        ts: now + 1,
      });
    }
    await until(async () => {
      const latest = await http(prefix("c1", "JSON") + "/latest");
      return latest.body.telemetry?.source === "broker1";
    }, "latest telemetry");
    assert.equal(
      (await http(prefix("c0", "JSON") + "/latest")).body.telemetry.source,
      "broker0",
    );
    await until(async () => {
      const r = await http(prefix("c0", "JSON") + "/alarms/local");
      return r.body.items.length === 1;
    }, "alarm dedup");
    await until(() => deliveries.length >= 3, "webhook bounded retry");
    assert.equal(
      new Set(deliveries.map((x) => x.key)).size,
      2,
      "only one notification per source/event",
    );
    assert.ok(
      deliveries.filter((x) => x.key === deliveries[0].key).length >= 2,
      "retry uses same idempotency key",
    );
    for (const d of ["JSON", "PB"]) {
      const alarm =
        d === "JSON"
          ? {
              eventId: "8001",
              alarmId: "7001",
              ruleId: 1,
              type: "DISP_LIMIT",
              transition: "SYNCED",
              ts: now,
              level: "ALERT",
            }
          : {
              schemaVersion: 1,
              eventId: "8001",
              alarmId: "7001",
              ruleId: 1,
              alarmType: "ALARM_TYPE_DISPLACEMENT_LIMIT",
              transition: "ALARM_TRANSITION_SYNCED",
              ts: String(now),
              level: "ALARM_LEVEL_ALERT",
            };
      await publish(brokers[0].client, d, "3A", alarm);
      await publish(brokers[0].client, d, "3A", { ...alarm, eventId: "8002" });
      await until(async () => {
        const { body } = await http(prefix("c0", d) + "/alarms/local");
        const a = body.items.find((x) => String(x.alarmId) === "7001");
        return a && a.needsReconcile === false && a.currentActive === false;
      }, "equal timestamp reconciliation " + d);
    }
    const parts = [0, 1].map((i) =>
      fs.readFileSync(path.join(root, `tests/fixtures/evidence-${i}.bin`)),
    );
    const evidence = require("./fixtures/evidence.json");
    const files = (dir) =>
      !fs.existsSync(dir)
        ? []
        : fs
            .readdirSync(dir, { withFileTypes: true })
            .flatMap((e) =>
              e.isDirectory()
                ? files(path.join(dir, e.name))
                : [path.join(dir, e.name)],
            );
    await new Promise((r, j) =>
      brokers[0].client.publish("vdm/JSON/image", parts[1], { qos: 1 }, (e) =>
        e ? j(e) : r(),
      ),
    );
    await until(
      () => files(path.join(dir, "data")).some((f) => f.endsWith(".part")),
      "durable out-of-order partial evidence",
    );
    assert.equal(
      brokers[0].client.acks.length,
      0,
      "incomplete evidence cannot be acknowledged",
    );
    await stop();
    await sleep(200);
    start();
    await until(async () => {
      const h = await http("/health");
      return h.body.status === "ok";
    }, "restart health");
    assert.equal(
      (await http(prefix("c0", "JSON") + "/latest")).body.telemetry.source,
      "broker0",
    );
    await new Promise((r, j) =>
      brokers[0].client.publish("vdm/JSON/image", parts[0], { qos: 1 }, (e) =>
        e ? j(e) : r(),
      ),
    );
    await until(
      () => brokers[0].client.acks.length >= 2,
      "verified evidence ACK retry after restart",
    );
    assert.ok(
      brokers[0].client.acks.every(
        (x) => x.params.packageSha256 === evidence.packageSha256,
      ),
    );
    assert.ok(
      files(path.join(dir, "data")).some((f) => f.endsWith(".tar")),
      "verified archive on disk",
    );
    const invalidAck = await http(prefix("c0", "JSON") + "/evidence/ack", {
      params: {
        eventId: "9999",
        kind: "SNAPSHOT",
        packageSha256: "0".repeat(64),
      },
    });
    assert.equal(invalidAck.status, 400);
    console.log(
      `PASS ${language}: 4 tuple clients, JSON/PB RPC, broker isolation, errors/timeout, capability gate, auth, latest persistence, alarm dedup, equal-ts reconcile, webhook idempotent retry, image restart/verified ACK retry`,
    );
  } catch (e) {
    console.error(
      fs.readFileSync(path.join(dir, "service.log"), "utf8").slice(-8000),
    );
    throw e;
  } finally {
    await stop();
    await new Promise((r) => webhook.close(r));
    fs.closeSync(logs);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}
(async () => {
  const a = Number(process.env.VDM_TEST_BROKER_A || 19883),
    b = Number(process.env.VDM_TEST_BROKER_B || 19884);
  const clients = [
    { port: a, client: await broker(a, "BROKER_A") },
    { port: b, client: await broker(b, "BROKER_B") },
  ];
  try {
    for (const language of process.argv.slice(2).length
      ? process.argv.slice(2)
      : ["python", "go", "java", "javascript"])
      await run(language, clients);
  } finally {
    await Promise.all(
      clients.map((x) => new Promise((r) => x.client.end(true, {}, r))),
    );
  }
})().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
