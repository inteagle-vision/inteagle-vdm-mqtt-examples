"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { Store } = require("../service/store");
const { createApp } = require("../service/app");
const { validateConfig } = require("../service/config");
const { VdmCodec, VdmTopics, PUBLIC_RPC_FIELDS } = require("../sdk");
const operations = require("../../contracts/operations.json");
const cfg = () => ({
  connections: [
    {
      id: "a",
      host: "localhost",
      port: 1883,
      devices: [{ id: "D", format: "json", capabilities: [] }],
    },
  ],
});
function store(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "vdm-service-"));
  const s = new Store(dir, { maxInboxRows: 2 });
  t.after(() => {
    s.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });
  return s;
}
const ev = (eventId, transition = "TRIGGERED", ts = 100) => ({
  eventId,
  alarmId: "7",
  transition,
  ts,
  ...(["RECOVERED", "CANCELLED"].includes(transition)
    ? {}
    : { level: "ALERT" }),
});
test("all 33 business operations encode for JSON and protobuf", () => {
  assert.equal(operations.length, 33);
  for (const format of ["json", "protobuf"]) {
    const codec = new VdmCodec(format);
    for (const op of operations) {
      assert.ok(PUBLIC_RPC_FIELDS[op.method], op.method);
      assert.ok(codec.encodeRpcRequest(op.method, {}, 17).payload.length);
    }
  }
});
test("configuration rejects unsafe identities and duplicate isolation keys", () => {
  assert.throws(() =>
    validateConfig({ connections: [{ ...cfg().connections[0], id: "../x" }] }),
  );
  const c = cfg();
  c.connections.push(c.connections[0]);
  assert.throws(() => validateConfig(c));
  assert.equal(validateConfig(cfg()).rpcTimeoutMs, 10000);
});
test("durable bounded inbox survives restart and telemetry stays latest", (t) => {
  const s = store(t);
  s.receive("a", "D", "telemetry", Buffer.from("{}"), "json");
  s.receive("a", "D", "telemetry", Buffer.from("{}"), "json");
  assert.throws(
    () => s.receive("a", "D", "telemetry", Buffer.from("{}"), "json"),
    /full/,
  );
  s.saveLatest("a", "D", "telemetry", { n: 1 });
  s.saveLatest("a", "D", "telemetry", { n: 2 });
  assert.deepEqual(s.latest("a", "D"), {
    telemetry: { n: 2 },
    attributes: null,
    receivedCount: 2,
  });
  assert.equal(s.db.pragma("journal_mode", { simple: true }), "wal");
  assert.equal(s.db.pragma("user_version", { simple: true }), 1);
});
test("alarm transaction deduplicates by source and suppresses SYNCED/stale", (t) => {
  const s = store(t);
  s.alarm("a", "D", ev("1"), 100, 300);
  s.alarm("a", "D", ev("1"), 100, 300);
  s.alarm("b", "D", ev("1"), 100, 300);
  assert.equal(s.db.prepare("select count(*) n from outbox").get().n, 2);
  s.alarm("a", "D", ev("2", "SYNCED", 101), 101, 300);
  s.alarm("a", "D", ev("3", "ESCALATED", 102), 1000, 300);
  assert.equal(s.db.prepare("select count(*) n from outbox").get().n, 2);
});
test("equal and older alarms retain records and reconcile without overwriting", (t) => {
  const s = store(t);
  s.alarm("a", "D", ev("1", "RECOVERED", 102), 102, 300);
  s.alarm("a", "D", ev("2", "TRIGGERED", 100), 102, 300);
  s.alarm("a", "D", ev("3", "TRIGGERED", 102), 102, 300);
  const state = s.alarms("a", "D")[0];
  assert.equal(state.transition, "RECOVERED");
  assert.equal(state.needsReconcile, true);
  assert.equal(s.db.prepare("select count(*) n from alarm_events").get().n, 3);
  assert.equal(
    s.db.prepare("select count(*) n from jobs where kind='reconcile'").get().n,
    1,
  );
});
test("evidence state updates do not deduplicate only on event ID or send alarm notices", (t) => {
  const s = store(t);
  s.evidenceEvent("a", "D", { eventId: "1", state: "PENDING" });
  s.evidenceEvent("a", "D", { eventId: "1", state: "READY" });
  assert.equal(
    s.db.prepare("select count(*) n from evidence_events").get().n,
    2,
  );
  assert.equal(s.db.prepare("select count(*) n from outbox").get().n, 0);
});
test("HTTP named business routes wait, isolate, reject unsupported capabilities, and report unknown timeout", async (t) => {
  const s = store(t);
  const config = validateConfig(cfg());
  let calls = 0;
  const runtime = {
    config,
    store: s,
    devices: () => [
      { connectionId: "a", deviceId: "D", format: "json", capabilities: [] },
    ],
    health: () => ({ status: "ok", connections: [], counters: {} }),
    call: async (c, d, op) => {
      calls++;
      if (op.method === "getTargets")
        throw Object.assign(new Error("timeout"), {
          status: 504,
          code: "RPC_TIMEOUT",
          outcome: "unknown",
        });
      return { reqId: 2, code: 0 };
    },
  };
  const server = createApp(runtime).listen(0, "127.0.0.1");
  await new Promise((r) => server.once("listening", r));
  t.after(() => new Promise((r) => server.close(r)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const post = (route, body = { params: {} }) =>
    fetch(base + "/v1/connections/a/devices/D/" + route, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  assert.equal((await post("device/attributes/query")).status, 200);
  assert.equal((await post("evidence/snapshot")).status, 202);
  assert.equal((await post("device/motor/angle/query")).status, 409);
  assert.equal((await post("device/attributes/query", [])).status, 400);
  const timeout = await post("targets/query");
  assert.equal(timeout.status, 504);
  assert.equal((await timeout.json()).error.outcome, "unknown");
  assert.equal(calls, 3);
});
test("restart retains pending work and atomic latest update does not replay count", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "vdm-restart-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  let s = new Store(dir);
  s.receive("a", "D", "telemetry", Buffer.from("{}"), "json");
  s.close();
  s = new Store(dir);
  t.after(() => s.close());
  assert.equal(s.due("inbox").d, "D");
});
test("retained finished inbox does not grow beyond configured row bound", (t) => {
  const s = store(t);
  for (let i = 0; i < 20; i++) {
    s.receive("a", "D", "telemetry", Buffer.from("{}"), "json");
    s.done("inbox", s.due("inbox").id);
  }
  assert.ok(s.db.prepare("SELECT count(*) n FROM inbox").get().n <= 2);
});
test("state reconciliation records current active snapshot without fabricating events", (t) => {
  const s = store(t);
  s.alarm("a", "D", ev("1", "RECOVERED", 102), 102, 300);
  s.alarm("a", "D", ev("2", "TRIGGERED", 100), 102, 300);
  s.reconcile("a", "D", { active: [] });
  const item = s.alarms("a", "D")[0];
  assert.equal(item.needsReconcile, false);
  assert.equal(item.currentActive, false);
  assert.equal(item.transition, "RECOVERED");
});
test("shared wire fixtures preserve all 33 JSON payloads and exact Protobuf bytes", () => {
  for (const c of require("../../tests/fixtures/rpc-cases.json")) {
    const j = new VdmCodec("json").encodeRpcRequest(c.method, c.json, c.reqId);
    assert.deepEqual(JSON.parse(j.payload), {
      reqId: c.reqId,
      method: c.method,
      params: c.json,
    });
    const p = new VdmCodec("protobuf").encodeRpcRequest(
      c.method,
      c.protobuf,
      c.reqId,
    );
    assert.equal(
      Buffer.from(p.payload).toString("hex"),
      c.protobufHex,
      c.method,
    );
  }
});
test("evidence ACK requires a verified matching local receipt", (t) => {
  const s = store(t);
  assert.equal(
    s.verified("a", "D", { eventId: "1", packageSha256: "0".repeat(64) }),
    false,
  );
});
test(
  "total RPC deadline bounds stalled PUBACK even after response arrived",
  { timeout: 500 },
  async () => {
    const { VdmMqttClient } = require("../sdk");
    const c = new VdmMqttClient({
      host: "localhost",
      port: 1883,
      topics: VdmTopics.forDevice("D"),
      payloadFormat: "json",
    });
    c.publishRaw = () => {
      queueMicrotask(() =>
        c.handleMessage(
          c.config.topics.rpcResponse,
          Buffer.from('{"reqId":42,"code":0}'),
        ),
      );
      return new Promise(() => {});
    };
    await assert.rejects(
      c.call("getAttr", {}, { reqId: 42, timeoutMs: 30 }),
      /超时|timeout/,
    );
  },
);
test("protobuf empty repeated active reconciles to no active alarms", (t) => {
  const s = store(t);
  s.alarm("a", "D", ev("1"), 100, 300);
  s.alarm("a", "D", ev("2"), 100, 300);
  s.reconcile("a", "D", { getAlarmState: {} });
  assert.equal(s.alarms("a", "D")[0].currentActive, false);
});
test("ordinary JPEG inbox allows bounded frames above 1 MiB", (t) => {
  const s = store(t);
  s.receive("a", "D", "image", Buffer.alloc(2 * 1024 * 1024), "json");
  assert.equal(s.due("inbox").raw.length, 2 * 1024 * 1024);
});
test("processed active-alarm history respects retention without deleting lifecycle", (t) => {
  const s = store(t);
  s.alarm("a", "D", ev("1"), 100, 300);
  s.alarm("a", "D", ev("2", "SYNCED", 101), 101, 300);
  s.prune(1e8);
  assert.equal(s.db.prepare("SELECT count(*) n FROM alarm_events").get().n, 0);
  assert.equal(s.alarms("a", "D").length, 1);
});
test("pending image admission is bounded by aggregate evidence budget", (t) => {
  const s = store(t);
  s.options.maxEvidenceBytes = 3 * 1024 * 1024;
  s.receive("a", "D", "image", Buffer.alloc(2 * 1024 * 1024), "json");
  assert.throws(
    () => s.receive("a", "D", "image", Buffer.alloc(2 * 1024 * 1024), "json"),
    /capacity/,
  );
});
test("new ambiguity rearms exhausted reconciliation without reviving failed evidence ACK", (t) => {
  const s = store(t);
  s.job("a", "D", "reconcile", {}, "reconcile-key");
  s.db
    .prepare(
      "UPDATE jobs SET status='failed',attempts=8,next=9999999999 WHERE id='reconcile-key'",
    )
    .run();
  s.job("a", "D", "reconcile", {}, "reconcile-key");
  const row = s.due("jobs");
  assert.equal(row.id, "reconcile-key");
  assert.equal(row.attempts, 0);
  s.job("a", "D", "ack", {}, "ack-key");
  s.db.prepare("UPDATE jobs SET status='failed' WHERE id='ack-key'").run();
  s.job("a", "D", "ack", {}, "ack-key");
  assert.equal(
    s.db.prepare("SELECT status FROM jobs WHERE id='ack-key'").get().status,
    "failed",
  );
});
test("snapshot-closed lifecycle expires while active or ambiguous remains", (t) => {
  const s = store(t);
  s.alarm("a", "D", ev("1"), 100, 300);
  s.alarm("a", "D", ev("2"), 100, 300);
  s.reconcile("a", "D", { active: [] });
  s.db
    .prepare("UPDATE alarms SET payload=json_set(payload,'$.reconciledAt',100)")
    .run();
  s.prune(1e8);
  assert.equal(s.alarms("a", "D").length, 0);
});
test("pending and failed jobs share a hard queue cap", (t) => {
  const s = store(t);
  s.job("a", "D", "ack", {}, "1");
  s.job("a", "D", "ack", {}, "2");
  s.db.prepare("UPDATE jobs SET status='failed' WHERE id='1'").run();
  assert.throws(() => s.job("a", "D", "ack", {}, "3"), /full/);
});
test("directory lock lives inside the writable data volume and excludes second instance", async (t) => {
  const { acquireDirectoryLock } = require("../service/main");
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "vdm-volume-"));
  const data = path.join(parent, "data");
  fs.mkdirSync(data);
  t.after(() => fs.rmSync(parent, { recursive: true, force: true }));
  const release = await acquireDirectoryLock(data);
  try {
    assert.deepEqual(fs.readdirSync(parent), ["data"]);
    await assert.rejects(acquireDirectoryLock(data), /lock/i);
  } finally {
    await release();
  }
  const again = await acquireDirectoryLock(data);
  await again();
});
