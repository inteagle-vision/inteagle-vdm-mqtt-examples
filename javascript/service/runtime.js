"use strict";
const path = require("node:path");
const fs = require("node:fs");
const crypto = require("node:crypto");
const { Worker } = require("node:worker_threads");
const { VdmMqttClient, VdmTopics, RpcError, VdmCodec } = require("../sdk");
const { Store, digest } = require("./store");
const { fail } = require("./app");
const { consumeImage } = require("./modules/evidence");
class Runtime {
  constructor(config, directory) {
    this.config = config;
    this.directory = directory;
    this.store = new Store(directory, config);
    this.clients = new Map();
    this.stopping = false;
    this.busy = false;
    this.activeCalls = 0;
    this.imageWorker = new Worker(path.join(__dirname, "evidence-worker.js"), {
      workerData: {
        directory: path.join(directory, "evidence"),
        maxBytes: config.maxEvidenceBytes,
      },
    });
    this.imageRequests = new Map();
    this.imageSequence = 0;
    this.imageWorker.on("message", (m) => {
      const p = this.imageRequests.get(m.id);
      this.imageRequests.delete(m.id);
      if (p) m.error ? p.reject(new Error(m.error)) : p.resolve(m.completed);
    });
    this.imageWorker.on("error", (e) => {
      for (const p of this.imageRequests.values()) p.reject(e);
      this.imageRequests.clear();
      this.log(e);
    });
    for (const c of config.connections)
      for (const d of c.devices) {
        const key = JSON.stringify([c.id, d.id]);
        const sdk = new VdmMqttClient(
          {
            host: c.host,
            port: c.port,
            topics: VdmTopics.forDevice(d.id),
            payloadFormat: d.format,
            username: process.env[c.usernameEnv || "MQTT_USERNAME"],
            password: process.env[c.passwordEnv || "MQTT_PASSWORD"],
            clientId: "vdm-js-" + digest([c.id, d.id]).slice(0, 32),
            subscriptionSuffixes: [
              "rpc/resp",
              "telemetry",
              "attributes",
              "3A",
              "event",
              "image",
            ],
            connectTimeoutMs: 3000,
            durableReceive: (topic, raw, qos) => {
              this.store.receive(
                c.id,
                d.id,
                VdmTopics.forDevice(d.id).suffix(topic),
                raw,
                d.format,
              );
              if (qos === 0) this.store.count("qos0Received");
            },
          },
          null,
          (e) => this.log(e),
        );
        this.clients.set(key, {
          sdk,
          connectionId: c.id,
          deviceId: d.id,
          format: d.format,
          capabilities: d.capabilities,
          connecting: false,
        });
      }
  }
  log(e) {
    console.error("SERVICE_ERROR", e.message);
    try {
      this.store.count("errors");
    } catch {
      /* database failure already logged */
    }
  }
  devices() {
    return [...this.clients.values()].map(
      ({ connectionId, deviceId, format, capabilities }) => ({
        connectionId,
        deviceId,
        format,
        capabilities,
      }),
    );
  }
  health() {
    const connections = [...this.clients.values()].map((x) => ({
      connectionId: x.connectionId,
      deviceId: x.deviceId,
      connected: Boolean(x.sdk.client?.connected),
    }));
    const counters = this.store.healthCounts();
    return {
      status:
        connections.every((x) => x.connected) &&
        !counters.inboxfailed &&
        !counters.jobsfailed &&
        !counters.outboxfailed
          ? "ok"
          : "degraded",
      connections,
      counters,
    };
  }
  async call(c, d, operation, params = {}) {
    const entry = this.clients.get(JSON.stringify([c, d]));
    if (!entry) throw fail(404, "NOT_FOUND", "Unknown connection/device");
    if (
      operation.method === "ackEvidencePackage" &&
      !this.store.verified(c, d, params)
    )
      throw fail(
        400,
        "INVALID_ARGUMENT",
        "Matching verified local evidence receipt required",
      );
    if (!entry.sdk.client?.connected)
      throw fail(503, "UNAVAILABLE", "MQTT connection unavailable");
    if (this.activeCalls >= 64)
      throw fail(503, "OVERLOADED", "RPC concurrency limit reached");
    try {
      entry.sdk.codec.encodeRpcRequest(operation.method, params, 1);
    } catch (e) {
      throw fail(400, "INVALID_ARGUMENT", e.message);
    }
    this.activeCalls++;
    try {
      return (
        await entry.sdk.call(operation.method, params, {
          timeoutMs: this.config.rpcTimeoutMs,
        })
      ).data;
    } catch (e) {
      if (e instanceof RpcError)
        throw fail(502, "DEVICE_ERROR", e.message, { deviceCode: e.code });
      if (/超时|timeout/i.test(e.message))
        throw fail(
          504,
          "RPC_TIMEOUT",
          "RPC result unknown; read back device state before retrying",
          { outcome: "unknown" },
        );
      throw fail(503, "UNAVAILABLE", "RPC connection lost; result unknown", {
        outcome: "unknown",
      });
    } finally {
      this.activeCalls--;
    }
  }
  image(c, d, raw) {
    const id = ++this.imageSequence;
    return new Promise((resolve, reject) => {
      this.imageRequests.set(id, { resolve, reject });
      this.imageWorker.postMessage({
        id,
        c,
        d,
        raw,
        persistentBytes: this.store.db
          .prepare("SELECT coalesce(sum(length(raw)),0) n FROM chunks")
          .get().n,
      });
    });
  }
  async start() {
    // Rebuild the assembler bitmap from committed raw chunks before receiving new
    // work. QoS1 chunks remain durable across restart, independent of MQTT session.
    for (const row of this.store.db
      .prepare("SELECT * FROM chunks ORDER BY c,d,event,idx")
      .iterate()) {
      try {
        const result = await this.image(row.c, row.d, row.raw);
        if (result) this.store.job(row.c, row.d, "ack", result);
      } catch (e) {
        this.log(e);
      }
    }
    this.connectTimer = setInterval(() => this.connect(), 1000);
    this.connect();
    this.workerTimer = setInterval(() => this.tick(), 100);
    this.pruneTimer = setInterval(() => {
      if (this.busy) return;
      try {
        this.store.prune();
        this.pruneEvidence();
      } catch (e) {
        this.log(e);
      }
    }, 60000);
  }
  connect() {
    for (const entry of this.clients.values())
      if (!entry.sdk.client && !entry.connecting && !this.stopping) {
        entry.connecting = true;
        entry.sdk
          .start()
          .catch((e) => this.log(e))
          .finally(() => {
            entry.connecting = false;
          });
      }
  }
  async tick() {
    if (this.busy || this.stopping) return;
    this.busy = true;
    try {
      for (const table of ["inbox", "jobs", "outbox"]) {
        const row = this.store.due(table);
        if (row) {
          try {
            if (table === "inbox") await this.consume(row);
            else if (table === "jobs") await this.job(row);
            else {
              if (!process.env.VDM_WEBHOOK_URL) continue;
              await this.notify(row);
            }
            this.store.done(table, row.id);
          } catch (e) {
            this.store.fail(table, row, e);
            this.log(e);
          }
        }
      }
    } catch (e) {
      this.log(e);
    } finally {
      this.busy = false;
    }
  }
  async consume(row) {
    const topics = VdmTopics.forDevice(row.d);
    const message = new VdmCodec(row.format).decode(
      topics.topic(row.suffix),
      topics,
      row.raw,
    );
    if (["telemetry", "attributes"].includes(row.suffix))
      this.store.db.transaction(() => {
        this.store.saveLatest(row.c, row.d, row.suffix, message.data);
        this.store.done("inbox", row.id);
      })();
    else if (row.suffix === "3A")
      this.store.alarm(
        row.c,
        row.d,
        message.data,
        Date.now() / 1000,
        this.config.notificationMaxAgeSeconds,
      );
    else if (row.suffix === "event")
      this.store.evidenceEvent(row.c, row.d, message.data);
    else if (row.suffix === "image") {
      await consumeImage(this, row, message);
    }
  }
  async job(row) {
    const p = JSON.parse(row.payload);
    if (row.kind === "ack") {
      const entry = this.clients.get(JSON.stringify([row.c, row.d]));
      const kind =
        entry.format === "json" ? "SNAPSHOT" : "EVIDENCE_KIND_SNAPSHOT";
      await this.call(
        row.c,
        row.d,
        { method: "ackEvidencePackage" },
        { eventId: p.eventId, kind, packageSha256: p.packageSha256 },
      );
      this.store.db
        .prepare("DELETE FROM chunks WHERE c=? AND d=? AND event=? AND hash=?")
        .run(row.c, row.d, String(p.eventId), p.packageSha256);
    } else if (row.kind === "reconcile") {
      const response = await this.call(
        row.c,
        row.d,
        { method: "getAlarmState" },
        {},
      );
      // Store authoritative snapshot separately. Keep ambiguous event state marked:
      // a state read cannot invent an ordering for equal timestamps.
      this.store.reconcile(row.c, row.d, response);
    }
  }
  async notify(row) {
    const response = await fetch(process.env.VDM_WEBHOOK_URL, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "Idempotency-Key": row.id,
        ...(process.env.VDM_WEBHOOK_TOKEN
          ? { Authorization: `Bearer ${process.env.VDM_WEBHOOK_TOKEN}` }
          : {}),
      },
      body: row.payload,
      signal: AbortSignal.timeout(5000),
      redirect: "error",
    });
    await response.body?.cancel();
    if (!response.ok) throw new Error(`webhook HTTP ${response.status}`);
  }
  pruneEvidence() {
    const base = path.join(this.directory, "evidence");
    const cutoff = Date.now() - this.config.retentionDays * 86400000;
    const walk = (dir) => {
      if (!fs.existsSync(dir)) return;
      for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
        const file = path.join(dir, ent.name);
        if (ent.isDirectory()) walk(file);
        else if (
          !ent.name.endsWith(".part") &&
          fs.statSync(file).mtimeMs < cutoff
        ) {
          const pending = this.store.db
            .prepare(
              "SELECT 1 FROM jobs WHERE kind='ack' AND status!='done' AND json_extract(payload,'$.packagePath')=?",
            )
            .get(file);
          if (!pending) fs.rmSync(file);
        }
      }
    };
    walk(base);
  }
  async stop() {
    this.stopping = true;
    clearInterval(this.connectTimer);
    clearInterval(this.workerTimer);
    clearInterval(this.pruneTimer);
    await Promise.allSettled(
      [...this.clients.values()].map((x) => x.sdk.stop()),
    );
    while (this.busy) await new Promise((r) => setTimeout(r, 20));
    await this.imageWorker.terminate();
    this.store.close();
  }
}
module.exports = { Runtime };
