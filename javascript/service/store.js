"use strict";
const Database = require("better-sqlite3");
const fs = require("node:fs");
const path = require("node:path");
const { digest } = require("./identity");
const { AlarmStore } = require("./modules/alarms");
class Store {
  constructor(directory, options = {}) {
    this.directory = directory;
    this.options = { maxInboxRows: 10000, retentionDays: 7, ...options };
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    this.db = new Database(path.join(directory, "service.sqlite"));
    this.db.pragma("busy_timeout=5000");
    this.db.pragma("journal_mode=WAL");
    this.db.pragma("synchronous=FULL");
    this.alarmStore = new AlarmStore(this);
    const version = this.db.pragma("user_version", { simple: true });
    if (version > 1) throw new Error("unsupported database version");
    this.db.transaction(() => {
      this.db
        .exec(`CREATE TABLE IF NOT EXISTS inbox(id INTEGER PRIMARY KEY,c TEXT,d TEXT,suffix TEXT,raw BLOB,format TEXT,status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,next REAL DEFAULT 0,created REAL,error TEXT);
      CREATE TABLE IF NOT EXISTS latest(c TEXT,d TEXT,suffix TEXT,payload TEXT,count INTEGER,PRIMARY KEY(c,d,suffix));
      CREATE TABLE IF NOT EXISTS alarm_events(c TEXT,d TEXT,event TEXT,alarm TEXT,ts REAL,payload TEXT,created REAL,PRIMARY KEY(c,d,event));
      CREATE TABLE IF NOT EXISTS alarms(c TEXT,d TEXT,alarm TEXT,ts REAL,payload TEXT,reconcile INTEGER DEFAULT 0,PRIMARY KEY(c,d,alarm));
      CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY,c TEXT,d TEXT,payload TEXT,status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,next REAL DEFAULT 0,created REAL,error TEXT);
      CREATE TABLE IF NOT EXISTS evidence_events(id TEXT PRIMARY KEY,c TEXT,d TEXT,payload TEXT,created REAL);
      CREATE TABLE IF NOT EXISTS chunks(c TEXT,d TEXT,event TEXT,hash TEXT,idx INTEGER,raw BLOB,created REAL,PRIMARY KEY(c,d,event,hash,idx));
      CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,c TEXT,d TEXT,kind TEXT,payload TEXT,status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,next REAL DEFAULT 0,created REAL,error TEXT);
      CREATE TABLE IF NOT EXISTS counters(name TEXT PRIMARY KEY,value INTEGER);PRAGMA user_version=1;`);
    })();
  }
  close() {
    this.db.close();
  }
  count(name) {
    this.db
      .prepare(
        "INSERT INTO counters VALUES(?,1) ON CONFLICT(name) DO UPDATE SET value=value+1",
      )
      .run(name);
  }
  counters() {
    return Object.fromEntries(
      this.db
        .prepare("SELECT * FROM counters")
        .all()
        .map((r) => [r.name, r.value]),
    );
  }
  receive(c, d, suffix, raw, format) {
    if (
      raw.length >
      (suffix === "image"
        ? Math.min(32 * 1024 * 1024, this.options.maxEvidenceBytes || 268435456)
        : 1024 * 1024)
    )
      throw new Error("message too large");
    this.db.transaction(() => {
      if (suffix === "image") {
        const pending = this.db
          .prepare(
            "SELECT coalesce(sum(length(raw)),0) n FROM inbox WHERE suffix='image' AND status!='done'",
          )
          .get().n;
        const chunks = this.db
          .prepare("SELECT coalesce(sum(length(raw)),0) n FROM chunks")
          .get().n;
        const used = (dir) =>
          !fs.existsSync(dir)
            ? 0
            : fs
                .readdirSync(dir, { withFileTypes: true })
                .reduce(
                  (n, e) =>
                    n +
                    (e.isDirectory()
                      ? used(path.join(dir, e.name))
                      : fs.statSync(path.join(dir, e.name)).size),
                  0,
                );
        if (
          pending +
            chunks +
            used(path.join(this.directory, "evidence")) +
            raw.length >
          (this.options.maxEvidenceBytes || 268435456)
        )
          throw new Error("evidence capacity exceeded");
      }
      if (
        this.db
          .prepare("SELECT count(*) n FROM inbox WHERE status!='done'")
          .get().n >= this.options.maxInboxRows
      )
        throw new Error("durable inbox full");
      this.db
        .prepare(
          "DELETE FROM inbox WHERE id IN (SELECT id FROM inbox WHERE status='done' ORDER BY id LIMIT max(0,(SELECT count(*) FROM inbox)-?+1))",
        )
        .run(this.options.maxInboxRows);
      this.db
        .prepare(
          "INSERT INTO inbox(c,d,suffix,raw,format,created) VALUES(?,?,?,?,?,?)",
        )
        .run(c, d, suffix, raw, format, Date.now() / 1000);
    })();
  }
  saveLatest(c, d, suffix, payload) {
    this.db
      .prepare(
        "INSERT INTO latest VALUES(?,?,?,?,1) ON CONFLICT(c,d,suffix) DO UPDATE SET payload=excluded.payload,count=count+1",
      )
      .run(c, d, suffix, JSON.stringify(payload));
  }
  latest(c, d) {
    const rows = this.db
      .prepare("SELECT * FROM latest WHERE c=? AND d=?")
      .all(c, d);
    const r = { telemetry: null, attributes: null, receivedCount: 0 };
    for (const x of rows) {
      r[x.suffix] = JSON.parse(x.payload);
      r.receivedCount += x.count;
    }
    return r;
  }
  alarms(...args) {
    return this.alarmStore.alarms(...args);
  }
  alarm(...args) {
    return this.alarmStore.alarm(...args);
  }
  reconcile(...args) {
    return this.alarmStore.reconcile(...args);
  }
  evidenceEvent(c, d, payload) {
    this.db
      .prepare("INSERT OR IGNORE INTO evidence_events VALUES(?,?,?,?,?)")
      .run(
        digest([c, d, payload]),
        c,
        d,
        JSON.stringify(payload),
        Date.now() / 1000,
      );
  }
  verified(c, d, params) {
    const rows = this.db
      .prepare("SELECT payload FROM jobs WHERE c=? AND d=? AND kind='ack'")
      .all(c, d);
    return rows.some((r) => {
      const p = JSON.parse(r.payload);
      return (
        String(p.eventId) === String(params.eventId) &&
        p.packageSha256 === params.packageSha256 &&
        fs.existsSync(p.packagePath)
      );
    });
  }
  job(c, d, kind, payload, id = digest([c, d, kind, payload])) {
    const previous = this.db
      .prepare("SELECT status FROM jobs WHERE id=?")
      .get(id);
    if (
      (!previous || previous.status === "done") &&
      this.db.prepare("SELECT count(*) n FROM jobs WHERE status!='done'").get()
        .n >= this.options.maxInboxRows
    )
      throw new Error("background jobs full");
    this.db
      .prepare(
        "INSERT INTO jobs(id,c,d,kind,payload,created) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=CASE WHEN excluded.kind='reconcile' OR jobs.status='done' THEN 'pending' ELSE jobs.status END, attempts=CASE WHEN excluded.kind='reconcile' THEN 0 ELSE jobs.attempts END, next=CASE WHEN excluded.kind='reconcile' THEN 0 ELSE jobs.next END,created=CASE WHEN excluded.kind='reconcile' THEN excluded.created ELSE jobs.created END",
      )
      .run(id, c, d, kind, JSON.stringify(payload), Date.now() / 1000);
  }
  due(table) {
    if (!["inbox", "jobs", "outbox"].includes(table))
      throw new Error("invalid table");
    return this.db
      .prepare(
        `SELECT * FROM ${table} WHERE status='pending' AND next<=? ORDER BY created,id LIMIT 1`,
      )
      .get(Date.now() / 1000);
  }
  done(table, id) {
    if (!["inbox", "jobs", "outbox"].includes(table))
      throw new Error("invalid table");
    this.db.prepare(`UPDATE ${table} SET status='done' WHERE id=?`).run(id);
    if (table === "inbox")
      this.db
        .prepare("UPDATE inbox SET raw=x'' WHERE id=? AND suffix='image'")
        .run(id);
  }
  fail(table, row, error) {
    if (!["inbox", "jobs", "outbox"].includes(table))
      throw new Error("invalid table");
    this.db
      .prepare(
        `UPDATE ${table} SET attempts=attempts+1,status=?,next=?,error=? WHERE id=?`,
      )
      .run(
        row.attempts + 1 >= 8 ? "failed" : "pending",
        Date.now() / 1000 + Math.min(300, 2 ** row.attempts),
        String(error.message).slice(0, 512),
        row.id,
      );
    this.count(`${table}Failures`);
  }
  healthCounts() {
    const r = this.counters();
    for (const table of ["inbox", "jobs", "outbox"])
      for (const row of this.db
        .prepare(`SELECT status,count(*) n FROM ${table} GROUP BY status`)
        .all())
        r[table + row.status] = row.n;
    return r;
  }
  prune(now = Date.now() / 1000) {
    const cut = now - this.options.retentionDays * 86400;
    this.db.transaction(() => {
      for (const t of ["inbox", "jobs", "outbox"])
        this.db
          .prepare(`DELETE FROM ${t} WHERE status='done' AND created<?`)
          .run(cut);
      this.db.prepare("DELETE FROM evidence_events WHERE created<?").run(cut);
      for (const table of ["alarm_events", "evidence_events"])
        this.db
          .prepare(
            `DELETE FROM ${table} WHERE rowid IN (SELECT rowid FROM ${table} ORDER BY created LIMIT max(0,(SELECT count(*) FROM ${table})-?))`,
          )
          .run(this.options.maxInboxRows * 10);
      // Dedup history has a retention window; active lifecycle state is independent.
      this.db.prepare("DELETE FROM alarm_events WHERE created<?").run(cut);
      this.db
        .prepare(
          "DELETE FROM alarms WHERE ts<? AND reconcile=0 AND ((json_type(payload,'$.currentActive') IS NULL AND json_extract(payload,'$.transition') IN ('RECOVERED','CANCELLED')) OR (json_extract(payload,'$.currentActive')=0 AND json_extract(payload,'$.reconciledAt')<?))",
        )
        .run(cut, cut);
    })();
    this.db.pragma("wal_checkpoint(PASSIVE)");
  }
}
module.exports = { Store, digest };
