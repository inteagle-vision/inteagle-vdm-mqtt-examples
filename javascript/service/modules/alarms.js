"use strict";
const { register } = require("./register");
const { digest } = require("../identity");
// Lifecycle state, event deduplication and notification insertion share a transaction.
class AlarmStore {
  constructor(store) {
    this.db = store.db;
    this.options = store.options;
    this.job = store.job.bind(store);
  }
  alarms(c, d) {
    return this.db
      .prepare("SELECT * FROM alarms WHERE c=? AND d=?")
      .all(c, d)
      .map((r) => ({
        ...JSON.parse(r.payload),
        needsReconcile: Boolean(r.reconcile),
      }));
  }
  alarm(c, d, payload, now = Date.now() / 1000, maxAge = 300) {
    const e = {
      ...payload,
      transition: String(payload.transition || "").replace(
        "ALARM_TRANSITION_",
        "",
      ),
      level: payload.level
        ? String(payload.level).replace("ALARM_LEVEL_", "")
        : undefined,
    };
    for (const key of ["eventId", "alarmId"])
      if (
        !/^[1-9][0-9]*$/.test(String(e[key])) ||
        BigInt(e[key]) > 0xffffffffffffffffn
      )
        throw new Error(`invalid ${key}`);
    const terminal = ["RECOVERED", "CANCELLED"].includes(e.transition);
    if (
      ![
        "TRIGGERED",
        "ESCALATED",
        "DEESCALATED",
        "SYNCED",
        "RECOVERED",
        "CANCELLED",
      ].includes(e.transition) ||
      !Number.isFinite(Number(e.ts)) ||
      Number(e.ts) <= 0
    )
      throw new Error("invalid alarm");
    if (
      terminal
        ? e.level !== undefined
        : !["ALERT", "ALARM", "ACTION"].includes(e.level)
    )
      throw new Error("invalid alarm level");
    this.db.transaction(() => {
      const inserted = this.db
        .prepare("INSERT OR IGNORE INTO alarm_events VALUES(?,?,?,?,?,?,?)")
        .run(
          c,
          d,
          String(e.eventId),
          String(e.alarmId),
          Number(e.ts),
          JSON.stringify(e),
          now,
        ).changes;
      if (!inserted) return;
      const prev = this.db
        .prepare("SELECT * FROM alarms WHERE c=? AND d=? AND alarm=?")
        .get(c, d, String(e.alarmId));
      if (prev && Number(e.ts) <= prev.ts) {
        this.db
          .prepare(
            "UPDATE alarms SET reconcile=1 WHERE c=? AND d=? AND alarm=?",
          )
          .run(c, d, String(e.alarmId));
        this.job(c, d, "reconcile", {}, digest([c, d, "reconcile"]));
        return;
      }
      this.db
        .prepare(
          "INSERT INTO alarms(c,d,alarm,ts,payload) VALUES(?,?,?,?,?) ON CONFLICT(c,d,alarm) DO UPDATE SET ts=excluded.ts,payload=excluded.payload",
        )
        .run(c, d, String(e.alarmId), Number(e.ts), JSON.stringify(e));
      const old = prev ? JSON.parse(prev.payload) : null;
      const eligible =
        e.transition === "TRIGGERED"
          ? !old
          : e.transition === "ESCALATED"
            ? !old ||
              { ALERT: 1, ALARM: 2, ACTION: 3 }[e.level] >
                { ALERT: 1, ALARM: 2, ACTION: 3 }[old.level]
            : terminal;
      if (eligible && now - Number(e.ts) >= 0 && now - Number(e.ts) <= maxAge) {
        if (
          this.db
            .prepare("SELECT count(*) n FROM outbox WHERE status!='done'")
            .get().n >= this.options.maxInboxRows
        )
          throw new Error("notification outbox full");
        const id = digest([c, d, String(e.eventId), e.transition]);
        this.db
          .prepare(
            "INSERT OR IGNORE INTO outbox(id,c,d,payload,created) VALUES(?,?,?,?,?)",
          )
          .run(
            id,
            c,
            d,
            JSON.stringify({ connectionId: c, deviceId: d, event: e }),
            now,
          );
      }
    })();
  }
  reconcile(c, d, response) {
    const state = response.getAlarmState || response.data || response;
    if (state.active !== undefined && !Array.isArray(state.active))
      throw new Error("invalid alarm state reconciliation response");
    const active = new Map(
      (state.active || []).map((x) => [String(x.alarmId), x]),
    );
    this.db.transaction(() => {
      for (const row of this.db
        .prepare("SELECT * FROM alarms WHERE c=? AND d=? AND reconcile=1")
        .all(c, d)) {
        const payload = {
          ...JSON.parse(row.payload),
          currentActive: active.has(row.alarm),
          currentState: active.get(row.alarm) || null,
          reconciledAt: Date.now() / 1000,
        };
        this.db
          .prepare(
            "UPDATE alarms SET payload=?,reconcile=0 WHERE c=? AND d=? AND alarm=?",
          )
          .run(JSON.stringify(payload), c, d, row.alarm);
      }
    })();
  }
}
module.exports = { register, AlarmStore };
