"use strict";
const express = require("express");
const crypto = require("node:crypto");
const operations = require("../../contracts/operations.json");
const { validateParams } = require("./validation");
const fail = (status, code, message, extra = {}) =>
  Object.assign(new Error(message), { status, code, ...extra });
function createApp(runtime, { token = process.env.VDM_API_TOKEN } = {}) {
  const app = express();
  app.disable("x-powered-by");
  app.get("/health", (_req, res) => res.json(runtime.health()));
  app.use((req, _res, next) => {
    if (token) {
      const got = Buffer.from(req.headers.authorization || "");
      const expected = Buffer.from(`Bearer ${token}`);
      if (
        got.length !== expected.length ||
        !crypto.timingSafeEqual(got, expected)
      )
        return next(fail(401, "UNAUTHORIZED", "Bearer token required"));
    }
    next();
  });
  app.use(express.json({ limit: "1mb", strict: true }));
  app.get("/v1/devices", (_req, res) =>
    res.json({ devices: runtime.devices() }),
  );
  const prefix = "/v1/connections/:connectionId/devices/:deviceId";
  const known = (req, _res, next) => {
    const d = runtime
      .devices()
      .find(
        (x) =>
          x.connectionId === req.params.connectionId &&
          x.deviceId === req.params.deviceId,
      );
    if (!d) return next(fail(404, "NOT_FOUND", "Unknown connection/device"));
    req.vdmDevice = d;
    next();
  };
  app.get(prefix + "/latest", known, (req, res) =>
    res.json(
      runtime.store.latest(req.params.connectionId, req.params.deviceId),
    ),
  );
  app.get(prefix + "/alarms/local", known, (req, res) =>
    res.json({
      items: runtime.store.alarms(req.params.connectionId, req.params.deviceId),
    }),
  );
  for (const domain of [
    "device",
    "targets",
    "measurement",
    "alarms",
    "evidence",
  ])
    require(`./modules/${domain}`).register(app, {
      prefix,
      known,
      runtime,
      operations: operations.filter((x) => x.module === domain),
      fail,
      validateParams,
    });
  app.use((_req, _res, next) => next(fail(404, "NOT_FOUND", "Unknown route")));
  app.use((err, _req, res, _next) => {
    const status = err.type === "entity.too.large" ? 400 : err.status || 500;
    res
      .status(status)
      .json({
        error: {
          code:
            err.code ||
            (status === 400 ? "INVALID_ARGUMENT" : "INTERNAL_ERROR"),
          message: status === 500 ? "Internal service error" : err.message,
          ...(err.outcome ? { outcome: err.outcome } : {}),
          ...(err.deviceCode !== undefined
            ? { deviceCode: err.deviceCode }
            : {}),
        },
      });
  });
  return app;
}
module.exports = { createApp, fail };
