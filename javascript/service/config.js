"use strict";
const fs = require("node:fs");
const path = require("node:path");
function validateConfig(input) {
  if (!input || !Array.isArray(input.connections))
    throw new Error("connections must be an array");
  const config = {
    rpcTimeoutMs: 10000,
    notificationMaxAgeSeconds: 300,
    maxInboxRows: 10000,
    retentionDays: 7,
    maxEvidenceBytes: 268435456,
    ...input,
  };
  for (const [k, min, max] of [
    ["rpcTimeoutMs", 100, 60000],
    ["notificationMaxAgeSeconds", 0, 31536000],
    ["maxInboxRows", 1, 1000000],
    ["retentionDays", 1, 3650],
    ["maxEvidenceBytes", 1024, 2 ** 40],
  ]) {
    if (!Number.isSafeInteger(config[k]) || config[k] < min || config[k] > max)
      throw new Error(`invalid ${k}`);
  }
  const id = (v) => typeof v === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(v);
  const seen = new Set();
  for (const c of config.connections) {
    if (!id(c.id) || seen.has(c.id))
      throw new Error("invalid/duplicate connection ID");
    seen.add(c.id);
    if (
      typeof c.host !== "string" ||
      !c.host.trim() ||
      !Number.isInteger(c.port) ||
      c.port < 1 ||
      c.port > 65535 ||
      !Array.isArray(c.devices)
    )
      throw new Error("invalid broker");
    const devices = new Set();
    for (const d of c.devices) {
      if (!id(d.id) || devices.has(d.id))
        throw new Error("invalid/duplicate device ID");
      devices.add(d.id);
      if (
        !["json", "protobuf"].includes(d.format) ||
        !Array.isArray(d.capabilities) ||
        d.capabilities.some((x) => typeof x !== "string")
      )
        throw new Error("invalid device format/capabilities");
    }
  }
  return config;
}
function loadConfig() {
  return validateConfig(
    JSON.parse(
      fs.readFileSync(
        process.env.VDM_SERVICE_CONFIG ||
          path.resolve(__dirname, "../../contracts/config.example.json"),
        "utf8",
      ),
    ),
  );
}
module.exports = { validateConfig, loadConfig };
