#!/usr/bin/env node
"use strict";
const path = require("node:path");
const fs = require("node:fs");
const lockfile = require("proper-lockfile");
const { loadConfig } = require("./config");
const { Runtime } = require("./runtime");
const { createApp } = require("./app");
async function acquireDirectoryLock(directory) {
  return lockfile.lock(directory, {
    lockfilePath: path.join(directory, ".service.lock"),
    stale: 10000,
    update: 2000,
    retries: 0,
  });
}
async function main() {
  const host = process.env.VDM_HTTP_HOST || "127.0.0.1";
  const token = process.env.VDM_API_TOKEN;
  if (!["127.0.0.1", "::1", "localhost"].includes(host) && !token)
    throw new Error("Remote bind requires VDM_API_TOKEN");
  const directory = path.resolve(process.env.VDM_DATA_DIR || "./data");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const release = await acquireDirectoryLock(directory);
  const runtime = new Runtime(loadConfig(), directory);
  await runtime.start();
  const server = createApp(runtime).listen(
    Number(process.env.VDM_HTTP_PORT || 8080),
    host,
    () => console.log(`VDM service listening ${host}:${server.address().port}`),
  );
  let closing = false;
  async function stop() {
    if (closing) return;
    closing = true;
    server.close();
    await runtime.stop();
    await release();
  }
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  server.on("error", async (e) => {
    console.error(e.message);
    await stop();
    process.exitCode = 1;
  });
}
if (require.main === module)
  main().catch((e) => {
    console.error(e.message);
    process.exitCode = 1;
  });
module.exports = { main, acquireDirectoryLock };
