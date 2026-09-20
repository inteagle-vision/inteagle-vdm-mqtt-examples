"use strict";
const { parentPort, workerData } = require("node:worker_threads");
const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { AlarmSnapshotPackageAssembler, VdmCodec } = require("../sdk");
const assemblers = new Map();
function used(dir) {
  if (!fs.existsSync(dir)) return 0;
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .reduce(
      (n, e) =>
        n +
        (e.isDirectory()
          ? used(path.join(dir, e.name))
          : fs.statSync(path.join(dir, e.name)).size),
      0,
    );
}
parentPort.on("message", ({ id, c, d, raw, persistentBytes = 0 }) => {
  try {
    const image = VdmCodec.decodeImage(Buffer.from(raw));
    const directory = path.join(workerData.directory, c, d);
    let additional = image.jpeg?.length || 0;
    if (image.messageType === 2) {
      const base = path.join(
        directory,
        String(image.eventId),
        image.packageSha256 + ".tar",
      );
      const existing = [base, base + ".part"].reduce(
        (n, p) => n + (fs.existsSync(p) ? fs.statSync(p).size : 0),
        0,
      );
      additional = Math.max(0, Number(image.packageLength) - existing) + 1024;
    }
    if (
      used(workerData.directory) + persistentBytes + additional >
      workerData.maxBytes
    )
      throw new Error("evidence capacity exceeded");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    let completed = null;
    if (image.messageType === 2) {
      const key = JSON.stringify([c, d]);
      if (!assemblers.has(key))
        assemblers.set(
          key,
          new AlarmSnapshotPackageAssembler(directory, { maxPendingEvents: 8 }),
        );
      completed = assemblers.get(key).accept(image);
    } else {
      const filename = path.join(
        directory,
        crypto.createHash("sha256").update(image.jpeg).digest("hex") + ".jpg",
      );
      if (!fs.existsSync(filename)) {
        const temp = filename + ".tmp";
        const fd = fs.openSync(temp, "w", 0o600);
        try {
          fs.writeFileSync(fd, image.jpeg);
          fs.fsyncSync(fd);
        } finally {
          fs.closeSync(fd);
        }
        fs.renameSync(temp, filename);
        const dirfd = fs.openSync(directory, "r");
        try {
          fs.fsyncSync(dirfd);
        } finally {
          fs.closeSync(dirfd);
        }
      }
    }
    // Persist directory entries up to the evidence root before scheduling ACK.
    let syncPath = completed ? path.dirname(completed.packagePath) : directory;
    while (syncPath.startsWith(workerData.directory)) {
      const fd = fs.openSync(syncPath, "r");
      try {
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      if (syncPath === workerData.directory) break;
      syncPath = path.dirname(syncPath);
    }
    parentPort.postMessage({ id, completed });
  } catch (e) {
    parentPort.postMessage({ id, error: e.message });
  }
});
