"use strict";
const { register } = require("./register");
// Persist deduplicated chunks before assembling and scheduling a verified ACK.
async function consumeImage(runtime, row, message) {
  const chunk = message.value;
  if (chunk.messageType === 2) {
    const old = runtime.store.db
      .prepare(
        "SELECT raw FROM chunks WHERE c=? AND d=? AND event=? AND hash=? AND idx=?",
      )
      .get(
        row.c,
        row.d,
        String(chunk.eventId),
        chunk.packageSha256,
        chunk.chunkIndex,
      );
    if (old && !old.raw.equals(row.raw))
      throw new Error("conflicting duplicate image chunk");
    const bytes = runtime.store.db
      .prepare("SELECT coalesce(sum(length(raw)),0) n FROM chunks")
      .get().n;
    if (!old && bytes + row.raw.length > runtime.config.maxEvidenceBytes)
      throw new Error("chunk persistence capacity exceeded");
    runtime.store.db
      .prepare("INSERT OR IGNORE INTO chunks VALUES(?,?,?,?,?,?,?)")
      .run(
        row.c,
        row.d,
        String(chunk.eventId),
        chunk.packageSha256,
        chunk.chunkIndex,
        row.raw,
        Date.now() / 1000,
      );
  }
  const completed = await runtime.image(row.c, row.d, row.raw);
  if (completed) runtime.store.job(row.c, row.d, "ack", completed);
}
module.exports = { register, consumeImage };
