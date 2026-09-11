#!/usr/bin/env node
"use strict";

const path = require("node:path");
const { randomInt } = require("node:crypto");
const { setTimeout: sleep } = require("node:timers/promises");
const { Worker, isMainThread, parentPort, workerData } = require("node:worker_threads");
const { AlarmSnapshotPackageAssembler, RpcError, VdmMqttClient, VdmTopics } = require("./sdk");

async function acknowledgeWithRetry(client, completed, { signal, wait = sleep } = {}) {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    signal?.throwIfAborted();
    try {
      return await client.ackEvidencePackage(completed.eventId, completed.packageSha256, {
        reqId: randomInt(1, 2147483647), timeoutMs: 30_000,
      });
    } catch (error) {
      const retryable = error instanceof RpcError
        ? [4, 5].includes(error.code)
        : /等待 RPC 响应超时|MQTT 连接断开|MQTT client 未连接/.test(error.message)
          || ["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "EPIPE"].includes(error.code);
      if (!retryable || attempt === 3 || signal?.aborted) throw error;
      await wait(1000 * (2 ** attempt), undefined, { signal });
    }
  }
}

// One disk worker keeps USTAR hashing, validation and fsync off the MQTT loop.
// At most eight queued 128 KiB chunks and eight unfinished packages are retained.
function createEvidenceProcessor({ outputDirectory, onComplete, onError, maxQueuedChunks = 8 }) {
  if (!Number.isInteger(maxQueuedChunks) || maxQueuedChunks < 1) {
    throw new Error("maxQueuedChunks must be a positive integer");
  }
  const worker = new Worker(__filename, { workerData: { outputDirectory } });
  const queue = [];
  let busy = false;
  let closed = false;
  function dispatch() {
    if (closed || busy || queue.length === 0) return;
    busy = true;
    worker.postMessage(queue.shift());
  }
  worker.on("message", (result) => {
    busy = false;
    if (closed) return;
    if (result.error) onError(new Error(result.error));
    else if (result.completed) onComplete(result.completed);
    dispatch();
  });
  worker.on("error", (error) => { if (!closed) onError(error); });
  worker.on("exit", (code) => {
    if (!closed) onError(new Error(`Evidence worker stopped (${code})`));
  });
  return {
    enqueue(chunk) {
      if (closed) throw new Error("Evidence receiver is stopping");
      if (queue.length >= maxQueuedChunks) throw new Error("Evidence chunk queue full; retry the package after restarting the receiver");
      queue.push(chunk);
      dispatch();
    },
    async close() {
      closed = true;
      queue.length = 0;
      await worker.terminate();
    },
  };
}

async function main() {
  if (!process.env.MQTT_HOST || !process.env.VDM_DEVICE_ID) {
    throw new Error("Set MQTT_HOST and VDM_DEVICE_ID");
  }
  const acknowledgements = new Map();
  let acknowledging = false;
  let stopping = false;
  let fatalError;
  let resolveStop;
  const cancellation = new AbortController();
  const stopped = new Promise((resolve) => { resolveStop = resolve; });
  const fail = (error) => {
    fatalError ||= error;
    stopping = true;
    cancellation.abort();
    resolveStop();
  };
  const client = new VdmMqttClient({
    host: process.env.MQTT_HOST,
    port: Number(process.env.MQTT_PORT || "1883"),
    topics: VdmTopics.forDevice(process.env.VDM_DEVICE_ID),
    payloadFormat: process.env.VDM_PAYLOAD_FORMAT || "protobuf",
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    subscriptionSuffixes: ["image", "rpc/resp"],
  }, (message) => {
    if (!stopping && message.suffix === "image" && message.value.messageType === 2) {
      try { processor.enqueue(message.value); } catch (error) { fail(error); }
    }
  }, (error) => console.error("DECODE_ERROR", error.message));

  async function acknowledge() {
    if (acknowledging) return;
    acknowledging = true;
    try {
      while (!stopping && acknowledgements.size) {
        const [key, completed] = acknowledgements.entries().next().value;
        const response = await acknowledgeWithRetry(client, completed, {
          signal: cancellation.signal,
        });
        console.log(`ACKED eventId=${completed.eventId} response=${JSON.stringify({ code: 0, ...response.data })}`);
        acknowledgements.delete(key);
      }
    } catch (error) {
      if (!stopping) fail(error);
    } finally {
      acknowledging = false;
    }
  }
  const processor = createEvidenceProcessor({
    outputDirectory: path.resolve(process.env.VDM_EVIDENCE_DIR || "./evidence"),
    onComplete(completed) {
      if (stopping) return;
      const key = `${completed.eventId}:${completed.packageSha256}`;
      if (!acknowledgements.has(key) && acknowledgements.size >= 8) {
        fail(new Error("Evidence ACK queue full; verified packages remain on disk"));
        return;
      }
      console.log(`VERIFIED eventId=${completed.eventId} package=${completed.packagePath}`);
      acknowledgements.set(key, completed);
      void acknowledge();
    },
    onError: fail,
  });
  const stop = () => { stopping = true; cancellation.abort(); resolveStop(); };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    await client.start();
    console.log("READY evidence receiver; Ctrl-C to stop");
    await stopped;
    if (fatalError) throw fatalError;
  } finally {
    stopping = true;
    cancellation.abort();
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
    await client.stop();
    await processor.close();
  }
}

if (!isMainThread) {
  const assembler = new AlarmSnapshotPackageAssembler(workerData.outputDirectory, { maxPendingEvents: 8 });
  parentPort.on("message", (chunk) => {
    try { parentPort.postMessage({ completed: assembler.accept(chunk) }); }
    catch (error) { parentPort.postMessage({ error: error.message }); }
  });
} else if (require.main === module) {
  main().catch((error) => { console.error("ERROR", error.message); process.exitCode = 1; });
}

module.exports = { createEvidenceProcessor, acknowledgeWithRetry };
