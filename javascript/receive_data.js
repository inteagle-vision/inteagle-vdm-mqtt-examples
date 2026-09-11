#!/usr/bin/env node
"use strict";

const { randomInt } = require("node:crypto");
const { parseArgs } = require("node:util");
const { VdmMqttClient, VdmTopics } = require("./sdk");

async function main() {
  const { values } = parseArgs({ options: { "query-attributes": { type: "boolean", default: false } } });
  if (!process.env.MQTT_HOST || !process.env.VDM_DEVICE_ID) {
    throw new Error("Set MQTT_HOST and VDM_DEVICE_ID");
  }
  const subscriptionSuffixes = ["telemetry", "attributes"];
  if (values["query-attributes"]) subscriptionSuffixes.push("rpc/resp");
  const client = new VdmMqttClient({
    host: process.env.MQTT_HOST,
    port: Number(process.env.MQTT_PORT || "1883"),
    topics: VdmTopics.forDevice(process.env.VDM_DEVICE_ID),
    payloadFormat: process.env.VDM_PAYLOAD_FORMAT || "protobuf",
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    subscriptionSuffixes,
  }, (message) => {
    if (["telemetry", "attributes"].includes(message.suffix)) {
      console.log(message.suffix, JSON.stringify(message.data));
    }
  }, (error) => console.error("DECODE_ERROR", error.message));

  let stopping = false;
  let resolveStop;
  const stopped = new Promise((resolve) => { resolveStop = resolve; });
  const stop = () => { stopping = true; client.stop().finally(resolveStop); };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    await client.start();
    console.log("READY waiting for telemetry/attributes; Ctrl-C to stop");
    if (values["query-attributes"]) {
      const response = await client.call("getAttr", {
        keys: ["deviceId", "deviceModel", "fwVer", "measureStatus"],
      }, { reqId: randomInt(1, 2147483647), timeoutMs: 30_000 });
      console.log("RESPONSE", JSON.stringify({ code: 0, ...response.data }));
    }
    await stopped;
  } catch (error) {
    if (!stopping) throw error;
  } finally {
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
    await client.stop();
  }
}

if (require.main === module) {
  main().catch((error) => { console.error("ERROR", error.message); process.exitCode = 1; });
}
