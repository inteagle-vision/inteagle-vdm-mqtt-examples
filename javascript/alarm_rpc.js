#!/usr/bin/env node
"use strict";
// One MQTT alarm RPC; default is a read-only capabilities query.
const fs = require("node:fs");
const path = require("node:path");
const { randomInt } = require("node:crypto");
const { parseArgs } = require("node:util");
const { VdmCodec, VdmMqttClient, VdmTopics, parsePayloadFormat } = require("./sdk");

async function main() {
  const { values } = parseArgs({ options: {
    case: { type: "string" }, request: { type: "string" },
    "print-only": { type: "boolean", default: false }, listen: { type: "string", default: "0" },
  }});
  if (values.case && values.request) throw new Error("Choose --case or --request");
  const seconds = Number(values.listen);
  if (!Number.isFinite(seconds) || seconds < 0) throw new Error("--listen must be nonnegative seconds");
  const format = parsePayloadFormat(process.env.VDM_PAYLOAD_FORMAT || "protobuf");
  let request;
  if (values.request) request = JSON.parse(fs.readFileSync(values.request, "utf8"));
  else {
    const cases = JSON.parse(fs.readFileSync(path.join(__dirname, "../examples/alarms/requests.json"), "utf8"));
    const item = cases.find((item) => item.name === (values.case || "capabilities"));
    if (!item) throw new Error("Unknown case");
    request = { method: item.method, params: item[format] };
  }
  const reqId = randomInt(1, 2147483647);
  const encoded = new VdmCodec(format).encodeRpcRequest(request.method, request.params, reqId);
  console.log(JSON.stringify(request, null, 2));
  console.log(`format=${format} bytes=${encoded.payload.length} reqId=${reqId}`);
  if (values["print-only"]) return;
  if (!process.env.MQTT_HOST || !process.env.VDM_DEVICE_ID) throw new Error("Set MQTT_HOST and VDM_DEVICE_ID");
  const client = new VdmMqttClient({
    host: process.env.MQTT_HOST, port: Number(process.env.MQTT_PORT || "1883"),
    topics: VdmTopics.forDevice(process.env.VDM_DEVICE_ID), payloadFormat: format, qos: 1,
    username: process.env.MQTT_USERNAME, password: process.env.MQTT_PASSWORD,
  }, (message) => {
    if (["3A", "event"].includes(message.suffix)) console.log(message.suffix, JSON.stringify(message.data));
  }, (error) => console.error("DECODE_ERROR", error.message));
  try {
    await client.start();
    const response = await client.call(request.method, request.params, { reqId, timeoutMs: 30_000 });
    console.log("RESPONSE", JSON.stringify(response.data));
    if (seconds) await new Promise((resolve) => setTimeout(resolve, seconds * 1000));
  } finally {
    await client.stop();
  }
}
main().catch((error) => { console.error(error.message); process.exitCode = 1; });
