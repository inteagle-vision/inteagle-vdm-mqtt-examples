#!/usr/bin/env node
"use strict";

const {
  VdmMqttClient,
  VdmTopics,
  parsePayloadFormat,
} = require("./sdk");

const REQUIRED_SUFFIXES = new Set([
  "telemetry",
  "attributes",
  "event",
  "3A",
  "evidence",
  "rpc/req",
  "rpc/resp",
  "image",
]);

function setting(name, fallback) {
  return process.env[name] || fallback;
}

async function main() {
  const format = parsePayloadFormat(setting("VDM_PAYLOAD_FORMAT", "protobuf"));
  const runId = setting("VDM_RUN_ID", "manual");
  const name = setting("VDM_CONSUMER_NAME", "javascript");
  const topics = new VdmTopics(setting("VDM_BASE_TOPIC", "vdm/DEMO001"));
  const seen = new Set();
  let rpcStarted = false;
  let rpcPassed = false;
  let resultSent = false;
  let client;

  const controlTopic = (kind) => `vdm-example/${runId}/${kind}/${name}`;
  const publishResult = async (value) => {
    await client.publishRaw(controlTopic("result"), value, true);
  };
  const fail = async (error) => {
    if (resultSent) return;
    resultSent = true;
    const reason = error?.message ?? String(error);
    console.error(`FAIL consumer=${name} reason=${reason}`);
    try {
      await publishResult(`FAIL:${reason}`);
    } catch (publishError) {
      console.error(`FAIL consumer=${name} 无法上报结果: ${publishError.message}`);
    }
  };
  const completeIfReady = async () => {
    if (
      !resultSent
      && rpcPassed
      && [...REQUIRED_SUFFIXES].every((suffix) => seen.has(suffix))
    ) {
      resultSent = true;
      await publishResult("PASS");
      console.log(
        `PASS consumer=${name} profile=${format} `
        + `topics=${[...seen].sort().join(",")} rpc=getAttr`,
      );
    }
  };
  const testRpc = async () => {
    try {
      const response = await client.call(
        "getAttr",
        { keys: ["deviceId", "fwVer"] },
        { reqId: 14_001, timeoutMs: 10_000 },
      );
      const deviceId = format === "protobuf"
        ? response.data?.getAttr?.attributes?.deviceId
        : response.data?.data?.deviceId;
      if (deviceId !== "DEMO001") {
        throw new Error(`RPC getAttr.deviceId 不匹配: ${deviceId}`);
      }
      rpcPassed = true;
      console.log(
        `RPC_PASS consumer=${name} method=getAttr req_id=14001 `
        + `data=${JSON.stringify(response.data)}`,
      );
      await completeIfReady();
    } catch (error) {
      await fail(error);
    }
  };

  client = new VdmMqttClient(
    {
      host: setting("MQTT_HOST", "127.0.0.1"),
      port: Number(setting("MQTT_PORT", "1883")),
      topics,
      payloadFormat: format,
      clientId: `vdm-example-${name}-${runId}`,
      qos: 1,
      connectTimeoutMs: Number(setting("VDM_WAIT_TIMEOUT", "45")) * 1000,
    },
    (message) => {
      if (!REQUIRED_SUFFIXES.has(message.suffix)) {
        void fail(new Error(`未支持的 Topic: ${message.suffix}`));
        return;
      }
      console.log(
        `DECODED consumer=${name} profile=${format} topic=${message.suffix} `
        + `bytes=${message.raw.length} data=${JSON.stringify(message.data)}`,
      );
      seen.add(message.suffix);
      if (message.suffix === "telemetry" && !rpcStarted) {
        rpcStarted = true;
        void testRpc();
      }
      void completeIfReady();
    },
    (error) => void fail(error),
  );

  await client.start();
  await client.publishRaw(controlTopic("ready"), "READY", true);
  console.log(`READY consumer=${name} profile=${format}`);
}

main().catch((error) => {
  console.error(`FAIL consumer=javascript reason=${error.message}`);
  process.exitCode = 1;
});
