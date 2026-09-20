#!/usr/bin/env node
"use strict";
// Minimal single-business example; modifying operations require explicit choice.
const { VdmMqttClient, VdmTopics } = require("./sdk");
const requests = {
  attributes: ["getAttr", {}],
  targets: ["getTargets", {}],
  alarms: ["getAlarmState", {}],
  lights: ["getLightLevel", {}],
  history: ["listAlarmHistory", { page: 1, pageSize: 20 }],
};
async function main() {
  const choice = process.argv[2] || "attributes";
  if (!requests[choice])
    throw new Error(`Choose ${Object.keys(requests).join(", ")}`);
  const sdk = new VdmMqttClient({
    host: process.env.MQTT_HOST,
    port: Number(process.env.MQTT_PORT || 1883),
    topics: VdmTopics.forDevice(process.env.VDM_DEVICE_ID),
    payloadFormat: process.env.VDM_PAYLOAD_FORMAT || "json",
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    subscriptionSuffixes: ["rpc/resp"],
  });
  try {
    await sdk.start();
    console.log(
      JSON.stringify((await sdk.call(...requests[choice])).data, null, 2),
    );
  } finally {
    await sdk.stop();
  }
}
if (require.main === module)
  main().catch((e) => {
    console.error(e.message);
    process.exitCode = 1;
  });
