import {
  DecodedPayload,
  VdmMqttClient,
  VdmTopics,
} from "../sdk";

const topics = VdmTopics.forDevice("DEMO001");
const onMessage = (message: DecodedPayload): void => {
  console.log(message.suffix, message.raw.byteLength, message.data);
};

const client = new VdmMqttClient(
  {
    host: "127.0.0.1",
    port: 1883,
    topics,
    payloadFormat: "protobuf",
    qos: 1,
    subscriptionSuffixes: ["telemetry", "attributes", "rpc/resp"],
  },
  onMessage,
);

async function compileOnly(): Promise<void> {
  await client.start();
  const response = await client.call(
    "getAttr",
    { keys: ["deviceId"] },
    { reqId: 1, timeoutMs: 10_000 },
  );
  console.log(response.data);
  await client.stop();
}

void compileOnly;
