"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  PUBLIC_RPC_FIELDS,
  VdmCodec,
  VdmMqttClient,
  VdmTopics,
} = require("../sdk");

test("Topic 映射并拒绝通配符", () => {
  const topics = VdmTopics.forDevice("DEMO001");
  assert.equal(topics.rpcRequest, "vdm/DEMO001/rpc/req");
  assert.equal(topics.suffix("vdm/DEMO001/telemetry"), "telemetry");
  assert.throws(() => VdmTopics.forDevice("bad/#"));
});

test("29 个公开 RPC 都构造强类型 oneof", () => {
  const codec = new VdmCodec("protobuf");
  assert.equal(Object.keys(PUBLIC_RPC_FIELDS).length, 29);
  let reqId = 100;
  for (const [method, field] of Object.entries(PUBLIC_RPC_FIELDS)) {
    const encoded = codec.encodeRpcRequest(method, {}, reqId++);
    const request = codec.rpcRequestType.decode(encoded.payload);
    assert.ok(Object.hasOwn(request, field), method);
    assert.equal(encoded.expectedResponseField, field);
  }
});

test("内部 RPC 在发布前被拒绝", () => {
  assert.throws(
    () => new VdmCodec("protobuf").encodeRpcRequest("wySetAttributes", {}, 9),
    /不属于公开 VDM API/,
  );
});

test("Protobuf 返回强类型对象和完整可读字段", () => {
  const codec = new VdmCodec("protobuf");
  const topics = VdmTopics.forDevice("DEMO001");
  const type = codec.types.attributes;
  const raw = type.encode(type.create({
    schemaVersion: 1,
    deviceId: "DEMO001",
    firmwareVersion: "example-1.0.0",
  })).finish();
  const decoded = codec.decode(topics.topic("attributes"), topics, raw);
  assert.equal(decoded.value.deviceId, "DEMO001");
  assert.equal(decoded.data.deviceId, "DEMO001");
  assert.equal(decoded.data.firmwareVersion, "example-1.0.0");
});

test("拒绝不支持的 schema_version", () => {
  const codec = new VdmCodec("protobuf");
  const topics = VdmTopics.forDevice("DEMO001");
  const type = codec.types.attributes;
  const raw = type.encode(type.create({ schemaVersion: 2 })).finish();
  assert.throws(() => codec.decode(topics.topic("attributes"), topics, raw));
});

test("解析图片 Header 并保留 JPEG", () => {
  const payload = Buffer.concat([
    Buffer.from([1, 8, 0, 1]),
    Buffer.from([0, 0, 0, 123]),
    Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
  ]);
  const image = VdmCodec.decodeImage(payload);
  assert.equal(image.timestampS, 123);
  assert.deepEqual(image.jpeg, Buffer.from([0xff, 0xd8, 0xff, 0xd9]));
});

test("不同云客户端实例不共享 RPC pending 表", () => {
  const cloudA = new VdmMqttClient({
    host: "127.0.0.1",
    port: 1883,
    topics: VdmTopics.forDevice("CLOUD-A-DEVICE"),
    payloadFormat: "protobuf",
  });
  const cloudB = new VdmMqttClient({
    host: "127.0.0.1",
    port: 1883,
    topics: VdmTopics.forDevice("CLOUD-B-DEVICE"),
    payloadFormat: "protobuf",
  });

  cloudA.pending.set(77, { cloud: "A" });
  cloudB.pending.set(77, { cloud: "B" });

  assert.notEqual(cloudA.pending, cloudB.pending);
  assert.equal(cloudA.pending.get(77).cloud, "A");
  assert.equal(cloudB.pending.get(77).cloud, "B");
});

test("断线清理当前客户端的全部 RPC pending", async () => {
  const client = new VdmMqttClient({
    host: "127.0.0.1",
    port: 1883,
    topics: VdmTopics.forDevice("DEMO001"),
    payloadFormat: "protobuf",
  });
  let rejected;
  const timer = setTimeout(() => {}, 10_000);
  client.pending.set(88, {
    timer,
    reject: (error) => { rejected = error; },
  });

  client.failPending(new Error("connection lost"));

  assert.equal(client.pending.size, 0);
  assert.match(rejected.message, /connection lost/);
});
