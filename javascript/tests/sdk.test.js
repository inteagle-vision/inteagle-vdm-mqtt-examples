"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  AlarmSnapshotPackageAssembler,
  PUBLIC_RPC_FIELDS,
  VdmCodec,
  VdmMqttClient,
  VdmTopics,
} = require("../sdk");

const SNAPSHOT_CHUNK_BYTES = 128 * 1024;

function writeTarOctal(header, offset, length, value) {
  const encoded = Buffer.from(value.toString(8).padStart(length - 1, "0"), "ascii");
  encoded.copy(header, offset);
  header[offset + length - 1] = 0;
}

function tarEntry(name, data) {
  const header = Buffer.alloc(512);
  Buffer.from(name, "ascii").copy(header, 0);
  writeTarOctal(header, 100, 8, 0o600);
  writeTarOctal(header, 108, 8, 0);
  writeTarOctal(header, 116, 8, 0);
  writeTarOctal(header, 124, 12, data.length);
  writeTarOctal(header, 136, 12, 0);
  header.fill(0x20, 148, 156);
  header[156] = 0x30;
  Buffer.from("ustar\0", "ascii").copy(header, 257);
  Buffer.from("00", "ascii").copy(header, 263);
  let checksum = 0;
  for (const value of header) checksum += value;
  Buffer.from(checksum.toString(8).padStart(6, "0"), "ascii").copy(header, 148);
  header[154] = 0;
  header[155] = 0x20;
  const padding = Buffer.alloc((512 - (data.length % 512)) % 512);
  return Buffer.concat([header, data, padding]);
}

function snapshotUstar(imageName = "frame-000.jpg") {
  const image = Buffer.alloc(140_004);
  image.set([0xff, 0xd8], 0);
  image.set([0xff, 0xd9], image.length - 2);
  return Buffer.concat([
    tarEntry("manifest.json", Buffer.from('{"eventId":"9001"}')),
    tarEntry(imageName, image),
    Buffer.alloc(1024),
  ]);
}

function snapshotChunk(packageBytes, eventId, chunkIndex) {
  const chunkCount = Math.ceil(packageBytes.length / SNAPSHOT_CHUNK_BYTES);
  const chunkOffset = chunkIndex * SNAPSHOT_CHUNK_BYTES;
  return {
    messageType: 2,
    headerLength: 76,
    packageFormat: 1,
    evidenceKind: 1,
    eventId: String(eventId),
    packageLength: String(packageBytes.length),
    packageSha256: crypto.createHash("sha256").update(packageBytes).digest("hex"),
    chunkIndex,
    chunkCount,
    chunkOffset: String(chunkOffset),
    chunk: packageBytes.subarray(chunkOffset, Math.min(chunkOffset + SNAPSHOT_CHUNK_BYTES, packageBytes.length)),
  };
}

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

test("8 个不可用 RPC 在两种 Payload 下都被拒绝", () => {
  const unavailable = [
    "getStorageInfo", "queryTelemetry", "uploadS3", "setCruisePoint",
    "removeCruisePoint", "startPatrol", "stopPatrol", "getPatrolStatus",
  ];
  for (const payloadFormat of ["json", "protobuf"]) {
    const codec = new VdmCodec(payloadFormat);
    for (const method of unavailable) {
      assert.throws(() => codec.encodeRpcRequest(method, {}, 200), /\u4e0d\u5c5e\u4e8e\u516c\u5f00 VDM API/);
    }
  }
});

test("内部 RPC 在发布前被拒绝", () => {
  assert.throws(
    () => new VdmCodec("protobuf").encodeRpcRequest("privateDeviceCommand", {}, 9),
    /不属于公开 VDM API/,
  );
});

test("RPC 错误提示只由本地数字码表生成", () => {
  const json = new VdmCodec("json").responseInfo({
    reqId: 8,
    code: 4,
    msg: "private device diagnostic",
  });
  assert.equal(json.message, "RPC request rate limited");

  const protobuf = new VdmCodec("protobuf").responseInfo({
    reqId: 9,
    code: 300,
    message: "private device diagnostic",
  });
  assert.equal(protobuf.message, "motor unavailable");
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

test("严格解析类型 2 告警抓拍图像包分块", () => {
  const packageBytes = Buffer.alloc(512);
  packageBytes.write("ustar", 257, "ascii");
  const payload = Buffer.alloc(76 + packageBytes.length);
  payload.set([2, 76, 1, 1]);
  payload.writeBigUInt64BE(9001n, 4);
  payload.writeBigUInt64BE(BigInt(packageBytes.length), 12);
  crypto.createHash("sha256").update(packageBytes).digest().copy(payload, 20);
  payload.writeUInt32BE(0, 52);
  payload.writeUInt32BE(1, 56);
  payload.writeBigUInt64BE(0n, 60);
  payload.writeUInt32BE(packageBytes.length, 68);
  payload.writeUInt32BE(0, 72);
  packageBytes.copy(payload, 76);
  const chunk = VdmCodec.decodeImage(payload);
  assert.equal(chunk.messageType, 2);
  assert.equal(chunk.eventId, "9001");
  assert.equal(chunk.packageLength, "512");
  assert.deepEqual(chunk.chunk, packageBytes);
});

test("告警抓拍图像包乱序落盘、重复幂等并生成回执", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "vdm-js-snapshot-"));
  try {
    const packageBytes = snapshotUstar();
    const assembler = new AlarmSnapshotPackageAssembler(directory, { maxPendingEvents: 2 });
    const second = snapshotChunk(packageBytes, "9001", 1);
    assert.equal(assembler.accept(second), null);
    assert.equal(assembler.accept(second), null);
    const completed = assembler.accept(snapshotChunk(packageBytes, "9001", 0));
    assert.ok(completed);
    assert.deepEqual(fs.readFileSync(completed.packagePath), packageBytes);
    assert.ok(fs.statSync(path.join(path.dirname(completed.packagePath), "receipt.json")).isFile());
    assert.ok(assembler.accept(second));
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("告警抓拍图像包拒绝冲突、不安全 USTAR 和超额待处理事件", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "vdm-js-snapshot-invalid-"));
  try {
    const packageBytes = snapshotUstar();
    const assembler = new AlarmSnapshotPackageAssembler(path.join(directory, "conflict"), { maxPendingEvents: 1 });
    const first = snapshotChunk(packageBytes, "9002", 0);
    assert.equal(assembler.accept(first), null);
    const conflict = { ...first, chunk: Buffer.from(first.chunk) };
    conflict.chunk[0] ^= 0xff;
    assert.throws(() => assembler.accept(conflict), /冲突/);
    assert.throws(() => assembler.accept(snapshotChunk(packageBytes, "9003", 0)), /有界限制/);

    const unsafePackage = snapshotUstar("../outside.jpg");
    const unsafeAssembler = new AlarmSnapshotPackageAssembler(path.join(directory, "unsafe"));
    assert.equal(unsafeAssembler.accept(snapshotChunk(unsafePackage, "9004", 0)), null);
    assert.throws(() => unsafeAssembler.accept(snapshotChunk(unsafePackage, "9004", 1)), /不安全/);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("抓拍图像 RPC 便捷方法使用稳定协议方法名", async () => {
  const client = new VdmMqttClient({
    host: "127.0.0.1",
    port: 1883,
    topics: VdmTopics.forDevice("DEMO001"),
    payloadFormat: "protobuf",
  });
  const calls = [];
  client.call = async (method, params, options) => {
    calls.push({ method, params, options });
    return { method, params };
  };
  await client.getEvidenceStatus(9001n);
  await client.retryEvidence("9001");
  await client.ackEvidencePackage("9001", "a".repeat(64));
  assert.deepEqual(calls.map((entry) => entry.method), [
    "getEvidenceStatus", "retryEvidence", "ackEvidencePackage",
  ]);
  assert.ok(calls.every((entry) => entry.params.kind === "EVIDENCE_KIND_SNAPSHOT"));
  assert.throws(() => client.ackEvidencePackage("9001", "BAD"), /小写十六进制/);
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
