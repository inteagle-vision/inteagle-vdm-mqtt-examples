"use strict";

const path = require("node:path");
const crypto = require("node:crypto");
const mqtt = require("mqtt");
const protobuf = require("protobufjs");
const { AlarmSnapshotPackageAssembler } = require("./snapshot-package");

// fromObject supports ProtoJSON enum names and decimal uint64 strings. Validate
// first: protobufjs otherwise silently drops unknown fields/enum names, which
// can turn a misspelled alarm rule into a different request.
function validateProtoObject(type, object, location = type.name) {
  if (object === null || Array.isArray(object) || typeof object !== "object") {
    throw new Error(`${location} must be an object`);
  }
  for (const oneof of type.oneofsArray) {
    if (oneof.oneof.filter((name) => object[name] != null).length > 1) {
      throw new Error(`${location}.${oneof.name}: multiple oneof fields`);
    }
  }
  for (const [name, value] of Object.entries(object)) {
    const field = type.fields[name];
    if (!field) throw new Error(`${location}: unknown field ${name}`);
    if (value == null) continue; // ProtoJSON null means absent.
    field.resolve();
    if (field.repeated && !Array.isArray(value)) {
      throw new Error(`${location}.${name} must be an array`);
    }
    if (field.map && (Array.isArray(value) || typeof value !== "object")) {
      throw new Error(`${location}.${name} must be an object`);
    }
    const values = field.map ? Object.values(value) : field.repeated ? value : [value];
    for (const item of values) {
      if (field.resolvedType instanceof protobuf.Type) {
        validateProtoObject(field.resolvedType, item, `${location}.${name}`);
      } else if (field.resolvedType instanceof protobuf.Enum) {
        const enums = field.resolvedType.values;
        if (!(typeof item === "string" && Object.hasOwn(enums, item))
            && !(Number.isInteger(item) && Object.values(enums).includes(item))) {
          throw new Error(`${location}.${name}: invalid enum ${item}`);
        }
      } else if (/^(u?int|sint|fixed|sfixed)64$/.test(field.type)) {
        if (typeof item === "number" && !Number.isSafeInteger(item)) {
          throw new Error(`${location}.${name}: use a decimal string for 64-bit integers`);
        }
        const decimal = protobuf.util.Long?.isLong(item) ? item.toString() : item;
        if (!(typeof decimal === "number" && Number.isSafeInteger(decimal))
            && !(typeof decimal === "string" && /^-?[0-9]+$/.test(decimal))) {
          throw new Error(`${location}.${name}: expected a decimal 64-bit integer`);
        }
        const integer = BigInt(decimal);
        const unsigned = ["uint64", "fixed64"].includes(field.type);
        const min = unsigned ? 0n : -(1n << 63n);
        const max = unsigned ? (1n << 64n) - 1n : (1n << 63n) - 1n;
        if (integer < min || integer > max) {
          throw new Error(`${location}.${name}: 64-bit integer out of range`);
        }
      } else if (/^(u?int|sint|fixed|sfixed)32$/.test(field.type)) {
        const unsigned = ["uint32", "fixed32"].includes(field.type);
        const min = unsigned ? 0 : -(2 ** 31);
        const max = unsigned ? 2 ** 32 - 1 : 2 ** 31 - 1;
        if (!Number.isInteger(item) || item < min || item > max) {
          throw new Error(`${location}.${name}: expected a 32-bit integer in range`);
        }
      } else if (field.type === "bool" && typeof item !== "boolean") {
        throw new Error(`${location}.${name}: expected a boolean`);
      } else if (field.type === "string" && typeof item !== "string") {
        throw new Error(`${location}.${name}: expected a string`);
      } else if (["float", "double"].includes(field.type)
          && (typeof item !== "number" || !Number.isFinite(item)
              || (field.type === "float" && !Number.isFinite(Math.fround(item))))) {
        throw new Error(`${location}.${name}: expected a finite ${field.type}`);
      }
    }
  }
}

const SCHEMA_VERSION = 1;

const PUBLIC_RPC_FIELDS = Object.freeze({
  getAttr: "getAttr",
  setAttr: "setAttr",
  reboot: "reboot",
  syncTime: "syncTime",
  initRefTargets: "initRefTargets",
  addTargets: "addTargets",
  getTargets: "getTargets",
  setTargets: "setTargets",
  deleteTargets: "deleteTargets",
  startMeasurement: "startMeasurement",
  stopMeasurement: "stopMeasurement",
  setLightLevel: "setLightLevel",
  getLightLevel: "getLightLevel",
  snapshot: "snapshot",
  ispCtl: "ispCtl",
  setMotorAngle: "setMotorAngle",
  getMotorAngle: "getMotorAngle",
  setMotorZero: "setMotorZero",
  enableMotor: "enableMotor",
  disableMotor: "disableMotor",
  getCruisePaths: "getCruisePaths",
  getEvidenceStatus: "getEvidenceStatus",
  retryEvidence: "retryEvidence",
  ackEvidencePackage: "ackEvidencePackage",
  getAlarmCaps: "getAlarmCaps",
  listAlarmRules: "listAlarmRules",
  applyAlarmRules: "applyAlarmRules",
  getAlarmState: "getAlarmState",
  listAlarmHistory: "listAlarmHistory",
});

const RPC_CODE_MESSAGES = Object.freeze({
  0: "success",
  1: "RPC request failed",
  2: "invalid RPC request",
  3: "unsupported RPC method",
  4: "RPC request rate limited",
  5: "RPC request timed out",
  6: "resource state changed",
  100: "resource not found",
  102: "reference target initialization failed",
  104: "target lost",
  200: "measurement not started",
  201: "measurement already running",
  300: "motor unavailable",
  302: "motor moving",
  303: "motor limit reached",
  310: "vertical motor unavailable",
  400: "cruise unavailable",
  403: "cruise already running",
});

function rpcCodeMessage(code) {
  return RPC_CODE_MESSAGES[code] ?? `RPC request failed (code=${code})`;
}

const ROOT_TYPES = Object.freeze({
  telemetry: "inteagle.vdm.mqtt.v1.Telemetry",
  attributes: "inteagle.vdm.mqtt.v1.Attributes",
  event: "inteagle.vdm.mqtt.v1.Event",
  "3A": "inteagle.vdm.mqtt.v1.Alarm",
  "rpc/req": "inteagle.vdm.mqtt.v1.RpcRequest",
  "rpc/resp": "inteagle.vdm.mqtt.v1.RpcResponse",
});

function parsePayloadFormat(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized !== "json" && normalized !== "protobuf") {
    throw new Error(`不支持的 Payload 格式: ${value}`);
  }
  return normalized;
}

class VdmTopics {
  constructor(baseTopic) {
    const normalized = String(baseTopic ?? "").replace(/^\/+|\/+$/g, "");
    if (!normalized || /[+#]/.test(normalized)) {
      throw new Error("baseTopic 不能为空且不能包含 MQTT 通配符");
    }
    this.baseTopic = normalized;
  }

  static forDevice(deviceId) {
    const normalized = String(deviceId ?? "").trim();
    if (!normalized || /[/+#]/.test(normalized)) {
      throw new Error("deviceId 不能为空且不能包含 MQTT Topic 分隔符或通配符");
    }
    return new VdmTopics(`vdm/${normalized}`);
  }

  topic(suffix) {
    return `${this.baseTopic}/${String(suffix).replace(/^\/+|\/+$/g, "")}`;
  }

  suffix(topic) {
    const prefix = `${this.baseTopic}/`;
    if (!topic.startsWith(prefix)) {
      throw new Error(`Topic 不属于当前设备: ${topic}`);
    }
    return topic.slice(prefix.length);
  }

  get wildcard() {
    return this.topic("#");
  }

  get rpcRequest() {
    return this.topic("rpc/req");
  }

  get rpcResponse() {
    return this.topic("rpc/resp");
  }
}

class VdmCodec {
  constructor(payloadFormat, options = {}) {
    this.payloadFormat = parsePayloadFormat(payloadFormat);
    const schemaPath = options.schemaPath ?? path.resolve(
      __dirname,
      "../../proto/inteagle_vdm_mqtt_v1.proto",
    );
    this.root = protobuf.loadSync(schemaPath);
    this.types = Object.fromEntries(
      Object.entries(ROOT_TYPES).map(([suffix, name]) => [suffix, this.root.lookupType(name)]),
    );
    this.rpcRequestType = this.types["rpc/req"];
  }

  decode(topic, topics, payload) {
    const suffix = topics.suffix(topic);
    const raw = Buffer.from(payload);
    let value;
    let data;
    if (suffix === "image") {
      value = VdmCodec.decodeImage(raw);
      data = value.messageType === 2
        ? {
            messageType: value.messageType,
            headerLength: value.headerLength,
            packageFormat: value.packageFormat,
            evidenceKind: value.evidenceKind,
            eventId: value.eventId,
            packageLength: value.packageLength,
            packageSha256: value.packageSha256,
            chunkIndex: value.chunkIndex,
            chunkCount: value.chunkCount,
            chunkOffset: value.chunkOffset,
            chunkBytes: value.chunk.length,
          }
        : {
            version: value.version,
            headerLength: value.headerLength,
            sensorId: value.sensorId,
            imageType: value.imageType,
            timestampS: value.timestampS,
            jpegBytes: value.jpeg.length,
          };
    } else if (this.payloadFormat === "json") {
      try {
        value = JSON.parse(raw.toString("utf8"));
      } catch (error) {
        throw new Error(`JSON 解析失败: ${error.message}`);
      }
      if (value === null || Array.isArray(value) || typeof value !== "object") {
        throw new Error("JSON 根节点必须是对象");
      }
      data = value;
    } else {
      const type = this.types[suffix];
      if (!type) {
        throw new Error(`未支持的 Topic: ${suffix}`);
      }
      value = type.decode(raw);
      if (value.schemaVersion !== SCHEMA_VERSION) {
        throw new Error(`不支持 schema_version=${value.schemaVersion ?? 0}`);
      }
      data = type.toObject(value, {
        enums: String,
        longs: String,
        bytes: String,
        defaults: false,
      });
    }
    return { topic, suffix, raw, value, data };
  }

  encodeRpcRequest(method, params = {}, reqId) {
    if (!Number.isInteger(reqId) || reqId === 0 || reqId < -2147483648 || reqId > 2147483647) {
      throw new Error("reqId 必须是非零 signed int32");
    }
    const expectedResponseField = PUBLIC_RPC_FIELDS[method];
    if (!expectedResponseField) {
      throw new Error(`RPC 方法不属于公开 VDM API: ${method}`);
    }
    if (params === null || Array.isArray(params) || typeof params !== "object") {
      throw new Error(`${method} 参数必须是对象`);
    }
    if (this.payloadFormat === "json") {
      return {
        payload: Buffer.from(JSON.stringify({ reqId, method, params })),
        expectedResponseField,
      };
    }
    const object = {
      schemaVersion: SCHEMA_VERSION,
      reqId,
      [expectedResponseField]: params,
    };
    validateProtoObject(this.rpcRequestType, object);
    const message = this.rpcRequestType.fromObject(object);
    const error = this.rpcRequestType.verify(message);
    if (error) {
      throw new Error(`${method} Protobuf 参数不合法: ${error}`);
    }
    return {
      payload: Buffer.from(this.rpcRequestType.encode(message).finish()),
      expectedResponseField,
    };
  }

  responseInfo(value) {
    if (this.payloadFormat === "protobuf") {
      const responseField = Object.values(PUBLIC_RPC_FIELDS).find(
        (field) => Object.hasOwn(value, field) && value[field] != null,
      ) ?? null;
      return {
        reqId: value.reqId,
        code: value.code,
        message: rpcCodeMessage(value.code),
        responseField,
      };
    }
    const reqId = value.reqId ?? value.req_id;
    if (!Number.isInteger(Number(reqId))) {
      throw new Error("JSON RPC 响应缺少 reqId");
    }
    const rawCode = value.code ?? 1;
    const normalized = String(rawCode).toLowerCase();
    const code = Number.isFinite(Number(rawCode))
      ? Number(rawCode)
      : (["ok", "success"].includes(normalized) ? 0 : 1);
    return {
      reqId: Number(reqId),
      code,
      message: rpcCodeMessage(code),
      responseField: null,
    };
  }

  static decodeImage(payload) {
    payload = Buffer.from(payload);
    if (payload.length > 0 && payload[0] === 2) {
      return VdmCodec.decodeEvidencePackageChunk(payload);
    }
    if (payload.length < 10) {
      throw new Error("图片 Payload 小于 VDM Header 与 JPEG 最小长度");
    }
    const headerLength = payload[1];
    if (headerLength < 8 || headerLength > payload.length) {
      throw new Error(`非法图片 Header 长度: ${headerLength}`);
    }
    const jpeg = Buffer.from(payload.subarray(headerLength));
    if (jpeg.length < 2 || jpeg[0] !== 0xff || jpeg[1] !== 0xd8) {
      throw new Error("图片数据不是 JPEG");
    }
    return {
      version: payload[0],
      headerLength,
      sensorId: payload[2],
      imageType: payload[3],
      timestampS: payload.readUInt32BE(4),
      jpeg,
    };
  }

  static decodeEvidencePackageChunk(payload) {
    payload = Buffer.from(payload);
    const headerLength = 76;
    const chunkBytes = 128 * 1024;
    if (payload.length < headerLength) {
      throw new Error("告警抓拍图像包 Payload 小于 76 字节固定 Header");
    }
    if (payload[0] !== 2 || payload[1] !== headerLength || payload[2] !== 1 || payload[3] !== 1) {
      throw new Error("当前只支持 USTAR SNAPSHOT 抓拍图像包");
    }
    const eventId = payload.readBigUInt64BE(4);
    const packageLength = payload.readBigUInt64BE(12);
    const chunkIndex = payload.readUInt32BE(52);
    const chunkCount = payload.readUInt32BE(56);
    const chunkOffset = payload.readBigUInt64BE(60);
    const chunkLength = payload.readUInt32BE(68);
    const flags = payload.readUInt32BE(72);
    const expectedCount = Number((packageLength + BigInt(chunkBytes - 1)) / BigInt(chunkBytes));
    const expectedOffset = BigInt(chunkIndex) * BigInt(chunkBytes);
    const expectedLength = Number(
      packageLength - expectedOffset < BigInt(chunkBytes)
        ? packageLength - expectedOffset : BigInt(chunkBytes),
    );
    if (eventId === 0n || packageLength < 1n || packageLength > 32n * 1024n * 1024n
        || chunkCount !== expectedCount || chunkIndex >= chunkCount
        || chunkOffset !== expectedOffset || chunkLength !== expectedLength
        || payload.length !== headerLength + chunkLength || flags !== 0) {
      throw new Error("告警抓拍图像包身份、分块范围、长度或 flags 非法");
    }
    const chunk = Buffer.from(payload.subarray(headerLength));
    if (chunkIndex === 0 && (chunk.length < 262 || chunk.subarray(257, 262).toString("ascii") !== "ustar")) {
      throw new Error("告警抓拍图像包首块缺少 USTAR 标识");
    }
    return {
      messageType: 2,
      headerLength,
      packageFormat: payload[2],
      evidenceKind: payload[3],
      eventId: eventId.toString(),
      packageLength: packageLength.toString(),
      packageSha256: payload.subarray(20, 52).toString("hex"),
      chunkIndex,
      chunkCount,
      chunkOffset: chunkOffset.toString(),
      chunk,
    };
  }
}

class RpcError extends Error {
  constructor(reqId, code, message) {
    super(`RPC req_id=${reqId} code=${code}: ${message}`);
    this.name = "RpcError";
    this.reqId = reqId;
    this.code = code;
  }
}

class VdmMqttClient {
  constructor(config, onMessage = null, onError = null) {
    if (!config?.host || !Number.isInteger(config.port) || config.port < 1 || config.port > 65535) {
      throw new Error("MQTT host/port 无效");
    }
    if (!(config.topics instanceof VdmTopics)) {
      throw new Error("topics 必须是 VdmTopics");
    }
    const qos = config.qos ?? 1;
    if (![0, 1, 2].includes(qos)) {
      throw new Error("MQTT qos 必须是 0、1 或 2");
    }
    const supportedSuffixes = new Set(["telemetry", "attributes", "event", "3A", "image", "rpc/req", "rpc/resp"]);
    if (config.subscriptionSuffixes != null
        && (!Array.isArray(config.subscriptionSuffixes) || config.subscriptionSuffixes.length === 0
            || config.subscriptionSuffixes.some((suffix) => !supportedSuffixes.has(suffix)))) {
      throw new Error("subscriptionSuffixes 必须包含有效的 VDM Topic 后缀");
    }
    this.config = {
      ...config,
      subscriptionSuffixes: config.subscriptionSuffixes == null ? null : [...config.subscriptionSuffixes],
      qos,
      payloadFormat: parsePayloadFormat(config.payloadFormat),
      connectTimeoutMs: config.connectTimeoutMs ?? 10_000,
      clientId: config.clientId ?? `vdm-sdk-js-${Math.random().toString(16).slice(2, 14)}`,
    };
    this.codec = new VdmCodec(this.config.payloadFormat, { schemaPath: config.schemaPath });
    this.onMessage = onMessage;
    this.onError = onError;
    this.client = null;
    this.pending = new Map();
    this.nextReqId = crypto.randomInt(1, 2147483647);
    this.started = false;
    this.stopping = false;
  }

  async start() {
    if (this.client) {
      throw new Error("VDM MQTT client 已启动");
    }
    const options = {
      clientId: this.config.clientId,
      clean: true,
      reconnectPeriod: 1_000,
      connectTimeout: Math.min(this.config.connectTimeoutMs, 10_000),
      username: this.config.username,
      password: this.config.password,
    };
    this.client = mqtt.connect(`mqtt://${this.config.host}:${this.config.port}`, options);
    this.client.on("message", (topic, payload) => this.handleMessage(topic, payload));
    this.client.on("error", (error) => {
      if (this.started) this.report(error);
    });
    this.client.on("close", () => {
      if (this.started && !this.stopping) {
        this.failPending(new Error("MQTT 连接断开"));
      }
    });

    try {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error("连接 MQTT Broker 超时")),
          this.config.connectTimeoutMs,
        );
        this.client.once("connect", () => {
          this.subscribe().then(() => {
            clearTimeout(timer);
            this.started = true;
            resolve();
          }, (error) => {
            clearTimeout(timer);
            reject(error);
          });
        });
      });
    } catch (error) {
      await this.stop();
      throw error;
    }
    this.client.on("connect", () => {
      this.subscribe().catch((error) => this.report(error));
    });
  }

  async subscribe() {
    const topics = this.config.subscriptionSuffixes?.map((suffix) => this.config.topics.topic(suffix))
      ?? this.config.topics.wildcard;
    const expected = Array.isArray(topics) ? topics.length : 1;
    await new Promise((resolve, reject) => {
      this.client.subscribe(
        topics,
        { qos: this.config.qos },
        (error, granted) => {
          if (error) return reject(error);
          if (!Array.isArray(granted) || granted.length !== expected
            || granted.some(({ qos }) => ![0, 1, 2].includes(qos))) {
            return reject(new Error("MQTT Broker 未确认全部订阅"));
          }
          resolve();
        },
      );
    });
  }

  async publishRaw(topic, payload, retained = false) {
    if (!this.client?.connected) {
      throw new Error("MQTT client 未连接");
    }
    await new Promise((resolve, reject) => {
      this.client.publish(
        topic,
        payload,
        { qos: this.config.qos, retain: retained },
        (error) => (error ? reject(error) : resolve()),
      );
    });
  }

  async call(method, params = {}, options = {}) {
    let reqId = options.reqId ?? this.nextRequestId();
    const timeoutMs = options.timeoutMs ?? 10_000;
    const allowError = options.allowError ?? false;
    const encoded = this.codec.encodeRpcRequest(method, params, reqId);
    if (this.pending.has(reqId)) {
      throw new Error(`reqId 已在当前连接等待响应: ${reqId}`);
    }

    let resolvePending;
    let rejectPending;
    const result = new Promise((resolve, reject) => {
      resolvePending = resolve;
      rejectPending = reject;
    });
    const pending = {
      expected: encoded.expectedResponseField,
      resolve: resolvePending,
      reject: rejectPending,
      timer: null,
    };
    pending.timer = setTimeout(() => {
      if (this.pending.get(reqId) === pending) {
        this.pending.delete(reqId);
        pending.reject(new Error(`等待 RPC 响应超时: method=${method}, reqId=${reqId}`));
      }
    }, timeoutMs);
    this.pending.set(reqId, pending);

    try {
      await this.publishRaw(this.config.topics.rpcRequest, encoded.payload);
      const response = await result;
      const info = this.codec.responseInfo(response.value);
      if (info.reqId !== reqId) {
        throw new Error(`RPC reqId 不匹配: request=${reqId} response=${info.reqId}`);
      }
      if (info.code !== 0 && !allowError) {
        throw new RpcError(reqId, info.code, info.message);
      }
      if (
        info.code === 0
        && this.config.payloadFormat === "protobuf"
        && info.responseField !== pending.expected
      ) {
        throw new Error(
          `RPC response oneof 不匹配: expected=${pending.expected} actual=${info.responseField}`,
        );
      }
      return response;
    } finally {
      clearTimeout(pending.timer);
      if (this.pending.get(reqId) === pending) {
        this.pending.delete(reqId);
      }
    }
  }

  getEvidenceStatus(eventId, options = {}) {
    return this.call("getEvidenceStatus", {
      eventId: normalizeEventId(eventId),
      kind: this.snapshotKind(),
    }, options);
  }

  retryEvidence(eventId, options = {}) {
    return this.call("retryEvidence", {
      eventId: normalizeEventId(eventId),
      kind: this.snapshotKind(),
    }, options);
  }

  ackEvidencePackage(eventId, packageSha256, options = {}) {
    if (!/^[0-9a-f]{64}$/.test(String(packageSha256))) {
      throw new Error("packageSha256 必须是 64 个小写十六进制字符");
    }
    return this.call("ackEvidencePackage", {
      eventId: normalizeEventId(eventId),
      kind: this.snapshotKind(),
      packageSha256,
    }, options);
  }

  snapshotKind() {
    return this.config.payloadFormat === "protobuf" ? "EVIDENCE_KIND_SNAPSHOT" : "SNAPSHOT";
  }

  nextRequestId() {
    do {
      this.nextReqId = this.nextReqId >= 2147483647 ? -2147483648 : this.nextReqId + 1;
    } while (this.nextReqId === 0);
    return this.nextReqId;
  }

  handleMessage(topic, payload) {
    try {
      const decoded = this.codec.decode(topic, this.config.topics, payload);
      if (decoded.suffix === "rpc/resp") {
        const info = this.codec.responseInfo(decoded.value);
        const pending = this.pending.get(info.reqId);
        if (pending) {
          pending.resolve(decoded);
        }
      }
      this.onMessage?.(decoded);
    } catch (error) {
      this.report(error);
    }
  }

  failPending(error) {
    const pending = [...this.pending.values()];
    this.pending.clear();
    for (const call of pending) {
      clearTimeout(call.timer);
      call.reject(error);
    }
  }

  report(error) {
    this.onError?.(error);
  }

  async stop() {
    if (!this.client) {
      return;
    }
    this.stopping = true;
    this.started = false;
    this.failPending(new Error("VDM MQTT client stopped"));
    const client = this.client;
    this.client = null;
    await new Promise((resolve) => client.end(false, {}, resolve));
  }
}

module.exports = {
  AlarmSnapshotPackageAssembler,
  PUBLIC_RPC_FIELDS,
  RpcError,
  SCHEMA_VERSION,
  VdmCodec,
  VdmMqttClient,
  VdmTopics,
  parsePayloadFormat,
};

function normalizeEventId(eventId) {
  const value = typeof eventId === "bigint" ? eventId.toString() : String(eventId);
  if (!/^[1-9][0-9]*$/.test(value) || BigInt(value) > 0xffff_ffff_ffff_ffffn) {
    throw new Error("eventId 必须是非零十进制整数");
  }
  return value;
}
