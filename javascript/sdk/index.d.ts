export type PayloadFormat = "json" | "protobuf";
export type PublicRpcMethod =
  | "getAttr"
  | "setAttr"
  | "reboot"
  | "syncTime"
  | "initRefTargets"
  | "addTargets"
  | "getTargets"
  | "setTargets"
  | "deleteTargets"
  | "startMeasurement"
  | "stopMeasurement"
  | "setLightLevel"
  | "getLightLevel"
  | "snapshot"
  | "ispCtl"
  | "setMotorAngle"
  | "getMotorAngle"
  | "setMotorZero"
  | "enableMotor"
  | "disableMotor"
  | "getCruisePaths"
  | "getEvidenceStatus"
  | "retryEvidence"
  | "ackEvidenceImages"
  | "getAlarmCaps"
  | "listAlarmRules"
  | "applyAlarmRules"
  | "getAlarmState"
  | "listAlarmHistory";

export class VdmTopics {
  readonly baseTopic: string;
  constructor(baseTopic: string);
  static forDevice(deviceId: string): VdmTopics;
  topic(suffix: string): string;
  suffix(topic: string): string;
  readonly wildcard: string;
  readonly rpcRequest: string;
  readonly rpcResponse: string;
}

export interface ImageFrame {
  version: number;
  headerLength: number;
  sensorId: number;
  imageType: number;
  timestampS: number;
  jpeg: Uint8Array;
}

export interface EvidenceImageChunk {
  messageType: 2;
  headerLength: 112;
  cameraId: number;
  triggerType: 1;
  capturedAtMs: string;
  eventId: string;
  imageIndex: number;
  imageCount: number;
  actualOffsetMs: number;
  jpegLength: number;
  jpegSha256: string;
  manifestSha256: string;
  chunkIndex: number;
  chunkCount: number;
  chunkOffset: number;
  chunk: Uint8Array;
}

export interface DecodedPayload {
  topic: string;
  suffix: string;
  raw: Uint8Array;
  value: object | ImageFrame | EvidenceImageChunk;
  data: Record<string, unknown>;
}

export class VdmCodec {
  constructor(payloadFormat: PayloadFormat, options?: { schemaPath?: string });
  readonly payloadFormat: PayloadFormat;
  decode(topic: string, topics: VdmTopics, payload: Uint8Array): DecodedPayload;
  encodeRpcRequest(
    method: PublicRpcMethod,
    params: Record<string, unknown>,
    reqId: number,
  ): { payload: Uint8Array; expectedResponseField: string };
  responseInfo(value: object): {
    reqId: number;
    code: number;
    message: string;
    responseField: string | null;
  };
  static decodeImage(payload: Uint8Array): ImageFrame | EvidenceImageChunk;
  static decodeEvidenceImageChunk(payload: Uint8Array): EvidenceImageChunk;
}

export interface VdmMqttClientConfig {
  host: string;
  port: number;
  topics: VdmTopics;
  payloadFormat: PayloadFormat;
  username?: string;
  password?: string;
  clientId?: string;
  qos?: 0 | 1 | 2;
  connectTimeoutMs?: number;
  schemaPath?: string;
}

export class RpcError extends Error {
  readonly reqId: number;
  readonly code: number;
}

export class VdmMqttClient {
  constructor(
    config: VdmMqttClientConfig,
    onMessage?: ((message: DecodedPayload) => void) | null,
    onError?: ((error: Error) => void) | null,
  );
  readonly codec: VdmCodec;
  start(): Promise<void>;
  stop(): Promise<void>;
  publishRaw(topic: string, payload: string | Uint8Array, retained?: boolean): Promise<void>;
  call(
    method: PublicRpcMethod,
    params?: Record<string, unknown>,
    options?: { reqId?: number; timeoutMs?: number; allowError?: boolean },
  ): Promise<DecodedPayload>;
}

export const SCHEMA_VERSION: 1;
export const PUBLIC_RPC_FIELDS: Readonly<Record<PublicRpcMethod, string>>;
export function parsePayloadFormat(value: string): PayloadFormat;
