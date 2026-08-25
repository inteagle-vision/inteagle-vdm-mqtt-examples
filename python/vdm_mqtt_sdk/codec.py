"""VDM MQTT Topic 与 JSON/Protobuf 编解码。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

from google.protobuf import json_format
from google.protobuf.message import DecodeError, Message

import inteagle_vdm_mqtt_v1_pb2 as pb


SCHEMA_VERSION = 1
EVIDENCE_PACKAGE_HEADER_LENGTH = 76
EVIDENCE_CHUNK_BYTES = 128 * 1024
MAX_EVIDENCE_PACKAGE_BYTES = 32 * 1024 * 1024

RPC_CODE_MESSAGES = {
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
}


def rpc_code_message(code: int) -> str:
    return RPC_CODE_MESSAGES.get(code, f"RPC request failed (code={code})")


class PayloadFormat(str, Enum):
    JSON = "json"
    PROTOBUF = "protobuf"

    @classmethod
    def parse(cls, value: str | "PayloadFormat") -> "PayloadFormat":
        if isinstance(value, cls):
            return value
        normalized = value.strip().lower()
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError(f"不支持的 Payload 格式: {value}") from exc


@dataclass(frozen=True)
class VdmTopics:
    """一个设备的标准 VDM Topic 集合。"""

    base_topic: str

    @classmethod
    def for_device(cls, device_id: str) -> "VdmTopics":
        normalized = device_id.strip()
        if not normalized or "/" in normalized or "+" in normalized or "#" in normalized:
            raise ValueError("device_id 不能为空且不能包含 MQTT Topic 分隔符或通配符")
        return cls(f"vdm/{normalized}")

    def __post_init__(self) -> None:
        normalized = self.base_topic.strip("/")
        if not normalized or "+" in normalized or "#" in normalized:
            raise ValueError("base_topic 不能为空且不能包含 MQTT 通配符")
        object.__setattr__(self, "base_topic", normalized)

    def topic(self, suffix: str) -> str:
        return f"{self.base_topic}/{suffix.strip('/')}"

    def suffix(self, topic: str) -> str:
        prefix = f"{self.base_topic}/"
        if not topic.startswith(prefix):
            raise ValueError(f"Topic 不属于当前设备: {topic}")
        return topic[len(prefix) :]

    @property
    def wildcard(self) -> str:
        return self.topic("#")

    @property
    def rpc_request(self) -> str:
        return self.topic("rpc/req")

    @property
    def rpc_response(self) -> str:
        return self.topic("rpc/resp")


@dataclass(frozen=True)
class ImageFrame:
    version: int
    header_length: int
    sensor_id: int
    image_type: int
    timestamp_s: int
    jpeg: bytes


@dataclass(frozen=True)
class EvidencePackageChunk:
    message_type: int
    header_length: int
    package_format: int
    evidence_kind: int
    event_id: int
    package_length: int
    package_sha256: bytes
    chunk_index: int
    chunk_count: int
    chunk_offset: int
    chunk: bytes


@dataclass(frozen=True)
class DecodedPayload:
    topic: str
    suffix: str
    raw: bytes
    value: dict[str, Any] | Message | ImageFrame | EvidencePackageChunk

    def as_dict(self) -> dict[str, Any]:
        """返回便于日志、Web API 和业务分派使用的完整字段对象。

        ``value`` 仍保留生成的 Protobuf 强类型消息；本方法只生成一份 JSON 兼容视图。
        图片不会复制 JPEG 内容，只返回头字段与 JPEG 字节数。
        """
        if isinstance(self.value, dict):
            return self.value
        if isinstance(self.value, ImageFrame):
            return {
                "version": self.value.version,
                "headerLength": self.value.header_length,
                "sensorId": self.value.sensor_id,
                "imageType": self.value.image_type,
                "timestampS": self.value.timestamp_s,
                "jpegBytes": len(self.value.jpeg),
            }
        if isinstance(self.value, EvidencePackageChunk):
            return {
                "messageType": self.value.message_type,
                "headerLength": self.value.header_length,
                "packageFormat": self.value.package_format,
                "evidenceKind": self.value.evidence_kind,
                "eventId": str(self.value.event_id),
                "packageLength": self.value.package_length,
                "packageSha256": self.value.package_sha256.hex(),
                "chunkIndex": self.value.chunk_index,
                "chunkCount": self.value.chunk_count,
                "chunkOffset": self.value.chunk_offset,
                "chunkBytes": len(self.value.chunk),
            }
        return json_format.MessageToDict(
            self.value,
            preserving_proto_field_name=False,
            use_integers_for_enums=False,
        )


PROTOBUF_MESSAGE_TYPES: dict[str, type[Message]] = {
    "telemetry": pb.Telemetry,
    "attributes": pb.Attributes,
    "event": pb.Event,
    "3A": pb.Alarm,
    "rpc/req": pb.RpcRequest,
    "rpc/resp": pb.RpcResponse,
}

# 客户方法名、RpcRequest oneof 字段、请求消息类型。通过这个显式矩阵，SDK 不会
# 把设备内部命令名称当成可调用的客户 RPC。
RPC_REQUEST_TYPES: dict[str, tuple[str, type[Message]]] = {
    "getAttr": ("get_attr", pb.GetAttributesRequest),
    "setAttr": ("set_attr", pb.SetAttributesRequest),
    "reboot": ("reboot", pb.Empty),
    "syncTime": ("sync_time", pb.SyncTimeRequest),
    "initRefTargets": ("init_ref_targets", pb.InitReferenceTargetsRequest),
    "addTargets": ("add_targets", pb.AddTargetsRequest),
    "getTargets": ("get_targets", pb.Empty),
    "setTargets": ("set_targets", pb.SetTargetsRequest),
    "deleteTargets": ("delete_targets", pb.DeleteTargetsRequest),
    "startMeasurement": ("start_measurement", pb.Empty),
    "stopMeasurement": ("stop_measurement", pb.Empty),
    "setLightLevel": ("set_light_level", pb.SetLightLevelRequest),
    "getLightLevel": ("get_light_level", pb.Empty),
    "snapshot": ("snapshot", pb.SnapshotRequest),
    "ispCtl": ("isp_ctl", pb.IspControlRequest),
    "setMotorAngle": ("set_motor_angle", pb.SetMotorAngleRequest),
    "getMotorAngle": ("get_motor_angle", pb.Empty),
    "setMotorZero": ("set_motor_zero", pb.SetMotorZeroRequest),
    "enableMotor": ("enable_motor", pb.Empty),
    "disableMotor": ("disable_motor", pb.Empty),
    "getCruisePaths": ("get_cruise_paths", pb.GetCruisePathsRequest),
    "getEvidenceStatus": ("get_evidence_status", pb.EvidenceQueryRequest),
    "retryEvidence": ("retry_evidence", pb.EvidenceQueryRequest),
    "ackEvidencePackage": ("ack_evidence_package", pb.EvidencePackageAckRequest),
    "getAlarmCaps": ("get_alarm_caps", pb.GetAlarmCapsRequest),
    "listAlarmRules": ("list_alarm_rules", pb.ListAlarmRulesRequest),
    "applyAlarmRules": ("apply_alarm_rules", pb.ApplyAlarmRulesRequest),
    "getAlarmState": ("get_alarm_state", pb.GetAlarmStateRequest),
    "listAlarmHistory": ("list_alarm_history", pb.ListAlarmHistoryRequest),
}


class VdmCodec:
    def __init__(self, payload_format: str | PayloadFormat) -> None:
        self.payload_format = PayloadFormat.parse(payload_format)

    @staticmethod
    def decode_image(payload: bytes) -> ImageFrame | EvidencePackageChunk:
        if payload and payload[0] == 2:
            return VdmCodec.decode_evidence_package_chunk(payload)
        if len(payload) < 10:
            raise ValueError("图片 Payload 小于 VDM Header 与 JPEG 最小长度")
        header_length = payload[1]
        if header_length < 8 or header_length > len(payload):
            raise ValueError(f"非法图片 Header 长度: {header_length}")
        jpeg = payload[header_length:]
        if not jpeg.startswith(b"\xff\xd8"):
            raise ValueError("图片数据不是 JPEG")
        return ImageFrame(
            version=payload[0],
            header_length=header_length,
            sensor_id=payload[2],
            image_type=payload[3],
            timestamp_s=int.from_bytes(payload[4:8], "big"),
            jpeg=jpeg,
        )

    @staticmethod
    def decode_evidence_package_chunk(payload: bytes) -> EvidencePackageChunk:
        if len(payload) < EVIDENCE_PACKAGE_HEADER_LENGTH:
            raise ValueError("告警抓拍图像包 Payload 小于 76 字节固定 Header")
        if payload[0] != 2 or payload[1] != EVIDENCE_PACKAGE_HEADER_LENGTH:
            raise ValueError("告警抓拍图像包 messageType/headerLen 非法")
        if payload[2] != 1 or payload[3] != 1:
            raise ValueError("当前只支持 USTAR SNAPSHOT 抓拍图像包")

        event_id = int.from_bytes(payload[4:12], "big")
        package_length = int.from_bytes(payload[12:20], "big")
        chunk_index = int.from_bytes(payload[52:56], "big")
        chunk_count = int.from_bytes(payload[56:60], "big")
        chunk_offset = int.from_bytes(payload[60:68], "big")
        chunk_length = int.from_bytes(payload[68:72], "big")
        flags = int.from_bytes(payload[72:76], "big")

        if event_id == 0:
            raise ValueError("告警抓拍图像包 eventId 非法")
        if package_length == 0 or package_length > MAX_EVIDENCE_PACKAGE_BYTES:
            raise ValueError("告警抓拍图像包长度超出 1..32 MiB")
        expected_count = (package_length + EVIDENCE_CHUNK_BYTES - 1) // EVIDENCE_CHUNK_BYTES
        expected_offset = chunk_index * EVIDENCE_CHUNK_BYTES
        expected_length = min(EVIDENCE_CHUNK_BYTES, package_length - expected_offset)
        if (
            chunk_count != expected_count
            or chunk_index >= chunk_count
            or chunk_offset != expected_offset
            or chunk_length != expected_length
            or len(payload) != EVIDENCE_PACKAGE_HEADER_LENGTH + chunk_length
            or flags != 0
        ):
            raise ValueError("告警抓拍图像包分块范围、长度或 flags 非法")
        chunk = bytes(payload[EVIDENCE_PACKAGE_HEADER_LENGTH:])
        if chunk_index == 0 and (len(chunk) < 262 or chunk[257:262] != b"ustar"):
            raise ValueError("告警抓拍图像包首块缺少 USTAR 标识")
        return EvidencePackageChunk(
            message_type=2,
            header_length=EVIDENCE_PACKAGE_HEADER_LENGTH,
            package_format=payload[2],
            evidence_kind=payload[3],
            event_id=event_id,
            package_length=package_length,
            package_sha256=bytes(payload[20:52]),
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            chunk_offset=chunk_offset,
            chunk=chunk,
        )

    def decode(self, topic: str, topics: VdmTopics, payload: bytes) -> DecodedPayload:
        suffix = topics.suffix(topic)
        if suffix == "image":
            value: dict[str, Any] | Message | ImageFrame | EvidencePackageChunk = self.decode_image(
                payload
            )
        elif self.payload_format is PayloadFormat.JSON:
            try:
                value = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"JSON 解析失败: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError("JSON 根节点必须是对象")
        else:
            message_type = PROTOBUF_MESSAGE_TYPES.get(suffix)
            if message_type is None:
                raise ValueError(f"未支持的 Topic: {suffix}")
            message = message_type()
            try:
                message.ParseFromString(payload)
            except DecodeError as exc:
                raise ValueError(f"Protobuf 解析失败: {exc}") from exc
            if getattr(message, "schema_version", 0) != SCHEMA_VERSION:
                raise ValueError(
                    f"不支持 schema_version={getattr(message, 'schema_version', 0)}"
                )
            value = message
        return DecodedPayload(topic=topic, suffix=suffix, raw=payload, value=value)

    def encode_rpc_request(
        self,
        method: str,
        params: dict[str, Any] | None,
        req_id: int,
    ) -> tuple[bytes, str]:
        if req_id == 0 or not -(2**31) <= req_id < 2**31:
            raise ValueError("req_id 必须是非零 signed int32")
        route = RPC_REQUEST_TYPES.get(method)
        if route is None:
            raise ValueError(f"RPC 方法不属于公开 VDM API: {method}")
        params = params or {}
        if self.payload_format is PayloadFormat.JSON:
            return (
                json.dumps(
                    {"reqId": req_id, "method": method, "params": params},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8"),
                route[0],
            )

        response_field, message_type = route

        body = message_type()
        try:
            json_format.ParseDict(params, body, ignore_unknown_fields=False)
        except (json_format.ParseError, TypeError, ValueError) as exc:
            raise ValueError(f"{method} Protobuf 参数不合法: {exc}") from exc
        request = pb.RpcRequest(schema_version=SCHEMA_VERSION, req_id=req_id)
        getattr(request, response_field).CopyFrom(body)
        return request.SerializeToString(), response_field

    @staticmethod
    def response_info(value: dict[str, Any] | Message) -> tuple[int | None, int, str, str | None]:
        if isinstance(value, pb.RpcResponse):
            return value.req_id, value.code, rpc_code_message(value.code), value.WhichOneof("response")
        if not isinstance(value, dict):
            raise ValueError("RPC 响应类型错误")
        req_id_value = value.get("reqId", value.get("req_id"))
        req_id = int(req_id_value) if req_id_value is not None else None
        raw_code = value.get("code", 1)
        try:
            code = int(raw_code)
        except (TypeError, ValueError):
            code = 0 if str(raw_code).lower() in {"ok", "success"} else 1
        return req_id, code, rpc_code_message(code), None
