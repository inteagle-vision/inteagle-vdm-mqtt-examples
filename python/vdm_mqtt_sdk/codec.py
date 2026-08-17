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
EVIDENCE_IMAGE_HEADER_LENGTH = 112
EVIDENCE_CHUNK_BYTES = 128 * 1024
MAX_EVIDENCE_IMAGE_BYTES = 2 * 1024 * 1024
MAX_EVIDENCE_IMAGES = 64


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
class EvidenceImageChunk:
    message_type: int
    header_length: int
    camera_id: int
    trigger_type: int
    captured_at_ms: int
    event_id: int
    image_index: int
    image_count: int
    actual_offset_ms: int
    jpeg_length: int
    jpeg_sha256: bytes
    manifest_sha256: bytes
    chunk_index: int
    chunk_count: int
    chunk_offset: int
    chunk: bytes


@dataclass(frozen=True)
class DecodedPayload:
    topic: str
    suffix: str
    raw: bytes
    value: dict[str, Any] | Message | ImageFrame | EvidenceImageChunk

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
        if isinstance(self.value, EvidenceImageChunk):
            return {
                "messageType": self.value.message_type,
                "headerLength": self.value.header_length,
                "cameraId": self.value.camera_id,
                "triggerType": self.value.trigger_type,
                "capturedAtMs": self.value.captured_at_ms,
                "eventId": str(self.value.event_id),
                "imageIndex": self.value.image_index,
                "imageCount": self.value.image_count,
                "actualOffsetMs": self.value.actual_offset_ms,
                "jpegLength": self.value.jpeg_length,
                "jpegSha256": self.value.jpeg_sha256.hex(),
                "manifestSha256": self.value.manifest_sha256.hex(),
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
    "evidence": pb.AlarmEvidence,
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
    "getStorageInfo": ("get_storage_info", pb.Empty),
    "queryTelemetry": ("query_telemetry", pb.QueryTelemetryRequest),
    "uploadS3": ("upload_s3", pb.UploadS3Request),
    "ispCtl": ("isp_ctl", pb.IspControlRequest),
    "setMotorAngle": ("set_motor_angle", pb.SetMotorAngleRequest),
    "getMotorAngle": ("get_motor_angle", pb.Empty),
    "setMotorZero": ("set_motor_zero", pb.SetMotorZeroRequest),
    "enableMotor": ("enable_motor", pb.Empty),
    "disableMotor": ("disable_motor", pb.Empty),
    "getCruisePaths": ("get_cruise_paths", pb.GetCruisePathsRequest),
    "setCruisePoint": ("set_cruise_point", pb.SetCruisePointRequest),
    "removeCruisePoint": ("remove_cruise_point", pb.RemoveCruisePointRequest),
    "startPatrol": ("start_patrol", pb.StartPatrolRequest),
    "stopPatrol": ("stop_patrol", pb.Empty),
    "getPatrolStatus": ("get_patrol_status", pb.Empty),
    "getEvidenceStatus": ("get_evidence_status", pb.EvidenceQueryRequest),
    "retryEvidence": ("retry_evidence", pb.EvidenceQueryRequest),
    "ackEvidenceImages": ("ack_evidence_images", pb.EvidenceImagesAckRequest),
}

# 0.8.5 设备已经向 StdMqtt 开放这些告警 RPC，但 V1 Protobuf oneof 尚未为其分配
# 字段编号。JSON 连接可以使用；Protobuf 连接会在发布前给出明确错误，避免发出设备
# 无法解码的伪 Protobuf 请求。
JSON_ONLY_RPC_FIELDS: dict[str, str] = {
    "getAlarmCaps": "get_alarm_caps",
    "listAlarmRules": "list_alarm_rules",
    "applyAlarmRules": "apply_alarm_rules",
    "getAlarmState": "get_alarm_state",
    "listAlarmHistory": "list_alarm_history",
}


class VdmCodec:
    def __init__(self, payload_format: str | PayloadFormat) -> None:
        self.payload_format = PayloadFormat.parse(payload_format)

    @staticmethod
    def decode_image(payload: bytes) -> ImageFrame | EvidenceImageChunk:
        if payload and payload[0] == 2:
            return VdmCodec.decode_evidence_image_chunk(payload)
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
    def decode_evidence_image_chunk(payload: bytes) -> EvidenceImageChunk:
        if len(payload) < EVIDENCE_IMAGE_HEADER_LENGTH:
            raise ValueError("告警证据图片 Payload 小于 112 字节固定 Header")
        if payload[0] != 2 or payload[1] != EVIDENCE_IMAGE_HEADER_LENGTH:
            raise ValueError("告警证据图片 messageType/headerLen 非法")
        if payload[3] != 1:
            raise ValueError("告警证据图片 triggerType 必须为 1")

        captured_at_ms = int.from_bytes(payload[4:12], "big")
        event_id = int.from_bytes(payload[12:20], "big")
        image_index = int.from_bytes(payload[20:22], "big")
        image_count = int.from_bytes(payload[22:24], "big")
        actual_offset_ms = int.from_bytes(payload[24:28], "big", signed=True)
        jpeg_length = int.from_bytes(payload[28:32], "big")
        chunk_index = int.from_bytes(payload[96:98], "big")
        chunk_count = int.from_bytes(payload[98:100], "big")
        chunk_offset = int.from_bytes(payload[100:104], "big")
        chunk_length = int.from_bytes(payload[104:108], "big")
        flags = int.from_bytes(payload[108:112], "big")

        if captured_at_ms == 0 or event_id == 0:
            raise ValueError("告警证据图片时间或 eventId 非法")
        if image_count == 0 or image_count > MAX_EVIDENCE_IMAGES or image_index >= image_count:
            raise ValueError("告警证据图片 imageIndex/imageCount 非法")
        if jpeg_length == 0 or jpeg_length > MAX_EVIDENCE_IMAGE_BYTES:
            raise ValueError("告警证据 JPEG 长度超出 1..2 MiB")
        expected_count = (jpeg_length + EVIDENCE_CHUNK_BYTES - 1) // EVIDENCE_CHUNK_BYTES
        expected_offset = chunk_index * EVIDENCE_CHUNK_BYTES
        expected_length = min(EVIDENCE_CHUNK_BYTES, jpeg_length - expected_offset)
        if (
            chunk_count != expected_count
            or chunk_index >= chunk_count
            or chunk_offset != expected_offset
            or chunk_length != expected_length
            or len(payload) != EVIDENCE_IMAGE_HEADER_LENGTH + chunk_length
            or flags != 0
        ):
            raise ValueError("告警证据图片分块范围、长度或 flags 非法")
        chunk = bytes(payload[EVIDENCE_IMAGE_HEADER_LENGTH:])
        if chunk_index == 0 and not chunk.startswith(b"\xff\xd8"):
            raise ValueError("告警证据 JPEG 首块缺少 SOI")
        return EvidenceImageChunk(
            message_type=2,
            header_length=EVIDENCE_IMAGE_HEADER_LENGTH,
            camera_id=payload[2],
            trigger_type=payload[3],
            captured_at_ms=captured_at_ms,
            event_id=event_id,
            image_index=image_index,
            image_count=image_count,
            actual_offset_ms=actual_offset_ms,
            jpeg_length=jpeg_length,
            jpeg_sha256=bytes(payload[32:64]),
            manifest_sha256=bytes(payload[64:96]),
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            chunk_offset=chunk_offset,
            chunk=chunk,
        )

    def decode(self, topic: str, topics: VdmTopics, payload: bytes) -> DecodedPayload:
        suffix = topics.suffix(topic)
        if suffix == "image":
            value: dict[str, Any] | Message | ImageFrame | EvidenceImageChunk = self.decode_image(
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
            if suffix != "evidence" and getattr(message, "schema_version", 0) != SCHEMA_VERSION:
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
        json_only_field = JSON_ONLY_RPC_FIELDS.get(method)
        if route is None and json_only_field is None:
            raise ValueError(f"RPC 方法不属于公开 VDM API: {method}")
        params = params or {}
        if self.payload_format is PayloadFormat.JSON:
            return (
                json.dumps(
                    {"reqId": req_id, "method": method, "params": params},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8"),
                route[0] if route is not None else json_only_field,
            )

        if route is None:
            raise ValueError(f"{method} 在 0.8.5 仅支持 StdMqtt JSON Payload")
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
            return value.req_id, value.code, value.message, value.WhichOneof("response")
        if not isinstance(value, dict):
            raise ValueError("RPC 响应类型错误")
        req_id_value = value.get("reqId", value.get("req_id"))
        req_id = int(req_id_value) if req_id_value is not None else None
        raw_code = value.get("code", 1)
        try:
            code = int(raw_code)
        except (TypeError, ValueError):
            code = 0 if str(raw_code).lower() in {"ok", "success"} else 1
        message = str(value.get("msg", value.get("message", "")))
        return req_id, code, message, None
