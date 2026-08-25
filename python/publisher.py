#!/usr/bin/env python3
"""等待所有示例订阅端就绪，发布测试数据并汇总结果。"""

from __future__ import annotations

import json
import hashlib
import io
import os
import tarfile
import threading
import time
from typing import Any

import paho.mqtt.client as mqtt

import common
import inteagle_vdm_mqtt_v1_pb2 as pb


TIMESTAMP_S = 1_721_805_600


def json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


EVIDENCE_EVENT_ID = 9_001
EVIDENCE_JPEG = b"\xff\xd8vdm-evidence-demo\xff\xd9"
def evidence_package() -> bytes:
    manifest = json_bytes(
        {
            "eventId": str(EVIDENCE_EVENT_ID),
            "kind": "SNAPSHOT",
            "images": [{"file": "000.jpg", "sha256": hashlib.sha256(EVIDENCE_JPEG).hexdigest()}],
        }
    )
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, content in (("manifest.json", manifest), ("000.jpg", EVIDENCE_JPEG)):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.mode = 0o600
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def evidence_package_frame() -> bytes:
    package = evidence_package()
    header = bytearray()
    header.extend((2, 76, 1, 1))
    header.extend(EVIDENCE_EVENT_ID.to_bytes(8, "big"))
    header.extend(len(package).to_bytes(8, "big"))
    header.extend(hashlib.sha256(package).digest())
    header.extend((0).to_bytes(4, "big"))
    header.extend((1).to_bytes(4, "big"))
    header.extend((0).to_bytes(8, "big"))
    header.extend(len(package).to_bytes(4, "big"))
    header.extend((0).to_bytes(4, "big"))
    assert len(header) == 76
    return bytes(header) + package


def protobuf_fixtures() -> list[tuple[str, bytes]]:
    telemetry = pb.Telemetry(schema_version=1)
    telemetry.displacement.sample_frequency_hz = 20
    telemetry.displacement.first_sample_timestamp_ms = TIMESTAMP_S * 1000
    target = telemetry.displacement.targets.add(target_id="T01")
    target.dx_mm.extend((0.125, 0.25, 0.375))
    target.dy_mm.extend((-0.5, -0.625, -0.75))

    attributes = pb.Attributes(
        schema_version=1,
        device_id="DEMO001",
        device_model="X1",
        firmware_version="example-1.0.0",
        measurement_status=pb.MEASUREMENT_STATUS_IDLE,
        sample_frequency_hz=20,
    )

    event = pb.Event(
        schema_version=1,
        timestamp_s=TIMESTAMP_S,
        event_type=pb.EVENT_TYPE_REF_INIT_RESULT,
    )
    event.ref_init_result.successful_target_ids.append("T01")

    alarm = pb.Alarm(
        schema_version=1,
        event_id=9_001,
        alarm_id=701,
        rule_id=31,
        alarm_type=pb.ALARM_TYPE_DISPLACEMENT_LIMIT,
        level=pb.ALARM_LEVEL_ALERT,
        transition=pb.ALARM_TRANSITION_TRIGGERED,
        timestamp_s=TIMESTAMP_S,
    )
    alarm.displacement.target_id = "T01"
    alarm.displacement.metric = pb.ALARM_METRIC_DX
    alarm.displacement.direction = pb.DISPLACEMENT_DIRECTION_POSITIVE
    alarm.displacement.value_mm = 3.5
    alarm.displacement.limit_mm = 3.0

    evidence = pb.Event(
        schema_version=1,
        timestamp_s=TIMESTAMP_S,
        event_type=pb.EVENT_TYPE_ALARM_EVIDENCE,
    )
    evidence.alarm_evidence.event_id = EVIDENCE_EVENT_ID
    evidence.alarm_evidence.kind = pb.EVIDENCE_KIND_SNAPSHOT
    evidence.alarm_evidence.state = pb.EVIDENCE_STATE_READY

    request = pb.RpcRequest(schema_version=1, req_id=42)
    request.get_attr.keys.extend(("deviceId", "fwVer"))

    response = pb.RpcResponse(schema_version=1, req_id=42, code=0)
    response.get_attr.attributes.schema_version = 1
    response.get_attr.attributes.device_id = "DEMO001"
    response.get_attr.attributes.firmware_version = "example-1.0.0"

    return [
        ("telemetry", telemetry.SerializeToString(deterministic=True)),
        ("attributes", attributes.SerializeToString(deterministic=True)),
        ("event", event.SerializeToString(deterministic=True)),
        ("3A", alarm.SerializeToString(deterministic=True)),
        ("event", evidence.SerializeToString(deterministic=True)),
        ("rpc/req", request.SerializeToString(deterministic=True)),
        ("rpc/resp", response.SerializeToString(deterministic=True)),
        ("image", evidence_package_frame()),
    ]


def json_fixtures() -> list[tuple[str, bytes]]:
    return [
        (
            "telemetry",
            json_bytes(
                {
                    "disp": {
                        "t": TIMESTAMP_S,
                        "f": 20,
                        "d": {
                            "T01": {
                                "dx": [0.125, 0.25, 0.375],
                                "dy": [-0.5, -0.625, -0.75],
                            }
                        },
                    }
                }
            ),
        ),
        (
            "attributes",
            json_bytes(
                {
                    "deviceId": "DEMO001",
                    "deviceModel": "X1",
                    "fwVer": "example-1.0.0",
                    "status": "idle",
                    "sampleFrequency": 20,
                }
            ),
        ),
        (
            "event",
            json_bytes(
                {
                    "type": "REF_INIT_RESULT",
                    "ts": TIMESTAMP_S,
                    "detail": {"ok": ["T01"], "fail": []},
                }
            ),
        ),
        (
            "3A",
            json_bytes(
                {
                    "eventId": "9001",
                    "alarmId": "701",
                    "ruleId": 31,
                    "type": "DISP_LIMIT",
                    "level": "ALERT",
                    "transition": "TRIGGERED",
                    "ts": TIMESTAMP_S,
                    "detail": {
                        "targetId": "T01",
                        "metric": "DX",
                        "direction": "POSITIVE",
                        "value": 3.5,
                        "limit": 3.0,
                    },
                }
            ),
        ),
        (
            "event",
            json_bytes(
                {
                    "type": "ALARM_EVIDENCE",
                    "ts": TIMESTAMP_S,
                    "detail": {
                        "eventId": str(EVIDENCE_EVENT_ID),
                        "kind": "SNAPSHOT",
                        "state": "READY",
                    },
                }
            ),
        ),
        (
            "rpc/req",
            json_bytes(
                {"reqId": 42, "method": "getAttr", "params": {"keys": ["deviceId", "fwVer"]}}
            ),
        ),
        (
            "rpc/resp",
            json_bytes(
                {
                    "reqId": 42,
                    "code": 0,
                    "data": {"deviceId": "DEMO001", "fwVer": "example-1.0.0"},
                }
            ),
        ),
        ("image", evidence_package_frame()),
    ]


class Publisher:
    def __init__(self) -> None:
        self.host = common.setting("MQTT_HOST", "127.0.0.1")
        self.port = int(common.setting("MQTT_PORT", "1883"))
        self.base_topic = common.setting("VDM_BASE_TOPIC", "vdm/DEMO001").strip("/")
        self.profile = common.setting("VDM_PAYLOAD_FORMAT", "protobuf")
        self.run_id = common.setting("VDM_RUN_ID", "manual")
        self.timeout = float(common.setting("VDM_WAIT_TIMEOUT", "45"))
        self.required = {
            value.strip()
            for value in common.setting("VDM_REQUIRED_CONSUMERS", "python,go,java").split(",")
            if value.strip()
        }
        if self.profile not in {"json", "protobuf"}:
            raise ValueError(f"不支持的 Payload 格式: {self.profile}")
        self.ready: set[str] = set()
        self.results: dict[str, str] = {}
        self.condition = threading.Condition()
        self.subscribed = threading.Event()
        self.client = common.new_client(f"vdm-example-publisher-{self.run_id}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    @property
    def control_topic(self) -> str:
        return f"vdm-example/{self.run_id}"

    def on_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        _flags: Any,
        reason_code: Any,
        _properties: Any = None,
    ) -> None:
        if not common.reason_code_ok(reason_code):
            return
        result, _mid = client.subscribe(
            [
                (f"{self.control_topic}/#", 1),
                (f"{self.base_topic}/rpc/req", 1),
            ]
        )
        if result == mqtt.MQTT_ERR_SUCCESS:
            self.subscribed.set()

    def on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        if msg.topic == f"{self.base_topic}/rpc/req":
            self.respond_rpc(msg.payload)
            return
        prefix = f"{self.control_topic}/"
        if not msg.topic.startswith(prefix):
            return
        parts = msg.topic[len(prefix) :].split("/", maxsplit=1)
        if len(parts) != 2:
            return
        kind, consumer = parts
        value = msg.payload.decode("utf-8", errors="replace")
        with self.condition:
            if kind == "ready" and value == "READY":
                self.ready.add(consumer)
            elif kind == "result":
                self.results[consumer] = value
            self.condition.notify_all()

    def respond_rpc(self, payload: bytes) -> None:
        """作为纯 MQTT 模拟设备，响应四种语言 SDK 发起的 getAttr。"""
        try:
            if self.profile == "protobuf":
                rpc_request = pb.RpcRequest.FromString(payload)
                if rpc_request.req_id == 42:
                    return
                response = pb.RpcResponse(
                    schema_version=1,
                    req_id=rpc_request.req_id,
                    code=0,
                )
                if rpc_request.WhichOneof("request") != "get_attr":
                    response.code = 3
                else:
                    response.get_attr.attributes.schema_version = 1
                    response.get_attr.attributes.device_id = "DEMO001"
                    response.get_attr.attributes.firmware_version = "example-1.0.0"
                encoded = response.SerializeToString(deterministic=True)
            else:
                rpc_request = json.loads(payload)
                if int(rpc_request["reqId"]) == 42:
                    return
                success = rpc_request.get("method") == "getAttr"
                response_value = {
                    "reqId": int(rpc_request["reqId"]),
                    "code": 0 if success else 3,
                }
                if success:
                    response_value["data"] = {
                        "deviceId": "DEMO001",
                        "fwVer": "example-1.0.0",
                    }
                encoded = json_bytes(response_value)
            info = self.client.publish(
                f"{self.base_topic}/rpc/resp", encoded, qos=1, retain=False
            )
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"RPC response publish rc={info.rc}")
        except Exception as exc:
            print(f"FAIL mock-rpc reason={exc}", flush=True)

    def wait_until(self, predicate: Any, description: str) -> None:
        deadline = time.monotonic() + self.timeout
        with self.condition:
            while not predicate():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"等待{description}超时")
                self.condition.wait(timeout=remaining)

    def publish_fixtures(self) -> tuple[int, int]:
        fixtures = protobuf_fixtures() if self.profile == "protobuf" else json_fixtures()
        total_bytes = 0
        for suffix, payload in fixtures:
            topic = f"{self.base_topic}/{suffix}"
            result = self.client.publish(topic, payload, qos=1, retain=False)
            result.wait_for_publish(timeout=5)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"发布失败 topic={topic} rc={result.rc}")
            total_bytes += len(payload)
            print(
                f"PUBLISHED profile={self.profile} topic={suffix} bytes={len(payload)}",
                flush=True,
            )
        return len(fixtures), total_bytes

    def run(self) -> int:
        common.connect_with_retry(self.client, self.host, self.port, self.timeout)
        self.client.loop_start()
        try:
            if not self.subscribed.wait(timeout=self.timeout):
                raise TimeoutError("发布器订阅控制 Topic 超时")
            self.wait_until(
                lambda: self.required <= self.ready,
                f"订阅端就绪，缺少 {sorted(self.required - self.ready)}",
            )
            print(f"READY consumers={','.join(sorted(self.ready))}", flush=True)
            count, total_bytes = self.publish_fixtures()
            self.wait_until(
                lambda: self.required <= self.results.keys(),
                f"订阅端结果，缺少 {sorted(self.required - self.results.keys())}",
            )
            failures = {
                name: value for name, value in self.results.items() if value != "PASS"
            }
            if failures:
                print(f"FAIL profile={self.profile} results={failures}", flush=True)
                return 1
            print(
                f"PASS profile={self.profile} consumers={','.join(sorted(self.required))} "
                f"messages={count} bytes={total_bytes}",
                flush=True,
            )
            return 0
        except Exception as exc:
            print(f"FAIL publisher profile={self.profile} reason={exc}", flush=True)
            return 1
        finally:
            self.client.disconnect()
            self.client.loop_stop()


if __name__ == "__main__":
    raise SystemExit(Publisher().run())
