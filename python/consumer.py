#!/usr/bin/env python3
"""使用 Python SDK 订阅并解析 VDM JSON 或 Protobuf Payload。"""

from __future__ import annotations

import json
import os
import threading
import time

from vdm_mqtt_sdk import (
    DecodedPayload,
    VdmMqttClient,
    VdmMqttClientConfig,
    VdmTopics,
)


REQUIRED_SUFFIXES = {
    "telemetry",
    "attributes",
    "event",
    "3A",
    "evidence",
    "rpc/req",
    "rpc/resp",
    "image",
}


def setting(name: str, default: str) -> str:
    return os.getenv(name, default)


class Consumer:
    def __init__(self) -> None:
        self.profile = setting("VDM_PAYLOAD_FORMAT", "protobuf")
        self.run_id = setting("VDM_RUN_ID", "manual")
        self.name = setting("VDM_CONSUMER_NAME", "python")
        self.timeout = float(setting("VDM_WAIT_TIMEOUT", "45"))
        self.seen: set[str] = set()
        self.rpc_started = False
        self.rpc_passed = False
        self.finished = False
        self.lock = threading.Lock()
        self.client = VdmMqttClient(
            VdmMqttClientConfig(
                host=setting("MQTT_HOST", "127.0.0.1"),
                port=int(setting("MQTT_PORT", "1883")),
                topics=VdmTopics(setting("VDM_BASE_TOPIC", "vdm/DEMO001")),
                payload_format=self.profile,
                connect_timeout=self.timeout,
                client_id=f"vdm-example-{self.name}-{self.run_id}",
            ),
            on_message=self.on_message,
            on_error=self.fail,
        )

    def control_topic(self, kind: str) -> str:
        return f"vdm-example/{self.run_id}/{kind}/{self.name}"

    def publish_control(self, kind: str, value: str) -> None:
        self.client.publish_raw(self.control_topic(kind), value, retain=True)

    def fail(self, reason: Exception | str) -> None:
        with self.lock:
            if self.finished:
                return
            self.finished = True
        print(f"FAIL consumer={self.name} reason={reason}", flush=True)
        try:
            self.publish_control("result", f"FAIL:{reason}")
        except Exception as exc:
            print(f"FAIL consumer={self.name} 无法上报结果: {exc}", flush=True)

    def on_message(self, message: DecodedPayload) -> None:
        if message.suffix not in REQUIRED_SUFFIXES:
            self.fail(f"未支持的 Topic: {message.suffix}")
            return
        print(
            f"DECODED consumer={self.name} profile={self.profile} "
            f"topic={message.suffix} bytes={len(message.raw)} "
            f"data={json.dumps(message.as_dict(), ensure_ascii=False, separators=(',', ':'))}",
            flush=True,
        )
        start_rpc = False
        with self.lock:
            if not self.finished:
                self.seen.add(message.suffix)
                if message.suffix == "telemetry" and not self.rpc_started:
                    self.rpc_started = True
                    start_rpc = True
        if start_rpc:
            threading.Thread(target=self.test_rpc, daemon=True).start()
        self.complete_if_ready()

    def test_rpc(self) -> None:
        try:
            response = self.client.call(
                "getAttr",
                {"keys": ["deviceId", "fwVer"]},
                req_id=11_001,
                timeout=10,
            )
            data = response.as_dict()
            if self.profile == "protobuf":
                device_id = data.get("getAttr", {}).get("attributes", {}).get("deviceId")
            else:
                device_id = data.get("data", {}).get("deviceId")
            if device_id != "DEMO001":
                raise AssertionError(f"RPC getAttr.deviceId 不匹配: {device_id}")
            with self.lock:
                self.rpc_passed = True
            print(
                f"RPC_PASS consumer={self.name} method=getAttr req_id=11001 data={data}",
                flush=True,
            )
            self.complete_if_ready()
        except Exception as exc:
            self.fail(exc)

    def complete_if_ready(self) -> None:
        with self.lock:
            should_pass = (
                not self.finished
                and self.rpc_passed
                and self.seen == REQUIRED_SUFFIXES
            )
            if should_pass:
                self.finished = True
        if should_pass:
            self.publish_control("result", "PASS")
            print(
                f"PASS consumer={self.name} profile={self.profile} "
                f"topics={','.join(sorted(self.seen))} rpc=getAttr",
                flush=True,
            )

    def run(self) -> None:
        self.client.start()
        self.publish_control("ready", "READY")
        print(f"READY consumer={self.name} profile={self.profile}", flush=True)
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    Consumer().run()
