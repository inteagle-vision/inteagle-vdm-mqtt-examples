#!/usr/bin/env python3
"""接收、校验并确认 StdMqtt 告警抓拍证据。"""

from __future__ import annotations

import os
import threading
import time

from vdm_mqtt_sdk import (
    EvidenceImageAssembler,
    EvidenceImageChunk,
    VdmMqttClient,
    VdmMqttClientConfig,
    VdmTopics,
)


def setting(name: str, default: str) -> str:
    return os.getenv(name, default)


assembler = EvidenceImageAssembler(setting("VDM_EVIDENCE_DIR", "./evidence"))
client: VdmMqttClient


def acknowledge(event_id: int, manifest_sha256: str) -> None:
    try:
        response = client.ack_evidence_images(event_id, manifest_sha256)
        print(
            f"ACKED eventId={event_id} manifestSha256={manifest_sha256} "
            f"response={response.as_dict()}",
            flush=True,
        )
    except Exception as error:
        print(f"ACK_FAILED eventId={event_id} error={error}", flush=True)


def on_message(message) -> None:
    if not isinstance(message.value, EvidenceImageChunk):
        return
    completed = assembler.accept(message.value)
    print(
        f"CHUNK eventId={message.value.event_id} image={message.value.image_index + 1}/"
        f"{message.value.image_count} chunk={message.value.chunk_index + 1}/"
        f"{message.value.chunk_count}",
        flush=True,
    )
    if completed is not None:
        print(
            f"VERIFIED eventId={completed.event_id} images={len(completed.image_paths)} "
            f"dir={completed.image_paths[0].parent}",
            flush=True,
        )
        # Paho 的 on_message 在线程内执行；RPC 必须换线程等待响应，避免阻塞网络循环。
        threading.Thread(
            target=acknowledge,
            args=(completed.event_id, completed.manifest_sha256),
            daemon=True,
        ).start()


client = VdmMqttClient(
    VdmMqttClientConfig(
        host=setting("MQTT_HOST", "127.0.0.1"),
        port=int(setting("MQTT_PORT", "1883")),
        topics=VdmTopics.for_device(setting("VDM_DEVICE_ID", "DEMO001")),
        payload_format=setting("VDM_PAYLOAD_FORMAT", "json"),
        username=os.getenv("MQTT_USERNAME"),
        password=os.getenv("MQTT_PASSWORD"),
        qos=1,
    ),
    on_message=on_message,
    on_error=lambda error: print(f"ERROR {error}", flush=True),
)


if __name__ == "__main__":
    client.start()
    print("READY evidence receiver", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        client.stop()
