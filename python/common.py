"""VDM MQTT Python 示例共享方法。"""

from __future__ import annotations

import os
import time
from typing import Any

import paho.mqtt.client as mqtt


def new_client(client_id: str) -> mqtt.Client:
    return mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )


def reason_code_ok(reason_code: Any) -> bool:
    try:
        return int(reason_code) == 0
    except (TypeError, ValueError):
        return str(reason_code).lower() in {"0", "success", "success: 0"}


def connect_with_retry(client: mqtt.Client, host: str, port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client.connect(host, port, keepalive=20)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
    raise TimeoutError(f"连接 MQTT Broker 超时 {host}:{port}: {last_error}")


def setting(name: str, default: str) -> str:
    return os.getenv(name, default)
