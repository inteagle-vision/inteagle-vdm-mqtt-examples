"""带 Topic 订阅与 RPC 关联的 VDM MQTT 云端客户端。"""

from __future__ import annotations

import itertools
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

import paho.mqtt.client as mqtt

from .codec import DecodedPayload, PayloadFormat, VdmCodec, VdmTopics


MessageHandler = Callable[[DecodedPayload], None]
ErrorHandler = Callable[[Exception], None]


def _reason_code_ok(reason_code: Any) -> bool:
    try:
        return int(reason_code) == 0
    except (TypeError, ValueError):
        return str(reason_code).lower() in {"0", "success", "success: 0"}


class RpcError(RuntimeError):
    def __init__(self, req_id: int, code: int, message: str) -> None:
        super().__init__(f"RPC req_id={req_id} code={code}: {message}")
        self.req_id = req_id
        self.code = code
        self.message = message


@dataclass(frozen=True)
class VdmMqttClientConfig:
    host: str
    port: int
    topics: VdmTopics
    payload_format: PayloadFormat | str
    username: str | None = None
    password: str | None = None
    qos: int = 1
    keepalive: int = 30
    connect_timeout: float = 10.0
    client_id: str = field(
        default_factory=lambda: f"vdm-sdk-python-{uuid.uuid4().hex[:12]}"
    )
    subscription_suffixes: tuple[str, ...] | None = None
    persistent_session: bool = False
    manual_ack: bool = False

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("MQTT host 不能为空")
        if not 1 <= self.port <= 65535:
            raise ValueError("MQTT port 超出范围")
        if self.qos not in {0, 1, 2}:
            raise ValueError("MQTT qos 必须是 0、1 或 2")
        object.__setattr__(self, "payload_format", PayloadFormat.parse(self.payload_format))
        if self.subscription_suffixes is not None:
            supported = {"telemetry", "attributes", "event", "3A", "image", "rpc/req", "rpc/resp"}
            if not self.subscription_suffixes or any(
                suffix not in supported for suffix in self.subscription_suffixes
            ):
                raise ValueError("subscription_suffixes 必须包含有效的 VDM Topic 后缀")


@dataclass
class _PendingRpc:
    expected_field: str
    event: threading.Event = field(default_factory=threading.Event)
    result: DecodedPayload | None = None
    error: Exception | None = None


class VdmMqttClient:
    """客户云侧 VDM MQTT 客户端。

    客户端只订阅一个设备的标准 Topic，自动按配置格式解码，并用 req_id 关联并发
    RPC。不同设备或不同云连接应创建独立实例，不要跨连接共享 pending 表。
    """

    def __init__(
        self,
        config: VdmMqttClientConfig,
        on_message: MessageHandler | None = None,
        on_error: ErrorHandler | None = None,
        on_raw_message: Callable[[str, bytes], None] | None = None,
    ) -> None:
        self.config = config
        self.codec = VdmCodec(config.payload_format)
        self.on_message = on_message
        self.on_error = on_error
        self.on_raw_message = on_raw_message
        self._connected = threading.Event()
        self._subscribed = threading.Event()
        self._subscription_error: Exception | None = None
        self._stopped = threading.Event()
        self._pending: dict[int, _PendingRpc] = {}
        self._pending_lock = threading.Lock()
        self._req_ids = itertools.count(secrets.randbelow((1 << 31) - 1) + 1)
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=config.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=not config.persistent_session,
            manual_ack=config.manual_ack,
        )
        if config.username is not None:
            self._client.username_pw_set(config.username, config.password)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_subscribe = self._on_subscribe
        self._client.on_message = self._on_message

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    def start_background(self) -> None:
        """Start Paho's reconnecting loop without blocking HTTP startup."""
        self._stopped.clear()
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)
        self._client.connect_async(self.config.host, self.config.port, self.config.keepalive)
        self._client.loop_start()

    def reconnect_background(self) -> None:
        """Restart after explicit disconnect; invoke outside the MQTT callback."""
        self._client.loop_stop()
        if not self._stopped.is_set():
            self.start_background()

    def start(self) -> None:
        deadline = time.monotonic() + self.config.connect_timeout
        last_error: OSError | None = None
        while time.monotonic() < deadline:
            try:
                self._client.connect(
                    self.config.host,
                    self.config.port,
                    keepalive=self.config.keepalive,
                )
                break
            except OSError as exc:
                last_error = exc
                time.sleep(0.2)
        else:
            raise TimeoutError(
                f"连接 MQTT Broker 超时 {self.config.host}:{self.config.port}: {last_error}"
            )
        self._client.loop_start()
        if not self._subscribed.wait(self.config.connect_timeout):
            self.stop()
            raise TimeoutError(f"订阅 VDM Topic 超时: {self.config.topics.wildcard}")
        if self._subscription_error is not None:
            error = self._subscription_error
            self.stop()
            raise error

    def stop(self) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        with self._pending_lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for call in pending:
            call.error = ConnectionError("VDM MQTT client stopped")
            call.event.set()
        try:
            self._client.disconnect()
        finally:
            self._client.loop_stop()

    def __enter__(self) -> "VdmMqttClient":
        self.start()
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.stop()

    def publish_raw(self, topic: str, payload: bytes | str, *, retain: bool = False, timeout: float = 5.0) -> None:
        info = self._client.publish(topic, payload=payload, qos=self.config.qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish 失败 rc={info.rc}")
        info.wait_for_publish(timeout=timeout)
        if not info.is_published():
            raise TimeoutError(f"MQTT publish ACK 超时: {topic}")

    def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float = 10.0,
        req_id: int | None = None,
        allow_error: bool = False,
    ) -> DecodedPayload:
        if timeout <= 0:
            raise ValueError("RPC timeout must be positive")
        deadline = time.monotonic() + timeout
        if req_id is None:
            req_id = (next(self._req_ids) - 1) % ((1 << 31) - 1) + 1
        payload, expected_field = self.codec.encode_rpc_request(method, params, req_id)
        pending = _PendingRpc(expected_field=expected_field)
        with self._pending_lock:
            if req_id in self._pending:
                raise ValueError(f"req_id 已在当前连接等待响应: {req_id}")
            self._pending[req_id] = pending
        try:
            self.publish_raw(self.config.topics.rpc_request, payload, timeout=max(0, deadline-time.monotonic()))
            if not pending.event.wait(max(0, deadline-time.monotonic())):
                raise TimeoutError(f"等待 RPC 响应超时: method={method}, req_id={req_id}")
            if pending.error is not None:
                raise pending.error
            assert pending.result is not None
            response_req_id, code, message, response_field = self.codec.response_info(
                pending.result.value
            )
            if response_req_id != req_id:
                raise RuntimeError(
                    f"RPC req_id 不匹配: request={req_id}, response={response_req_id}"
                )
            if code != 0 and not allow_error:
                raise RpcError(req_id, code, message)
            if (
                code == 0
                and self.config.payload_format is PayloadFormat.PROTOBUF
                and response_field != expected_field
            ):
                raise RuntimeError(
                    f"RPC response oneof 不匹配: expected={expected_field}, actual={response_field}"
                )
            return pending.result
        finally:
            with self._pending_lock:
                if self._pending.get(req_id) is pending:
                    self._pending.pop(req_id, None)

    def get_evidence_status(self, event_id: int, *, timeout: float = 10.0) -> DecodedPayload:
        if event_id <= 0:
            raise ValueError("event_id 必须是非零整数")
        kind = (
            "EVIDENCE_KIND_SNAPSHOT"
            if self.config.payload_format is PayloadFormat.PROTOBUF
            else "SNAPSHOT"
        )
        return self.call(
            "getEvidenceStatus",
            {"eventId": str(event_id), "kind": kind},
            timeout=timeout,
        )

    def retry_evidence(self, event_id: int, *, timeout: float = 10.0) -> DecodedPayload:
        if event_id <= 0:
            raise ValueError("event_id 必须是非零整数")
        kind = (
            "EVIDENCE_KIND_SNAPSHOT"
            if self.config.payload_format is PayloadFormat.PROTOBUF
            else "SNAPSHOT"
        )
        return self.call(
            "retryEvidence",
            {"eventId": str(event_id), "kind": kind},
            timeout=timeout,
        )

    def ack_evidence_package(
        self,
        event_id: int,
        package_sha256: str,
        *,
        timeout: float = 10.0,
    ) -> DecodedPayload:
        if event_id <= 0:
            raise ValueError("event_id 必须是非零整数")
        if len(package_sha256) != 64 or any(
            value not in "0123456789abcdef" for value in package_sha256
        ):
            raise ValueError("package_sha256 必须是 64 个小写十六进制字符")
        kind = (
            "EVIDENCE_KIND_SNAPSHOT"
            if self.config.payload_format is PayloadFormat.PROTOBUF
            else "SNAPSHOT"
        )
        return self.call(
            "ackEvidencePackage",
            {
                "eventId": str(event_id),
                "kind": kind,
                "packageSha256": package_sha256,
            },
            timeout=timeout,
        )

    def _on_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        _flags: Any,
        reason_code: Any,
        _properties: Any = None,
    ) -> None:
        if not _reason_code_ok(reason_code):
            self._report_error(ConnectionError(f"MQTT 连接被拒绝: {reason_code}"))
            return
        self._connected.set()
        self._subscription_error = None
        if self.config.subscription_suffixes is None:
            subscriptions = [(self.config.topics.wildcard, self.config.qos)]
        else:
            subscriptions = [
                (self.config.topics.topic(suffix), self.config.qos)
                for suffix in self.config.subscription_suffixes
            ]
        result, _mid = client.subscribe(subscriptions)
        if result != mqtt.MQTT_ERR_SUCCESS:
            self._report_error(RuntimeError(f"MQTT subscribe 失败 rc={result}"))

    def _on_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _disconnect_flags: Any,
        reason_code: Any,
        _properties: Any = None,
    ) -> None:
        self._connected.clear()
        self._subscribed.clear()
        if not self._stopped.is_set() and not _reason_code_ok(reason_code):
            error = ConnectionError(f"MQTT 连接断开: {reason_code}")
            with self._pending_lock:
                pending = list(self._pending.values())
                self._pending.clear()
            for call in pending:
                call.error = error
                call.event.set()
            self._report_error(error)

    def _on_subscribe(self, _client: mqtt.Client, _userdata: Any, _mid: int, *extra: Any) -> None:
        reasons = extra[0] if extra else []
        if any(getattr(code, "is_failure", False) or (isinstance(code, int) and code >= 128) for code in reasons):
            self._subscription_error = PermissionError("MQTT Broker 拒绝订阅，请检查 Topic 权限")
            self._report_error(self._subscription_error)
        self._subscribed.set()

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            suffix = self.config.topics.suffix(msg.topic)
            if suffix != "rpc/resp" and self.on_raw_message is not None:
                # Callback must COMMIT the raw inbox before returning. Do not decode,
                # perform application work or wait for RPC from the network loop.
                self.on_raw_message(msg.topic, bytes(msg.payload))
                if self.config.manual_ack and msg.qos:
                    _client.ack(msg.mid, msg.qos)
                return
            decoded = self.codec.decode(
                msg.topic,
                self.config.topics,
                bytes(msg.payload),
            )
            if decoded.suffix == "rpc/resp":
                req_id, _code, _message, _field = self.codec.response_info(decoded.value)
                if req_id is not None:
                    with self._pending_lock:
                        pending = self._pending.get(req_id)
                    if pending is not None:
                        pending.result = decoded
                        pending.event.set()
            if self.on_message is not None:
                self.on_message(decoded)
            if self.config.manual_ack and msg.qos:
                _client.ack(msg.mid, msg.qos)
        except Exception as exc:
            if self.config.manual_ack and msg.qos:
                # No PUBACK on failure. Service supervisor reconnects this stable
                # persistent session to let the broker redeliver the message.
                _client.disconnect()
            self._report_error(exc)

    def _report_error(self, error: Exception) -> None:
        if self.on_error is not None:
            self.on_error(error)
