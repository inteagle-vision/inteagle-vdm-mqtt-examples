"""Validated service configuration. Secrets are resolved only from environment."""
from dataclasses import dataclass, field
import ipaddress
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_ENV = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")

@dataclass
class Settings:
    devices: list[dict]
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 8080
    api_token: str | None = field(default=None, repr=False)
    webhook_url: str | None = field(default=None, repr=False)
    webhook_token: str | None = field(default=None, repr=False)
    rpc_timeout_ms: int = 10000
    notification_max_age_seconds: int = 300
    max_inbox_rows: int = 10000
    retention_days: int = 7
    max_evidence_bytes: int = 268435456

    @classmethod
    def from_dict(cls, config, *, data_dir=None, environ=None):
        env = os.environ if environ is None else environ
        if not isinstance(config, dict) or not isinstance(config.get("connections"), list):
            raise ValueError("connections must be an array")
        host = env.get("VDM_HTTP_HOST", "127.0.0.1")
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = host == "localhost"
        token = env.get("VDM_API_TOKEN") or None
        if not loopback and not token:
            raise ValueError("non-loopback binding requires VDM_API_TOKEN")
        devices, connections_seen = [], set()
        for connection in config["connections"]:
            cid = connection.get("id", "")
            if not isinstance(cid, str) or not _ID.fullmatch(cid) or cid in connections_seen:
                raise ValueError("invalid/duplicate connection id")
            connections_seen.add(cid)
            if "username" in connection or "password" in connection:
                raise ValueError("MQTT credentials must use environment references")
            if not isinstance(connection.get("host"), str) or not connection["host"].strip():
                raise ValueError("connection host is required")
            port = connection.get("port", 1883)
            if type(port) is not int or not 1 <= port <= 65535:
                raise ValueError("invalid MQTT port")
            credentials = {}
            for name in ("username", "password"):
                ref = connection.get(name + "Env")
                if ref is not None and (not isinstance(ref, str) or not _ENV.fullmatch(ref)):
                    raise ValueError("invalid credential environment reference")
                credentials[name] = env.get(ref) if ref else None
            seen = set()
            if not isinstance(connection.get("devices"), list):
                raise ValueError("devices must be an array")
            for device in connection["devices"]:
                did = device.get("id", "")
                if not isinstance(did, str) or not _ID.fullmatch(did) or did in seen:
                    raise ValueError("invalid/duplicate device id")
                seen.add(did)
                fmt = device.get("format", "json")
                caps = device.get("capabilities", [])
                if fmt not in ("json", "protobuf") or not isinstance(caps, list) or not all(isinstance(c, str) for c in caps):
                    raise ValueError("invalid device format/capabilities")
                devices.append(dict(connectionId=cid, deviceId=did, format=fmt,
                                    capabilities=caps, host=connection["host"], port=port, **credentials))
        values = {}
        for key, attr, default, low, high in [
            ("rpcTimeoutMs", "rpc_timeout_ms", 10000, 100, 60000),
            ("notificationMaxAgeSeconds", "notification_max_age_seconds", 300, 1, 86400*365),
            ("maxInboxRows", "max_inbox_rows", 10000, 1, 1000000),
            ("retentionDays", "retention_days", 7, 1, 3650),
            ("maxEvidenceBytes", "max_evidence_bytes", 268435456, 1, 2**50),
        ]:
            value = config.get(key, default)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{key} outside {low}..{high}")
            values[attr] = value
        http_port = int(env.get("VDM_HTTP_PORT", "8080"))
        if not 1 <= http_port <= 65535:
            raise ValueError("invalid HTTP port")
        webhook = env.get("VDM_WEBHOOK_URL") or None
        if webhook:
            parsed = urlsplit(webhook)
            if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
                raise ValueError("webhook must be an HTTP(S) URL without inline credentials")
        return cls(devices, Path(data_dir or env.get("VDM_DATA_DIR", "./data")), host, http_port,
                   token, webhook, env.get("VDM_WEBHOOK_TOKEN"), **values)

    @classmethod
    def load(cls):
        default = Path(__file__).resolve().parents[2] / "contracts/config.example.json"
        return cls.from_dict(json.loads(Path(os.environ.get("VDM_SERVICE_CONFIG", default)).read_text()))
