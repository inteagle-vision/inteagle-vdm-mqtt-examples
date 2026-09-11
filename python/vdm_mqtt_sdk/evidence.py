"""MQTT 告警抓拍图像 USTAR 分块的有界落盘与完整性校验。"""

from __future__ import annotations

import hashlib
import json
import os
import tarfile
import threading
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from .codec import EvidencePackageChunk


_HEADER_LENGTH = 76
_CHUNK_BYTES = 128 * 1024
_MAX_PACKAGE_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class CompletedAlarmSnapshotPackage:
    event_id: int
    package_sha256: str
    package_path: Path


@dataclass
class _PackageState:
    package_length: int
    package_sha256: bytes
    chunk_count: int
    received: set[int] = field(default_factory=set)


class AlarmSnapshotPackageAssembler:
    """Strict, bounded, disk-first alarm snapshot USTAR package assembler.

    Duplicate chunks are idempotent and conflicting bytes are rejected. The
    package is returned only after its length, SHA-256 and safe USTAR member
    layout have been verified. Production services should additionally persist
    the received bitmap when receiver-process restart recovery is required.
    """

    def __init__(self, output_dir: str | Path, *, max_pending_events: int = 8) -> None:
        if max_pending_events < 1:
            raise ValueError("max_pending_events 必须大于 0")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.max_pending_events = max_pending_events
        self._events: dict[int, _PackageState] = {}
        self._lock = threading.RLock()

    def accept(self, chunk: EvidencePackageChunk) -> CompletedAlarmSnapshotPackage | None:
        self._validate_chunk(chunk)
        with self._lock:
            return self._accept(chunk)

    def _accept(self, chunk: EvidencePackageChunk) -> CompletedAlarmSnapshotPackage | None:
        event_dir = self.output_dir / str(chunk.event_id)
        event_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        hash_hex = chunk.package_sha256.hex()
        part_path = event_dir / f"{hash_hex}.tar.part"
        final_path = event_dir / f"{hash_hex}.tar"

        if final_path.is_file():
            self._validate_complete_package(
                final_path, chunk.package_length, chunk.package_sha256
            )
            with final_path.open("rb") as output:
                output.seek(chunk.chunk_offset)
                if output.read(len(chunk.chunk)) != chunk.chunk:
                    raise ValueError("已完成抓拍图像包的重复分块冲突")
            self._write_receipt(
                event_dir,
                {
                    "eventId": str(chunk.event_id),
                    "kind": "SNAPSHOT",
                    "packageSha256": hash_hex,
                    "package": final_path.name,
                },
            )
            self._fsync_directory(event_dir)
            self._events.pop(chunk.event_id, None)
            return CompletedAlarmSnapshotPackage(chunk.event_id, hash_hex, final_path)

        state = self._events.get(chunk.event_id)
        if state is None:
            if len(self._events) >= self.max_pending_events:
                raise RuntimeError("待接收告警抓拍图像事件数量超过有界限制")
            state = _PackageState(
                chunk.package_length,
                chunk.package_sha256,
                chunk.chunk_count,
            )
            self._events[chunk.event_id] = state
        if (
            state.package_length != chunk.package_length
            or state.package_sha256 != chunk.package_sha256
            or state.chunk_count != chunk.chunk_count
        ):
            raise ValueError("同一 eventId 的抓拍图像包标识冲突")

        if chunk.chunk_index in state.received:
            with part_path.open("rb") as output:
                output.seek(chunk.chunk_offset)
                if output.read(len(chunk.chunk)) != chunk.chunk:
                    raise ValueError("重复分块的字节内容冲突")
        else:
            mode = "r+b" if part_path.exists() else "w+b"
            with part_path.open(mode) as output:
                output.seek(chunk.chunk_offset)
                output.write(chunk.chunk)
                output.flush()
                os.fsync(output.fileno())
            state.received.add(chunk.chunk_index)

        if len(state.received) != state.chunk_count:
            return None

        try:
            self._validate_complete_package(
                part_path, state.package_length, state.package_sha256
            )
        except Exception:
            self._events.pop(chunk.event_id, None)
            part_path.unlink(missing_ok=True)
            raise

        os.replace(part_path, final_path)
        self._write_receipt(
            event_dir,
            {
                "eventId": str(chunk.event_id),
                "kind": "SNAPSHOT",
                "packageSha256": hash_hex,
                "package": final_path.name,
            },
        )
        self._fsync_directory(event_dir)
        self._events.pop(chunk.event_id, None)
        return CompletedAlarmSnapshotPackage(chunk.event_id, hash_hex, final_path)

    @staticmethod
    def _validate_chunk(chunk: EvidencePackageChunk) -> None:
        if not isinstance(chunk, EvidencePackageChunk):
            raise TypeError("抓拍图像分块类型错误")
        if (
            chunk.message_type != 2
            or chunk.header_length != _HEADER_LENGTH
            or chunk.package_format != 1
            or chunk.evidence_kind != 1
        ):
            raise ValueError("抓拍图像分块格式不受支持")
        if (
            chunk.event_id <= 0
            or chunk.package_length < 1
            or chunk.package_length > _MAX_PACKAGE_BYTES
            or len(chunk.package_sha256) != 32
        ):
            raise ValueError("抓拍图像包身份或长度非法")
        expected_count = (chunk.package_length + _CHUNK_BYTES - 1) // _CHUNK_BYTES
        expected_offset = chunk.chunk_index * _CHUNK_BYTES
        if (
            chunk.chunk_count != expected_count
            or chunk.chunk_index < 0
            or chunk.chunk_index >= chunk.chunk_count
            or chunk.chunk_offset != expected_offset
            or expected_offset >= chunk.package_length
        ):
            raise ValueError("抓拍图像包分块范围非法")
        expected_length = min(_CHUNK_BYTES, chunk.package_length - expected_offset)
        if len(chunk.chunk) != expected_length:
            raise ValueError("抓拍图像包分块长度非法")
        if chunk.chunk_index == 0 and (
            len(chunk.chunk) < 262 or chunk.chunk[257:262] != b"ustar"
        ):
            raise ValueError("抓拍图像包首块缺少 USTAR 标识")

    @classmethod
    def _validate_complete_package(
        cls, path: Path, expected_length: int, expected_sha256: bytes
    ) -> None:
        if path.stat().st_size != expected_length:
            raise ValueError("重组后的抓拍图像包长度不匹配")
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for block in iter(lambda: source.read(64 * 1024), b""):
                digest.update(block)
        if digest.digest() != expected_sha256:
            raise ValueError("重组后的抓拍图像包 SHA-256 不匹配")
        cls._validate_ustar(path)

    @staticmethod
    def _validate_ustar(path: Path) -> None:
        with path.open("rb") as source:
            header = source.read(262)
        if len(header) < 262 or header[257:262] != b"ustar":
            raise ValueError("抓拍图像包缺少 USTAR 标识")
        with tarfile.open(path, mode="r:") as archive:
            members = archive.getmembers()
            if not members or members[0].name != "manifest.json":
                raise ValueError("抓拍图像 USTAR 的第一项必须是 manifest.json")
            names: set[str] = set()
            for member in members:
                name = PurePosixPath(member.name)
                if (
                    member.name in names
                    or "\\" in member.name
                    or name.is_absolute()
                    or any(part in {"", ".", ".."} for part in name.parts)
                    or not member.isfile()
                ):
                    raise ValueError("抓拍图像 USTAR 包含不安全、重复或非普通文件成员")
                names.add(member.name)

    @staticmethod
    def _write_receipt(event_dir: Path, receipt: dict[str, object]) -> None:
        receipt_path = event_dir / "receipt.json"
        temporary_path = event_dir / "receipt.json.tmp"
        payload = json.dumps(receipt, ensure_ascii=False, separators=(",", ":")).encode()
        with temporary_path.open("wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, receipt_path)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


# 兼容最初示例名称；新接入统一使用 AlarmSnapshot*。
CompletedEvidencePackage = CompletedAlarmSnapshotPackage
EvidencePackageAssembler = AlarmSnapshotPackageAssembler
