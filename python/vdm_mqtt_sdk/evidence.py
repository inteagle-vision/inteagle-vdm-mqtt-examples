"""StdMqtt 告警证据 USTAR 分块的有界落盘与完整性校验。"""

from __future__ import annotations

import hashlib
import json
import os
import tarfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from .codec import EvidencePackageChunk


@dataclass(frozen=True)
class CompletedEvidencePackage:
    event_id: int
    package_sha256: str
    package_path: Path


@dataclass
class _PackageState:
    package_length: int
    package_sha256: bytes
    chunk_count: int
    received: set[int] = field(default_factory=set)


class EvidencePackageAssembler:
    """Strict, disk-first USTAR package assembler.

    Duplicate chunks are idempotent and conflicting bytes are rejected. The
    package is returned only after its length, SHA-256 and safe USTAR member
    layout have been verified. Production services should additionally persist
    the received bitmap when receiver-process restart recovery is required.
    """

    def __init__(self, output_dir: str | Path, *, max_pending_events: int = 8) -> None:
        if max_pending_events < 1:
            raise ValueError("max_pending_events 必须大于 0")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_pending_events = max_pending_events
        self._events: dict[int, _PackageState] = {}

    def accept(self, chunk: EvidencePackageChunk) -> CompletedEvidencePackage | None:
        state = self._events.get(chunk.event_id)
        if state is None:
            if len(self._events) >= self.max_pending_events:
                raise RuntimeError("待接收证据事件数量超过有界限制")
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
            raise ValueError("同一 eventId 的证据包标识冲突")

        event_dir = self.output_dir / str(chunk.event_id)
        event_dir.mkdir(parents=True, exist_ok=True)
        hash_hex = chunk.package_sha256.hex()
        part_path = event_dir / f"{hash_hex}.tar.part"
        final_path = event_dir / f"{hash_hex}.tar"

        if chunk.chunk_index in state.received:
            source_path = final_path if final_path.is_file() else part_path
            with source_path.open("rb") as output:
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

        data = part_path.read_bytes()
        if len(data) != state.package_length:
            raise ValueError("重组后的证据包长度不匹配")
        if hashlib.sha256(data).digest() != state.package_sha256:
            raise ValueError("重组后的证据包 SHA-256 不匹配")
        self._validate_ustar(part_path)

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
        return CompletedEvidencePackage(chunk.event_id, hash_hex, final_path)

    @staticmethod
    def _validate_ustar(path: Path) -> None:
        with tarfile.open(path, mode="r:") as archive:
            members = archive.getmembers()
            if not members or members[0].name != "manifest.json":
                raise ValueError("证据 USTAR 的第一项必须是 manifest.json")
            for member in members:
                name = PurePosixPath(member.name)
                if name.is_absolute() or ".." in name.parts or not member.isfile():
                    raise ValueError("证据 USTAR 包含不安全或非普通文件成员")

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
