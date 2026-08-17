"""StdMqtt 告警证据 JPEG 分块的有界落盘与完整性校验。"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from .codec import EvidenceImageChunk


@dataclass(frozen=True)
class CompletedEvidenceSet:
    event_id: int
    manifest_sha256: str
    image_paths: tuple[Path, ...]


@dataclass
class _ImageState:
    jpeg_length: int
    jpeg_sha256: bytes
    chunk_count: int
    received: set[int] = field(default_factory=set)


@dataclass
class _EvidenceState:
    manifest_sha256: bytes
    image_count: int
    images: dict[int, _ImageState] = field(default_factory=dict)


class EvidenceImageAssembler:
    """严格、磁盘优先的证据分块重组器。

    `accept()` 对重复分块幂等；相同身份但内容不同会拒绝。只有全部图片长度、JPEG
    SOI/EOI 和 SHA-256 均验证通过后才返回 `CompletedEvidenceSet`。生产系统还应把
    received bitmap 持久化，以便接收服务自身重启后继续去重。
    """

    def __init__(self, output_dir: str | Path, *, max_pending_events: int = 8) -> None:
        if max_pending_events < 1:
            raise ValueError("max_pending_events 必须大于 0")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_pending_events = max_pending_events
        self._events: dict[int, _EvidenceState] = {}

    def accept(self, chunk: EvidenceImageChunk) -> CompletedEvidenceSet | None:
        state = self._events.get(chunk.event_id)
        if state is None:
            if len(self._events) >= self.max_pending_events:
                raise RuntimeError("待接收证据事件数量超过有界限制")
            state = _EvidenceState(chunk.manifest_sha256, chunk.image_count)
            self._events[chunk.event_id] = state
        if (
            state.manifest_sha256 != chunk.manifest_sha256
            or state.image_count != chunk.image_count
        ):
            raise ValueError("同一 eventId 的 manifestSha256/imageCount 冲突")

        image = state.images.get(chunk.image_index)
        if image is None:
            image = _ImageState(chunk.jpeg_length, chunk.jpeg_sha256, chunk.chunk_count)
            state.images[chunk.image_index] = image
        if (
            image.jpeg_length != chunk.jpeg_length
            or image.jpeg_sha256 != chunk.jpeg_sha256
            or image.chunk_count != chunk.chunk_count
        ):
            raise ValueError("同一证据图片的长度、SHA-256 或 chunkCount 冲突")

        event_dir = self.output_dir / str(chunk.event_id)
        event_dir.mkdir(parents=True, exist_ok=True)
        part_path = event_dir / f"{chunk.image_index:03d}.jpg.part"
        final_path = event_dir / f"{chunk.image_index:03d}.jpg"
        if chunk.chunk_index in image.received:
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
                image.received.add(chunk.chunk_index)

        if len(image.received) == image.chunk_count and not final_path.is_file():
            self._finalize_image(part_path, image)

        if len(state.images) != state.image_count:
            return None
        completed_paths = tuple(event_dir / f"{index:03d}.jpg" for index in range(state.image_count))
        if not all(path.is_file() for path in completed_paths):
            return None

        self._write_receipt(
            event_dir,
            {
                "eventId": str(chunk.event_id),
                "kind": "SNAPSHOT",
                "manifestSha256": chunk.manifest_sha256.hex(),
                "images": [path.name for path in completed_paths],
            },
        )
        self._events.pop(chunk.event_id, None)
        return CompletedEvidenceSet(
            event_id=chunk.event_id,
            manifest_sha256=chunk.manifest_sha256.hex(),
            image_paths=completed_paths,
        )

    @staticmethod
    def _finalize_image(part_path: Path, image: _ImageState) -> None:
        data = part_path.read_bytes()
        if len(data) != image.jpeg_length:
            raise ValueError("重组后的 JPEG 长度不匹配")
        if not data.startswith(b"\xff\xd8") or not data.endswith(b"\xff\xd9"):
            raise ValueError("重组后的 JPEG SOI/EOI 非法")
        if hashlib.sha256(data).digest() != image.jpeg_sha256:
            raise ValueError("重组后的 JPEG SHA-256 不匹配")
        os.replace(part_path, part_path.with_suffix(""))
        EvidenceImageAssembler._fsync_directory(part_path.parent)

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
        EvidenceImageAssembler._fsync_directory(event_dir)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
