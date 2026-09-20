from __future__ import annotations

import json
import hashlib
import io
import tarfile
import tempfile
import unittest
from dataclasses import replace

import inteagle_vdm_mqtt_v1_pb2 as pb

from vdm_mqtt_sdk import (
    AlarmSnapshotPackageAssembler,
    EvidencePackageChunk,
    ImageFrame,
    PayloadFormat,
    VdmCodec,
    VdmTopics,
)
from vdm_mqtt_sdk.codec import RPC_REQUEST_TYPES


class VdmCodecTests(unittest.TestCase):
    def test_public_seconds_timestamps_use_ts_and_preserve_wire_tags(self) -> None:
        for message, number in [(pb.EnvironmentTelemetry, 1), (pb.DeviceStatusTelemetry, 1),
                                (pb.Event, 2), (pb.Alarm, 8), (pb.AlarmEvidence, 4)]:
            with self.subTest(message=message.__name__):
                field = message.DESCRIPTOR.fields_by_name["ts"]
                self.assertEqual(field.number, number)
                self.assertEqual(field.type, field.TYPE_UINT64)
                self.assertEqual(message.FromString(message(ts=1734567890).SerializeToString()).ts, 1734567890)
        telemetry = pb.Telemetry(schema_version=1)
        topics = VdmTopics.for_device("DEMO001")
        for field in ["environment", "device_status"]:
            telemetry.ClearField("environment")
            telemetry.ClearField("device_status")
            getattr(telemetry, field).ts = 1734567890
            decoded = VdmCodec("protobuf").decode(
                topics.topic("telemetry"), topics, telemetry.SerializeToString()
            ).as_dict()
            self.assertEqual(decoded["environment" if field == "environment" else "deviceStatus"]["ts"], "1734567890")

    def test_displacement_arrays_use_public_dx_dy_dz_names(self) -> None:
        telemetry = pb.Telemetry(schema_version=1)
        target = telemetry.displacement.targets.add(target_id="T01")
        target.dx.extend([0.125])
        target.dy.extend([-0.5])
        target.dz.extend([0.75])
        topics = VdmTopics.for_device("DEMO001")
        decoded = VdmCodec("protobuf").decode(
            "vdm/DEMO001/telemetry", topics, telemetry.SerializeToString()
        ).as_dict()
        self.assertEqual(decoded["displacement"]["targets"][0], {
            "targetId": "T01", "dx": [0.125], "dy": [-0.5], "dz": [0.75],
        })
        for name, number in [("dx", 3), ("dy", 4), ("dz", 9)]:
            field = pb.TargetDisplacementSeries.DESCRIPTOR.fields_by_name[name]
            self.assertEqual(field.number, number)
            self.assertEqual(field.type, field.TYPE_FLOAT)
            self.assertTrue(field.GetOptions().packed)

    def test_topics_reject_wildcards_and_map_suffixes(self) -> None:
        topics = VdmTopics.for_device("DEMO001")
        self.assertEqual(topics.rpc_request, "vdm/DEMO001/rpc/req")
        self.assertEqual(topics.suffix("vdm/DEMO001/telemetry"), "telemetry")
        with self.assertRaises(ValueError):
            VdmTopics.for_device("bad/#")

    def test_protobuf_rpc_uses_typed_oneof(self) -> None:
        codec = VdmCodec(PayloadFormat.PROTOBUF)
        payload, expected = codec.encode_rpc_request("getAttr", {"keys": ["deviceId"]}, 7)
        request = pb.RpcRequest.FromString(payload)
        self.assertEqual(request.schema_version, 1)
        self.assertEqual(request.req_id, 7)
        self.assertEqual(request.WhichOneof("request"), "get_attr")
        self.assertEqual(request.get_attr.keys, ["deviceId"])
        self.assertEqual(expected, "get_attr")

    def test_json_rpc_keeps_public_method_name(self) -> None:
        codec = VdmCodec(PayloadFormat.JSON)
        payload, expected = codec.encode_rpc_request("getTargets", {}, 8)
        self.assertEqual(
            json.loads(payload),
            {"reqId": 8, "method": "getTargets", "params": {}},
        )
        self.assertEqual(expected, "get_targets")

    def test_rpc_error_text_is_derived_locally_from_numeric_code(self) -> None:
        json_info = VdmCodec("json").response_info(
            {"reqId": 8, "code": 4, "msg": "private device diagnostic"}
        )
        self.assertEqual(json_info[2], "RPC request rate limited")

        response = pb.RpcResponse(
            schema_version=1,
            req_id=9,
            code=300,
            message="private device diagnostic",
        )
        protobuf_info = VdmCodec("protobuf").response_info(response)
        self.assertEqual(protobuf_info[2], "motor unavailable")

    def test_internal_rpc_name_is_rejected_before_publish(self) -> None:
        with self.assertRaises(ValueError):
            VdmCodec("protobuf").encode_rpc_request("privateDeviceCommand", {}, 9)

    def test_all_public_rpc_methods_build_a_typed_body(self) -> None:
        codec = VdmCodec("protobuf")
        self.assertEqual(len(RPC_REQUEST_TYPES), 33)
        for req_id, (method, (expected, _message_type)) in enumerate(
            RPC_REQUEST_TYPES.items(), start=100
        ):
            with self.subTest(method=method):
                payload, actual = codec.encode_rpc_request(method, {}, req_id)
                request = pb.RpcRequest.FromString(payload)
                self.assertEqual(request.WhichOneof("request"), expected)
                self.assertEqual(actual, expected)

    def test_unavailable_rpc_methods_are_rejected_in_both_formats(self) -> None:
        unavailable = (
            "getStorageInfo", "queryTelemetry", "uploadS3", "setCruisePoint",
            "removeCruisePoint", "startPatrol", "stopPatrol", "getPatrolStatus",
        )
        for payload_format in ("json", "protobuf"):
            codec = VdmCodec(payload_format)
            for method in unavailable:
                with self.subTest(payload_format=payload_format, method=method):
                    with self.assertRaises(ValueError):
                        codec.encode_rpc_request(method, {}, 200)

    def test_protobuf_schema_version_is_checked(self) -> None:
        topics = VdmTopics.for_device("DEMO001")
        valid = pb.Attributes(schema_version=1, device_id="DEMO001").SerializeToString()
        decoded = VdmCodec("protobuf").decode(
            "vdm/DEMO001/attributes",
            topics,
            valid,
        )
        self.assertEqual(decoded.value.device_id, "DEMO001")
        self.assertEqual(decoded.as_dict()["deviceId"], "DEMO001")
        invalid = pb.Attributes(schema_version=2).SerializeToString()
        with self.assertRaises(ValueError):
            VdmCodec("protobuf").decode("vdm/DEMO001/attributes", topics, invalid)

    def test_image_header_remains_binary(self) -> None:
        payload = bytes([1, 8, 0, 1]) + (123).to_bytes(4, "big") + b"\xff\xd8\xff\xd9"
        image = VdmCodec.decode_image(payload)
        self.assertIsInstance(image, ImageFrame)
        self.assertEqual(image.timestamp_s, 123)
        self.assertEqual(image.jpeg, b"\xff\xd8\xff\xd9")

        decoded = VdmCodec("protobuf").decode(
            "vdm/DEMO001/image", VdmTopics.for_device("DEMO001"), payload
        )
        self.assertEqual(
            decoded.as_dict(),
            {
                "version": 1,
                "headerLength": 8,
                "sensorId": 0,
                "imageType": 1,
                "timestampS": 123,
                "jpegBytes": 4,
            },
        )

    @staticmethod
    def _evidence_package(
        padding: int = 0, image_name: str = "frame-000.jpg"
    ) -> bytes:
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            for name, data in (
                ("manifest.json", b'{"eventId":"9001"}'),
                (image_name, b"\xff\xd8snapshot\xff\xd9" + b"x" * padding),
            ):
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mtime = 0
                archive.addfile(info, io.BytesIO(data))
        return output.getvalue()

    @staticmethod
    def _package_chunk(package: bytes, event_id: int, chunk_index: int) -> EvidencePackageChunk:
        chunk_bytes = 128 * 1024
        chunk_count = (len(package) + chunk_bytes - 1) // chunk_bytes
        offset = chunk_index * chunk_bytes
        return EvidencePackageChunk(
            message_type=2,
            header_length=76,
            package_format=1,
            evidence_kind=1,
            event_id=event_id,
            package_length=len(package),
            package_sha256=hashlib.sha256(package).digest(),
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            chunk_offset=offset,
            chunk=package[offset : offset + chunk_bytes],
        )

    def test_evidence_package_chunk_is_strictly_decoded_and_reassembled(self) -> None:
        package = self._evidence_package()
        source = self._package_chunk(package, 9001, 0)
        header = bytearray((2, 76, 1, 1))
        header.extend(source.event_id.to_bytes(8, "big"))
        header.extend(source.package_length.to_bytes(8, "big"))
        header.extend(source.package_sha256)
        header.extend(source.chunk_index.to_bytes(4, "big"))
        header.extend(source.chunk_count.to_bytes(4, "big"))
        header.extend(source.chunk_offset.to_bytes(8, "big"))
        header.extend(len(source.chunk).to_bytes(4, "big"))
        header.extend((0).to_bytes(4, "big"))
        payload = bytes(header) + source.chunk

        chunk = VdmCodec.decode_image(payload)
        self.assertIsInstance(chunk, EvidencePackageChunk)
        self.assertEqual(chunk.event_id, 9001)
        self.assertEqual(chunk.package_sha256, hashlib.sha256(package).digest())
        with tempfile.TemporaryDirectory() as directory:
            completed = AlarmSnapshotPackageAssembler(directory).accept(chunk)
            self.assertIsNotNone(completed)
            self.assertEqual(completed.event_id, 9001)
            self.assertEqual(completed.package_path.read_bytes(), package)

        malformed = bytearray(payload)
        malformed[60:68] = (1).to_bytes(8, "big")
        with self.assertRaises(ValueError):
            VdmCodec.decode_image(bytes(malformed))

    def test_package_chunk_duplicate_is_idempotent_while_pending(self) -> None:
        package = self._evidence_package(140_000)
        with tempfile.TemporaryDirectory() as directory:
            assembler = AlarmSnapshotPackageAssembler(directory)
            first = self._package_chunk(package, 9002, 0)
            self.assertIsNone(assembler.accept(first))
            self.assertIsNone(assembler.accept(first))
            completed = assembler.accept(self._package_chunk(package, 9002, 1))
            self.assertIsNotNone(completed)
            self.assertEqual(completed.package_path.read_bytes(), package)
            repeated = assembler.accept(first)
            self.assertIsNotNone(repeated)
            self.assertEqual(repeated.package_sha256, completed.package_sha256)

    def test_package_assembler_rejects_conflict_unsafe_tar_and_pending_overflow(self) -> None:
        package = self._evidence_package(140_000)
        with tempfile.TemporaryDirectory() as directory:
            assembler = AlarmSnapshotPackageAssembler(directory, max_pending_events=1)
            first = self._package_chunk(package, 9101, 0)
            self.assertIsNone(assembler.accept(first))
            conflict = replace(
                first,
                chunk=bytes((first.chunk[0] ^ 0xFF,)) + first.chunk[1:],
            )
            with self.assertRaises(ValueError):
                assembler.accept(conflict)
            with self.assertRaises(RuntimeError):
                assembler.accept(self._package_chunk(package, 9102, 0))

        unsafe = self._evidence_package(140_000, "../outside.jpg")
        with tempfile.TemporaryDirectory() as directory:
            assembler = AlarmSnapshotPackageAssembler(directory)
            self.assertIsNone(assembler.accept(self._package_chunk(unsafe, 9201, 0)))
            with self.assertRaises(ValueError):
                assembler.accept(self._package_chunk(unsafe, 9201, 1))


if __name__ == "__main__":
    unittest.main()
