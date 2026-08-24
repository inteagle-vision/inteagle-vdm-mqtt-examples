from __future__ import annotations

import json
import hashlib
import tempfile
import unittest

import inteagle_vdm_mqtt_v1_pb2 as pb

from vdm_mqtt_sdk import (
    EvidenceImageAssembler,
    EvidenceImageChunk,
    ImageFrame,
    PayloadFormat,
    VdmCodec,
    VdmTopics,
)
from vdm_mqtt_sdk.codec import RPC_REQUEST_TYPES


class VdmCodecTests(unittest.TestCase):
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
        self.assertEqual(len(RPC_REQUEST_TYPES), 29)
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

    def test_evidence_image_chunk_is_strictly_decoded_and_reassembled(self) -> None:
        jpeg = b"\xff\xd8evidence\xff\xd9"
        manifest_sha256 = hashlib.sha256(b"manifest").digest()
        header = bytearray((2, 112, 1, 1))
        header.extend((1_721_805_600_000).to_bytes(8, "big"))
        header.extend((9001).to_bytes(8, "big"))
        header.extend((0).to_bytes(2, "big"))
        header.extend((1).to_bytes(2, "big"))
        header.extend((-1000).to_bytes(4, "big", signed=True))
        header.extend(len(jpeg).to_bytes(4, "big"))
        header.extend(hashlib.sha256(jpeg).digest())
        header.extend(manifest_sha256)
        header.extend((0).to_bytes(2, "big"))
        header.extend((1).to_bytes(2, "big"))
        header.extend((0).to_bytes(4, "big"))
        header.extend(len(jpeg).to_bytes(4, "big"))
        header.extend((0).to_bytes(4, "big"))
        payload = bytes(header) + jpeg

        chunk = VdmCodec.decode_image(payload)
        self.assertIsInstance(chunk, EvidenceImageChunk)
        self.assertEqual(chunk.event_id, 9001)
        self.assertEqual(chunk.actual_offset_ms, -1000)
        self.assertEqual(chunk.manifest_sha256, manifest_sha256)
        with tempfile.TemporaryDirectory() as directory:
            completed = EvidenceImageAssembler(directory).accept(chunk)
            self.assertIsNotNone(completed)
            self.assertEqual(completed.event_id, 9001)
            self.assertEqual(completed.image_paths[0].read_bytes(), jpeg)

        malformed = bytearray(payload)
        malformed[100:104] = (1).to_bytes(4, "big")
        with self.assertRaises(ValueError):
            VdmCodec.decode_image(bytes(malformed))

    def test_completed_image_chunk_duplicate_is_idempotent_while_group_is_pending(self) -> None:
        jpeg = b"\xff\xd8one\xff\xd9"
        manifest_sha256 = hashlib.sha256(b"two-images").digest()

        def make_chunk(image_index: int) -> EvidenceImageChunk:
            return EvidenceImageChunk(
                message_type=2,
                header_length=112,
                camera_id=0,
                trigger_type=1,
                captured_at_ms=1_721_805_600_000 + image_index,
                event_id=9002,
                image_index=image_index,
                image_count=2,
                actual_offset_ms=image_index * 100,
                jpeg_length=len(jpeg),
                jpeg_sha256=hashlib.sha256(jpeg).digest(),
                manifest_sha256=manifest_sha256,
                chunk_index=0,
                chunk_count=1,
                chunk_offset=0,
                chunk=jpeg,
            )

        with tempfile.TemporaryDirectory() as directory:
            assembler = EvidenceImageAssembler(directory)
            self.assertIsNone(assembler.accept(make_chunk(0)))
            self.assertIsNone(assembler.accept(make_chunk(0)))
            completed = assembler.accept(make_chunk(1))
            self.assertIsNotNone(completed)
            self.assertEqual(len(completed.image_paths), 2)


if __name__ == "__main__":
    unittest.main()
