from __future__ import annotations

import json
import unittest

import inteagle_vdm_mqtt_v1_pb2 as pb

from vdm_mqtt_sdk import ImageFrame, PayloadFormat, VdmCodec, VdmTopics
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

    def test_internal_rpc_name_is_rejected_before_publish(self) -> None:
        with self.assertRaises(ValueError):
            VdmCodec("protobuf").encode_rpc_request("wySetAttributes", {}, 9)

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


if __name__ == "__main__":
    unittest.main()
