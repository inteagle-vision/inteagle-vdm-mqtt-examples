"""The integration guide's actual request files must work with both codecs."""
import json
from pathlib import Path
import unittest
from google.protobuf import json_format
import inteagle_vdm_mqtt_v1_pb2 as pb
from vdm_mqtt_sdk import VdmCodec, VdmTopics

EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "alarms"


class AlarmContractTest(unittest.TestCase):
    def test_configuration_requests(self):
        for case in json.loads((EXAMPLES / "requests.json").read_text()):
            for profile in ("protobuf", "json"):
                with self.subTest(case=case["name"], profile=profile):
                    raw, _ = VdmCodec(profile).encode_rpc_request(
                        case["method"], case[profile], 91
                    )
                    if profile == "json":
                        self.assertEqual(
                            json.loads(raw),
                            {
                                "reqId": 91,
                                "method": case["method"],
                                "params": case["json"],
                            },
                        )
                    else:
                        expected = json_format.ParseDict(
                            {
                                "schemaVersion": 1,
                                "reqId": 91,
                                case["method"]: case["protobuf"],
                            },
                            pb.RpcRequest(),
                        )
                        self.assertEqual(pb.RpcRequest.FromString(raw), expected)

    def test_lifecycle_presence_and_large_ids(self):
        topics = VdmTopics.for_device("DEMO001")
        for case in json.loads((EXAMPLES / "events.json").read_text()):
            for profile in ("protobuf", "json"):
                with self.subTest(case=case["name"], profile=profile):
                    value = case[profile]
                    raw = (
                        json_format.ParseDict(value, pb.Alarm()).SerializeToString()
                        if profile == "protobuf"
                        else json.dumps(value).encode()
                    )
                    decoded = VdmCodec(profile).decode(topics.topic("3A"), topics, raw)
                    fields = decoded.as_dict()
                    self.assertEqual(fields["eventId"], value["eventId"])
                    self.assertEqual(fields["alarmId"], value["alarmId"])
                    self.assertEqual("level" in fields, "level" in value)
                    if profile == "protobuf":
                        self.assertEqual(
                            decoded.value.HasField("level"), "level" in value
                        )
                    self.assertEqual(
                        fields["ts"],
                        value["ts"],
                    )
