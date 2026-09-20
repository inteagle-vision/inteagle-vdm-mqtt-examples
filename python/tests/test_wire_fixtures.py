"""Wire parity against firmware-derived, cross-language request fixtures."""
import json
from pathlib import Path
import unittest
from vdm_mqtt_sdk import VdmCodec
from vdm_mqtt_sdk.codec import RPC_REQUEST_TYPES


class WireFixtureTests(unittest.TestCase):
    def test_all_33_json_and_protobuf_requests_match_golden(self):
        cases=json.loads((Path(__file__).resolve().parents[2]/'tests/fixtures/rpc-cases.json').read_text())
        self.assertEqual({case['method'] for case in cases},set(RPC_REQUEST_TYPES))
        for case in cases:
            with self.subTest(method=case['method']):
                wire,_=VdmCodec('json').encode_rpc_request(case['method'],case['json'],case['reqId'])
                self.assertEqual(json.loads(wire),{'reqId':case['reqId'],'method':case['method'],'params':case['json']})
                wire,_=VdmCodec('protobuf').encode_rpc_request(case['method'],case['protobuf'],case['reqId'])
                self.assertEqual(wire.hex(),case['protobufHex'])
