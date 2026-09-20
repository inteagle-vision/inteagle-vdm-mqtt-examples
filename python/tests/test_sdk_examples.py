import unittest
from vdm_mqtt_sdk import VdmCodec
try:
    from sdk_examples import requests_for
except ImportError:
    requests_for=None

class ExamplesTests(unittest.TestCase):
    def test_business_examples_encode_actual_parameters_for_both_formats(self):
        self.assertIsNotNone(requests_for,'SDK business examples are missing')
        for profile in ('json','protobuf'):
            cases=requests_for(profile)
            for name,(method,params) in cases.items():
                with self.subTest(profile=profile,name=name):
                    self.assertIsInstance(params,dict)
                    payload,_=VdmCodec(profile).encode_rpc_request(method,params,123)
                    self.assertTrue(payload)
            self.assertEqual(cases['targets-add'][1]['targets'][0]['targetId'],'T01')
            self.assertEqual(cases['measurement-status'][1]['jobId'],'REPLACE_WITH_JOB_ID')
            self.assertEqual(cases['alarms-events'][1]['pageSize'],20)
