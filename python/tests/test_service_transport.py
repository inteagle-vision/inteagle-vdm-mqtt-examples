import json
import unittest
from unittest.mock import Mock
import paho.mqtt.client as mqtt
from vdm_mqtt_sdk import VdmMqttClient, VdmMqttClientConfig, VdmTopics

class DurableTransportTests(unittest.TestCase):
    def test_persistence_precedes_ack_and_failed_persistence_never_acks(self):
        order = []
        config = VdmMqttClientConfig(host='localhost', port=1883, topics=VdmTopics.for_device('D'), payload_format='json')
        self.assertIn('manual_ack', config.__dataclass_fields__, 'SDK lacks durable raw callback/manual acknowledgement')
        config = VdmMqttClientConfig(host='localhost', port=1883, topics=VdmTopics.for_device('D'), payload_format='json', manual_ack=True, persistent_session=True)
        sdk = VdmMqttClient(config, on_raw_message=lambda topic, raw: order.append('persist'))
        transport = Mock()
        transport.ack.side_effect = lambda *args: order.append('ack')
        msg = mqtt.MQTTMessage(mid=1)
        msg.topic = b'vdm/D/telemetry'; msg.payload = b'{}'; msg.qos = 1
        sdk._on_message(transport, None, msg)
        self.assertEqual(order, ['persist', 'ack'])
        order.clear()
        def full(*args): raise BufferError('full')
        sdk.on_raw_message = full
        sdk._on_message(transport, None, msg)
        self.assertEqual(order, [])
        transport.disconnect.assert_called_once()

    def test_rpc_response_bypasses_durable_raw_hook(self):
        config = VdmMqttClientConfig(host='localhost', port=1883, topics=VdmTopics.for_device('D'), payload_format='json')
        self.assertIn('manual_ack', config.__dataclass_fields__, 'manual ack missing')
        persisted = []
        sdk = VdmMqttClient(VdmMqttClientConfig(host='localhost', port=1883, topics=VdmTopics.for_device('D'), payload_format='json', manual_ack=True), on_raw_message=lambda *a: persisted.append(a))
        msg = mqtt.MQTTMessage(mid=1)
        msg.topic=b'vdm/D/rpc/resp'; msg.qos=1
        transport = Mock()
        def respond(topic, payload, **kwargs):
            msg.payload=json.dumps({'reqId':json.loads(payload)['reqId'],'code':0}).encode()
            sdk._on_message(transport,None,msg)
        sdk.publish_raw=respond
        self.assertEqual(sdk.call('getAttr',{}).as_dict()['code'],0)
        self.assertEqual(persisted, [])
        transport.ack.assert_called_once_with(1,1)

    def test_rpc_publish_ack_uses_requested_timeout_budget(self):
        sdk=VdmMqttClient(VdmMqttClientConfig(host='localhost',port=1883,topics=VdmTopics.for_device('D'),payload_format='json'))
        publish=Mock()
        publish.rc=0
        publish.is_published.return_value=False
        sdk._client.publish=Mock(return_value=publish)
        with self.assertRaises(TimeoutError): sdk.call('getAttr',{},timeout=.1)
        self.assertLessEqual(publish.wait_for_publish.call_args.kwargs['timeout'],.1)
