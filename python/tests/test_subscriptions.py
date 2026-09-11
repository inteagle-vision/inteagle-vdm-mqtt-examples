"""Subscription selection must keep existing clients compatible."""

import unittest
from unittest.mock import Mock

from vdm_mqtt_sdk import VdmMqttClient, VdmMqttClientConfig, VdmTopics


class SubscriptionTests(unittest.TestCase):
    def test_default_and_data_only_subscriptions(self):
        for suffixes in (
            None,
            ("telemetry", "attributes"),
            ("telemetry", "attributes", "rpc/resp"),
        ):
            with self.subTest(suffixes=suffixes):
                client = VdmMqttClient(
                    VdmMqttClientConfig(
                        host="localhost",
                        port=1883,
                        topics=VdmTopics.for_device("DEMO001"),
                        payload_format="json",
                        subscription_suffixes=suffixes,
                    )
                )
                mqtt = Mock()
                mqtt.subscribe.return_value = (0, 1)
                client._on_connect(mqtt, None, None, 0)
                expected = [
                    ("vdm/DEMO001/" + suffix, 1) for suffix in (suffixes or ("#",))
                ]
                mqtt.subscribe.assert_called_once_with(expected)
                self.assertTrue(client.is_connected)

    def test_empty_unknown_and_wildcard_subscriptions_are_rejected(self):
        for suffixes in ((), ("",), ("#",), ("+",), ("telemetry/#",), ("unknown",)):
            with self.subTest(suffixes=suffixes), self.assertRaises(ValueError):
                VdmMqttClientConfig(
                    host="localhost",
                    port=1883,
                    topics=VdmTopics.for_device("DEMO001"),
                    payload_format="json",
                    subscription_suffixes=suffixes,
                )

    def test_broker_denial_is_not_reported_as_successful_subscription(self):
        client = VdmMqttClient(VdmMqttClientConfig(host="localhost", port=1883, topics=VdmTopics.for_device("DEMO001"), payload_format="json"))
        client._client = Mock()
        client._client.connect.side_effect = lambda *a, **k: client._on_subscribe(None, None, 1, [128])
        with self.assertRaises(PermissionError):
            client.start()

    def test_automatic_request_ids_wrap_without_zero(self):
        import itertools
        import json
        import paho.mqtt.client as mqtt
        client = VdmMqttClient(VdmMqttClientConfig(host="localhost", port=1883, topics=VdmTopics.for_device("DEMO001"), payload_format="json"))
        client._req_ids = itertools.count((1 << 31) - 1)
        observed = []
        def reply(topic, payload, **kwargs):
            request = json.loads(payload)
            observed.append(request["reqId"])
            message = mqtt.MQTTMessage()
            message.topic = b"vdm/DEMO001/rpc/resp"
            message.payload = json.dumps({"reqId": request["reqId"], "code": 0, "data": {"deviceId": "DEMO001"}}).encode()
            client._on_message(None, None, message)
        client.publish_raw = reply
        client.call("getAttr", {})
        client.call("getAttr", {})
        self.assertEqual(observed, [(1 << 31) - 1, 1])
