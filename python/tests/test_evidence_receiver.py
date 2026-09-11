import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from evidence_receiver import acknowledge_with_retry
from vdm_mqtt_sdk import RpcError

class EvidenceReceiverTests(unittest.TestCase):
    def test_rate_limit_and_timeout_retry_then_succeed(self):
        client, stopped = Mock(), Mock()
        stopped.wait.return_value = False
        client.ack_evidence_package.side_effect = [RpcError(1,4,"limited"), TimeoutError(), None]
        self.assertTrue(acknowledge_with_retry(client, SimpleNamespace(event_id=9, package_sha256="hash"), stopped))
        self.assertEqual(client.ack_evidence_package.call_count,3)
        self.assertEqual([c.args[0] for c in stopped.wait.call_args_list],[1,2])

    def test_invalid_parameter_is_not_retried(self):
        client, stopped = Mock(), Mock()
        client.ack_evidence_package.side_effect = RpcError(1,2,"invalid")
        with self.assertRaises(RpcError):
            acknowledge_with_retry(client, SimpleNamespace(event_id=9, package_sha256="hash"), stopped)
        self.assertEqual(client.ack_evidence_package.call_count,1)
        stopped.wait.assert_not_called()

    def test_retries_are_bounded_and_shutdown_cancels_wait(self):
        item=SimpleNamespace(event_id=9, package_sha256="hash")
        client, stopped=Mock(), Mock()
        client.ack_evidence_package.side_effect=TimeoutError()
        stopped.wait.return_value=False
        with self.assertRaises(TimeoutError):acknowledge_with_retry(client,item,stopped)
        self.assertEqual(client.ack_evidence_package.call_count,4)
        stopped.wait.return_value=True
        client.reset_mock()
        self.assertFalse(acknowledge_with_retry(client,item,stopped))
        self.assertEqual(client.ack_evidence_package.call_count,1)
