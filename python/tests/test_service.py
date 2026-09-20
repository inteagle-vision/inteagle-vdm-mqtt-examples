"""Behavior tests for the persistent service, using a fake device at the transport edge."""
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from vdm_service.app import create_app
from vdm_service.config import Settings
from vdm_service.runtime import Service


CONFIG = {"connections": [{"id": "local", "host": "localhost", "port": 1883,
    "devices": [{"id": "DEMO", "format": "json", "capabilities": []}]}],
    "rpcTimeoutMs": 100, "maxInboxRows": 100, "notificationMaxAgeSeconds": 300,
    "retentionDays": 7, "maxEvidenceBytes": 1024 * 1024}


class Device:
    def __init__(self, config, **kwargs):
        self.config = config
        self.is_connected = True
        self.calls = []
        self.error = None
        self.response = {"reqId": 1, "code": 0, "data": {"active": []}}
    def call(self, method, params, **kwargs):
        self.calls.append((method, params))
        if self.error:
            raise self.error
        return SimpleNamespace(as_dict=lambda: self.response)
    def stop(self):
        pass


class ServiceFixture(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(Service, "durable Python service has not been implemented")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = Settings.from_dict(CONFIG, data_dir=self.tmp.name, environ={})
        self.svc = Service(self.settings, client_factory=Device)
        self.addCleanup(self.svc.close)

    def alarm(self, event_id="1", ts=None, transition="TRIGGERED"):
        return {"eventId": event_id, "alarmId": "99", "ts": int(time.time()) if ts is None else ts,
                "transition": transition, "level": "ALERT"}

    def ingest(self, suffix, payload, connection="local"):
        self.svc.ingest(connection, "DEMO", "vdm/DEMO/" + suffix, json.dumps(payload).encode())
        self.svc.process_one()


class ServiceTests(ServiceFixture):
    def test_latest_survives_restart_and_increments_count(self):
        self.ingest("telemetry", {"x": 1})
        self.ingest("attributes", {"deviceId": "DEMO"})
        self.svc.close()
        self.svc = Service(self.settings, client_factory=Device)
        self.addCleanup(self.svc.close)
        self.assertEqual(self.svc.store.latest("local", "DEMO"), {
            "telemetry": {"x": 1}, "attributes": {"deviceId": "DEMO"}, "receivedCount": 2})
        self.assertEqual(self.svc.store.db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        self.assertEqual(self.svc.store.db.execute("PRAGMA user_version").fetchone()[0], 1)

    def test_alarm_dedup_atomic_outbox_and_equal_timestamp_reconcile(self):
        event = self.alarm()
        self.ingest("3A", event)
        self.ingest("3A", event)
        self.assertEqual(self.svc.store.count("alarm_events"), 1)
        self.assertEqual(self.svc.store.count("outbox"), 1)
        self.ingest("3A", self.alarm("2", event["ts"], "RECOVERED"))
        state = self.svc.store.alarms("local", "DEMO")[0]
        self.assertEqual(state["transition"], "TRIGGERED")
        self.assertTrue(state["needsReconcile"])
        self.assertEqual(self.svc.store.count("jobs"), 1)
        self.assertEqual(self.svc.store.count("outbox"), 1)
        self.svc.process_job()
        self.assertEqual(self.svc.clients[("local", "DEMO")].calls[0][0], "getAlarmState")
        self.assertEqual(self.svc.store.alarms("local", "DEMO")[0]["status"], "CLOSED")

    def test_stale_future_synced_and_evidence_do_not_notify(self):
        now = int(time.time())
        for i, ts, transition in [(1, now-400, "TRIGGERED"), (2, now+400, "ESCALATED"),
                                  (3, now+401, "ALARM_TRANSITION_SYNCED")]:
            self.ingest("3A", self.alarm(str(i), ts, transition))
        self.ingest("event", {"eventId": "1", "state": "PENDING"})
        self.ingest("event", {"eventId": "1", "state": "COMPLETE"})
        self.assertEqual(self.svc.store.count("outbox"), 0)
        self.assertEqual(self.svc.store.count("evidence_events"), 2)

    def test_capacity_failure_keeps_existing_inbox(self):
        self.svc.settings.max_inbox_rows = 1
        self.svc.ingest("local", "DEMO", "vdm/DEMO/telemetry", b'{}')
        with self.assertRaises(BufferError):
            self.svc.ingest("local", "DEMO", "vdm/DEMO/telemetry", b'{}')
        self.assertEqual(self.svc.store.count("inbox"), 1)

    def test_http_contract_validation_unknown_device_async_and_errors(self):
        app = create_app(self.svc, manage_lifecycle=False)
        client = TestClient(app)
        prefix = "/v1/connections/local/devices/DEMO/"
        self.assertEqual(client.get("/health").status_code, 200)
        self.assertEqual(len(client.get("/v1/devices").json()["devices"]), 1)
        self.assertEqual(client.post(prefix + "targets/query", json=[]).status_code, 400)
        self.assertEqual(client.post(prefix + "targets/query", json={"extra": 1}).status_code, 400)
        self.assertEqual(client.post(prefix + "device/motor/enable", json={}).status_code, 409)
        self.assertEqual(client.post(prefix.replace("DEMO", "UNKNOWN") + "targets/query", json={}).status_code, 404)
        r = client.post(prefix + "measurement/sync", json={"params": {"type": "displacement"}})
        self.assertEqual(r.status_code, 202)
        self.assertEqual(r.json()["status"], "accepted")
        self.svc.clients[("local", "DEMO")].error = TimeoutError()
        r = client.post(prefix + "targets/add", json={"params": {"targets": []}})
        self.assertEqual(r.status_code, 504)
        self.assertEqual(r.json()["error"]["outcome"], "unknown")

    def test_nonloopback_requires_token_and_auth_all_but_health(self):
        with self.assertRaises(ValueError):
            Settings.from_dict(CONFIG, data_dir=self.tmp.name, environ={"VDM_HTTP_HOST": "0.0.0.0"})
        self.svc.settings.api_token = "secret"
        client = TestClient(create_app(self.svc, manage_lifecycle=False))
        self.assertEqual(client.get("/health").status_code, 200)
        self.assertEqual(client.get("/v1/devices").status_code, 401)
        self.assertEqual(client.get("/v1/devices", headers={"Authorization": "Bearer secret"}).status_code, 200)

    def test_only_one_service_instance_uses_data_directory(self):
        with self.assertRaises(RuntimeError):
            Service(self.settings, client_factory=Device)


if __name__ == '__main__':
    unittest.main()
