"""Notification policy remains idempotent across MQTT replay and process restart."""

import json
from pathlib import Path
import tempfile
import unittest

from alarm_notifications import AlarmNotificationStore


EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "alarms" / "events.json"


class AlarmNotificationStoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = {item["name"]: item["json"] for item in json.loads(EXAMPLES.read_text())}

    def test_reconnect_and_qos_replay_do_not_create_notification_storm(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "alarms.sqlite3"
            store = AlarmNotificationStore(database)
            self.assertEqual(store.process("DEMO001", self.events["triggered"]).action, "NOTIFY")
            self.assertEqual(store.process("DEMO001", self.events["triggered"]).action, "DUPLICATE")
            store.close()

            # De-duplication survives a consumer restart because it is not in memory.
            store = AlarmNotificationStore(database)
            self.assertEqual(store.process("DEMO001", self.events["triggered"]).action, "DUPLICATE")
            self.assertEqual(store.process("DEMO001", self.events["synced"]).action, "STATE_ONLY")
            self.assertEqual(store.process("DEMO001", self.events["escalated"]).notification, "ALARM_ESCALATED")
            self.assertEqual(store.process("DEMO001", self.events["recovered"]).notification, "ALARM_RECOVERED")
            self.assertEqual(store.process("DEMO001", self.events["recovered"]).action, "DUPLICATE")
            self.assertEqual(
                [item["notification"] for item in store.pending_notifications()],
                ["ALARM_TRIGGERED", "ALARM_ESCALATED", "ALARM_RECOVERED"],
            )
            store.close()

    def test_terminal_snapshot_without_known_active_alarm_is_state_only(self):
        store = AlarmNotificationStore()
        try:
            decision = store.process("DEMO001", self.events["cancelled"])
            self.assertEqual(decision.action, "STATE_ONLY")
            self.assertEqual(store.pending_notifications(), [])
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
