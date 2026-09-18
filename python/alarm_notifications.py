#!/usr/bin/env python3
"""Demonstrate durable, reconnect-safe notification decisions for VDM alarms."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sqlite3
from typing import Any


ACTIVE_TRANSITIONS = {"TRIGGERED", "ESCALATED", "DEESCALATED", "SYNCED"}
TERMINAL_TRANSITIONS = {"RECOVERED", "CANCELLED"}
TRANSITIONS = ACTIVE_TRANSITIONS | TERMINAL_TRANSITIONS


@dataclass(frozen=True)
class NotificationDecision:
    action: str
    notification: str | None
    reason: str


class AlarmNotificationStore:
    """Reference SQLite inbox/state/outbox implementation.

    Production services may use another database, but should preserve the same
    three durable boundaries: event de-duplication, incident state, and a unique
    notification outbox record committed in one transaction.
    """

    def __init__(self, database: str | Path = ":memory:") -> None:
        self.db = sqlite3.connect(str(database))
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS alarm_events (
                device_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                alarm_id TEXT NOT NULL,
                transition TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (device_id, event_id)
            );
            CREATE TABLE IF NOT EXISTS alarm_incidents (
                device_id TEXT NOT NULL,
                alarm_id TEXT NOT NULL,
                status TEXT NOT NULL,
                level TEXT,
                last_event_id TEXT NOT NULL,
                last_transition TEXT NOT NULL,
                PRIMARY KEY (device_id, alarm_id)
            );
            CREATE TABLE IF NOT EXISTS notification_outbox (
                device_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                notification TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                PRIMARY KEY (device_id, event_id, notification)
            );
            """
        )

    def close(self) -> None:
        self.db.close()

    def process(self, device_id: str, event: dict[str, Any]) -> NotificationDecision:
        device_id = str(device_id).strip()
        if not device_id:
            raise ValueError("deviceId must not be empty")
        event_id = self._decimal_id(event.get("eventId"), "eventId")
        alarm_id = self._decimal_id(event.get("alarmId"), "alarmId")
        transition = str(event.get("transition", "")).upper()
        if transition not in TRANSITIONS:
            raise ValueError(f"unsupported alarm transition: {transition or '<empty>'}")
        level = event.get("level")
        if transition in ACTIVE_TRANSITIONS and not level:
            raise ValueError(f"{transition} requires level")
        if transition in TERMINAL_TRANSITIONS and level is not None:
            raise ValueError(f"{transition} must not contain level")

        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        self.db.execute("BEGIN IMMEDIATE")
        try:
            inserted = self.db.execute(
                """
                INSERT INTO alarm_events
                    (device_id, event_id, alarm_id, transition, payload_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (device_id, event_id) DO NOTHING
                """,
                (device_id, event_id, alarm_id, transition, payload),
            ).rowcount
            if inserted == 0:
                self.db.commit()
                return NotificationDecision(
                    "DUPLICATE", None, "deviceId + eventId already processed"
                )

            previous = self.db.execute(
                """
                SELECT status, level
                FROM alarm_incidents
                WHERE device_id = ? AND alarm_id = ?
                """,
                (device_id, alarm_id),
            ).fetchone()
            was_active = previous is not None and previous["status"] == "ACTIVE"
            previous_level = previous["level"] if previous is not None else None
            status = "ACTIVE" if transition in ACTIVE_TRANSITIONS else "CLOSED"
            current_level = str(level) if status == "ACTIVE" else None
            self.db.execute(
                """
                INSERT INTO alarm_incidents
                    (device_id, alarm_id, status, level, last_event_id, last_transition)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (device_id, alarm_id) DO UPDATE SET
                    status = excluded.status,
                    level = excluded.level,
                    last_event_id = excluded.last_event_id,
                    last_transition = excluded.last_transition
                """,
                (
                    device_id,
                    alarm_id,
                    status,
                    current_level,
                    event_id,
                    transition,
                ),
            )

            notification = self._notification_for(
                transition, was_active, previous_level, current_level
            )
            if notification:
                self.db.execute(
                    """
                    INSERT INTO notification_outbox
                        (device_id, event_id, notification)
                    VALUES (?, ?, ?)
                    ON CONFLICT (device_id, event_id, notification) DO NOTHING
                    """,
                    (device_id, event_id, notification),
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        if notification:
            return NotificationDecision(
                "NOTIFY", notification, f"first actionable {transition} transition"
            )
        return NotificationDecision(
            "STATE_ONLY", None, self._state_only_reason(transition, was_active)
        )

    def pending_notifications(self) -> list[dict[str, str]]:
        rows = self.db.execute(
            """
            SELECT device_id, event_id, notification
            FROM notification_outbox
            WHERE status = 'PENDING'
            ORDER BY rowid
            """
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _notification_for(
        transition: str,
        was_active: bool,
        previous_level: str | None,
        current_level: str | None,
    ) -> str | None:
        if transition == "TRIGGERED" and not was_active:
            return "ALARM_TRIGGERED"
        if transition == "ESCALATED" and previous_level != current_level:
            return "ALARM_ESCALATED"
        if transition == "RECOVERED" and was_active:
            return "ALARM_RECOVERED"
        if transition == "CANCELLED" and was_active:
            return "ALARM_CANCELLED"
        return None

    @staticmethod
    def _state_only_reason(transition: str, was_active: bool) -> str:
        if transition == "SYNCED":
            return "connection recovery state sync never creates a user notification"
        if transition == "DEESCALATED":
            return "severity decrease updates state; notification is disabled by default"
        if transition in TERMINAL_TRANSITIONS and not was_active:
            return "terminal replay closes state without an orphan notification"
        return "incident state already represents this transition"

    @staticmethod
    def _decimal_id(value: Any, name: str) -> str:
        if isinstance(value, bool):
            raise ValueError(f"{name} must be a non-zero decimal integer")
        text = str(value).strip()
        if not text.isdecimal() or int(text) == 0:
            raise ValueError(f"{name} must be a non-zero decimal integer")
        return text


def demo(database: str | Path) -> None:
    examples = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "alarms"
            / "events.json"
        ).read_text()
    )
    events = {item["name"]: item["json"] for item in examples}
    steps = ["triggered", "triggered", "synced", "escalated", "recovered", "recovered"]
    store = AlarmNotificationStore(database)
    try:
        for name in steps:
            decision = store.process("DEMO001", events[name])
            print(
                json.dumps(
                    {
                        "input": name,
                        "eventId": events[name]["eventId"],
                        **asdict(decision),
                    },
                    ensure_ascii=False,
                )
            )
        print(
            json.dumps(
                {"pendingNotifications": store.pending_notifications()},
                ensure_ascii=False,
            )
        )
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=":memory:",
        help="SQLite path for durable de-duplication; defaults to an in-memory demo",
    )
    args = parser.parse_args()
    demo(args.database)


if __name__ == "__main__":
    main()
