package com.inteagle.vdm.service;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoreTest {
  @TempDir Path dir;
  final ObjectMapper json = new ObjectMapper();

  @Test
  void retentionUsesCurrentSnapshotClosureWithoutDroppingActiveOrAmbiguousState() throws Exception {
    try (var db = new Store(dir, 10, 300)) {
      for (int i = 1; i <= 3; i++)
        db.alarm(
            "a",
            "d",
            json.createObjectNode()
                .put("eventId", Integer.toString(i))
                .put("alarmId", Integer.toString(i))
                .put("ts", 1)
                .put("transition", "TRIGGERED")
                .put("level", "ALERT"));
      db.update("update incidents set current_active=0,reconciled_at=1 where alarm_id in('1','3')");
      db.update("update incidents set needs_reconcile=1 where alarm_id='3'");
      db.prune(7);
      assertEquals(
          List.of("2", "3"),
          db.rows("select alarm_id from incidents order by alarm_id").stream()
              .map(r -> r.get("alarm_id"))
              .toList());
    }
  }

  @Test
  void fullOutboxRollsBackAlarmEventAndStateUntilCapacityReturns() throws Exception {
    try (var db = new Store(dir, 1, 300)) {
      var event =
          json.createObjectNode()
              .put("eventId", "1")
              .put("alarmId", "1")
              .put("ts", Store.now())
              .put("transition", "TRIGGERED")
              .put("level", "ALERT");
      db.alarm("a", "d", event);
      event.put("eventId", "2").put("alarmId", "2");
      assertThrows(IllegalStateException.class, () -> db.alarm("a", "d", event));
      assertEquals(1, db.rows("select * from alarm_events").size());
      assertEquals(1, db.rows("select * from incidents").size());
      db.update("update outbox set status='failed'");
      assertThrows(IllegalStateException.class, () -> db.alarm("a", "d", event));
      db.update("update outbox set status='done'");
      db.alarm("a", "d", event);
      assertEquals(2, db.rows("select * from alarm_events").size());
      assertEquals(1, db.rows("select * from outbox where status='pending'").size());
    }
  }

  @Test
  void historyHasCapacityWithoutEvictingPendingWork() throws Exception {
    try (var db = new Store(dir, 1, 300)) {
      for (int i = 1; i <= 15; i++) {
        db.event("a", "d", json.createObjectNode().put("eventId", i).put("state", "READY"));
        db.alarm(
            "a",
            "d",
            json.createObjectNode()
                .put("eventId", Integer.toString(i))
                .put("alarmId", Integer.toString(i))
                .put("ts", Store.now())
                .put("transition", i == 1 ? "TRIGGERED" : "SYNCED")
                .put("level", "ALERT"));
      }
      assertEquals(10, db.rows("select * from alarm_events").size());
      assertEquals(10, db.rows("select * from evidence_events").size());
      assertEquals(1, db.rows("select * from outbox where status='pending'").size());
    }
  }

  @Test
  void distinctRepeatedTriggersAndNonIncreasingEscalationsAreStateOnly() throws Exception {
    try (var db = new Store(dir, 100, 300)) {
      long ts = Store.now() - 10;
      var event = json.createObjectNode().put("alarmId", "2").put("level", "ALARM");
      String[] transitions = {"TRIGGERED", "TRIGGERED", "ESCALATED", "ESCALATED", "RECOVERED"};
      for (int i = 0; i < transitions.length; i++) {
        event
            .put("eventId", Integer.toString(i + 1))
            .put("ts", ts + i)
            .put("transition", transitions[i]);
        if (i == 3) event.put("level", "ACTION");
        if (i == 4) event.remove("level");
        db.alarm("a", "d", event);
      }
      assertEquals(3, db.rows("select * from outbox").size());
    }
  }

  @Test
  void imageQuotaCountsPendingPayloadsAndExistingEvidenceFiles() throws Exception {
    byte[] twoMiB = new byte[2 * 1024 * 1024], oneAndHalfMiB = new byte[1536 * 1024];
    try (var db = new Store(dir, 10, 300, 3L * 1024 * 1024)) {
      db.enqueue("a", "d", "vdm/d/image", twoMiB);
      assertEquals(twoMiB.length, db.pendingEvidenceBytes(-1));
      assertThrows(
          Store.CapacityException.class, () -> db.enqueue("a", "d", "vdm/d/image", oneAndHalfMiB));
      assertEquals(1, db.rows("select * from inbox").size());
      db.update("update inbox set status='done'");
      java.nio.file.Files.createDirectories(dir.resolve("evidence"));
      java.nio.file.Files.write(dir.resolve("evidence/saved.jpg"), twoMiB);
      assertThrows(
          Store.CapacityException.class, () -> db.enqueue("a", "d", "vdm/d/image", oneAndHalfMiB));
      assertThrows(
          IllegalArgumentException.class,
          () -> db.enqueue("a", "d", "vdm/d/image", new byte[4 * 1024 * 1024]));
    }
  }

  @Test
  void durableBoundedInboxAndExclusiveDirectory() throws Exception {
    try (var db = new Store(dir, 1, 300)) {
      db.enqueue("a", "d", "vdm/d/telemetry", new byte[] {1});
      assertThrows(Store.CapacityException.class, () -> db.enqueue("a", "d", "x", new byte[] {2}));
      assertThrows(Exception.class, () -> new Store(dir, 1, 300));
    }
    try (var db = new Store(dir, 1, 300)) {
      assertEquals(1, db.rows("select * from inbox").size());
    }
  }

  @Test
  void alarmsDeduplicatePerConnectionAndNeverRegressOrNotifyStale() throws Exception {
    long now = System.currentTimeMillis() / 1000;
    try (var db = new Store(dir, 100, 300)) {
      var event =
          json.readTree(
              "{\"eventId\":\"1\",\"alarmId\":\"4\",\"ts\":"
                  + now
                  + ",\"transition\":\"ALARM_TRANSITION_TRIGGERED\",\"level\":\"ALARM_LEVEL_WARNING\"}");
      db.alarm("a", "d", event);
      db.alarm("a", "d", event);
      db.alarm("b", "d", event);
      assertEquals(2, db.rows("select * from outbox").size());
      var older = event.deepCopy();
      ((com.fasterxml.jackson.databind.node.ObjectNode) older)
          .put("eventId", "2")
          .put("transition", "RECOVERED")
          .put("ts", now - 1)
          .remove("level");
      db.alarm("a", "d", older);
      assertEquals(2, db.rows("select * from outbox").size());
      assertEquals(
          1,
          ((Number)
                  db.rows("select * from incidents where c='a'").getFirst().get("needs_reconcile"))
              .intValue());
      assertEquals(
          "TRIGGERED", db.rows("select * from incidents where c='a'").getFirst().get("transition"));
      ((com.fasterxml.jackson.databind.node.ObjectNode) older).put("eventId", "3").put("ts", now);
      db.alarm("a", "d", older);
      assertEquals(2, db.rows("select * from outbox").size());
      ((com.fasterxml.jackson.databind.node.ObjectNode) event)
          .put("eventId", "9")
          .put("alarmId", "9")
          .put("ts", now - 1000);
      db.alarm("a", "d", event);
      assertEquals(2, db.rows("select * from outbox").size());
      assertEquals(1, db.rows("select * from jobs where kind='reconcile'").size());
    }
  }

  @Test
  void completedInboxRetentionCannotExceedCapacity() throws Exception {
    try (var db = new Store(dir, 2, 300)) {
      for (int i = 0; i < 8; i++) {
        db.enqueue("a", "d", "vdm/d/telemetry", new byte[] {1});
        db.update("update inbox set status='done'");
      }
      assertTrue(db.rows("select * from inbox").size() <= 2);
      assertEquals(
          8, ((Number) db.rows("select max(id) n from inbox").getFirst().get("n")).intValue());
    }
  }

  @Test
  void latestAndEvidenceEventsStaySeparate() throws Exception {
    try (var db = new Store(dir, 100, 300)) {
      db.latest("a", "d", "telemetry", json.readTree("{\"ts\":1}"));
      db.latest("a", "d", "attributes", json.readTree("{\"name\":\"demo\"}"));
      assertEquals(2, db.latest("a", "d").get("receivedCount"));
      db.event("a", "d", json.readTree("{\"eventId\":\"1\",\"state\":\"READY\"}"));
      db.event("a", "d", json.readTree("{\"eventId\":\"1\",\"state\":\"ACKED\"}"));
      assertEquals(2, db.rows("select * from evidence_events").size());
      assertEquals(0, db.rows("select * from outbox").size());
    }
  }
}
