package com.inteagle.vdm.service;

import static org.junit.jupiter.api.Assertions.*;

import com.inteagle.vdm.service.alarms.AlarmService;
import java.nio.file.Path;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class ReconciliationTest {
  @TempDir Path dir;

  @Test
  void omittedProtobufEmptyActiveListClearsWithoutInventingTimestamp() throws Exception {
    try (var store = new Store(dir, 10, 300)) {
      long ts = Store.now();
      store.alarm(
          "c",
          "d",
          Store.JSON.readTree(
              "{\"eventId\":\"1\",\"alarmId\":\"2\",\"ts\":"
                  + ts
                  + ",\"transition\":\"TRIGGERED\",\"level\":\"ALARM\"}"));
      store.alarm(
          "c",
          "d",
          Store.JSON.readTree(
              "{\"eventId\":\"3\",\"alarmId\":\"2\",\"ts\":"
                  + ts
                  + ",\"transition\":\"RECOVERED\"}"));
      AlarmService.StateReader rpc =
          (c, d) -> Store.JSON.readTree("{\"schemaVersion\":1,\"reqId\":7,\"getAlarmState\":{}}");
      new AlarmService(store, rpc).reconcile("c", "d");
      var row = store.rows("select * from incidents").getFirst();
      assertEquals(0, ((Number) row.get("needs_reconcile")).intValue());
      assertEquals(ts, ((Number) row.get("ts")).longValue());
      assertEquals(
          "TRIGGERED", row.get("transition"), "snapshot must not invent a lifecycle transition");
      assertEquals(0, ((Number) row.get("current_active")).intValue());
      assertEquals(
          "TRIGGERED", new AlarmService(store, rpc).local("c", "d").getFirst().get("transition"));
      assertEquals(
          false, new AlarmService(store, rpc).local("c", "d").getFirst().get("currentActive"));
      assertEquals(1, store.rows("select * from outbox").size());
    }
  }

  @Test
  void concurrentEqualEventKeepsReconciliationPending() throws Exception {
    try (var store = new Store(dir, 10, 300)) {
      long ts = Store.now();
      var event =
          Store.JSON
              .createObjectNode()
              .put("eventId", "1")
              .put("alarmId", "2")
              .put("ts", ts)
              .put("transition", "TRIGGERED")
              .put("level", "ALARM");
      store.alarm("c", "d", event);
      event.put("eventId", "2");
      store.alarm("c", "d", event);
      AlarmService.StateReader rpc =
          (c, d) -> {
            event.put("eventId", "3");
            store.alarm("c", "d", event);
            return Store.JSON.readTree("{\"code\":0,\"data\":{\"active\":[]}}");
          };
      new AlarmService(store, rpc).reconcile("c", "d");
      assertEquals(
          1,
          ((Number) store.rows("select * from incidents").getFirst().get("needs_reconcile"))
              .intValue());
    }
  }
}
