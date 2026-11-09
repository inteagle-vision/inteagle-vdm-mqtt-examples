package com.inteagle.examples.vdm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** SDK 维护者回归测试：使用临时 SQLite 验证持久化去重与 outbox；不发送任何外部通知。 */
final class AlarmNotificationStoreTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir Path temporaryDirectory;

  // 关闭并重开同一个数据库，证明去重依赖持久化约束，而不是进程内集合。
  @Test
  void deduplicationSurvivesProcessRestart() throws Exception {
    Path database = temporaryDirectory.resolve("alarms.sqlite3");
    JsonNode triggered = event("101", "77", "TRIGGERED", "ALERT");

    try (AlarmNotificationStore store = new AlarmNotificationStore(database)) {
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY, store.process("DEVICE-A", triggered).action());
      assertEquals(1, store.pendingNotificationCount());
    }

    try (AlarmNotificationStore reopened = new AlarmNotificationStore(database)) {
      assertEquals(
          AlarmNotificationStore.Action.DUPLICATE,
          reopened.process("DEVICE-A", triggered).action());
      assertEquals(1, reopened.pendingNotificationCount());
    }
  }

  // SYNCED 只更新状态；需要通知的状态变化只创建一次 outbox 记录。
  @Test
  void syncNeverNotifiesAndLifecycleTransitionsNotifyOnce() throws Exception {
    try (AlarmNotificationStore store =
        new AlarmNotificationStore(temporaryDirectory.resolve("lifecycle.sqlite3"))) {
      assertEquals(
          AlarmNotificationStore.Action.STATE_ONLY,
          store.process("DEVICE-A", event("201", "88", "SYNCED", "ALERT")).action());
      assertEquals(
          AlarmNotificationStore.Action.STATE_ONLY,
          store.process("DEVICE-A", event("202", "88", "TRIGGERED", "ALERT")).action());
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY,
          store.process("DEVICE-A", event("203", "88", "ESCALATED", "ACTION")).action());
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY,
          store.process("DEVICE-A", event("204", "88", "RECOVERED", null)).action());
      assertEquals(
          AlarmNotificationStore.Action.STATE_ONLY,
          store.process("DEVICE-A", event("205", "88", "RECOVERED", null)).action());
      assertEquals(2, store.pendingNotificationCount());
    }
  }

  // Protobuf 枚举名与 JSON 简写应归一为同一通知策略。
  @Test
  void protobufEnumNamesUseTheSamePolicy() throws Exception {
    try (AlarmNotificationStore store =
        new AlarmNotificationStore(temporaryDirectory.resolve("protobuf.sqlite3"))) {
      JsonNode payload = event("301", "99", "ALARM_TRANSITION_TRIGGERED", "ALARM_LEVEL_ALERT");
      AlarmNotificationStore.Decision decision = store.process("DEVICE-A", payload);

      assertEquals(AlarmNotificationStore.Action.NOTIFY, decision.action());
      assertEquals("ALARM_TRIGGERED", decision.notification());
    }
  }

  // 相同 eventId 在不同设备上是两个事件，去重键必须包含 deviceId。
  @Test
  void eventIdIsScopedByDevice() throws Exception {
    try (AlarmNotificationStore store =
        new AlarmNotificationStore(temporaryDirectory.resolve("devices.sqlite3"))) {
      JsonNode payload = event("401", "100", "TRIGGERED", "ALERT");
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY, store.process("DEVICE-A", payload).action());
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY, store.process("DEVICE-B", payload).action());
      assertEquals(2, store.pendingNotificationCount());
    }
  }

  // 用公共文档的整组消息回放，验证重复、同步和终态不会额外创建通知。
  @Test
  void documentedAlarmSequenceProducesOnlyThreeNotifications() throws Exception {
    JsonNode examples = MAPPER.readTree(getClass().getResourceAsStream("/alarms/events.json"));
    try (AlarmNotificationStore store =
        new AlarmNotificationStore(temporaryDirectory.resolve("examples.sqlite3"))) {
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY,
          store.process("DEVICE-A", examples.get(0).path("json")).action());
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY,
          store.process("DEVICE-A", examples.get(1).path("json")).action());
      assertEquals(
          AlarmNotificationStore.Action.STATE_ONLY,
          store.process("DEVICE-A", examples.get(2).path("json")).action());
      assertEquals(
          AlarmNotificationStore.Action.STATE_ONLY,
          store.process("DEVICE-A", examples.get(3).path("json")).action());
      assertEquals(
          AlarmNotificationStore.Action.NOTIFY,
          store.process("DEVICE-A", examples.get(4).path("json")).action());
      assertEquals(
          AlarmNotificationStore.Action.STATE_ONLY,
          store.process("DEVICE-A", examples.get(5).path("json")).action());
      assertEquals(
          AlarmNotificationStore.Action.DUPLICATE,
          store.process("DEVICE-A", examples.get(0).path("json")).action());
      assertEquals(3, store.pendingNotificationCount());
    }
  }

  // 验证 outbox 的领取租约、完成和重试状态；避免多个 Worker 重复领取。
  @Test
  void workerLeaseControlsDeliveryAndRetry() throws Exception {
    try (AlarmNotificationStore store =
        new AlarmNotificationStore(temporaryDirectory.resolve("outbox.sqlite3"))) {
      store.process("DEVICE-A", event("501", "101", "TRIGGERED", "ALERT"));

      AlarmNotificationStore.Notification first =
          store.claimNext("worker-one", java.time.Duration.ofMinutes(1));
      assertEquals("DEVICE-A:501:ALARM_TRIGGERED", first.idempotencyKey());
      assertEquals(null, store.claimNext("worker-two", java.time.Duration.ofMinutes(1)));

      store.retryLater(first, java.time.Duration.ZERO, "temporary webhook failure");
      AlarmNotificationStore.Notification retried =
          store.claimNext("worker-two", java.time.Duration.ofMinutes(1));
      assertEquals("worker-two", retried.deliveryToken());
      store.markSent(retried);
      assertEquals(null, store.claimNext("worker-three", java.time.Duration.ofMinutes(1)));
    }
  }

  private static JsonNode event(String eventId, String alarmId, String transition, String level) {
    var node =
        MAPPER
            .createObjectNode()
            .put("eventId", eventId)
            .put("alarmId", alarmId)
            .put("transition", transition);
    if (level != null) {
      node.put("level", level);
    }
    return node;
  }
}
