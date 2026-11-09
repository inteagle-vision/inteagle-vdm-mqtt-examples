package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** SDK 维护者回归测试：验证按需订阅和请求 ID 隔离；只创建客户端，不连接 Broker。 */
class VdmSubscriptionTest {
  // 兼容历史构造器的全订阅行为；Starter 默认订阅由独立测试保证。
  @Test
  void originalConfigConstructorKeepsWildcardSubscription() {
    var config =
        new VdmMqttClient.Config(
            "localhost",
            1883,
            VdmTopics.forDevice("TEST"),
            PayloadFormat.PROTOBUF,
            null,
            null,
            null,
            1,
            Duration.ofSeconds(10));
    assertEquals(List.of("#"), config.subscriptionSuffixes());
  }

  // 配置快照必须不可变，外部修改原列表不能悄悄改变已经创建的客户端。
  @Test
  void explicitSubscriptionsAreImmutableAndValidated() {
    var suffixes = new ArrayList<>(List.of("telemetry", "attributes"));
    var config = config(suffixes);
    suffixes.add("rpc/resp");
    assertEquals(List.of("telemetry", "attributes"), config.subscriptionSuffixes());
    assertThrows(
        UnsupportedOperationException.class, () -> config.subscriptionSuffixes().add("rpc/resp"));
    assertThrows(IllegalArgumentException.class, () -> config(List.of()));
    assertThrows(IllegalArgumentException.class, () -> config(List.of("+/rpc/resp")));
  }

  // 缺少响应订阅时在发布前报错，避免向设备发出永远无法完成关联的命令。
  @Test
  void rpcWithoutResponseSubscriptionFailsBeforePublishing() throws Exception {
    try (var client =
        new VdmMqttClient(
            config(List.of("telemetry", "attributes")), new TestTransport(), null, null)) {
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> client.call("getAttr", Map.of(), Duration.ofSeconds(1)));
      assertTrue(error.getMessage().contains("rpc/resp"));
    }
  }

  // 多个客户端不能都从固定 ID 开始，降低共享设备响应 Topic 上的串响应概率。
  @Test
  void independentClientsDoNotShareAFixedRequestSequence() throws Exception {
    var next = VdmMqttClient.class.getDeclaredMethod("nextReqId");
    next.setAccessible(true);
    var firstIds = new HashSet<Integer>();
    for (int i = 0; i < 16; i++) {
      try (var client =
          new VdmMqttClient(config(List.of("rpc/resp")), new TestTransport(), null, null)) {
        int id = (Integer) next.invoke(client);
        assertNotEquals(0, id);
        firstIds.add(id);
      }
    }
    // The old fixed sequence produced 1 for every independent connection.
    assertTrue(firstIds.size() > 1);
  }

  // 通过反射把计数器放到边界，验证 signed int32 回绕并跳过协议禁止的 0。
  @Test
  void requestSequenceSkipsZeroAfterSignedInt32Wraparound() throws Exception {
    var next = VdmMqttClient.class.getDeclaredMethod("nextReqId");
    next.setAccessible(true);
    var field = VdmMqttClient.class.getDeclaredField("reqIds");
    field.setAccessible(true);
    try (var client =
        new VdmMqttClient(config(List.of("rpc/resp")), new TestTransport(), null, null)) {
      AtomicInteger counter = (AtomicInteger) field.get(client);
      counter.set(Integer.MAX_VALUE);
      assertEquals(Integer.MIN_VALUE, next.invoke(client));
      counter.set(-1);
      assertEquals(1, next.invoke(client));
    }
  }

  // 纯内存占位传输：这些测试只验证配置和请求 ID，不依赖任何 MQTT 客户端库。
  private static final class TestTransport implements VdmMqttTransport {
    public void start(List<String> topics, int qos, Duration timeout, Listener listener) {}

    public void publish(
        String topic, byte[] payload, int qos, boolean retained, Duration timeout) {}

    public void close() {}
  }

  private static VdmMqttClient.Config config(List<String> suffixes) {
    return new VdmMqttClient.Config(
        "localhost",
        1883,
        VdmTopics.forDevice("TEST"),
        PayloadFormat.PROTOBUF,
        null,
        null,
        null,
        1,
        Duration.ofSeconds(10),
        suffixes);
  }
}
