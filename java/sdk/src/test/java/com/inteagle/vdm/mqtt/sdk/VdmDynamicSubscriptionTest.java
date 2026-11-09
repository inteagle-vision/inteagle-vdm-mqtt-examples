package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** SDK 维护者回归测试：标准通配符、100 台设备自动接入、动态增减与设备级 RPC 权限。 */
class VdmDynamicSubscriptionTest {
  private final Duration timeout = Duration.ofSeconds(1);

  @Test
  void followsStandardMqttWildcardRules() {
    assertTrue(MqttTopicFilters.matches("vdm/#", "vdm"));
    assertTrue(MqttTopicFilters.matches("vdm/+/rpc/+", "vdm/A/rpc/resp"));
    assertFalse(MqttTopicFilters.matches("vdm/+/+", "vdm/A/rpc/resp"));
    assertFalse(MqttTopicFilters.matches("#", "$SYS/broker"));
    assertTrue(
        MqttTopicFilters.matches(
            "vdm/+/telemetry", "vdm//telemetry")); // MQTT 空层合法，VDM 路由另行拒绝空设备 ID。
    for (String filter : List.of("", "vdm/A+", "vdm/#/image", "vdm/A/im#age")) {
      assertThrows(IllegalArgumentException.class, () -> MqttTopicFilters.validate(filter));
    }
  }

  @Test
  void wildcardDiscoversManyDevicesAndDynamicFiltersControlExistingClient() throws Exception {
    var transport = new SimulatedTransport();
    var received = new ArrayList<String>();
    var config =
        new VdmMqttClient.Config(
            "unused",
            1883,
            VdmTopics.forDevice("UNUSED"),
            PayloadFormat.JSON,
            null,
            null,
            null,
            1,
            timeout,
            List.of("telemetry"));
    try (var gateway =
        new VdmMqttGateway(
            config,
            List.of("vdm/+/attributes"),
            transport,
            (device, message) -> received.add(device + ":" + message.suffix()),
            error -> fail(error))) {
      gateway.start();
      for (int i = 0; i < 100; i++) {
        // Payload 伪造设备 ID 不得改变 Topic 路由；没有设备注册或额外连接过程。
        transport.listener.onMessage(
            "vdm/D" + i + "/attributes", "{\"deviceId\":\"SPOOF\"}".getBytes());
      }
      assertEquals(100, received.size());
      assertEquals("D99:attributes", received.get(received.size() - 1));
      assertEquals(1, transport.starts);
      var device = gateway.device("D0");
      assertThrows(IllegalStateException.class, () -> device.getTargets(timeout));
      gateway.subscribe("vdm/D0/rpc/resp");
      gateway.subscribe("vdm/D0/rpc/resp");
      assertEquals(1, transport.adds);
      assertTrue(device.getTargets(timeout).isEmpty()); // 已获取的逻辑客户端也必须看到新订阅。
      assertThrows(IllegalStateException.class, () -> gateway.device("D1").getTargets(timeout));
      gateway.subscribe("vdm/D0/image");
      gateway.subscribe("vdm/+/image");
      gateway.unsubscribe("vdm/D0/image");
      assertNotNull(device.requestSnapshot()); // 通配订阅仍覆盖此设备，取消精确过滤器不能将它排除。
      gateway.unsubscribe("vdm/D0/rpc/resp");
      assertThrows(IllegalStateException.class, () -> device.getTargets(timeout));
      assertThrows(IllegalStateException.class, () -> gateway.subscribe("vdm/DENIED/#"));
      assertFalse(gateway.topicFilters().contains("vdm/DENIED/#"));
    }
  }

  @Test
  void removingResponseSubscriptionFailsOnlyThatDevicesPendingRpc() throws Exception {
    var transport = new SimulatedTransport();
    transport.respond = false;
    var config =
        new VdmMqttClient.Config(
            "unused",
            1883,
            VdmTopics.forDevice("D0"),
            PayloadFormat.JSON,
            null,
            null,
            null,
            1,
            timeout,
            List.of("rpc/resp"));
    var threads = Executors.newSingleThreadExecutor();
    try (var gateway =
        new VdmMqttGateway(
            config, List.of("vdm/D0/rpc/resp", "vdm/D1/rpc/resp"), transport, null, null)) {
      gateway.start();
      var pending = threads.submit(() -> gateway.device("D0").getTargets(Duration.ofSeconds(5)));
      assertTrue(transport.published.await(1, TimeUnit.SECONDS));
      gateway.unsubscribe("vdm/D0/rpc/resp");
      assertThrows(ExecutionException.class, () -> pending.get(1, TimeUnit.SECONDS));
      transport.respond = true;
      assertTrue(gateway.device("D1").getTargets(timeout).isEmpty());
    } finally {
      threads.shutdownNow();
    }
  }

  private static class SimulatedTransport implements VdmMqttTransport {
    Listener listener;
    int starts;
    int adds;
    boolean respond = true;
    CountDownLatch published = new CountDownLatch(1);

    public void start(List<String> topics, int qos, Duration timeout, Listener listener) {
      this.listener = listener;
      starts++;
    }

    public void subscribe(String filter, int qos, Duration timeout) {
      if (filter.contains("DENIED")) throw new IllegalStateException("模拟 SUBACK 拒绝");
      adds++;
    }

    public void unsubscribe(String filter, Duration timeout) {}

    public void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
        throws Exception {
      published.countDown();
      if (!respond) return;
      int requestId =
          new com.fasterxml.jackson.databind.ObjectMapper().readTree(payload).path("reqId").asInt();
      listener.onMessage(
          topic.replace("/req", "/resp"),
          ("{\"reqId\":" + requestId + ",\"code\":0,\"data\":{\"targets\":[]}}").getBytes());
    }

    public void close() {}
  }
}
