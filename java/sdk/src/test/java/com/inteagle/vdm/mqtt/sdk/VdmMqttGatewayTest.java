package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SDK 维护者回归测试：验证平台共用连接、跨设备路由和请求关联；完全不依赖 Paho。 */
class VdmMqttGatewayTest {
  @Test
  void sharesOneConnectionWhileKeepingDeviceRpcAndSubscriptionsSeparate() throws Exception {
    var transport = new SimulatedTransport();
    var received = new ArrayList<String>();
    try (var gateway =
        new VdmMqttGateway(
            config(),
            List.of("D1", "D2"),
            false,
            transport,
            (id, message) -> received.add(id),
            error -> fail(error))) {
      gateway.start();
      gateway.start();
      assertEquals(1, transport.starts); // 重复启动不能产生第二条物理连接。
      assertEquals(
          List.of("vdm/D1/telemetry", "vdm/D1/rpc/resp", "vdm/D2/telemetry", "vdm/D2/rpc/resp"),
          transport.topics);
      assertEquals(
          "D1",
          gateway
              .device("D1")
              .call("getTargets", Map.of(), Duration.ofSeconds(1))
              .data()
              .path("data")
              .asText());
      assertEquals(
          "D2",
          gateway
              .device("D2")
              .call("getTargets", Map.of(), Duration.ofSeconds(1))
              .data()
              .path("data")
              .asText());
      assertEquals(List.of("D1", "D2"), received);
      assertThrows(IllegalArgumentException.class, () -> gateway.device("D3"));
    }
    assertEquals(1, transport.closes);
  }

  @Test
  void allDevicesRequiresExplicitModeAndUsesTopicWildcard() throws Exception {
    var transport = new SimulatedTransport();
    try (var gateway = new VdmMqttGateway(config(), List.of(), true, transport, null, null)) {
      gateway.start();
      assertEquals(List.of("vdm/+/telemetry", "vdm/+/rpc/resp"), transport.topics);
      assertNotNull(gateway.device("NEW_DEVICE"));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new VdmMqttGateway(config(), List.of(), false, transport, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new VdmMqttGateway(config(), List.of("D1"), true, transport, null, null));
  }

  private VdmMqttClient.Config config() {
    return new VdmMqttClient.Config(
        "unused",
        1883,
        VdmTopics.forDevice("D1"),
        PayloadFormat.JSON,
        null,
        null,
        null,
        1,
        Duration.ofSeconds(1),
        List.of("telemetry", "rpc/resp"));
  }

  private static class SimulatedTransport implements VdmMqttTransport {
    int starts;
    int closes;
    List<String> topics;
    Listener listener;

    public void start(List<String> topics, int qos, Duration timeout, Listener listener) {
      starts++;
      this.topics = topics;
      this.listener = listener;
    }

    public void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
        throws Exception {
      // 响应携带所在设备 ID，验证不同设备的响应不会串入同一逻辑客户端。
      var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      int id = mapper.readTree(payload).path("reqId").asInt();
      listener.onMessage(
          topic.replace("/rpc/req", "/rpc/resp"),
          mapper.writeValueAsBytes(Map.of("reqId", id, "code", 0, "data", topic.split("/")[1])));
    }

    public void close() {
      closes++;
    }
  }
}
