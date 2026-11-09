package com.inteagle.examples.boot;

import static org.junit.jupiter.api.Assertions.*;

import com.inteagle.vdm.mqtt.sdk.*;
import com.inteagle.vdm.mqtt.v1.*;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** SDK 维护者回归测试：两台设备拥有同名标靶，属性增量与清空操作不得串设备。 */
class VdmBusinessExampleTest {
  @ParameterizedTest
  @EnumSource(PayloadFormat.class)
  void sameTargetIdOnDifferentDevicesRemainsIndependent(PayloadFormat format) throws Exception {
    var catalog =
        new VdmBusinessExample(
            null, java.nio.file.Path.of("unused"), false, java.nio.file.Path.of("unused.sqlite3"));
    catalog.onMessage("A", attributes(format, "A", 1));
    catalog.onMessage("B", attributes(format, "B", 1));
    assertEquals("A", catalog.targets("A").get(0).deviceId());
    assertEquals("B", catalog.targets("B").get(0).deviceId());
    assertEquals("T01", catalog.targets("A").get(0).targetId());
    assertEquals("T01", catalog.targets("B").get(0).targetId());
    catalog.onMessage("A", attributes(format, "A", -1)); // 不含列表：保留旧值。
    assertEquals(1, catalog.targets("A").size());
    catalog.onMessage("A", attributes(format, "A", 0)); // 明确空列表：只清空 A。
    assertTrue(catalog.targets("A").isEmpty());
    assertEquals(1, catalog.targets("B").size());
  }

  @org.junit.jupiter.api.Test
  void oneComponentUsesConfiguredSubscriptionWithoutStartupRpc() {
    for (String subscription : java.util.List.of("telemetry", "image", "attributes,rpc/resp")) {
      var transport = new RecordingTransport();
      new org.springframework.boot.test.context.runner.ApplicationContextRunner()
          .withUserConfiguration(VdmExampleApplication.class)
          .withPropertyValues("vdm.mqtt.all-devices=true", "vdm.mqtt.subscriptions=" + subscription)
          .withBean(VdmMqttTransport.class, () -> transport)
          .run(
              context -> {
                assertNull(context.getStartupFailure());
                assertEquals(
                    1,
                    context
                        .getBeansOfType(com.inteagle.vdm.mqtt.spring.VdmMessageListener.class)
                        .size());
                assertEquals(
                    java.util.Arrays.stream(subscription.split(","))
                        .map(suffix -> "vdm/+/" + suffix)
                        .toList(),
                    transport.topics);
                assertEquals(0, transport.publishes); // 查询和拍照是业务调用，启动不能自动对设备发送命令。
              });
    }
  }

  private static class RecordingTransport implements VdmMqttTransport {
    java.util.List<String> topics;
    int publishes;

    public void start(
        java.util.List<String> topics, int qos, java.time.Duration timeout, Listener listener) {
      this.topics = topics;
    }

    public void publish(
        String topic, byte[] payload, int qos, boolean retained, java.time.Duration timeout) {
      publishes++;
    }

    public void close() {}
  }

  private DecodedPayload attributes(PayloadFormat format, String device, int size)
      throws Exception {
    byte[] payload;
    if (format == PayloadFormat.PROTOBUF) {
      var message = Attributes.newBuilder().setSchemaVersion(1);
      if (size >= 0) {
        var list = TargetList.newBuilder();
        if (size > 0) list.addTargets(TargetInfo.newBuilder().setTargetId("T01"));
        message.setTargetList(list);
      }
      payload = message.build().toByteArray();
    } else {
      payload =
          (size < 0
                  ? "{}"
                  : size == 0 ? "{\"targets\":[]}" : "{\"targets\":[{\"targetId\":\"T01\"}]}")
              .getBytes(StandardCharsets.UTF_8);
    }
    var topics = VdmTopics.forDevice(device);
    return new VdmCodec(format).decode(topics.topic("attributes"), topics, payload);
  }
}
