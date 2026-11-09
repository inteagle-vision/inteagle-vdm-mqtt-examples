package com.inteagle.vdm.mqtt.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.sdk.ImageFrame;
import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.v1.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * SDK 维护者回归测试：仅连接显式指定的本地测试 Broker，用模拟设备验证真实 MQTT 收发。 使用 -Dvdm.test.mqtt.port=端口
 * 启用；不会连接现场设备或修改任何设备配置。
 */
@EnabledIfSystemProperty(named = "vdm.test.mqtt.port", matches = "\\d+")
class MqttBrokerIntegrationTest {
  @ParameterizedTest
  @MethodSource("formatsAndExecutors")
  void receivesTelemetryAndJpegAndCanCallRpcFromBusinessListener(
      PayloadFormat format, boolean virtualThreads) throws Exception {
    String id = "TEST" + UUID.randomUUID().toString().replace("-", "");
    int port = Integer.parseInt(System.getProperty("vdm.test.mqtt.port"));
    String base = "vdm/" + id;
    var mapper = new ObjectMapper();
    var error = new AtomicReference<Throwable>();
    var rpcDone = new CountDownLatch(1);
    var imageDone = new CountDownLatch(1);
    var client = new AtomicReference<VdmMqttClient>();
    var device =
        new MqttAsyncClient("tcp://127.0.0.1:" + port, "device-" + id, new MemoryPersistence());
    device.setCallback(
        new MqttCallback() {
          @Override
          public void connectionLost(Throwable cause) {
            error.set(cause);
          }

          @Override
          public void deliveryComplete(IMqttDeliveryToken token) {}

          @Override
          public void messageArrived(String topic, MqttMessage message) throws Exception {
            // 模拟设备只响应 getTargets；返回请求原始 ID，验证接入方的关联机制。
            byte[] response;
            if (format == PayloadFormat.PROTOBUF) {
              var request = RpcRequest.parseFrom(message.getPayload());
              response =
                  RpcResponse.newBuilder()
                      .setSchemaVersion(1)
                      .setReqId(request.getReqId())
                      .setGetTargets(GetTargetsResponse.newBuilder())
                      .build()
                      .toByteArray();
            } else {
              int reqId = mapper.readTree(message.getPayload()).path("reqId").asInt();
              response =
                  mapper.writeValueAsBytes(
                      Map.of(
                          "reqId",
                          reqId,
                          "code",
                          0,
                          "data",
                          Map.of("targets", java.util.List.of())));
            }
            // 使用异步发布，不在模拟设备自己的 Paho 回调中等待投递完成。
            device.publish(base + "/rpc/resp", response, 1, false);
          }
        });
    try {
      device.connect().waitForCompletion(5000);
      device.subscribe(base + "/rpc/req", 1).waitForCompletion(5000);
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(VdmMqttAutoConfiguration.class))
          .withPropertyValues(
              "vdm.mqtt.host=127.0.0.1",
              "vdm.mqtt.port=" + port,
              "vdm.mqtt.device-id=" + id,
              "vdm.mqtt.payload-format=" + format,
              "spring.threads.virtual.enabled=" + virtualThreads,
              "vdm.mqtt.subscriptions=telemetry,image,rpc/resp")
          .withBean(
              VdmMessageListener.class,
              () ->
                  (deviceId, message) -> {
                    try {
                      assertThat(deviceId).isEqualTo(id);
                      if (virtualThreads)
                        assertThat(
                                Thread.class.getMethod("isVirtual").invoke(Thread.currentThread()))
                            .isEqualTo(true);
                      if (message.suffix().equals("telemetry")) {
                        // 在业务监听器里同步调用 RPC，若错误地使用 MQTT 接收线程，此测试会超时。
                        assertThat(client.get().call("getTargets", Map.of(), Duration.ofSeconds(3)))
                            .isNotNull();
                        rpcDone.countDown();
                      } else if (message.value() instanceof ImageFrame image) {
                        assertThat(image.jpeg())
                            .containsExactly((byte) 0xff, (byte) 0xd8, (byte) 0xff, (byte) 0xd9);
                        imageDone.countDown();
                      }
                    } catch (Throwable failure) {
                      error.set(failure);
                    }
                  })
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                client.set(context.getBean(VdmMqttClient.class));
                byte[] telemetry =
                    format == PayloadFormat.PROTOBUF
                        ? Telemetry.newBuilder()
                            .setSchemaVersion(1)
                            .setDisplacement(
                                DisplacementTelemetry.newBuilder()
                                    .setFirstSampleTimestampMs(1000)
                                    .setSampleFrequencyHz(20))
                            .build()
                            .toByteArray()
                        : "{\"disp\":{\"t\":1,\"f\":20,\"d\":{}}}".getBytes(StandardCharsets.UTF_8);
                device.publish(base + "/telemetry", telemetry, 1, false).waitForCompletion(5000);
                device
                    .publish(
                        base + "/image",
                        new byte[] {
                          1, 8, 1, 1, 0, 0, 0, 1, (byte) 0xff, (byte) 0xd8, (byte) 0xff, (byte) 0xd9
                        },
                        1,
                        false)
                    .waitForCompletion(5000);
                assertThat(rpcDone.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(imageDone.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(error.get()).isNull();
              });
    } finally {
      if (device.isConnected()) device.disconnect().waitForCompletion(2000);
      device.close(true);
    }
  }

  // JDK 21 在同一 Broker 上额外验证虚拟线程中的同步 RPC，不能只验证配置绑定。
  static java.util.stream.Stream<Arguments> formatsAndExecutors() {
    return java.util.Arrays.stream(PayloadFormat.values())
        .flatMap(
            format ->
                Runtime.version().feature() >= 21
                    ? java.util.stream.Stream.of(
                        Arguments.of(format, false), Arguments.of(format, true))
                    : java.util.stream.Stream.of(Arguments.of(format, false)));
  }
}
