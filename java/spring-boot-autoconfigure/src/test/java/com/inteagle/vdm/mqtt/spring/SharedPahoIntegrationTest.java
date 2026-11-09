package com.inteagle.vdm.mqtt.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.inteagle.vdm.mqtt.paho.SharedPahoMqttTransport;
import com.inteagle.vdm.mqtt.sdk.VdmMqttTransport;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** SDK 维护者回归测试：真实 Broker 下验证复用应用 Paho Bean 不覆盖回调、不关闭应用连接。 */
@EnabledIfSystemProperty(named = "vdm.test.mqtt.port", matches = "\\d+")
class SharedPahoIntegrationTest {
  @Test
  void borrowedClientAndOriginalCallbackSurviveSpringContextShutdown() throws Exception {
    String id = "SHARED" + UUID.randomUUID().toString().replace("-", "");
    String customerTopic = "customer/" + id;
    var originalCallback = new CountDownLatch(1);
    var vdmMessage = new CountDownLatch(1);
    var borrowed = new AtomicReference<SharedPahoMqttTransport>();
    var customer =
        new MqttAsyncClient(
            "tcp://127.0.0.1:" + System.getProperty("vdm.test.mqtt.port"),
            id,
            new MemoryPersistence());
    customer.setCallback(
        new MqttCallbackExtended() {
          @Override
          public void connectComplete(boolean reconnect, String uri) {
            if (borrowed.get() != null) borrowed.get().connectionRestored();
          }

          @Override
          public void connectionLost(Throwable cause) {
            if (borrowed.get() != null) borrowed.get().connectionLost(cause);
          }

          @Override
          public void messageArrived(String topic, MqttMessage message) {
            if (topic.equals(customerTopic)) originalCallback.countDown();
          }

          @Override
          public void deliveryComplete(IMqttDeliveryToken token) {}
        });
    try {
      customer.connect().waitForCompletion(5000);
      customer.subscribe(customerTopic, 1).waitForCompletion(5000);
      borrowed.set(new SharedPahoMqttTransport(customer));
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(VdmMqttAutoConfiguration.class))
          // 不传 host/port：外部 Bean 已经配置连接参数，Starter 不应强制重复填写。
          .withPropertyValues("vdm.mqtt.device-id=" + id, "vdm.mqtt.payload-format=json")
          .withBean(VdmMqttTransport.class, borrowed::get)
          .withBean(VdmMessageListener.class, () -> (device, message) -> vdmMessage.countDown())
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                customer
                    .publish(
                        "vdm/" + id + "/telemetry",
                        "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        1,
                        false)
                    .waitForCompletion(5000);
                assertThat(vdmMessage.await(5, TimeUnit.SECONDS)).isTrue();
              });
      // Spring 容器已经关闭，应用自己的连接和非 VDM 订阅必须仍然可用。
      assertThat(customer.isConnected()).isTrue();
      customer.publish(customerTopic, new byte[] {1}, 1, false).waitForCompletion(5000);
      assertThat(originalCallback.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      if (customer.isConnected()) customer.disconnect().waitForCompletion(2000);
      customer.close(true);
    }
  }
}
