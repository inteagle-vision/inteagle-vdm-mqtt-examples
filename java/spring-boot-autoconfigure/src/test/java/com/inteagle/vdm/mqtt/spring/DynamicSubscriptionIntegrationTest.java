package com.inteagle.vdm.mqtt.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.inteagle.vdm.mqtt.paho.*;
import com.inteagle.vdm.mqtt.sdk.*;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** SDK 维护者回归测试：本地 Broker 验证两种适配器动态订阅、retained 接收、取消和借用连接重连。 */
@EnabledIfSystemProperty(named = "vdm.test.mqtt.port", matches = "\\d+")
class DynamicSubscriptionIntegrationTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void changesSubscriptionsOnSameConnectionAndRestoresCurrentSet(boolean shared) throws Exception {
    String id = "DYNAMIC" + UUID.randomUUID().toString().replace("-", "");
    String topic = "vdm/" + id + "/attributes";
    int port = Integer.parseInt(System.getProperty("vdm.test.mqtt.port"));
    var config =
        new VdmMqttClient.Config(
            "127.0.0.1",
            port,
            VdmTopics.forDevice(id),
            PayloadFormat.JSON,
            null,
            null,
            id,
            1,
            Duration.ofSeconds(5),
            List.of("telemetry"));
    var customer =
        new MqttAsyncClient("tcp://127.0.0.1:" + port, "owner-" + id, new MemoryPersistence());
    var publisher =
        new MqttAsyncClient("tcp://127.0.0.1:" + port, "device-" + id, new MemoryPersistence());
    var received = new LinkedBlockingQueue<String>();
    var errors = new LinkedBlockingQueue<Throwable>();
    SharedPahoMqttTransport borrowed = shared ? new SharedPahoMqttTransport(customer) : null;
    VdmMqttTransport transport = shared ? borrowed : new PahoMqttTransport(config);
    try (var gateway =
        new VdmMqttGateway(
            config,
            List.of(),
            transport,
            (device, message) -> received.add(device + ":" + message.suffix()),
            errors::add)) {
      if (shared) customer.connect().waitForCompletion(5000);
      publisher.connect().waitForCompletion(5000);
      gateway.start(); // 空列表启动后再增加，初始不应偷偷订阅 telemetry。
      publisher.publish(topic, "{\"targets\":[]}".getBytes(), 1, true).waitForCompletion(5000);
      gateway.subscribe(topic);
      assertThat(received.poll(5, TimeUnit.SECONDS))
          .isEqualTo(id + ":attributes"); // 包括 SUBACK 附近立即交付的 retained。
      gateway.unsubscribe(topic);
      publisher.publish(topic, "{}".getBytes(), 1, false).waitForCompletion(5000);
      assertThat(received.poll(300, TimeUnit.MILLISECONDS)).isNull();
      gateway.subscribe("vdm/" + id + "/event");
      if (shared) {
        // 原连接所有者完成重连后转发事件；已取消 attributes 不得复活，event 必须恢复。
        customer.disconnect().waitForCompletion(5000);
        customer.connect().waitForCompletion(5000);
        borrowed.connectionRestored();
      }
      // 重连订阅异步完成，短时间重发模拟事件，直到观察到恢复。
      String result = null;
      for (int i = 0; i < 20 && result == null; i++) {
        publisher
            .publish("vdm/" + id + "/event", "{}".getBytes(), 1, false)
            .waitForCompletion(5000);
        result = received.poll(100, TimeUnit.MILLISECONDS);
      }
      assertThat(result).isEqualTo(id + ":event");
      assertThat(errors).isEmpty();
    } finally {
      if (publisher.isConnected()) {
        publisher
            .publish(topic, new byte[0], 1, true)
            .waitForCompletion(2000); // 清理本测试创建的 retained。
        publisher.disconnect().waitForCompletion(2000);
      }
      publisher.close(true);
      if (customer.isConnected()) customer.disconnect().waitForCompletion(2000);
      customer.close(true);
    }
  }
}
