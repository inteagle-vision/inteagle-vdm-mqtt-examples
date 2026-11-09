package com.inteagle.vdm.mqtt.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmMqttTransport;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** SDK 维护者回归测试：验证自动配置、默认订阅和用户覆盖；关闭自动连接，无需 Broker。 */
class VdmMqttAutoConfigurationTest {
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(VdmMqttAutoConfiguration.class));

  @Test
  void disabledNeedsNeitherBrokerNorDevice() {
    // 禁用应整体生效，不能因缺少 host/device-id 而阻止业务应用启动。
    runner
        .withPropertyValues("vdm.mqtt.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed().doesNotHaveBean(VdmMqttClient.class);
              assertThat(context).doesNotHaveBean(VdmMessageDispatcher.class);
            });
  }

  @Test
  void defaultOnlySubscribesTelemetry() {
    offline()
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(VdmMqttClient.class);
              var config = context.getBean(VdmMqttProperties.class).clientConfig();
              assertThat(config.subscriptionSuffixes()).containsExactly("telemetry");
              assertThat(config.payloadFormat()).isEqualTo(PayloadFormat.PROTOBUF);
              assertThat(context.getBean(VdmMqttLifecycle.class).isRunning()).isFalse();
            });
  }

  @Test
  void imageAndFullSubscriptionsAreAvailableWithoutExtraHandlers() {
    // 按需精简只是配置默认值，不能裁剪 Starter 本身的完整能力。
    for (String subscription : List.of("image", "#")) {
      offline()
          .withPropertyValues(
              "vdm.mqtt.subscriptions[0]=" + subscription,
              "vdm.mqtt.payload-format=json",
              "vdm.mqtt.connect-timeout=1500ms")
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                var config = context.getBean(VdmMqttProperties.class).clientConfig();
                assertThat(config.subscriptionSuffixes()).containsExactly(subscription);
                assertThat(config.connectTimeout()).isEqualTo(Duration.ofMillis(1500));
                assertThat(config.payloadFormat()).isEqualTo(PayloadFormat.JSON);
              });
    }
  }

  @Test
  void invalidConfigurationFailsBeforeConnecting() {
    for (String property :
        List.of(
            "vdm.mqtt.device-id=bad/id",
            "vdm.mqtt.port=0",
            "vdm.mqtt.qos=3",
            "vdm.mqtt.connect-timeout=0ms",
            "vdm.mqtt.listener-queue-capacity=0",
            "vdm.mqtt.listener-concurrency=0",
            "vdm.mqtt.subscriptions[0]=unknown")) {
      offline().withPropertyValues(property).run(context -> assertThat(context).hasFailed());
    }
  }

  @Test
  void customClientOwnsItsLifecycle() throws Exception {
    // 用户显式提供客户端后，Starter 不应强加配置、重复连接或注册自己的生命周期。
    var custom =
        new VdmMqttClient(
            new VdmMqttClient.Config(
                "localhost",
                1883,
                VdmTopics.forDevice("CUSTOM"),
                PayloadFormat.JSON,
                null,
                null,
                null,
                1,
                Duration.ofSeconds(1),
                List.of("image")),
            new TestTransport(),
            null,
            null);
    try (custom) {
      runner
          .withBean(VdmMqttClient.class, () -> custom)
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(VdmMqttClient.class)).isSameAs(custom);
                assertThat(context).doesNotHaveBean(VdmMqttLifecycle.class);
              });
    }
  }

  // 用无网络传输证明客户端可以完全脱离 Paho。
  static class TestTransport implements VdmMqttTransport {
    boolean started;

    public void start(List<String> topics, int qos, Duration timeout, Listener listener) {
      started = true;
    }

    public void publish(
        String topic, byte[] payload, int qos, boolean retained, Duration timeout) {}

    public void close() {}
  }

  @Test
  void customTransportWorksEvenWithoutPahoOnClasspath() {
    var transport = new TestTransport();
    offline()
        .withPropertyValues("vdm.mqtt.auto-startup=true")
        .withClassLoader(
            new org.springframework.boot.test.context.FilteredClassLoader(
                "org.eclipse.paho", "com.inteagle.vdm.mqtt.paho"))
        .withBean(VdmMqttTransport.class, () -> transport)
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(VdmMqttClient.class);
              assertThat(context.getBean(VdmMqttTransport.class)).isSameAs(transport);
              assertThat(transport.started).isTrue();
            });
  }

  @Test
  void multipleDevicesUseGatewayWithoutCreatingASingleDeviceBean() {
    runner
        .withPropertyValues("vdm.mqtt.device-ids=D1,D2", "vdm.mqtt.auto-startup=true")
        .withBean(VdmMqttTransport.class, TestTransport::new)
        .run(
            context -> {
              assertThat(context).hasNotFailed().doesNotHaveBean(VdmMqttClient.class);
              var gateway = context.getBean(com.inteagle.vdm.mqtt.sdk.VdmMqttGateway.class);
              assertThat(gateway.device("D1")).isNotSameAs(gateway.device("D2"));
            });
  }

  @Test
  void mqttTopicFiltersBindWithoutSingleDeviceAndRejectMixedModes() {
    runner
        .withPropertyValues(
            "vdm.mqtt.topic-filters[0]=vdm/+/attributes", "vdm.mqtt.topic-filters[1]=vdm/B/image")
        .withBean(VdmMqttTransport.class, TestTransport::new)
        .run(
            context -> {
              assertThat(context).hasNotFailed().doesNotHaveBean(VdmMqttClient.class);
              assertThat(
                      context
                          .getBean(com.inteagle.vdm.mqtt.sdk.VdmMqttGateway.class)
                          .topicFilters())
                  .containsExactly("vdm/+/attributes", "vdm/B/image");
            });
    runner
        .withPropertyValues("vdm.mqtt.topic-filters[0]=vdm/#", "vdm.mqtt.all-devices=true")
        .withBean(VdmMqttTransport.class, TestTransport::new)
        .run(context -> assertThat(context).hasFailed());
  }

  private ApplicationContextRunner offline() {
    return runner.withPropertyValues(
        "vdm.mqtt.host=localhost", "vdm.mqtt.device-id=TEST", "vdm.mqtt.auto-startup=false");
  }

  @Test
  void namedExecutorOverridesDefaultAndOtherApplicationExecutorsAreIgnored() {
    var received = new java.util.concurrent.CountDownLatch(1);
    var threadName = new java.util.concurrent.atomic.AtomicReference<String>();
    offline()
        .withBean(
            "applicationTaskExecutor", java.util.concurrent.Executor.class, () -> Runnable::run)
        .withBean(
            "vdmMqttTaskExecutor",
            org.springframework.core.task.SimpleAsyncTaskExecutor.class,
            () -> new org.springframework.core.task.SimpleAsyncTaskExecutor("customer-vdm-"))
        .withBean(
            VdmMessageListener.class,
            () ->
                (device, payload) -> {
                  threadName.set(Thread.currentThread().getName());
                  received.countDown();
                })
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              context
                  .getBean(VdmMessageDispatcher.class)
                  .accept(
                      "TEST",
                      new com.inteagle.vdm.mqtt.sdk.DecodedPayload(
                          "vdm/TEST/telemetry", "telemetry", new byte[0], null, null));
              assertThat(received.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
              assertThat(threadName.get()).startsWith("customer-vdm-");
            });
  }

  @Test
  void virtualThreadsPropertySelectsVirtualExecutorOnJava21() {
    var received = new java.util.concurrent.CountDownLatch(1);
    var thread = new java.util.concurrent.atomic.AtomicReference<Thread>();
    offline()
        .withPropertyValues("spring.threads.virtual.enabled=true")
        .withBean(
            VdmMessageListener.class,
            () ->
                (device, payload) -> {
                  thread.set(Thread.currentThread());
                  received.countDown();
                })
        .run(
            context -> {
              if (Runtime.version().feature() < 21) {
                assertThat(context).hasFailed();
                return;
              }
              assertThat(context).hasNotFailed();
              context
                  .getBean(VdmMessageDispatcher.class)
                  .accept(
                      "TEST",
                      new com.inteagle.vdm.mqtt.sdk.DecodedPayload(
                          "vdm/TEST/telemetry", "telemetry", new byte[0], null, null));
              assertThat(received.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
              assertThat(Thread.class.getMethod("isVirtual").invoke(thread.get())).isEqualTo(true);
            });
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
  void shutdownDrainsMessagesBeforeDestroyingBusinessResources(boolean autoStartup) {
    var stopped = new java.util.concurrent.CountDownLatch(1);
    var resource = new DrainingListener();
    var transport =
        new TestTransport() {
          @Override
          public void close() {
            stopped.countDown();
          }
        };
    runner
        .withPropertyValues("vdm.mqtt.all-devices=true", "vdm.mqtt.auto-startup=" + autoStartup)
        .withBean(VdmMqttTransport.class, () -> transport)
        .withBean(DrainingListener.class, () -> resource)
        .run(
            context -> {
              // 业务组件通常依赖 Gateway；显式注册同样的销毁依赖，复现业务资源先于 Dispatcher 销毁的风险。
              var application = context.getSourceApplicationContext();
              String listenerName = context.getBeanNamesForType(DrainingListener.class)[0];
              application.getBeanFactory().registerDependentBean("vdmMqttGateway", listenerName);
              if (!autoStartup)
                context.getBean(com.inteagle.vdm.mqtt.sdk.VdmMqttGateway.class).start();
              var dispatcher = context.getBean(VdmMessageDispatcher.class);
              var message =
                  new com.inteagle.vdm.mqtt.sdk.DecodedPayload(
                      "vdm/A/telemetry", "telemetry", new byte[0], null, null);
              dispatcher.accept("A", message);
              assertThat(resource.entered.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
              dispatcher.accept("A", message);
              var closing = new Thread(application::close);
              closing.start();
              try {
                assertThat(stopped.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThat(resource.destroyed).isFalse();
              } finally {
                resource.release.countDown();
                closing.join(7000);
              }
              assertThat(closing.isAlive()).isFalse();
              assertThat(resource.events).containsExactly("message", "message", "destroy");
            });
  }

  static class DrainingListener
      implements VdmMessageListener, org.springframework.beans.factory.DisposableBean {
    final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    final java.util.List<String> events = new java.util.concurrent.CopyOnWriteArrayList<>();
    volatile boolean destroyed;

    public void onMessage(String device, com.inteagle.vdm.mqtt.sdk.DecodedPayload payload)
        throws Exception {
      entered.countDown();
      release.await(3, java.util.concurrent.TimeUnit.SECONDS);
      events.add(destroyed ? "closed-resource" : "message");
    }

    public void destroy() {
      destroyed = true;
      events.add("destroy");
    }
  }

  @Test
  void blankSingleDeviceOverrideDoesNotCreateSingleDeviceClientInAllDevicesMode() {
    // 真实接入发现的回归：ConditionalOnProperty 会匹配空字符串，不能据此创建单设备 Bean。
    runner
        .withPropertyValues("vdm.mqtt.device-id=", "vdm.mqtt.all-devices=true")
        .withBean(VdmMqttTransport.class, TestTransport::new)
        .run(
            context -> {
              assertThat(context).hasNotFailed().doesNotHaveBean(VdmMqttClient.class);
              assertThat(context).hasSingleBean(com.inteagle.vdm.mqtt.sdk.VdmMqttGateway.class);
            });
  }
}
