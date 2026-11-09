package com.inteagle.vdm.mqtt.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.inteagle.vdm.mqtt.sdk.DecodedPayload;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/** SDK 维护者回归测试：验证业务线程隔离、监听器异常隔离和队列过载行为。 */
class VdmMessageDispatcherTest {
  private final DecodedPayload message =
      new DecodedPayload("vdm/TEST/telemetry", "telemetry", new byte[0], null, null);

  @ParameterizedTest
  @MethodSource("executionModes")
  void callbackIsOffMqttThreadAndOtherListenersSurviveFailure(String mode) throws Exception {
    var beans = new DefaultListableBeanFactory();
    var finished = new CountDownLatch(1);
    var callbackThread = new AtomicReference<Thread>();
    beans.registerSingleton(
        "broken",
        (VdmMessageListener)
            (device, payload) -> {
              throw new IllegalStateException("模拟业务异常");
            });
    beans.registerSingleton(
        "working",
        (VdmMessageListener)
            (device, payload) -> {
              callbackThread.set(Thread.currentThread());
              finished.countDown();
            });
    try (var custom = new SimpleAsyncTaskExecutor("customer-vdm-");
        var dispatcher = dispatcher(mode, 1, 1, beans, custom)) {
      dispatcher.accept("TEST", message);
      assertThat(finished.await(2, TimeUnit.SECONDS)).isTrue();
      assertThat(callbackThread.get()).isNotSameAs(Thread.currentThread());
      if (mode.equals("virtual"))
        assertThat(Thread.class.getMethod("isVirtual").invoke(callbackThread.get()))
            .isEqualTo(true);
      if (mode.equals("custom"))
        assertThat(callbackThread.get().getName()).startsWith("customer-vdm-");
    }
  }

  @ParameterizedTest
  @MethodSource("executionModes")
  void overloadFailsExplicitlyInsteadOfRunningOnMqttThread(String mode) throws Exception {
    // 阻塞第一个业务任务、填满唯一队列槽，第三条必须报错，不能偷偷回退到调用线程。
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton(
        "slow",
        (VdmMessageListener)
            (device, payload) -> {
              entered.countDown();
              release.await(3, TimeUnit.SECONDS);
            });
    try (var custom = new SimpleAsyncTaskExecutor("customer-vdm-");
        var dispatcher = dispatcher(mode, 1, 1, beans, custom)) {
      try {
        dispatcher.accept("TEST", message);
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        dispatcher.accept("TEST", message);
        assertThatThrownBy(() -> dispatcher.accept("TEST", message))
            .isInstanceOf(RejectedExecutionException.class);
      } finally {
        release.countDown();
      }
    }
  }

  @ParameterizedTest
  @MethodSource("executionModes")
  void keepsEachDeviceOrderedWithoutBlockingOtherPartitions(String mode) throws Exception {
    // A/B 在两个不同分区；阻塞 A 的首条消息，B 仍应完成，A 的第二条只能随后执行。
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var otherDeviceDone = new CountDownLatch(1);
    var allDone = new CountDownLatch(3);
    var received = new java.util.concurrent.CopyOnWriteArrayList<String>();
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton(
        "multi",
        (VdmMessageListener)
            (device, payload) -> {
              if (device.equals("A") && payload.suffix().equals("first")) {
                entered.countDown();
                release.await(3, TimeUnit.SECONDS);
              }
              received.add(device + ":" + payload.suffix());
              if (device.equals("B")) otherDeviceDone.countDown();
              allDone.countDown();
            });
    try (var custom = new SimpleAsyncTaskExecutor("customer-vdm-");
        var dispatcher = dispatcher(mode, 2, 2, beans, custom)) {
      try {
        dispatcher.accept("A", payload("A", "first"));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        dispatcher.accept("A", payload("A", "second"));
        dispatcher.accept("B", payload("B", "first"));
        assertThat(otherDeviceDone.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactly("B:first");
      } finally {
        release.countDown();
      }
      assertThat(allDone.await(2, TimeUnit.SECONDS)).isTrue();
      assertThat(received).containsExactly("B:first", "A:first", "A:second");
    }
  }

  @Test
  void synchronousExecutorIsRejectedBeforeCallingBusinessCode() {
    // 应用传入 Runnable::run 或 CallerRunsPolicy 时明确报错，不能让 MQTT 回调被业务 RPC 阻塞。
    var called = new java.util.concurrent.atomic.AtomicBoolean();
    var beans = new DefaultListableBeanFactory();
    beans.registerSingleton("listener", (VdmMessageListener) (device, payload) -> called.set(true));
    try (var dispatcher =
        new VdmMessageDispatcher(
            1, 1, beans.getBeanProvider(VdmMessageListener.class), Runnable::run, false)) {
      assertThatThrownBy(() -> dispatcher.accept("A", message))
          .isInstanceOf(RejectedExecutionException.class);
      assertThat(called).isFalse();
    }
  }

  @Test
  void closingDispatcherDoesNotCloseBorrowedExecutor() throws Exception {
    var beans = new DefaultListableBeanFactory();
    try (var custom = new SimpleAsyncTaskExecutor("customer-vdm-")) {
      var dispatcher = dispatcher("custom", 1, 1, beans, custom);
      dispatcher.close();
      var finished = new CountDownLatch(1);
      custom.execute(finished::countDown);
      assertThat(finished.await(2, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(() -> dispatcher.accept("A", message))
          .isInstanceOf(RejectedExecutionException.class);
    }
  }

  @Test
  void java17ReportsAnExplicitErrorForVirtualThreads() {
    if (Runtime.version().feature() >= 21) return;
    var beans = new DefaultListableBeanFactory();
    assertThatThrownBy(() -> dispatcher("virtual", 1, 1, beans, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("JDK 21+");
  }

  // 同一组顺序、过载和异常测试覆盖默认执行器、自定义执行器，以及 JDK 21 的真实虚拟线程。
  static java.util.stream.Stream<String> executionModes() {
    return Runtime.version().feature() >= 21
        ? java.util.stream.Stream.of("platform", "custom", "virtual")
        : java.util.stream.Stream.of("platform", "custom");
  }

  private VdmMessageDispatcher dispatcher(
      String mode,
      int concurrency,
      int capacity,
      DefaultListableBeanFactory beans,
      SimpleAsyncTaskExecutor custom) {
    return new VdmMessageDispatcher(
        concurrency,
        capacity,
        beans.getBeanProvider(VdmMessageListener.class),
        mode.equals("custom") ? custom : null,
        mode.equals("virtual"));
  }

  private DecodedPayload payload(String device, String sequence) {
    return new DecodedPayload("vdm/" + device + "/telemetry", sequence, new byte[0], null, null);
  }
}
